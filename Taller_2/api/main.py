import os
from pathlib import Path
from urllib.parse import quote_plus

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text


# ============================================================
# 1. CREAR LA APLICACION FASTAPI
# ============================================================

app = FastAPI(
    title="Penguins MLOps API",
    version="1.0.0",
    description="API de inferencia para el modelo entrenado por Airflow",
)


# ============================================================
# 2. RUTA DEL MODELO ENTRENADO
# ============================================================

MODEL_PATH = Path(
    os.getenv(
        "MODEL_PATH",
        "/opt/ml/model/penguins_model.joblib"
    )
)


# Variables que usa el modelo para hacer una prediccion
DEFAULT_FEATURES = [
    "bill_length_mm",
    "bill_depth_mm",
    "flipper_length_mm",
    "body_mass_g",
]


# ============================================================
# 3. CONEXION A MYSQL
# ============================================================

def get_engine():

    host = os.getenv("MYSQL_HOST", "mysql")
    port = os.getenv("MYSQL_PORT", "3306")
    database = os.getenv("MYSQL_DATABASE", "mlops")

    user = quote_plus(
        os.getenv(
            "MYSQL_USER",
            "mlops_user"
        )
    )

    password = quote_plus(
        os.getenv(
            "MYSQL_PASSWORD",
            "mlops_pass_2026"
        )
    )

    connection_string = (
        f"mysql+pymysql://"
        f"{user}:{password}"
        f"@{host}:{port}/{database}"
    )

    return create_engine(
        connection_string,
        pool_pre_ping=True
    )


# ============================================================
# 4. DEFINIR LOS DATOS QUE RECIBE /predict
# ============================================================

class PenguinRequest(BaseModel):

    bill_length_mm: float = Field(..., gt=0)

    bill_depth_mm: float = Field(..., gt=0)

    flipper_length_mm: float = Field(..., gt=0)

    body_mass_g: float = Field(..., gt=0)


# ============================================================
# 5. CARGAR EL MODELO
# ============================================================

def load_model_artifact():

    if not MODEL_PATH.exists():

        raise HTTPException(
            status_code=503,
            detail=(
                "El modelo todavia no existe. "
                "Ejecute primero el DAG "
                "01_penguins_training_pipeline."
            )
        )

    try:

        saved_object = joblib.load(MODEL_PATH)

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"No se pudo cargar el modelo: {exc}"
        )


    # --------------------------------------------------------
    # Caso recomendado:
    # el DAG guardo un diccionario con modelo + metadata
    # --------------------------------------------------------

    if isinstance(saved_object, dict) and "model" in saved_object:

        model = saved_object["model"]

        features = saved_object.get(
            "features",
            DEFAULT_FEATURES
        )

        raw_mapping = saved_object.get(
            "id_to_species",
            {
                0: "Adelie",
                1: "Chinstrap",
                2: "Gentoo"
            }
        )

        id_to_species = {
            int(key): value
            for key, value in raw_mapping.items()
        }

        return {
            "model": model,
            "features": features,
            "id_to_species": id_to_species,
            "metadata": saved_object
        }


    # --------------------------------------------------------
    # Caso alternativo:
    # el DAG guardo directamente RandomForestClassifier
    # --------------------------------------------------------

    return {

        "model": saved_object,

        "features": DEFAULT_FEATURES,

        "id_to_species": {
            0: "Adelie",
            1: "Chinstrap",
            2: "Gentoo"
        },

        "metadata": {}
    }


# ============================================================
# 6. CREAR TABLA DE LOG DE PREDICCIONES
# ============================================================

def create_prediction_log_table():

    engine = get_engine()

    sql = """
    CREATE TABLE IF NOT EXISTS prediction_log (

        id BIGINT AUTO_INCREMENT PRIMARY KEY,

        prediction_mode VARCHAR(30) NOT NULL,

        bill_length_mm DOUBLE NOT NULL,

        bill_depth_mm DOUBLE NOT NULL,

        flipper_length_mm DOUBLE NOT NULL,

        body_mass_g DOUBLE NOT NULL,

        predicted_target INT NOT NULL,

        predicted_species VARCHAR(50) NOT NULL,

        confidence DOUBLE NULL,

        predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    with engine.begin() as connection:

        connection.execute(
            text(sql)
        )


# ============================================================
# 7. RUTA PRINCIPAL
# ============================================================

@app.get("/")
def root():

    return {

        "service": "Penguins MLOps API",

        "status": "running",

        "health": "/health",

        "prediction_method_1": "/predict",

        "prediction_method_2": "/predict-db",

        "documentation": "/docs"
    }


# ============================================================
# 8. HEALTH CHECK
# ============================================================

@app.get("/health")
def health():

    mysql_status = "error"

    mysql_error = None

    try:

        engine = get_engine()

        with engine.connect() as connection:

            connection.execute(
                text("SELECT 1")
            )

        mysql_status = "ok"

    except Exception as exc:

        mysql_error = str(exc)


    return {

        "api": "ok",

        "mysql": mysql_status,

        "mysql_error": mysql_error,

        "model_ready": MODEL_PATH.exists(),

        "model_path": str(MODEL_PATH)
    }


# ============================================================
# 9. METODO DE PREDICCION NUMERO 1
#
# PREDICCION INDIVIDUAL
# ============================================================

@app.post("/predict")
def predict_single(payload: PenguinRequest):

    loaded = load_model_artifact()

    model = loaded["model"]

    features = loaded["features"]

    id_to_species = loaded["id_to_species"]

    metadata = loaded["metadata"]


    # Crear una fila con los datos recibidos

    row = pd.DataFrame(
        [
            {
                "bill_length_mm":
                    payload.bill_length_mm,

                "bill_depth_mm":
                    payload.bill_depth_mm,

                "flipper_length_mm":
                    payload.flipper_length_mm,

                "body_mass_g":
                    payload.body_mass_g
            }
        ]
    )


    # Hacer prediccion

    predicted_target = int(
        model.predict(
            row[features]
        )[0]
    )


    predicted_species = id_to_species.get(
        predicted_target,
        str(predicted_target)
    )


    # Calcular confianza si el modelo lo permite

    confidence = None

    if hasattr(model, "predict_proba"):

        probabilities = model.predict_proba(
            row[features]
        )[0]

        confidence = float(
            max(probabilities)
        )


    # Crear tabla de auditoria

    create_prediction_log_table()


    # Guardar prediccion en MySQL

    engine = get_engine()

    insert_sql = """
    INSERT INTO prediction_log (

        prediction_mode,

        bill_length_mm,

        bill_depth_mm,

        flipper_length_mm,

        body_mass_g,

        predicted_target,

        predicted_species,

        confidence

    )
    VALUES (

        'single',

        :bill_length_mm,

        :bill_depth_mm,

        :flipper_length_mm,

        :body_mass_g,

        :predicted_target,

        :predicted_species,

        :confidence
    )
    """


    with engine.begin() as connection:

        connection.execute(

            text(insert_sql),

            {

                "bill_length_mm":
                    payload.bill_length_mm,

                "bill_depth_mm":
                    payload.bill_depth_mm,

                "flipper_length_mm":
                    payload.flipper_length_mm,

                "body_mass_g":
                    payload.body_mass_g,

                "predicted_target":
                    predicted_target,

                "predicted_species":
                    predicted_species,

                "confidence":
                    confidence
            }
        )


    return {

        "method":
            "single_prediction",

        "predicted_target":
            predicted_target,

        "predicted_species":
            predicted_species,

        "confidence":
            confidence,

        "model_accuracy_at_training":
            metadata.get("accuracy"),

        "model_trained_at_utc":
            metadata.get("trained_at_utc")
    }


# ============================================================
# 10. METODO DE PREDICCION NUMERO 2
#
# PREDICCION BATCH DESDE MYSQL
# ============================================================

@app.post("/predict-db")
def predict_from_mysql(

    limit: int = Query(
        default=25,
        ge=1,
        le=500
    )
):

    loaded = load_model_artifact()

    model = loaded["model"]

    features = loaded["features"]

    id_to_species = loaded["id_to_species"]


    engine = get_engine()


    # --------------------------------------------------------
    # Leer datos procesados desde MySQL
    # --------------------------------------------------------

    try:

        source = pd.read_sql(

            text(
                """
                SELECT

                    species,

                    bill_length_mm,

                    bill_depth_mm,

                    flipper_length_mm,

                    body_mass_g,

                    target

                FROM penguins_processed

                ORDER BY RAND()

                LIMIT :limit
                """
            ),

            engine,

            params={
                "limit": limit
            }
        )


    except Exception as exc:

        raise HTTPException(

            status_code=500,

            detail=(
                "No fue posible leer "
                "penguins_processed. "
                "Ejecute primero el DAG 01. "
                f"Error: {exc}"
            )
        )


    if source.empty:

        raise HTTPException(

            status_code=404,

            detail=(
                "La tabla penguins_processed "
                "no contiene registros."
            )
        )


    # --------------------------------------------------------
    # Hacer predicciones
    # --------------------------------------------------------

    predicted_targets = model.predict(
        source[features]
    ).astype(int)


    result = source.copy()


    result["predicted_target"] = (
        predicted_targets
    )


    result["predicted_species"] = [

        id_to_species.get(
            int(value),
            str(value)
        )

        for value in predicted_targets
    ]


    # --------------------------------------------------------
    # Calcular probabilidades
    # --------------------------------------------------------

    if hasattr(model, "predict_proba"):

        probabilities = model.predict_proba(
            source[features]
        )

        result["confidence"] = [

            float(max(row))

            for row in probabilities
        ]

    else:

        result["confidence"] = None


    result["predicted_at_utc"] = (
        pd.Timestamp.utcnow()
    )


    # --------------------------------------------------------
    # Guardar resultado batch en MySQL
    # --------------------------------------------------------

    result.to_sql(

        "api_batch_predictions",

        engine,

        if_exists="replace",

        index=False
    )


    # --------------------------------------------------------
    # Respuesta HTTP
    # --------------------------------------------------------

    return {

        "method":
            "batch_prediction_from_mysql",

        "source_table":
            "penguins_processed",

        "destination_table":
            "api_batch_predictions",

        "total_predictions":
            int(len(result)),

        "predictions":
            result.to_dict(
                orient="records"
            )
   }
