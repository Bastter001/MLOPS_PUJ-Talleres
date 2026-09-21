from datetime import datetime
import os
from pathlib import Path
from urllib.parse import quote_plus

import joblib
import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator


FEATURES = [
    "bill_length_mm",
    "bill_depth_mm",
    "flipper_length_mm",
    "body_mass_g",
]

MODEL_PATH = Path(
    os.getenv("MODEL_PATH", "/opt/ml/model/penguins_model.joblib")
)


def get_engine():
    """
    Conexión a MySQL usando el nombre del servicio Docker: mysql.
    """
    host = os.getenv("MYSQL_HOST", "mysql")
    port = os.getenv("MYSQL_PORT", "3306")
    database = os.getenv("MYSQL_DATABASE", "mlops")
    user = quote_plus(os.getenv("MYSQL_USER", "mlops_user"))
    password = quote_plus(os.getenv("MYSQL_PASSWORD", "mlops_pass_2026"))

    return create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
        pool_pre_ping=True,
    )


def validate_model_and_data():
    """
    Verifica que exista el artefacto entrenado
    y que penguins_processed esté disponible.
    """
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"No existe {MODEL_PATH}. "
            "Ejecute primero 01_penguins_training_pipeline."
        )

    engine = get_engine()

    schema_name = os.getenv(
        "MYSQL_DATABASE",
        "mlops",
    )

    with engine.connect() as connection:
        table_exists = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = :schema_name
                  AND table_name = 'penguins_processed'
                """
            ),
            {
                "schema_name": schema_name
            },
        ).scalar_one()

    if int(table_exists) != 1:
        raise ValueError(
            "No existe la tabla penguins_processed."
        )

    with engine.connect() as connection:
        row_count = connection.execute(
            text(
                "SELECT COUNT(*) "
                "FROM penguins_processed"
            )
        ).scalar_one()

    if int(row_count) == 0:
        raise ValueError(
            "penguins_processed está vacía."
        )

    print(
        "OK: modelo y datos preprocesados "
        "están disponibles."
    )
    print(f"Registros disponibles: {row_count}")


def run_batch_inference():
    """
    Toma una muestra desde MySQL,
    ejecuta el modelo y guarda batch_predictions.
    """
    engine = get_engine()

    source = pd.read_sql(
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
        LIMIT 25
        """,
        engine,
    )

    if source.empty:
        raise ValueError(
            "No existen registros para inferencia."
        )

    artifact = joblib.load(MODEL_PATH)

    if isinstance(artifact, dict) and "model" in artifact:
        model = artifact["model"]
        features = artifact.get(
            "features",
            FEATURES,
        )
        raw_mapping = artifact.get(
            "id_to_species",
            {},
        )
        id_to_species = {
            int(key): value
            for key, value in raw_mapping.items()
        }
    else:
        # Compatibilidad si se guardó únicamente el modelo.
        model = artifact
        features = FEATURES

        mapping = (
            source[["species", "target"]]
            .drop_duplicates()
            .sort_values("target")
        )

        id_to_species = {
            int(row.target): row.species
            for row in mapping.itertuples(index=False)
        }

    predictions = (
        model.predict(
            source[features]
        )
        .astype(int)
    )

    result = source.copy()

    result["predicted_target"] = predictions

    result["predicted_species"] = [
        id_to_species.get(
            int(value),
            str(value),
        )
        for value in predictions
    ]

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

    result.to_sql(
        "batch_predictions",
        engine,
        if_exists="replace",
        index=False,
    )

    print(
        f"OK: {len(result)} predicciones "
        "guardadas en batch_predictions."
    )


def summarize_batch():
    """
    Muestra en los logs un resumen por especie predicha.
    """
    engine = get_engine()

    summary = pd.read_sql(
        """
        SELECT
            predicted_species,
            COUNT(*) AS total
        FROM batch_predictions
        GROUP BY predicted_species
        ORDER BY total DESC
        """,
        engine,
    )

    print("RESUMEN DE PREDICCIONES:")
    print(summary.to_string(index=False))


with DAG(
    dag_id="02_penguins_batch_inference",
    description=(
        "Valida el modelo, hace inferencia batch "
        "y guarda predicciones en MySQL."
    ),
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "penguins", "batch-inference"],
) as dag:

    validate = PythonOperator(
        task_id="01_validate_model_and_data",
        python_callable=validate_model_and_data,
    )

    infer = PythonOperator(
        task_id="02_run_batch_inference",
        python_callable=run_batch_inference,
    )

    summarize = PythonOperator(
        task_id="03_summarize_batch",
        python_callable=summarize_batch,
    )

    validate >> infer >> summarize

