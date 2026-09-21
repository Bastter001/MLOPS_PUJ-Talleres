from datetime import datetime
import os
from pathlib import Path
from urllib.parse import quote_plus

import joblib
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


FEATURES = [
    "bill_length_mm",
    "bill_depth_mm",
    "flipper_length_mm",
    "body_mass_g",
]

DATASET_PATH = Path(
    os.getenv("DATASET_PATH", "/opt/ml/data/penguins.csv")
)

MODEL_PATH = Path(
    os.getenv("MODEL_PATH", "/opt/ml/model/penguins_model.joblib")
)


def get_engine():
    """
    Crea la conexión hacia la base MySQL del ejercicio.
    Importante: MYSQL_HOST debe ser 'mysql', porque ese es
    el nombre del servicio dentro de Docker Compose.
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


def reset_exercise_tables():
    """
    Borra las tablas generadas por el ejercicio anterior.
    NO borra MySQL.
    NO borra el volumen.
    NO borra PostgreSQL.
    """
    engine = get_engine()

    tables = [
        "api_batch_predictions",
        "batch_predictions",
        "model_metrics",
        "prediction_log",
        "penguins_processed",
        "penguins_raw",
    ]

    with engine.begin() as connection:
        for table_name in tables:
            connection.execute(
                text(f"DROP TABLE IF EXISTS `{table_name}`")
            )

    print("OK: tablas anteriores del ejercicio eliminadas.")


def load_raw_penguins():
    """
    Lee penguins.csv y lo carga SIN preprocesamiento
    en la tabla penguins_raw.
    """
    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No existe el archivo {DATASET_PATH}. "
            "Debe existir ./data/penguins.csv en Rocky."
        )

    df = pd.read_csv(DATASET_PATH)

    if df.empty:
        raise ValueError("penguins.csv está vacío.")

    engine = get_engine()

    df.to_sql(
        "penguins_raw",
        engine,
        if_exists="replace",
        index=False,
    )

    print(f"OK: {len(df)} filas cargadas en penguins_raw.")
    print(f"Columnas RAW: {list(df.columns)}")


def preprocess_penguins():
    """
    Lee los datos RAW desde MySQL y genera
    penguins_processed para entrenamiento.
    """
    engine = get_engine()

    raw = pd.read_sql(
        "SELECT * FROM penguins_raw",
        engine,
    )

    required_columns = ["species"] + FEATURES

    missing_columns = [
        column
        for column in required_columns
        if column not in raw.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Faltan columnas requeridas: {missing_columns}"
        )

    processed = raw[required_columns].copy()

    # Preprocesamiento
    processed = processed.dropna(
        subset=required_columns
    )

    processed = processed.drop_duplicates()

    species_names = sorted(
        processed["species"].unique().tolist()
    )

    species_to_id = {
        species: index
        for index, species in enumerate(species_names)
    }

    processed["target"] = (
        processed["species"]
        .map(species_to_id)
        .astype(int)
    )

    processed = processed[
        ["species"] + FEATURES + ["target"]
    ]

    processed.to_sql(
        "penguins_processed",
        engine,
        if_exists="replace",
        index=False,
    )

    print(
        f"OK: {len(processed)} filas guardadas "
        "en penguins_processed."
    )
    print(f"Mapeo de clases: {species_to_id}")


def train_model():
    """
    Entrena RandomForest utilizando únicamente
    la tabla penguins_processed de MySQL.
    """
    engine = get_engine()

    df = pd.read_sql(
        "SELECT * FROM penguins_processed",
        engine,
    )

    if df.empty:
        raise ValueError(
            "penguins_processed no contiene registros."
        )

    X = df[FEATURES]
    y = df["target"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.25,
        random_state=42,
        stratify=y,
    )

    model = RandomForestClassifier(
        n_estimators=200,
        random_state=42,
        n_jobs=-1,
    )

    model.fit(
        X_train,
        y_train,
    )

    y_pred = model.predict(X_test)

    accuracy = float(
        accuracy_score(
            y_test,
            y_pred,
        )
    )

    # Recuperar mapeo target -> species
    mapping = (
        df[["species", "target"]]
        .drop_duplicates()
        .sort_values("target")
    )

    id_to_species = {
        int(row.target): row.species
        for row in mapping.itertuples(index=False)
    }

    artifact = {
        "model": model,
        "features": FEATURES,
        "id_to_species": id_to_species,
        "accuracy": accuracy,
        "trained_at_utc": datetime.utcnow().isoformat(),
    }

    MODEL_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    joblib.dump(
        artifact,
        MODEL_PATH,
    )

    metrics = pd.DataFrame(
        [
            {
                "model_name": "RandomForestClassifier",
                "accuracy": accuracy,
                "train_rows": int(len(X_train)),
                "test_rows": int(len(X_test)),
                "trained_at_utc": datetime.utcnow(),
                "model_path": str(MODEL_PATH),
            }
        ]
    )

    metrics.to_sql(
        "model_metrics",
        engine,
        if_exists="replace",
        index=False,
    )

    print(f"OK: modelo guardado en {MODEL_PATH}")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Clases: {id_to_species}")


with DAG(
    dag_id="01_penguins_training_pipeline",
    description=(
        "Limpia tablas, carga RAW, preprocesa, "
        "entrena y dispara el DAG de inferencia."
    ),
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "penguins", "training"],
) as dag:

    reset_tables = PythonOperator(
        task_id="01_reset_exercise_tables",
        python_callable=reset_exercise_tables,
    )

    load_raw = PythonOperator(
        task_id="02_load_raw_penguins",
        python_callable=load_raw_penguins,
    )

    preprocess = PythonOperator(
        task_id="03_preprocess_penguins",
        python_callable=preprocess_penguins,
    )

    train = PythonOperator(
        task_id="04_train_model",
        python_callable=train_model,
    )

    trigger_batch = TriggerDagRunOperator(
        task_id="05_trigger_batch_inference_dag",
        trigger_dag_id="02_penguins_batch_inference",
        wait_for_completion=False,
        reset_dag_run=False,
    )

    (
        reset_tables
        >> load_raw
        >> preprocess
        >> train
        >> trigger_batch
    )

