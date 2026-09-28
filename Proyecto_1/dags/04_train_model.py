import io
import json
import logging
import os
from datetime import datetime as py_datetime

import joblib
import pandas as pd
import psycopg2

from minio import Minio

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    ExtraTreesClassifier,
    RandomForestClassifier,
)
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    OneHotEncoder,
    StandardScaler,
)

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from pendulum import datetime


logger = logging.getLogger(__name__)

RANDOM_STATE = 42

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "minio:9000",
)

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "ml-models",
)


NUMERIC_FEATURES = [
    "elevation",
    "aspect",
    "slope",
    "horizontal_distance_to_hydrology",
    "vertical_distance_to_hydrology",
    "horizontal_distance_to_roadways",
    "hillshade_9am",
    "hillshade_noon",
    "hillshade_3pm",
    "horizontal_distance_to_fire_points",
]

CATEGORICAL_FEATURES = [
    "wilderness_area",
    "soil_type",
]

FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

TARGET = "cover_type"


def get_postgres_connection():
    return psycopg2.connect(
        host=os.environ["MLOPS_POSTGRES_HOST"],
        port=os.environ.get(
            "MLOPS_POSTGRES_PORT",
            "5432",
        ),
        dbname=os.environ["MLOPS_POSTGRES_DB"],
        user=os.environ["MLOPS_POSTGRES_USER"],
        password=os.environ["MLOPS_POSTGRES_PASSWORD"],
    )


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def upload_bytes(
    client,
    bucket,
    object_name,
    data,
    content_type,
):
    buffer = io.BytesIO(data)

    client.put_object(
        bucket,
        object_name,
        buffer,
        length=len(data),
        content_type=content_type,
    )


@dag(
    dag_id="04_train_model",
    description=(
        "Entrena y evalúa modelos para Cover Type "
        "y almacena artefactos en MinIO"
    ),
    schedule=None,
    start_date=datetime(
        2026,
        9,
        1,
        tz="America/Bogota",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mlops",
        "retries": 0,
    },
    tags=[
        "mlops",
        "training",
        "models",
        "minio",
    ],
)
def train_model_dag():

    @task
    def validate_training_data():
        connection = None
        cursor = None

        try:
            connection = get_postgres_connection()
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM training.dataset;
                """
            )

            total = cursor.fetchone()[0]

            if total == 0:
                raise AirflowFailException(
                    "training.dataset está vacío"
                )

            cursor.execute(
                """
                SELECT
                    cover_type,
                    COUNT(*)
                FROM training.dataset
                GROUP BY cover_type
                ORDER BY cover_type;
                """
            )

            distribution = cursor.fetchall()

            observed_classes = [
                row[0]
                for row in distribution
            ]

            logger.info(
                "Registros de entrenamiento: %s",
                total,
            )

            logger.info(
                "Distribución de clases: %s",
                distribution,
            )

            logger.info(
                "Clases observadas: %s",
                observed_classes,
            )

            invalid_classes = [
                value
                for value in observed_classes
                if value < 0 or value > 6
            ]

            if invalid_classes:
                raise AirflowFailException(
                    "Se encontraron clases fuera "
                    f"del rango 0-6: {invalid_classes}"
                )

            expected = set(range(7))

            missing = sorted(
                expected - set(observed_classes)
            )

            if missing:
                logger.warning(
                    "Clases no presentes en esta extracción: %s",
                    missing,
                )

            return {
                "total": total,
                "observed_classes": observed_classes,
                "missing_classes": missing,
                "distribution": distribution,
            }

        finally:
            if cursor:
                cursor.close()

            if connection:
                connection.close()

    @task
    def train_evaluate_upload():
        connection = None

        try:
            connection = get_postgres_connection()

            query = """
                SELECT
                    elevation,
                    aspect,
                    slope,
                    horizontal_distance_to_hydrology,
                    vertical_distance_to_hydrology,
                    horizontal_distance_to_roadways,
                    hillshade_9am,
                    hillshade_noon,
                    hillshade_3pm,
                    horizontal_distance_to_fire_points,
                    wilderness_area,
                    soil_type,
                    cover_type
                FROM training.dataset
                ORDER BY id;
            """

            dataframe = pd.read_sql_query(
                query,
                connection,
            )

        finally:
            if connection:
                connection.close()

        if dataframe.empty:
            raise AirflowFailException(
                "No se pudieron cargar datos de training.dataset"
            )

        logger.info(
            "Dataset cargado: %s filas x %s columnas",
            dataframe.shape[0],
            dataframe.shape[1],
        )

        X = dataframe[FEATURES]
        y = dataframe[TARGET]

        observed_classes = sorted(
            y.unique().tolist()
        )

        logger.info(
            "Clases usadas para entrenamiento: %s",
            observed_classes,
        )

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.20,
            random_state=RANDOM_STATE,
            stratify=y,
        )

        preprocessor = ColumnTransformer(
            transformers=[
                (
                    "numeric",
                    StandardScaler(),
                    NUMERIC_FEATURES,
                ),
                (
                    "categorical",
                    OneHotEncoder(
                        handle_unknown="ignore"
                    ),
                    CATEGORICAL_FEATURES,
                ),
            ]
        )

        models = {
            "logistic_regression": LogisticRegression(
                max_iter=1000,
                class_weight="balanced",
                random_state=RANDOM_STATE,
            ),
            "random_forest": RandomForestClassifier(
                n_estimators=150,
                class_weight="balanced",
                random_state=RANDOM_STATE,
                n_jobs=-1,
            ),
            "extra_trees": ExtraTreesClassifier(
                n_estimators=150,
                class_weight="balanced",
                random_state=RANDOM_STATE,
                n_jobs=-1,
            ),
        }

        metrics = {}
        trained_pipelines = {}

        for model_name, classifier in models.items():

            logger.info(
                "Entrenando modelo: %s",
                model_name,
            )

            pipeline = Pipeline(
                steps=[
                    (
                        "preprocessor",
                        preprocessor,
                    ),
                    (
                        "classifier",
                        classifier,
                    ),
                ]
            )

            pipeline.fit(
                X_train,
                y_train,
            )

            predictions = pipeline.predict(
                X_test
            )

            model_metrics = {
                "accuracy": float(
                    accuracy_score(
                        y_test,
                        predictions,
                    )
                ),
                "precision_macro": float(
                    precision_score(
                        y_test,
                        predictions,
                        average="macro",
                        zero_division=0,
                    )
                ),
                "recall_macro": float(
                    recall_score(
                        y_test,
                        predictions,
                        average="macro",
                        zero_division=0,
                    )
                ),
                "f1_macro": float(
                    f1_score(
                        y_test,
                        predictions,
                        average="macro",
                        zero_division=0,
                    )
                ),
                "confusion_matrix": confusion_matrix(
                    y_test,
                    predictions,
                    labels=observed_classes,
                ).tolist(),
            }

            metrics[model_name] = model_metrics
            trained_pipelines[model_name] = pipeline

            logger.info(
                "%s -> accuracy=%.4f "
                "precision_macro=%.4f "
                "recall_macro=%.4f "
                "f1_macro=%.4f",
                model_name,
                model_metrics["accuracy"],
                model_metrics["precision_macro"],
                model_metrics["recall_macro"],
                model_metrics["f1_macro"],
            )

        best_model_name = max(
            metrics,
            key=lambda name: metrics[name]["f1_macro"],
        )

        best_pipeline = trained_pipelines[
            best_model_name
        ]

        logger.info(
            "Modelo seleccionado: %s",
            best_model_name,
        )

        logger.info(
            "F1 macro seleccionado: %.4f",
            metrics[best_model_name]["f1_macro"],
        )

        minio_client = get_minio_client()

        if not minio_client.bucket_exists(
            MINIO_BUCKET
        ):
            minio_client.make_bucket(
                MINIO_BUCKET
            )

        for model_name, pipeline in trained_pipelines.items():

            model_buffer = io.BytesIO()

            joblib.dump(
                pipeline,
                model_buffer,
            )

            upload_bytes(
                minio_client,
                MINIO_BUCKET,
                f"models/{model_name}.pkl",
                model_buffer.getvalue(),
                "application/octet-stream",
            )

        best_buffer = io.BytesIO()

        joblib.dump(
            best_pipeline,
            best_buffer,
        )

        upload_bytes(
            minio_client,
            MINIO_BUCKET,
            "models/best_model.pkl",
            best_buffer.getvalue(),
            "application/octet-stream",
        )

        metrics_document = {
            "selection_metric": "f1_macro",
            "best_model": best_model_name,
            "models": metrics,
        }

        upload_bytes(
            minio_client,
            MINIO_BUCKET,
            "metadata/metrics.json",
            json.dumps(
                metrics_document,
                indent=2,
            ).encode("utf-8"),
            "application/json",
        )

        metadata = {
            "created_at": (
                py_datetime.utcnow()
                .isoformat()
                + "Z"
            ),
            "dataset": "training.dataset",
            "total_records": int(
                len(dataframe)
            ),
            "train_records": int(
                len(X_train)
            ),
            "test_records": int(
                len(X_test)
            ),
            "target": TARGET,
            "class_range": [
                0,
                1,
                2,
                3,
                4,
                5,
                6,
            ],
            "observed_classes": observed_classes,
            "missing_classes": sorted(
                set(range(7))
                - set(observed_classes)
            ),
            "best_model": best_model_name,
            "selection_metric": "f1_macro",
            "random_state": RANDOM_STATE,
        }

        upload_bytes(
            minio_client,
            MINIO_BUCKET,
            "metadata/model_info.json",
            json.dumps(
                metadata,
                indent=2,
            ).encode("utf-8"),
            "application/json",
        )

        feature_document = {
            "numeric_features": NUMERIC_FEATURES,
            "categorical_features": CATEGORICAL_FEATURES,
            "all_features": FEATURES,
            "target": TARGET,
        }

        upload_bytes(
            minio_client,
            MINIO_BUCKET,
            "metadata/features.json",
            json.dumps(
                feature_document,
                indent=2,
            ).encode("utf-8"),
            "application/json",
        )

        logger.info(
            "Artefactos cargados correctamente a MinIO"
        )

        return {
            "best_model": best_model_name,
            "best_f1_macro": metrics[
                best_model_name
            ]["f1_macro"],
            "observed_classes": observed_classes,
            "total_records": int(
                len(dataframe)
            ),
        }

    @task
    def validate_minio_artifacts():

        client = get_minio_client()

        required_objects = [
            "models/logistic_regression.pkl",
            "models/random_forest.pkl",
            "models/extra_trees.pkl",
            "models/best_model.pkl",
            "metadata/metrics.json",
            "metadata/model_info.json",
            "metadata/features.json",
        ]

        existing_objects = {
            obj.object_name
            for obj in client.list_objects(
                MINIO_BUCKET,
                recursive=True,
            )
        }

        missing_objects = [
            name
            for name in required_objects
            if name not in existing_objects
        ]

        if missing_objects:
            raise AirflowFailException(
                "Faltan artefactos en MinIO: "
                f"{missing_objects}"
            )

        logger.info(
            "Validación MinIO correcta"
        )

        for object_name in sorted(
            existing_objects
        ):
            logger.info(
                "MinIO: %s",
                object_name,
            )

        return {
            "bucket": MINIO_BUCKET,
            "validated_objects": required_objects,
        }

    validation = validate_training_data()

    training = train_evaluate_upload()

    minio_validation = validate_minio_artifacts()

    validation >> training >> minio_validation


train_model_dag()
