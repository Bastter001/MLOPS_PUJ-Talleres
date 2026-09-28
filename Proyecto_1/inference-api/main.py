import io
import logging
import os

import joblib
import pandas as pd

from fastapi import FastAPI, HTTPException
from minio import Minio
from pydantic import BaseModel


logging.basicConfig(
    level=logging.INFO
)

logger = logging.getLogger(__name__)


MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "minio:9000",
)

MINIO_ACCESS_KEY = os.environ[
    "MINIO_ACCESS_KEY"
]

MINIO_SECRET_KEY = os.environ[
    "MINIO_SECRET_KEY"
]

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "ml-models",
)

MODEL_OBJECT = os.getenv(
    "MODEL_OBJECT",
    "models/best_model.pkl",
)


app = FastAPI(
    title="Forest Cover Inference API",
    description=(
        "API para inferencia de Cover Type "
        "usando el modelo almacenado en MinIO"
    ),
    version="1.0.0",
)


model = None


class PredictionRequest(BaseModel):
    elevation: float
    aspect: float
    slope: float

    horizontal_distance_to_hydrology: float
    vertical_distance_to_hydrology: float
    horizontal_distance_to_roadways: float

    hillshade_9am: float
    hillshade_noon: float
    hillshade_3pm: float

    horizontal_distance_to_fire_points: float

    wilderness_area: str
    soil_type: str


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def load_model_from_minio():
    global model

    logger.info(
        "Conectando a MinIO endpoint=%s bucket=%s object=%s",
        MINIO_ENDPOINT,
        MINIO_BUCKET,
        MODEL_OBJECT,
    )

    client = get_minio_client()

    if not client.bucket_exists(
        MINIO_BUCKET
    ):
        raise RuntimeError(
            f"El bucket {MINIO_BUCKET} no existe"
        )

    response = client.get_object(
        MINIO_BUCKET,
        MODEL_OBJECT,
    )

    try:
        model_bytes = response.read()
    finally:
        response.close()
        response.release_conn()

    if not model_bytes:
        raise RuntimeError(
            "El modelo descargado desde MinIO está vacío"
        )

    model = joblib.load(
        io.BytesIO(model_bytes)
    )

    logger.info(
        "Modelo cargado correctamente: %s",
        type(model),
    )


@app.on_event("startup")
def startup_event():
    try:
        load_model_from_minio()

    except Exception:
        logger.exception(
            "No fue posible cargar "
            "el modelo desde MinIO"
        )

        raise


@app.get("/")
def root():
    return {
        "service": (
            "Forest Cover Inference API"
        ),
        "status": "running",
    }


@app.get("/health")
def health():
    return {
        "status": (
            "healthy"
            if model is not None
            else "unhealthy"
        ),
        "model_loaded": (
            model is not None
        ),
        "model_object": MODEL_OBJECT,
        "bucket": MINIO_BUCKET,
    }


@app.post("/predict")
def predict(
    request: PredictionRequest
):
    if model is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "El modelo no está cargado"
            ),
        )

    try:
        input_data = pd.DataFrame(
            [
                {
                    "elevation":
                        request.elevation,

                    "aspect":
                        request.aspect,

                    "slope":
                        request.slope,

                    "horizontal_distance_to_hydrology":
                        request.horizontal_distance_to_hydrology,

                    "vertical_distance_to_hydrology":
                        request.vertical_distance_to_hydrology,

                    "horizontal_distance_to_roadways":
                        request.horizontal_distance_to_roadways,

                    "hillshade_9am":
                        request.hillshade_9am,

                    "hillshade_noon":
                        request.hillshade_noon,

                    "hillshade_3pm":
                        request.hillshade_3pm,

                    "horizontal_distance_to_fire_points":
                        request.horizontal_distance_to_fire_points,

                    "wilderness_area":
                        request.wilderness_area,

                    "soil_type":
                        request.soil_type,
                }
            ]
        )

        prediction = model.predict(
            input_data
        )

        predicted_class = int(
            prediction[0]
        )

        response = {
            "prediction": predicted_class,
        }

        if hasattr(
            model,
            "predict_proba"
        ):
            try:
                probabilities = (
                    model.predict_proba(
                        input_data
                    )[0]
                )

                classes = (
                    model.classes_
                    if hasattr(
                        model,
                        "classes_"
                    )
                    else None
                )

                if classes is not None:
                    response[
                        "probabilities"
                    ] = {
                        str(
                            int(class_value)
                        ): float(
                            probability
                        )

                        for (
                            class_value,
                            probability,
                        )

                        in zip(
                            classes,
                            probabilities,
                        )
                    }

            except Exception:
                logger.exception(
                    "No fue posible "
                    "calcular probabilidades"
                )

        return response

    except Exception as exc:
        logger.exception(
            "Error realizando predicción"
        )

        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )
