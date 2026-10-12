import os
import logging
from contextlib import asynccontextmanager

import mlflow
import mlflow.sklearn
import pandas as pd

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, ConfigDict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
mlflow.set_registry_uri(os.environ["MLFLOW_TRACKING_URI"])

MODEL_NAME = "PenguinsClassifier"

FEATURES = [
    "bill_length_mm",
    "bill_depth_mm",
    "flipper_length_mm",
    "body_mass_g",
]

state = None


def load_champion():
    global state

    version = mlflow.MlflowClient().get_model_version_by_alias(
        MODEL_NAME,
        "champion",
    )

    model_uri = f"models:/{MODEL_NAME}/{version.version}"
    model = mlflow.sklearn.load_model(model_uri)

    state = {
        "model": model,
        "version": version.version,
        "run_id": version.run_id,
        "model_uri": model_uri,
    }


@asynccontextmanager
async def lifespan(app):
    try:
        load_champion()
    except Exception:
        logger.exception("No se pudo cargar champion.")
    yield


app = FastAPI(
    title="API Penguins mediante MLflow",
    lifespan=lifespan,
)


class Penguin(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bill_length_mm: float | None = Field(
        ..., gt=0, allow_inf_nan=False
    )
    bill_depth_mm: float | None = Field(
        ..., gt=0, allow_inf_nan=False
    )
    flipper_length_mm: float | None = Field(
        ..., gt=0, allow_inf_nan=False
    )
    body_mass_g: float | None = Field(
        ..., gt=0, allow_inf_nan=False
    )


class PredictRequest(BaseModel):
    instances: list[Penguin] = Field(
        ..., min_length=1, max_length=1000
    )


@app.get("/health")
def health():
    return {
        "status": "up",
        "model_loaded": state is not None,
    }


@app.get("/ready")
def ready():
    current = state

    if current is None:
        raise HTTPException(503, "Modelo no cargado.")

    return {
        "model_name": MODEL_NAME,
        "version": current["version"],
        "run_id": current["run_id"],
        "model_uri": current["model_uri"],
    }


@app.post("/reload")
def reload_model():
    try:
        load_champion()
        return ready()
    except Exception:
        logger.exception("Falló la carga desde MLflow.")
        raise HTTPException(
            503,
            "No se pudo cargar champion desde MLflow.",
        )


@app.post("/predict")
def predict(request: PredictRequest):
    current = state

    if current is None:
        raise HTTPException(503, "Modelo no cargado.")

    frame = pd.DataFrame(
        [item.model_dump() for item in request.instances],
        columns=FEATURES,
    ).astype("float64")

    predictions = current["model"].predict(frame)

    return {
        "predictions": predictions.tolist(),
        "model_name": MODEL_NAME,
        "version": current["version"],
        "run_id": current["run_id"],
    }
