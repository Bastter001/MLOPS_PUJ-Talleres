import os
import socket
from contextlib import asynccontextmanager

import mlflow
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

modelo = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global modelo

    mlflow.set_tracking_uri(
        os.getenv(
            "MLFLOW_TRACKING_URI",
            "http://mlflow:5000"
        )
    )

    modelo = mlflow.pyfunc.load_model(
        os.getenv(
            "MODEL_URI",
            "models:/modelo_locust@champion"
        )
    )

    print("Modelo cargado desde MLflow")
    yield

app = FastAPI(lifespan=lifespan)

class InputData(BaseModel):
    feature1: float
    feature2: float
    feature3: float

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/predict")
def predict(data: InputData):
    datos = pd.DataFrame([data.model_dump()])
    prediccion = modelo.predict(datos)

    return {
        "resultado": float(prediccion[0]),
        "replica": socket.gethostname()
    }

