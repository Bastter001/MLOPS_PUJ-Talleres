from fastapi import FastAPI
import joblib
import numpy as np
import os


app = FastAPI()


MODEL_PATH="/models/modelo.pkl"


if os.path.exists(MODEL_PATH):
    model=joblib.load(MODEL_PATH)
else:
    model=None



@app.get("/")
def home():

    return {
        "mensaje":"API MLOps funcionando",
        "modelo": model is not None
    }



@app.post("/predict")
def predict(data:list):

    if model is None:
        return {
            "error":"Modelo no encontrado"
        }


    x=np.array(data).reshape(1,-1)

    prediction=model.predict(x)


    return {
        "prediction":int(prediction[0])
    }
