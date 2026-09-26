from fastapi import FastAPI, Body
import joblib
import numpy as np


app = FastAPI(
    title="API Modelos ML"
)


models={}


@app.on_event("startup")
def load_models():

    models["lr"] = joblib.load(
        "/models/logistic_regression.pkl"
    )

    models["rf"] = joblib.load(
        "/models/random_forest.pkl"
    )

    models["svm"] = joblib.load(
        "/models/svm.pkl"
    )

    models["gb"] = joblib.load(
        "/models/gradient_boosting.pkl"

    )

@app.get("/")
def home():

    return {

        "estado":"API activa",

        "modelos":
        list(models.keys())

    }


@app.post("/predict/{model_name}")
def predict(

    model_name:str,

    data:list = Body(...)

):

    if model_name not in models:

        return {
            "error":"Modelo no encontrado"
        }


    model=models[model_name]


    prediction=model.predict(

        np.array(data).reshape(1,-1)

    )


    return {

        "modelo":model_name,

        "prediccion":
        prediction.tolist()

    }
