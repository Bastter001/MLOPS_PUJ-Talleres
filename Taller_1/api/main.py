from fastapi import FastAPI

import joblib

import numpy as np


app=FastAPI(
title="API Modelos ML"
)



models={}



@app.on_event("startup")
def load_models():


    models["lr"]=joblib.load(
    "/models/model_lr.pkl"
    )


    models["rf"]=joblib.load(
    "/models/model_rf.pkl"
    )


    models["svm"]=joblib.load(
    "/models/model_svm.pkl"
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

data:list

):


    model=models[model_name]


    prediction=model.predict(

        np.array(data).reshape(1,-1)

    )


    return {

    "modelo":model_name,

    "prediccion":
    prediction.tolist()

    }
