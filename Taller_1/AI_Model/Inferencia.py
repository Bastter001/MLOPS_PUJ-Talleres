from fastapi import FastAPI
import pickle

app = FastAPI()

# Cargar el modelo en memoria iniciar
with open("model.pkl", "rb") as f:
    model = pickle.load(f)	

