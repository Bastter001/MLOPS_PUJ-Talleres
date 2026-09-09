from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np

# --- Inicializar la aplicación ---
app = FastAPI()

# --- Cargar el modelo en memoria al iniciar ---
with open("model.pkl", "rb") as f:
    model = pickle.load(f)

# --- Definir el modelo de entrada con los tipos de datos---
class PenguinFeatures(BaseModel):
    bill_length_mm: float
    bill_depth_mm: float
    flipper_length_mm: float
    body_mass_g: float
    island_Biscoe: int = 0
    island_Dream: int = 0
    island_Torgersen: int = 0
    sex_female: int = 0
    sex_male: int = 0	

