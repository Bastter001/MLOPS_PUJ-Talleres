from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pickle
import pandas as pd

# --- Inicializar la aplicación ---
app = FastAPI(title="Penguin Species Predictor")

# --- Cargar el modelo ---
with open("./AI_Model/model.pkl", "rb") as f:
    model = pickle.load(f)

# --- Definir el modelo de entrada ---
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

# --- Endpoint de predicción ---
@app.post("/predict")
def predict(features: PenguinFeatures):
    # Validación manual: valores deben ser positivos
    if (
        features.bill_length_mm <= 0 or
        features.bill_depth_mm <= 0 or
        features.flipper_length_mm <= 0 or
        features.body_mass_g <= 0
    ):
        raise HTTPException(
            status_code=422,
            detail="Las medidas físicas deben ser mayores que cero"
        )

    # Crear un diccionario con los datos recibidos
    input_dict = {
        "bill_length_mm": features.bill_length_mm,
        "bill_depth_mm": features.bill_depth_mm,
        "flipper_length_mm": features.flipper_length_mm,
        "body_mass_g": features.body_mass_g,
        "island_Biscoe": features.island_Biscoe,
        "island_Dream": features.island_Dream,
        "island_Torgersen": features.island_Torgersen,
        "sex_female": features.sex_female,
        "sex_male": features.sex_male
    }

    input_df = pd.DataFrame([input_dict])

    # Alinear columnas con las que el modelo espera
    for col in model.feature_names_in_:
        if col not in input_df.columns:
            input_df[col] = 0
    input_df = input_df[model.feature_names_in_]

    # Realizar la predicción
    prediction = model.predict(input_df)
    species_map = {0: "Adelie", 1: "Chinstrap", 2: "Gentoo"}
    species = species_map.get(int(prediction[0]), "Unknown")

    return JSONResponse(content={"predicted_species": species})

