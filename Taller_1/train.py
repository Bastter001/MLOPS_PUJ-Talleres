# train.py
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
import joblib

# --- 1. Load ---
def load_data(path):
    print("Loading data...")
    return pd.read_csv(path)

# --- 2. Clean ---
def clean_data(df):
    print("Cleaning data...")
    # Elimina filas con valores nulos en cualquier columna
    df = df.dropna()
    # Elimina filas donde species esté vacío
    df = df[df['species'].notnull()]
    return df

# --- 3. Transform ---
def transform_data(df):
    print("Transforming data...")
    # Convertimos species a códigos numéricos
    df['species'] = df['species'].astype('category').cat.codes
    return df

# --- 4. Validate ---
def validate_data(df):
    print("Validating data...")
    assert not df.isnull().values.any(), "Data contains null values!"
    return True

# --- 5. Feature Engineering ---
def feature_engineering(df):
    print("Feature engineering...")
    # Convertir variables categóricas a numéricas (One-Hot Encoding)
    df_encoded = pd.get_dummies(df.drop('species', axis=1))
    
    # Escalar solo las columnas numéricas
    scaler = StandardScaler()
    df_scaled = pd.DataFrame(scaler.fit_transform(df_encoded), columns=df_encoded.columns)
    
    # Añadir la columna objetivo
    df_scaled['species'] = df['species']
    return df_scaled

# --- 6. Split ---
def split_data(df):
    print("Splitting data...")
    X = df.drop('species', axis=1)
    y = df['species']
    # Asegurar que no haya NaN en y
    mask = y.notnull()
    X, y = X[mask], y[mask]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )
    return X_train, X_test, y_train, y_test

# --- 7. Train Model ---
def train_model(X_train, y_train):
    print("Training model...")
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    return model

# --- 8. Save Model ---
def save_model(model, path="./AI_Model/model.pkl"):
    print(f"Saving model to {path}...")
    joblib.dump(model, path)

# --- Main pipeline ---
if __name__ == "__main__":
    data_path = "./Data_Files/dataset.csv"
    df = load_data(data_path)
    df = clean_data(df)
    df = transform_data(df)
    validate_data(df)
    df = feature_engineering(df)
    X_train, X_test, y_train, y_test = split_data(df)
    model = train_model(X_train, y_train)
    save_model(model)
    print("Pipeline completed successfully!")
    print(f"Train shape: {X_train.shape}, Test shape: {X_test.shape}")




