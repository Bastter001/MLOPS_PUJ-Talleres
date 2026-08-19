# train.py
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# --- 1. Load ---
def load_data(path):
    print("Loading data...")
    return pd.read_csv(path)

# --- 2. Clean ---
def clean_data(df):
    print("Cleaning data...")
    df = df.dropna()  # elimina filas con valores nulos
    return df

# --- 3. Transform ---
def transform_data(df):
    print("Transforming data...")
    df['target'] = df['target'].astype('category').cat.codes  # ejemplo de transformación
    return df

# --- 4. Validate ---
def validate_data(df):
    print("Validating data...")
    assert not df.isnull().values.any(), "Data contains null values!"
    return True

# --- 5. Feature Engineering ---
def feature_engineering(df):
    print("Feature engineering...")
    scaler = StandardScaler()
    features = df.drop('target', axis=1)
    df_scaled = pd.DataFrame(scaler.fit_transform(features), columns=features.columns)
    df_scaled['target'] = df['target']
    return df_scaled

# --- 6. Split ---
def split_data(df):
    print("Splitting data...")
    X = df.drop('target', axis=1)
    y = df['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)
    return X_train, X_test, y_train, y_test

# --- Main pipeline ---
if __name__ == "__main__":
    data_path = "./Data_Files/dataset.csv"
    df = load_data(data_path)
    df = clean_data(df)
    df = transform_data(df)
    validate_data(df)
    df = feature_engineering(df)
    X_train, X_test, y_train, y_test = split_data(df)
    print("Pipeline completed successfully!")

