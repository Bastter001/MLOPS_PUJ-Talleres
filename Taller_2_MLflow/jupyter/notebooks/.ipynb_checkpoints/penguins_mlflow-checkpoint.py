import os

import pandas as pd
import numpy as np

import mlflow
import mlflow.sklearn

from mlflow import MlflowClient
from mlflow.models import infer_signature

from sqlalchemy import create_engine

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

from sklearn.compose import ColumnTransformer

from sklearn.preprocessing import (
    OneHotEncoder,
    StandardScaler
)

from sklearn.impute import SimpleImputer

from sklearn.ensemble import RandomForestClassifier

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report
)

MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    "http://mlflow:5000"
)

DATA_DB_URL = os.getenv(
    "DATA_DB_URL",
    "postgresql://mluser:mluser123@postgres_data:5432/penguins_db"
)

mlflow.set_tracking_uri(
    MLFLOW_TRACKING_URI
)

engine = create_engine(
    DATA_DB_URL
)

print("MLflow:", MLFLOW_TRACKING_URI)
print("DB:", DATA_DB_URL)

DATA_PATH = "/workspace/data/penguins.csv"

df_raw = pd.read_csv(
    DATA_PATH
)

df_raw.head()
