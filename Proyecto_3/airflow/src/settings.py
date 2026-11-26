import os


DATA_API_URL = os.getenv(
    "DATA_API_URL",
    "http://data-api:80"
).rstrip("/")

DATA_API_GROUP = int(
    os.environ["DATA_API_GROUP"]
)

if DATA_API_GROUP != 8:
    raise ValueError(
        "El proyecto esta asignado al grupo 8"
    )

DATA_API_DAY = os.getenv(
    "DATA_API_DAY",
    ""
).strip()

if DATA_API_DAY not in ("", "Tuesday", "Wednesday"):
    raise ValueError(
        "DATA_API_DAY no valido"
    )

DATA_API_CONNECT_TIMEOUT = 10

DATA_API_READ_TIMEOUT = 120


DB_HOST = os.getenv(
    "PROYECTO3_DB_HOST",
    "postgres"
)

DB_PORT = int(
    os.getenv("PROYECTO3_DB_PORT", "5432")
)

DB_NAME = os.getenv(
    "PROYECTO3_DB_NAME",
    "proyecto3_data"
)

DB_USER = os.getenv(
    "PROYECTO3_DB_USER",
    "proyecto3_training"
)

MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    "http://mlflow:5000"
)

MLFLOW_EXPERIMENT_NAME = os.getenv(
    "MLFLOW_EXPERIMENT_NAME",
    "proyecto3-real-estate"
)

REGISTERED_MODEL_NAME = os.getenv(
    "REGISTERED_MODEL_NAME",
    "proyecto3-real-estate-price"
)

MODEL_ALIAS = os.getenv(
    "MODEL_ALIAS",
    "champion"
)

GIT_COMMIT = os.getenv(
    "GIT_COMMIT",
    "unknown"
)


EXPECTED_COLUMNS = (
    "brokered_by",
    "status",
    "price",
    "bed",
    "bath",
    "acre_lot",
    "street",
    "city",
    "state",
    "zip_code",
    "house_size",
    "prev_sold_date"
)

TARGET_COLUMN = "price"

NUMERIC_FEATURES = (
    "bed",
    "bath",
    "acre_lot",
    "house_size"
)

CATEGORICAL_FEATURES = (
    "brokered_by",
    "status",
    "street",
    "city",
    "state",
    "zip_code"
)
