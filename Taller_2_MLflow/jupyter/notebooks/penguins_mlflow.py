import os
import hashlib
from itertools import product

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn

from sqlalchemy import create_engine, text
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score
from mlflow.models import infer_signature
from mlflow import MlflowClient

FEATURES = [
    "bill_length_mm",
    "bill_depth_mm",
    "flipper_length_mm",
    "body_mass_g",
]

MODEL_NAME = "PenguinsClassifier"

engine = create_engine(
    os.environ["DATA_DB_URI"],
    pool_pre_ping=True,
)

mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
mlflow.set_registry_uri(os.environ["MLFLOW_TRACKING_URI"])
mlflow.set_experiment("penguins_taller_terminal")

client = MlflowClient()

with engine.connect() as connection:
    print("PostgreSQL:", connection.execute(text("SELECT 1")).scalar())

print("MLflow:", mlflow.get_tracking_uri())

csv_path = os.environ["DATA_CSV"]

with open(csv_path, "rb") as file:
    csv_sha256 = hashlib.sha256(file.read()).hexdigest()


raw_csv = pd.read_csv(csv_path, na_values=["NA"])

assert not raw_csv["rowid"].duplicated().any()
assert not raw_csv["species"].isna().any()

with engine.begin() as connection:
    raw_csv.to_sql(
        "penguins_raw",
        connection,
        if_exists="replace",
        index=False,
    )

print("Filas importadas:", len(raw_csv))
print("SHA-256:", csv_sha256)

with engine.connect() as connection:
    raw = pd.read_sql(
        text("SELECT * FROM penguins_raw ORDER BY rowid"),
        connection,
    )

assert len(raw) == 344

print(raw.shape)
print(raw.isna().sum())

raw.head()

X = raw[FEATURES].astype("float64")
y = raw["species"]

train_idx, remaining_idx = train_test_split(
    np.arange(len(raw)),
    test_size=0.4,
    stratify=y,
    random_state=42,
)

validation_idx, test_idx = train_test_split(
    remaining_idx,
    test_size=0.5,
    stratify=y.iloc[remaining_idx],
    random_state=42,
)

imputer = SimpleImputer(strategy="median")

# Las medianas se calculan únicamente con entrenamiento.
imputer.fit(X.iloc[train_idx])

processed = pd.DataFrame(
    imputer.transform(X),
    columns=FEATURES,
)

processed["rowid"] = raw["rowid"].to_numpy()
processed["species"] = y.to_numpy()
processed["split"] = ""

processed.loc[train_idx, "split"] = "train"
processed.loc[validation_idx, "split"] = "validation"
processed.loc[test_idx, "split"] = "test"

assert not processed[FEATURES].isna().any().any()

with engine.begin() as connection:
    processed.to_sql(
        "penguins_processed",
        connection,
        if_exists="replace",
        index=False,
    )

print(processed["split"].value_counts())

with engine.connect() as connection:
    processed_db = pd.read_sql(
        text("SELECT * FROM penguins_processed ORDER BY rowid"),
        connection,
    )

assert len(processed_db) == 344
assert not processed_db[FEATURES].isna().any().any()

train_data = processed_db.query("split == 'train'")
validation_data = processed_db.query("split == 'validation'")

print("Entrenamiento:", len(train_data))
print("Validación:", len(validation_data))

processed_db.head()

results = []

combinations = list(product(
    [50, 100, 150, 200, 250],
    [3, 5, 10, None],
))

assert len(combinations) == 20

for number, (n_estimators, max_depth) in enumerate(combinations, start=1):

    with mlflow.start_run(run_name=f"random_forest_{number:02d}") as run:

        classifier = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            random_state=42,
            n_jobs=1,
        )

        classifier.fit(
            train_data[FEATURES].to_numpy(),
            train_data["species"],
        )

        predictions = classifier.predict(
            validation_data[FEATURES].to_numpy()
        )

        val_accuracy = accuracy_score(
            validation_data["species"],
            predictions,
        )

        val_f1 = f1_score(
            validation_data["species"],
            predictions,
            average="macro",
        )

        mlflow.log_params({
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "random_state": 42,
            "imputation": "median_train",
        })

        mlflow.log_metrics({
            "val_accuracy": val_accuracy,
            "val_f1_macro": val_f1,
        })

        mlflow.set_tag("csv_sha256", csv_sha256)

        mlflow.log_dict(
            {
                "raw_table": "penguins_raw",
                "processed_table": "penguins_processed",
                "features": FEATURES,
                "csv_sha256": csv_sha256,
                "train_rows": 206,
                "validation_rows": 69,
                "test_rows": 69,
            },
            "data_info.json",
        )

        # Conservamos el imputador ya ajustado y el clasificador.
        pipeline = Pipeline([
            ("imputer", imputer),
            ("classifier", classifier),
        ])

        example = X.dropna().head(3)

        # MLflow 3: registrar el modelo usando el URI real devuelto
        # por log_model, no reconstruir rutas runs:/ manualmente.
        # Compatible tanto con clientes MLflow 2.x como 3.x.
        from inspect import signature as function_signature
        model_path_kw = (
            {"name": "model"}
            if "name" in function_signature(mlflow.sklearn.log_model).parameters
            else {"artifact_path": "model"}
        )
        model_info = mlflow.sklearn.log_model(
            sk_model=pipeline,
            **model_path_kw,
            signature=infer_signature(
                example,
                pipeline.predict(example),
            ),
            input_example=example,
        )

        version = mlflow.register_model(
            model_uri=model_info.model_uri,
            name=MODEL_NAME,
            await_registration_for=120,
        )

        results.append({
            "run_id": run.info.run_id,
            "version": int(version.version),
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "val_accuracy": val_accuracy,
            "val_f1_macro": val_f1,
        })

        print(
            f"{number}/20",
            f"versión={version.version}",
            f"F1 validación={val_f1:.4f}",
        )

results_df = pd.DataFrame(results).sort_values(
    ["val_f1_macro", "val_accuracy", "version"],
    ascending=[False, False, True],
).reset_index(drop=True)

# Persistir también las 20 combinaciones en PostgreSQL de datos.
with engine.begin() as connection:
    results_df.to_sql(
        "penguins_experiments",
        connection,
        if_exists="replace",
        index=False,
    )

print("Resultados almacenados en penguins_experiments:", len(results_df))

winner = results_df.iloc[0]
winner_version = str(int(winner["version"]))

winner_model = mlflow.sklearn.load_model(
    f"models:/{MODEL_NAME}/{winner_version}"
)

test_rowids = processed_db.loc[
    processed_db["split"] == "test",
    "rowid",
]

test_mask = raw["rowid"].isin(test_rowids)

X_test = X.loc[test_mask]
y_test = raw.loc[test_mask, "species"]

test_predictions = winner_model.predict(X_test)

test_metrics = {
    "test_accuracy": accuracy_score(y_test, test_predictions),
    "test_f1_macro": f1_score(
        y_test,
        test_predictions,
        average="macro",
    ),
}

with mlflow.start_run(run_id=winner["run_id"]):
    mlflow.log_metrics(test_metrics)
    mlflow.log_text(
        results_df.to_csv(index=False),
        "comparison.csv",
    )

client.set_registered_model_alias(
    MODEL_NAME,
    "champion",
    winner_version,
)

print("Versión ganadora:", winner_version)
print("Métricas de prueba:", test_metrics)

assert len(results_df) == 20

for run_id in results_df["run_id"]:
    stored_run = client.get_run(run_id)

    assert stored_run.info.status == "FINISHED"
    assert "val_f1_macro" in stored_run.data.metrics

champion = client.get_model_version_by_alias(
    MODEL_NAME,
    "champion",
)

assert champion.version == winner_version

print("20 runs finalizados.")
print("Champion apunta a la versión:", champion.version)