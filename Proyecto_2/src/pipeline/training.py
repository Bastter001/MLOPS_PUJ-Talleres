"""Entrenamiento y seguimiento en MLflow."""
import numpy as np
import pandas as pd

from sqlalchemy import text

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    f1_score,
    recall_score,
    precision_score,
    roc_auc_score,
    confusion_matrix
)
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from .common import engine
from .preprocessing import NUMERIC


MODEL_NAME = "DiabetesReadmission"


def load_dataset():

    with engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT raw_id, patient_id, target, features
            FROM diabetes.clean_data
            WHERE record_status = 'ready'
              AND target IS NOT NULL
            ORDER BY raw_id
        """)).all()

    if len(rows) < 100:
        raise ValueError(
            f"Se requieren al menos 100 registros. "
            f"Disponibles: {len(rows)}"
        )

    X = pd.DataFrame([
        row.features for row in rows
    ])

    y = pd.Series(
        [int(row.target) for row in rows],
        name="readmitted"
    )

    groups = pd.Series([
        row.patient_id or f"unknown-{row.raw_id}"
        for row in rows
    ])

    X = X.drop(
        columns=[
            "readmitted",
            "encounter_id",
            "patient_nbr"
        ],
        errors="ignore"
    )

    for col in X.columns:
        if col in NUMERIC:
            X[col] = pd.to_numeric(
                X[col], errors="coerce"
            )
        else:
            X[col] = X[col].astype("object")
            X[col] = X[col].where(
                X[col].notna(), np.nan
            )

    return X, y, groups


def create_splits(X, y, groups):

    outer = GroupShuffleSplit(
        n_splits=1,
        test_size=0.30,
        random_state=42
    )

    train_idx, remaining_idx = next(
        outer.split(X, y, groups)
    )

    inner = GroupShuffleSplit(
        n_splits=1,
        test_size=0.50,
        random_state=42
    )

    val_rel, test_rel = next(
        inner.split(
            X.iloc[remaining_idx],
            y.iloc[remaining_idx],
            groups.iloc[remaining_idx]
        )
    )

    val_idx = remaining_idx[val_rel]
    test_idx = remaining_idx[test_rel]

    for indices in (train_idx, val_idx, test_idx):
        if y.iloc[indices].nunique() < 2:
            raise ValueError(
                "Una particion contiene una sola clase"
            )

    return train_idx, val_idx, test_idx


def split_dataset(clean):

    X, y, groups = load_dataset()

    train_idx, val_idx, test_idx = create_splits(
        X, y, groups
    )

    print("Entrenamiento:", len(train_idx))
    print("Validacion:", len(val_idx))
    print("Prueba:", len(test_idx))

    return {
        "batch_id": clean["batch_id"],
        "total_rows": len(X),
        "train_rows": len(train_idx),
        "validation_rows": len(val_idx),
        "test_rows": len(test_idx)
    }


def train_models(split_info):

    import mlflow
    import mlflow.sklearn
    from mlflow.models import infer_signature

    X, y, groups = load_dataset()

    train_idx, val_idx, test_idx = create_splits(
        X, y, groups
    )

    X_train = X.iloc[train_idx]
    X_val = X.iloc[val_idx]
    X_test = X.iloc[test_idx]

    y_train = y.iloc[train_idx]
    y_val = y.iloc[val_idx]
    y_test = y.iloc[test_idx]

    numeric = X.select_dtypes(
        include="number"
    ).columns.tolist()

    categorical = [
        col for col in X.columns
        if col not in numeric
    ]

    preprocessor = ColumnTransformer([
        (
            "numeric",
            SimpleImputer(
                strategy="median",
                keep_empty_features=True
            ),
            numeric
        ),
        (
            "categorical",
            Pipeline([
                (
                    "imputer",
                    SimpleImputer(
                        strategy="most_frequent",
                        keep_empty_features=True
                    )
                ),
                (
                    "onehot",
                    OneHotEncoder(
                        handle_unknown="ignore",
                        max_categories=40,
                        sparse_output=True
                    )
                )
            ]),
            categorical
        )
    ])

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        (
            "classifier",
            LogisticRegression(
                max_iter=300,
                solver="liblinear",
                class_weight="balanced",
                random_state=42
            )
        )
    ])

    # Fit solo sobre train.
    pipeline.fit(X_train, y_train)

    val_proba = pipeline.predict_proba(X_val)[:, 1]
    test_proba = pipeline.predict_proba(X_test)[:, 1]

    val_pred = (val_proba >= 0.5).astype(int)
    test_pred = (test_proba >= 0.5).astype(int)

    val_f1 = float(
        f1_score(y_val, val_pred, zero_division=0)
    )

    metrics = {
        "val_f1": val_f1,
        "val_recall": float(
            recall_score(y_val, val_pred, zero_division=0)
        ),
        "val_precision": float(
            precision_score(y_val, val_pred, zero_division=0)
        ),
        "val_roc_auc": float(
            roc_auc_score(y_val, val_proba)
        ),
        "test_f1": float(
            f1_score(y_test, test_pred, zero_division=0)
        ),
        "test_recall": float(
            recall_score(y_test, test_pred, zero_division=0)
        ),
        "test_roc_auc": float(
            roc_auc_score(y_test, test_proba)
        )
    }

    mlflow.set_experiment("diabetes-incremental")

    with mlflow.start_run(
        run_name=f"batch-{split_info['batch_id']}"
    ) as run:

        mlflow.log_params({
            "batch_id": split_info["batch_id"],
            "training_rows": len(X_train),
            "validation_rows": len(X_val),
            "test_rows": len(X_test),
            "model": "LogisticRegression",
            "split_strategy": "patient_group",
            "random_state": 42,
            "target": "readmitted_<30",
            "selection_metric": "val_f1"
        })

        mlflow.log_metrics(metrics)

        mlflow.log_dict({
            "features": list(X.columns),
            "numeric_features": numeric,
            "categorical_features": categorical,
            "imputation": "median/mode",
            "val_confusion_matrix":
                confusion_matrix(y_val, val_pred).tolist()
        }, "reports/preprocessing.json")

        example = X_train.head(3)

        signature = infer_signature(
            example,
            pipeline.predict(example)
        )

        model_info = mlflow.sklearn.log_model(
            sk_model=pipeline,
            name="model",
            signature=signature,
            input_example=example
        )

        return {
            "run_id": run.info.run_id,
            "model_uri": model_info.model_uri,
            "val_f1": val_f1,
            "batch_id": split_info["batch_id"]
        }
