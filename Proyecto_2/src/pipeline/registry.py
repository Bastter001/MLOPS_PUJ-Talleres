"""Registro y promocion automatica de modelos."""
import mlflow
from mlflow.tracking import MlflowClient

from .training import MODEL_NAME


def register_experiment(result):

    version = mlflow.register_model(
        model_uri=result["model_uri"],
        name=MODEL_NAME,
        await_registration_for=90
    )

    print(
        "Version registrada:",
        version.version
    )

    return {
        **result,
        "version": str(version.version)
    }


def compare_with_champion(result):

    client = MlflowClient()

    try:
        current = client.get_model_version_by_alias(
            MODEL_NAME,
            "champion"
        )

    except mlflow.exceptions.MlflowException as exc:

        message = str(exc).lower()

        if (
            "not found" not in message
            and "not exist" not in message
        ):
            raise

        return {
            **result,
            "promote": True,
            "champion_val_f1": None
        }

    previous_run = client.get_run(
        current.run_id
    )

    previous_f1 = previous_run.data.metrics.get(
        "val_f1"
    )

    if previous_f1 is None:
        raise RuntimeError(
            "El modelo champion no tiene val_f1"
        )

    return {
        **result,
        "promote": result["val_f1"] > previous_f1,
        "champion_val_f1": float(previous_f1)
    }


def promote_champion(comparison):

    promoted = comparison["promote"]

    if promoted:
        client = MlflowClient()

        client.set_registered_model_alias(
            MODEL_NAME,
            "champion",
            comparison["version"]
        )

        print(
            "Champion actualizado:",
            comparison["version"]
        )

    else:
        print(
            "Se conserva el champion anterior"
        )

    return {
        "version": comparison["version"],
        "promoted": promoted
    }
