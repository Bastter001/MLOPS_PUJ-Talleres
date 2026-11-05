from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

DEFAULT_ARGS = {
    "owner": "mlops",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="diabetes_mlops_pipeline",
    description="Pipeline incremental automatizado de diabetes",
    start_date=pendulum.datetime(
        2026, 10, 28,
        tz="America/Bogota"
    ),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    default_args=DEFAULT_ARGS,
    tags=["diabetes", "mlops", "incremental"]
)
def diabetes_mlops_pipeline():

    @task
    def validate_source():
        from src.pipeline.ingestion import validate_source
        return validate_source()

    @task
    def ingest_batch(source):
        from src.pipeline.ingestion import ingest_next_batch
        return ingest_next_batch(
            source,
            max_rows=15000
        )

    @task
    def validate_quality(batch):
        from src.pipeline.quality import validate_batch
        return validate_batch(batch)

    @task
    def preprocess(batch_quality):
        from src.pipeline.preprocessing import preprocess_batch
        return preprocess_batch(batch_quality)

    @task
    def save_clean(stage):
        from src.pipeline.preprocessing import persist_clean
        return persist_clean(stage)

    @task
    def split_dataset(clean):
        from src.pipeline.training import split_dataset
        return split_dataset(clean)

    @task
    def train_models(splits):
        from src.pipeline.training import train_models
        return train_models(splits)

    @task
    def register_mlflow(training):
        from src.pipeline.registry import register_experiment
        return register_experiment(training)

    @task
    def compare_models(registered):
        from src.pipeline.registry import compare_with_champion
        return compare_with_champion(registered)

    @task
    def promote_champion(comparison):
        from src.pipeline.registry import promote_champion
        return promote_champion(comparison)

    source = validate_source()
    batch = ingest_batch(source)
    quality = validate_quality(batch)

    stage = preprocess(quality)
    clean = save_clean(stage)

    splits = split_dataset(clean)
    training = train_models(splits)

    registered = register_mlflow(training)
    compared = compare_models(registered)

    promote_champion(compared)


diabetes_mlops_pipeline()
