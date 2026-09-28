import logging
import os

import psycopg2

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from pendulum import datetime


logger = logging.getLogger(__name__)


GROUP_NUMBER = int(
    os.getenv(
        "DATA_API_GROUP",
        "8",
    )
)


def get_postgres_connection():
    """
    Conexión a PostgreSQL del proyecto.
    """

    return psycopg2.connect(
        host=os.environ["MLOPS_POSTGRES_HOST"],
        port=os.environ.get(
            "MLOPS_POSTGRES_PORT",
            "5432",
        ),
        dbname=os.environ["MLOPS_POSTGRES_DB"],
        user=os.environ["MLOPS_POSTGRES_USER"],
        password=os.environ["MLOPS_POSTGRES_PASSWORD"],
    )


@dag(
    dag_id="03_training_dataset",
    description=(
        "Construye training.dataset "
        "a partir de processed.forest_data"
    ),
    schedule=None,
    start_date=datetime(
        2026,
        9,
        1,
        tz="America/Bogota",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mlops",
        "retries": 0,
    },
    tags=[
        "mlops",
        "training",
        "dataset",
    ],
)
def training_dataset_dag():

    @task
    def validate_processed_source():
        """
        Verifica que processed.forest_data
        tenga datos suficientes para entrenamiento.
        """

        connection = None
        cursor = None

        try:
            connection = get_postgres_connection()
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM processed.forest_data
                WHERE group_number = %s;
                """,
                (GROUP_NUMBER,),
            )

            processed_count = cursor.fetchone()[0]

            logger.info(
                "Registros procesados disponibles: %s",
                processed_count,
            )

            if processed_count == 0:
                raise AirflowFailException(
                    "processed.forest_data está vacío"
                )

            cursor.execute(
                """
                SELECT COUNT(DISTINCT batch_number)
                FROM processed.forest_data
                WHERE group_number = %s;
                """,
                (GROUP_NUMBER,),
            )

            batch_count = cursor.fetchone()[0]

            logger.info(
                "Batches presentes en processed: %s/10",
                batch_count,
            )

            if batch_count != 10:
                raise AirflowFailException(
                    f"Solo existen {batch_count}/10 batches "
                    "en processed.forest_data"
                )

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM processed.forest_data
                WHERE group_number = %s
                  AND (
                    cover_type IS NULL
                    OR cover_type < 0
                    OR cover_type > 6
                  );
                """,
                (GROUP_NUMBER,),
            )

            invalid_target = cursor.fetchone()[0]

            if invalid_target > 0:
                raise AirflowFailException(
                    f"Hay {invalid_target} registros "
                    "con cover_type inválido"
                )

            return {
                "processed_count": processed_count,
                "batch_count": batch_count,
                "invalid_target": invalid_target,
            }

        except psycopg2.Error as exc:
            logger.exception(
                "Error validando processed.forest_data"
            )

            raise AirflowFailException(
                f"Error PostgreSQL: {exc}"
            ) from exc

        finally:
            if cursor:
                cursor.close()

            if connection:
                connection.close()

    @task
    def prepare_training_table():
        """
        Limpia training.dataset para generar
        un dataset de entrenamiento reproducible.
        """

        connection = None
        cursor = None

        try:
            connection = get_postgres_connection()
            cursor = connection.cursor()

            cursor.execute(
                """
                TRUNCATE TABLE training.dataset
                RESTART IDENTITY;
                """
            )

            connection.commit()

            logger.info(
                "training.dataset limpiada correctamente"
            )

            return True

        except psycopg2.Error as exc:
            if connection:
                connection.rollback()

            logger.exception(
                "Error limpiando training.dataset"
            )

            raise AirflowFailException(
                f"Error PostgreSQL: {exc}"
            ) from exc

        finally:
            if cursor:
                cursor.close()

            if connection:
                connection.close()

    @task
    def build_training_dataset():
        """
        Copia los registros válidos desde
        processed.forest_data hacia training.dataset.
        """

        connection = None
        cursor = None

        try:
            connection = get_postgres_connection()
            cursor = connection.cursor()

            insert_sql = """
                INSERT INTO training.dataset
                (
                    source_processed_id,

                    elevation,
                    aspect,
                    slope,

                    horizontal_distance_to_hydrology,
                    vertical_distance_to_hydrology,
                    horizontal_distance_to_roadways,

                    hillshade_9am,
                    hillshade_noon,
                    hillshade_3pm,

                    horizontal_distance_to_fire_points,

                    wilderness_area,
                    soil_type,

                    cover_type
                )

                SELECT
                    id,

                    elevation,
                    aspect,
                    slope,

                    horizontal_distance_to_hydrology,
                    vertical_distance_to_hydrology,
                    horizontal_distance_to_roadways,

                    hillshade_9am,
                    hillshade_noon,
                    hillshade_3pm,

                    horizontal_distance_to_fire_points,

                    wilderness_area,
                    soil_type,

                    cover_type

                FROM processed.forest_data

                WHERE group_number = %s

                  AND elevation IS NOT NULL
                  AND aspect IS NOT NULL
                  AND slope IS NOT NULL

                  AND horizontal_distance_to_hydrology IS NOT NULL
                  AND vertical_distance_to_hydrology IS NOT NULL
                  AND horizontal_distance_to_roadways IS NOT NULL

                  AND hillshade_9am IS NOT NULL
                  AND hillshade_noon IS NOT NULL
                  AND hillshade_3pm IS NOT NULL

                  AND horizontal_distance_to_fire_points IS NOT NULL

                  AND wilderness_area IS NOT NULL
                  AND soil_type IS NOT NULL

                  AND cover_type BETWEEN 0 AND 6

                ON CONFLICT (source_processed_id)
                DO NOTHING;
            """

            cursor.execute(
                insert_sql,
                (GROUP_NUMBER,),
            )

            inserted_count = cursor.rowcount

            connection.commit()

            logger.info(
                "Registros insertados en training.dataset: %s",
                inserted_count,
            )

            return inserted_count

        except psycopg2.Error as exc:

            if connection:
                connection.rollback()

            logger.exception(
                "Error construyendo training.dataset"
            )

            raise AirflowFailException(
                f"Error PostgreSQL: {exc}"
            ) from exc

        finally:

            if cursor:
                cursor.close()

            if connection:
                connection.close()

    @task
    def validate_training_dataset():
        """
        Valida el dataset final de entrenamiento.
        """

        connection = None
        cursor = None

        try:
            connection = get_postgres_connection()
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM training.dataset;
                """
            )

            training_count = cursor.fetchone()[0]

            logger.info(
                "Registros en training.dataset: %s",
                training_count,
            )

            if training_count == 0:
                raise AirflowFailException(
                    "training.dataset quedó vacío"
                )

            cursor.execute(
                """
                SELECT
                    COUNT(DISTINCT cover_type)
                FROM training.dataset;
                """
            )

            class_count = cursor.fetchone()[0]

            logger.info(
                "Número de clases en training.dataset: %s",
                class_count,
            )

            if class_count != 7:
                raise AirflowFailException(
                    f"Se esperaban 7 clases "
                    f"y se encontraron {class_count}"
                )

            cursor.execute(
                """
                SELECT
                    cover_type,
                    COUNT(*)
                FROM training.dataset
                GROUP BY cover_type
                ORDER BY cover_type;
                """
            )

            distribution = cursor.fetchall()

            logger.info(
                "Distribución de cover_type: %s",
                distribution,
            )

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM training.dataset
                WHERE
                    elevation IS NULL
                    OR aspect IS NULL
                    OR slope IS NULL
                    OR horizontal_distance_to_hydrology IS NULL
                    OR vertical_distance_to_hydrology IS NULL
                    OR horizontal_distance_to_roadways IS NULL
                    OR hillshade_9am IS NULL
                    OR hillshade_noon IS NULL
                    OR hillshade_3pm IS NULL
                    OR horizontal_distance_to_fire_points IS NULL
                    OR wilderness_area IS NULL
                    OR soil_type IS NULL
                    OR cover_type IS NULL;
                """
            )

            null_count = cursor.fetchone()[0]

            if null_count > 0:
                raise AirflowFailException(
                    f"Se encontraron {null_count} "
                    "registros con valores nulos"
                )

            logger.info(
                "training.dataset validado correctamente"
            )

            return {
                "training_count": training_count,
                "class_count": class_count,
                "distribution": distribution,
                "null_count": null_count,
            }

        except psycopg2.Error as exc:
            logger.exception(
                "Error validando training.dataset"
            )

            raise AirflowFailException(
                f"Error PostgreSQL: {exc}"
            ) from exc

        finally:
            if cursor:
                cursor.close()

            if connection:
                connection.close()

    source_validation = validate_processed_source()

    table_preparation = prepare_training_table()

    dataset_build = build_training_dataset()

    final_validation = validate_training_dataset()

    source_validation >> table_preparation
    table_preparation >> dataset_build
    dataset_build >> final_validation


training_dataset_dag()
