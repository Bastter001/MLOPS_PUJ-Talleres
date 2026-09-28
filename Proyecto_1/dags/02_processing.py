import logging
import os

import psycopg2

from psycopg2.extras import execute_values

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from pendulum import datetime


logger = logging.getLogger(__name__)


GROUP_NUMBER = int(
    os.getenv("DATA_API_GROUP", "8")
)


def get_postgres_connection():

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
    dag_id="02_processing",
    description=(
        "Transforma los payload JSON de raw.api_data "
        "y los almacena en processed.forest_data"
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
        "processing",
        "processed",
    ],
)
def processing_dag():

    @task
    def validate_raw_collection():
        """
        Verifica que los 10 batches estén disponibles
        antes de comenzar la transformación.
        """

        connection = None
        cursor = None

        try:

            connection = get_postgres_connection()
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT
                    COUNT(DISTINCT batch_number),
                    COUNT(*),
                    COALESCE(
                        SUM(
                            jsonb_array_length(
                                payload->'data'
                            )
                        ),
                        0
                    )
                FROM raw.api_data
                WHERE group_number = %s;
                """,
                (GROUP_NUMBER,),
            )

            result = cursor.fetchone()

            batch_count = result[0]
            raw_count = result[1]
            record_count = result[2]

            logger.info(
                "Batches encontrados: %s",
                batch_count,
            )

            logger.info(
                "Payloads RAW encontrados: %s",
                raw_count,
            )

            logger.info(
                "Registros contenidos: %s",
                record_count,
            )

            if batch_count != 10:

                raise AirflowFailException(
                    "No se puede iniciar procesamiento. "
                    f"Solo existen {batch_count}/10 batches."
                )

            return {
                "batch_count": batch_count,
                "raw_count": raw_count,
                "record_count": record_count,
            }

        finally:

            if cursor:
                cursor.close()

            if connection:
                connection.close()

    @task
    def process_raw_data():
        """
        Convierte los registros RAW al esquema tipado
        processed.forest_data.
        """

        connection = None
        cursor = None

        try:

            connection = get_postgres_connection()
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT
                    id,
                    batch_number,
                    group_number,
                    payload
                FROM raw.api_data
                WHERE group_number = %s
                ORDER BY batch_number;
                """,
                (GROUP_NUMBER,),
            )

            raw_rows = cursor.fetchall()

            if not raw_rows:
                raise AirflowFailException(
                    "No existen datos en raw.api_data"
                )

            total_received = 0
            total_valid = 0
            total_invalid = 0

            insert_sql = """
                INSERT INTO processed.forest_data
                (
                    source_raw_id,
                    source_row_number,
                    batch_number,
                    group_number,

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
                VALUES %s

                ON CONFLICT
                (
                    source_raw_id,
                    source_row_number
                )
                DO NOTHING;
            """

            for (
                raw_id,
                batch_number,
                group_number,
                payload,
            ) in raw_rows:

                data = payload.get("data", [])

                logger.info(
                    "Procesando raw_id=%s batch=%s "
                    "con %s registros",
                    raw_id,
                    batch_number,
                    len(data),
                )

                rows_to_insert = []

                for row_number, row in enumerate(data):

                    total_received += 1

                    #
                    # Deben existir exactamente
                    # 13 valores.
                    #
                    if not isinstance(row, list):
                        total_invalid += 1
                        continue

                    if len(row) != 13:
                        total_invalid += 1

                        logger.warning(
                            "Fila inválida raw_id=%s "
                            "row=%s: longitud=%s",
                            raw_id,
                            row_number,
                            len(row),
                        )

                        continue

                    try:

                        processed_row = (
                            raw_id,
                            row_number,
                            batch_number,
                            group_number,

                            float(row[0]),
                            float(row[1]),
                            float(row[2]),

                            float(row[3]),
                            float(row[4]),
                            float(row[5]),

                            float(row[6]),
                            float(row[7]),
                            float(row[8]),

                            float(row[9]),

                            str(row[10]),
                            str(row[11]),

                            int(row[12]),
                        )

                    except (
                        ValueError,
                        TypeError,
                    ):

                        total_invalid += 1

                        logger.exception(
                            "Error convirtiendo raw_id=%s "
                            "fila=%s",
                            raw_id,
                            row_number,
                        )

                        continue

                    rows_to_insert.append(
                        processed_row
                    )

                    total_valid += 1

                if rows_to_insert:

                    execute_values(
                        cursor,
                        insert_sql,
                        rows_to_insert,
                        page_size=1000,
                    )

                    #
                    # Commit por batch.
                    #
                    connection.commit()

                    logger.info(
                        "Batch %s procesado: %s filas válidas",
                        batch_number,
                        len(rows_to_insert),
                    )

            logger.info(
                "Procesamiento terminado."
            )

            logger.info(
                "Total recibidos: %s",
                total_received,
            )

            logger.info(
                "Total válidos: %s",
                total_valid,
            )

            logger.info(
                "Total inválidos: %s",
                total_invalid,
            )

            return {
                "received": total_received,
                "valid": total_valid,
                "invalid": total_invalid,
            }

        except psycopg2.Error as exc:

            if connection:
                connection.rollback()

            logger.exception(
                "Error PostgreSQL durante procesamiento"
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
    def validate_processed_data():
        """
        Realiza controles sobre processed.forest_data.
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

            cursor.execute(
                """
                SELECT
                    COALESCE(
                        SUM(
                            jsonb_array_length(
                                payload->'data'
                            )
                        ),
                        0
                    )
                FROM raw.api_data
                WHERE group_number = %s;
                """,
                (GROUP_NUMBER,),
            )

            expected_count = cursor.fetchone()[0]

            logger.info(
                "RAW esperado: %s registros",
                expected_count,
            )

            logger.info(
                "PROCESSED: %s registros",
                processed_count,
            )

            #
            # Validación de Cover_Type.
            #
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM processed.forest_data
                WHERE group_number = %s
                  AND (
                    cover_type < 1
                    OR cover_type > 7
                    OR cover_type IS NULL
                  );
                """,
                (GROUP_NUMBER,),
            )

            invalid_cover_type = cursor.fetchone()[0]

            if invalid_cover_type > 0:

                raise AirflowFailException(
                    "Se encontraron "
                    f"{invalid_cover_type} valores "
                    "inválidos de cover_type."
                )

            #
            # Validación de nulos principales.
            #
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM processed.forest_data
                WHERE group_number = %s
                  AND (
                    elevation IS NULL
                    OR aspect IS NULL
                    OR slope IS NULL
                    OR wilderness_area IS NULL
                    OR soil_type IS NULL
                    OR cover_type IS NULL
                  );
                """,
                (GROUP_NUMBER,),
            )

            null_count = cursor.fetchone()[0]

            if null_count > 0:

                raise AirflowFailException(
                    f"Se encontraron {null_count} "
                    "filas con valores nulos."
                )

            logger.info(
                "Validación de datos procesados exitosa."
            )

            logger.info(
                "Cover_Type válido: rango 1-7."
            )

            logger.info(
                "Filas con nulos críticos: %s",
                null_count,
            )

            return {
                "processed_count": processed_count,
                "expected_count": expected_count,
                "invalid_cover_type": invalid_cover_type,
                "null_count": null_count,
            }

        finally:

            if cursor:
                cursor.close()

            if connection:
                connection.close()

    validation = validate_raw_collection()

    processing = process_raw_data()

    final_validation = validate_processed_data()

    validation >> processing >> final_validation


processing_dag()
