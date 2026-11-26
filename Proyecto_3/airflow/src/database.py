import os

from contextlib import contextmanager

import psycopg2


@contextmanager
def get_connection():

    connection = psycopg2.connect(
        host=os.getenv(
            "PROYECTO3_DB_HOST",
            "postgres"
        ),
        port=int(
            os.getenv(
                "PROYECTO3_DB_PORT",
                "5432"
            )
        ),
        dbname=os.getenv(
            "PROYECTO3_DB_NAME",
            "proyecto3_data"
        ),
        user=os.getenv(
            "PROYECTO3_DB_USER",
            "proyecto3_training"
        ),
        password=os.environ[
            "PROYECTO3_DB_PASSWORD"
        ],
        connect_timeout=10,
        application_name="proyecto3-airflow"
    )

    try:

        yield connection

        connection.commit()

    except Exception:

        connection.rollback()

        raise

    finally:

        connection.close()
