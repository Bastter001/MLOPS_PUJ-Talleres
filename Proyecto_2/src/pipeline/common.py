"""Conexion PostgreSQL para los datos del proyecto."""
import os
from sqlalchemy import create_engine


def engine():
    uri = os.environ.get("DIABETES_DATABASE_URL")

    if not uri:
        raise RuntimeError(
            "Falta DIABETES_DATABASE_URL. "
            "Revisar el Secret diabetes-db."
        )

    return create_engine(
        uri,
        pool_pre_ping=True,
        future=True
    )
