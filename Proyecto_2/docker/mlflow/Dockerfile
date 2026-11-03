FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN pip install --no-cache-dir \
    "mlflow==3.5.1" \
    "SQLAlchemy==2.0.55" \
    "psycopg2-binary" \
    "boto3" \
    && python -c \
    "from sqlalchemy.pool import FallbackAsyncAdaptedQueuePool; print('SQLAlchemy compatible')"

WORKDIR /app

EXPOSE 5000

CMD ["mlflow", "--version"]

