# Imagen de UV fijada para reproducibilidad
FROM ghcr.io/astral-sh/uv:0.4.30 AS uv

# Imagen base de Airflow
FROM apache/airflow:2.6.0

# Durante el build necesitamos permisos para escribir
# en /usr/local/lib/python3.7/site-packages
USER root

# Copiar UV desde la imagen oficial
COPY --from=uv /uv /usr/local/bin/uv

# Copiar el lock generado previamente para Python 3.7
COPY requirements.lock /tmp/requirements.lock

# Instalar las dependencias en el Python del sistema
RUN uv pip install \
    --system \
    --python /usr/local/bin/python \
    --no-cache \
    --only-binary=:all: \
    -r /tmp/requirements.lock

# La ejecución normal de Airflow vuelve a su usuario no privilegiado
USER airflow
