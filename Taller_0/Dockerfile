# Usar una imagen base ligera con Python
FROM python:3.9-slim

# Definir el directorio de trabajo
WORKDIR /app

# Copiar archivo de dependencias
COPY requirements.txt /app/

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar la API y el modelo
COPY api.py /app/
COPY AI_Model/model.pkl /app/AI_Model/

# Comando por defecto: levantar el servidor con uvicorn
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "80"]

