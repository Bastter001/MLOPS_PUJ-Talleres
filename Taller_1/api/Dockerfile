FROM python:3.12-slim


WORKDIR /app


RUN pip install uv


RUN uv pip install --system \
    fastapi \
    uvicorn \
    pandas \
    numpy \
    scikit-learn \
    joblib


COPY api .


EXPOSE 8000


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
