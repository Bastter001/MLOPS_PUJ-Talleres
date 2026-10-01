# Proyecto 1 - Implementación MLOps PUJ

## Pipeline completo de ingestión, procesamiento, entrenamiento e inferencia

## 1. Resumen del proceso implementado

Se implementó un pipeline MLOps completo para el dataset del conjunto de datos **Forest ** almacenado en una API expuesta que permitio la extracción de los datos, a través de 10 lotes. 

La solución del proyectoi integro: 

-   API externa de generación de datos.
-   Airflow para orquestación.
-   1 db PostgreSQL para almacenamiento de datos.
-   1 db PostgreSQL para almacenamiento de metadatos.
-   Procesamiento y preparación del dataset.
-   Entrenamiento y evaluación de modelos Machine Learning.
-   MinIO para almacenamiento de objetos [modelos ML].
-   FastAPI para generar inferencias.

Arquitectura:

    API Datos
        |
        v
    01_ingestion DAG
        |
        v
    raw.api_data
        |
        v
    02_processing DAG
        |
        v
    processed.forest_data
        |
        v
    03_training_dataset DAG
        |
        v
    training.dataset
        |
        v
    04_train_model DAG
        |
        v
    MinIO
        |
        v
    Inference API FastAPI

------------------------------------------------------------------------

# 2. Resultados obtenidos

## Ingestión

DAG ejecutado:

    01_ingestion

Resultado:

-   Extracción completa de los 10 batches.
-   Grupo: 8.
-   5.810 registros por batch.
-   Total aproximado: 58.100 registros.

Tabla:

    raw.api_data

------------------------------------------------------------------------

## Procesamiento

DAG:

    02_processing

Estado:

    SUCCESS

Tabla:

    processed.forest_data

Distribución:

  cover_type     registros
  ------------ -----------
  0                  30583
  1                  22520
  4                    191
  6                   4806

------------------------------------------------------------------------

## Dataset de entrenamiento

DAG:

    03_training_dataset

Estado:

    SUCCESS

Tabla final:

    training.dataset

------------------------------------------------------------------------

## Entrenamiento de modelos

DAG:

    04_train_model

Modelos evaluados:

-   Logistic Regression.
-   Random Forest.
-   Extra Trees.
-   SVM

Procesamiento:

-   StandardScaler para variables numéricas.
-   OneHotEncoder para variables categóricas.
-   Pipeline Scikit-Learn.

Métrica principal:

    F1 Macro

seleccionada por el desbalance de clases.

------------------------------------------------------------------------

# 3. Artefactos almacenados en MinIO

Bucket:

    ml-models

Estructura:

    models/
     ├── logistic_regression.pkl
     ├── random_forest.pkl
     ├── extra_trees.pkl
     └── best_model.pkl

    metadata/
     ├── metrics.json
     ├── model_info.json
     └── features.json

------------------------------------------------------------------------

# 4. Inference API

Servicio implementado con FastAPI.

Puerto:

    8081

Endpoints:

## Health

    GET /health

Valida:

-   Estado del servicio.
-   Conexión con MinIO.
-   Carga del modelo.

## Predicción

    POST /predict

Flujo:

    JSON Request
          |
          v
    FastAPI
          |
          v
    Modelo desde MinIO
          |
          v
    Predicción cover_type

El modelo utilizado:

    models/best_model.pkl

------------------------------------------------------------------------

# 5. Inconvenientes y ajustes realizados

## ON CONFLICT PostgreSQL

Problema:

    there is no unique or exclusion constraint matching the ON CONFLICT specification

Causa:

La tabla no tenía una restricción compatible.

Solución:

Se ajustó la estrategia de inserción del DAG.

------------------------------------------------------------------------

## API sin disponibilidad después de completar batches

Problema:

    Ya se recolectó toda la información minima necesaria

Solución:

Se utilizó:

    /restart_data_generation

para reiniciar la generación.

------------------------------------------------------------------------

## Problemas de instalación con Docker

Problema inicial:

    apt-get 404 Not Found

Solución:

Se migró la instalación de dependencias a:

    UV package manager

------------------------------------------------------------------------

## Error de permisos UV

Problema:

    Permission denied

Causa:

Instalación ejecutada como usuario airflow.

Solución:

Instalación durante construcción como root:

    USER root
    RUN uv pip install
    USER airflow

------------------------------------------------------------------------

## Compatibilidad de modelos

Se mantuvieron versiones compatibles:

    Python 3.7
    scikit-learn 1.0.2
    joblib 1.2.0

para evitar errores al cargar modelos serializados.

------------------------------------------------------------------------

# 6. Hallazgos

## Dataset desbalanceado

Clases observadas:

    0, 1, 4, 6

Clases ausentes:

    2, 3, 5

No se realizó balanceo artificial; se conservaron los datos reales.

------------------------------------------------------------------------

## Métrica seleccionada

Accuracy no fue suficiente debido al desbalance.

Se utilizó:

    F1 Macro

para considerar todas las clases presentes.

------------------------------------------------------------------------

# 7. Conclusiones

El proyecto implementó un flujo MLOps completo:

✅ Ingestión automatizada.\
✅ Procesamiento reproducible.\
✅ Dataset preparado para entrenamiento.\
✅ Entrenamiento automático de modelos.\
✅ Evaluación y selección del mejor modelo.\
✅ Gestión de artefactos con MinIO.\
✅ Inferencia mediante FastAPI.

Principales aprendizajes:

-   Importancia del control de versiones de dependencias.
-   Separación entre entrenamiento e inferencia.
-   Almacenamiento externo de modelos.
-   Uso de métricas adecuadas para datos desbalanceados.
-   Automatización mediante Airflow.

------------------------------------------------------------------------