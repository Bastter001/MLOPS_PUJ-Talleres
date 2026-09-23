# Taller 0 - Introducción a MLOps

## Descripción

Este repositorio contiene el desarrollo del **Taller 0** de la
asignatura de **MLOps** de la Pontificia Universidad Javeriana. Su
propósito es implementar un flujo básico de aprendizaje automático que
incluya entrenamiento de modelos, gestión de datos, despliegue mediante
una API y creación de contenedores con Docker.

## Estructura del proyecto

``` text
Taller_0/
│
├── AI_Model/          # Modelos entrenados y artefactos generados
├── Data_Files/        # Conjuntos de datos empleados
├── api.py             # Servicio API para exponer el modelo
├── train.py           # Script de entrenamiento
├── requirements.txt   # Dependencias del proyecto
├── Dockerfile         # Configuración de contenedor Docker
├── __pycache__/       # Archivos temporales de Python
└── venv/              # Entorno virtual local
```

## Requisitos

-   Python 3.10
-   pip
-   Docker

## Instalación

Clonar el repositorio:

``` bash
git clone https://github.com/Bastter001/MLOPS_PUJ-Talleres.git
cd MLOPS_PUJ-Talleres/Taller_0
```

Instalar dependencias:

``` bash
pip install -r requirements.txt
```

## Entrenamiento del modelo

Ejecutar el script de entrenamiento:

``` bash
python train.py
```

Los artefactos generados se almacenarán en la carpeta correspondiente
del proyecto.

## Ejecución de la API

Iniciar el servicio:

``` bash
python api.py
```

Una vez iniciado, la API permitirá realizar consultas o predicciones
utilizando el modelo entrenado.

## Despliegue con Docker

Construir la imagen:

``` bash
docker build -t taller0-mlops .
```

Ejecutar el contenedor:

``` bash
docker run -p 8000:8000 taller0-mlops
```

## Objetivos del taller

-   Comprender la estructura básica de un proyecto MLOps.
-   Gestionar dependencias de forma reproducible.
-   Automatizar procesos de entrenamiento.
-   Exponer modelos mediante servicios API.
-   Utilizar Docker para despliegue y portabilidad.

## Autor

**Bastter001**

Repositorio académico desarrollado para las actividades.
