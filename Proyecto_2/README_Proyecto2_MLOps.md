# Proyecto 2 — MLOps con Rancher y K3s

**Proyecto:** `~/MLOPS_PUJ-Talleres/Proyecto_2`  

## 1. Objetivo y arquitectura

Implementar en **Kubernetes (K3s)** un flujo de datos clínicos de diabetes con ingesta incremental, separación RAW/CLEAN, entrenamiento y registro de modelos en MLflow, inferencia mediante FastAPI y Streamlit, pruebas de carga con Locust y observabilidad con Prometheus/Grafana.

**Arquitectura prevista:**

```text
Rocky Linux / NBAM02
Docker: Rancher Manager (administración)
K3s: clúster Kubernetes (ejecución del taller)
Airflow DAG ──> PostgreSQL diabetesdb
    │diabetes.raw_data
    │diabetes.clean_stage
    │diabetes.clean_data
    │diabetes.ingestion_batches
    │diabetes.inference_logs
MLflow ───────> PostgreSQL (backend) + MinIO (artefactos)
FastAPI ──────> modelo champion en MLflow + logs en PostgreSQL
Streamlit ────> FastAPI
Locust ───────> FastAPI
Prometheus ──> /metrics de FastAPI ──> Grafana
```

Rancher **no sustituye** el clúster de ejecución: la entrega exige desplegar los componentes del taller en K3s, no solo en contenedores Docker independientes.

## 2. Estructura de trabajo identificada

La raíz real mostrada en terminal contiene:

```text
Proyecto_2/
├── dags/
├── data/
├── docker/
├── docs/
├── grafana/
├── k8s/
├── kubernetes/
├── locust/
├── minio.yaml
├── sql/
└── src/
    ├── api/
    ├── pipeline/
    │   ├── __init__.py
    │   ├── common.py
    │   ├── ingestion.py
    │   ├── quality.py
    │   ├── preprocessing.py
    │   ├── training.py
    │   └── registry.py
    └── streamlit/
```

> Esta estructura corresponde a lo visto en la terminal; no debe reemplazarse por estructuras de carpetas alternativas propuestas anteriormente sin necesidad.

## 3. Avances y hallazgos comprobados

| Elemento | Hallazgo | Estado |
|---|---|---|
| Rancher / K3s | Reportados como creados previamente; falta evidencia reciente de estado del clúster y de la importación a Rancher. | Por verificar operativamente |
| PostgreSQL | El pod `postgres-0` respondió consultas a `diabetesdb`. | Conexión confirmada |
| Base `diabetesdb` | La consulta `psql -d diabetesdb` devolvió sus relaciones. | Confirmada |
| Esquema `diabetes` | Cinco tablas identificadas. | Confirmado |
| Tablas | `raw_data`, `clean_stage`, `clean_data`, `ingestion_batches`, `inference_logs`. | Estructura presente; contenido no verificado |
| SQLAlchemy local | Tras instalarlo, se reportó `SQLAlchemy 2.0.57` en Python 3.9 del usuario. | Instalado localmente, no confirmado en Airflow |
| `preprocessing.py` | `python3 -m py_compile` terminó sin error. | Sintaxis confirmada |
| `training.py` | `python3 -m py_compile` terminó sin error. | Sintaxis confirmada |
| `registry.py` | Se abrió/editó, sin verificación mostrada de sintaxis o ejecución. | Pendiente |
| DAG | Se encontró `dags/` vacío en un punto; luego se abrió `vim diabetes_mlops_pipeline.py`. La compilación fallaba por ruta relativa incorrecta. | Existencia/contenido y carga en Airflow pendientes de confirmar |
| MLflow, MinIO, FastAPI, Streamlit, Locust, Prometheus y Grafana | Se definieron pasos y arquitectura, pero no se aportaron salidas que acrediten su funcionamiento integrado. | Pendientes de validación |

## 4. Incidentes y correcciones

### 4.1. SQL ejecutado con comandos Bash incrustados

Al cargar `sql/diabetes_tables.sql`, PostgreSQL devolvió:

```text
ERROR: syntax error at or near "cat"
LINE 1: cat > sql/diabetes_tables.sql <<'EOF'
```

**Causa:** el archivo contenía instrucciones de shell (`cat ... <<'EOF'`) en lugar de solo SQL. **Corrección indicada:** eliminar la cabecera `cat` y el `EOF` final, conservar las sentencias SQL y ejecutar desde la raíz del proyecto:

```bash
kubectl -n mlops exec -i postgres-0 -- sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 \
  -U "$POSTGRES_USER" -d diabetesdb' < sql/diabetes_tables.sql
```

La posterior consulta de relaciones confirma las cinco tablas existentes; no demuestra por sí sola qué ejecución las creó.

### 4.2. Error de rutas relativas

Estando dentro de `sql/`, la ruta `sql/diabetes_tables.sql` no existía. Lo mismo ocurrió en `dags/` al ejecutar `py_compile dags/diabetes_mlops_pipeline.py` desde dentro de esa carpeta.

**Regla:** desde la raíz usar `dags/diabetes_mlops_pipeline.py`; desde `dags/` usar `diabetes_mlops_pipeline.py`.

```bash
cd ~/MLOPS_PUJ-Talleres/Proyecto_2
python3 -m py_compile dags/diabetes_mlops_pipeline.py
```

No se mostró todavía la salida exitosa de este último comando.

### 4.3. Importación relativa de Python

El comando `python3 -m preprocessing.py` falló con:

```text
ImportError: attempted relative import with no known parent package
```

El módulo usa `from .common import engine` y debe importarse como parte del paquete `pipeline`:

```bash
cd ~/MLOPS_PUJ-Talleres/Proyecto_2
export PYTHONPATH="$PWD/src"
python3 -c 'import pipeline.preprocessing; print("Importación correcta")'
```

No se ha aportado el resultado de esa prueba. La compilación sintáctica **no** prueba imports ni conexión a la base.

### 4.4. Dependencias y `sudo`

`sqlalchemy` faltaba inicialmente y se instaló con `pip3 install SQLAlchemy`. Esa instalación en Python local **no implica** que el paquete esté dentro de la imagen de Airflow. Algunos comandos `kubectl` solicitaron contraseña de `sudo`; revisar `type kubectl` y los permisos del kubeconfig. Una contraseña escrita en la línea de comandos debe cambiarse si era auténtica.

## 5. Próximos pasos, en orden

1. **Validar clúster y Airflow existente:** `kubectl get nodes`, `kubectl get pods -A`, `helm list -A` y estado de Rancher. Evitar instalaciones duplicadas.
2. **Confirmar DAG real:** verificar archivo, imports y funciones existentes; reemplazar cualquier `pending_step` antes de ejecutarlo.
3. **Identificar imagen y scheduler de Airflow:** comprobar que los pods contienen `dags/diabetes_mlops_pipeline.py` y el paquete `pipeline`, con sus dependencias y Secrets.
4. **Comprobar importación en Airflow:** `airflow dags list`, `airflow dags list-import-errors` y `airflow tasks list diabetes_mlops_pipeline` desde el pod correcto.
5. **Verificar conexiones:** Airflow → PostgreSQL `diabetesdb`; Airflow → MLflow; MLflow → PostgreSQL backend y MinIO artifacts.
6. **Ejecutar ingesta incremental:** lotes de máximo **15.000 filas**, identificadores de lote y control de duplicados; comprobar filas RAW y CLEAN.
7. **Entrenar y registrar:** experimentos, métricas de validación, parámetros, artefactos, versiones y promoción automática de `champion` con criterios comparables.
8. **Activar inferencia:** API `/health`, `/predict`, `/model-info`, `/metrics`; modelo desde MLflow sin ruta local fija; guardar cada predicción en `diabetes.inference_logs`; Streamlit consume exclusivamente FastAPI.
9. **Observar carga:** Prometheus recolecta `/metrics`; Grafana visualiza RPS, latencia, percentiles y errores mientras Locust carga la API.
10. **Auditar despliegue:** comprobar `requests` y `limits` de todos los contenedores, PVC necesarios, ConfigMaps, Secrets, probes e imágenes propias en DockerHub.

**Comandos de control inmediatos:**

```bash
cd ~/MLOPS_PUJ-Talleres/Proyecto_2
kubectl get nodes -o wide
kubectl get pods -n mlops
helm list -A
ls -lh dags/diabetes_mlops_pipeline.py
grep -nE '^(def |class |async def )' src/pipeline/*.py
grep -nE '^(from |import )' dags/diabetes_mlops_pipeline.py
kubectl get pods -n mlops -o custom-columns='POD:.metadata.name,IMAGES:.spec.containers[*].image'
```

## 6. Criterios de aceptación del taller

- [ ] Todos los componentes desplegados en Kubernetes.
- [ ] DAG de Airflow funcional con tareas independientes.
- [ ] RAW y CLEAN almacenados en tablas separadas.
- [ ] Carga incremental por lotes de **máximo 15.000 registros**.
- [ ] MLflow con backend PostgreSQL y artefactos externos (MinIO), sin SQLite como backend final.
- [ ] Modelo consumido desde MLflow por la API.
- [ ] API sin ruta local fija ni versión de modelo escrita en el código.
- [ ] Registro persistente de cada inferencia.
- [ ] API con endpoint `/metrics`.
- [ ] Prometheus recolecta las métricas de la API.
- [ ] Grafana muestra el efecto de Locust durante una prueba de carga.
- [ ] `resources.requests` y `resources.limits` para todos los contenedores.
- [ ] Imágenes propias publicadas en registro externo.

**Otros entregables:** repositorio público, Dockerfiles, manifiestos/Helm Charts, scripts SQL, DAG, código de entrenamiento/API/UI, archivo Locust, configuración Prometheus, dashboard Grafana JSON, README y video de máximo 10 minutos.

## 7. Conclusión del avance

Se confirmó la base PostgreSQL `diabetesdb` con cinco tablas separadas y se superó la verificación de sintaxis de `preprocessing.py` y `training.py`. Los obstáculos encontrados fueron principalmente rutas relativas, comandos Bash dentro de un archivo SQL, dependencias ausentes e imports de paquetes Python. **Aún no hay evidencia de una ejecución exitosa del DAG ni de una arquitectura MLOps integrada.** La prioridad inmediata es validar los imports, el despliegue de Airflow y las conexiones con PostgreSQL y MLflow sin alterar los servicios que ya funcionan.
