import mlflow
import mlflow.sklearn
import pandas as pd

from mlflow.tracking import MlflowClient
from sklearn.datasets import make_regression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split

mlflow.set_experiment("taller_locust")

# Datos sintéticos con tres variables de entrada
X, y = make_regression(
    n_samples=4000,
    n_features=3,
    noise=0.25,
    random_state=42
)

X = pd.DataFrame(
    X,
    columns=["feature1", "feature2", "feature3"]
)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

modelo = RandomForestRegressor(
    n_estimators=50,
    max_depth=10,
    n_jobs=1,
    random_state=42
)

modelo.fit(X_train, y_train)
r2 = r2_score(y_test, modelo.predict(X_test))

with mlflow.start_run():
    mlflow.log_param("n_estimators", 50)
    mlflow.log_param("max_depth", 10)
    mlflow.log_metric("r2", r2)

    mlflow.sklearn.log_model(
        sk_model=modelo,
        name="modelo",
        input_example=X_test.head(3),
        registered_model_name="modelo_locust"
    )

# El alias champion apunta a la version mas reciente
client = MlflowClient()
versiones = client.search_model_versions(
    "name='modelo_locust'"
)
ultima = max(int(v.version) for v in versiones)

client.set_registered_model_alias(
    "modelo_locust", "champion", ultima
)

print(f"Modelo registrado: version {ultima}")
print(f"R2 de prueba: {r2:.4f}")

