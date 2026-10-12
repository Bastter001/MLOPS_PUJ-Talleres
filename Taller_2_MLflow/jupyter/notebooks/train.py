"""Ejecuta el entrenamiento definido en penguins_mlflow.py."""
from pathlib import Path
import runpy

script = Path(__file__).with_name("penguins_mlflow.py")
runpy.run_path(str(script), run_name="__main__")
