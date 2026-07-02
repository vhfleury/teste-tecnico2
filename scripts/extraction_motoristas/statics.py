"""Constants for the extraction_motoristas pipeline. No functions here."""
import os

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
LAKEHOUSE_DIR = os.environ.get("LAKEHOUSE_DIR", "/opt/airflow/lakehouse")

MOTORISTAS_JSON = os.path.join(DATA_DIR, "motoristas", "motoristas.json")
RAW_DIR = f"{LAKEHOUSE_DIR}/raw/motoristas"
