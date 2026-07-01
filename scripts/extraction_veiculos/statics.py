"""Constants for the extraction_veiculos pipeline. No functions here."""
import os

PLATE_MERCOSUL_REGEX = r"^[A-Z]{3}[0-9][A-Z][0-9]{2}$"
VALID_STATUS = ["ativo", "em_manutencao", "inativo"]
VALID_TYPES = [
    "VUC",
    "Caminhão Toco",
    "Caminhão Truck",
    "Carreta Simples",
    "Carreta LS",
    "Bitrem",
]
MIN_MANUFACTURE_YEAR = 1990

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
LAKEHOUSE_DIR = os.environ.get("LAKEHOUSE_DIR", "/opt/airflow/lakehouse")

VEICULOS_CSV = os.path.join(DATA_DIR, "veiculos", "veiculos.csv")
RAW_DIR = f"{LAKEHOUSE_DIR}/raw/veiculos"
STAGING_DIR = f"{LAKEHOUSE_DIR}/staging/veiculos"
