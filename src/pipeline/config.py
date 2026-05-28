"""Configuración central del pipeline (env con defaults para entorno LOCAL).
En Kubernetes estas variables vienen de ConfigMaps/Secrets."""

import os

# Conexiones (defaults = docker-compose local con puertos remapeados)
DATABASE_URL   = os.getenv("DATABASE_URL", "postgresql://mlops:mlops2026@localhost:5442/mlops")
MLFLOW_URI     = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5010")
DATA_API_URL   = os.getenv("DATA_API_URL", "http://localhost:8000")
GROUP_NUMBER   = int(os.getenv("DATA_GROUP_NUMBER", "1"))

# Artefactos MLflow (MinIO) — el cliente sube directo al bucket
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9010"))
os.environ.setdefault("AWS_ACCESS_KEY_ID", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin2026"))

# MLflow / modelo
EXPERIMENT  = os.getenv("MLFLOW_EXPERIMENT", "realty-price-regression")
MODEL_NAME  = os.getenv("MODEL_NAME", "realty-champion")
MODEL_ALIAS = os.getenv("MODEL_ALIAS", "champion")

# Columnas
RAW_COLUMNS = [
    "brokered_by", "status", "price", "bed", "bath", "acre_lot",
    "street", "city", "state", "zip_code", "house_size", "prev_sold_date",
]
NUMERIC_FEATURES     = ["bed", "bath", "acre_lot", "house_size"]
CATEGORICAL_FEATURES = ["status", "brokered_by", "street", "city", "state", "zip_code"]
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET   = "price"

# Reglas de decisión (RF4 / RF6)
MIN_VOLUME_INCREASE_PCT = float(os.getenv("MIN_VOLUME_INCREASE_PCT", "5.0"))
DRIFT_PVALUE_THRESHOLD  = float(os.getenv("DRIFT_PVALUE_THRESHOLD", "0.05"))
NEW_CATEGORY_MIN_FREQ   = float(os.getenv("NEW_CATEGORY_MIN_FREQ", "0.01"))  # 1% del lote
MAE_IMPROVE_MIN_PCT     = float(os.getenv("MAE_IMPROVE_MIN_PCT", "3.0"))
RMSE_WORSEN_MAX_PCT     = float(os.getenv("RMSE_WORSEN_MAX_PCT", "1.0"))

# Suficiencia mínima de datos para entrenar
MIN_TRAIN_ROWS = int(os.getenv("MIN_TRAIN_ROWS", "200"))
