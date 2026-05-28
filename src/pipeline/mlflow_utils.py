"""Utilidades de MLflow Registry: identificar y consultar el modelo productivo (RF6)."""

import logging

import mlflow
from mlflow.tracking import MlflowClient

from pipeline.config import MLFLOW_URI, MODEL_NAME, MODEL_ALIAS

logger = logging.getLogger(__name__)


def get_client() -> MlflowClient:
    mlflow.set_tracking_uri(MLFLOW_URI)
    return MlflowClient(tracking_uri=MLFLOW_URI)


def get_production_version():
    """Versión productiva (alias @champion) o None si no existe."""
    client = get_client()
    try:
        return client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
    except Exception:
        return None


def get_run_metrics(run_id: str) -> dict:
    client = get_client()
    try:
        return dict(client.get_run(run_id).data.metrics)
    except Exception:
        return {}


def get_production_metrics() -> dict:
    """Métricas del run que generó la versión productiva (o {} si no hay)."""
    ver = get_production_version()
    if ver is None:
        return {}
    return get_run_metrics(ver.run_id)


def set_champion(version: str) -> None:
    client = get_client()
    client.set_registered_model_alias(MODEL_NAME, MODEL_ALIAS, version)
    logger.info("Alias '%s' -> %s v%s", MODEL_ALIAS, MODEL_NAME, version)
