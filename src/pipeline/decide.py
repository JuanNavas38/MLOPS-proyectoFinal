"""Decisión automática de entrenamiento (RF4) y de promoción (RF6).
Reglas técnicas explícitas — no periodicidad."""

import json
import logging

from pipeline.config import (MIN_VOLUME_INCREASE_PCT, MIN_TRAIN_ROWS,
                             MAE_IMPROVE_MIN_PCT, RMSE_WORSEN_MAX_PCT)
from pipeline.db import read_sql
from pipeline.mlflow_utils import get_production_version

logger = logging.getLogger(__name__)


def _audit_row(batch_id: str) -> dict:
    df = read_sql("SELECT * FROM training_audit WHERE batch_id = %(b)s", {"b": batch_id})
    return df.iloc[0].to_dict() if not df.empty else {}


def _clean_train_count() -> int:
    df = read_sql("SELECT COUNT(*) AS n FROM clean_properties WHERE dataset_split = 'train'")
    return int(df["n"].iloc[0])


def evaluate_training_decision(batch_id: str) -> tuple:
    """Devuelve (should_train: bool, reason: str). Registra el porqué."""
    row = _audit_row(batch_id)

    if row.get("quality_status") == "invalid":
        return False, "No se entrena: lote inválido (calidad)."

    if _clean_train_count() < MIN_TRAIN_ROWS:
        return False, f"No se entrena: datos de entrenamiento insuficientes (< {MIN_TRAIN_ROWS})."

    # Línea base: si no hay modelo productivo, entrenar.
    if get_production_version() is None:
        return True, "Entrena: no existe modelo productivo (línea base inicial)."

    if bool(row.get("drift_detected")):
        return True, "Entrena: drift significativo en variables numéricas."

    new_cats = row.get("new_categories")
    if isinstance(new_cats, str):
        try: new_cats = json.loads(new_cats)
        except Exception: new_cats = {}
    if new_cats:
        cols = ", ".join(new_cats.keys())
        return True, f"Entrena: nuevas categorías relevantes en {cols}."

    n_batch = row.get("n_records_batch") or 0
    n_total = row.get("n_records_total") or 0
    prev = max(n_total - n_batch, 1)
    inc_pct = 100.0 * n_batch / prev
    if inc_pct >= MIN_VOLUME_INCREASE_PCT:
        return True, f"Entrena: el lote aumenta el volumen acumulado en {inc_pct:.1f}%."

    return False, (f"No se entrena: lote sin drift, sin categorías nuevas y crecimiento "
                   f"de volumen {inc_pct:.1f}% < {MIN_VOLUME_INCREASE_PCT}%.")


def evaluate_promotion_decision(candidate: dict, production: dict) -> tuple:
    """RF6: promover solo si MAE baja >= umbral y RMSE no empeora > umbral.
    Devuelve (promote: bool, reason: str)."""
    if not production:
        return True, "Promueve: no había modelo productivo previo (línea base)."

    cand_mae, prod_mae = candidate.get("mae"), production.get("mae")
    cand_rmse, prod_rmse = candidate.get("rmse"), production.get("rmse")
    if None in (cand_mae, prod_mae, cand_rmse, prod_rmse):
        return False, "Rechaza: métricas incompletas para comparar."

    mae_improve = 100.0 * (prod_mae - cand_mae) / prod_mae
    rmse_change = 100.0 * (cand_rmse - prod_rmse) / prod_rmse  # >0 = empeora

    if mae_improve >= MAE_IMPROVE_MIN_PCT and rmse_change <= RMSE_WORSEN_MAX_PCT:
        return True, (f"Promueve: MAE mejora {mae_improve:.1f}% (>= {MAE_IMPROVE_MIN_PCT}%) "
                      f"y RMSE varía {rmse_change:+.1f}% (<= {RMSE_WORSEN_MAX_PCT}%).")
    return False, (f"Rechaza: MAE mejora {mae_improve:.1f}% / RMSE varía {rmse_change:+.1f}% "
                   f"— no cumple la regla (MAE>={MAE_IMPROVE_MIN_PCT}%, RMSE<={RMSE_WORSEN_MAX_PCT}%).")
