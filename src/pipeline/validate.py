"""Validaciones del lote (RF3): esquema, calidad, nuevas categorías y drift."""

import logging

import pandas as pd
from scipy.stats import ks_2samp

from pipeline.config import (RAW_COLUMNS, DRIFT_PVALUE_THRESHOLD,
                             NEW_CATEGORY_MIN_FREQ)
from pipeline.db import read_sql

logger = logging.getLogger(__name__)

_CATS = ["status_listing", "city", "state", "zip_code", "brokered_by", "street"]
_NUMS = ["price", "house_size", "acre_lot", "bed", "bath"]


def validate_schema(records: list) -> dict:
    """Compara columnas recibidas vs esperadas."""
    got = set(records[0].keys()) if records else set()
    expected = set(RAW_COLUMNS)
    missing = sorted(expected - got)
    extra = sorted(got - expected)
    if "price" in missing:
        status = "invalid"          # sin target no se puede entrenar
    elif missing or extra:
        status = "changed"          # recuperable: el pipeline ignora extras / imputa
    else:
        status = "ok"
    return {"schema_status": status, "missing": missing, "extra": extra}


def validate_quality(batch_id: str) -> dict:
    """Calidad sobre el lote ya almacenado en RAW."""
    df = read_sql("SELECT * FROM raw_properties WHERE batch_id = %(b)s", {"b": batch_id})
    n = len(df)
    if n == 0:
        return {"quality_status": "invalid", "reason": "lote vacío"}

    price_invalid = int((df["price"].isna() | (df["price"] <= 0)).sum())
    dup = int(df.duplicated(subset=[c for c in df.columns if c not in
              ("id", "load_timestamp", "row_hash")]).sum())
    null_rates = {c: round(float(df[c].isna().mean()), 4) for c in
                  ["bed", "bath", "acre_lot", "house_size", "prev_sold_date"]}

    price_invalid_pct = price_invalid / n
    if price_invalid_pct > 0.5:
        status = "invalid"
    elif price_invalid_pct > 0.05 or dup > 0:
        status = "warning"
    else:
        status = "ok"

    return {
        "quality_status": status,
        "n": n,
        "price_invalid": price_invalid,
        "price_invalid_pct": round(price_invalid_pct, 4),
        "duplicates": dup,
        "null_rates": null_rates,
    }


def _historical_df(batch_id: str, cols: list) -> pd.DataFrame:
    return read_sql(
        f"SELECT {', '.join(cols)} FROM raw_properties WHERE batch_id <> %(b)s",
        {"b": batch_id},
    )


def detect_new_categories(batch_id: str) -> dict:
    """Categorías nuevas en el lote (no vistas antes) con frecuencia relevante."""
    batch = read_sql(
        f"SELECT {', '.join(_CATS)} FROM raw_properties WHERE batch_id = %(b)s",
        {"b": batch_id})
    hist = _historical_df(batch_id, _CATS)
    n = len(batch)
    result = {}
    if hist.empty:
        return {"new_categories": {}, "note": "sin histórico (primer lote)"}
    for col in _CATS:
        seen = set(hist[col].dropna().astype(str))
        freq = batch[col].dropna().astype(str).value_counts()
        new_vals = [v for v, c in freq.items()
                    if v not in seen and (c / n) >= NEW_CATEGORY_MIN_FREQ]
        if new_vals:
            result[col] = new_vals[:50]
    return {"new_categories": result}


def detect_data_drift(batch_id: str) -> dict:
    """KS test del lote vs histórico sobre variables numéricas."""
    batch = read_sql(
        f"SELECT {', '.join(_NUMS)} FROM raw_properties WHERE batch_id = %(b)s",
        {"b": batch_id})
    hist = _historical_df(batch_id, _NUMS)
    if hist.empty or len(batch) < 30:
        return {"drift_detected": False, "drift_details": {}, "note": "histórico insuficiente"}

    details, drift = {}, False
    for col in _NUMS:
        a = batch[col].dropna().astype(float)
        b = hist[col].dropna().astype(float)
        if len(a) < 30 or len(b) < 30:
            continue
        stat, p = ks_2samp(a, b)
        details[col] = {"ks": round(float(stat), 4), "pvalue": round(float(p), 6)}
        if p < DRIFT_PVALUE_THRESHOLD:
            drift = True
    return {"drift_detected": drift, "drift_details": details}
