"""Transformación RAW -> CLEAN (RF2). Reproducible y trazable por raw_id.
Las categóricas se conservan como texto; el encoding ocurre en el pipeline del modelo."""

import logging

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

from pipeline.db import get_conn, read_sql

logger = logging.getLogger(__name__)

_CLEAN_COLS = [
    "raw_id", "batch_id", "bed", "bath", "acre_lot", "house_size",
    "house_age_years", "status", "brokered_by", "street", "city", "state",
    "zip_code", "price", "log_price", "dataset_split",
]


def preprocess_batch(batch_id: str, seed: int = 42) -> dict:
    """Procesa el lote crudo y lo inserta en clean_properties."""
    df = read_sql("SELECT * FROM raw_properties WHERE batch_id = %(b)s", {"b": batch_id})
    n_raw = len(df)
    if n_raw == 0:
        return {"clean_rows": 0, "dropped": 0}

    # Filtrar registros sin target válido
    df = df[df["price"].notna() & (df["price"] > 0)].copy()

    # Feature derivada: antigüedad desde la última venta
    sold = pd.to_datetime(df["prev_sold_date"], errors="coerce")
    df["house_age_years"] = (pd.Timestamp.utcnow().tz_localize(None) - sold).dt.days / 365.25

    # Target estabilizado
    df["log_price"] = np.log1p(df["price"])

    # Split reproducible 70/15/15
    rng = np.random.default_rng(seed)
    r = rng.random(len(df))
    split = np.where(r < 0.70, "train", np.where(r < 0.85, "val", "test"))
    df["dataset_split"] = split

    rows = [(
        int(row["id"]), batch_id,
        None if pd.isna(row["bed"]) else int(row["bed"]),
        None if pd.isna(row["bath"]) else int(row["bath"]),
        None if pd.isna(row["acre_lot"]) else float(row["acre_lot"]),
        None if pd.isna(row["house_size"]) else float(row["house_size"]),
        None if pd.isna(row["house_age_years"]) else float(row["house_age_years"]),
        row["status_listing"], row["brokered_by"], row["street"],
        row["city"], row["state"], row["zip_code"],
        float(row["price"]), float(row["log_price"]), row["dataset_split"],
    ) for _, row in df.iterrows()]

    conn = get_conn(); cur = conn.cursor()
    execute_values(
        cur,
        f"INSERT INTO clean_properties ({', '.join(_CLEAN_COLS)}) VALUES %s",
        rows, page_size=5000,
    )
    conn.commit(); cur.close(); conn.close()

    dropped = n_raw - len(df)
    logger.info("CLEAN %s: %s filas (descartadas %s sin precio)", batch_id, len(df), dropped)
    return {"clean_rows": len(df), "dropped": dropped}
