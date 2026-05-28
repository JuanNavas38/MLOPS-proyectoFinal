"""Ingesta del lote crudo a raw_properties (RF1/RF2).
Inserción masiva con deduplicación por row_hash (idempotente)."""

import hashlib
import json
import logging
from datetime import datetime

from psycopg2.extras import execute_values

from pipeline.config import RAW_COLUMNS
from pipeline.db import get_conn

logger = logging.getLogger(__name__)

_INSERT_COLS = [
    "batch_id", "source", "row_hash",
    "brokered_by", "status_listing", "price", "bed", "bath", "acre_lot",
    "street", "city", "state", "zip_code", "house_size", "prev_sold_date",
]


def _clean_id(v):
    """brokered_by/street/zip_code llegan como float (603.0) -> '603'."""
    if v is None or (isinstance(v, float) and v != v):  # NaN
        return None
    if isinstance(v, float):
        return str(int(v))
    return str(v).strip()


def _to_int(v):
    if v is None or (isinstance(v, float) and v != v):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _to_float(v):
    if v is None or (isinstance(v, float) and v != v):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_date(v):
    if not v or not isinstance(v, str):
        return None
    try:
        datetime.strptime(v[:10], "%Y-%m-%d")
        return v[:10]
    except ValueError:
        return None


def _row_hash(rec: dict) -> str:
    payload = json.dumps({k: rec.get(k) for k in RAW_COLUMNS}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def store_raw_batch(batch_id: str, source: str, records: list) -> dict:
    """Inserta el lote en raw_properties. Devuelve {loaded, skipped}."""
    rows = []
    for rec in records:
        rows.append((
            batch_id, source, _row_hash(rec),
            _clean_id(rec.get("brokered_by")),
            rec.get("status"),
            _to_float(rec.get("price")),
            _to_int(rec.get("bed")),
            _to_int(rec.get("bath")),
            _to_float(rec.get("acre_lot")),
            _clean_id(rec.get("street")),
            rec.get("city"),
            rec.get("state"),
            _clean_id(rec.get("zip_code")),
            _to_float(rec.get("house_size")),
            _to_date(rec.get("prev_sold_date")),
        ))

    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_properties")
    before = cur.fetchone()[0]

    sql = (
        f"INSERT INTO raw_properties ({', '.join(_INSERT_COLS)}) VALUES %s "
        f"ON CONFLICT (row_hash) DO NOTHING"
    )
    execute_values(cur, sql, rows, page_size=5000)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM raw_properties")
    after = cur.fetchone()[0]
    cur.close(); conn.close()

    loaded = after - before
    skipped = len(rows) - loaded
    logger.info("RAW %s: %s insertados, %s duplicados", batch_id, loaded, skipped)
    return {"loaded": loaded, "skipped": skipped, "total_after": after}
