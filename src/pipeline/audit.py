"""Gestión de la tabla training_audit: una fila por lote, actualizada por las
tareas del DAG. Es la fuente del historial en Streamlit (RF4/RF9)."""

import json

from pipeline.db import get_conn

# Columnas que aceptan dict/list y se serializan a JSON
_JSON_FIELDS = {"new_categories", "drift_details", "candidate_metrics", "production_metrics"}
_ALLOWED = {
    "n_records_batch", "n_records_total", "schema_status", "quality_status",
    "new_categories", "drift_detected", "drift_details", "decision",
    "decision_reason", "candidate_run_id", "candidate_metrics",
    "production_metrics", "promoted", "promotion_reason", "model_name",
    "model_version", "status",
}


def create_audit(batch_id: str, n_records_batch: int, n_records_total: int) -> None:
    """Crea (o reinicia) la fila de auditoría del lote."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM training_audit WHERE batch_id = %s", (batch_id,))
    cur.execute(
        "INSERT INTO training_audit (batch_id, n_records_batch, n_records_total, status) "
        "VALUES (%s, %s, %s, 'running')",
        (batch_id, n_records_batch, n_records_total),
    )
    conn.commit(); cur.close(); conn.close()


def update_audit(batch_id: str, **fields) -> None:
    """Actualiza columnas de la fila del lote. dict/list -> JSONB."""
    sets, vals = [], []
    for k, v in fields.items():
        if k not in _ALLOWED:
            raise ValueError(f"Campo de auditoría no permitido: {k}")
        if k in _JSON_FIELDS and v is not None:
            v = json.dumps(v)
        sets.append(f"{k} = %s")
        vals.append(v)
    if not sets:
        return
    vals.append(batch_id)
    conn = get_conn(); cur = conn.cursor()
    cur.execute(f"UPDATE training_audit SET {', '.join(sets)} WHERE batch_id = %s", vals)
    conn.commit(); cur.close(); conn.close()
