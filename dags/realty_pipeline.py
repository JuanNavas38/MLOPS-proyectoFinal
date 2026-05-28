"""
DAG principal — MLOps Proyecto Final (Nivel 4)
Dominio: regresión de precios de propiedades inmobiliarias.

19 tareas con DOS bifurcaciones explícitas:
  - decide_training   : entrenar vs. no entrenar (RF4)
  - decide_promotion  : promover vs. rechazar el candidato (RF6)

La fila de `training_audit` (una por lote) es el estado compartido entre tareas
y la fuente del historial en Streamlit (RF9).
"""

import os
import sys
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Hacer importable el paquete del pipeline (montado en la imagen / PYTHONPATH)
sys.path.insert(0, os.getenv("PIPELINE_SRC", "/opt/airflow/src"))

from pipeline import audit, config                       # noqa: E402
from pipeline.ingest import store_raw_batch              # noqa: E402
from pipeline.preprocess import preprocess_batch         # noqa: E402
from pipeline import validate as V                       # noqa: E402
from pipeline.decide import (evaluate_training_decision, # noqa: E402
                             evaluate_promotion_decision)
from pipeline.train import train_candidate               # noqa: E402
from pipeline.mlflow_utils import get_production_metrics, set_champion  # noqa: E402
from ingestion.data_api_client import DataAPIClient, NoMoreDataError    # noqa: E402

TMP_DIR = os.getenv("PIPELINE_TMP", "/tmp")

DEFAULT_ARGS = {
    "owner": "mlops",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def _payload_path(batch_id: str) -> str:
    return os.path.join(TMP_DIR, f"{batch_id}.json")


# ── Tasks ────────────────────────────────────────────────────────────────────
def fetch_batch_from_api(**ctx):
    client = DataAPIClient(config.DATA_API_URL, group_number=config.GROUP_NUMBER)
    try:
        payload = client.fetch_batch()
    except NoMoreDataError as e:
        raise AirflowSkipException(f"Sin más lotes: {e}")

    batch_number = payload.get("batch_number")
    records = payload["data"]
    batch_id = f"g{config.GROUP_NUMBER}_b{batch_number}"

    with open(_payload_path(batch_id), "w", encoding="utf-8") as f:
        json.dump(payload, f)

    ti = ctx["ti"]
    ti.xcom_push(key="batch_id", value=batch_id)
    ti.xcom_push(key="n_records", value=len(records))
    ti.xcom_push(key="source", value=f"data-api/group{config.GROUP_NUMBER}/batch{batch_number}")


def store_raw_batch_task(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    source = ti.xcom_pull(key="source", task_ids="fetch_batch_from_api")
    with open(_payload_path(batch_id), encoding="utf-8") as f:
        records = json.load(f)["data"]

    res = store_raw_batch(batch_id, source, records)

    total = res["total_after"]
    audit.create_audit(batch_id, n_records_batch=res["loaded"], n_records_total=total)


def validate_schema_task(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    with open(_payload_path(batch_id), encoding="utf-8") as f:
        records = json.load(f)["data"]
    res = V.validate_schema(records)
    audit.update_audit(batch_id, schema_status=res["schema_status"])
    if res["schema_status"] == "invalid":
        raise ValueError(f"Esquema inválido: faltan {res['missing']}")


def validate_data_quality_task(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    res = V.validate_quality(batch_id)
    audit.update_audit(batch_id, quality_status=res["quality_status"])
    if res["quality_status"] == "invalid":
        raise ValueError(f"Calidad inválida: {res}")


def detect_new_categories_task(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    res = V.detect_new_categories(batch_id)
    audit.update_audit(batch_id, new_categories=res.get("new_categories", {}))


def detect_data_drift_task(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    res = V.detect_data_drift(batch_id)
    audit.update_audit(batch_id, drift_detected=res["drift_detected"],
                       drift_details=res.get("drift_details", {}))


def preprocess_data_task(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    preprocess_batch(batch_id)


def decide_training(**ctx) -> str:
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    should_train, reason = evaluate_training_decision(batch_id)
    audit.update_audit(batch_id, decision="trained" if should_train else "skipped",
                       decision_reason=reason)
    return "train_candidate_model" if should_train else "skip_training"


def skip_training(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    audit.update_audit(batch_id, status="success")


def train_candidate_model(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    row = V.read_sql("SELECT decision_reason FROM training_audit WHERE batch_id=%(b)s",
                     {"b": batch_id})
    reason = row["decision_reason"].iloc[0] if not row.empty else ""
    res = train_candidate(batch_id, reason=reason)
    ti.xcom_push(key="candidate", value=res)
    audit.update_audit(batch_id, candidate_run_id=res["run_id"],
                       model_name=config.MODEL_NAME, model_version=res["version"])


def evaluate_candidate_model(**ctx):
    """Consolida las métricas del candidato (calculadas en el run de entrenamiento)."""
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    cand = ti.xcom_pull(key="candidate", task_ids="train_candidate_model")
    audit.update_audit(batch_id, candidate_metrics=cand["metrics"])


def register_candidate_in_mlflow(**ctx):
    """El modelo ya quedó registrado en el run de entrenamiento; aquí se deja
    trazado en auditoría y se valida que exista la versión."""
    ti = ctx["ti"]
    cand = ti.xcom_pull(key="candidate", task_ids="train_candidate_model")
    if not cand or cand.get("version") is None:
        raise ValueError("No se registró la versión del candidato en MLflow")


def compare_with_production(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    prod = get_production_metrics()
    ti.xcom_push(key="production", value=prod)
    audit.update_audit(batch_id, production_metrics=prod)


def decide_promotion(**ctx) -> str:
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    cand = ti.xcom_pull(key="candidate", task_ids="train_candidate_model")
    prod = ti.xcom_pull(key="production", task_ids="compare_with_production")
    promote, reason = evaluate_promotion_decision(cand["metrics"], prod or {})
    audit.update_audit(batch_id, promotion_reason=reason)
    return "promote_model" if promote else "reject_model"


def promote_model(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    cand = ti.xcom_pull(key="candidate", task_ids="train_candidate_model")
    set_champion(cand["version"])
    audit.update_audit(batch_id, promoted=True)


def reject_model(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    audit.update_audit(batch_id, promoted=False)


def notify_or_log_result(**ctx):
    ti = ctx["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="fetch_batch_from_api")
    audit.update_audit(batch_id, status="success")
    try:
        os.remove(_payload_path(batch_id))
    except OSError:
        pass


# ── DAG definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id="realty_pipeline",
    description="MLOps Nivel 4: ingesta por lotes → validación → decisión de "
                "entrenamiento → registro en MLflow → comparación → promoción condicionada",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "realty", "nivel4", "javeriana"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    t_fetch   = PythonOperator(task_id="fetch_batch_from_api",  python_callable=fetch_batch_from_api)
    t_store   = PythonOperator(task_id="store_raw_batch",       python_callable=store_raw_batch_task)
    t_schema  = PythonOperator(task_id="validate_schema",       python_callable=validate_schema_task)
    t_quality = PythonOperator(task_id="validate_data_quality", python_callable=validate_data_quality_task)
    t_newcat  = PythonOperator(task_id="detect_new_categories", python_callable=detect_new_categories_task)
    t_drift   = PythonOperator(task_id="detect_data_drift",     python_callable=detect_data_drift_task)
    t_prep    = PythonOperator(task_id="preprocess_data",       python_callable=preprocess_data_task)

    t_decide_train = BranchPythonOperator(task_id="decide_training", python_callable=decide_training)
    t_skip         = PythonOperator(task_id="skip_training",         python_callable=skip_training)

    t_train    = PythonOperator(task_id="train_candidate_model",      python_callable=train_candidate_model)
    t_eval     = PythonOperator(task_id="evaluate_candidate_model",   python_callable=evaluate_candidate_model)
    t_register = PythonOperator(task_id="register_candidate_in_mlflow", python_callable=register_candidate_in_mlflow)
    t_compare  = PythonOperator(task_id="compare_with_production",    python_callable=compare_with_production)

    t_decide_promo = BranchPythonOperator(task_id="decide_promotion", python_callable=decide_promotion)
    t_promote      = PythonOperator(task_id="promote_model",          python_callable=promote_model)
    t_reject       = PythonOperator(task_id="reject_model",           python_callable=reject_model)

    t_notify = PythonOperator(
        task_id="notify_or_log_result",
        python_callable=notify_or_log_result,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> t_fetch >> t_store >> t_schema >> t_quality >> t_newcat >> t_drift >> t_prep >> t_decide_train
    t_decide_train >> t_skip >> t_notify
    t_decide_train >> t_train >> t_eval >> t_register >> t_compare >> t_decide_promo
    t_decide_promo >> t_promote >> t_notify
    t_decide_promo >> t_reject  >> t_notify
    t_notify >> end
