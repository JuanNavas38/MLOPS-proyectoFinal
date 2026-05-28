"""Ejecuta el flujo del DAG para N lotes SIN Airflow (desarrollo/pruebas locales).
Replica la lógica de realty_pipeline.py de forma lineal.

  PYTHONPATH=src python scripts/run_pipeline_once.py [N_lotes]
"""

import logging
import os
import sys

# Windows: la consola cp1252 no codifica los emojis que imprime MLflow.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline import audit, config                              # noqa: E402
from pipeline.ingest import store_raw_batch                     # noqa: E402
from pipeline import validate as V                              # noqa: E402
from pipeline.preprocess import preprocess_batch                # noqa: E402
from pipeline.decide import (evaluate_training_decision,        # noqa: E402
                             evaluate_promotion_decision)
from pipeline.train import train_candidate                      # noqa: E402
from pipeline.mlflow_utils import get_production_metrics, set_champion  # noqa: E402
from ingestion.data_api_client import DataAPIClient, NoMoreDataError    # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("runner")


def run_once(client: DataAPIClient) -> bool:
    payload = client.fetch_batch()
    bn = payload["batch_number"]
    records = payload["data"]
    batch_id = f"g{config.GROUP_NUMBER}_b{bn}"
    source = f"data-api/group{config.GROUP_NUMBER}/batch{bn}"
    log.info("=== Lote %s (%s registros) ===", batch_id, len(records))

    res = store_raw_batch(batch_id, source, records)
    audit.create_audit(batch_id, res["loaded"], res["total_after"])

    audit.update_audit(batch_id, schema_status=V.validate_schema(records)["schema_status"])
    audit.update_audit(batch_id, quality_status=V.validate_quality(batch_id)["quality_status"])
    nc = V.detect_new_categories(batch_id)
    audit.update_audit(batch_id, new_categories=nc.get("new_categories", {}))
    dr = V.detect_data_drift(batch_id)
    audit.update_audit(batch_id, drift_detected=dr["drift_detected"],
                       drift_details=dr.get("drift_details", {}))

    preprocess_batch(batch_id)

    should, reason = evaluate_training_decision(batch_id)
    audit.update_audit(batch_id, decision="trained" if should else "skipped",
                       decision_reason=reason)
    log.info("DECISIÓN ENTRENAMIENTO: %s — %s", should, reason)
    if not should:
        audit.update_audit(batch_id, status="success")
        return True

    cand = train_candidate(batch_id, reason)
    audit.update_audit(batch_id, candidate_run_id=cand["run_id"],
                       model_name=config.MODEL_NAME, model_version=cand["version"],
                       candidate_metrics=cand["metrics"])
    prod = get_production_metrics()
    audit.update_audit(batch_id, production_metrics=prod)

    promote, preason = evaluate_promotion_decision(cand["metrics"], prod or {})
    audit.update_audit(batch_id, promotion_reason=preason)
    log.info("DECISIÓN PROMOCIÓN: %s — %s", promote, preason)
    if promote:
        set_champion(cand["version"])
        audit.update_audit(batch_id, promoted=True)
    else:
        audit.update_audit(batch_id, promoted=False)
    audit.update_audit(batch_id, status="success")
    return True


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    client = DataAPIClient(config.DATA_API_URL, group_number=config.GROUP_NUMBER)
    if not client.health():
        log.error("API de datos no responde en %s", config.DATA_API_URL)
        sys.exit(1)
    for i in range(n):
        try:
            run_once(client)
        except NoMoreDataError as e:
            log.info("Fin de datos: %s", e)
            break


if __name__ == "__main__":
    main()
