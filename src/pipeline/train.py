"""Entrenamiento, evaluación y registro del modelo candidato en MLflow (RF5)."""

import logging
import os
import tempfile

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from pipeline.config import (MLFLOW_URI, EXPERIMENT, MODEL_NAME,
                             NUMERIC_FEATURES, CATEGORICAL_FEATURES, FEATURES, TARGET,
                             TRAIN_SAMPLE_MAX)
from pipeline.db import read_sql
from pipeline.mlflow_utils import get_client

logger = logging.getLogger(__name__)


def _load_split(split: str) -> pd.DataFrame:
    cols = ", ".join(FEATURES + [TARGET])
    return read_sql(
        f"SELECT {cols} FROM clean_properties WHERE dataset_split = %(s)s",
        {"s": split})


def _metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mask = y_true > 0
    mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)
    r2 = float(r2_score(y_true, y_pred))
    return {"mae": mae, "rmse": rmse, "mape": mape, "r2": r2}


def _build_estimator() -> TransformedTargetRegressor:
    numeric = Pipeline([("impute", SimpleImputer(strategy="median"))])
    categorical = Pipeline([
        ("impute", SimpleImputer(strategy="constant", fill_value="missing")),
        ("encode", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
    ])
    pre = ColumnTransformer([
        ("num", numeric, NUMERIC_FEATURES),
        ("cat", categorical, CATEGORICAL_FEATURES),
    ], remainder="drop")
    model = Pipeline([
        ("prep", pre),
        ("reg", HistGradientBoostingRegressor(
            max_iter=150, learning_rate=0.1, max_depth=8, random_state=42)),
    ])
    # Entrena en log(price) y predice en escala de precio.
    return TransformedTargetRegressor(regressor=model, func=np.log1p, inverse_func=np.expm1)


def _log_residual_plot(y_true, y_pred):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        return
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.scatter(y_true, y_pred, s=4, alpha=0.3)
    lim = [0, float(np.percentile(y_true, 99))]
    ax.plot(lim, lim, "r--", lw=1)
    ax.set_xlabel("Precio real"); ax.set_ylabel("Precio predicho")
    ax.set_title("Predicho vs Real (test)"); ax.set_xlim(lim); ax.set_ylim(lim)
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "pred_vs_real.png")
        fig.savefig(p, dpi=110, bbox_inches="tight")
        mlflow.log_artifact(p, artifact_path="plots")
    plt.close(fig)


def train_candidate(batch_id: str, reason: str = "") -> dict:
    """Entrena y registra un candidato. Devuelve {run_id, version, metrics}."""
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    train_df, val_df, test_df = _load_split("train"), _load_split("val"), _load_split("test")
    if test_df.empty:        # garantizar evaluación
        test_df = val_df if not val_df.empty else train_df

    # Tope de muestreo del fit: en minikube (nodo único) entrenar sobre cientos de
    # miles de filas satura la CPU y ahoga el control plane.
    if len(train_df) > TRAIN_SAMPLE_MAX:
        train_df = train_df.sample(n=TRAIN_SAMPLE_MAX, random_state=42)
        logger.info("Muestreo de entrenamiento: %s filas (de tope %s)", len(train_df), TRAIN_SAMPLE_MAX)

    X_train, y_train = train_df[FEATURES], train_df[TARGET].to_numpy()
    X_test,  y_test  = test_df[FEATURES],  test_df[TARGET].to_numpy()

    est = _build_estimator()

    with mlflow.start_run(run_name=f"candidate_{batch_id}") as run:
        est.fit(X_train, y_train)
        y_pred = est.predict(X_test)
        metrics = _metrics(y_test, y_pred)

        mlflow.log_params({
            "model": "HistGradientBoostingRegressor",
            "target_transform": "log1p",
            "n_features": len(FEATURES),
            "train_rows": len(X_train),
            "test_rows": len(X_test),
            "batch_id": batch_id,
        })
        mlflow.log_metrics(metrics)
        mlflow.set_tag("batch_id", batch_id)
        mlflow.set_tag("training_reason", reason)
        _log_residual_plot(y_test, y_pred)

        signature = infer_signature(X_test, y_pred)
        mlflow.sklearn.log_model(
            sk_model=est,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
            signature=signature,
            input_example=X_test.head(2),
        )
        run_id = run.info.run_id

    # Resolver la versión recién registrada para este run
    client = get_client()
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    version = next((v.version for v in versions if v.run_id == run_id), None)

    logger.info("Candidato %s v%s — MAE=%.0f RMSE=%.0f R2=%.3f",
                MODEL_NAME, version, metrics["mae"], metrics["rmse"], metrics["r2"])
    return {"run_id": run_id, "version": version, "metrics": metrics}
