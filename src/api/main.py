"""
API de inferencia — MLOps Proyecto Final (Nivel 4)
Regresión de precios de propiedades. MLflow es la única fuente de verdad del
modelo productivo (RF7): no se queman rutas ni versiones; se carga por alias.
"""

import os
import time
import uuid
import json
import logging
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import mlflow
import mlflow.pyfunc
import pandas as pd
import psycopg2
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel, Field
from starlette.responses import Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MLFLOW_URI    = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-svc:5000")
MODEL_NAME    = os.getenv("MODEL_NAME", "realty-champion")
MODEL_ALIAS   = os.getenv("MODEL_ALIAS", "champion")
DB_URL        = os.getenv("DATABASE_URL", "postgresql://mlops:mlops2026@postgres-svc:5432/mlops")
RELOAD_TOKEN  = os.getenv("RELOAD_TOKEN", "")  # protege el endpoint admin /reload

# ── Métricas Prometheus (RF10) ──────────────────────────────────────────────────
REQUEST_COUNT = Counter("api_requests_total", "Total de solicitudes",
                        ["method", "endpoint", "status"])
REQUEST_LATENCY = Histogram("api_request_duration_seconds", "Latencia de solicitudes",
                            ["endpoint"], buckets=[.01, .025, .05, .1, .25, .5, 1.0, 2.5])
PREDICTION_COUNT = Counter("api_predictions_total", "Total de predicciones")
MODEL_VERSION_G  = Gauge("api_model_version", "Versión del modelo cargado")


# ── Model cache con recarga segura (RF7) ────────────────────────────────────────
class ModelCache:
    """Mantiene el modelo productivo cargado desde MLflow.
    La recarga es atómica: si falla la descarga, conserva el modelo previo (fallback).
    Un lock evita que una petición use un modelo a medio reemplazar (concurrencia).
    """
    def __init__(self):
        self._lock      = threading.RLock()
        self.model      = None
        self.version    = None
        self.model_name = MODEL_NAME
        self.loaded_at  = None

    def load(self) -> dict:
        mlflow.set_tracking_uri(MLFLOW_URI)
        model_uri = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
        logger.info(f"Cargando modelo desde {model_uri}")
        # Descargar FUERA del lock para no bloquear inferencias durante la descarga
        new_model = mlflow.pyfunc.load_model(model_uri)
        client    = mlflow.tracking.MlflowClient(tracking_uri=MLFLOW_URI)
        version   = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS).version
        # Intercambio atómico
        with self._lock:
            self.model     = new_model
            self.version   = version
            self.loaded_at = datetime.utcnow()
        try:
            MODEL_VERSION_G.set(float(version))
        except (TypeError, ValueError):
            pass
        logger.info(f"Modelo cargado: {MODEL_NAME} v{version}")
        return {"model_name": MODEL_NAME, "version": version}

    def predict(self, df: pd.DataFrame) -> float:
        with self._lock:
            if self.model is None:
                raise RuntimeError("Modelo no disponible")
            return float(self.model.predict(df)[0])


cache = ModelCache()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        cache.load()
    except Exception as e:  # arranca aunque MLflow aún no tenga un champion
        logger.warning(f"No se pudo cargar el modelo al iniciar: {e}")
    yield


app = FastAPI(
    title="Realty Price API",
    description="API de inferencia MLOps — Proyecto Final Nivel 4 (PUJ)",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


# ── Schemas ─────────────────────────────────────────────────────────────────────
class PropertyFeatures(BaseModel):
    """Features crudas de una propiedad. El modelo productivo (pipeline MLflow)
    incorpora su propio preprocesamiento, por eso recibe valores naturales."""
    brokered_by:    Optional[str]   = Field(None, description="Agencia/corredor codificado")
    status:         str             = Field("for_sale", description="for_sale / ready_to_build")
    bed:            int             = Field(..., ge=0)
    bath:           int             = Field(..., ge=0)
    acre_lot:       float           = Field(..., ge=0)
    street:         Optional[str]   = Field(None)
    city:           str             = Field(...)
    state:          str             = Field(...)
    zip_code:       str             = Field(...)
    house_size:     float           = Field(..., ge=0)
    prev_sold_date: Optional[str]   = Field(None, description="YYYY-MM-DD si existe")


class PredictResponse(BaseModel):
    request_id:       str
    predicted_price:  float
    model_name:       str
    model_version:    str
    model_alias:      str
    response_time_ms: float


# ── DB helper (RF8) ───────────────────────────────────────────────────────────
def log_inference(req_id, input_data, predicted_price, response_ms, status="success", err=None):
    try:
        conn = psycopg2.connect(DB_URL)
        cur  = conn.cursor()
        cur.execute(
            """
            INSERT INTO inference_logs (
                request_id, input_data, predicted_price,
                model_name, model_version, model_alias, response_time_ms,
                status, error_message
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (req_id, json.dumps(input_data), predicted_price,
             cache.model_name, str(cache.version), MODEL_ALIAS, response_ms, status, err),
        )
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        logger.error(f"Error logging inference: {e}")


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_loaded": cache.model is not None,
        "model_version": cache.version,
        "loaded_at": str(cache.loaded_at),
    }


@app.get("/model-info")
def model_info():
    if cache.model is None:
        raise HTTPException(503, "Modelo no cargado")
    return {
        "model_name": cache.model_name,
        "model_version": cache.version,
        "model_alias": MODEL_ALIAS,
        "loaded_at": str(cache.loaded_at),
    }


@app.post("/reload")
def reload_model(x_reload_token: str = Header(default="")):
    """RF7: recarga el modelo productivo desde MLflow SIN redesplegar.
    Protegido por token. Ante error de descarga conserva el modelo previo (fallback)."""
    if RELOAD_TOKEN and x_reload_token != RELOAD_TOKEN:
        raise HTTPException(401, "Token inválido")
    try:
        info = cache.load()
        return {"status": "reloaded", **info}
    except Exception as e:
        logger.error(f"Recarga fallida, se conserva el modelo previo: {e}")
        raise HTTPException(503, f"Recarga fallida (fallback al modelo previo): {e}")


@app.post("/predict", response_model=PredictResponse)
def predict(req: PropertyFeatures):
    if cache.model is None:
        REQUEST_COUNT.labels("POST", "/predict", "503").inc()
        raise HTTPException(503, "Modelo no disponible")

    start  = time.time()
    req_id = str(uuid.uuid4())
    df = pd.DataFrame([req.dict()])

    try:
        predicted_price = cache.predict(df)
    except Exception as e:
        elapsed_ms = (time.time() - start) * 1000
        REQUEST_COUNT.labels("POST", "/predict", "500").inc()
        log_inference(req_id, req.dict(), None, elapsed_ms, status="error", err=str(e))
        raise HTTPException(500, f"Error de inferencia: {e}")

    elapsed_ms = (time.time() - start) * 1000
    REQUEST_COUNT.labels("POST", "/predict", "200").inc()
    REQUEST_LATENCY.labels("/predict").observe(elapsed_ms / 1000)
    PREDICTION_COUNT.inc()
    log_inference(req_id, req.dict(), predicted_price, elapsed_ms)

    return PredictResponse(
        request_id=req_id,
        predicted_price=predicted_price,
        model_name=cache.model_name,
        model_version=str(cache.version),
        model_alias=MODEL_ALIAS,
        response_time_ms=elapsed_ms,
    )


@app.get("/metrics")
def metrics():
    REQUEST_COUNT.labels("GET", "/metrics", "200").inc()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
