import os
import time
import uuid
import logging
from contextlib import asynccontextmanager
from datetime import datetime

import mlflow
import mlflow.sklearn
import numpy as np
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel, Field
from starlette.responses import Response

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MLFLOW_URI   = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-svc:5000")
MODEL_NAME   = os.getenv("MODEL_NAME", "diabetes-champion")
MODEL_ALIAS  = os.getenv("MODEL_ALIAS", "champion")
DB_URL       = os.getenv("DATABASE_URL", "postgresql://mlops:mlops2026@postgres-svc:5432/mlops")

# ── Prometheus metrics ────────────────────────────────────────────────────────
REQUEST_COUNT = Counter(
    "api_requests_total",
    "Total de solicitudes",
    ["method", "endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "api_request_duration_seconds",
    "Latencia de solicitudes",
    ["endpoint"],
    buckets=[.01, .025, .05, .1, .25, .5, 1.0, 2.5]
)
PREDICTION_COUNT = Counter(
    "api_predictions_total",
    "Total de predicciones",
    ["prediction_label"]
)

# ── Model cache ───────────────────────────────────────────────────────────────
class ModelCache:
    def __init__(self):
        self.model        = None
        self.version      = None
        self.model_name   = None
        self.loaded_at    = None

    def load(self):
        mlflow.set_tracking_uri(MLFLOW_URI)
        model_uri = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
        logger.info(f"Cargando modelo desde {model_uri}")
        self.model      = mlflow.sklearn.load_model(model_uri)
        client          = mlflow.tracking.MlflowClient(tracking_uri=MLFLOW_URI)
        version_info    = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
        self.version    = version_info.version
        self.model_name = MODEL_NAME
        self.loaded_at  = datetime.utcnow()
        logger.info(f"Modelo cargado: {MODEL_NAME} v{self.version}")

cache = ModelCache()

# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    cache.load()
    yield

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Diabetes Readmission API",
    description="API de inferencia MLOps — Pontificia Universidad Javeriana",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Schemas ───────────────────────────────────────────────────────────────────
class PredictRequest(BaseModel):
    time_in_hospital:         int   = Field(..., ge=1, le=14)
    num_lab_procedures:       int   = Field(..., ge=0)
    num_procedures:           int   = Field(..., ge=0)
    num_medications:          int   = Field(..., ge=0)
    number_outpatient:        int   = Field(0,   ge=0)
    number_emergency:         int   = Field(0,   ge=0)
    number_inpatient:         int   = Field(0,   ge=0)
    number_diagnoses:         int   = Field(..., ge=0)
    age_encoded:              int   = Field(..., ge=0, le=9)
    admission_type_encoded:   int   = Field(1,   ge=0)
    discharge_encoded:        int   = Field(1,   ge=0)
    admission_source_encoded: int   = Field(1,   ge=0)
    insulin_encoded:          int   = Field(0,   ge=0)
    change_encoded:           int   = Field(0,   ge=0)
    diabetesmed_encoded:      int   = Field(1,   ge=0)
    a1cresult_encoded:        int   = Field(0,   ge=0)
    max_glu_serum_encoded:    int   = Field(0,   ge=0)
    num_medications_log:      float = Field(0.0, ge=0)
    service_utilization:      int   = Field(0,   ge=0)

class PredictResponse(BaseModel):
    request_id:        str
    prediction:        int
    prediction_label:  str
    probability_class0: float
    probability_class1: float
    model_name:        str
    model_version:     str
    model_alias:       str
    response_time_ms:  float

# ── DB helper ─────────────────────────────────────────────────────────────────
def log_inference(req_id, input_data, prediction, prob0, prob1, response_ms):
    try:
        conn = psycopg2.connect(DB_URL)
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO inference_logs (
                request_id, input_data, prediction, prediction_label,
                probability_class0, probability_class1,
                model_name, model_version, model_alias, response_time_ms, status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'success')
        """, (
            req_id,
            str(input_data),
            prediction,
            "readmitted_early" if prediction == 1 else "not_readmitted_early",
            prob0, prob1,
            cache.model_name, cache.version, MODEL_ALIAS,
            response_ms,
        ))
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
    if not cache.model:
        raise HTTPException(503, "Modelo no cargado")
    return {
        "model_name":    cache.model_name,
        "model_version": cache.version,
        "model_alias":   MODEL_ALIAS,
        "loaded_at":     str(cache.loaded_at),
        "model_type":    type(cache.model).__name__,
    }

@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    if not cache.model:
        raise HTTPException(503, "Modelo no disponible")

    start = time.time()
    req_id = str(uuid.uuid4())

    features = [[
        req.time_in_hospital, req.num_lab_procedures, req.num_procedures,
        req.num_medications, req.number_outpatient, req.number_emergency,
        req.number_inpatient, req.number_diagnoses, req.age_encoded,
        req.admission_type_encoded, req.discharge_encoded,
        req.admission_source_encoded, req.insulin_encoded, req.change_encoded,
        req.diabetesmed_encoded, req.a1cresult_encoded, req.max_glu_serum_encoded,
        req.num_medications_log, req.service_utilization,
    ]]

    prediction = int(cache.model.predict(features)[0])
    proba      = cache.model.predict_proba(features)[0]
    prob0, prob1 = float(proba[0]), float(proba[1])
    elapsed_ms = (time.time() - start) * 1000
    label      = "readmitted_early" if prediction == 1 else "not_readmitted_early"

    # Métricas Prometheus
    REQUEST_COUNT.labels("POST", "/predict", "200").inc()
    REQUEST_LATENCY.labels("/predict").observe(elapsed_ms / 1000)
    PREDICTION_COUNT.labels(label).inc()

    # Log en DB
    log_inference(req_id, req.dict(), prediction, prob0, prob1, elapsed_ms)

    return PredictResponse(
        request_id=req_id,
        prediction=prediction,
        prediction_label=label,
        probability_class0=prob0,
        probability_class1=prob1,
        model_name=cache.model_name,
        model_version=str(cache.version),
        model_alias=MODEL_ALIAS,
        response_time_ms=elapsed_ms,
    )

@app.get("/metrics")
def metrics():
    REQUEST_COUNT.labels("GET", "/metrics", "200").inc()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)