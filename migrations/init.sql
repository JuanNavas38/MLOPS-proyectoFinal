-- ============================================================================
-- Inicialización de bases de datos — MLOps Proyecto Final (Nivel 4)
-- Dominio: regresión de precios de propiedades inmobiliarias.
-- ============================================================================

-- Backend de metadatos de MLflow (NO mezclar con RAW/CLEAN)
SELECT 'CREATE DATABASE mlflow_backend'
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = 'mlflow_backend'
)\gexec

-- Backend de metadatos de Airflow
SELECT 'CREATE DATABASE airflow_metadata'
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = 'airflow_metadata'
)\gexec

-- Esquemas de datos del dominio viven en la base 'mlops'
\c mlops

-- ── RAW DATA ────────────────────────────────────────────────────────────────
-- Los lotes se almacenan tal como llegan desde la API externa (RF2).
-- 'status_listing' = columna 'status' del dataset (for_sale / ready_to_build).
-- 'status' (sin sufijo) = estado interno de ingestión.
CREATE TABLE IF NOT EXISTS raw_properties (
  id              SERIAL PRIMARY KEY,
  batch_id        VARCHAR(64)  NOT NULL,
  load_timestamp  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  source          VARCHAR(255) NOT NULL,
  row_hash        VARCHAR(64)  NOT NULL,
  status          VARCHAR(20)  NOT NULL DEFAULT 'loaded',
  -- Campos originales del dataset (12 variables)
  brokered_by     VARCHAR(64),
  status_listing  VARCHAR(50),
  price           DOUBLE PRECISION,
  bed             INTEGER,
  bath            INTEGER,
  acre_lot        DOUBLE PRECISION,
  street          VARCHAR(128),
  city            VARCHAR(128),
  state           VARCHAR(64),
  zip_code        VARCHAR(16),
  house_size      DOUBLE PRECISION,
  prev_sold_date  DATE,
  UNIQUE (row_hash)
);

CREATE INDEX IF NOT EXISTS idx_raw_batch  ON raw_properties(batch_id);
CREATE INDEX IF NOT EXISTS idx_raw_status ON raw_properties(status);
CREATE INDEX IF NOT EXISTS idx_raw_ts     ON raw_properties(load_timestamp);

-- ── CLEAN DATA ──────────────────────────────────────────────────────────────
-- Datos transformados y listos para entrenamiento (RF2). Trazables al lote crudo.
-- Las categóricas se guardan como TEXTO limpio; el encoding vive dentro del
-- Pipeline sklearn registrado en MLflow (clave para RF7: la API manda features crudas).
CREATE TABLE IF NOT EXISTS clean_properties (
  id                   SERIAL PRIMARY KEY,
  raw_id               INTEGER REFERENCES raw_properties(id),
  batch_id             VARCHAR(64) NOT NULL,
  processed_timestamp  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Features numéricas
  bed                  INTEGER,
  bath                 INTEGER,
  acre_lot             DOUBLE PRECISION,
  house_size           DOUBLE PRECISION,
  house_age_years      DOUBLE PRECISION,        -- derivada de prev_sold_date
  -- Features categóricas (texto limpio, encoding en el pipeline del modelo)
  status               VARCHAR(50),
  brokered_by          VARCHAR(64),
  street               VARCHAR(128),
  city                 VARCHAR(128),
  state                VARCHAR(64),
  zip_code             VARCHAR(16),
  -- Target
  price                DOUBLE PRECISION NOT NULL,
  log_price            DOUBLE PRECISION,         -- target estabilizado
  -- Split
  dataset_split        VARCHAR(10) DEFAULT 'train'
);

CREATE INDEX IF NOT EXISTS idx_clean_batch ON clean_properties(batch_id);
CREATE INDEX IF NOT EXISTS idx_clean_split ON clean_properties(dataset_split);

-- ── TRAINING AUDIT ──────────────────────────────────────────────────────────
-- Historial por lote: validaciones, decisión de entrenamiento y promoción (RF4/RF9).
-- Esta tabla es la fuente del "Historial" en Streamlit.
CREATE TABLE IF NOT EXISTS training_audit (
  id                  SERIAL PRIMARY KEY,
  batch_id            VARCHAR(64) NOT NULL,
  executed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  n_records_batch     INTEGER,
  n_records_total     INTEGER,
  schema_status       VARCHAR(20),     -- ok | changed | invalid
  quality_status      VARCHAR(20),     -- ok | warning | invalid
  new_categories      JSONB,           -- {columna: [valores nuevos]}
  drift_detected      BOOLEAN DEFAULT FALSE,
  drift_details       JSONB,
  decision            VARCHAR(20),     -- trained | skipped
  decision_reason     TEXT,
  candidate_run_id    VARCHAR(64),
  candidate_metrics   JSONB,           -- {mae, rmse, mape, r2}
  production_metrics  JSONB,
  promoted            BOOLEAN DEFAULT FALSE,
  promotion_reason    TEXT,
  model_name          VARCHAR(100),
  model_version       VARCHAR(50),
  status              VARCHAR(20) DEFAULT 'success'
);

CREATE INDEX IF NOT EXISTS idx_audit_batch ON training_audit(batch_id);
CREATE INDEX IF NOT EXISTS idx_audit_ts    ON training_audit(executed_at);

-- ── INFERENCE LOGS ──────────────────────────────────────────────────────────
-- Cada petición a la API de inferencia deja registro en el dominio RAW (RF8).
CREATE TABLE IF NOT EXISTS inference_logs (
  id                  SERIAL PRIMARY KEY,
  request_id          UUID NOT NULL DEFAULT gen_random_uuid(),
  inference_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  input_data          JSONB NOT NULL,
  predicted_price     DOUBLE PRECISION,
  model_name          VARCHAR(100),
  model_version       VARCHAR(50),
  model_alias         VARCHAR(50),
  response_time_ms    FLOAT,
  status              VARCHAR(20) DEFAULT 'success',
  error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_infer_ts      ON inference_logs(inference_timestamp);
CREATE INDEX IF NOT EXISTS idx_infer_model   ON inference_logs(model_name, model_version);
CREATE INDEX IF NOT EXISTS idx_infer_request ON inference_logs(request_id);
