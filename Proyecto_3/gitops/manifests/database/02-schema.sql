\set ON_ERROR_STOP on

BEGIN;

SET ROLE proyecto3_owner;

-- =====================================================
-- 1. ESQUEMAS
-- =====================================================

CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS clean_data;
CREATE SCHEMA IF NOT EXISTS audit;

-- =====================================================
-- 2. RAW DATA: LOTES ORIGINALES
-- =====================================================

CREATE TABLE IF NOT EXISTS raw_data.batches (

    batch_id TEXT PRIMARY KEY,

    dag_run_id TEXT NOT NULL UNIQUE,

    source_group INTEGER NOT NULL DEFAULT 1,

    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    source_batch_date TIMESTAMPTZ,

    raw_payload BYTEA NOT NULL,

    content_type TEXT,

    payload_sha256 VARCHAR(64) NOT NULL UNIQUE,

    record_count INTEGER
        CHECK (record_count >= 0),

    schema_received JSONB,

    ingestion_status VARCHAR(30)
        NOT NULL DEFAULT 'RECEIVED',

    processed_at TIMESTAMPTZ

);

-- =====================================================
-- 3. RAW DATA: INFERENCIAS
-- =====================================================

CREATE TABLE IF NOT EXISTS raw_data.inference_events (

    inference_id UUID PRIMARY KEY
        DEFAULT gen_random_uuid(),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    input_data JSONB NOT NULL,

    prediction DOUBLE PRECISION,

    model_name TEXT,

    model_version TEXT,

    mlflow_run_id TEXT,

    status VARCHAR(30) NOT NULL,

    latency_ms DOUBLE PRECISION,

    error_message TEXT

);

-- =====================================================
-- 4. CLEAN DATA
-- =====================================================

CREATE TABLE IF NOT EXISTS clean_data.properties (

    property_id BIGINT GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    batch_id TEXT NOT NULL
        REFERENCES raw_data.batches(batch_id),

    row_hash VARCHAR(64) NOT NULL UNIQUE,

    brokered_by TEXT,

    status TEXT,

    price DOUBLE PRECISION NOT NULL
        CHECK (price > 0 AND price < 'Infinity'::float8),

    bed DOUBLE PRECISION,

    bath DOUBLE PRECISION,

    acre_lot DOUBLE PRECISION,

    street TEXT,

    city TEXT,

    state TEXT,

    zip_code TEXT,

    house_size DOUBLE PRECISION,

    prev_sold_date DATE,

    transformation_version TEXT NOT NULL DEFAULT 'v1',

    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()

);

-- =====================================================
-- 5. AUDITORIA: HISTORIAL DE ENTRENAMIENTO
-- =====================================================

CREATE TABLE IF NOT EXISTS audit.training_history (

    dag_run_id TEXT PRIMARY KEY,

    batch_id TEXT
        REFERENCES raw_data.batches(batch_id),

    execution_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    records_received INTEGER,

    records_valid INTEGER,

    records_rejected INTEGER,

    accumulated_records INTEGER,

    schema_validation JSONB,

    quality_report JSONB,

    new_categories JSONB,

    drift_report JSONB,

    training_decision VARCHAR(30),

    training_reason TEXT,

    candidate_version TEXT,

    production_version TEXT,

    promotion_decision VARCHAR(30),

    promotion_reason TEXT,

    candidate_metrics JSONB,

    production_metrics JSONB,

    performance_change JSONB,

    mlflow_run_id TEXT,

    git_commit TEXT,

    final_status VARCHAR(30),

    error_message TEXT,

    finished_at TIMESTAMPTZ

);

-- =====================================================
-- 6. AUDITORIA: REPORTES DE VALIDACION
-- =====================================================

CREATE TABLE IF NOT EXISTS audit.validation_reports (

    report_id BIGINT GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    dag_run_id TEXT NOT NULL
        REFERENCES audit.training_history(dag_run_id),

    batch_id TEXT NOT NULL
        REFERENCES raw_data.batches(batch_id),

    validation_type TEXT NOT NULL,

    validation_status TEXT NOT NULL,

    details JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (dag_run_id, batch_id, validation_type)

);

-- =====================================================
-- 7. AUDITORIA: DATASETS UTILIZADOS POR CADA MODELO
-- =====================================================

CREATE TABLE IF NOT EXISTS audit.model_datasets (

    mlflow_run_id TEXT NOT NULL,

    batch_id TEXT NOT NULL
        REFERENCES raw_data.batches(batch_id),

    dataset_hash VARCHAR(64) NOT NULL,

    dataset_role VARCHAR(20) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        mlflow_run_id,
        batch_id,
        dataset_role
    ),

    CHECK (
        dataset_role IN (
            'TRAIN',
            'VALIDATION',
            'TEST'
        )
    )

);

-- =====================================================
-- 8. INDICES
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_clean_properties_batch
ON clean_data.properties(batch_id);

CREATE INDEX IF NOT EXISTS idx_clean_properties_location
ON clean_data.properties(state, city);

CREATE INDEX IF NOT EXISTS idx_training_execution
ON audit.training_history(execution_date DESC);

CREATE INDEX IF NOT EXISTS idx_inference_created
ON raw_data.inference_events(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_inference_model_version
ON raw_data.inference_events(model_version);

CREATE INDEX IF NOT EXISTS idx_validation_run
ON audit.validation_reports(dag_run_id);

-- =====================================================
-- 9. PERMISOS
-- =====================================================

GRANT USAGE ON SCHEMA raw_data, clean_data, audit
TO proyecto3_training;

GRANT SELECT, INSERT
ON raw_data.batches
TO proyecto3_training;

-- Solo permite actualizar metadatos de procesamiento.
-- El payload original no puede ser modificado.

GRANT UPDATE (
    ingestion_status,
    processed_at,
    record_count,
    schema_received,
    source_batch_date
)
ON raw_data.batches
TO proyecto3_training;

GRANT SELECT, INSERT
ON clean_data.properties
TO proyecto3_training;

GRANT SELECT, INSERT, UPDATE
ON ALL TABLES IN SCHEMA audit
TO proyecto3_training;

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA clean_data, audit
TO proyecto3_training;

-- FastAPI registra inferencias.

GRANT USAGE ON SCHEMA raw_data
TO proyecto3_inference;

GRANT INSERT
ON raw_data.inference_events
TO proyecto3_inference;

-- Streamlit consulta historiales.

GRANT USAGE ON SCHEMA raw_data, audit
TO proyecto3_reader;

GRANT SELECT
ON raw_data.inference_events
TO proyecto3_reader;

GRANT SELECT
ON ALL TABLES IN SCHEMA audit
TO proyecto3_reader;

RESET ROLE;

COMMIT;

