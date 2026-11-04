
CREATE SCHEMA IF NOT EXISTS diabetes;

CREATE TABLE IF NOT EXISTS diabetes.ingestion_batches (
    batch_id BIGSERIAL PRIMARY KEY,
    source_hash TEXT NOT NULL,
    source_file TEXT NOT NULL,
    first_row INTEGER NOT NULL,
    last_row INTEGER NOT NULL,
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 15000),
    load_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT 'ingested',
    UNIQUE (source_hash, first_row, last_row)
);

CREATE TABLE IF NOT EXISTS diabetes.raw_data (
    raw_id BIGSERIAL PRIMARY KEY,
    source_hash TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    batch_id BIGINT NOT NULL
        REFERENCES diabetes.ingestion_batches(batch_id),
    source_file TEXT NOT NULL,
    load_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    record_status TEXT NOT NULL DEFAULT 'raw',
    payload JSONB NOT NULL,
    UNIQUE (source_hash, source_row)
);

CREATE INDEX IF NOT EXISTS idx_raw_batch
ON diabetes.raw_data(batch_id);

CREATE TABLE IF NOT EXISTS diabetes.clean_stage (
    raw_id BIGINT PRIMARY KEY
        REFERENCES diabetes.raw_data(raw_id),
    batch_id BIGINT NOT NULL
        REFERENCES diabetes.ingestion_batches(batch_id),
    encounter_id TEXT,
    patient_id TEXT,
    target SMALLINT,
    record_status TEXT NOT NULL,
    reason TEXT,
    features JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS diabetes.clean_data (
    raw_id BIGINT PRIMARY KEY
        REFERENCES diabetes.raw_data(raw_id),
    batch_id BIGINT NOT NULL
        REFERENCES diabetes.ingestion_batches(batch_id),
    encounter_id TEXT,
    patient_id TEXT,
    target SMALLINT,
    record_status TEXT NOT NULL,
    reason TEXT,
    features JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_clean_status
ON diabetes.clean_data(record_status);

CREATE INDEX IF NOT EXISTS idx_clean_batch
ON diabetes.clean_data(batch_id);

CREATE TABLE IF NOT EXISTS diabetes.inference_logs (
    request_id UUID PRIMARY KEY,
    inference_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    request_payload JSONB NOT NULL,
    prediction JSONB NOT NULL,
    score DOUBLE PRECISION,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    latency_ms DOUBLE PRECISION NOT NULL
);

