CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS processed;
CREATE SCHEMA IF NOT EXISTS training;

CREATE TABLE IF NOT EXISTS raw.api_data (
    id SERIAL PRIMARY KEY,
    batch_number INTEGER NOT NULL,
    group_number INTEGER NOT NULL,
    payload JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_raw_api_data_batch_group
        UNIQUE (batch_number, group_number)
);

CREATE TABLE IF NOT EXISTS processed.forest_data (
    id SERIAL PRIMARY KEY,

    source_raw_id INTEGER NOT NULL,
    source_row_number INTEGER NOT NULL,
    batch_number INTEGER,
    group_number INTEGER,

    elevation DOUBLE PRECISION,
    aspect DOUBLE PRECISION,
    slope DOUBLE PRECISION,
    horizontal_distance_to_hydrology DOUBLE PRECISION,
    vertical_distance_to_hydrology DOUBLE PRECISION,
    horizontal_distance_to_roadways DOUBLE PRECISION,
    hillshade_9am DOUBLE PRECISION,
    hillshade_noon DOUBLE PRECISION,
    hillshade_3pm DOUBLE PRECISION,
    horizontal_distance_to_fire_points DOUBLE PRECISION,
    wilderness_area VARCHAR(100),
    soil_type VARCHAR(100),
    cover_type INTEGER,

    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_processed_raw
        FOREIGN KEY (source_raw_id)
        REFERENCES raw.api_data(id),

    CONSTRAINT uq_processed_source_row
        UNIQUE (source_raw_id, source_row_number)
);

CREATE TABLE IF NOT EXISTS training.dataset (
    id SERIAL PRIMARY KEY,

    source_processed_id INTEGER NOT NULL,

    elevation DOUBLE PRECISION,
    aspect DOUBLE PRECISION,
    slope DOUBLE PRECISION,
    horizontal_distance_to_hydrology DOUBLE PRECISION,
    vertical_distance_to_hydrology DOUBLE PRECISION,
    horizontal_distance_to_roadways DOUBLE PRECISION,
    hillshade_9am DOUBLE PRECISION,
    hillshade_noon DOUBLE PRECISION,
    hillshade_3pm DOUBLE PRECISION,
    horizontal_distance_to_fire_points DOUBLE PRECISION,

    wilderness_area VARCHAR(100),
    soil_type VARCHAR(100),

    cover_type INTEGER,

    CONSTRAINT fk_training_processed
        FOREIGN KEY (source_processed_id)
        REFERENCES processed.forest_data(id),

    CONSTRAINT uq_training_source
        UNIQUE (source_processed_id)
);


