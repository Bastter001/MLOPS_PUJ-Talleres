CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS processed;
CREATE SCHEMA IF NOT EXISTS training;

CREATE TABLE IF NOT EXISTS raw.api_data (
    id SERIAL PRIMARY KEY,
    batch_number INTEGER,
    group_number INTEGER,
    payload JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed.forest_data (
    id SERIAL PRIMARY KEY,
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
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS training.dataset (
    id SERIAL PRIMARY KEY,
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
    cover_type INTEGER
);

