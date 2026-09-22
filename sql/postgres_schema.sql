-- Canonical warehouse contract shared by city connectors and Superset datasets.
CREATE SCHEMA IF NOT EXISTS energy;

CREATE TABLE IF NOT EXISTS energy.city (
    city_id TEXT PRIMARY KEY,
    city_name TEXT NOT NULL,
    country_code CHAR(2),
    source_type TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS energy.asset (
    asset_id TEXT PRIMARY KEY,
    city_id TEXT NOT NULL REFERENCES energy.city(city_id),
    asset_type TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    attributes JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS energy.energy_observation (
    observation_id BIGSERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL REFERENCES energy.asset(asset_id),
    observed_at TIMESTAMPTZ NOT NULL,
    consumption_kwh NUMERIC,
    demand_kw NUMERIC,
    quality_status TEXT NOT NULL DEFAULT 'unknown',
    source_record_id TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    UNIQUE (asset_id, observed_at)
);

CREATE INDEX IF NOT EXISTS energy_observation_time_idx
    ON energy.energy_observation (observed_at);
