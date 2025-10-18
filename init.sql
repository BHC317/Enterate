-- init.sql
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.fact_incident (
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    status TEXT NOT NULL,
    city TEXT NOT NULL,
    street TEXT,
    street_number TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    geom GEOGRAPHY(POINT, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(lon, lat), 4326)
    ) STORED,
    start_ts_utc TIMESTAMP WITH TIME ZONE,
    end_ts_utc TIMESTAMP WITH TIME ZONE,
    description TEXT,
    event_id TEXT PRIMARY KEY,
    ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    fingerprint TEXT NOT NULL,
    CONSTRAINT chk_source CHECK (source IN ('gas', 'ayto', 'ide', 'canal')),
    CONSTRAINT chk_category CHECK (category IN ('gas', 'road', 'electricity', 'water')),
    CONSTRAINT chk_status CHECK (status IN ('planned', 'active', '1'))
);

-- Índices para mejor rendimiento
CREATE INDEX IF NOT EXISTS idx_incident_fingerprint ON core.fact_incident(fingerprint);
CREATE INDEX IF NOT EXISTS idx_incident_event_id ON core.fact_incident(event_id);
CREATE INDEX IF NOT EXISTS idx_incident_geom ON core.fact_incident USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_incident_city ON core.fact_incident(city);
CREATE INDEX IF NOT EXISTS idx_incident_source ON core.fact_incident(source);