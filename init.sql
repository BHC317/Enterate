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

INSERT INTO core.fact_incident (
    source,
    category,
    status,
    city,
    street,
    street_number,
    lat,
    lon,
    start_ts_utc,
    end_ts_utc,
    description,
    event_id,
    ingested_at_utc,
    fingerprint
) VALUES
-- 1️⃣ Ayuntamiento: Plaza Santa Cristina
(
    'ayto',
    'road',
    '1',
    'Madrid',
    NULL,
    NULL,
    40.4140944039972,
    -3.72740297037982,
    '2025-01-22T07:00:00Z',
    '2025-11-30T17:00:00Z',
    'PLAZA SANTA CRISTINA entre Pso. de Extremadura y calle San Crispin. Corte total y ocupación de la vía. Motivo: REFORMA Y MODERNIZACIÓN DE LA CTRS DE LÍNEA 6 (METRO PUERTA DEL ANGEL)',
    'tmadrid-39507',
    '2025-10-15T18:46:07Z',
    '606e1352fdf3f10d3fd8e5a3195a68bce1e9d0d2'
),

-- 2️⃣ Ayuntamiento: Calle Carmen Bruguera
(
    'ayto',
    'road',
    '1',
    'Madrid',
    'Calle de Carmen Bruguera desde el',
    '28',
    40.3880087701598,
    -3.70381241229599,
    '2025-01-27T07:00:00Z',
    '2025-11-30T17:00:00Z',
    'Calle de Carmen Bruguera desde el nº28 al nº36 entre la calle Marcelo Usera y la calle Mirasierra. Ocupación de la vía pública con cortes de circulación. Motivo: Izado rejillas metálicas.',
    'tmadrid-39508',
    '2025-10-15T18:46:07Z',
    '94ecd3579b0e63179a81c89ee35df609addf2221'
),

-- 3️⃣ Gas: Simulado - Centro
(
    'gas',
    'gas',
    'planned',
    'Madrid',
    'Simulado - Centro',
    NULL,
    40.4153581517141,
    -3.705390019295314,
    NULL,
    NULL,
    NULL,
    'gas-next7-0001',
    '2025-10-15T18:46:07Z',
    '0adc00a4a4259302be7ebb7b47f1185bce9de97e'
),

-- 4️⃣ Gas: Simulado - Villa de Vallecas
(
    'gas',
    'gas',
    'planned',
    'Madrid',
    'Simulado - Villa de Vallecas',
    NULL,
    40.365919188877754,
    -3.6030254481007855,
    NULL,
    NULL,
    NULL,
    'gas-next7-0002',
    '2025-10-15T18:46:07Z',
    'a7911e665f4c1f0a13be31739d98f81d18285a92'
),

-- 5️⃣ i-DE: Cl Velázquez 103
(
    'ide',
    'electricity',
    'planned',
    'Madrid',
    'Cl Velázquez',
    '103',
    40.488625,
    -3.3619526,
    '2025-10-13T04:00:00Z',
    '2025-10-13T04:15:00Z',
    NULL,
    'electricity-next1-0002',
    '2025-10-15T18:46:07Z',
    '136ff5ddf1bfb888a31e84ad9f56e865aa912972'
),

-- 6️⃣ i-DE: Cl Velázquez 105
(
    'ide',
    'electricity',
    'planned',
    'Madrid',
    'Cl Velázquez',
    '105',
    40.4356837,
    -3.6834731,
    '2025-10-13T04:00:00Z',
    '2025-10-13T04:15:00Z',
    NULL,
    'electricity-next2-0002',
    '2025-10-15T18:46:07Z',
    '08efa003e9e4ebcbb760cc3c5e4a9a413aaba5d5'
);