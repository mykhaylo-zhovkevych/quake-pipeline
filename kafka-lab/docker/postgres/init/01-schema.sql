CREATE TABLE quake_event_log (
    ingest_id BIGSERIAL PRIMARY KEY,
    usgs_id VARCHAR(64) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (usgs_id, updated_at)
);


CREATE TABLE quake_current (
    usgs_id VARCHAR(64) PRIMARY KEY,
    magnitude NUMERIC(4,2),
    place TEXT,
    region_key VARCHAR(8),
    lon         NUMERIC(9,5),
    lat         NUMERIC(8,5),
    depth_km    NUMERIC(7,3),
    status      VARCHAR(16),
    occurred_at TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL
);

CREATE INDEX idx_current_region ON quake_current (region_key, occurred_at ASC);
