CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS api_logs (
    id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    method TEXT NOT NULL,
    status_code INT NOT NULL,
    latency_ms DOUBLE PRECISION NOT NULL,
    memory_mb DOUBLE PRECISION NOT NULL DEFAULT 0,
    trace_id TEXT,
    error_message TEXT,
    query_plan JSONB,
    PRIMARY KEY (id, timestamp)
);

-- Convert api_logs into a TimescaleDB hypertable partitioned by timestamp
SELECT create_hypertable('api_logs', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_api_logs_service_endpoint_ts
    ON api_logs (service, endpoint, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_api_logs_status_ts
    ON api_logs (status_code, timestamp DESC);

CREATE TABLE IF NOT EXISTS anomaly_incidents (
    id SERIAL PRIMARY KEY,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    service TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    anomaly_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    z_score DOUBLE PRECISION NOT NULL,
    isolation_score DOUBLE PRECISION NOT NULL,
    p95_latency_ms DOUBLE PRECISION NOT NULL,
    error_rate DOUBLE PRECISION NOT NULL,
    metrics_summary JSONB NOT NULL,
    llm_diagnosis JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN'
);

CREATE INDEX IF NOT EXISTS idx_anomaly_incidents_detected_at
    ON anomaly_incidents (detected_at DESC);
