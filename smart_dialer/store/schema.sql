-- ============================================================
-- SmartDialer — PostgreSQL Schema
-- ============================================================
-- Designed for high-concurrency access from N dialer workers.
-- Key pattern: SELECT … FOR UPDATE SKIP LOCKED for contention-free
-- agent/borrower reservation.
-- ============================================================

-- ---- Campaigns ----
CREATE TABLE IF NOT EXISTS campaigns (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    mode            VARCHAR(20)  NOT NULL DEFAULT 'PROGRESSIVE'
                        CHECK (mode IN ('PROGRESSIVE', 'PREDICTIVE')),
    provider_name   VARCHAR(100) NOT NULL DEFAULT 'reliable',
    max_abandon_rate NUMERIC(5,4) NOT NULL DEFAULT 0.0300,
    target_answer_rate NUMERIC(5,4) NOT NULL DEFAULT 0.5000,
    avg_talk_time_secs INTEGER NOT NULL DEFAULT 120,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---- Agents ----
-- state values: OFFLINE, AVAILABLE, RESERVED, DIALING, CONNECTED, WRAP_UP, PAUSED
CREATE TABLE IF NOT EXISTS agents (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    campaign_id     UUID REFERENCES campaigns(id),
    state           VARCHAR(20)  NOT NULL DEFAULT 'OFFLINE',
    current_call_id UUID,
    worker_id       VARCHAR(100),
    state_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    version         BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agents_available
    ON agents (campaign_id, state_changed_at)
    WHERE state = 'AVAILABLE';

CREATE INDEX IF NOT EXISTS idx_agents_campaign_state
    ON agents (campaign_id, state);

-- ---- Borrowers (call list) ----
CREATE TABLE IF NOT EXISTS borrowers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id     UUID NOT NULL REFERENCES campaigns(id),
    name            VARCHAR(255) NOT NULL,
    phone           VARCHAR(30)  NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 0,
    last_called_at  TIMESTAMPTZ,
    call_attempts   INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 5,
    is_callable     BOOLEAN NOT NULL DEFAULT TRUE,
    locked_by       VARCHAR(100),          -- worker that owns the reservation
    locked_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_borrowers_callable
    ON borrowers (campaign_id, priority DESC, last_called_at NULLS FIRST)
    WHERE is_callable = TRUE AND locked_by IS NULL;

-- ---- Calls ----
-- state values: QUEUED, RESERVED, INITIATED, RINGING, ANSWERED, CONNECTED,
--               COMPLETED, FAILED, CANCELLED
CREATE TABLE IF NOT EXISTS calls (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id     UUID NOT NULL REFERENCES campaigns(id),
    borrower_id     UUID NOT NULL REFERENCES borrowers(id),
    agent_id        UUID REFERENCES agents(id),
    provider_name   VARCHAR(100),
    provider_call_id VARCHAR(255),
    state           VARCHAR(20) NOT NULL DEFAULT 'QUEUED',
    worker_id       VARCHAR(100),
    initiated_at    TIMESTAMPTZ,
    ringing_at      TIMESTAMPTZ,
    answered_at     TIMESTAMPTZ,
    connected_at    TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    failure_reason  TEXT,
    duration_ms     INTEGER,
    version         BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_calls_campaign_state
    ON calls (campaign_id, state);

CREATE INDEX IF NOT EXISTS idx_calls_agent
    ON calls (agent_id)
    WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');

CREATE INDEX IF NOT EXISTS idx_calls_provider
    ON calls (provider_name, provider_call_id);

-- ---- Processed Events (idempotency) ----
-- Every telecom webhook carries a UUID event_id.
-- INSERT … ON CONFLICT DO NOTHING ensures at-most-once processing.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        UUID PRIMARY KEY,
    call_id         UUID NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    provider_name   VARCHAR(100),
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---- Campaign Metrics (sliding window) ----
CREATE TABLE IF NOT EXISTS campaign_metrics (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id     UUID NOT NULL REFERENCES campaigns(id),
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    calls_initiated INTEGER NOT NULL DEFAULT 0,
    calls_answered  INTEGER NOT NULL DEFAULT 0,
    calls_abandoned INTEGER NOT NULL DEFAULT 0,
    calls_failed    INTEGER NOT NULL DEFAULT 0,
    avg_wait_time_ms INTEGER,
    answer_rate     NUMERIC(5,4),
    abandon_rate    NUMERIC(5,4),
    agent_utilization NUMERIC(5,4),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metrics_campaign_window
    ON campaign_metrics (campaign_id, window_end DESC);

-- ---- Worker Heartbeats (crash detection) ----
CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id       VARCHAR(100) PRIMARY KEY,
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    campaigns       UUID[] NOT NULL DEFAULT '{}'
);
