-- 0001_events.sql (WD2a, 2026-09-19)
-- The D1 event ledger. Observability, not money: token_tx:* in KV stays the
-- source of truth for balances. Every row is idempotent on dedupe_key.
-- event_json is the untyped escape hatch for new fields; no migration needed
-- to add one. digest_runs is unused until WD2b and exists now so WD2b needs
-- no migration.

CREATE TABLE events (
    event_id             TEXT PRIMARY KEY,
    dedupe_key           TEXT NOT NULL UNIQUE,
    schema_version       INTEGER NOT NULL DEFAULT 1,
    event_name           TEXT NOT NULL,
    level                TEXT NOT NULL CHECK (level IN ('debug','info','warn','error')),
    occurred_at_ms       INTEGER NOT NULL,
    reporting_day        TEXT NOT NULL,
    ingested_at_ms       INTEGER NOT NULL,
    environment          TEXT NOT NULL,
    source_service       TEXT NOT NULL,
    external_event_id    TEXT,
    user_id              TEXT,
    job_id               TEXT,
    request_id           TEXT,
    queue_message_id     TEXT,
    provider             TEXT,
    provider_job_id      TEXT,
    attempt              INTEGER,
    style                TEXT,
    requested_size       TEXT,
    final_size           TEXT,
    outcome              TEXT,
    provider_status      TEXT,
    error_code           TEXT,
    failure_stage        TEXT,
    http_status          INTEGER,
    retryable            INTEGER,
    refund_expected      INTEGER,
    latency_ms           INTEGER,
    queue_wait_ms        INTEGER,
    amount_minor         INTEGER,
    currency             TEXT,
    units_delta          INTEGER,
    caused_by_event_id   TEXT,
    event_json           TEXT NOT NULL,
    event_sha256         TEXT NOT NULL
) STRICT;

CREATE INDEX idx_events_day_name_time ON events(reporting_day, event_name, occurred_at_ms);
CREATE INDEX idx_events_user_day_time ON events(user_id, reporting_day, occurred_at_ms);
CREATE INDEX idx_events_job_time      ON events(job_id, occurred_at_ms);

CREATE TABLE digest_runs (
    reporting_day       TEXT PRIMARY KEY,
    state               TEXT NOT NULL CHECK (state IN ('started','sent','failed')),
    started_at_ms       INTEGER NOT NULL,
    sent_at_ms          INTEGER,
    resend_message_id   TEXT,
    error_code          TEXT
) STRICT;
