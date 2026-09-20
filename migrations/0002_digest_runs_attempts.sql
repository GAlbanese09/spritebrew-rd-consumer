-- 0002_digest_runs_attempts.sql (WD2b, 2026-09-19)
-- The digest retries a failed send at the next 15-minute slot and stops after
-- 3 attempts per reporting day. 0001 gave digest_runs no place to count them.
ALTER TABLE digest_runs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
