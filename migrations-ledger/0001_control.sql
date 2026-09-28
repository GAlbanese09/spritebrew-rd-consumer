-- spritebrew-ledger: control table (n1-ledger.md 005 section 1, release 1).
-- One row per switch. money_pause gates every money write and every fresh
-- generation in both repos; the readers fail closed, so this row must exist
-- with value '0' before any release 1 deploy. dev_fault is never inserted
-- here: it is a dev-only row written by hand for tests.
CREATE TABLE control (
  key TEXT PRIMARY KEY CHECK (key IN ('money_pause', 'switch_at_ms', 'migration_open', 'dev_fault')),
  value TEXT NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  updated_by TEXT NOT NULL
) STRICT;

INSERT INTO control (key, value, updated_at_ms, updated_by) VALUES
  ('money_pause', '0', 0, 'migration'),
  ('migration_open', '0', 0, 'migration');
