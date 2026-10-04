-- spritebrew-ledger: release 2's tables (n1-release-2-spec.md revision 9,
-- section 3, lines 430 to 653), copied from the spec as written (A9). Built in
-- S1; applied to dev D1 in S1's part B and to production at runbook step 0.
-- Every table stays empty until step 5, except switch_marks, switch_obligations,
-- switch_attempts, switch_drift and backstop_runs (section 3). The last line
-- inserts the switch_at_ms sentinel (D6), so the unpause is one guarded
-- statement (4.15).

CREATE TABLE balances (
  user_id         TEXT PRIMARY KEY CHECK (user_id GLOB 'user_*' OR user_id GLOB 'ledgertest_*'),
  balance         INTEGER NOT NULL,
  created_at_ms   INTEGER NOT NULL,
  updated_at_ms   INTEGER NOT NULL,
  opened_via      TEXT NOT NULL CHECK (opened_via IN ('snapshot', 'signup', 'early_adopter', 'disposable', 'zero_alarm')),
  kv_last_updated TEXT
) STRICT;

CREATE TABLE ledger (
  seq           INTEGER PRIMARY KEY AUTOINCREMENT,
  id            TEXT NOT NULL UNIQUE,
  user_id       TEXT NOT NULL,
  type          TEXT NOT NULL CHECK (type IN ('credit', 'debit', 'opening')),
  amount        INTEGER NOT NULL CHECK (type = 'opening' OR amount > 0),
  reason        TEXT NOT NULL,
  source        TEXT,
  job_id        TEXT,
  style         TEXT,
  mode          TEXT CHECK (mode IS NULL OR mode IN ('create', 'animate')),
  size          INTEGER,
  balance_after INTEGER NOT NULL,
  idem_key      TEXT NOT NULL UNIQUE,
  created_at_ms INTEGER NOT NULL,
  meta_json     TEXT CHECK (meta_json IS NULL OR json_valid(meta_json)),
  CHECK (type <> 'opening' OR balance_after = amount)
) STRICT;
CREATE INDEX ledger_user_seq ON ledger (user_id, seq);
CREATE INDEX ledger_job ON ledger (job_id) WHERE job_id IS NOT NULL;
CREATE INDEX ledger_purchase_pi ON ledger (json_extract(meta_json, '$.payment_intent'))
  WHERE source = 'token_pack_purchase';

CREATE TABLE jobs (
  job_id               TEXT PRIMARY KEY,
  user_id              TEXT NOT NULL,
  mode                 TEXT NOT NULL CHECK (mode IN ('create', 'animate')),
  token_cost           INTEGER CHECK (token_cost IS NULL OR token_cost BETWEEN 1 AND 50),   -- NULL only for kv identity and held rows (A3)
  client_key           TEXT,
  request_hash         TEXT,
  provenance           TEXT NOT NULL CHECK (provenance IN ('d1', 'kv', 'tombstone')),
  state                TEXT NOT NULL CHECK (state IN ('debited', 'enqueued', 'claimed', 'finished')),
  phase                TEXT NOT NULL DEFAULT 'primary' CHECK (phase IN ('primary', 'fallback')),
  claim_id             TEXT,
  claim_attempt        INTEGER,
  claimed_at_ms        INTEGER,
  lease_at_ms          INTEGER,
  released_at_ms       INTEGER,
  submitted_at_ms      INTEGER,
  task_id              TEXT,
  refund_due_code      TEXT,
  hold_reason          TEXT CHECK (hold_reason IS NULL OR hold_reason IN
                         ('contradictory', 'unexplained', 'no_cost', 'index_only', 'carried')),  -- A2: no claim, no canceller; R5-3
  import_json          TEXT CHECK (import_json IS NULL OR json_valid(import_json)),             -- A2: the frozen classification and its evidence
  outcome              TEXT CHECK (outcome IS NULL OR outcome IN
                         ('succeeded', 'rescued', 'refunded', 'refunded_legacy', 'no_record')),
  error_code           TEXT,
  error_message        TEXT,
  refunded_amount      INTEGER CHECK (refunded_amount IS NULL OR refunded_amount > 0),
  artifact             TEXT NOT NULL DEFAULT 'none'
                         CHECK (artifact IN ('none', 'staged', 'published', 'discarded')),
  artifact_meta_json   TEXT CHECK (artifact_meta_json IS NULL OR json_valid(artifact_meta_json)),
  status_written_at_ms INTEGER,
  created_at_ms        INTEGER NOT NULL,
  enqueued_at_ms       INTEGER,
  finished_at_ms       INTEGER,
  CHECK ((state = 'finished') = (finished_at_ms IS NOT NULL)),
  CHECK ((state = 'finished') = (outcome IS NOT NULL)),
  CHECK ((claim_id IS NULL) = (claimed_at_ms IS NULL)),
  CHECK ((claim_id IS NULL) = (lease_at_ms IS NULL)),
  CHECK (provenance <> 'd1' OR (client_key IS NOT NULL AND request_hash IS NOT NULL)),
  CHECK (provenance = 'kv' OR token_cost IS NOT NULL),
  CHECK (provenance <> 'kv' OR finished_at_ms IS NOT NULL OR hold_reason IS NOT NULL OR token_cost IS NOT NULL),
  CHECK (hold_reason IS NULL OR (provenance = 'kv' AND finished_at_ms IS NULL)),
  CHECK (provenance = 'kv' OR import_json IS NULL),
  CHECK (outcome IS NOT 'refunded' OR refunded_amount IS NOT NULL),
  CHECK (artifact <> 'published' OR outcome IN ('succeeded', 'rescued'))
) STRICT;
CREATE INDEX jobs_open ON jobs (state, created_at_ms) WHERE finished_at_ms IS NULL;
CREATE INDEX jobs_repair ON jobs (finished_at_ms)
  WHERE finished_at_ms IS NOT NULL AND (status_written_at_ms IS NULL OR artifact = 'staged');

CREATE TABLE legacy_idem (
  key           TEXT PRIMARY KEY,
  kind          TEXT NOT NULL CHECK (kind IN ('refund_job', 'refund_gen', 'debit_gen', 'signup', 'daily_login',
                                              'email_list', 'earnback', 'stripe_credit', 'stripe_event', 'other')),
  kv_expires_ms INTEGER,
  copied_at_ms  INTEGER NOT NULL,
  keep_until_ms INTEGER NOT NULL
) STRICT;

-- S-1: a pre-switch Stripe event with no evidence and no admission, or a refund or dispute waiting for its purchase
CREATE TABLE stripe_pending (
  event_id       TEXT PRIMARY KEY,
  reason         TEXT NOT NULL CHECK (reason IN ('no_evidence', 'mapping_missing')),
  first_seen_ms  INTEGER NOT NULL,
  disposition    TEXT CHECK (disposition IS NULL OR disposition IN ('apply', 'none')),   -- George's
  evidence_note  TEXT,                -- N14 and R5-15: why nothing moves; no customer identity
  revised_from   TEXT CHECK (revised_from IS NULL OR revised_from = 'apply'),   -- R5-15: George's audited change to 'none'
  decided_by     TEXT,
  decided_at_ms  INTEGER,
  resolved_at_ms INTEGER,
  CHECK ((disposition IS NULL) = (decided_at_ms IS NULL)),
  CHECK (reason = 'no_evidence' OR disposition IS NULL OR (disposition = 'none' AND evidence_note IS NOT NULL)),
  CHECK (revised_from IS NULL OR (disposition = 'none' AND evidence_note IS NOT NULL))
) STRICT;

-- S-1 (b), HQ-8 condition 1: this switch's pause-start cutoff, written once at step 2 and never updated
CREATE TABLE switch_marks (
  name           TEXT PRIMARY KEY CHECK (name IN ('pause_start_ms')),
  value_ms       INTEGER NOT NULL,
  recorded_at_ms INTEGER NOT NULL
) STRICT;

-- 4.14: a purchase's charge recovered by hand from Stripe when the lookup failed
CREATE TABLE charge_recovered (
  stripe_event_id TEXT PRIMARY KEY,
  charge          TEXT NOT NULL,
  recorded_by     TEXT NOT NULL,
  recorded_at_ms  INTEGER NOT NULL
) STRICT;

-- R4-7, R5-3, R5-4: obligations an abort hands back; never deleted by the abort. A 'carry' row waits for the next switch
CREATE TABLE switch_obligations (
  pause_epoch_ms       INTEGER NOT NULL,
  subject_kind         TEXT NOT NULL CHECK (subject_kind IN ('job', 'event')),
  subject_id           TEXT NOT NULL,     -- the job id, or the Stripe event id
  user_id              TEXT,
  mode                 TEXT CHECK (mode IS NULL OR mode IN ('create', 'animate')),
  source               TEXT NOT NULL CHECK (source IN ('hold', 'kept_copy', 'stripe_refusal')),
  evidence_json        TEXT NOT NULL CHECK (json_valid(evidence_json)),   -- as read after the drain, with its fingerprint
  token_cost           INTEGER CHECK (token_cost IS NULL OR token_cost BETWEEN 1 AND 50),
  kv_payable           INTEGER NOT NULL CHECK (kv_payable IN (0, 1)),     -- release 1 would pay its current KV copy (8)
  handed_off_at_ms     INTEGER NOT NULL,
  disposition          TEXT CHECK (disposition IS NULL OR disposition IN ('release1_pays', 'nothing_owed', 'carry')),
  decision_note        TEXT,              -- the evidence for 'nothing_owed', and for a change to 'carry'
  kv_neutralized_at_ms INTEGER,
  found_unpaid_at_ms   INTEGER,           -- a 'release1_pays' row found unpaid, changed to 'carry'
  paid_evidence_json   TEXT CHECK (paid_evidence_json IS NULL OR json_valid(paid_evidence_json)),  -- R6-10: kept once found
  paid_found_at_ms     INTEGER,
  decided_by           TEXT,
  decided_at_ms        INTEGER,
  settled_at_ms        INTEGER,           -- a carried row, once a later switch's D1 settled it
  PRIMARY KEY (pause_epoch_ms, subject_kind, subject_id),
  CHECK ((disposition IS NULL) = (decided_at_ms IS NULL)),
  CHECK (subject_kind <> 'job' OR (user_id IS NOT NULL AND mode IS NOT NULL AND source <> 'stripe_refusal')),
  CHECK (subject_kind <> 'event' OR (source = 'stripe_refusal' AND kv_payable = 0)),
  CHECK (disposition IS NOT 'release1_pays' OR (kv_payable = 1 AND kv_neutralized_at_ms IS NULL)),
  CHECK (disposition IS NOT 'nothing_owed' OR decision_note IS NOT NULL),
  CHECK (disposition IS NULL OR disposition = 'release1_pays' OR kv_payable = 0
         OR kv_neutralized_at_ms IS NOT NULL OR found_unpaid_at_ms IS NOT NULL),
  CHECK (found_unpaid_at_ms IS NULL OR (disposition = 'carry' AND decision_note IS NOT NULL)),
  CHECK (settled_at_ms IS NULL OR disposition = 'carry'),
  CHECK ((paid_found_at_ms IS NULL) = (paid_evidence_json IS NULL))
) STRICT;

-- R6-16 (HQ condition 2 of `2026-10-02-006`): why each switch attempt ended, beside its pause; never deleted by an abort
CREATE TABLE switch_attempts (
  pause_epoch_ms INTEGER PRIMARY KEY,
  ended_at_ms    INTEGER NOT NULL,
  outcome        TEXT NOT NULL CHECK (outcome IN ('switched', 'abort', 'rollback_abort')),
  reason         TEXT NOT NULL CHECK (reason IN ('switched', 'open_admission', 'other')),
  note           TEXT,
  hq_decision    TEXT,            -- the HQ room id George records after two aborts for an open admission record
  recorded_by    TEXT NOT NULL,
  CHECK ((outcome = 'switched') = (reason = 'switched'))
) STRICT;

-- R6-1, R6-18, R7-2: what a backstop run found changed in KV since the snapshot; George disposes of each
CREATE TABLE switch_drift (
  pause_epoch_ms INTEGER NOT NULL,
  kind           TEXT NOT NULL CHECK (kind IN ('balance', 'key', 'job_record')),
  subject        TEXT NOT NULL,   -- the user id, the key, or the job id
  user_id        TEXT,
  snapshot_json  TEXT CHECK (snapshot_json IS NULL OR json_valid(snapshot_json)),   -- what the snapshot read, if anything
  found_json     TEXT NOT NULL CHECK (json_valid(found_json)),
  found_at_ms    INTEGER NOT NULL,
  disposition    TEXT CHECK (disposition IS NULL OR disposition IN ('settled', 'nothing_owed')),
  note           TEXT,
  decided_by     TEXT,
  decided_at_ms  INTEGER,
  last_run_id    TEXT,            -- R7-2: the latest backstop run that found it
  PRIMARY KEY (pause_epoch_ms, kind, subject),
  CHECK ((disposition IS NULL) = (decided_at_ms IS NULL)),
  CHECK (disposition IS NULL OR note IS NOT NULL)
) STRICT;

-- R7-2 (`S2 032` amendment 2): each backstop run with its coverage; a partial or failed run authorizes nothing (4.16)
CREATE TABLE backstop_runs (
  run_id          TEXT PRIMARY KEY,
  pause_epoch_ms  INTEGER NOT NULL,   -- the switch pause's epoch (switch_marks): the snapshot baseline it reads against
  purpose         TEXT NOT NULL CHECK (purpose IN ('step_9b', 'reopen', 'close')),
  run_pause_ms    INTEGER,            -- money_pause.updated_at_ms when the run was recorded paused, read by the INSERT; NULL if open
  recorded_at_ms  INTEGER NOT NULL,   -- when George recorded the run, before starting the scan
  started_at_ms   INTEGER,            -- the scan's own start and finish, from its answers (one clock, the Worker's)
  completed_at_ms INTEGER,
  balances_listed INTEGER, balances_read INTEGER,   -- token_balance: keys
  keys_listed     INTEGER, keys_read     INTEGER,   -- token_idempotency: and webhook:stripe: keys still inside their lifetimes
  jobs_listed     INTEGER, jobs_read     INTEGER,   -- job records still inside their lifetimes
  -- R8-1: each prefix's listing reached its end (no cursor left, no failed page), as the scan reports it
  balances_exhausted INTEGER CHECK (balances_exhausted IN (0, 1)),
  idem_exhausted     INTEGER CHECK (idem_exhausted IN (0, 1)),
  marks_exhausted    INTEGER CHECK (marks_exhausted IN (0, 1)),
  jobs_exhausted     INTEGER CHECK (jobs_exhausted IN (0, 1)),
  -- R8-1: coverage by identity: snapshot customers (D1's balances opened by the snapshot) read and not read
  snapshot_read      INTEGER,
  snapshot_missing   INTEGER,
  drift_found     INTEGER,
  abandoned_at_ms INTEGER,            -- a run whose scan gave no usable answer, closed by George; never complete
  note            TEXT,               -- R9-1: a closed run's late answer, kept as non-authorizing audit material
  complete        INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0, 1)),
  recorded_by     TEXT NOT NULL,
  CHECK (abandoned_at_ms IS NULL OR (completed_at_ms IS NULL AND complete = 0)),
  CHECK (completed_at_ms IS NULL OR (started_at_ms IS NOT NULL AND completed_at_ms >= started_at_ms)),
  CHECK (snapshot_read IS NULL OR snapshot_read <= balances_read),
  CHECK (complete = 0 OR (completed_at_ms IS NOT NULL AND drift_found IS NOT NULL
         AND balances_listed IS NOT NULL AND balances_read IS balances_listed
         AND keys_listed IS NOT NULL AND keys_read IS keys_listed
         AND jobs_listed IS NOT NULL AND jobs_read IS jobs_listed
         AND balances_exhausted IS 1 AND idem_exhausted IS 1 AND marks_exhausted IS 1 AND jobs_exhausted IS 1
         AND snapshot_read IS NOT NULL AND snapshot_missing IS 0))
) STRICT;

-- D6: switch_at_ms exists from the migration with a far-future sentinel, so the unpause is one guarded statement
INSERT INTO control (key, value, updated_at_ms, updated_by) VALUES ('switch_at_ms', '99999999999999', 0, 'migration');
