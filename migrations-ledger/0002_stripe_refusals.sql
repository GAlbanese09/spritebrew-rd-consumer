-- spritebrew-ledger: S0, release 1 code before the switch (n1-release-2-spec.md
-- revision 9, section 3 and section 11's S0 row). Two tables, copied from the
-- spec as written:
--   stripe_held: a Stripe event the pause refused, written by release 1's
--     webhook before its 503, with release 1's two keys read at refusal (4.13).
--   money_admissions: one row per release 1 HTTP request that writes money,
--     admitted only while money is open and completed in a finally (6.2, 8).
-- George's closing statement and late review write only the columns their
-- CHECKs allow; S0's code writes the admission and the completion only.

-- S-1: a Stripe event the pause refused, recorded by the webhook before its 503
CREATE TABLE stripe_held (
  event_id         TEXT NOT NULL,
  pause_epoch_ms   INTEGER NOT NULL,   -- money_pause.updated_at_ms of the pause that refused it
  r1_keys          TEXT NOT NULL CHECK (r1_keys IN ('absent', 'present', 'unknown')),  -- release 1's two keys at refusal
  event_type       TEXT NOT NULL,
  event_created_ms INTEGER NOT NULL,
  refused_ms       INTEGER NOT NULL,   -- the first refusal in that pause with this r1_keys result
  PRIMARY KEY (event_id, pause_epoch_ms, r1_keys)
) STRICT;

-- R5-1 (`S2 028` amendment 1): one row per release 1 HTTP request that writes money, admitted only while money is open
CREATE TABLE money_admissions (
  admission_id        TEXT PRIMARY KEY,    -- fresh per request; S0 logs it at admission and at the end, with the request's ids and outcome
  route               TEXT NOT NULL CHECK (route IN ('generate', 'stripe_webhook', 'daily_reward', 'email_list', 'opening')),
  subject_kind        TEXT NOT NULL CHECK (subject_kind IN ('job', 'event', 'user')),
  subject_id          TEXT NOT NULL,       -- the job id, the Stripe event id, or the user id
  user_id             TEXT,
  meta_json           TEXT CHECK (meta_json IS NULL OR json_valid(meta_json)),   -- generate: mode, token cost, request id
  admitted_at_ms      INTEGER NOT NULL,
  completed_at_ms     INTEGER,             -- after the request's last money write, in a finally; kept after a closure too (R7-1)
  closed_by           TEXT,                -- R6-17, R7-1: George's closing statement, only on named evidence (8)
  closed_at_ms        INTEGER,
  close_note          TEXT,
  close_evidence_json TEXT CHECK (close_evidence_json IS NULL OR json_valid(close_evidence_json)),
  late_review         TEXT CHECK (late_review IS NULL OR late_review IN ('delayed_completion', 'late_writer')),  -- R7-1
  late_review_note    TEXT,
  late_reviewed_by    TEXT,
  late_reviewed_at_ms INTEGER,
  CHECK (subject_kind <> 'job' OR (user_id IS NOT NULL AND meta_json IS NOT NULL)),
  CHECK ((closed_at_ms IS NULL) = (closed_by IS NULL)),
  CHECK ((late_review IS NULL) = (late_reviewed_at_ms IS NULL)),
  CHECK (late_review IS NULL OR (closed_at_ms IS NOT NULL AND completed_at_ms IS NOT NULL
         AND late_reviewed_by IS NOT NULL AND late_review_note IS NOT NULL
         AND trim(late_review_note, ' ' || char(9) || char(10) || char(13)) <> '')),
  CHECK (closed_at_ms IS NULL OR (
          close_note IS NOT NULL AND trim(close_note, ' ' || char(9) || char(10) || char(13)) <> ''
      AND close_evidence_json IS NOT NULL
      -- the end, named: its kind, a non-empty reference and an integer time; both reads with integer times
      AND COALESCE(json_extract(close_evidence_json, '$.end.kind'), '') IN ('log_line', 'last_write')
      AND json_type(close_evidence_json, '$.end.ref') IS 'text'
      AND trim(json_extract(close_evidence_json, '$.end.ref'), ' ' || char(9) || char(10) || char(13)) <> ''
      AND json_type(close_evidence_json, '$.end.at_ms') IS 'integer'
      AND json_type(close_evidence_json, '$.reads[0].at_ms') IS 'integer'
      AND json_type(close_evidence_json, '$.reads[1].at_ms') IS 'integer'
      -- the order: the admission, the end, the first read; the second read 24 hours after it; then the closure
      AND admitted_at_ms <= json_extract(close_evidence_json, '$.end.at_ms')
      AND json_extract(close_evidence_json, '$.end.at_ms') <= json_extract(close_evidence_json, '$.reads[0].at_ms')
      AND json_extract(close_evidence_json, '$.reads[1].at_ms')
          - json_extract(close_evidence_json, '$.reads[0].at_ms') >= 86400000
      AND closed_at_ms >= json_extract(close_evidence_json, '$.reads[1].at_ms')
      -- the named evidence, as lists; each element's own reference and time is checked by the closing statement (8)
      AND json_type(close_evidence_json, '$.rows') IS 'array'
      AND json_array_length(close_evidence_json, '$.rows') > 0
      AND json_type(close_evidence_json, '$.log_lines') IS 'array'
      AND json_type(close_evidence_json, '$.other_tx') IS 'array'
      AND json_extract(close_evidence_json, '$.nothing_new') IS 1
      -- both balances, explained; or no customer, only where the route allows it
      AND ((json_type(close_evidence_json, '$.reads[0].balance') IS 'integer'
            AND json_type(close_evidence_json, '$.reads[1].balance') IS 'integer'
            AND json_extract(close_evidence_json, '$.balance_explained') IS 1)
           OR (route = 'stripe_webhook' AND user_id IS NULL
               AND json_extract(close_evidence_json, '$.no_customer.basis') IS 'event_evidence'
               AND json_type(close_evidence_json, '$.no_customer.ref') IS 'text'
               AND trim(json_extract(close_evidence_json, '$.no_customer.ref'), ' ' || char(9) || char(10) || char(13)) <> '')
           OR (route = 'opening'
               AND json_extract(close_evidence_json, '$.no_customer.basis') IS 'balance_key_absent'
               AND json_extract(close_evidence_json, '$.reads[0].key_absent') IS 1
               AND json_extract(close_evidence_json, '$.reads[1].key_absent') IS 1
               AND COALESCE(json_type(close_evidence_json, '$.reads[0].balance'), 'null') = 'null'
               AND COALESCE(json_type(close_evidence_json, '$.reads[1].balance'), 'null') = 'null'))))
      -- every term is NULL-safe: a missing field never passes
) STRICT;
CREATE INDEX money_admissions_open ON money_admissions (admitted_at_ms)
  WHERE completed_at_ms IS NULL AND closed_at_ms IS NULL;
CREATE INDEX money_admissions_subject ON money_admissions (subject_kind, subject_id);

