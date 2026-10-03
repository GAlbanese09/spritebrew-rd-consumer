// scripts/ledger-s0-test.mjs
//
// Offline checks of S0's ledger migration, 0002_stripe_refusals.sql
// (n1-release-2-spec.md revision 9, section 3), and of T31's Q4 records:
// 0001 and 0002 applied in an in-memory SQLite (node:sqlite), then the
// spec's own SQL as written (named parameters): S0's admission and
// completion, Q4, George's closing statement and his late review.
// Run from the repo root: .

import { readFileSync } from 'node:fs';
import path from 'node:path';

process.removeAllListeners('warning');
const { DatabaseSync } = await import('node:sqlite');
const ROOT = process.cwd();

// The spec's SQL, copied as written (revision 9: 6.2, section 8).
const ADMIT_SQL = "-- S0: admission, before the first money write\nINSERT INTO money_admissions (admission_id, route, subject_kind, subject_id, user_id, meta_json, admitted_at_ms)\nSELECT :admission, :route, :kind, :subject, :uid, :meta, :now\n WHERE EXISTS (SELECT 1 FROM control WHERE key = 'money_pause' AND value = '0');\nSELECT admission_id FROM money_admissions WHERE admission_id = :admission;\n-- S0: completion, in the finally; kept beside a closure (R7-1)\nUPDATE money_admissions SET completed_at_ms = :now WHERE admission_id = :admission AND completed_at_ms IS NULL;\nSELECT completed_at_ms, closed_at_ms FROM money_admissions WHERE admission_id = :admission;\n";
const Q4_SQL = "-- step 3, Q4: open records, of any pause; the clock is the latest completion or closure (R7-1),\n-- an open record counting by its admission, so NULL means no record exists at all\nSELECT (SELECT COUNT(*) FROM money_admissions\n         WHERE completed_at_ms IS NULL AND closed_at_ms IS NULL)                          AS open_records,\n       (SELECT MAX(CASE WHEN completed_at_ms IS NULL AND closed_at_ms IS NULL THEN admitted_at_ms\n                        ELSE max(COALESCE(completed_at_ms, closed_at_ms), COALESCE(closed_at_ms, completed_at_ms))\n                   END)\n          FROM money_admissions)                                                          AS last_completed_ms;\n";
const CLOSE_SQL = "-- George's closing statement (G); the evidence's shape (R6-17, R7-1):\n-- {\"rows\": [{\"ref\": ..., \"at_ms\": ...}, ...], \"log_lines\": [{\"ref\": <id and text>, \"at_ms\": ...}, ...],\n--  \"end\": {\"kind\": \"log_line\" | \"last_write\", \"ref\": ..., \"at_ms\": ...},\n--  \"reads\": [{\"at_ms\": ..., \"balance\": ...}, {\"at_ms\": ..., \"balance\": ...}], \"other_tx\": [{\"ref\": ..., \"at_ms\": ...}, ...],\n--  \"nothing_new\": true, \"balance_explained\": true}\n-- no customer, in place of the balances: {\"no_customer\": {\"basis\": \"event_evidence\", \"ref\": ...}} for an event,\n--  or {\"no_customer\": {\"basis\": \"balance_key_absent\"}} with \"key_absent\": true, and no balance or a null one, on both reads for an opening\nUPDATE money_admissions\n   SET closed_by = :who, closed_at_ms = :now, close_note = :note, close_evidence_json = :evidence\n WHERE admission_id = :admission AND completed_at_ms IS NULL AND closed_at_ms IS NULL\n   AND NOT EXISTS (\n         SELECT 1 FROM (SELECT 'request' AS list, type, value FROM json_each(:evidence, '$.rows')\n                        UNION ALL SELECT 'request', type, value FROM json_each(:evidence, '$.log_lines')\n                        UNION ALL SELECT 'other', type, value FROM json_each(:evidence, '$.other_tx')) AS x\n          WHERE x.type IS NOT 'object'\n             OR json_type(x.value, '$.ref') IS NOT 'text'\n             OR trim(json_extract(x.value, '$.ref'), ' ' || char(9) || char(10) || char(13)) = ''\n             OR json_type(x.value, '$.at_ms') IS NOT 'integer'\n             -- the request's rows and lines at or before the first read; the other rows between the reads\n             OR (x.list = 'request' AND json_extract(x.value, '$.at_ms') > json_extract(:evidence, '$.reads[0].at_ms'))\n             OR (x.list = 'other' AND json_extract(x.value, '$.at_ms')\n                   NOT BETWEEN json_extract(:evidence, '$.reads[0].at_ms') AND json_extract(:evidence, '$.reads[1].at_ms')));\nSELECT closed_at_ms, closed_by FROM money_admissions WHERE admission_id = :admission;\n";
const REVIEW_SQL = "-- George's review of a completion after his closure (G), before the next switch's step 0\nUPDATE money_admissions\n   SET late_review = :review, late_review_note = :note, late_reviewed_by = :who, late_reviewed_at_ms = :now\n WHERE admission_id = :admission AND closed_at_ms IS NOT NULL AND completed_at_ms IS NOT NULL\n   AND late_review IS NULL;\nSELECT late_review, completed_at_ms, closed_at_ms FROM money_admissions WHERE admission_id = :admission;\n-- step 0: no completion after a closure is waiting for its review\nSELECT COUNT(*) AS late_unreviewed FROM money_admissions\n WHERE closed_at_ms IS NOT NULL AND completed_at_ms IS NOT NULL AND late_review IS NULL;\n";

let pass = 0, fail = 0;
const check = (name, ok) => { if (ok) pass++; else { fail++; console.log('[ledger-s0-test] FAIL', name); } };

function fresh() {
  const db = new DatabaseSync(':memory:');
  for (const f of ['0001_control.sql', '0002_stripe_refusals.sql']) {
    db.exec(readFileSync(path.join(ROOT, 'migrations-ledger', f), 'utf8'));
  }
  return db;
}
// Run a block of statements; answer the last statement's rows.
function run(db, block, params = {}) {
  const stmts = block.split(/;\s*\n/).map((x) => x.trim()).filter((x) => x && !/^(--[^\n]*\n?)+$/.test(x));
  let rows = [];
  for (const st of stmts) {
    const s = db.prepare(st.endsWith(';') ? st : st + ';');
    const names = [...st.matchAll(/:([a-z_]+)/g)].map((m) => m[1]);
    const bound = Object.fromEntries(names.map((n) => [n, params[n] ?? null]));
    if (/^\s*(--[^\n]*\n\s*)*SELECT/i.test(st)) rows = s.all(bound).map((r) => ({ ...r }));
    else s.run(bound);
  }
  return rows;
}
const setPause = (db, v, at) => db.prepare("UPDATE control SET value = ?, updated_at_ms = ?, updated_by = 'test' WHERE key = 'money_pause'").run(v, at);
const admit = (db, id, at, over = {}) => run(db, ADMIT_SQL.split('-- S0: completion')[0],
  { admission: id, route: 'generate', kind: 'job', subject: 'job_' + id, uid: 'user_t', meta: '{"mode":"create","tokenCost":16,"requestId":"gen:x"}', now: at, ...over });
const complete = (db, id, at) => run(db, '-- S0: completion' + ADMIT_SQL.split('-- S0: completion')[1], { admission: id, now: at });
const q4 = (db) => run(db, Q4_SQL)[0];
const D = 86400000;
const evidence = (admittedAt) => JSON.stringify({
  rows: [{ ref: 'token_tx:user_t:1', at_ms: admittedAt + 10 }], log_lines: [{ ref: 'id1 response 202', at_ms: admittedAt + 20 }], other_tx: [],
  end: { kind: 'log_line', ref: 'id1 response 202', at_ms: admittedAt + 20 },
  reads: [{ at_ms: admittedAt + 30, balance: 5 }, { at_ms: admittedAt + 30 + D, balance: 5 }],
  nothing_new: true, balance_explained: true,
});
const closeIt = (db, id, admittedAt, now) => run(db, CLOSE_SQL, { admission: id, who: 'george', now, note: 'checked', evidence: evidence(admittedAt) });

// ── 0002 as written ──
let db = fresh();
const objects = db.prepare("SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name").all().map((r) => r.type + ':' + r.name).join(',');
check('0002 verification: both tables and both indexes exist beside control',
  objects === 'index:money_admissions_open,index:money_admissions_subject,table:control,table:money_admissions,table:stripe_held');
check('0002 is STRICT on both tables', ['stripe_held', 'money_admissions'].every((t) => /STRICT\s*$/.test(db.prepare("SELECT sql FROM sqlite_master WHERE name = ?").get(t).sql)));

// ── S0's admission, as written ──
db = fresh();
let r = admit(db, 'a1', 1000);
check("admission while money_pause is '0' -> its row", r.length === 1 && r[0].admission_id === 'a1');
setPause(db, '1', 2000);
r = admit(db, 'a2', 2001);
check("admission while paused -> zero rows (the paused answer)", r.length === 0);
db.prepare("DELETE FROM control WHERE key = 'money_pause'").run();
check('admission with the pause row absent -> zero rows (fail closed)', admit(db, 'a3', 2002).length === 0);
let bad = false; db = fresh();
try { admit(db, 'a4', 1, { meta: null }); } catch { bad = true; }
check("a job admission without meta_json is refused by 0002's CHECK", bad);

// ── T31: Q4's records ──
db = fresh();
let q = q4(db);
check('Q4 with no record at all -> open 0, clock NULL', q.open_records === 0 && q.last_completed_ms === null);
admit(db, 'o1', 1000);
q = q4(db);
check('Q4 with one open record -> open 1, clock its admission', q.open_records === 1 && q.last_completed_ms === 1000);
complete(db, 'o1', 5000);
q = q4(db);
check('Q4 after its completion -> open 0, clock the completion', q.open_records === 0 && q.last_completed_ms === 5000);
// A request completing before the 15 minutes and one after.
admit(db, 'early', 6000); admit(db, 'late', 6000);
complete(db, 'early', 6000 + 5 * 60000);
q = q4(db);
check('Q4 with one request still open 15 minutes after the pause -> it stops the switch', q.open_records === 1);
complete(db, 'late', 6000 + 20 * 60000);
q = q4(db);
check('...and holds once the late one completes, the clock its completion', q.open_records === 0 && q.last_completed_ms === 6000 + 20 * 60000);
// A failed completion write and a killed request: the record stays open.
admit(db, 'failed', 30 * 60000); admit(db, 'killed', 30 * 60000);
q = q4(db);
check('Q4 with a failed completion and a killed request (no completion) -> both open', q.open_records === 2);
// An open record from an earlier pause still counts.
setPause(db, '1', 40 * 60000); setPause(db, '0', 41 * 60000); setPause(db, '1', 50 * 60000);
check('Q4 counts an open record of any earlier pause', q4(db).open_records === 2);
// George's closing statement on named evidence closes each; the clock takes the closure.
const now1 = 30 * 60000 + 2 * D;
closeIt(db, 'failed', 30 * 60000, now1);
closeIt(db, 'killed', 30 * 60000, now1 + 1);
q = q4(db);
check('Q4 after both closures -> open 0, the clock the latest closure', q.open_records === 0 && q.last_completed_ms === now1 + 1);
// A completion arriving after the closure is kept, moves the clock and waits for George's review.
complete(db, 'failed', now1 + 100);
const rowF = db.prepare("SELECT completed_at_ms, closed_at_ms FROM money_admissions WHERE admission_id = 'failed'").get();
check('a completion after the closure is kept beside it', rowF.completed_at_ms === now1 + 100 && rowF.closed_at_ms === now1);
check('...and moves the clock', q4(db).last_completed_ms === now1 + 100);
let late = run(db, REVIEW_SQL.split('-- step 0')[1] ? '-- step 0' + REVIEW_SQL.split('-- step 0')[1] : REVIEW_SQL, {})[0];
check("step 0's late_unreviewed counts it", late.late_unreviewed === 1);
run(db, REVIEW_SQL.split('-- step 0')[0], { admission: 'failed', review: 'delayed_completion', note: 'the job record shows the end', who: 'george', now: now1 + 200 });
late = run(db, '-- step 0' + REVIEW_SQL.split('-- step 0')[1], {})[0];
check("...until George's review is recorded", late.late_unreviewed === 0);
// The closing statement refuses a closure without its named evidence.
setPause(db, '0', 59 * 60000);
admit(db, 'noev', 60 * 60000);
let refused = false;
try { run(db, CLOSE_SQL, { admission: 'noev', who: 'george', now: 60 * 60000 + 2 * D, note: 'n', evidence: '{}' }); } catch { refused = true; }
check('a closure with no evidence is refused (Q4 stays open)', refused && q4(db).open_records === 1);

console.log('[ledger-s0-test]', pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
