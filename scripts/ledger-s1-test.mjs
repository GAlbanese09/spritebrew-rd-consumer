// scripts/ledger-s1-test.mjs
//
// S1's offline harness (n1-release-2-spec.md revision 9: section 11's S1 row,
// 10.1). Run from the repo root: `node scripts/ledger-s1-test.mjs`.
//
// The spec's SQL runs as written (A9): scripts/ledger-s1-spec-sql.json holds
// every SQL block of the spec and every complete statement in its prose,
// extracted from the room copy with its sha256; when that copy is present at
// ../SpriteBrew/room/n1-release-2-spec.md the harness extracts again and
// compares. Migrations 0001 to 0003 are applied to an in-memory SQLite
// (node:sqlite). Spec statements run with their named parameters; the library
// (src/ledger.ts, bundled with esbuild into dist/.ledger-s1-test, gitignored)
// runs through a D1-shaped adapter: ordered parameters only, integers bound as
// INTEGER, each batch one transaction, as D1's batch() is.
//
// LEDGER_MUTATION='{"target":"lib"|"spec","from":"...","to":"..."}' runs the
// same tests against a mutated library or spec text (the text checks are
// skipped then); a surviving mutation exits 0, a caught one exits 1, and a
// target that is not found exactly once exits 3.
//
// Output: per-test counts and the name of any failing case; no customer data
// exists here (synthetic ledgertest_ ids only).

import { build } from 'esbuild';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

process.removeAllListeners('warning');
const { DatabaseSync } = await import('node:sqlite');
const ROOT = process.cwd();
const OUT = path.join(ROOT, 'dist', '.ledger-s1-test');
const MUT = process.env.LEDGER_MUTATION ? JSON.parse(process.env.LEDGER_MUTATION) : null;
const SPEC_COPY = path.join(ROOT, '..', 'SpriteBrew', 'room', 'n1-release-2-spec.md');

const FIX = JSON.parse(readFileSync(path.join(ROOT, 'scripts', 'ledger-s1-spec-sql.json'), 'utf8'));
const countOf = (hay, needle) => hay.split(needle).length - 1;

if (MUT?.target === 'spec') {
  const all = [...FIX.blocks, ...FIX.inline];
  const n = all.reduce((s, b) => s + countOf(b.text, MUT.from), 0);
  if (n !== 1) { console.log(`[ledger-s1-test] mutation target found ${n} times`); process.exit(3); }
  for (const b of all) b.text = b.text.replace(MUT.from, MUT.to);
}

const mutationPlugin = {
  name: 'mutation',
  setup(b) {
    if (MUT?.target !== 'lib') return;
    b.onLoad({ filter: /src[\\/]ledger\.ts$/ }, (args) => {
      const src = readFileSync(args.path, 'utf8');
      const n = countOf(src, MUT.from);
      if (n !== 1) { console.log(`[ledger-s1-test] mutation target found ${n} times`); process.exit(3); }
      return { contents: src.replace(MUT.from, MUT.to), loader: 'ts' };
    });
  },
};
await build({
  entryPoints: { ledger: path.join(ROOT, 'src/ledger.ts') },
  bundle: true, platform: 'neutral', format: 'esm', outdir: OUT, logLevel: 'error',
  outExtension: { '.js': '.mjs' }, plugins: [mutationPlugin],
});
const L = await import(pathToFileURL(path.join(OUT, 'ledger.mjs')).href + `?v=${Date.now()}`);

// ── Counting ──

const counts = new Map();
let failed = 0;
function check(tag, name, ok) {
  const c = counts.get(tag) ?? { pass: 0, fail: 0 };
  if (ok) c.pass++; else { c.fail++; failed++; console.log(`[ledger-s1-test] FAIL ${tag}: ${name}`); }
  counts.set(tag, c);
}
async function throws(fn) { try { await fn(); return false; } catch { return true; } }

// ── The spec's statements ──

function codePart(l) {
  let q = false;
  for (let k = 0; k < l.length; k++) {
    if (l[k] === "'") q = !q;
    if (!q && l.startsWith('--', k)) return l.slice(0, k);
  }
  return l;
}
/** A block's statements, leading comment lines dropped, as 4.0 counts them. */
function splitBlock(text) {
  const out = []; let acc = [];
  for (const l of text.replace(/\n$/, '').split('\n')) {
    if (!acc.length && (l.trim() === '' || l.trim().startsWith('--'))) continue;
    acc.push(l);
    if (codePart(l).trimEnd().endsWith(';')) { out.push(acc.join('\n')); acc = []; }
  }
  if (acc.length) out.push(acc.join('\n'));
  return out;
}
const block = (line) => {
  const b = FIX.blocks.find((x) => x.line === line);
  if (!b) throw new Error(`no spec block at line ${line}`);
  return splitBlock(b.text);
};
const inline = (line, i = 0) => FIX.inline.filter((x) => x.line === line)[i].text;
const S = (line, i) => block(line)[i];

// ── SQLite, the spec's way and D1's way ──

const MIG = ['0001_control.sql', '0002_stripe_refusals.sql', '0003_ledger.sql'];
function fresh(upTo = 3) {
  const db = new DatabaseSync(':memory:');
  for (const f of MIG.slice(0, upTo)) db.exec(readFileSync(path.join(ROOT, 'migrations-ledger', f), 'utf8'));
  return db;
}
const toBind = (v) => (typeof v === 'number' && Number.isInteger(v) ? BigInt(v) : typeof v === 'boolean' ? (v ? 1n : 0n) : v ?? null);
const isRead = (sql) => /^\s*(?:--[^\n]*\n\s*)*(SELECT|WITH)\b/i.test(sql);

/** A spec statement as written, with named parameters. */
function runStmt(db, sql, params = {}) {
  const names = [...new Set([...stripLits(sql).matchAll(/:([a-z_][a-z0-9_]*)/g)].map((m) => m[1]))];
  const bound = Object.fromEntries(names.map((n) => {
    if (!(n in params)) throw new Error(`harness: :${n} not given`);
    return [n, toBind(params[n])];
  }));
  const st = db.prepare(sql);
  if (isRead(sql)) return { rows: st.all(bound).map((r) => ({ ...r })), changes: 0 };
  const r = st.run(bound);
  return { rows: [], changes: Number(r.changes) };
}
function stripLits(sql) {
  return sql.replace(/'(?:[^']|'')*'/g, "''").replace(/--[^\n]*/g, '');
}
/** Several spec statements as one transaction, as a D1 batch would. */
function runTx(db, stmts, params) {
  db.exec('BEGIN');
  try {
    const out = stmts.map((s) => runStmt(db, s, params));
    db.exec('COMMIT');
    return out;
  } catch (e) { db.exec('ROLLBACK'); throw e; }
}

/** D1's surface over node:sqlite: ordered parameters, one transaction per
 *  batch. `hooks.before(sqls)` / `hooks.after(sqls)` may throw to model an
 *  error before the batch or a lost response after its commit. */
function d1(db, hooks = {}) {
  return {
    prepare(sql) {
      if (/:[a-z_]/.test(stripLits(sql))) throw new Error('D1: named parameters are not supported');
      const stmt = { sql, values: [] };
      stmt.bind = (...v) => { stmt.values = v; return stmt; };
      return stmt;
    },
    async batch(stmts) {
      if (hooks.yield) await new Promise((r) => setImmediate(r));
      const sqls = stmts.map((s) => s.sql);
      hooks.before?.(sqls);
      db.exec('BEGIN');
      let res;
      try {
        res = stmts.map((s) => {
          if (s.values.some((v) => v === undefined)) throw new Error('D1_TYPE_ERROR: undefined');
          const p = db.prepare(s.sql);
          const vals = s.values.map(toBind);
          if (isRead(s.sql)) return { results: p.all(...vals).map((r) => ({ ...r })), meta: { changes: 0 } };
          const r = p.run(...vals);
          return { results: [], meta: { changes: Number(r.changes) } };
        });
        db.exec('COMMIT');
      } catch (e) { db.exec('ROLLBACK'); throw e; }
      hooks.after?.(sqls);
      return res;
    },
  };
}

let T = 1_800_000_000_000;
const MIN = 60_000, HOUR = 3_600_000, DAY = 86_400_000;
let idn = 0;
const ctxOf = (db, extra = {}) => ({ db: d1(db, extra.hooks), now: () => T, newId: () => `id_${++idn}`, ...extra });
const q = (db, sql, ...a) => db.prepare(sql).all(...a.map(toBind)).map((r) => ({ ...r }));
const one = (db, sql, ...a) => q(db, sql, ...a)[0];
const setCtl = (db, key, value) => {
  if (value === undefined) { db.prepare('DELETE FROM control WHERE key = ?').run(key); return; }
  db.prepare("INSERT INTO control (key, value, updated_at_ms, updated_by) VALUES (?, ?, ?, 'test') ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at_ms = excluded.updated_at_ms").run(key, value, toBind(T));
};
const PAUSE_VARIANTS = [['absent', undefined, 'a'], ["'1'", '1', 'o'], ["'x'", 'x', 'x'], ["''", '', 'e']];
/** A hook that throws on its first call only (a later read goes through). */
const once = (msg) => { let done = false; return () => { if (!done) { done = true; throw new Error(msg); } }; };
const balanceOf = (db, uid) => one(db, 'SELECT balance FROM balances WHERE user_id = ?', uid)?.balance ?? null;
const ledgerRows = (db, uid) => q(db, 'SELECT * FROM ledger WHERE user_id = ? ORDER BY seq', uid);
const job = (db, id) => one(db, 'SELECT * FROM jobs WHERE job_id = ?', id);

/** Test setup only: a jobs row in a given state (the spec's batches then act on it). */
function seedJob(db, row) {
  const r = {
    user_id: 'ledgertest_1', mode: 'create', token_cost: 10, client_key: 'ck', request_hash: 'h1', provenance: 'd1',
    state: 'debited', created_at_ms: T, ...row,
  };
  if (r.provenance !== 'd1' && !('client_key' in row)) { r.client_key = null; r.request_hash = null; }
  const cols = Object.keys(r);
  db.prepare(`INSERT INTO jobs (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`).run(...cols.map((c) => toBind(r[c])));
}
const seedLegacy = (db, key, keepUntil, kind = 'other') =>
  db.prepare('INSERT INTO legacy_idem (key, kind, kv_expires_ms, copied_at_ms, keep_until_ms) VALUES (?, ?, NULL, ?, ?)').run(key, kind, toBind(T), toBind(keepUntil));

async function openUser(db, uid, amount, extra) {
  const r = await L.openBalance(ctxOf(db, extra), { uid, amount, reason: 'signup_bonus', source: 'signup', via: 'signup' });
  if (r.outcome !== 'opened') throw new Error(`setup: opening ${uid} answered ${r.outcome}`);
}
/** A d1 generation charged through the library. */
async function charge(db, uid, jobId, cost = 10, hash = 'h1', mode = 'create') {
  const r = await L.generationDebit(ctxOf(db), { uid, job: jobId, cost, mode, ckey: 'ck', hash, size: 64, meta: '{"width":64,"height":64}' });
  if (r.outcome !== 'charged') throw new Error(`setup: debit ${jobId} answered ${r.outcome}`);
  return r;
}
const credit = (db, uid, amount, idem, extra = {}, ctxExtra) => L.movement(ctxOf(db, ctxExtra),
  { uid, type: 'credit', amount, reason: 'token_pack_purchase', source: 'token_pack_purchase', idem, ...extra });

// ════════════════════════════════════════════════════════════════════════
// T0 (A9): the spec's text, and every SQL block as written
// ════════════════════════════════════════════════════════════════════════

check('T0', 'fixture: 46 SQL blocks and 10 prose statements', FIX.blocks.length === 46 && FIX.inline.length === 10);
if (existsSync(SPEC_COPY)) {
  const raw = readFileSync(SPEC_COPY);
  const sha = createHash('sha256').update(raw).digest('hex');
  check('T0', 'fixture: the room copy of the spec is the revision the fixture was taken from', sha === FIX.sha256);
  if (!MUT) {
    const lines = raw.toString('utf8').split('\n');
    const again = [];
    for (let i = 0; i < lines.length; i++) {
      const m = lines[i].match(/^( *)```sql\s*$/);
      if (!m) continue;
      const ind = m[1].length; const body = []; const start = i + 2;
      for (i++; !/^ *```\s*$/.test(lines[i]); i++) body.push(lines[i].slice(0, ind).trim() === '' ? lines[i].slice(ind) : lines[i]);
      again.push({ line: start, text: body.join('\n') + '\n' });
    }
    check('T0', 'fixture: every block equals a fresh extraction from the spec', again.length === FIX.blocks.length
      && again.every((b, k) => b.line === FIX.blocks[k].line && b.text === FIX.blocks[k].text));
    check('T0', 'fixture: every prose statement is on its line in the spec',
      FIX.inline.every((x) => lines[x.line - 1].includes('`' + x.text + '`')));
  }
} else {
  console.log('[ledger-s1-test] note: room copy of the spec not present; fixture used as recorded');
}

if (!MUT) {
  const mig3 = readFileSync(path.join(ROOT, 'migrations-ledger', '0003_ledger.sql'), 'utf8');
  const body = mig3.slice(mig3.indexOf('CREATE TABLE balances'));
  const header = mig3.slice(0, mig3.indexOf('CREATE TABLE balances'));
  check('T0', '0003_ledger.sql: everything after its comment header is section 3 lines 430 to 653, byte for byte',
    body === FIX.blocks.find((b) => b.line === 430).text);
  check('T0', '0003_ledger.sql: the header is comment lines only', header.split('\n').every((l) => l === '' || l.startsWith('--')));
  // The library's statements are the spec's.
  const specStmts = new Set([...FIX.blocks.flatMap((b) => splitBlock(b.text)), ...FIX.inline.map((x) => x.text)]);
  const libNames = Object.keys(L.LEDGER_SQL);
  const notSpec = libNames.filter((n) => !specStmts.has(L.LEDGER_SQL[n]));
  check('T0', `library: all ${libNames.length} statements equal a spec statement as written${notSpec.length ? ' (not: ' + notSpec.join(', ') + ')' : ''}`, notSpec.length === 0);
  const libSet = new Set(Object.values(L.LEDGER_SQL));
  const sec4 = FIX.blocks.filter((b) => b.line >= 696 && b.line <= 1440).flatMap((b) => splitBlock(b.text));
  const missing = sec4.filter((s) => !libSet.has(s));
  check('T0', `library: every statement of 4.1 to 4.16 is in the library (${sec4.length})`, missing.length === 0);
  check('T0', 'library: the three prose statements of 4.11, 4.12 and 4.16 are in it',
    [1110, 1171, 1440].every((l) => libSet.has(inline(l))));
}

// toPositional
{
  const a = L.toPositional("SELECT :id, 'open:' || :uid, ':not' AS x, :uid -- :comment\n, 'it''s :no', :now");
  check('T0', 'toPositional: names in first-appearance order, repeats share a number',
    a.sql === "SELECT ?1, 'open:' || ?2, ':not' AS x, ?2 -- :comment\n, 'it''s :no', ?3" && a.names.join() === 'id,uid,now');
  const r1 = L.toPositional(L.LEDGER_SQL.R1_REFUND);
  check('T0', "toPositional: 'token_idempotency:refund:' inside a literal is not a parameter", !r1.names.includes('refund') && r1.names.includes('fence'));
  check('T0', 'toPositional: no named parameter survives in any library statement',
    Object.values(L.LEDGER_SQL).every((s) => !/:[a-z_]/.test(stripLits(L.toPositional(s).sql))));
}

check('T0', "D1's limits: every library statement under 100,000 bytes with at most 100 bound parameters",
  Object.values(L.LEDGER_SQL).every((x) => Buffer.byteLength(L.toPositional(x).sql) < 100_000 && L.toPositional(x).names.length <= 100));

// Every SQL block and prose statement runs as written, on 0001 to 0003.
{
  const base = {
    id: 'smoke_id', uid: 'ledgertest_smoke', type: 'credit', amount: 5, reason: 'smoke', source: 'smoke', job: 'ledgertest_job_smoke',
    style: null, mode: 'create', size: 64, delta: 5, idem: 'smoke:1', now: T, meta: null, floor: null, legacy1: null, legacy2: null,
    event: 'ledgertest_evt_smoke', via: 'signup', kv_last: null, cost: 10, ckey: 'k', hash: 'h', claim: 'c1', attempt: 1, task: 't1',
    outcome: 'succeeded', fence: 'canceller', code: 'x', msg: null, r1_keys: 'absent', created_ms: 1, disposition: 'apply', note: 'n',
    who: 'george', epoch: 1, to: 'scan', from: '0', cutoff: T, admission: 'a1', route: 'generate', kind: 'job', subject: 'ledgertest_job_smoke',
    evidence: '{}', review: 'delayed_completion', room_id: 'hq-1', run: 'r1', purpose: 'step_9b', scan_started: 1, scan_finished: 2,
    bl: 0, br: 0, kl: 0, kr: 0, jl: 0, jr: 0, drift: 0, bal_done: 1, idem_done: 1, marks_done: 1, jobs_done: 1, snap_read: 0, snap_missing: 0,
    snapshot: null, found: '{}', uids: '[]', keys: '[]', jobs: '[]', subjects: '[]', key: 'k1', kv_exp: null, state: 'claimed', due_code: null,
    hold: null, import_json: null, artifact: 'none', status_at: null, created: T, enqueued: null, finished: null, source_: null,
    kv_payable: 0, neutralized_ms: null, uid_: null,
  };
  const over = {
    1302: { reason: 'no_evidence' }, 1745: { meta: '{}' }, 2038: { outcome: 'abort', reason: 'other' }, 2163: { kind: 'event', source: 'stripe_refusal', disposition: 'carry', cost: null },
    1596: { kind: 'event' }, 1528: { cost: 10 },
  };
  let ran = 0; const bad = [];
  for (const b of FIX.blocks) {
    const db = b.line <= 430 ? new DatabaseSync(':memory:') : fresh();
    if (b.line === 347) db.exec(FIX.blocks[0].text);
    if (b.line === 430) { db.exec(FIX.blocks[0].text); db.exec(FIX.blocks[1].text); }
    try {
      if (b.line <= 430) db.exec(b.text);
      else for (const s of splitBlock(b.text)) runStmt(db, s, { ...base, ...(over[b.line] ?? {}) });
      ran++;
    } catch (e) { bad.push(`${b.line}: ${e.message.slice(0, 80)}`); }
  }
  for (const x of FIX.inline) {
    const db = fresh();
    try { runStmt(db, x.text, base); ran++; } catch (e) { bad.push(`${x.line}: ${e.message.slice(0, 80)}`); }
  }
  check('T0', `every SQL block (46) and prose statement (10) runs as written${bad.length ? ': ' + bad.join(' | ') : ''}`, ran === 56);
  // 0001 and 0002 as applied equal the spec's blocks (schema by schema).
  const a = fresh(2); const s = new DatabaseSync(':memory:'); s.exec(FIX.blocks[0].text); s.exec(FIX.blocks[1].text);
  const schema = (db) => q(db, "SELECT type, name, sql FROM sqlite_master ORDER BY name").map((r) => `${r.type}:${r.name}:${(r.sql ?? '').replace(/\s+/g, ' ')}`).join('\n');
  check('T0', 'migrations 0001 and 0002 as applied equal the spec blocks at lines 333 and 347', schema(a) === schema(s));
  const db3 = fresh();
  check('T0', '0003: thirteen tables beside control, STRICT each',
    q(db3, "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").length === 14
    && q(db3, "SELECT sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").every((r) => /\)\s*STRICT\s*$/.test(r.sql)));
}

// ════════════════════════════════════════════════════════════════════════
// T0: 4.1 Movement, its table in order
// ════════════════════════════════════════════════════════════════════════
{
  let db = fresh();
  await openUser(db, 'ledgertest_1', 100);
  let r = await credit(db, 'ledgertest_1', 500, 'stripe:evt_1', { event: 'ledgertest_evt_1' });
  check('T0', '4.1 applied: the read-back balance, one row, the balance moved', r.outcome === 'applied' && r.balance === 600
    && balanceOf(db, 'ledgertest_1') === 600 && ledgerRows(db, 'ledgertest_1').length === 2);
  r = await credit(db, 'ledgertest_1', 500, 'stripe:evt_1', { event: 'ledgertest_evt_1' });
  check('T0', '4.1 replayed: the same key again (a thrown UNIQUE on idem_key), identity equal, nothing moved',
    r.outcome === 'replayed' && balanceOf(db, 'ledgertest_1') === 600 && ledgerRows(db, 'ledgertest_1').length === 2);
  r = await credit(db, 'ledgertest_1', 499, 'stripe:evt_1', { event: 'ledgertest_evt_1' });
  check('T0', '4.1 error with unique_mismatch: the same key, a different amount', r.outcome === 'error' && r.alarm === 'unique_mismatch' && balanceOf(db, 'ledgertest_1') === 600);
  seedLegacy(db, 'token_idempotency:evt_2', T + DAY, 'stripe_credit');
  r = await credit(db, 'ledgertest_1', 500, 'stripe:evt_2', { legacy1: 'token_idempotency:evt_2', legacy2: 'webhook:stripe:evt_2' });
  check('T0', '4.1 replayed by live legacy evidence (applied before the switch): nothing moved', r.outcome === 'replayed' && balanceOf(db, 'ledgertest_1') === 600);
  seedLegacy(db, 'token_idempotency:evt_3', T - 1, 'stripe_credit');
  r = await credit(db, 'ledgertest_1', 500, 'stripe:evt_3', { legacy1: 'token_idempotency:evt_3', legacy2: 'webhook:stripe:evt_3' });
  check('T0', '4.1 lapsed legacy evidence (keep_until_ms past) does not block: applied', r.outcome === 'applied' && balanceOf(db, 'ledgertest_1') === 1100);
  db.prepare("INSERT INTO stripe_pending (event_id, reason, first_seen_ms, disposition, evidence_note, decided_by, decided_at_ms) VALUES ('ledgertest_evt_4', 'no_evidence', 1, 'none', 'n', 'george', 1)").run();
  r = await credit(db, 'ledgertest_1', 500, 'stripe:ledgertest_evt_4', { event: 'ledgertest_evt_4' });
  check('T0', "4.1 refused by decision: a recorded 'none' governs inside the movement's own statement", r.outcome === 'refused_by_decision' && balanceOf(db, 'ledgertest_1') === 1100);
  r = await credit(db, 'ledgertest_1', 7, 'support:2026-10-04:1', { reason: 'support', source: 'support', event: null });
  check('T0', "4.1 with :event NULL is unaffected by any recorded 'none'", r.outcome === 'applied');
  for (const [label, v, key] of PAUSE_VARIANTS) {
    const before = balanceOf(db, 'ledgertest_1');
    setCtl(db, 'money_pause', v);
    r = await credit(db, 'ledgertest_1', 5, `support:p:${label}`, { reason: 'support', source: 'support' });
    check('T0', `4.1 paused with money_pause ${label}: no row, nothing moved`, r.outcome === 'paused' && balanceOf(db, 'ledgertest_1') === before
      && !one(db, 'SELECT 1 AS x FROM ledger WHERE idem_key = ?', `support:p:${label}`));
  }
  setCtl(db, 'money_pause', '0');
  r = await credit(db, 'ledgertest_nobal', 5, 'support:nb', { reason: 'support', source: 'support' });
  check('T0', '4.1 no balance row: answered no_balance (open, then once more)', r.outcome === 'no_balance');
  r = await L.movement(ctxOf(db), { uid: 'ledgertest_1', type: 'debit', amount: 5000, reason: 'refund_debit', source: 'refund_debit', idem: 'stripe:ledgertest_evt_5', floor: 0, event: 'ledgertest_evt_5' });
  check('T0', '4.1 declined: a debit below a floor (unreachable for release 2, which binds NULL)', r.outcome === 'declined');
  r = await L.movement(ctxOf(db), { uid: 'ledgertest_1', type: 'debit', amount: 5000, reason: 'refund_debit', source: 'refund_debit', idem: 'stripe:ledgertest_evt_6', floor: null, event: 'ledgertest_evt_6' });
  check('T0', '4.1 a Stripe debit with :floor NULL goes negative (the lock signal)', r.outcome === 'applied' && r.balance < 0);
  check('T0', '4.1 a credit with a floor is refused before anything is sent (A6)',
    await throws(() => L.movement(ctxOf(db), { uid: 'ledgertest_1', type: 'credit', amount: 1, reason: 'x', source: 'x', idem: 'x:1', floor: 0 })));
  check('T0', '4.1 a non-integer or non-positive amount is refused before anything is sent',
    await throws(() => credit(db, 'ledgertest_1', 1.5, 'x:2')) && await throws(() => credit(db, 'ledgertest_1', 0, 'x:3')));
  // Reused :id refused, as written: the UNIQUE on ledger.id rolls the batch back.
  const used = ledgerRows(db, 'ledgertest_1')[1].id;
  const before = balanceOf(db, 'ledgertest_1');
  let threw = false;
  try {
    runTx(db, block(701), { id: used, uid: 'ledgertest_1', type: 'credit', amount: 9, reason: 'support', source: 'support', job: null, style: null, mode: null, size: null, delta: 9, idem: 'support:reuse', now: T, meta: null, floor: null, legacy1: null, legacy2: null, event: null });
  } catch (e) { threw = /UNIQUE constraint failed: ledger\.id/.test(e.message); }
  check('T0', 'a reused :id is refused (UNIQUE on ledger.id) and the batch rolls back', threw && balanceOf(db, 'ledgertest_1') === before
    && !one(db, "SELECT 1 AS x FROM ledger WHERE idem_key = 'support:reuse'"));
  // An unrelated UNIQUE is an error, never a replay.
  const uniq = ctxOf(db, { hooks: { before: once('D1_ERROR: UNIQUE constraint failed: balances.user_id: SQLITE_CONSTRAINT') } });
  r = await L.movement(uniq, { uid: 'ledgertest_1', type: 'credit', amount: 500, reason: 'token_pack_purchase', source: 'token_pack_purchase', idem: 'stripe:evt_1' });
  check('T0', 'an unrelated UNIQUE is an error (not a replay, even with the key already applied)', r.outcome === 'error' && !r.alarm);
  // A late failing statement rolls back the earlier ones.
  db = fresh(); await openUser(db, 'ledgertest_2', 100);
  db.exec("CREATE TRIGGER late_fail BEFORE UPDATE ON balances BEGIN SELECT RAISE(ABORT, 'late statement failed'); END");
  r = await credit(db, 'ledgertest_2', 50, 'support:late', { reason: 'support', source: 'support' });
  check('T0', 'a late failing statement (the balance update) rolls back the ledger row: uncertain, read back, nothing applied',
    r.outcome === 'error' && r.uncertain === true && !one(db, "SELECT 1 AS x FROM ledger WHERE idem_key = 'support:late'") && balanceOf(db, 'ledgertest_2') === 100);
  db.exec('DROP TRIGGER late_fail');
  // Uncertain: committed then the response lost, and an error before the batch.
  const lost = ctxOf(db, { hooks: { after: once('network connection lost') } });
  r = await L.movement(lost, { uid: 'ledgertest_2', type: 'credit', amount: 50, reason: 'support', source: 'support', idem: 'support:lost' });
  check('T0', '4.1 committed with its response lost: the read decides, applied by this execution', r.outcome === 'applied' && r.uncertain && balanceOf(db, 'ledgertest_2') === 150);
  const early = ctxOf(db, { hooks: { before: once('D1 overloaded') } });
  r = await L.movement(early, { uid: 'ledgertest_2', type: 'credit', amount: 50, reason: 'support', source: 'support', idem: 'support:early' });
  check('T0', '4.1 an error before the batch: the read finds nothing applied, an error', r.outcome === 'error' && r.uncertain && balanceOf(db, 'ledgertest_2') === 150);
  const both = ctxOf(db, { hooks: { before: () => { throw new Error('D1 overloaded'); } } });
  both.db.batch = async () => { throw new Error('D1 overloaded'); };
  r = await L.movement(both, { uid: 'ledgertest_2', type: 'credit', amount: 50, reason: 'support', source: 'support', idem: 'support:both' });
  check('T0', '4.1 uncertain and the read fails too: error', r.outcome === 'error' && r.uncertain);
}

// ════════════════════════════════════════════════════════════════════════
// T0: 4.2 Opening
// ════════════════════════════════════════════════════════════════════════
{
  const db = fresh();
  let r = await L.openBalance(ctxOf(db), { uid: 'ledgertest_3', amount: 30, reason: 'signup_bonus', source: 'signup', via: 'signup' });
  check('T0', '4.2 opened at :amount, one opening row with balance_after = amount', r.outcome === 'opened' && r.balance === 30
    && one(db, "SELECT amount, balance_after, idem_key FROM ledger WHERE user_id = 'ledgertest_3'").idem_key === 'open:ledgertest_3');
  r = await L.openBalance(ctxOf(db), { uid: 'ledgertest_3', amount: 99, reason: 'signup_bonus', source: 'signup', via: 'signup' });
  check('T0', '4.2 the second opener: already open, uses the balance, no second row', r.outcome === 'already_open' && r.balance === 30
    && q(db, "SELECT 1 FROM ledger WHERE user_id = 'ledgertest_3'").length === 1);
  for (const [label, v, key] of PAUSE_VARIANTS) {
    setCtl(db, 'money_pause', v);
    r = await L.openBalance(ctxOf(db), { uid: `ledgertest_p${key}`, amount: 30, reason: 'signup_bonus', source: 'signup', via: 'signup' });
    check('T0', `4.2 paused with money_pause ${label}: nothing opened`, r.outcome === 'paused' && balanceOf(db, `ledgertest_p${key}`) === null);
  }
  setCtl(db, 'money_pause', '0');
  db.prepare("INSERT INTO balances VALUES ('ledgertest_bal', 7, 1, 1, 'signup', NULL)").run();
  r = await L.openBalance(ctxOf(db), { uid: 'ledgertest_bal', amount: 30, reason: 'signup_bonus', source: 'signup', via: 'signup' });
  check('T0', '4.2 a balance already there (no opening row of its own): already open, no opening written', r.outcome === 'already_open' && r.balance === 7
    && !one(db, "SELECT 1 AS x FROM ledger WHERE user_id = 'ledgertest_bal'"));
  r = await L.openBalance(ctxOf(db), { uid: 'ledgertest_z', amount: 0, reason: 'legacy_signup', source: 'signup', via: 'zero_alarm' });
  check('T0', "4.2 legacy signup evidence without a balance: opened at 0, 'zero_alarm', with its alarm", r.outcome === 'opened' && r.balance === 0 && r.alarm === 'zero_alarm');
  let threw = false;
  try { await L.openBalance(ctxOf(db), { uid: 'someone_else', amount: 0, reason: 'x', source: 'x', via: 'signup' }); } catch { threw = true; }
  check('T0', "4.2 a user id outside 'user_*' and 'ledgertest_*' is refused by balances' CHECK (opened nothing)",
    !threw && balanceOf(db, 'someone_else') === null && !one(db, "SELECT 1 AS x FROM ledger WHERE user_id = 'someone_else'"));
}

// ════════════════════════════════════════════════════════════════════════
// T0: 4.3 Generation debit and the identity read
// ════════════════════════════════════════════════════════════════════════
{
  const db = fresh();
  await openUser(db, 'ledgertest_4', 100);
  let r = await charge(db, 'ledgertest_4', 'ledgertest_job_a', 16);
  check('T0', '4.3 charged by this execution: debit row, job row debited, balance down', r.balance === 84
    && job(db, 'ledgertest_job_a').state === 'debited' && job(db, 'ledgertest_job_a').provenance === 'd1');
  r = await L.generationDebit(ctxOf(db), { uid: 'ledgertest_4', job: 'ledgertest_job_a', cost: 16, mode: 'create', ckey: 'ck', hash: 'h1' });
  check('T0', "4.3 the same job again (thrown UNIQUE on 'debit:'): replay, never enqueued here", r.outcome === 'replay' && balanceOf(db, 'ledgertest_4') === 84);
  r = await L.generationDebit(ctxOf(db), { uid: 'ledgertest_4', job: 'ledgertest_job_a', cost: 16, mode: 'create', ckey: 'ck', hash: 'h2' });
  check('T0', '4.3 the same job, another request hash: 409 (conflict)', r.outcome === 'conflict');
  r = await L.generationDebit(ctxOf(db), { uid: 'ledgertest_other', job: 'ledgertest_job_a', cost: 16, mode: 'create', ckey: 'ck', hash: 'h1' });
  check('T0', '4.3 the same job, another user: 409 (conflict)', r.outcome === 'conflict');
  seedJob(db, { job_id: 'ledgertest_job_kv', user_id: 'ledgertest_4', provenance: 'kv', state: 'finished', outcome: 'succeeded', finished_at_ms: T, token_cost: null });
  const balKv = balanceOf(db, 'ledgertest_4');
  r = await L.generationDebit(ctxOf(db), { uid: 'ledgertest_4', job: 'ledgertest_job_kv', cost: 16, mode: 'create', ckey: 'ck', hash: 'h9' });
  check('T0', "4.3 an imported 'kv' row, user equal: old request (the late jobs.job_id UNIQUE rolls back the debit)",
    r.outcome === 'old_request' && balanceOf(db, 'ledgertest_4') === balKv && !one(db, "SELECT 1 AS x FROM ledger WHERE idem_key = 'debit:ledgertest_job_kv'"));
  seedJob(db, { job_id: 'ledgertest_job_tomb', user_id: 'ledgertest_4', provenance: 'tombstone', state: 'finished', outcome: 'no_record', finished_at_ms: T });
  r = await L.generationDebit(ctxOf(db), { uid: 'ledgertest_4', job: 'ledgertest_job_tomb', cost: 16, mode: 'create', ckey: 'ck', hash: 'h9' });
  check('T0', '4.3 a tombstone, user equal: old request, never charged', r.outcome === 'old_request' && balanceOf(db, 'ledgertest_4') === balKv);
  await openUser(db, 'ledgertest_x', 100);
  r = await L.generationDebit(ctxOf(db), { uid: 'ledgertest_x', job: 'ledgertest_job_kv', cost: 16, mode: 'create', ckey: 'ck', hash: 'h9' });
  check('T0', "4.3 an imported row, another user: 409, the debit rolled back", r.outcome === 'conflict' && balanceOf(db, 'ledgertest_x') === 100);
  for (const [label, v, key] of PAUSE_VARIANTS) {
    setCtl(db, 'money_pause', v);
    r = await L.generationDebit(ctxOf(db), { uid: 'ledgertest_4', job: `ledgertest_job_p${key}`, cost: 5, mode: 'create', ckey: 'ck', hash: 'h1' });
    check('T0', `4.3 paused with money_pause ${label}: 503 not charged, no job row`, r.outcome === 'paused' && !job(db, `ledgertest_job_p${key}`));
  }
  setCtl(db, 'money_pause', '0');
  r = await L.generationDebit(ctxOf(db), { uid: 'ledgertest_nb', job: 'ledgertest_job_nb', cost: 5, mode: 'create', ckey: 'ck', hash: 'h1' });
  check('T0', '4.3 no balance row: open (4.2) and run once more', r.outcome === 'no_balance' && !job(db, 'ledgertest_job_nb'));
  await openUser(db, 'ledgertest_poor', 10);
  r = await L.generationDebit(ctxOf(db), { uid: 'ledgertest_poor', job: 'ledgertest_job_big', cost: 50, mode: 'create', ckey: 'ck', hash: 'h1' });
  check('T0', '4.3 no row, nothing applied: 402 insufficient (the floor at 0)', r.outcome === 'insufficient' && !job(db, 'ledgertest_job_big') && balanceOf(db, 'ledgertest_poor') === 10);
  const early = ctxOf(db, { hooks: { before: once('D1 overloaded') } });
  r = await L.generationDebit(early, { uid: 'ledgertest_4', job: 'ledgertest_job_u', cost: 5, mode: 'create', ckey: 'ck', hash: 'h1' });
  check('T0', '4.3 uncertain and no row: not charged (503)', r.outcome === 'not_charged');
  const lost = ctxOf(db, { hooks: { after: once('response lost') } });
  r = await L.generationDebit(lost, { uid: 'ledgertest_4', job: 'ledgertest_job_l', cost: 5, mode: 'create', ckey: 'ck', hash: 'h1' });
  check('T0', "4.3 uncertain, the identity read's debit_id = :id: charged by this execution", r.outcome === 'charged' && r.uncertain);
  const dead = ctxOf(db); let calls = 0;
  const inner = dead.db.batch;
  dead.db.batch = async (s) => { calls++; if (calls === 1) { await inner(s); throw new Error('response lost'); } throw new Error('read failed'); };
  r = await L.generationDebit(dead, { uid: 'ledgertest_4', job: 'ledgertest_job_d', cost: 5, mode: 'create', ckey: 'ck', hash: 'h1' });
  check('T0', '4.3 uncertain and the identity read fails: unconfirmed (503, refunded if it happened)', r.outcome === 'unconfirmed' && !!job(db, 'ledgertest_job_d'));
  check('T0', '4.3 a cost outside 1 to 50 is refused before anything is sent',
    await throws(() => L.generationDebit(ctxOf(db), { uid: 'ledgertest_4', job: 'j', cost: 51, mode: 'create', ckey: 'c', hash: 'h' })));
  const id = await L.readGenerationIdentity(ctxOf(db), 'ledgertest_job_a');
  check('T0', '4.3 the identity read returns the row and its debit id', id.provenance === 'd1' && id.request_hash === 'h1' && typeof id.debit_id === 'string');
  check('T0', '4.3 the identity read of an unknown job: none', (await L.readGenerationIdentity(ctxOf(db), 'ledgertest_job_none')) === null);
}

// ════════════════════════════════════════════════════════════════════════
// T0: 4.4, 4.6 claims (the claim table), 4.7 owner updates
// ════════════════════════════════════════════════════════════════════════
{
  const db = fresh();
  await openUser(db, 'ledgertest_5', 500);
  await charge(db, 'ledgertest_5', 'ledgertest_job_c');
  check('T0', '4.4 enqueued: debited to enqueued once', await L.markEnqueued(ctxOf(db), 'ledgertest_job_c') && job(db, 'ledgertest_job_c').state === 'enqueued');
  check('T0', '4.4 a second mark changes nothing', !(await L.markEnqueued(ctxOf(db), 'ledgertest_job_c')));
  const broken = ctxOf(db); broken.db.batch = async () => { throw new Error('down'); };
  const errs = []; const orig = console.error; console.error = (m) => errs.push(m);
  const ok = await L.markEnqueued(broken, 'ledgertest_job_c'); console.error = orig;
  check('T0', '4.4 best effort: a failure is logged, never thrown', ok === false && errs.length === 1 && errs[0].includes('enqueued_mark_failed'));

  let r = await L.claimJob(ctxOf(db), 'submit', { job: 'ledgertest_job_c', claim: 'cl1', attempt: 1 });
  check('T0', '4.6 c-submit on a fresh run: won', r.outcome === 'won' && job(db, 'ledgertest_job_c').state === 'claimed');
  r = await L.claimJob(ctxOf(db), 'submit', { job: 'ledgertest_job_c', claim: 'cl2', attempt: 2 });
  check('T0', '4.6 c-submit against a live claim: the row decides', r.outcome === 'row_decides' && job(db, 'ledgertest_job_c').claim_id === 'cl1');
  T += 300_001;
  r = await L.claimJob(ctxOf(db), 'submit', { job: 'ledgertest_job_c', claim: 'cl2', attempt: 2 });
  check('T0', '4.6 c-submit after the 300 s lease: won by the new claim', r.outcome === 'won');
  await charge(db, 'ledgertest_5', 'ledgertest_job_c2');
  for (const [label, v, key] of PAUSE_VARIANTS) {
    setCtl(db, 'money_pause', v);
    r = await L.claimJob(ctxOf(db), 'submit', { job: 'ledgertest_job_c2', claim: 'x', attempt: 1 });
    check('T0', `4.6 c-submit paused with money_pause ${label}`, r.outcome === 'paused' && job(db, 'ledgertest_job_c2').claim_id === null);
  }
  setCtl(db, 'money_pause', '0');
  seedJob(db, { job_id: 'ledgertest_job_t', user_id: 'ledgertest_5', mode: 'animate', task_id: 'task1', submitted_at_ms: T - HOUR, state: 'claimed', claim_id: 'old', claimed_at_ms: T - HOUR, lease_at_ms: T - HOUR });
  for (const ph of ['scan', 'snapshot', 'verify', undefined]) {
    setCtl(db, 'migration_open', ph);
    r = await L.claimJob(ctxOf(db), 'resume', { job: 'ledgertest_job_t', claim: 'r1', attempt: 1 });
    check('T0', `4.6 c-resume refused with the phase ${ph ?? 'row absent'}: migrator phase open`, r.outcome === 'phase_open' && job(db, 'ledgertest_job_t').claim_id === 'old');
  }
  setCtl(db, 'migration_open', '0');
  setCtl(db, 'money_pause', '1');
  r = await L.claimJob(ctxOf(db), 'resume', { job: 'ledgertest_job_t', claim: 'r1', attempt: 1 });
  check('T0', '4.6 c-resume while paused: polling bills nothing, won', r.outcome === 'won');
  setCtl(db, 'money_pause', '0');
  seedJob(db, { job_id: 'ledgertest_job_s', user_id: 'ledgertest_5', artifact: 'staged', state: 'claimed', claim_id: 'o', claimed_at_ms: T, lease_at_ms: T, artifact_meta_json: '{}' });
  r = await L.claimJob(ctxOf(db), 'finalize', { job: 'ledgertest_job_s', claim: 'f1', attempt: 1 });
  check('T0', '4.6 c-finalize against a live owner: owner live', r.outcome === 'owner_live');
  setCtl(db, 'migration_open', 'scan');
  r = await L.claimJob(ctxOf(db), 'finalize', { job: 'ledgertest_job_s', claim: 'f1', attempt: 1 });
  check('T0', '4.6 c-finalize with the phase open: migrator phase open', r.outcome === 'phase_open');
  setCtl(db, 'migration_open', '0');
  T += 300_001;
  for (const ph of ['scan', 'snapshot', 'verify']) {
    setCtl(db, 'migration_open', ph);
    r = await L.claimJob(ctxOf(db), 'finalize', { job: 'ledgertest_job_s', claim: 'f1', attempt: 1 });
    check('T0', `4.6 c-finalize, the owner gone but the phase '${ph}': refused, the claim untouched`, r.outcome === 'phase_open' && job(db, 'ledgertest_job_s').claim_id === 'o');
  }
  setCtl(db, 'migration_open', '0');
  r = await L.claimJob(ctxOf(db), 'finalize', { job: 'ledgertest_job_s', claim: 'f1', attempt: 1 });
  check('T0', '4.6 c-finalize once the owner is gone: won', r.outcome === 'won');
  seedJob(db, { job_id: 'ledgertest_job_h', user_id: 'ledgertest_5', provenance: 'kv', state: 'claimed', hold_reason: 'contradictory', token_cost: null, client_key: null, request_hash: null });
  for (const k of ['submit', 'resume', 'finalize']) {
    r = await L.claimJob(ctxOf(db), k, { job: 'ledgertest_job_h', claim: 'h1', attempt: 1 });
    check('T0', `4.6 ${k} on a held row: held, never run`, r.outcome === 'held');
  }
  r = await L.claimJob(ctxOf(db), 'submit', { job: 'ledgertest_job_none', claim: 'n', attempt: 1 });
  check('T0', '4.6 no row: no_row', r.outcome === 'no_row');
  seedJob(db, { job_id: 'ledgertest_job_sub', user_id: 'ledgertest_5', state: 'claimed', claim_id: 'z', claimed_at_ms: T - HOUR, lease_at_ms: T - HOUR, submitted_at_ms: T - HOUR });
  r = await L.claimJob(ctxOf(db), 'submit', { job: 'ledgertest_job_sub', claim: 'n', attempt: 2 });
  check('T0', '4.6 c-submit never claims a row with a billable submit outstanding', r.outcome === 'row_decides' && job(db, 'ledgertest_job_sub').claim_id === 'z');
}

// 4.7 Owner updates: each kind, and the lost-response table (R3-17).
{
  const devCtx = (db, fault) => { setCtl(db, 'dev_fault', fault); return ctxOf(db, { appEnv: 'dev' }); };
  const kinds = [
    ['submitted', 'create', {}, (j) => j.submitted_at_ms === T],
    ['task', 'animate', { submitted_at_ms: T - 1 }, (j) => j.task_id === 'tk1'],
    ['fallback', 'animate', {}, (j) => j.phase === 'fallback' && j.submitted_at_ms === T],
    ['release_create', 'create', {}, (j) => j.released_at_ms === T && j.submitted_at_ms === null],
    ['release_animate', 'animate', { task_id: 'tk0', submitted_at_ms: T - 1 }, (j) => j.released_at_ms === T],
    ['stage', 'create', {}, (j) => j.artifact === 'staged' && j.lease_at_ms === T],
  ];
  const scope = (k) => (k.startsWith('release') ? 'release' : k);
  for (const [kind, mode, extra, marker] of kinds) {
    const db = fresh();
    const seed = (id) => seedJob(db, { job_id: id, mode, state: 'claimed', claim_id: 'own', claimed_at_ms: T - 1, lease_at_ms: T - 1, ...extra });
    seed('ledgertest_j1');
    let r = await L.ownerUpdate(ctxOf(db), kind, { job: 'ledgertest_j1', claim: 'own', task: 'tk1', meta: '{"createdAt":1}' });
    check('T0', `4.7 ${kind}: committed, its marker set`, r === 'committed' && marker(job(db, 'ledgertest_j1')));
    seed('ledgertest_j2');
    r = await L.ownerUpdate(ctxOf(db), kind, { job: 'ledgertest_j2', claim: 'thief', task: 'tk1', meta: '{}' });
    check('T0', `4.7 ${kind}: changes 0 is ownership lost`, r === 'ownership_lost');
    seed('ledgertest_j3');
    r = await L.ownerUpdate(devCtx(db, `batch_response_lost:${scope(kind)}`), kind, { job: 'ledgertest_j3', claim: 'own', task: 'tk1', meta: '{}' });
    check('T0', `4.7 ${kind} committed with its response lost (R3-17): the owner read finds this execution's marker, committed`, r === 'committed' && marker(job(db, 'ledgertest_j3')));
    seed('ledgertest_j4');
    let n = 0; const c = ctxOf(db, { hooks: { before: () => { if (++n === 1) throw new Error('D1 overloaded'); } } });
    r = await L.ownerUpdate(c, kind, { job: 'ledgertest_j4', claim: 'own', task: 'tk1', meta: '{}' });
    const j4 = job(db, 'ledgertest_j4');
    check('T0', `4.7 ${kind} not committed: run once more with a fresh :now, then committed`, r === 'committed' && n === 4
      && (kind === 'task' ? j4.task_id === 'tk1' : kind === 'stage' ? j4.artifact === 'staged' : kind.startsWith('release') ? j4.released_at_ms === T + 1 : j4.submitted_at_ms === T + 1));
    seed('ledgertest_j5');
    r = await L.ownerUpdate(devCtx(db, `batch_throw_before:${scope(kind)}`), kind, { job: 'ledgertest_j5', claim: 'own', task: 'tk1', meta: '{}' });
    check('T0', `4.7 ${kind}: a second uncertain answer retries the message, nothing committed`, r === 'retry_message' && !marker(job(db, 'ledgertest_j5')));
    seed('ledgertest_j6'); db.prepare("UPDATE jobs SET claim_id = 'other' WHERE job_id = 'ledgertest_j6'").run();
    r = await L.ownerUpdate(devCtx(db, `batch_throw_before:${scope(kind)}`), kind, { job: 'ledgertest_j6', claim: 'own', task: 'tk1', meta: '{}' });
    check('T0', `4.7 ${kind}: an error, and the read shows another claim: ownership lost`, r === 'ownership_lost');
  }
  {
    const db = fresh();
    seedJob(db, { job_id: 'ledgertest_om', state: 'claimed', claim_id: 'own', claimed_at_ms: T - 10, lease_at_ms: T - 10, submitted_at_ms: T - 5 });
    const r = await L.ownerUpdate(devCtx(db, 'batch_throw_before:submitted'), 'submitted', { job: 'ledgertest_om', claim: 'own' });
    check('T0', "4.7 the owner read: a submitted marker from an earlier execution is not this one's (never 'committed', no RD call)", r === 'retry_message');
  }
  // success (A4)
  const db = fresh();
  const seed = (id, extra = {}) => seedJob(db, { job_id: id, state: 'claimed', claim_id: 'own', claimed_at_ms: T, lease_at_ms: T, artifact: 'staged', artifact_meta_json: '{}', ...extra });
  seed('ledgertest_s1');
  check('T0', '4.7 success: succeeded', (await L.successUpdate(ctxOf(db), { job: 'ledgertest_s1', claim: 'own', outcome: 'succeeded' })) === 'succeeded'
    && job(db, 'ledgertest_s1').state === 'finished');
  for (const [label, v, key] of PAUSE_VARIANTS) {
    setCtl(db, 'money_pause', v);
    seed(`ledgertest_sp${key}`);
    const r = await L.successUpdate(ctxOf(db), { job: `ledgertest_sp${key}`, claim: 'own', outcome: 'succeeded' });
    check('T0', `4.7 success paused with money_pause ${label}: paused, the row still staged and unfinished`, r === 'paused' && job(db, `ledgertest_sp${key}`).finished_at_ms === null);
    seed(`ledgertest_sl${key}`, { claim_id: 'other' });
    const r2 = await L.successUpdate(ctxOf(db), { job: `ledgertest_sl${key}`, claim: 'own', outcome: 'succeeded' });
    check('T0', `4.7 success paused (${label}) against ownership lost (A4): ownership lost, not paused`, r2 === 'ownership_lost');
  }
  setCtl(db, 'money_pause', '0');
  seed('ledgertest_sother', { claim_id: 'other' });
  check('T0', '4.7 success with money open on a lost claim: ownership lost, the row not finished',
    (await L.successUpdate(ctxOf(db), { job: 'ledgertest_sother', claim: 'own', outcome: 'succeeded' })) === 'ownership_lost' && job(db, 'ledgertest_sother').finished_at_ms === null);
  seed('ledgertest_srel', { released_at_ms: T });
  check('T0', '4.7 success on a released claim: ownership lost', (await L.successUpdate(ctxOf(db), { job: 'ledgertest_srel', claim: 'own', outcome: 'succeeded' })) === 'ownership_lost');
  seed('ledgertest_slost');
  const lost = ctxOf(db, { hooks: { after: once('lost') } });
  check('T0', '4.7 success committed, response lost: the read-back says succeeded', (await L.successUpdate(lost, { job: 'ledgertest_slost', claim: 'own', outcome: 'rescued' })) === 'succeeded');
}

// ════════════════════════════════════════════════════════════════════════
// T0: 4.8 Refund and finish (r5's table), the tombstone; T3b
// ════════════════════════════════════════════════════════════════════════
const refund = (db, f, extra) => L.refundAndFinish(ctxOf(db, extra), { code: 'test_code', msg: 'm', ...f });
{
  let db = fresh();
  await openUser(db, 'ledgertest_6', 100);
  await charge(db, 'ledgertest_6', 'ledgertest_r1', 16);
  db.prepare("UPDATE jobs SET state = 'claimed', claim_id = 'own', claimed_at_ms = ?, lease_at_ms = ? WHERE job_id = 'ledgertest_r1'").run(toBind(T), toBind(T));
  let r = await refund(db, { job: 'ledgertest_r1', fence: 'owner', claim: 'own' });
  check('T0', '4.8 refunded: the debit amount back, the row finished refunded with that amount', r.outcome === 'refunded' && r.amount === 16
    && balanceOf(db, 'ledgertest_6') === 100 && job(db, 'ledgertest_r1').outcome === 'refunded' && job(db, 'ledgertest_r1').refunded_amount === 16);
  r = await refund(db, { job: 'ledgertest_r1', fence: 'owner', claim: 'own' });
  check('T0', '4.8 already finished: nothing moved', r.outcome === 'already_finished' && balanceOf(db, 'ledgertest_6') === 100);
  await charge(db, 'ledgertest_6', 'ledgertest_r2', 16);
  seedLegacy(db, 'token_idempotency:refund:ledgertest_r2', T + DAY, 'refund_job');
  r = await refund(db, { job: 'ledgertest_r2', fence: 'recovery' });
  check('T0', '4.8 r4: refunded before the switch (live legacy key): finished refunded_legacy, nothing moved',
    r.outcome === 'already_refunded_legacy' && job(db, 'ledgertest_r2').outcome === 'refunded_legacy' && balanceOf(db, 'ledgertest_6') === 84);
  seedJob(db, { job_id: 'ledgertest_r3', user_id: 'ledgertest_6', provenance: 'kv', state: 'claimed', hold_reason: 'unexplained', token_cost: 10, client_key: null, request_hash: null, created_at_ms: T - DAY });
  for (const fence of ['canceller', 'recovery', 'pages']) {
    r = await refund(db, { job: 'ledgertest_r3', fence });
    check('T0', `4.8 held ('${fence}', past every floor): nothing moved, never paid`, r.outcome === 'held' && balanceOf(db, 'ledgertest_6') === 84 && job(db, 'ledgertest_r3').finished_at_ms === null);
  }
  await charge(db, 'ledgertest_6', 'ledgertest_r4', 5);
  db.prepare("UPDATE jobs SET state = 'claimed', claim_id = 'other', claimed_at_ms = ?, lease_at_ms = ? WHERE job_id = 'ledgertest_r4'").run(toBind(T), toBind(T));
  r = await refund(db, { job: 'ledgertest_r4', fence: 'owner', claim: 'own' });
  check('T0', "4.8 'owner' with another claim: ownership lost", r.outcome === 'ownership_lost' && balanceOf(db, 'ledgertest_6') === 79);
  db.prepare("UPDATE jobs SET claim_id = 'own', released_at_ms = ? WHERE job_id = 'ledgertest_r4'").run(toBind(T));
  r = await refund(db, { job: 'ledgertest_r4', fence: 'owner', claim: 'own' });
  check('T0', "4.8 'owner' after its own release: ownership lost", r.outcome === 'ownership_lost');
  await charge(db, 'ledgertest_6', 'ledgertest_r5', 5);
  db.prepare("UPDATE jobs SET state = 'claimed', claim_id = 'o', claimed_at_ms = ?, lease_at_ms = ?, artifact = 'staged', artifact_meta_json = '{}' WHERE job_id = 'ledgertest_r5'").run(toBind(T - DAY), toBind(T - DAY));
  r = await refund(db, { job: 'ledgertest_r5', fence: 'canceller' });
  check('T0', "4.8 a staged row, canceller: staged result, never refunded (A4)", r.outcome === 'staged_result' && job(db, 'ledgertest_r5').finished_at_ms === null && balanceOf(db, 'ledgertest_6') === 74);
  await charge(db, 'ledgertest_6', 'ledgertest_r6', 5);
  db.prepare("UPDATE jobs SET created_at_ms = ? WHERE job_id = 'ledgertest_r6'").run(toBind(T - DAY));
  for (const fence of ['canceller', 'recovery']) {
    for (const [label, v, key] of PAUSE_VARIANTS) {
      setCtl(db, 'money_pause', v);
      r = await refund(db, { job: 'ledgertest_r6', fence });
      check('T0', `4.8 '${fence}' paused with money_pause ${label}: the unfinished row is the kept debt`, r.outcome === 'paused' && job(db, 'ledgertest_r6').finished_at_ms === null);
    }
    setCtl(db, 'money_pause', '0');
  }
  setCtl(db, 'money_pause', '1');
  r = await refund(db, { job: 'ledgertest_r6', fence: 'pages' });
  check('T0', "4.8 'pages' omits the pause predicate (P17): refunded while paused, phase '0'", r.outcome === 'refunded');
  setCtl(db, 'money_pause', '0');
  await charge(db, 'ledgertest_6', 'ledgertest_r7', 5);
  for (const ph of ['scan', undefined]) {
    setCtl(db, 'migration_open', ph);
    r = await refund(db, { job: 'ledgertest_r7', fence: 'pages' });
    check('T0', `4.8 'pages' with the phase ${ph ?? 'row absent'}: paused (the migrator phase)`, r.outcome === 'paused' && job(db, 'ledgertest_r7').finished_at_ms === null);
  }
  setCtl(db, 'migration_open', '0');
  db.prepare("UPDATE balances SET balance = balance WHERE user_id = 'ledgertest_6'").run();
  // corruption (T3b below adds the mismatched cases)
  seedJob(db, { job_id: 'ledgertest_r8', user_id: 'ledgertest_6' });
  r = await refund(db, { job: 'ledgertest_r8', fence: 'recovery' });
  check('T0', "4.8 a 'd1' row with no debit: corruption, alarm debit_missing, no refund", r.outcome === 'corruption' && r.alarm === 'debit_missing' && job(db, 'ledgertest_r8').finished_at_ms === null);
  // balance NULL: an imported row of a user with no balance
  seedJob(db, { job_id: 'ledgertest_r9', user_id: 'ledgertest_nob', provenance: 'kv', state: 'claimed', token_cost: 7, client_key: null, request_hash: null });
  r = await refund(db, { job: 'ledgertest_r9', fence: 'recovery' });
  check('T0', '4.8 no balance row: open at 0 with zero_alarm (4.2) and run once more', r.outcome === 'no_balance');
  await L.openBalance(ctxOf(db), { uid: 'ledgertest_nob', amount: 0, reason: 'refund_no_balance', source: 'refund', via: 'zero_alarm' });
  r = await refund(db, { job: 'ledgertest_r9', fence: 'recovery' });
  check('T0', "4.8 then refunded: a 'kv' row pays its token_cost", r.outcome === 'refunded' && r.amount === 7 && balanceOf(db, 'ledgertest_nob') === 7);
  await charge(db, 'ledgertest_6', 'ledgertest_r10', 5);
  db.prepare("UPDATE jobs SET state = 'claimed', claim_id = 'live', claimed_at_ms = ?, lease_at_ms = ? WHERE job_id = 'ledgertest_r10'").run(toBind(T), toBind(T));
  for (const fence of ['canceller', 'recovery', 'pages']) {
    r = await refund(db, { job: 'ledgertest_r10', fence });
    check('T0', `4.8 '${fence}' against a live owner: live owner, nothing moved`, r.outcome === 'live_owner');
  }
  db.prepare("UPDATE jobs SET lease_at_ms = ?, claimed_at_ms = ? WHERE job_id = 'ledgertest_r10'").run(toBind(T - 400_000), toBind(T - 400_000));
  r = await refund(db, { job: 'ledgertest_r10', fence: 'canceller' });
  check('T0', "4.8 'canceller' with a stale lease but under the 20-minute floor: live owner", r.outcome === 'live_owner');
  db.prepare("UPDATE jobs SET lease_at_ms = ?, claimed_at_ms = ? WHERE job_id = 'ledgertest_r10'").run(toBind(T - 1_200_001), toBind(T - 1_200_001));
  r = await refund(db, { job: 'ledgertest_r10', fence: 'canceller' });
  check('T0', "4.8 'canceller' past the floor: refunded", r.outcome === 'refunded');
  seedJob(db, { job_id: 'ledgertest_rdone', user_id: 'ledgertest_6', provenance: 'kv', state: 'finished', outcome: 'succeeded', finished_at_ms: T - DAY, token_cost: 5, client_key: null, request_hash: null, created_at_ms: T - DAY });
  const balDone = balanceOf(db, 'ledgertest_6');
  r = await refund(db, { job: 'ledgertest_rdone', fence: 'canceller' });
  check('T0', '4.8 a finished success (no refund row of its own): already finished, never refunded', r.outcome === 'already_finished' && balanceOf(db, 'ledgertest_6') === balDone
    && !one(db, "SELECT 1 AS x FROM ledger WHERE idem_key = 'refund:ledgertest_rdone'"));
  r = await refund(db, { job: 'ledgertest_none', fence: 'canceller' });
  check('T0', '4.8 no row: no record, alarm no_record, no refund', r.outcome === 'no_record' && r.alarm === 'no_record');
  check('T0', "4.8 the 'owner' fence without its claim is refused before anything is sent", await throws(() => refund(db, { job: 'ledgertest_r10', fence: 'owner' })));
  await charge(db, 'ledgertest_6', 'ledgertest_r11', 5);
  const lost = { hooks: { after: once('lost') } };
  r = await refund(db, { job: 'ledgertest_r11', fence: 'recovery' }, lost);
  check('T0', '4.8 committed with the response lost: r5 read alone says refunded (this :id)', r.outcome === 'refunded' && r.uncertain);
  // The tombstone and its table.
  db = fresh();
  let t = await L.tombstone(ctxOf(db), { job: 'ledgertest_t1', uid: 'ledgertest_7', mode: 'create', cost: 10, code: 'no_record' });
  check('T0', '4.8 tombstone: tombstoned, with its alarm', t.outcome === 'tombstoned' && t.alarm === 'no_record' && job(db, 'ledgertest_t1').outcome === 'no_record');
  T += 1;
  t = await L.tombstone(ctxOf(db), { job: 'ledgertest_t1', uid: 'ledgertest_7', mode: 'create', cost: 10, code: 'no_record' });
  check('T0', '4.8 tombstone again (after a lost reply): row exists, the deduped alarm again', t.outcome === 'row_exists' && t.alarm === 'no_record');
  seedJob(db, { job_id: 'ledgertest_t2', user_id: 'ledgertest_7' });
  t = await L.tombstone(ctxOf(db), { job: 'ledgertest_t2', uid: 'ledgertest_7', mode: 'create', cost: 10, code: 'no_record' });
  check('T0', '4.8 tombstone over a real row: row exists, the row decides, no alarm', t.outcome === 'row_exists' && !t.alarm && job(db, 'ledgertest_t2').provenance === 'd1');
  for (const [label, v, key] of PAUSE_VARIANTS) {
    setCtl(db, 'money_pause', v);
    t = await L.tombstone(ctxOf(db), { job: `ledgertest_t3${key}`, uid: 'ledgertest_7', mode: 'create', cost: 10, code: 'no_record' });
    check('T0', `4.8 tombstone paused with money_pause ${label}: no row (read-back: paused)`, t.outcome === 'paused' && !job(db, `ledgertest_t3${key}`));
  }
}

// T3b: refund with an absent or mismatched debit: no refund, debit_missing.
{
  const db = fresh();
  await openUser(db, 'ledgertest_8', 100);
  await openUser(db, 'ledgertest_9', 100);
  seedJob(db, { job_id: 'ledgertest_b1', user_id: 'ledgertest_8' });
  seedJob(db, { job_id: 'ledgertest_b2', user_id: 'ledgertest_8' });
  seedJob(db, { job_id: 'ledgertest_b3', user_id: 'ledgertest_8' });
  // a debit row on the job's key but for another user, and one for another job id
  db.prepare("INSERT INTO ledger (id, user_id, type, amount, reason, source, job_id, balance_after, idem_key, created_at_ms) VALUES ('m1', 'ledgertest_9', 'debit', 10, 'generation', 'generation', 'ledgertest_b2', 90, 'debit:ledgertest_b2', 1)").run();
  db.prepare("INSERT INTO ledger (id, user_id, type, amount, reason, source, job_id, balance_after, idem_key, created_at_ms) VALUES ('m2', 'ledgertest_8', 'debit', 10, 'generation', 'generation', 'ledgertest_other', 90, 'debit:ledgertest_b3', 1)").run();
  for (const [jobId, what] of [['ledgertest_b1', 'absent'], ['ledgertest_b2', "another user's"], ['ledgertest_b3', "another job's"]]) {
    for (const fence of ['recovery', 'canceller', 'pages']) {
      if (fence === 'canceller') db.prepare('UPDATE jobs SET created_at_ms = ? WHERE job_id = ?').run(toBind(T - DAY), jobId);
      const r = await refund(db, { job: jobId, fence });
      check('T3b', `${what} debit, fence '${fence}': corruption, debit_missing, no refund`, r.outcome === 'corruption' && r.alarm === 'debit_missing'
        && balanceOf(db, 'ledgertest_8') === 100 && !one(db, 'SELECT 1 AS x FROM ledger WHERE idem_key = ?', `refund:${jobId}`) && job(db, jobId).finished_at_ms === null);
    }
  }
  db.prepare("UPDATE jobs SET state = 'claimed', claim_id = 'own', claimed_at_ms = ?, lease_at_ms = ? WHERE job_id = 'ledgertest_b1'").run(toBind(T), toBind(T));
  const r = await refund(db, { job: 'ledgertest_b1', fence: 'owner', claim: 'own' });
  check('T3b', "absent debit, fence 'owner': corruption, no refund", r.outcome === 'corruption' && balanceOf(db, 'ledgertest_8') === 100);
}

// ════════════════════════════════════════════════════════════════════════
// T0: 4.9 publish and marker, 4.11 repair pass, 4.12 sweep
// ════════════════════════════════════════════════════════════════════════
{
  const db = fresh();
  seedJob(db, { job_id: 'ledgertest_p1', state: 'finished', outcome: 'succeeded', finished_at_ms: T, artifact: 'staged', claim_id: 'c', claimed_at_ms: T, lease_at_ms: T });
  seedJob(db, { job_id: 'ledgertest_p2', state: 'finished', outcome: 'refunded', refunded_amount: 10, finished_at_ms: T, artifact: 'staged' });
  check('T0', '4.9 publish: a staged success becomes published', await L.markPublished(ctxOf(db), 'ledgertest_p1') && job(db, 'ledgertest_p1').artifact === 'published');
  check('T0', '4.9 publish never touches a refunded row', !(await L.markPublished(ctxOf(db), 'ledgertest_p2')));
  check('T0', '4.9 the status marker on a finished row', await L.markStatusWritten(ctxOf(db), 'ledgertest_p1') && job(db, 'ledgertest_p1').status_written_at_ms === T);
  seedJob(db, { job_id: 'ledgertest_p3' });
  check('T0', '4.9 no status marker on an unfinished row', !(await L.markStatusWritten(ctxOf(db), 'ledgertest_p3')));
  let r = await L.repairList(ctxOf(db));
  check('T0', '4.11 the repair list: unwritten status or staged, finished in the last day', r.outcome === 'ok' && r.rows.map((x) => x.job_id).join() === 'ledgertest_p2');
  for (const ph of ['scan', 'snapshot', 'verify', undefined]) {
    setCtl(db, 'migration_open', ph);
    r = await L.repairList(ctxOf(db));
    check('T0', `4.11 the repair pass refused with the phase ${ph ?? 'row absent'}`, r.outcome === 'phase_open' && r.rows.length === 0);
  }
  setCtl(db, 'migration_open', '0');
  check('T0', '4.11 discard of a staged refunded row', await L.discardStaged(ctxOf(db), 'ledgertest_p2') && job(db, 'ledgertest_p2').artifact === 'discarded');
  seedJob(db, { job_id: 'ledgertest_p4', state: 'finished', outcome: 'succeeded', finished_at_ms: T, artifact: 'staged', claim_id: 'c', claimed_at_ms: T, lease_at_ms: T });
  check('T0', '4.11 the discard never touches a success', !(await L.discardStaged(ctxOf(db), 'ledgertest_p4')));
  seedJob(db, { job_id: 'ledgertest_p5', state: 'finished', outcome: 'refunded', refunded_amount: 3, finished_at_ms: T - 2 * DAY });
  seedJob(db, { job_id: 'ledgertest_p6', created_at_ms: T - 2 * DAY });
  const od = await L.overdueList(ctxOf(db));
  check('T0', '4.11 overdue: a status unwritten past a day, and a row unfinished past a day', od.map((x) => x.job_id).sort().join() === 'ledgertest_p5,ledgertest_p6');
}
{
  const db = fresh();
  setCtl(db, 'switch_at_ms', String(T - HOUR));
  const old = T - DAY;
  seedJob(db, { job_id: 'ledgertest_w6', state: 'claimed', claim_id: 'c', claimed_at_ms: old, lease_at_ms: old, artifact: 'staged', artifact_meta_json: '{}' });
  seedJob(db, { job_id: 'ledgertest_w3', provenance: 'kv', state: 'claimed', refund_due_code: 'rd', token_cost: 5, client_key: null, request_hash: null });
  seedJob(db, { job_id: 'ledgertest_w1', state: 'debited', created_at_ms: T - 600_001 });
  seedJob(db, { job_id: 'ledgertest_w1n', state: 'debited', created_at_ms: T - 500_000 });
  seedJob(db, { job_id: 'ledgertest_w5', state: 'enqueued', enqueued_at_ms: T - HOUR - 1 });
  seedJob(db, { job_id: 'ledgertest_w5n', state: 'enqueued', enqueued_at_ms: T - HOUR + 1000 });
  seedJob(db, { job_id: 'ledgertest_w2', state: 'claimed', claim_id: 'c', claimed_at_ms: old, lease_at_ms: old });
  seedJob(db, { job_id: 'ledgertest_w2n', state: 'claimed', claim_id: 'c', claimed_at_ms: T, lease_at_ms: T });
  seedJob(db, { job_id: 'ledgertest_w4', provenance: 'kv', state: 'enqueued', token_cost: 5, client_key: null, request_hash: null });
  seedJob(db, { job_id: 'ledgertest_wh', provenance: 'kv', state: 'claimed', hold_reason: 'no_cost', token_cost: null, client_key: null, request_hash: null });
  const rows = await L.sweepList(ctxOf(db));
  const got = Object.fromEntries(rows.map((r) => [r.job_id, L.sweepCandidate(r)]));
  check('T0', '4.12 the sweep list: candidates (6), (3), (1), (5), (2), (4); held and not-yet rows left out',
    JSON.stringify(got) === JSON.stringify({ ledgertest_w6: 6, ledgertest_w3: 3, ledgertest_w1: 1, ledgertest_w5: 5, ledgertest_w2: 2, ledgertest_w4: 4 }) || (
      got.ledgertest_w6 === 6 && got.ledgertest_w3 === 3 && got.ledgertest_w1 === 1 && got.ledgertest_w5 === 5 && got.ledgertest_w2 === 2 && got.ledgertest_w4 === 4
      && rows.length === 6));
  setCtl(db, 'switch_at_ms', '99999999999999');
  check('T0', '4.12 candidate (4) never fires while the sentinel is in place', !(await L.sweepList(ctxOf(db))).some((r) => r.job_id === 'ledgertest_w4'));
  setCtl(db, 'switch_at_ms', String(T - 1_799_000));
  check('T0', '4.12 candidate (4) waits 30 minutes after the unpause', !(await L.sweepList(ctxOf(db))).some((r) => r.job_id === 'ledgertest_w4'));
}
{
  // delivered finish and hold (kv rows), their read-back tables
  const db = fresh();
  const seed = (id, extra = {}) => seedJob(db, { job_id: id, provenance: 'kv', state: 'claimed', token_cost: 5, client_key: null, request_hash: null, created_at_ms: T - DAY, ...extra });
  seed('ledgertest_d1');
  let r = await L.deliveredFinish(ctxOf(db), { job: 'ledgertest_d1', meta: '{"createdAt":1}' });
  check('T0', '4.12 delivered finish: finished now, succeeded and staged, no money', r === 'finished_now' && job(db, 'ledgertest_d1').outcome === 'succeeded'
    && job(db, 'ledgertest_d1').artifact === 'staged' && q(db, 'SELECT 1 FROM ledger').length === 0);
  T += 1;
  r = await L.deliveredFinish(ctxOf(db), { job: 'ledgertest_d1', meta: '{}' });
  check('T0', '4.12 delivered finish on a finished row: fence lost', r === 'fence_lost');
  seed('ledgertest_d2');
  for (const [label, v, key] of PAUSE_VARIANTS) {
    setCtl(db, 'money_pause', v);
    r = await L.deliveredFinish(ctxOf(db), { job: 'ledgertest_d2', meta: '{}' });
    check('T0', `4.12 delivered finish paused with money_pause ${label}: paused, not fence lost`, r === 'paused' && job(db, 'ledgertest_d2').finished_at_ms === null);
  }
  setCtl(db, 'money_pause', '0');
  seed('ledgertest_d3', { claim_id: 'live', claimed_at_ms: T, lease_at_ms: T });
  check('T0', '4.12 delivered finish against a live owner: fence lost', (await L.deliveredFinish(ctxOf(db), { job: 'ledgertest_d3', meta: '{}' })) === 'fence_lost');
  seedJob(db, { job_id: 'ledgertest_dd1', state: 'claimed', created_at_ms: T - DAY });
  check('T0', "4.12 the delivered finish never touches a 'd1' row", (await L.deliveredFinish(ctxOf(db), { job: 'ledgertest_dd1', meta: '{}' })) === 'fence_lost' && job(db, 'ledgertest_dd1').finished_at_ms === null);
  seed('ledgertest_dnew', { created_at_ms: T });
  check('T0', '4.12 the delivered finish waits for the 20-minute floor', (await L.deliveredFinish(ctxOf(db), { job: 'ledgertest_dnew', meta: '{}' })) === 'fence_lost' && job(db, 'ledgertest_dnew').finished_at_ms === null);
  seed('ledgertest_dsub', { mode: 'animate', claim_id: 'live', claimed_at_ms: T - 1_300_000, lease_at_ms: T - 1_300_000, submitted_at_ms: T - 100_000 });
  check('T0', '4.12 the hold against an animate submit under 900 s (its lease past the floor): fence lost', (await L.holdIndexOnly(ctxOf(db), 'ledgertest_dsub')) === 'fence_lost'
    && job(db, 'ledgertest_dsub').claim_id === 'live');
  check('T0', '4.12 the delivered finish against the same: fence lost', (await L.deliveredFinish(ctxOf(db), { job: 'ledgertest_dsub', meta: '{}' })) === 'fence_lost');
  seed('ledgertest_d4');
  setCtl(db, 'money_pause', '1');
  check('T0', '4.12 the index-only hold runs while paused (not a settling transition): held', (await L.holdIndexOnly(ctxOf(db), 'ledgertest_d4')) === 'held'
    && job(db, 'ledgertest_d4').hold_reason === 'index_only');
  setCtl(db, 'money_pause', '0');
  check('T0', '4.12 the hold against a live owner: fence lost', (await L.holdIndexOnly(ctxOf(db), 'ledgertest_d3')) === 'fence_lost');
  seedJob(db, { job_id: 'ledgertest_d5', state: 'claimed', created_at_ms: T - DAY });
  check('T0', "4.12 the hold never touches a 'd1' row", (await L.holdIndexOnly(ctxOf(db), 'ledgertest_d5')) === 'fence_lost');
  seedJob(db, { job_id: 'ledgertest_d6', state: 'finished', outcome: 'refunded', refunded_amount: 5, finished_at_ms: T });
  check('T0', '4.12 the PNG-only discard after a refund', await L.discardPngOnly(ctxOf(db), 'ledgertest_d6') && job(db, 'ledgertest_d6').artifact === 'discarded');
  check('T0', '4.12 the PNG-only discard never touches an unrefunded row', !(await L.discardPngOnly(ctxOf(db), 'ledgertest_d5')));
}

// ════════════════════════════════════════════════════════════════════════
// T0: 4.13 refusals, admission, pending rows, the one resolution
// ════════════════════════════════════════════════════════════════════════
{
  const db = fresh();
  const ref = { event: 'ledgertest_e1', type: 'checkout.session.completed', createdMs: T - HOUR };
  check('T0', "4.13 no refusal row while money_pause reads '0'", !(await L.recordRefusalRelease1(ctxOf(db), { ...ref, r1Keys: 'absent' })) && !(await L.recordRefusal(ctxOf(db), ref)));
  setCtl(db, 'money_pause', undefined);
  check('T0', '4.13 no refusal row with the pause row absent', !(await L.recordRefusal(ctxOf(db), ref)));
  setCtl(db, 'money_pause', '1');
  const epoch = T;
  check('T0', "4.13 release 1's refusal: its r1_keys, this pause's epoch", await L.recordRefusalRelease1(ctxOf(db), { ...ref, r1Keys: 'absent' })
    && one(db, 'SELECT pause_epoch_ms FROM stripe_held').pause_epoch_ms === epoch);
  check('T0', '4.13 the same refusal again: nothing new', !(await L.recordRefusalRelease1(ctxOf(db), { ...ref, r1Keys: 'absent' })));
  check('T0', "4.13 a later refusal reading 'unknown' adds its own row", await L.recordRefusalRelease1(ctxOf(db), { ...ref, r1Keys: 'unknown' }));
  seedLegacy(db, 'webhook:stripe:ledgertest_e2', T + DAY);
  await L.recordRefusal(ctxOf(db), { ...ref, event: 'ledgertest_e2' });
  await L.recordRefusal(ctxOf(db), { ...ref, event: 'ledgertest_e3' });
  check('T0', "4.13 release 2's refusal: 'present' from live legacy evidence, 'absent' without",
    one(db, "SELECT r1_keys FROM stripe_held WHERE event_id = 'ledgertest_e2'").r1_keys === 'present' && one(db, "SELECT r1_keys FROM stripe_held WHERE event_id = 'ledgertest_e3'").r1_keys === 'absent');
  // admission
  const adm = (event, createdMs) => L.readAdmission(ctxOf(db), { event, createdMs, legacy1: `token_idempotency:${event}`, legacy2: `webhook:stripe:${event}` });
  let a = await adm('ledgertest_e1', T - HOUR);
  check('T0', '4.13 no switch recorded (pause_start_ms NULL): nothing admitted', !a.decision.admitted && a.decision.why === 'no_switch');
  db.prepare("INSERT INTO switch_marks (name, value_ms, recorded_at_ms) VALUES ('pause_start_ms', ?, ?)").run(toBind(epoch), toBind(epoch));
  setCtl(db, 'money_pause', '0');
  setCtl(db, 'switch_at_ms', String(T + HOUR));
  T += 2 * HOUR;
  a = await adm('ledgertest_e1', epoch - HOUR);
  check('T0', "4.13 (a): refused 'absent' in this pause, created within 3 days of the refusal: admitted", a.decision.admitted && a.decision.by === 'a');
  a = await adm('ledgertest_e2', epoch - HOUR);
  check('T0', "4.13 veto: live legacy evidence answers replayed, never admitted", !a.decision.admitted && a.decision.why === 'evidence');
  db.prepare("INSERT INTO stripe_held VALUES ('ledgertest_e4', ?, 'present', 'charge.refunded', ?, ?)").run(toBind(epoch), toBind(epoch - HOUR), toBind(epoch));
  db.prepare("INSERT INTO stripe_held VALUES ('ledgertest_e4', ?, 'absent', 'charge.refunded', ?, ?)").run(toBind(epoch), toBind(epoch - HOUR), toBind(epoch));
  a = await adm('ledgertest_e4', epoch + 1);
  check('T0', "4.13 veto: a 'present' refusal in this pause vetoes (a) and (b)", !a.decision.admitted && a.decision.why === 'veto');
  db.prepare("INSERT INTO stripe_held VALUES ('ledgertest_e5', ?, 'unknown', 'checkout.session.completed', ?, ?)").run(toBind(epoch), toBind(epoch - HOUR), toBind(epoch));
  a = await adm('ledgertest_e5', epoch - HOUR);
  check('T0', "4.13 only 'unknown' recorded: (a) does not hold, waits for George", !a.decision.admitted && a.decision.why === 'not_shown');
  a = await adm('ledgertest_e6', epoch + 5);
  check('T0', '4.13 (b): created after the cutoff: admitted', a.decision.admitted && a.decision.by === 'b');
  db.prepare("INSERT INTO stripe_held VALUES ('ledgertest_e7', ?, 'absent', 'checkout.session.completed', ?, ?)").run(toBind(epoch), toBind(epoch - 4 * DAY), toBind(epoch));
  a = await adm('ledgertest_e7', epoch - 4 * DAY);
  check('T0', '4.13 (a) fails for an event created more than 3 days before its refusal', !a.decision.admitted);
  db.prepare("INSERT INTO stripe_held VALUES ('ledgertest_e8', ?, 'absent', 'checkout.session.completed', ?, ?)").run(toBind(epoch - DAY), toBind(epoch - DAY - HOUR), toBind(epoch - DAY));
  a = await adm('ledgertest_e8', epoch - DAY - HOUR);
  check('T0', "4.13 a refusal from an earlier pause (another epoch) admits nothing here", !a.decision.admitted);
  db.prepare("INSERT INTO stripe_pending (event_id, reason, first_seen_ms, disposition, evidence_note, decided_by, decided_at_ms) VALUES ('ledgertest_e6', 'no_evidence', 1, 'none', 'n', 'g', 1)").run();
  a = await adm('ledgertest_e6', epoch + 5);
  check('T0', "4.13 veto: a recorded 'none'", !a.decision.admitted && a.decision.why === 'veto');
  // the bound
  seedLegacy(db, 'token_idempotency:zz', T + HOUR);
  a = await adm('ledgertest_e9', epoch + 5);
  check('T0', "4.13 before MIN(keep_until_ms): admitted", a.decision.admitted);
  T += HOUR;
  a = await adm('ledgertest_e9', epoch + 5);
  check('T0', '4.13 at MIN(keep_until_ms): lapsed, waits for George (N13)', !a.decision.admitted && a.decision.why === 'lapsed');
  db.exec('DELETE FROM legacy_idem');
  a = await adm('ledgertest_e9', epoch + 5);
  check('T0', '4.13 legacy_idem empty: the fallback bound switch_at_ms + 45 days admits', a.decision.admitted);
  T = Number(one(db, "SELECT CAST(value AS INTEGER) AS v FROM control WHERE key = 'switch_at_ms'").v) + 3888000000;
  a = await adm('ledgertest_e9', epoch + 5);
  check('T0', '4.13 at switch_at_ms + 45 days with legacy_idem empty: lapsed', !a.decision.admitted && a.decision.why === 'lapsed');
  T = 1_800_100_000_000;
}
{
  // pending rows, dispositions, N14, the one resolution
  const db = fresh();
  await openUser(db, 'ledgertest_10', 0);
  let p = await L.recordPending(ctxOf(db), { event: 'ledgertest_f1', reason: 'no_evidence' });
  check('T0', '4.13 the first unresolved delivery: a pending row, no disposition, moved 0', p.disposition === null && p.moved === 0);
  p = await L.recordPending(ctxOf(db), { event: 'ledgertest_f1', reason: 'mapping_missing' });
  check('T0', '4.13 the pending row is written once', one(db, "SELECT reason FROM stripe_pending WHERE event_id = 'ledgertest_f1'").reason === 'no_evidence');
  await credit(db, 'ledgertest_10', 500, 'stripe:ledgertest_f1', { event: 'ledgertest_f1' });
  p = await L.recordDisposition(ctxOf(db), { event: 'ledgertest_f1', disposition: 'none', note: 'not ours', who: 'george' });
  check('T0', "4.13 George's first 'none' with a stripe: row: refused, the read-back moved 1", p.disposition === null && p.moved === 1);
  await L.recordPending(ctxOf(db), { event: 'ledgertest_f2', reason: 'mapping_missing' });
  await L.movement(ctxOf(db), { uid: 'ledgertest_10', type: 'debit', amount: 5, reason: 'refund_debit', source: 'refund_debit', idem: 'stripe:ledgertest_f2', event: 'ledgertest_f2' });
  p = await L.recordNoneN14(ctxOf(db), { event: 'ledgertest_f2', note: 'charge not ours', who: 'george' });
  check('T0', "4.13 N14's 'none' with a stripe: row: refused, moved 1", p.disposition === null && p.moved === 1);
  await L.recordPending(ctxOf(db), { event: 'ledgertest_f3', reason: 'mapping_missing' });
  p = await L.recordNoneN14(ctxOf(db), { event: 'ledgertest_f3', note: 'charge not ours', who: 'george' });
  check('T0', "4.13 N14's evidenced 'none' with no movement: recorded", p.disposition === 'none' && p.moved === 0);
  p = await L.recordDisposition(ctxOf(db), { event: 'ledgertest_f3', disposition: 'apply', note: null, who: 'george' });
  check('T0', "4.13 George's 'no_evidence' disposition never touches a 'mapping_missing' row", p.disposition === 'none');
  await L.recordPending(ctxOf(db), { event: 'ledgertest_f3b', reason: 'mapping_missing' });
  p = await L.recordDisposition(ctxOf(db), { event: 'ledgertest_f3b', disposition: 'none', note: 'x', who: 'george' });
  check('T0', "4.13 the 'no_evidence' disposition statement never writes a 'mapping_missing' row (N14's is the only path)", p.disposition === null);
  await L.recordPending(ctxOf(db), { event: 'ledgertest_f4', reason: 'no_evidence' });
  p = await L.recordDisposition(ctxOf(db), { event: 'ledgertest_f4', disposition: 'apply', note: null, who: 'george' });
  check('T0', "4.13 George's 'apply' recorded", p.disposition === 'apply');
  p = await L.recordDisposition(ctxOf(db), { event: 'ledgertest_f4', disposition: 'none', note: 'x', who: 'george' });
  check('T0', '4.13 a recorded disposition is not overwritten by this statement', p.disposition === 'apply');
  // the one resolution
  await L.recordPending(ctxOf(db), { event: 'ledgertest_f5', reason: 'no_evidence' });
  let s = await L.resolvePending(ctxOf(db), 'ledgertest_f5');
  check('T0', '4.13 the one resolution with no movement, no none, no legacy: nothing resolved', !s.resolved);
  s = await L.resolvePending(ctxOf(db), 'ledgertest_f1');
  check('T0', '4.13 the one resolution with a stripe: row: resolved now', s.resolved && s.resolvedNow);
  T += 1;
  s = await L.resolvePending(ctxOf(db), 'ledgertest_f1');
  check('T0', '4.13 the one resolution again: resolved once, unchanged', s.resolved && !s.resolvedNow);
  s = await L.resolvePending(ctxOf(db), 'ledgertest_f3');
  check('T0', "4.13 the one resolution with a recorded 'none': resolved", s.resolved);
  await L.recordPending(ctxOf(db), { event: 'ledgertest_f6', reason: 'no_evidence' });
  seedLegacy(db, 'webhook:stripe:ledgertest_f6', T + DAY);
  s = await L.resolvePending(ctxOf(db), 'ledgertest_f6');
  check('T0', '4.13 the one resolution with live legacy evidence: resolved', s.resolved);
  check('T0', '4.13 the one resolution of an event with no pending row: nothing', (await L.resolvePending(ctxOf(db), 'ledgertest_f9')) === null);
  // refusals not yet settled
  const ep = 5;
  for (const [e, t] of [['ledgertest_u1', 'checkout.session.completed'], ['ledgertest_u2', 'charge.refunded'], ['ledgertest_u3', 'invoice.paid'], ['ledgertest_f1', 'checkout.session.completed'], ['ledgertest_f5', 'charge.dispute.created'], ['ledgertest_u4', 'checkout.session.completed']]) {
    db.prepare('INSERT INTO stripe_held VALUES (?, ?, ?, ?, 1, ?)').run(e, toBind(ep), 'absent', t, toBind(10));
  }
  db.prepare("INSERT INTO stripe_held VALUES ('ledgertest_u1', 5, 'unknown', 'checkout.session.completed', 1, 7)").run();
  seedLegacy(db, 'token_idempotency:ledgertest_u4', 1);
  const u = await L.unsettledRefusals(ctxOf(db), ep);
  check('T0', '4.13 refusals not yet settled: money types, no stripe: row, no pending row, no legacy key (any age), one row per event with its first refusal',
    u.map((x) => x.event_id).sort().join() === 'ledgertest_u1,ledgertest_u2' && u.find((x) => x.event_id === 'ledgertest_u1').first_refused_ms === 7);
}

// ════════════════════════════════════════════════════════════════════════
// T0: 4.15 the guarded unpause, 4.16 the kill switch
// ════════════════════════════════════════════════════════════════════════
const ctl = (db) => Object.fromEntries(q(db, 'SELECT key, value FROM control ORDER BY key').map((r) => [r.key, r.value]));
{
  const preStates = [
    ["money_pause '0'", (db) => setCtl(db, 'money_pause', '0')],
    ['money_pause absent', (db) => setCtl(db, 'money_pause', undefined)],
    ["money_pause 'x'", (db) => setCtl(db, 'money_pause', 'x')],
    ['switch_at_ms a time already', (db) => setCtl(db, 'switch_at_ms', '1700000000000')],
    ['switch_at_ms absent', (db) => setCtl(db, 'switch_at_ms', undefined)],
    ["the phase 'scan'", (db) => setCtl(db, 'migration_open', 'scan')],
    ["the phase 'verify'", (db) => setCtl(db, 'migration_open', 'verify')],
    ['the phase row absent', (db) => setCtl(db, 'migration_open', undefined)],
  ];
  for (const [label, set] of preStates) {
    const db = fresh(); setCtl(db, 'money_pause', '1'); set(db);
    const before = JSON.stringify(ctl(db));
    const r = await L.unpause(ctxOf(db));
    check('T0', `4.15 refused with ${label}: no change`, JSON.stringify(ctl(db)) === before && r.outcome !== 'committed');
  }
  let db = fresh(); setCtl(db, 'money_pause', '1');
  let r = await L.unpause(ctxOf(db));
  check('T0', "4.15 committed: both rows change, money_pause '0' and switch_at_ms the literal", r.outcome === 'committed' && ctl(db).money_pause === '0' && ctl(db).switch_at_ms === String(T));
  T += 1;
  r = await L.unpause(ctxOf(db));
  check('T0', "4.15 applies once: a second run changes nothing (its read-back still shows the first commit's time)", ctl(db).switch_at_ms === String(T - 1) && L.decideUnpause(r.rows) === 'committed');
  db = fresh(); setCtl(db, 'money_pause', '1');
  r = await L.unpause(ctxOf(db, { hooks: { after: once('lost') } }));
  check('T0', "4.15 a lost response: unclear; 4.15's read-back decides committed (the fix forward only)", r.outcome === 'unclear' && L.decideUnpause(r.rows) === 'committed');
  db = fresh(); setCtl(db, 'money_pause', '1');
  r = await L.unpause(ctxOf(db, { hooks: { before: once('lost') } }));
  check('T0', '4.15 a lost request: unclear; the read-back decides not committed (the rollback abort)', r.outcome === 'unclear' && L.decideUnpause(r.rows) === 'not_committed');
  db = fresh();
  r = await L.unpause(ctxOf(db));
  check('T0', "4.15 money_pause '0' with the sentinel: unclear, no change", r.outcome === 'unclear' && ctl(db).switch_at_ms === '99999999999999');
  const k = await L.killPause(ctxOf(db), 'george');
  check('T0', "4.15 then paused again by 4.16 (a new epoch), and the read-back decides: the sentinel takes the rollback abort",
    k.outcome === 'paused_now' && L.decideUnpause(q(db, L.LEDGER_SQL.UNPAUSE_READ)) === 'not_committed');
}
{
  let db = fresh();
  let k = await L.killPause(ctxOf(db), 'george');
  check('T0', '4.16 the pause: paused now, its epoch = :now', k.outcome === 'paused_now' && k.row.updated_at_ms === T);
  T += 1;
  k = await L.killPause(ctxOf(db), 'george');
  check('T0', '4.16 the pause again: already paused, the epoch kept', k.outcome === 'already_paused' && k.row.updated_at_ms === T - 1);
  k = await L.killReopen(ctxOf(db), 'george');
  check('T0', '4.16 the reopen with the sentinel in place (release 1, an abort): no run needed', k.outcome === 'reopened' && ctl(db).money_pause === '0');
  // after 4.15: runs and drift gate the reopen
  db = fresh();
  const epoch = T;
  db.prepare("INSERT INTO switch_marks VALUES ('pause_start_ms', ?, ?)").run(toBind(epoch), toBind(epoch));
  setCtl(db, 'money_pause', '1');
  await L.unpause(ctxOf(db));
  T += HOUR;
  const runIns = (run, purpose) => runStmt(db, S(2061, 0), { run, purpose, now: T, who: 'george' });
  const runDone = (run, over = {}) => runStmt(db, S(2061, 3), { run, scan_started: T - 10, scan_finished: T - 5, bl: 0, br: 0, kl: 0, kr: 0, jl: 0, jr: 0, drift: 0, bal_done: 1, idem_done: 1, marks_done: 1, jobs_done: 1, snap_read: 0, snap_missing: 0, ...over });
  runIns('run_9b', 'step_9b'); runDone('run_9b');
  await L.killPause(ctxOf(db), 'george');
  T += 1;
  k = await L.killReopen(ctxOf(db), 'george');
  check('T0', '4.16 after 4.15 with no run in the current pause: refused', k.outcome === 'refused');
  runIns('run_p1', 'reopen');
  runDone('run_p1', { bal_done: 0 });
  k = await L.killReopen(ctxOf(db), 'george');
  check('T0', '4.16 after a partial run (a prefix not exhausted): refused', k.outcome === 'refused');
  T += 1;
  runIns('run_p2', 'reopen');
  runStmt(db, S(2061, 5), { epoch, kind: 'balance', subject: 'ledgertest_dr', uid: 'ledgertest_dr', snapshot: null, found: '{"balance":5}', now: T, run: 'run_p2' });
  runDone('run_p2', { drift: 1 });
  k = await L.killReopen(ctxOf(db), 'george');
  check('T0', '4.16 with a complete run but an undisposed switch_drift row: refused', k.outcome === 'refused' && one(db, "SELECT complete FROM backstop_runs WHERE run_id = 'run_p2'").complete === 1);
  runStmt(db, S(2061, 6), { disposition: 'nothing_owed', note: 'checked', who: 'george', now: T, epoch, kind: 'balance', subject: 'ledgertest_dr' });
  k = await L.killReopen(ctxOf(db), 'george');
  check('T0', '4.16 with a complete run finished in this pause and every drift disposed: reopened', k.outcome === 'reopened');
  // a run recorded before the pause, its scan ending after it, never counts
  T += 1;
  runIns('run_pre', 'reopen');
  await L.killPause(ctxOf(db), 'george');
  T += 10;
  runDone('run_pre', { scan_finished: T });
  T += 1;
  k = await L.killReopen(ctxOf(db), 'george');
  check('T0', '4.16 a run recorded before the pause, even if its scan ended after it: refused', k.outcome === 'refused'
    && one(db, "SELECT complete, run_pause_ms FROM backstop_runs WHERE run_id = 'run_pre'").run_pause_ms === null);
  runIns('run_close', 'close'); runDone('run_close');
  T += 1;
  await L.killReopen(ctxOf(db), 'george');
  await L.killPause(ctxOf(db), 'george');
  T += 1;
  k = await L.killReopen(ctxOf(db), 'george');
  check('T0', "4.16 after Close's complete run: only the dispositions, reopened with no run in this pause", k.outcome === 'reopened');
  T += 1; await L.killPause(ctxOf(db), 'george');
  runStmt(db, 'INSERT INTO switch_drift (pause_epoch_ms, kind, subject, found_json, found_at_ms) VALUES (:epoch, :kind, :subject, :found, :now)', { epoch, kind: 'key', subject: 'k9', found: '{}', now: T });
  T += 1;
  k = await L.killReopen(ctxOf(db), 'george');
  check('T0', '4.16 after Close, an undisposed drift still refuses the reopen', k.outcome === 'refused');
  // before 0003
  db = fresh(1); setCtl(db, 'money_pause', '1');
  runStmt(db, inline(1440), { now: T, who: 'george' });
  check('T0', "4.16 the reopen before 0003, on 0001's rows alone: applies", ctl(db).money_pause === '0');
  db = fresh(1); setCtl(db, 'money_pause', '1');
  check('T0', "4.16's full reopen cannot run before 0003 (no switch_drift table): the prose form is needed", await throws(() => L.killReopen(ctxOf(db), 'george').then((x) => { if (x.outcome === 'error') throw new Error('x'); })) || ctl(db).money_pause === '1');
}

// ════════════════════════════════════════════════════════════════════════
// T0: section 8 and 4.17, 4.18, as written
// ════════════════════════════════════════════════════════════════════════
{
  // the phase contract: every pair over the four phases, paused and open
  const phases = ['0', 'scan', 'snapshot', 'verify'];
  const allowed = new Set(['0>scan', 'scan>0', 'scan>snapshot', 'snapshot>verify', 'snapshot>0', 'verify>0']);
  let okAll = true;
  for (const paused of ['0', '1']) for (const from of phases) for (const to of [...phases, 'x']) {
    const db = fresh(); setCtl(db, 'money_pause', paused); setCtl(db, 'migration_open', from);
    const r = runTx(db, block(1883), { to, from, now: T, who: 'george' });
    const moved = r[1].rows[0].value === to && from !== to;
    const want = allowed.has(`${from}>${to}`) && (!['snapshot', 'verify'].includes(to) || paused === '1');
    if (moved !== want) { okAll = false; console.log('  phase pair', paused, from, to, moved, want); }
  }
  check('T0', 'section 8 the phase contract: only the six pairs move, snapshot and verify only while paused', okAll);
  const db = fresh(); setCtl(db, 'migration_open', 'scan');
  const r = runTx(db, block(1883), { to: 'scan', from: 'scan', now: T, who: 'g' });
  check('T0', 'section 8 a pair with :from = :to is not in the table (no write)', r[0].changes === 0);
}
{
  // step 2's cutoff
  const db = fresh();
  const cut = T;
  let r = runTx(db, block(1912), { cutoff: cut });
  check('T0', 'step 2: the pause and the cutoff with one literal, both read back equal', r[2].rows[0].pause_epoch_ms === cut && r[2].rows[0].cutoff_ms === cut);
  r = runTx(db, block(1912).slice(1), { cutoff: cut + 5 });
  check('T0', "step 2's INSERT rerun with another literal writes nothing", r[0].changes === 0 && one(db, 'SELECT value_ms FROM switch_marks').value_ms === cut);
  const other = fresh();
  setCtl(other, 'money_pause', '1');
  const ins = runTx(other, block(1912).slice(1), { cutoff: T + 99 });
  check('T0', "step 2's INSERT writes nothing when the pause in place is not this literal's", ins[0].changes === 0 && ins[1].rows[0].cutoff_ms === null);
  const s5 = runStmt(db, S(1454, 0), {});
  check('T0', "4.17 the pause-start check: both non-NULL and equal", s5.rows[0].cutoff_ms === cut && s5.rows[0].pause_epoch_ms === cut);
  T += 1; setCtl(db, 'money_pause', '1');
  check('T0', '4.17 the pause-start check after a re-pause (another epoch): unequal, the abort', runStmt(db, S(1454, 0), {}).rows[0].pause_epoch_ms !== cut);
}
{
  // 4.17's import batches: refused unless paused in 'snapshot'
  const states = [
    ["money open, 'snapshot'", '0', 'snapshot'], ["paused, 'scan'", '1', 'scan'], ["paused, '0'", '1', '0'], ["paused, 'verify'", '1', 'verify'],
    ["pause absent, 'snapshot'", undefined, 'snapshot'], ["money_pause 'x', 'snapshot'", 'x', 'snapshot'], ["paused, the phase absent", '1', undefined],
  ];
  const imports = (db) => {
    runTx(db, block(1465), { id: `i_${++idn}`, uid: 'ledgertest_imp', amount: -10, meta: null, now: T, kv_last: 'kvts' });
    runTx(db, [S(1489, 0)], { key: 'token_idempotency:ledgertest_k', kind: 'other', kv_exp: null, now: T });
    runTx(db, [S(1505, 0)], { now: T });
    runTx(db, [S(1528, 0)], { job: 'ledgertest_ij', uid: 'ledgertest_imp', mode: 'create', cost: 10, state: 'claimed', due_code: null, hold: null, import_json: '{"class":"x","fingerprint":"f1"}', outcome: null, code: null, msg: null, artifact: 'none', status_at: null, created: T, enqueued: null, finished: null });
  };
  const seedCarry = (db) => db.prepare("INSERT INTO switch_obligations (pause_epoch_ms, subject_kind, subject_id, source, evidence_json, kv_payable, handed_off_at_ms, disposition, decided_at_ms) VALUES (1, 'event', 'ledgertest_ce', 'stripe_refusal', '{}', 0, 1, 'carry', 1)").run();
  for (const [label, pause, phase] of states) {
    const db = fresh(); seedCarry(db); setCtl(db, 'money_pause', pause); setCtl(db, 'migration_open', phase);
    imports(db);
    const n = ['balances', 'ledger', 'legacy_idem', 'stripe_pending', 'jobs'].map((t) => one(db, `SELECT COUNT(*) AS n FROM ${t}`).n).reduce((a, b) => a + b);
    const rb = runStmt(db, S(1478, 0), { uids: '["ledgertest_imp"]' }).rows[0];
    check('T0', `4.17 every import batch refused with ${label}: nothing written, the chunk reads refused`, n === 0 && rb.balance === null);
  }
  const db = fresh(); seedCarry(db); setCtl(db, 'money_pause', '1'); setCtl(db, 'migration_open', 'snapshot');
  imports(db);
  let rb = runStmt(db, S(1478, 0), { uids: '["ledgertest_imp"]' }).rows[0];
  check('T0', "4.17 paused in 'snapshot': imported; a negative opening (-10) as KV holds it", rb.balance === -10 && rb.opened_amount === -10 && rb.opened_source === 'snapshot');
  imports(db);
  rb = runStmt(db, S(1478, 0), { uids: '["ledgertest_imp"]' }).rows[0];
  check('T0', '4.17 a retried chunk of each kind: imported again, no new rows', rb.balance === -10 && one(db, 'SELECT COUNT(*) AS n FROM ledger').n === 1
    && one(db, 'SELECT COUNT(*) AS n FROM legacy_idem').n === 1 && one(db, 'SELECT COUNT(*) AS n FROM jobs').n === 1 && one(db, 'SELECT COUNT(*) AS n FROM stripe_pending').n === 1);
  check('T0', '4.17 legacy, carried event and jobs read-backs', runStmt(db, S(1489, 1), { keys: '["token_idempotency:ledgertest_k","nope"]' }).rows.map((x) => x.copied_at_ms !== null).join() === 'true,false'
    && runStmt(db, S(1505, 1), { now: T }).rows[0].reason === 'no_evidence' && runStmt(db, S(1528, 1), { jobs: '["ledgertest_ij"]' }).rows[0].fingerprint === 'f1');
  setCtl(db, 'migration_open', 'verify');
  check('T0', "4.17 an already imported chunk read outside 'snapshot' is still read, but its writes are refused (R4-17's caller rule)",
    runTx(db, [S(1528, 0)], { job: 'ledgertest_ij2', uid: 'ledgertest_imp', mode: 'create', cost: 10, state: 'claimed', due_code: null, hold: null, import_json: '{}', outcome: null, code: null, msg: null, artifact: 'none', status_at: null, created: T, enqueued: null, finished: null })[0].changes === 0);
  // The largest release 1 success record kept as repair_record, against D1's row limit (2,000,000 bytes).
  // Release 1's success record (C types.ts JobStateSuccess) carries the whole PNG as resultBase64. The
  // ceiling used here is a 512 x 512 RGBA PNG that does not compress at all (8-bit, filter bytes, no gain):
  // 512 * (512 * 4 + 1) bytes, base64. The real largest record is a production measurement (rule 6, part B).
  const pngBytes = 512 * (512 * 4 + 1) + 1024;
  const record = {
    status: 'success', userId: 'user_' + 'u'.repeat(27), mode: 'animate', enqueuedAt: T, startedAt: T, completedAt: T,
    resultBase64: 'A'.repeat(Math.ceil(pngBytes / 3) * 4), rdBalanceCost: 99.99, rescued: true,
    requestedWidth: 256, requestedHeight: 256, deliveredCellSize: 64, deliveredFrames: 64,
  };
  const importJson = JSON.stringify({ class: 'identity_succeeded', copies: { kv: 'success', r2: null },
    evidence: [{ kind: 'kv', key: 'job:' + 'j'.repeat(80), fingerprint: 'f'.repeat(64) }], fingerprint: 'f'.repeat(64), repair_record: record });
  setCtl(db, 'migration_open', 'snapshot');
  runTx(db, [S(1528, 0)], { job: 'ledgertest_big', uid: 'ledgertest_imp', mode: 'animate', cost: 50, state: 'finished', due_code: null, hold: null, import_json: importJson, outcome: 'succeeded', code: null, msg: null, artifact: 'published', status_at: null, created: T, enqueued: null, finished: T });
  const cols = q(db, 'PRAGMA table_info(jobs)').map((c) => c.name);
  const size = one(db, `SELECT ${cols.map((c) => `COALESCE(length(CAST(${c} AS BLOB)), 0) + 9`).join(' + ')} AS n FROM jobs WHERE job_id = 'ledgertest_big'`).n;
  check('T0', `a release 1 success record at the 512 x 512 uncompressed ceiling as repair_record: the row is ${size} bytes, under D1's 2,000,000`,
    size < 2_000_000 && one(db, "SELECT json_extract(import_json, '$.repair_record.status') AS s FROM jobs WHERE job_id = 'ledgertest_big'").s === 'success');
  console.log(`[ledger-s1-test] note: repair_record headroom: a resultBase64 up to about ${2_000_000 - (size - record.resultBase64.length)} characters fits one D1 row`);
}
{
  // admission records (6.2): the INSERT only with '0'; completion keyed on the id
  for (const [label, v, want] of [["'0'", '0', 1], ["'1'", '1', 0], ['absent', undefined, 0], ["'x'", 'x', 0], ["''", '', 0]]) {
    const db = fresh(); setCtl(db, 'money_pause', v);
    const r = runTx(db, block(1745).slice(0, 2), { admission: 'ad1', route: 'generate', kind: 'job', subject: 'ledgertest_j', uid: 'ledgertest_u', meta: '{"mode":"create"}', now: T });
    check('T0', `the admission record's INSERT with money_pause ${label}: ${want ? 'a row' : 'no row'}`, r[1].rows.length === want);
  }
  const db = fresh();
  runTx(db, block(1745).slice(0, 2), { admission: 'ad1', route: 'generate', kind: 'job', subject: 'ledgertest_j', uid: 'ledgertest_u', meta: '{}', now: T });
  runTx(db, block(1745).slice(0, 2), { admission: 'ad2', route: 'generate', kind: 'job', subject: 'ledgertest_j', uid: 'ledgertest_u', meta: '{}', now: T });
  const c = runTx(db, block(1745).slice(2), { admission: 'ad1', now: T + 5 });
  check('T0', 'the completion keyed on the id: only that record', c[1].rows[0].completed_at_ms === T + 5 && one(db, "SELECT completed_at_ms FROM money_admissions WHERE admission_id = 'ad2'").completed_at_ms === null);
}
{
  // Q4, George's closing statement (refused without the named evidence; accepted with it), the late completion and review
  const admitAt = T;
  const base = () => ({
    rows: [{ ref: 'token_tx:ledgertest_u:1', at_ms: admitAt + 10 }], log_lines: [{ ref: 'ad1 response 202', at_ms: admitAt + 20 }], other_tx: [],
    end: { kind: 'log_line', ref: 'ad1 response 202', at_ms: admitAt + 20 },
    reads: [{ at_ms: admitAt + 30, balance: 5 }, { at_ms: admitAt + 30 + DAY, balance: 5 }], nothing_new: true, balance_explained: true,
  });
  const closeAt = admitAt + 30 + DAY + 1;
  const tryClose = (ev, opts = {}) => {
    const db = fresh();
    runTx(db, block(1745).slice(0, 2), { admission: 'ad1', route: opts.route ?? 'generate', kind: opts.kind ?? 'job', subject: 's', uid: opts.uid === undefined ? 'ledgertest_u' : opts.uid, meta: opts.kind && opts.kind !== 'job' ? null : '{}', now: admitAt });
    let r;
    try { r = runTx(db, block(1994), { who: 'george', now: opts.closeAt ?? closeAt, note: opts.note ?? 'checked', evidence: typeof ev === 'string' ? ev : JSON.stringify(ev), admission: 'ad1' }); } catch { return { closed: false, db, threw: true }; }
    return { closed: r[1].rows[0].closed_at_ms !== null, db };
  };
  const variant = (f) => { const e = base(); f(e); return e; };
  const refusals = [
    ['no end', (e) => { delete e.end; }],
    ['no rows', (e) => { e.rows = []; }],
    ['reads under 24 hours apart', (e) => { e.reads[1].at_ms = e.reads[0].at_ms + DAY - 1; }],
    ['something new', (e) => { e.nothing_new = false; }],
    ['an unexplained balance', (e) => { e.balance_explained = false; }],
    ["evidence '{}'", () => {}, '{}'],
    ...['rows', 'log_lines', 'other_tx', 'end', 'reads', 'nothing_new', 'balance_explained'].map((m) => [`member ${m} missing`, (e) => { delete e[m]; }]),
    ['a read without its integer balance', (e) => { e.reads[1].balance = '5'; }],
    ['other_tx not a list', (e) => { e.other_tx = {}; }],
    ['log_lines not a list', (e) => { e.log_lines = 'x'; }],
    ['a blank note', null, null, { note: '' }],
    ['a whitespace-only note', null, null, { note: ' \t\n\r ' }],
    ['a missing end reference', (e) => { delete e.end.ref; }],
    ['a blank end reference', (e) => { e.end.ref = '   '; }],
    ['a missing end time', (e) => { delete e.end.at_ms; }],
    ['a non-integer end time', (e) => { e.end.at_ms = 1.5; }],
    ['a missing read time', (e) => { delete e.reads[0].at_ms; }],
    ['a non-integer read time', (e) => { e.reads[1].at_ms = String(e.reads[1].at_ms); }],
    ['an end before the admission', (e) => { e.end.at_ms = admitAt - 1; }],
    ["reads before the end (S2 032's trace)", (e) => { e.end.at_ms = e.reads[0].at_ms + 1; }],
    ['a closure before the second read', null, null, { closeAt: admitAt + 30 + DAY - 1 }],
    ['a list element without its own reference', (e) => { e.rows.push({ at_ms: admitAt + 11 }); }],
    ['a list element without an integer time', (e) => { e.log_lines.push({ ref: 'x', at_ms: admitAt + 10.5 }); }],
    ['a list element with its time as text', (e) => { e.rows.push({ ref: 'x', at_ms: String(admitAt + 10) }); }],
    ['a list element not an object', (e) => { e.other_tx.push('token_tx:x'); }],
    ['a request row dated after the first read (the late write listed as a row)', (e) => { e.rows.push({ ref: 'token_tx:late', at_ms: e.reads[0].at_ms + 1 }); }],
    ['a log line dated after the first read', (e) => { e.log_lines.push({ ref: 'ad1 late', at_ms: e.reads[0].at_ms + 1 }); }],
    ['an other_tx row outside the two reads', (e) => { e.other_tx.push({ ref: 'token_tx:o', at_ms: e.reads[1].at_ms + 1 }); }],
    ["the end's reference only a tab", (e) => { e.end.ref = '\t'; }],
    ["an element's reference only a newline", (e) => { e.rows[0].ref = '\n'; }],
    ['an opening with both keys absent but a balance on a read', (e) => { delete e.balance_explained; e.no_customer = { basis: 'balance_key_absent' }; e.reads[0].key_absent = true; e.reads[1].key_absent = true; }, null, { route: 'opening', kind: 'user' }],
    ['no_customer on a job', (e) => { delete e.reads[0].balance; delete e.reads[1].balance; e.no_customer = { basis: 'event_evidence', ref: 'evt' }; }],
    ["no_customer on a known user's reward", (e) => { delete e.reads[0].balance; delete e.reads[1].balance; e.no_customer = { basis: 'balance_key_absent' }; e.reads[0].key_absent = true; e.reads[1].key_absent = true; }, null, { route: 'daily_reward', kind: 'user' }],
    ["no_customer on an email-list request", (e) => { delete e.reads[0].balance; delete e.reads[1].balance; e.no_customer = { basis: 'event_evidence', ref: 'x' }; }, null, { route: 'email_list', kind: 'user' }],
    ['no_customer on an event with a user', (e) => { delete e.reads[0].balance; delete e.reads[1].balance; e.no_customer = { basis: 'event_evidence', ref: 'evt metadata' }; }, null, { route: 'stripe_webhook', kind: 'event' }],
    ["no_customer on an event without its evidence's reference", (e) => { delete e.reads[0].balance; delete e.reads[1].balance; e.no_customer = { basis: 'event_evidence' }; }, null, { route: 'stripe_webhook', kind: 'event', uid: null }],
    ["no_customer's reference only a tab", (e) => { delete e.reads[0].balance; delete e.reads[1].balance; e.no_customer = { basis: 'event_evidence', ref: '\t' }; }, null, { route: 'stripe_webhook', kind: 'event', uid: null }],
    ['no_customer on an opening present at one read', (e) => { delete e.reads[0].balance; e.no_customer = { basis: 'balance_key_absent' }; e.reads[0].key_absent = true; e.reads[1].key_absent = false; delete e.balance_explained; }, null, { route: 'opening', kind: 'user' }],
  ];
  for (const [name, f, raw, opts] of refusals) {
    const ev = raw ?? (f ? variant(f) : base());
    const r = tryClose(ev, opts ?? {});
    check('T0', `George's closing statement refused: ${name}`, !r.closed);
  }
  check('T0', "George's closing statement accepted with the named evidence", tryClose(base()).closed);
  check('T0', "accepted: no_customer on an event whose own evidence names no customer (no user)", tryClose(variant((e) => { delete e.reads[0].balance; delete e.reads[1].balance; delete e.balance_explained; e.no_customer = { basis: 'event_evidence', ref: 'evt_x type and metadata' }; }), { route: 'stripe_webhook', kind: 'event', uid: null }).closed);
  check('T0', 'accepted: no_customer on an opening absent at both reads', tryClose(variant((e) => { delete e.reads[0].balance; delete e.reads[1].balance; delete e.balance_explained; e.no_customer = { basis: 'balance_key_absent' }; e.reads[0].key_absent = true; e.reads[1].key_absent = true; }), { route: 'opening', kind: 'user' }).closed);
  // Q4 and the late completion
  const { db } = tryClose(base());
  const q4 = () => runStmt(db, S(1957, 0), {}).rows[0];
  check('T0', 'a closure with no completion: Q4 holds, the clock is the closure', q4().open_records === 0 && q4().last_completed_ms === closeAt);
  let c = runTx(db, block(1745).slice(2), { admission: 'ad1', now: closeAt + 100 });
  check('T0', 'a completion after the closure: kept beside it, read back with the closure', c[1].rows[0].completed_at_ms === closeAt + 100 && c[1].rows[0].closed_at_ms === closeAt);
  c = runTx(db, block(1745).slice(2), { admission: 'ad1', now: closeAt + 200 });
  check('T0', 'its reply lost, the rerun writes nothing', c[0].changes === 0 && c[1].rows[0].completed_at_ms === closeAt + 100);
  check('T0', 'the clock moved to the late completion', q4().last_completed_ms === closeAt + 100);
  const late = () => runStmt(db, S(2024, 2), {}).rows[0].late_unreviewed;
  check('T0', "step 0's late_unreviewed counts it", late() === 1);
  runTx(db, block(1745).slice(0, 2), { admission: 'ad_open', route: 'generate', kind: 'job', subject: 's', uid: 'ledgertest_u', meta: '{}', now: closeAt + 300 });
  let rv = runTx(db, block(2024).slice(0, 2), { review: 'delayed_completion', note: 'ended at its end', who: 'george', now: closeAt + 400, admission: 'ad_open' });
  check('T0', 'the review refused on an open record', rv[0].changes === 0);
  check('T0', 'Q4 with an open record counts it by its admission', q4().open_records === 1 && q4().last_completed_ms === closeAt + 300);
  rv = runTx(db, block(2024).slice(0, 2), { review: 'delayed_completion', note: 'ended at its end', who: 'george', now: closeAt + 400, admission: 'ad1' });
  const rv2 = runTx(db, block(2024).slice(0, 2), { review: 'late_writer', note: 'x', who: 'george', now: closeAt + 500, admission: 'ad1' });
  check('T0', 'the review recorded once', rv[1].rows[0].late_review === 'delayed_completion' && rv2[0].changes === 0 && late() === 0);
  const empty = fresh();
  check('T0', "Q4's clock is NULL only with no record", runStmt(empty, S(1957, 0), {}).rows[0].last_completed_ms === null && runStmt(empty, S(1957, 0), {}).rows[0].open_records === 0);
}
{
  // switch_attempts: the end, HQ's decision, step 0's refusal read
  const db = fresh();
  const att = (epoch, outcome, reason) => runStmt(db, S(2038, 0), { epoch, now: T, outcome, reason, note: null, who: 'george' });
  const step0 = () => runStmt(db, S(2038, 2), {}).rows[0];
  att(1, 'abort', 'open_admission');
  check('T0', 'switch_attempts: one open-admission abort does not refuse', step0().last_two_open_aborts === 1);
  check('T0', 'switch_attempts: an attempt is recorded once per epoch', att(1, 'abort', 'other').changes === 0);
  att(2, 'abort', 'open_admission');
  check('T0', "step 0 refuses after two open-admission aborts with no HQ decision", step0().last_two_open_aborts === 2 && step0().hq_decision === null);
  runStmt(db, S(2038, 1), { room_id: 'hq-2026-10-xx' });
  check('T0', "HQ's decision recorded on the latest attempt", step0().hq_decision === 'hq-2026-10-xx');
  check('T0', "'switched' with a reason other than 'switched' is refused by the CHECK", (() => { try { att(3, 'switched', 'other'); return false; } catch { return true; } })());
  att(3, 'switched', 'switched');
  check('T0', "'switched' recorded on the verified commit", one(db, 'SELECT outcome FROM switch_attempts WHERE pause_epoch_ms = 3').outcome === 'switched' && step0().last_two_open_aborts === 1);
}
{
  // backstop_runs: CHECK, INSERT gate, serialization, completion terms, drift writes, late notes
  const mk = () => {
    const db = fresh();
    db.prepare("INSERT INTO switch_marks VALUES ('pause_start_ms', 100, 100)").run();
    for (const u of ['ledgertest_s1', 'ledgertest_s2']) db.prepare("INSERT INTO balances VALUES (?, 1, 1, 1, 'snapshot', NULL)").run(u);
    return db;
  };
  const ins = (db, run, purpose = 'step_9b') => runStmt(db, S(2061, 0), { run, purpose, now: T, who: 'george' }).changes;
  const done = (db, run, over = {}) => runStmt(db, S(2061, 3), { run, scan_started: T - 10, scan_finished: T - 5, bl: 2, br: 2, kl: 3, kr: 3, jl: 1, jr: 1, drift: 0, bal_done: 1, idem_done: 1, marks_done: 1, jobs_done: 1, snap_read: 2, snap_missing: 0, ...over });
  const row = (db, run) => one(db, 'SELECT * FROM backstop_runs WHERE run_id = ?', run);
  let db = mk();
  check('T0', 'a run INSERT before 4.15 commits (the sentinel): no row', ins(db, 'r0') === 0);
  setCtl(db, 'switch_at_ms', String(T));
  check('T0', 'a run INSERT after 4.15: recorded, run_pause_ms NULL while open', ins(db, 'r1') === 1 && row(db, 'r1').run_pause_ms === null);
  check('T0', 'a second run while one of this switch is open: no row', ins(db, 'r2') === 0);
  setCtl(db, 'money_pause', '1');
  check('T0', 'a step 9b run still open across the fix-forward pause also blocks', ins(db, 'r2', 'reopen') === 0);
  setCtl(db, 'money_pause', '0');
  for (const [name, over] of [['balances short', { br: 1, snap_read: 1, snap_missing: 1 }], ['keys short', { kr: 2 }], ['jobs short', { jr: 0 }]]) {
    const d = mk(); setCtl(d, 'switch_at_ms', String(T)); ins(d, 'rs'); done(d, 'rs', over);
    check('T0', `a run whose list read is short (${name}): recorded incomplete`, row(d, 'rs').complete === 0 && row(d, 'rs').completed_at_ms !== null);
  }
  for (const flag of ['bal_done', 'idem_done', 'marks_done', 'jobs_done']) {
    const d = mk(); setCtl(d, 'switch_at_ms', String(T)); ins(d, 'rf'); done(d, 'rf', { [flag]: 0 });
    check('T0', `complete refused without every prefix exhausted (${flag} 0)`, row(d, 'rf').complete === 0);
  }
  {
    const d = mk(); setCtl(d, 'switch_at_ms', String(T)); ins(d, 'rm'); done(d, 'rm', { snap_read: 1, snap_missing: 1 });
    check('T0', 'complete refused with a snapshot customer missing', row(d, 'rm').complete === 0);
    const d2 = mk(); setCtl(d2, 'switch_at_ms', String(T)); ins(d2, 'rl'); done(d2, 'rl', { bl: 1, br: 1, snap_read: 1, snap_missing: 1 });
    check('T0', 'a run listing fewer balances than the snapshot imported: incomplete', row(d2, 'rl').complete === 0);
    const d3 = mk(); setCtl(d3, 'switch_at_ms', String(T)); ins(d3, 'ra');
    check('T0', "answers whose read and missing customers do not add up to the snapshot's set: no write", done(d3, 'ra', { snap_read: 1, snap_missing: 0 }).changes === 0 && row(d3, 'ra').completed_at_ms === null);
  }
  for (const [name, over] of [['bl -1', { bl: -1 }], ['br 1.5', { br: 1.5 }], ["kl '3'", { kl: '3' }], ['kr -1', { kr: -1 }], ['jl -1', { jl: -1 }], ['jr NULL', { jr: null }], ['drift -1', { drift: -1 }], ['snap_read 1.5', { snap_read: 1.5 }], ['snap_missing -1', { snap_missing: -1, snap_read: 3 }]]) {
    const d = mk(); setCtl(d, 'switch_at_ms', String(T)); ins(d, 'rn');
    check('T0', `a negative or non-integer count (${name}): no write`, done(d, 'rn', over).changes === 0);
  }
  {
    const d = mk(); setCtl(d, 'switch_at_ms', String(T)); d.exec("DELETE FROM balances WHERE user_id = 'ledgertest_s2'"); ins(d, 'probe');
    const w = done(d, 'probe', { bl: 1, br: 1, kl: -1, kr: -1, jl: -1, jr: -1, drift: 0, snap_read: 1, snap_missing: 0 });
    check('T0', "S2 036's probe (one snapshot customer read, every flag 1, nothing missing, keys and jobs at -1): no write", w.changes === 0 && row(d, 'probe').complete === 0);
  }
  db = mk(); setCtl(db, 'switch_at_ms', String(T)); ins(db, 'ok'); done(db, 'ok');
  check('T0', 'a run with every list read, every prefix exhausted, no customer missing: complete', row(db, 'ok').complete === 1);
  check('T0', 'the next run is recorded once the first is closed', ins(db, 'next') === 1);
  runStmt(db, S(2061, 1), { now: T, run: 'next' });
  check('T0', 'an abandoned run is never complete, and its abandon frees the next', row(db, 'next').abandoned_at_ms === T && done(db, 'next').changes === 0 && row(db, 'next').complete === 0 && ins(db, 'third') === 1);
  check('T0', "backstop_runs' CHECK: complete refused without a completion", (() => { try { db.exec("UPDATE backstop_runs SET complete = 1 WHERE run_id = 'third'"); return false; } catch { return true; } })());
  check('T0', "backstop_runs' CHECK: complete refused with a list read short", (() => { try { db.exec("UPDATE backstop_runs SET br = 1 WHERE run_id = 'ok'"); db.exec("UPDATE backstop_runs SET balances_read = 1 WHERE run_id = 'ok'"); return false; } catch { return true; } })());
  // drift writes
  const drift = (run, epoch, subject = 'ledgertest_s1', found = '{"balance":9}') => runStmt(db, S(2061, 5), { epoch, kind: 'balance', subject, uid: subject, snapshot: '{"balance":1}', found, now: T, run }).changes;
  check('T0', 'a drift write through a completed run: no row', drift('ok', 100) === 0);
  check('T0', 'a drift write through an abandoned run: no row', drift('next', 100) === 0);
  check('T0', 'a drift write through an unknown run: no row', drift('nope', 100) === 0);
  check('T0', 'a drift write through an open run under another epoch: no row', drift('third', 999) === 0);
  check('T0', 'a drift write through the open run of this switch: a row', drift('third', 100) === 1);
  done(db, 'third', { drift: 2 });
  check('T0', 'a run reporting drift with no switch_drift row naming it: incomplete', row(db, 'third').complete === 0);
  runStmt(db, S(2061, 6), { disposition: 'settled', note: 'support:settle:x', who: 'george', now: T, epoch: 100, kind: 'balance', subject: 'ledgertest_s1' });
  ins(db, 'fourth'); T += 1; drift('fourth', 100);
  const dr = () => one(db, "SELECT * FROM switch_drift WHERE subject = 'ledgertest_s1'");
  check('T0', 'a disposed drift re-found unchanged by a later run stays disposed', dr().disposition === 'settled' && dr().last_run_id === 'fourth');
  drift('fourth', 100, 'ledgertest_s1', '{"balance":12}');
  check('T0', "a further change goes back to George, the earlier settlement in the note", dr().disposition === null && dr().note.startsWith('earlier settled: '));
  check('T0', "the undisposed count reads it", runStmt(db, S(2061, 7), {}).rows[0].undisposed === 1);
  // late answers
  const note = (run, n) => runStmt(db, S(2061, 2), { note: n, run }).changes;
  check('T0', 'a NULL late-answer note: no change', note('ok', null) === 0);
  check('T0', 'a blank late-answer note: no change', note('ok', ' \t ') === 0);
  check('T0', 'a late answer on an open run: no change (only closed runs take notes)', note('fourth', 'x') === 0);
  check('T0', 'a late answer kept as a note on a completed run, appended', note('ok', 'late 1') === 1 && note('ok', 'late 2') === 1 && row(db, 'ok').note === 'late 1; late 2' && row(db, 'ok').complete === 1);
}
{
  // switch_obligations: the handoff, its read-back, the re-handoff, the dispositions, the carry, the paid evidence
  const db = fresh();
  const E = 500;
  const hand = (subject, over = {}) => runStmt(db, S(2163, 0), { epoch: E, kind: 'job', subject, uid: 'ledgertest_o', mode: 'create', source: 'hold', evidence: '{"fingerprint":"fp1"}', cost: null, kv_payable: 0, now: T, ...over }).changes;
  check('T0', "the handoff INSERT refused with money open", hand('ledgertest_o1') === 0);
  setCtl(db, 'migration_open', 'scan');
  check('T0', "the handoff INSERT refused with money open in 'scan' (step 1b's open 'scan')", hand('ledgertest_o1') === 0);
  setCtl(db, 'money_pause', '1'); setCtl(db, 'migration_open', 'snapshot');
  check('T0', "the handoff INSERT refused paused outside 'scan'", hand('ledgertest_o1') === 0);
  setCtl(db, 'migration_open', 'scan');
  check('T0', "the handoff INSERT paused in 'scan': a row", hand('ledgertest_o1') === 1 && hand('ledgertest_o2', { kv_payable: 1 }) === 1 && hand('ledgertest_o3', { kv_payable: 1 }) === 1);
  const rb = runStmt(db, S(2163, 1), { epoch: E, subjects: JSON.stringify([{ kind: 'job', id: 'ledgertest_o1' }, { kind: 'job', id: 'ledgertest_o9' }]) }).rows;
  check('T0', 'the handoff read-back: the stored fingerprint and kv_payable, NULL for an unlisted subject', rb[0].fingerprint === 'fp1' && rb[0].kv_payable === 0 && rb[1].fingerprint === null);
  const disp = (subject, disposition, over = {}) => runStmt(db, S(2163, 2), { disposition, note: null, who: 'george', now: T, cost: null, neutralized_ms: null, epoch: E, kind: 'job', subject, ...over }).changes;
  setCtl(db, 'money_pause', '0');
  check('T0', 'the disposition UPDATE refused with money open', disp('ledgertest_o2', 'release1_pays') === 0);
  setCtl(db, 'money_pause', '1');
  check('T0', "'release1_pays' on a payable copy: recorded", disp('ledgertest_o2', 'release1_pays') === 1);
  check('T0', "'release1_pays' on a neutralized row: refused", (() => { try { return disp('ledgertest_o3', 'release1_pays', { neutralized_ms: T }) === 0; } catch { return true; } })());
  check('T0', "a job 'carry' with no cost: refused", disp('ledgertest_o1', 'carry') === 0);
  check('T0', "the job 'carry' accepted with George's :cost", disp('ledgertest_o1', 'carry', { cost: 12 }) === 1);
  const holdList = q(db, "SELECT subject_id, user_id, token_cost FROM switch_obligations WHERE disposition = 'carry' AND subject_kind = 'job'");
  check('T0', 'the hold list shows the carried refund with its amount and job', holdList.length === 1 && holdList[0].token_cost === 12);
  // re-handoff replaces the row and clears its decision
  T += 1;
  hand('ledgertest_o2', { evidence: '{"fingerprint":"fp2"}', kv_payable: 1 });
  const o2 = one(db, "SELECT * FROM switch_obligations WHERE subject_id = 'ledgertest_o2'");
  check('T0', 'a re-handoff replaces the row and clears its decision', o2.disposition === null && o2.decided_at_ms === null && JSON.parse(o2.evidence_json).fingerprint === 'fp2');
  disp('ledgertest_o2', 'release1_pays');
  const change = (subject, over = {}) => runStmt(db, S(2163, 3), { note: 'expires before two sweeps', who: 'george', now: T, cost: 9, neutralized_ms: null, epoch: E, subject, ...over }).changes;
  check('T0', "the change to 'carry' without a note: refused", change('ledgertest_o2', { note: null }) === 0 && change('ledgertest_o2', { note: '  ' }) === 0);
  check('T0', "the change to 'carry' without a cost: refused", change('ledgertest_o2', { cost: null }) === 0);
  check('T0', "the change to 'carry' (found unpaid): recorded", change('ledgertest_o2') === 1
    && one(db, "SELECT disposition, found_unpaid_at_ms FROM switch_obligations WHERE subject_id = 'ledgertest_o2'").found_unpaid_at_ms === T);
  // a subject an earlier abort carried: only 'carry' again
  const E2 = 600;
  runStmt(db, S(2163, 0), { epoch: E2, kind: 'job', subject: 'ledgertest_o1', uid: 'ledgertest_o', mode: 'create', source: 'hold', evidence: '{"fingerprint":"fp3"}', cost: null, kv_payable: 0, now: T });
  const disp2 = (d, over = {}) => runStmt(db, S(2163, 2), { disposition: d, note: 'evidence', who: 'george', now: T, cost: 12, neutralized_ms: null, epoch: E2, kind: 'job', subject: 'ledgertest_o1', ...over }).changes;
  check('T0', "a non-carry disposition for a subject an earlier abort carried: refused", disp2('nothing_owed') === 0);
  check('T0', "carried again: accepted", disp2('carry') === 1);
  // paid evidence on every unsettled carry of the subject, never cleared
  const paid = (ev) => runTx(db, block(1596), { evidence: ev, now: T, kind: 'job', subject: 'ledgertest_o1' });
  let p = paid('{"found":"gen: index"}');
  check('T0', 'payment evidence recorded on every unsettled carry of the subject', p[1].rows.length === 2 && p[1].rows.every((x) => x.paid_found_at_ms === T));
  T += 1; p = paid('{"found":"other"}');
  check('T0', 'payment evidence never cleared or overwritten', p[0].changes === 0 && p[1].rows.every((x) => x.paid_found_at_ms === T - 1));
  // the post-reopen carry of a refusal
  setCtl(db, 'money_pause', '0'); setCtl(db, 'migration_open', '0');
  const post = (event, n) => runStmt(db, S(2163, 4), { epoch: E, event, evidence: '{"refusals":1}', now: T, note: n, who: 'george' }).changes;
  check('T0', "the post-reopen carry of a refusal needs a note", post('ledgertest_ev1', null) === 0 && post('ledgertest_ev1', ' ') === 0);
  check('T0', "the post-reopen carry of a refusal: carried as uncertain, with money open", post('ledgertest_ev1', 'no sign of release 1 by day 30') === 1
    && one(db, "SELECT disposition, source FROM switch_obligations WHERE subject_id = 'ledgertest_ev1'").disposition === 'carry');
  check('T0', 'the reopen gate counts undisposed obligations', runStmt(db, S(2163, 5), {}).rows[0].undisposed === 1);
}
{
  // the rollback abort's stop read
  const db = fresh();
  const stop = () => runStmt(db, S(2235, 0), {}).rows[0];
  check('T0', "the rollback abort's stop read on an empty snapshot: all 0", Object.values(stop()).every((v) => v === 0));
  db.prepare("INSERT INTO balances VALUES ('ledgertest_r', 5, 1, 1, 'snapshot', NULL)").run();
  db.prepare("INSERT INTO ledger (id, user_id, type, amount, reason, source, balance_after, idem_key, created_at_ms) VALUES ('o', 'ledgertest_r', 'opening', 5, 'migrated_from_kv', 'snapshot', 5, 'open:ledgertest_r', 1)").run();
  seedJob(db, { job_id: 'ledgertest_k1', provenance: 'kv', state: 'finished', outcome: 'refunded_legacy', finished_at_ms: 1, token_cost: null, client_key: null, request_hash: null, import_json: '{"class":"refunded_by_evidence"}' });
  seedJob(db, { job_id: 'ledgertest_k2', provenance: 'kv', state: 'claimed', hold_reason: 'index_only', token_cost: 5, client_key: null, request_hash: null });
  check('T0', "the stop read counts neither the snapshot's rows nor an index-only hold", Object.values(stop()).every((v) => v === 0));
  seedJob(db, { job_id: 'ledgertest_k3', provenance: 'kv', state: 'finished', outcome: 'succeeded', finished_at_ms: 1, token_cost: 5, client_key: null, request_hash: null, import_json: '{"class":"x"}' });
  check('T0', 'the stop read counts a result on an imported row (a delivered finish)', stop().claims_and_results === 1);
  db.prepare("INSERT INTO ledger (id, user_id, type, amount, reason, source, balance_after, idem_key, created_at_ms) VALUES ('m', 'ledgertest_r', 'credit', 5, 'x', 'x', 10, 'x:1', 1)").run();
  db.prepare("INSERT INTO stripe_pending (event_id, reason, first_seen_ms) VALUES ('ledgertest_pe', 'no_evidence', 1)").run();
  db.prepare("INSERT INTO charge_recovered VALUES ('ledgertest_ce', 'ch', 'g', 1)").run();
  const s = stop();
  check('T0', 'the stop read counts a movement, an uncarried pending row and a mapping', s.movements === 1 && s.decisions === 1 && s.mappings === 1);
}
{
  // 4.18: settlements, the audited change, the carry statement
  const db = fresh();
  await openUser(db, 'ledgertest_h', 0);
  seedJob(db, { job_id: 'ledgertest_h1', user_id: 'ledgertest_h', provenance: 'kv', state: 'claimed', hold_reason: 'unexplained', token_cost: 8, client_key: null, request_hash: null });
  const settle = (jobId, outcome, meta = null) => runTx(db, block(1548).slice(0, 2), { outcome, code: 'settled', msg: null, meta, job: jobId, now: T });
  seedJob(db, { job_id: 'ledgertest_hx', user_id: 'ledgertest_h', provenance: 'kv', state: 'claimed', token_cost: 8, client_key: null, request_hash: null });
  check('T0', '4.18 the held-row settlement never touches an unheld row', settle('ledgertest_hx', 'refunded_legacy')[0].changes === 0 && job(db, 'ledgertest_hx').finished_at_ms === null);
  check('T0', "4.18 'refunded' without the support credit fails the refunded_amount CHECK", await throws(async () => settle('ledgertest_h1', 'refunded')));
  const r = await L.movement(ctxOf(db), { uid: 'ledgertest_h', type: 'credit', amount: 8, reason: 'support_settlement', source: 'support_settlement', idem: 'support:settle:ledgertest_h1', job: 'ledgertest_h1' });
  check('T0', '4.18 the support credit under support:settle:{jobId}', r.outcome === 'applied');
  let s = settle('ledgertest_h1', 'refunded');
  check('T0', "4.18 the held row settled 'refunded' with the credit's amount; read-back confirms", s[1].rows[0].state === 'finished' && s[1].rows[0].hold_reason === null
    && job(db, 'ledgertest_h1').refunded_amount === 8);
  seedJob(db, { job_id: 'ledgertest_h2', user_id: 'ledgertest_h', provenance: 'kv', state: 'claimed', hold_reason: 'index_only', token_cost: 8, client_key: null, request_hash: null });
  s = settle('ledgertest_h2', 'succeeded', '{"createdAt":"2026-10-04T00:00:00Z"}');
  check('T0', "4.18 a delivered settlement ('succeeded'): published, the index row as meta, status NULL", job(db, 'ledgertest_h2').artifact === 'published' && job(db, 'ledgertest_h2').status_written_at_ms === null);
  await L.tombstone(ctxOf(db), { job: 'ledgertest_h3', uid: 'ledgertest_h', mode: 'create', cost: 5, code: 'no_record' });
  s = runTx(db, block(1548).slice(2), { outcome: 'refunded_legacy', code: 'settled', msg: null, meta: null, job: 'ledgertest_h3', now: T });
  check('T0', '4.18 a no_record tombstone settled refunded_legacy; read-back confirms', s[1].rows[0].outcome === 'refunded_legacy');
  // the audited change of 'apply' to 'none'
  db.prepare("INSERT INTO stripe_pending (event_id, reason, first_seen_ms, disposition, decided_by, decided_at_ms) VALUES ('ledgertest_a1', 'no_evidence', 1, 'apply', 'g', 1)").run();
  db.prepare("INSERT INTO stripe_pending (event_id, reason, first_seen_ms, disposition, decided_by, decided_at_ms) VALUES ('ledgertest_a2', 'no_evidence', 1, 'apply', 'g', 1)").run();
  const audit = (event, note) => runTx(db, block(1582), { note, who: 'george', now: T, event });
  check('T0', "4.18 the audited 'apply' to 'none' without a note: refused", audit('ledgertest_a1', null)[1].rows[0].disposition === 'apply' && audit('ledgertest_a1', '  ')[1].rows[0].disposition === 'apply');
  await credit(db, 'ledgertest_h', 100, 'stripe:ledgertest_a2', { event: 'ledgertest_a2' });
  check('T0', "4.18 the audited change with a stripe: row: refused", audit('ledgertest_a2', 'payment refunded')[1].rows[0].disposition === 'apply');
  const a = audit('ledgertest_a1', 'payment refunded in Stripe');
  check('T0', "4.18 the audited change with its note: 'none', revised_from 'apply'", a[1].rows[0].disposition === 'none' && a[1].rows[0].revised_from === 'apply');
  // the carry statement: refused before the unpause
  db.prepare("INSERT INTO switch_obligations (pause_epoch_ms, subject_kind, subject_id, user_id, mode, source, evidence_json, token_cost, kv_payable, handed_off_at_ms, disposition, decided_at_ms) VALUES (1, 'job', 'ledgertest_h1', 'ledgertest_h', 'create', 'hold', '{}', 8, 0, 1, 'carry', 1)").run();
  let c = runTx(db, block(1605), { now: T });
  check('T0', "4.18's carry statement refused before the unpause (the sentinel)", c[0].changes === 0 && c[1].rows[0].open_carries === 1);
  setCtl(db, 'switch_at_ms', String(T));
  setCtl(db, 'money_pause', '1');
  c = runTx(db, block(1605), { now: T });
  check('T0', "4.18's carry statement refused while paused", c[0].changes === 0);
  setCtl(db, 'money_pause', '0');
  c = runTx(db, block(1605), { now: T });
  check('T0', "4.18's carry statement after the unpause: the finished job's carry settled, none open", c[0].changes === 1 && c[1].rows[0].open_carries === 0);
  check('T0', "step 13's count of holds and no_record rows reads the settled rows", runStmt(db, inline(2278, 0), {}).rows[0]['COUNT(*)'] === 0);
}

// ════════════════════════════════════════════════════════════════════════
// T0 control-row variants on every guarded spec statement outside the library
// ════════════════════════════════════════════════════════════════════════
{
  for (const [label, v, key] of PAUSE_VARIANTS) {
    // 4.18 settlements (held row, tombstone) and the support credit
    const db = fresh();
    await openUser(db, 'ledgertest_g', 0);
    seedJob(db, { job_id: 'ledgertest_g1', user_id: 'ledgertest_g', provenance: 'kv', state: 'claimed', hold_reason: 'contradictory', token_cost: 4, client_key: null, request_hash: null });
    await L.tombstone(ctxOf(db), { job: 'ledgertest_g2', uid: 'ledgertest_g', mode: 'create', cost: 5, code: 'no_record' });
    setCtl(db, 'money_pause', v);
    const s1 = runTx(db, block(1548).slice(0, 2), { outcome: 'refunded_legacy', code: 'x', msg: null, meta: null, job: 'ledgertest_g1', now: T });
    const s2 = runTx(db, block(1548).slice(2), { outcome: 'refunded_legacy', code: 'x', msg: null, meta: null, job: 'ledgertest_g2', now: T });
    check('T0', `4.18 settlements with money_pause ${label}: refused, the read-back pause_value says paused`,
      s1[0].changes === 0 && s2[0].changes === 0 && L.isPausedValue(s1[1].rows[0].pause_value) && L.isPausedValue(s2[1].rows[0].pause_value));
    // S0's admission record
    const ad = runTx(db, block(1745).slice(0, 2), { admission: 'av', route: 'generate', kind: 'job', subject: 's', uid: 'u', meta: '{}', now: T });
    check('T0', `the admission INSERT with money_pause ${label}: no row`, ad[1].rows.length === 0);
  }
}

// ════════════════════════════════════════════════════════════════════════
// T2: the D1 race, 10 runs (the same as T1 on D1)
// ════════════════════════════════════════════════════════════════════════
for (let run = 1; run <= 10; run++) {
  const db = fresh();
  await openUser(db, 'ledgertest_race', 1000);
  const c = ctxOf(db, { hooks: { yield: true } });
  const work = [];
  for (let i = 0; i < 20; i++) {
    work.push(L.movement(c, { uid: 'ledgertest_race', type: 'credit', amount: 500, reason: 'token_pack_purchase', source: 'token_pack_purchase', idem: `stripe:ledgertest_race_${run}_${i}`, event: `ledgertest_race_${run}_${i}` }));
    work.push(L.generationDebit(c, { uid: 'ledgertest_race', job: `ledgertest_racejob_${run}_${i}`, cost: 25, mode: 'create', ckey: `k${i}`, hash: `h${i}` }));
  }
  const res = await Promise.all(work);
  const rows = ledgerRows(db, 'ledgertest_race');
  let chain = true;
  for (let k = 1; k < rows.length; k++) {
    const signed = rows[k].type === 'debit' ? -rows[k].amount : rows[k].amount;
    if (rows[k].balance_after !== rows[k - 1].balance_after + signed) chain = false;
  }
  check('T2', `run ${run}: exactly 10,500, 40 movement rows, a continuous chain`, balanceOf(db, 'ledgertest_race') === 10500
    && rows.length === 41 && rows.filter((r) => r.type !== 'opening').length === 40 && chain
    && res.every((r) => r.outcome === 'applied' || r.outcome === 'charged'));
}

// ════════════════════════════════════════════════════════════════════════
// T4 (offline): replay, through a bundled generate handler in 5.1's order
// ════════════════════════════════════════════════════════════════════════
/** 5.1's money steps as S4 will run them: the identity read (replay step),
 *  then the debit, then q.send and 4.4 only on 'charged'. */
async function generate(ctx, sent, req) {
  const pre = await L.readGenerationIdentity(ctx, req.job).catch(() => undefined);
  if (pre === undefined) return { status: 500 };
  const cls = L.classifyIdentity(pre ?? undefined, req.uid, req.hash, null);
  if (cls === 'old_request' || cls === 'replay') return { status: 200, from: pre.state, outcome: pre.outcome };
  if (cls === 'conflict') return { status: 409 };
  let d = await L.generationDebit(ctx, req);
  if (d.outcome === 'no_balance') {
    await L.openBalance(ctx, { uid: req.uid, amount: 30, reason: 'signup_bonus', source: 'signup', via: 'signup' });
    d = await L.generationDebit(ctx, req);
  }
  switch (d.outcome) {
    case 'charged': sent.push(req.job); await L.markEnqueued(ctx, req.job); return { status: 202 };
    case 'old_request': case 'replay': return { status: 200, from: d.row?.state };
    case 'conflict': return { status: 409 };
    case 'insufficient': return { status: 402 };
    default: return { status: 503, outcome: d.outcome };
  }
}
{
  const db = fresh(); const sent = [];
  await openUser(db, 'ledgertest_t4', 200);
  const req = { uid: 'ledgertest_t4', job: 'ledgertest_t4a', cost: 10, mode: 'create', ckey: 'key1', hash: 'hA' };
  const a = await generate(ctxOf(db), sent, req);
  const b = await generate(ctxOf(db), sent, req);
  check('T4', 'one key twice: one debit, one enqueue, the second replayed', a.status === 202 && b.status === 200 && sent.length === 1
    && q(db, "SELECT 1 FROM ledger WHERE type = 'debit'").length === 1 && job(db, 'ledgertest_t4a').state === 'enqueued');
  const c = await generate(ctxOf(db), sent, { ...req, hash: 'hB' });
  check('T4', 'the same key with a changed prompt: 409', c.status === 409 && sent.length === 1);
  // two different payloads with one key at once, one uncertain
  const reqX = { uid: 'ledgertest_t4', job: 'ledgertest_t4b', cost: 10, mode: 'create', ckey: 'key2', hash: 'hX' };
  const reqY = { ...reqX, hash: 'hY' };
  const lostOnce = ctxOf(db, { hooks: { yield: true } });
  let lost = false; const inner = lostOnce.db.batch;
  lostOnce.db.batch = async (s) => { const r = await inner(s); if (!lost && s.length === 4) { lost = true; throw new Error('response lost'); } return r; };
  const [x, y] = await Promise.all([generate(lostOnce, sent, reqX), generate(ctxOf(db, { hooks: { yield: true } }), sent, reqY)]);
  const statuses = [x.status, y.status].sort().join();
  check('T4', 'two payloads, one key, at once, one uncertain: one charged and enqueued, the other 409 and never enqueued',
    statuses === '202,409' && sent.filter((j) => j === 'ledgertest_t4b').length === 1 && q(db, "SELECT 1 FROM ledger WHERE idem_key = 'debit:ledgertest_t4b'").length === 1);
  // old requests: release 1 terminal identity rows, an imported pending job, a held row, a tombstone
  const before = balanceOf(db, 'ledgertest_t4');
  const olds = [
    ['a success identity row', { provenance: 'kv', state: 'finished', outcome: 'succeeded', finished_at_ms: T, token_cost: null }],
    ['a refunded_legacy identity row', { provenance: 'kv', state: 'finished', outcome: 'refunded_legacy', finished_at_ms: T, token_cost: null }],
    ['an imported pending job', { provenance: 'kv', state: 'claimed', token_cost: 10 }],
    ['a held row', { provenance: 'kv', state: 'claimed', hold_reason: 'no_cost', token_cost: null }],
    ['a tombstone', { provenance: 'tombstone', state: 'finished', outcome: 'no_record', finished_at_ms: T, token_cost: 10 }],
  ];
  let k = 0;
  for (const [name, row] of olds) {
    const id = `ledgertest_t4o${++k}`;
    seedJob(db, { job_id: id, user_id: 'ledgertest_t4', client_key: null, request_hash: null, ...row });
    const r = await generate(ctxOf(db), sent, { ...req, job: id, hash: 'anything' });
    check('T4', `a replay of ${name}: answered from its row (never 409), never charged, never enqueued`, r.status === 200 && r.from === row.state
      && balanceOf(db, 'ledgertest_t4') === before && !sent.includes(id));
    // and the race order: no pre-read row hit (the row lands between the read and the debit)
    const d = await L.generationDebit(ctxOf(db), { ...req, job: id, hash: 'anything' });
    check('T4', `${name} met by the debit itself (after the pre-read): answered from the row, never charged`, d.outcome === 'old_request' && balanceOf(db, 'ledgertest_t4') === before);
  }
}

// ════════════════════════════════════════════════════════════════════════
// T5 daily reward, T6 openings (library)
// ════════════════════════════════════════════════════════════════════════
{
  const db = fresh();
  await openUser(db, 'ledgertest_t5', 30);
  const c = ctxOf(db, { hooks: { yield: true } });
  const res = await Promise.all(Array.from({ length: 5 }, () => L.movement(c, { uid: 'ledgertest_t5', type: 'credit', amount: 5, reason: 'daily_login', source: 'daily_login', idem: 'daily_login:ledgertest_t5:2026-10-04', legacy1: 'token_idempotency:daily_login:ledgertest_t5:2026-10-04', legacy2: null })));
  check('T5', 'daily reward, 5 parallel: one row, one reward (one applied, four replayed)', res.filter((r) => r.outcome === 'applied').length === 1
    && res.filter((r) => r.outcome === 'replayed').length === 4 && balanceOf(db, 'ledgertest_t5') === 35
    && q(db, "SELECT 1 FROM ledger WHERE idem_key = 'daily_login:ledgertest_t5:2026-10-04'").length === 1);
  seedLegacy(db, 'token_idempotency:daily_login:ledgertest_t5:2026-10-05', T + DAY, 'daily_login');
  const r = await L.movement(ctxOf(db), { uid: 'ledgertest_t5', type: 'credit', amount: 5, reason: 'daily_login', source: 'daily_login', idem: 'daily_login:ledgertest_t5:2026-10-05', legacy1: 'token_idempotency:daily_login:ledgertest_t5:2026-10-05', legacy2: null });
  check('T5', "a reward release 1 already paid that day (legacy key): replayed, nothing moved", r.outcome === 'replayed' && balanceOf(db, 'ledgertest_t5') === 35);
}
{
  const db = fresh();
  const c = ctxOf(db, { hooks: { yield: true } });
  const res = await Promise.all(Array.from({ length: 5 }, () => L.openBalance(c, { uid: 'ledgertest_t6', amount: 30, reason: 'signup_bonus', source: 'signup', via: 'signup' })));
  check('T6', 'five parallel first touches: one opening', res.filter((r) => r.outcome === 'opened').length === 1 && res.filter((r) => r.outcome === 'already_open').length === 4
    && q(db, "SELECT 1 FROM ledger WHERE user_id = 'ledgertest_t6'").length === 1 && balanceOf(db, 'ledgertest_t6') === 30);
  setCtl(db, 'money_pause', '1'); setCtl(db, 'migration_open', 'snapshot');
  runTx(db, block(1465), { id: 'imp1', uid: 'ledgertest_t6neg', amount: -10, meta: null, now: T, kv_last: 'x' });
  setCtl(db, 'money_pause', '0'); setCtl(db, 'migration_open', '0');
  const after = await L.openBalance(ctxOf(db), { uid: 'ledgertest_t6neg', amount: 30, reason: 'signup_bonus', source: 'signup', via: 'signup' });
  check('T6', 'a -10 KV balance imported: -10, and a later touch keeps it (already open)', balanceOf(db, 'ledgertest_t6neg') === -10 && after.outcome === 'already_open' && after.balance === -10);
  const z = await L.openBalance(ctxOf(db), { uid: 'ledgertest_t6z', amount: 0, reason: 'legacy_signup', source: 'signup', via: 'zero_alarm' });
  check('T6', 'signup evidence without a balance: 0 with an alarm', z.outcome === 'opened' && z.balance === 0 && z.alarm === 'zero_alarm');
}

// ════════════════════════════════════════════════════════════════════════
// T7 (library): fail closed and uncertain
// ════════════════════════════════════════════════════════════════════════
{
  const dev = (db, fault) => { setCtl(db, 'dev_fault', fault); return ctxOf(db, { appEnv: 'dev' }); };
  const db = fresh(); const sent = [];
  await openUser(db, 'ledgertest_t7', 300);
  const req = (job, hash = 'h') => ({ uid: 'ledgertest_t7', job, cost: 10, mode: 'create', ckey: job, hash });
  let r = await generate(dev(db, 'batch_throw_before:generation'), sent, req('ledgertest_t7a'));
  check('T7', 'D1 error before the batch (batch_throw_before:generation): 503 not charged, nothing enqueued', r.status === 503 && r.outcome === 'not_charged'
    && balanceOf(db, 'ledgertest_t7') === 300 && sent.length === 0);
  r = await generate(dev(db, 'batch_response_lost:generation'), sent, req('ledgertest_t7b'));
  check('T7', 'commit then response lost (batch_response_lost:generation): resolved by key and identity, enqueued once', r.status === 202
    && sent.filter((j) => j === 'ledgertest_t7b').length === 1 && balanceOf(db, 'ledgertest_t7') === 290);
  check('T7', 'a fault scoped to another reason never fires here', (await generate(dev(db, 'batch_response_lost:token_pack_purchase'), sent, req('ledgertest_t7c'))).status === 202);
  check('T7', 'a fault with no scope never fires', (await generate(dev(db, 'batch_throw_before'), sent, req('ledgertest_t7d'))).status === 202);
  setCtl(db, 'dev_fault', 'batch_throw_before:generation');
  check('T7', "the dev fault row is never read outside APP_ENV 'dev'", (await generate(ctxOf(db, { appEnv: 'production' }), sent, req('ledgertest_t7e'))).status === 202);
  setCtl(db, 'dev_fault', undefined);
  // Pages dies before send: candidate (1) refunds once
  await L.generationDebit(ctxOf(db), req('ledgertest_t7f'));
  T += 600_001;
  let sw = (await L.sweepList(ctxOf(db))).filter((x) => x.job_id === 'ledgertest_t7f');
  check('T7', 'Pages dies before send: the row is candidate (1) after 10 minutes', sw.length === 1 && L.sweepCandidate(sw[0]) === 1);
  const b0 = balanceOf(db, 'ledgertest_t7');
  const rr = await L.refundAndFinish(ctxOf(db), { job: 'ledgertest_t7f', fence: 'recovery', code: 'debited_never_enqueued' });
  const rr2 = await L.refundAndFinish(ctxOf(db), { job: 'ledgertest_t7f', fence: 'recovery', code: 'debited_never_enqueued' });
  check('T7', 'recovery refunds once', rr.outcome === 'refunded' && rr2.outcome === 'already_finished' && balanceOf(db, 'ledgertest_t7') === b0 + 10);
  // the compensating refund fails: the Pages catch's refund throws, the recovery refunds once
  await L.generationDebit(ctxOf(db), req('ledgertest_t7g'));
  const catchR = await L.refundAndFinish(dev(db, 'batch_throw_before:generation_failed_refund'), { job: 'ledgertest_t7g', fence: 'pages', code: 'enqueue_failed' });
  setCtl(db, 'dev_fault', undefined);
  check('T7', "the compensating refund fails ('pages' fence, an error before its batch): nothing moved", catchR.outcome !== 'refunded' && job(db, 'ledgertest_t7g').finished_at_ms === null);
  T += 600_001;
  const rec = await L.refundAndFinish(ctxOf(db), { job: 'ledgertest_t7g', fence: 'recovery', code: 'debited_never_enqueued' });
  check('T7', 'then candidate (1) refunds it once', rec.outcome === 'refunded' && q(db, "SELECT 1 FROM ledger WHERE idem_key = 'refund:ledgertest_t7g'").length === 1);
  // an uncertain debit whose identity read finds another execution's row
  await L.generationDebit(ctxOf(db), req('ledgertest_t7h', 'same'));
  let d = await L.generationDebit(ctxOf(db, { hooks: { before: once('timeout') } }), req('ledgertest_t7h', 'same'));
  check('T7', "an uncertain debit, another execution's row with the same hash: replay, no enqueue", d.outcome === 'replay');
  d = await L.generationDebit(ctxOf(db, { hooks: { before: once('timeout') } }), req('ledgertest_t7h', 'different'));
  check('T7', 'the same, a different hash: 409', d.outcome === 'conflict');
  // an uncertain batch that committed, then a failed identity read
  const failRead = ctxOf(db); let n = 0; const inner = failRead.db.batch;
  failRead.db.batch = async (s) => { n++; if (n === 1) { await inner(s); throw new Error('lost'); } throw new Error('read failed'); };
  const sentBefore = sent.length;
  d = await L.generationDebit(failRead, req('ledgertest_t7i'));
  check('T7', 'committed, then a failed identity read: 503 (a charge, if it happened, will be refunded), no enqueue', d.outcome === 'unconfirmed' && sent.length === sentBefore);
  T += 600_001;
  const one1 = await L.refundAndFinish(ctxOf(db), { job: 'ledgertest_t7i', fence: 'recovery', code: 'debited_never_enqueued' });
  check('T7', 'one refund by candidate (1)', one1.outcome === 'refunded' && q(db, "SELECT 1 FROM ledger WHERE idem_key = 'refund:ledgertest_t7i'").length === 1);
  r = await generate(ctxOf(db), sent, req('ledgertest_t7i'));
  check('T7', 'a retry of the request is answered from the row (replay), not charged again', r.status === 200 && q(db, "SELECT 1 FROM ledger WHERE idem_key = 'debit:ledgertest_t7i'").length === 1);
  // each owner update committed with its response lost (R3-17): in T0's 4.7 block, counted here once more by kind
  for (const kind of ['submitted', 'task', 'fallback', 'release_create', 'release_animate', 'stage']) {
    const d2 = fresh();
    seedJob(d2, { job_id: 'ledgertest_o', mode: kind.endsWith('create') || kind === 'submitted' || kind === 'stage' ? 'create' : 'animate', state: 'claimed', claim_id: 'own', claimed_at_ms: T - 1, lease_at_ms: T - 1,
      ...(kind === 'task' ? { submitted_at_ms: T - 1 } : kind === 'release_animate' ? { task_id: 'tk', submitted_at_ms: T - 1 } : {}) });
    const before = { ...job(d2, 'ledgertest_o') };
    const out = await L.ownerUpdate(dev(d2, `batch_response_lost:${kind.startsWith('release') ? 'release' : kind}`), kind, { job: 'ledgertest_o', claim: 'own', task: 'tk1', meta: '{}' });
    const after = job(d2, 'ledgertest_o');
    check('T7', `${kind} committed, response lost: resolved from its own markers before any further step`, out === 'committed' && JSON.stringify(after) !== JSON.stringify(before));
  }
}

// ════════════════════════════════════════════════════════════════════════
// T25 (offline): the settling transitions while paused, and the rerun
// ════════════════════════════════════════════════════════════════════════
for (const [label, v, key] of PAUSE_VARIANTS) {
  const db = fresh();
  await openUser(db, 'ledgertest_25', 0);
  seedJob(db, { job_id: 'ledgertest_25s', user_id: 'ledgertest_25', state: 'claimed', claim_id: 'own', claimed_at_ms: T, lease_at_ms: T, artifact: 'staged', artifact_meta_json: '{}' });
  seedJob(db, { job_id: 'ledgertest_25r4', user_id: 'ledgertest_25', provenance: 'kv', state: 'claimed', token_cost: 5, client_key: null, request_hash: null, created_at_ms: T - DAY });
  seedLegacy(db, 'token_idempotency:refund:ledgertest_25r4', T + DAY, 'refund_job');
  seedJob(db, { job_id: 'ledgertest_25df', user_id: 'ledgertest_25', provenance: 'kv', state: 'claimed', token_cost: 5, client_key: null, request_hash: null, created_at_ms: T - DAY });
  seedJob(db, { job_id: 'ledgertest_25rec', user_id: 'ledgertest_25', state: 'debited', token_cost: 5, created_at_ms: T - DAY });
  db.prepare("INSERT INTO ledger (id, user_id, type, amount, reason, source, job_id, balance_after, idem_key, created_at_ms) VALUES ('d25', 'ledgertest_25', 'debit', 5, 'generation', 'generation', 'ledgertest_25rec', -5, 'debit:ledgertest_25rec', 1)").run();
  seedJob(db, { job_id: 'ledgertest_25h', user_id: 'ledgertest_25', provenance: 'kv', state: 'claimed', hold_reason: 'unexplained', token_cost: 6, client_key: null, request_hash: null });
  setCtl(db, 'money_pause', v);
  const su = await L.successUpdate(ctxOf(db), { job: 'ledgertest_25s', claim: 'own', outcome: 'succeeded' });
  const r4 = await refund(db, { job: 'ledgertest_25r4', fence: 'canceller' });
  const df = await L.deliveredFinish(ctxOf(db), { job: 'ledgertest_25df', meta: '{}' });
  const tb = await L.tombstone(ctxOf(db), { job: 'ledgertest_25t', uid: 'ledgertest_25', mode: 'create', cost: 5, code: 'no_record' });
  const rc = await refund(db, { job: 'ledgertest_25rec', fence: 'recovery' });
  check('T25', `money_pause ${label}: the success update, r4, the delivered finish, the tombstone and the recovery each read back paused (not fence lost, not an existing row)`,
    su === 'paused' && r4.outcome === 'paused' && df === 'paused' && tb.outcome === 'paused' && rc.outcome === 'paused'
    && job(db, 'ledgertest_25r4').finished_at_ms === null && q(db, "SELECT 1 FROM ledger WHERE idem_key LIKE 'refund:%'").length === 0);
  // 4.18: the support credit refused by the pause, then the rerun
  const credit25 = () => L.movement(ctxOf(db), { uid: 'ledgertest_25', type: 'credit', amount: 6, reason: 'support_settlement', source: 'support_settlement', idem: 'support:settle:ledgertest_25h', job: 'ledgertest_25h' });
  const c1 = await credit25();
  const s1 = runTx(db, block(1548).slice(0, 2), { outcome: 'refunded', code: 'settled', msg: null, meta: null, job: 'ledgertest_25h', now: T });
  check('T25', `money_pause ${label}: 4.18's credit and settlement read back paused`, c1.outcome === 'paused' && s1[0].changes === 0 && L.isPausedValue(s1[1].rows[0].pause_value));
  setCtl(db, 'money_pause', '0');
  const c2 = await credit25();
  // a lost reply on the settlement, then the rerun
  const c3 = await credit25();
  const s2 = runTx(db, block(1548).slice(0, 2), { outcome: 'refunded', code: 'settled', msg: null, meta: null, job: 'ledgertest_25h', now: T });
  check('T25', `money_pause ${label}, then open: the rerun reuses support:settle:{jobId}, a second run answers replayed, one credit, the row settled`,
    c2.outcome === 'applied' && c3.outcome === 'replayed' && q(db, "SELECT 1 FROM ledger WHERE idem_key = 'support:settle:ledgertest_25h'").length === 1
    && s2[1].rows[0].state === 'finished' && job(db, 'ledgertest_25h').refunded_amount === 6);
}

// ════════════════════════════════════════════════════════════════════════
// T27 (offline parts): dev isolation
// ════════════════════════════════════════════════════════════════════════
{
  const db = fresh();
  check('T27', "after 0003 the control rows hold money_pause '0', the phase '0' and the sentinel", JSON.stringify(ctl(db)) === JSON.stringify({ migration_open: '0', money_pause: '0', switch_at_ms: '99999999999999' }));
  const idx = await build({ entryPoints: [path.join(ROOT, 'src/index.ts')], bundle: true, platform: 'neutral', format: 'esm', write: false, logLevel: 'error', metafile: true });
  const inputs = Object.keys(idx.metafile.inputs);
  check('T27', 'no release 1 consumer path imports the library: the queue, scheduled and dead-letter handlers bundle without src/ledger.ts', !inputs.some((f) => /src[\\/]ledger\.ts$/.test(f)));
  const src = readFileSync(path.join(ROOT, 'src/ledger.ts'), 'utf8');
  check('T27', 'the library binds no queue and sends nothing (no .send, no Queue type)', !/\.send\(|\bQueue\b|sendBatch/.test(src));
  check('T27', "the library writes no control row except 4.15's and 4.16's own statements",
    Object.entries(L.LEDGER_SQL).filter(([, s]) => /UPDATE control|INSERT INTO control|DELETE FROM control/.test(s)).map(([n]) => n).sort().join() === 'KILL_PAUSE,KILL_REOPEN,REOPEN_BEFORE_0003,UNPAUSE');
}

// ════════════════════════════════════════════════════════════════════════
// T34 (offline): hand settlement (R3-18, R4-10)
// ════════════════════════════════════════════════════════════════════════
{
  const db = fresh();
  await openUser(db, 'ledgertest_34', 0);
  seedJob(db, { job_id: 'ledgertest_34a', user_id: 'ledgertest_34', provenance: 'kv', state: 'claimed', hold_reason: 'unexplained', token_cost: 9, client_key: null, request_hash: null });
  const cr = (ctx) => L.movement(ctx, { uid: 'ledgertest_34', type: 'credit', amount: 9, reason: 'support_settlement', source: 'support_settlement', idem: 'support:settle:ledgertest_34a', job: 'ledgertest_34a' });
  let c = await cr(ctxOf(db));
  setCtl(db, 'money_pause', '1');
  let s = runTx(db, block(1548).slice(0, 2), { outcome: 'refunded', code: 'settled', msg: null, meta: null, job: 'ledgertest_34a', now: T });
  check('T34', 'a support credit applied, the settlement refused by a pause', c.outcome === 'applied' && s[0].changes === 0 && L.isPausedValue(s[1].rows[0].pause_value));
  setCtl(db, 'money_pause', '0');
  c = await cr(ctxOf(db));
  s = runTx(db, block(1548).slice(0, 2), { outcome: 'refunded', code: 'settled', msg: null, meta: null, job: 'ledgertest_34a', now: T });
  check('T34', "the credit runs again under support:settle:{jobId} and answers replayed; the settlement finishes the row with that credit's amount; one credit in all",
    c.outcome === 'replayed' && s[1].rows[0].state === 'finished' && job(db, 'ledgertest_34a').refunded_amount === 9 && balanceOf(db, 'ledgertest_34') === 9);
  seedJob(db, { job_id: 'ledgertest_34b', user_id: 'ledgertest_34', provenance: 'kv', state: 'claimed', hold_reason: 'unexplained', token_cost: 4, client_key: null, request_hash: null });
  const lost = ctxOf(db, { hooks: { after: once('lost') } });
  c = await L.movement(lost, { uid: 'ledgertest_34', type: 'credit', amount: 4, reason: 'support_settlement', source: 'support_settlement', idem: 'support:settle:ledgertest_34b', job: 'ledgertest_34b' });
  const again = await L.movement(ctxOf(db), { uid: 'ledgertest_34', type: 'credit', amount: 4, reason: 'support_settlement', source: 'support_settlement', idem: 'support:settle:ledgertest_34b', job: 'ledgertest_34b' });
  check('T34', 'a lost reply on the credit: read back applied; the rerun replayed; one credit', c.outcome === 'applied' && again.outcome === 'replayed' && balanceOf(db, 'ledgertest_34') === 13);
  // a delivered settlement ('succeeded') whose status write fails, and a 4.12 delivered finish whose status write fails
  seedJob(db, { job_id: 'ledgertest_34c', user_id: 'ledgertest_34', provenance: 'kv', state: 'claimed', hold_reason: 'index_only', token_cost: 4, client_key: null, request_hash: null });
  runTx(db, block(1548).slice(0, 2), { outcome: 'succeeded', code: null, msg: null, meta: '{"createdAt":"2026-10-04T00:00:00Z","w":64}', job: 'ledgertest_34c', now: T });
  seedJob(db, { job_id: 'ledgertest_34d', user_id: 'ledgertest_34', provenance: 'kv', state: 'claimed', token_cost: 4, client_key: null, request_hash: null, created_at_ms: T - DAY });
  await L.deliveredFinish(ctxOf(db), { job: 'ledgertest_34d', meta: '{"createdAt":"2026-10-04T00:00:01Z"}' });
  const rep = await L.repairList(ctxOf(db));
  const pick = (id) => rep.rows.find((x) => x.job_id === id);
  check('T34', "the failed status write leaves the marker NULL: the repair list returns each row with artifact_meta_json and no repair_record (4.11's rebuild row: from the PNG and artifact_meta_json)",
    rep.outcome === 'ok' && ['ledgertest_34c', 'ledgertest_34d'].every((id) => pick(id) && pick(id).status_written_at_ms === null && pick(id).repair_record === null
      && JSON.parse(pick(id).artifact_meta_json).createdAt && pick(id).outcome === 'succeeded' && pick(id).provenance === 'kv'));
}

// ── Summary ──
const order = ['T0', 'T2', 'T3b', 'T4', 'T5', 'T6', 'T7', 'T25', 'T27', 'T34'];
let total = 0;
for (const t of order) {
  const c = counts.get(t) ?? { pass: 0, fail: 0 };
  total += c.pass + c.fail;
  console.log(`[ledger-s1-test] ${t}: ${c.pass}/${c.pass + c.fail}`);
}
console.log(`[ledger-s1-test] ${failed === 0 ? 'PASS' : 'FAIL'}: ${total - failed}/${total}`);
process.exit(failed === 0 ? 0 : 1);
