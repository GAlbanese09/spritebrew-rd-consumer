// scripts/ledger-s2-test.mjs
//
// S2's offline harness (n1-release-2-spec.md revision 9: section 11's S2 row,
// 4.17, 7.2, section 8; 10.1's T6 import, T22, T23, T26, T27, T31 to T33, and
// O14's mutation-phase check). Run from the repo root:
// `node scripts/ledger-s2-test.mjs`.
//
// src/index.ts is bundled with esbuild into dist/.ledger-s2-test (gitignored)
// and driven through its `fetch`, so the gate, the routes and the phase checks
// run as deployed. KV and R2 are in-memory stubs with list cursors and expiry;
// D1 is node:sqlite with migrations 0001 to 0003, behind a D1-shaped adapter
// (ordered parameters, integers as INTEGER, one transaction per batch). The
// steps George runs by hand (the phase moves, step 2, the backstop's run
// record) run the spec's SQL as written from scripts/ledger-s1-spec-sql.json.
//
// LEDGER_MUTATION='{"target":"lib","from":"...","to":"..."}' mutates
// src/migrator.ts at bundle time (exit 1 caught, 0 survived, 3 not found
// once). Output: counts per test and the name of any failing case. Synthetic
// ledgertest_ ids only; the token is a placeholder made for this run.

import { build } from 'esbuild';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

process.removeAllListeners('warning');
const { DatabaseSync } = await import('node:sqlite');
const ROOT = process.cwd();
const OUT = path.join(ROOT, 'dist', '.ledger-s2-test');
const MUT = process.env.LEDGER_MUTATION ? JSON.parse(process.env.LEDGER_MUTATION) : null;
const FIX = JSON.parse(readFileSync(path.join(ROOT, 'scripts', 'ledger-s1-spec-sql.json'), 'utf8'));

const mutationPlugin = {
  name: 'mutation',
  setup(b) {
    if (!MUT) return;
    b.onLoad({ filter: /src[\\/]migrator\.ts$/ }, (args) => {
      const src = readFileSync(args.path, 'utf8');
      const n = src.split(MUT.from).length - 1;
      if (n !== 1) { console.log(`[ledger-s2-test] mutation target found ${n} times`); process.exit(3); }
      return { contents: src.replace(MUT.from, MUT.to), loader: 'ts' };
    });
  },
};
await build({
  entryPoints: { index: path.join(ROOT, 'src/index.ts'), migrator: path.join(ROOT, 'src/migrator.ts'), ledger: path.join(ROOT, 'src/ledger.ts') },
  bundle: true, platform: 'neutral', format: 'esm', outdir: OUT, logLevel: 'error',
  outExtension: { '.js': '.mjs' }, plugins: [mutationPlugin],
});
const v = `?v=${Date.now()}`;
const worker = (await import(pathToFileURL(path.join(OUT, 'index.mjs')).href + v)).default;
const M = await import(pathToFileURL(path.join(OUT, 'migrator.mjs')).href + v);
const L = await import(pathToFileURL(path.join(OUT, 'ledger.mjs')).href + v);

const counts = new Map();
let failed = 0;
function check(tag, name, ok) {
  const c = counts.get(tag) ?? { pass: 0, fail: 0 };
  if (ok) c.pass++; else { c.fail++; failed++; console.log(`[ledger-s2-test] FAIL ${tag}: ${name}`); }
  counts.set(tag, c);
}

// ── The spec's text ──

function codePart(l) {
  let q = false;
  for (let k = 0; k < l.length; k++) {
    if (l[k] === "'") q = !q;
    if (!q && l.startsWith('--', k)) return l.slice(0, k);
  }
  return l;
}
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
const S = (line, i) => splitBlock(FIX.blocks.find((b) => b.line === line).text)[i];

// ── Clock ──

let T = 1_800_000_000_000;
const MIN = 60_000, HOUR = 3_600_000, DAY = 86_400_000;
const realNow = Date.now;
Date.now = () => T;

// ── D1 ──

const toBind = (x) => (typeof x === 'number' && Number.isInteger(x) ? BigInt(x) : typeof x === 'boolean' ? (x ? 1n : 0n) : x ?? null);
const isRead = (sql) => /^\s*(?:--[^\n]*\n\s*)*(SELECT|WITH)\b/i.test(sql);
const stripLits = (sql) => sql.replace(/'(?:[^']|'')*'/g, "''").replace(/--[^\n]*/g, '');
function fresh() {
  const db = new DatabaseSync(':memory:');
  for (const f of ['0001_control.sql', '0002_stripe_refusals.sql', '0003_ledger.sql']) db.exec(readFileSync(path.join(ROOT, 'migrations-ledger', f), 'utf8'));
  return db;
}
let d1Calls = 0;
/** d1.race = { match, run }: runs once, just before the first batch whose SQL
 *  matches, as a control change landing between the gate's read and the batch. */
d1.race = null;
function d1(db) {
  const exec = (s) => {
    const p = db.prepare(s.sql);
    const vals = s.values.map(toBind);
    if (isRead(s.sql)) return { results: p.all(...vals).map((r) => ({ ...r })), meta: { changes: 0 } };
    const r = p.run(...vals);
    return { results: [], meta: { changes: Number(r.changes) } };
  };
  return {
    prepare(sql) {
      d1Calls++;
      if (/:[a-z_]/.test(stripLits(sql))) throw new Error('D1: named parameters are not supported');
      const st = { sql, values: [] };
      st.bind = (...a) => { st.values = a; return st; };
      st.all = async () => { d1Calls++; return exec(st); };
      st.first = async () => (await st.all()).results[0] ?? null;
      st.run = async () => { d1Calls++; return exec(st); };
      return st;
    },
    async batch(stmts) {
      d1Calls++;
      if (d1.race && stmts.some((s) => d1.race.match.test(s.sql))) { const r = d1.race; d1.race = null; r.run(db); }
      db.exec('BEGIN');
      try { const r = stmts.map(exec); db.exec('COMMIT'); return r; } catch (e) { db.exec('ROLLBACK'); throw e; }
    },
  };
}
function runStmt(db, sql, params = {}) {
  const names = [...new Set([...stripLits(sql).matchAll(/:([a-z_][a-z0-9_]*)/g)].map((m) => m[1]))];
  const bound = Object.fromEntries(names.map((n) => { if (!(n in params)) throw new Error(`harness: :${n}`); return [n, toBind(params[n])]; }));
  const st = db.prepare(sql);
  if (isRead(sql)) return { rows: st.all(bound).map((r) => ({ ...r })), changes: 0 };
  return { rows: [], changes: Number(st.run(bound).changes) };
}
const q = (db, sql, ...a) => db.prepare(sql).all(...a.map(toBind)).map((r) => ({ ...r }));
const one = (db, sql, ...a) => q(db, sql, ...a)[0];
const setCtl = (db, key, value, at = T) => {
  if (value === undefined) { db.prepare('DELETE FROM control WHERE key = ?').run(key); return; }
  db.prepare("INSERT INTO control (key, value, updated_at_ms, updated_by) VALUES (?, ?, ?, 'test') ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at_ms = excluded.updated_at_ms").run(key, value, toBind(at));
};
const dump = (db) => ['control', 'balances', 'ledger', 'jobs', 'legacy_idem', 'stripe_pending', 'switch_marks', 'switch_obligations', 'switch_drift', 'backstop_runs']
  .map((t) => JSON.stringify(q(db, `SELECT * FROM ${t}`))).join('|');

// ── KV and R2 ──

let storeCalls = 0;
function kvStub() {
  const m = new Map();
  const live = (k) => { const e = m.get(k); return e && (e.exp === undefined || e.exp * 1000 > T) ? e : null; };
  const kv = {
    m, failList: null,
    async get(k) { storeCalls++; return live(k)?.value ?? null; },
    async put(k, value, opts) { storeCalls++; kv.puts++; m.set(k, { value, exp: opts?.expiration ?? (opts?.expirationTtl ? Math.floor(T / 1000) + opts.expirationTtl : undefined) }); },
    async delete(k) { m.delete(k); },
    async list({ prefix = '', cursor, limit = 1000 } = {}) {
      storeCalls++;
      if (kv.failList && prefix.startsWith(kv.failList)) throw new Error('KV list failed');
      const names = [...m.keys()].filter((k) => k.startsWith(prefix) && live(k)).sort();
      const start = cursor ? Number(cursor) : 0;
      const page = names.slice(start, start + limit);
      const done = start + limit >= names.length;
      return { keys: page.map((name) => ({ name, ...(m.get(name).exp ? { expiration: m.get(name).exp } : {}) })), list_complete: done, ...(done ? {} : { cursor: String(start + limit) }) };
    },
    puts: 0,
  };
  return kv;
}
function r2Stub() {
  const m = new Map();
  const live = (k) => { const e = m.get(k); return e && (e.exp === undefined || e.exp > T) ? e : null; };
  const r2 = {
    m, puts: 0,
    async get(k) { storeCalls++; const e = live(k); return e ? { text: async () => e.body } : null; },
    async head(k) { storeCalls++; return live(k) ? {} : null; },
    async put(k, body) { storeCalls++; r2.puts++; m.set(k, { body: String(body) }); return {}; },
    async list({ prefix = '', cursor, limit = 1000 } = {}) {
      storeCalls++;
      const keys = [...m.keys()].filter((k) => k.startsWith(prefix) && live(k)).sort();
      const start = cursor ? Number(cursor) : 0;
      const page = keys.slice(start, start + limit);
      const truncated = start + limit < keys.length;
      return { objects: page.map((key) => ({ key })), truncated, ...(truncated ? { cursor: String(start + limit) } : {}) };
    },
  };
  return r2;
}

const TOKEN = 'test-migrate-token-0123456789abcdef-not-a-secret';
function world({ phase = '0', pause = '0', pauseAt = T } = {}) {
  const db = fresh();
  setCtl(db, 'migration_open', phase);
  setCtl(db, 'money_pause', pause, pauseAt);
  const kv = kvStub();
  const r2 = r2Stub();
  const env = { SPRITEBREW_KV: kv, GALLERY_BUCKET: r2, LEDGER_DB: d1(db), EVENTS_DB: d1(fresh()), APP_ENV: 'dev', MIGRATE_TOKEN: TOKEN };
  return { db, kv, r2, env };
}
const BASE = 'https://spritebrew-rd-consumer-dev.example.workers.dev';
async function call(w, pathQ, { token = TOKEN, method = 'POST', auth } = {}) {
  const headers = auth !== undefined ? auth : token === null ? {} : { Authorization: `Bearer ${token}` };
  const r = await worker.fetch(new Request(BASE + pathQ, { method, headers }), w.env, {});
  const text = await r.text();
  let body = null;
  try { body = text ? JSON.parse(text) : null; } catch { body = text; }
  return { status: r.status, body, text };
}
async function drain(w, pathQ, maxCalls = 200) {
  const answers = [];
  let cursor = null;
  for (let i = 0; i < maxCalls; i++) {
    const sep = pathQ.includes('?') ? '&' : '?';
    const r = await call(w, cursor ? `${pathQ}${sep}cursor=${encodeURIComponent(cursor)}` : pathQ);
    answers.push(r);
    if (r.status !== 200 || r.body?.done || r.body?.stopped || r.body?.abort || !r.body?.cursor) break;
    cursor = r.body.cursor;
  }
  return answers;
}
const last = (a) => a[a.length - 1];

// ── Fixtures: release 1's records ──

const U = (n) => `ledgertest_${n}`;
const J = (n) => `ledgertest_job_${n}`;
const rid = (n) => `gen:${U(1)}:${1700000000000 + n}:r${n}`;
const base = (n, extra = {}) => ({ userId: U(1), mode: 'create', enqueuedAt: T - 2 * HOUR + n, ...extra });
const C = {
  success: (n, x = {}) => ({ status: 'success', ...base(n), startedAt: T - HOUR, completedAt: T - HOUR, resultBase64: 'AAAA', ...x }),
  refunded: (n, x = {}) => ({ status: 'error', ...base(n), failedAt: T - HOUR, error: 'rd failed', errorCode: 'provider_error', attempts: 1, refunded: true, ...x }),
  settled: (n) => C.refunded(n, { refundSettled: { by: 'sweep', at: T - HOUR, evidence: 'x' } }),
  owed: (n, x = {}) => ({ status: 'error', ...base(n), failedAt: T - HOUR, error: 'enqueue failed', errorCode: 'submission_failed', attempts: 0, refunded: false,
    refundOwed: { tokenCost: 16, reason: 'refund_credit_failed', requestId: rid(n), idempotencyKey: `refund:${rid(n)}`, balanceWritten: false }, ...x }),
  plain: (n) => ({ status: 'error', ...base(n), failedAt: T - HOUR, error: 'x', errorCode: 'provider_error', attempts: 1, refunded: false }),
  due: (n, x = {}) => ({ status: 'running', ...base(n), startedAt: T - HOUR, attempt: 1, tokenCost: 10, refundDue: { errorCode: 'provider_error', at: T - HOUR, error: 'rd 500' }, ...x }),
  pending: (n, x = {}) => ({ status: 'pending', ...base(n), tokenCost: 5, requestId: rid(n), ...x }),
  running: (n, x = {}) => ({ status: 'running', ...base(n), startedAt: T - HOUR, attempt: 1, tokenCost: 12, ...x }),
};
const noKeys = { consumer: false, pages: {} };

// ════════════════════════════════════════════════════════════════════════
// The spec's text
// ════════════════════════════════════════════════════════════════════════

if (!MUT) {
  const spec = new Set(FIX.blocks.flatMap((b) => splitBlock(b.text)));
  const notSpec = Object.entries(M.MIGRATOR_SQL).filter(([, s]) => !spec.has(s)).map(([n]) => n);
  check('spec', `the migrator's ${Object.keys(M.MIGRATOR_SQL).length} statements each equal a spec statement as written${notSpec.length ? ' (not: ' + notSpec.join(', ') + ')' : ''}`, notSpec.length === 0);
  const have = new Set(Object.values(M.MIGRATOR_SQL));
  const need = [1454, 1465, 1478, 1489, 1505, 1528].flatMap((l) => splitBlock(FIX.blocks.find((b) => b.line === l).text)).concat([S(2163, 0), S(2163, 1)]);
  check('spec', "every 4.17 import statement and the handoff's INSERT and read-back are in the migrator", need.every((s) => have.has(s)));
  const src = readFileSync(path.join(ROOT, 'src/migrator.ts'), 'utf8');
  check('spec', 'no em dash in the migrator', !src.includes('\u2014'));
}

// ════════════════════════════════════════════════════════════════════════
// O14: the gate, the paths, the phases
// ════════════════════════════════════════════════════════════════════════

{
  const w = world({ phase: '0' });
  const before = dump(w.db);
  const quiet = async (name, pq, opts) => {
    d1Calls = 0; storeCalls = 0;
    const r = await call(w, pq, opts);
    check('O14', `${name}: 404, no body, no D1, KV or R2 call`, r.status === 404 && r.text === '' && d1Calls === 0 && storeCalls === 0);
  };
  await quiet('no Authorization header', '/admin/ledger/scan?verify=1', { token: null });
  await quiet('a wrong token', '/admin/ledger/scan?verify=1', { token: TOKEN + 'x' });
  await quiet('the token one character short', '/admin/ledger/scan?verify=1', { token: TOKEN.slice(0, -1) });
  await quiet('the token without "Bearer "', '/admin/ledger/scan?verify=1', { auth: { Authorization: TOKEN } });
  await quiet('"bearer" in lower case', '/admin/ledger/scan?verify=1', { auth: { Authorization: `bearer ${TOKEN}` } });
  await quiet('the token in the query instead', `/admin/ledger/scan?verify=1&token=${TOKEN}`, { token: null });
  w.env.MIGRATE_TOKEN = undefined;
  await quiet('MIGRATE_TOKEN unset (even an empty Bearer)', '/admin/ledger/scan?verify=1', { auth: { Authorization: 'Bearer ' } });
  w.env.MIGRATE_TOKEN = 'short-token';
  await quiet('a MIGRATE_TOKEN under 32 characters, presented exactly', '/admin/ledger/scan?verify=1', { token: 'short-token' });
  w.env.MIGRATE_TOKEN = TOKEN;
  for (const [name, pq, method] of [
    ['GET', '/admin/ledger/scan?verify=1', 'GET'], ['PUT', '/admin/ledger/snapshot', 'PUT'],
    ['a trailing slash', '/admin/ledger/scan/?verify=1'], ['an export path (none exists)', '/admin/ledger/export'],
    ['another path', '/admin/ledger'], ['the root', '/'], ['a case change', '/admin/Ledger/scan?verify=1'],
    ['two flags', '/admin/ledger/scan?verify=1&handoff=1'], ['a flag not 1', '/admin/ledger/scan?verify=true'],
    ['an unknown query key', '/admin/ledger/scan?verify=1&x=1'], ['a repeated key', '/admin/ledger/scan?verify=1&verify=1'],
    ['a repeated cursor', '/admin/ledger/scan?verify=1&cursor=a&cursor=b'],
    ['a flag on the snapshot', '/admin/ledger/snapshot?verify=1'],
  ]) await quiet(`the right token, ${name}`, pq, method ? { method } : {});
  // Phase '0': only scan?verify=1, and it writes nothing.
  for (const pq of ['/admin/ledger/scan', '/admin/ledger/scan?classify=1', '/admin/ledger/scan?handoff=1', '/admin/ledger/snapshot']) {
    const r = await call(w, pq);
    check('O14', `phase '0': ${pq} answers 404`, r.status === 404 && r.text === '');
  }
  w.kv.m.set('token_balance:ledgertest_1', { value: JSON.stringify({ balance: 5 }) });
  w.kv.m.set('job:ledgertest_job_1', { value: JSON.stringify(C.running(1)) });
  const kvPuts = w.kv.puts, r2Puts = w.r2.puts;
  const a = await drain(w, '/admin/ledger/scan?verify=1');
  check('O14', "phase '0': scan?verify=1 answers 200 and writes nothing (no D1 row, no KV put, no R2 put)",
    last(a).status === 200 && last(a).body.done === true && dump(w.db) === before && w.kv.puts === kvPuts && w.r2.puts === r2Puts);
  setCtl(w.db, 'migration_open', 'verify');
  for (const pq of ['/admin/ledger/scan?verify=1', '/admin/ledger/scan', '/admin/ledger/snapshot']) {
    check('O14', `phase 'verify' (reads only, by hand): ${pq} answers 404`, (await call(w, pq)).status === 404);
  }
  setCtl(w.db, 'migration_open', undefined);
  check('O14', 'the phase row absent: every path 404', (await call(w, '/admin/ledger/scan?verify=1')).status === 404);
  setCtl(w.db, 'migration_open', 'x');
  check('O14', 'an invalid phase: every path 404', (await call(w, '/admin/ledger/scan?verify=1')).status === 404);
  const broken = world({ phase: 'scan' });
  broken.env.LEDGER_DB = { prepare() { throw new Error('D1 down'); }, batch: async () => { throw new Error('D1 down'); } };
  check('O14', 'the control read failing: 404 (fail closed)', (await call(broken, '/admin/ledger/scan')).status === 404);
  broken.env.LEDGER_DB = undefined;
  check('O14', 'no LEDGER_DB binding: 404', (await call(broken, '/admin/ledger/scan')).status === 404);
  // The phase matrix.
  const matrix = [
    ['scan', '0', { scan: 200, classify: 200, handoff: 404, verify: 200, snapshot: 404 }],
    ['scan', '1', { scan: 200, classify: 200, handoff: 200, verify: 200, snapshot: 404 }],
    ['snapshot', '0', { scan: 404, classify: 404, handoff: 404, verify: 404, snapshot: 404 }],
    ['snapshot', '1', { scan: 404, classify: 404, handoff: 404, verify: 404, snapshot: 200 }],
    ['0', '1', { scan: 404, classify: 404, handoff: 404, verify: 200, snapshot: 404 }],
  ];
  const paths = { scan: '/admin/ledger/scan', classify: '/admin/ledger/scan?classify=1', handoff: '/admin/ledger/scan?handoff=1', verify: '/admin/ledger/scan?verify=1', snapshot: '/admin/ledger/snapshot' };
  for (const [phase, pause, want] of matrix) {
    const m = world({ phase, pause });
    if (phase === 'snapshot') m.db.prepare("INSERT INTO switch_marks VALUES ('pause_start_ms', ?, ?)").run(toBind(T), toBind(T));
    let ok = true;
    for (const [mode, code] of Object.entries(want)) {
      const r = await call(m, paths[mode]);
      if (r.status !== code) { ok = false; console.log('   matrix', phase, pause, mode, r.status); }
    }
    check('O14', `phase '${phase}', money_pause '${pause}': each path answers as section 8's table admits`, ok);
  }
  // The handoff writes nothing unless paused in 'scan'; the imports refuse outside paused 'snapshot'.
  for (const [phase, pause] of [['0', '0'], ['0', '1'], ['scan', '0'], ['snapshot', '1'], ['verify', '1']]) {
    const h = world({ phase, pause });
    h.r2.m.set('jobs/ledgertest_job_9.json', { body: JSON.stringify(C.owed(9)) });
    const b4 = dump(h.db);
    await call(h, '/admin/ledger/scan?handoff=1');
    check('O14', `?handoff=1 in phase '${phase}', money_pause '${pause}': writes nothing`, dump(h.db) === b4);
  }
  for (const [phase, pause] of [['0', '0'], ['0', '1'], ['scan', '1'], ['snapshot', '0'], ['verify', '1']]) {
    const s = world({ phase, pause });
    s.db.prepare("INSERT INTO switch_marks VALUES ('pause_start_ms', ?, ?)").run(toBind(T), toBind(T));
    s.kv.m.set('token_balance:ledgertest_1', { value: JSON.stringify({ balance: 5 }) });
    const b4 = dump(s.db);
    const r = await call(s, '/admin/ledger/snapshot');
    check('O14', `the snapshot in phase '${phase}', money_pause '${pause}': 404, nothing imported`, r.status === 404 && dump(s.db) === b4);
  }
  check('O14', "release 1's queue and scheduled handlers are still exported beside fetch", typeof worker.queue === 'function' && typeof worker.scheduled === 'function' && typeof worker.fetch === 'function');
}

// ════════════════════════════════════════════════════════════════════════
// T23 and T32: 7.2's classification, every row, KV only, R2 only, both
// ════════════════════════════════════════════════════════════════════════

const cls = (kv, r2, keys = noKeys, opts = {}) => M.classifyJob('ledgertest_job_x', { kv, r2, keys }, { now: T, barrierProven: true, ...opts });
const both = async (name, mk, want, keys = noKeys, opts = {}) => {
  for (const [where, kv, r2] of [['KV only', mk(1), null], ['R2 only', null, mk(1)], ['both copies', mk(1), mk(1)]]) {
    const k = await cls(kv, r2, keys, opts);
    check('T23', `${name}, ${where}: ${want}`, k.cls === want);
  }
};
{
  // row 1
  for (const [where, kv, r2] of [['KV only', C.running(1), null], ['R2 only', null, C.running(1)], ['both', C.running(1), C.running(1)], ['beside a pending copy', C.running(1), C.pending(1)]]) {
    const before = await cls(kv, r2, noKeys, { barrierProven: false });
    check('T23', `row 1, a running copy ${where}, before the barrier: the snapshot stops`, before.cls === null && /barrier/.test(before.stop));
    const after = await cls(kv, r2);
    check('T23', `row 1, a running copy ${where}, after the barrier: held 'unexplained' with its cost`, after.cls === 'hold_running' && after.row.hold === 'unexplained' && after.row.cost === 12 && after.row.state === 'claimed');
  }
  const noCost = await cls(C.running(1, { tokenCost: undefined }), null);
  check('T23', 'row 1 without a cost on the copy: held, cost NULL', noCost.cls === 'hold_running' && noCost.row.cost === null);
  check('T23', 'a running copy with the consumer key (positive evidence) is not row 1: refunded', (await cls(C.running(1), null, { consumer: true, pages: {} })).cls === 'refunded_by_evidence');
  // row 2
  for (const [name, other, keys] of [['refunded', C.refunded(1), noKeys], ['owed', C.owed(1), noKeys], ['refund due', C.due(1), noKeys], ['plain unrefunded error', C.plain(1), noKeys]]) {
    const k = await cls(C.success(1), other, keys);
    const k2 = await cls(other, C.success(1), keys);
    check('T23', `row 2, success against ${name} (either copy): contradictory`, k.cls === 'hold_contradictory' && k2.cls === 'hold_contradictory' && k.row.hold === 'contradictory');
  }
  check('T23', "row 2, success with the consumer's key: contradictory", (await cls(C.success(1), null, { consumer: true, pages: {} })).cls === 'hold_contradictory');
  // row 3
  check('T23', 'row 3, refunded on one copy, owed on the other: contradictory', (await cls(C.refunded(1), C.owed(1))).cls === 'hold_contradictory');
  check('T23', 'row 3, refunded on one copy, a plain error on the other: contradictory', (await cls(C.plain(1), C.refunded(1))).cls === 'hold_contradictory');
  // row 4
  await both('row 4, an error refunded: true', C.refunded, 'refunded_by_evidence');
  await both('row 4, a settled copy (refundSettled)', C.settled, 'refunded_by_evidence');
  await both("row 4, an owed copy with balanceWritten", (n) => C.owed(n, { refundOwed: { ...C.owed(n).refundOwed, balanceWritten: true } }), 'refunded_by_evidence');
  await both("row 4, a plain error with the consumer's key", C.plain, 'refunded_by_evidence', { consumer: true, pages: {} });
  await both("row 4, a pending copy with Pages' key", C.pending, 'refunded_by_evidence', { consumer: false, pages: { [rid(1)]: true } });
  await both("row 4, an owed copy with Pages' key", C.owed, 'refunded_by_evidence', { consumer: false, pages: { [rid(1)]: true } });
  let k = await cls(C.refunded(1), C.pending(1));
  check('T23', "row 4, refunded on the KV copy with the R2 copy pending (S2 022's example): refunded, no second refund", k.cls === 'refunded_by_evidence' && k.row.outcome === 'refunded_legacy' && k.row.cost === null);
  k = await cls(C.refunded(1), null);
  check('T23', 'row 4 from KV only: status NULL (the repair pass writes it)', k.row.status_at === null && k.row.finished === T && k.row.code === 'provider_error');
  k = await cls(null, C.refunded(1));
  check('T23', 'row 4 with the R2 copy already refunded: status set', k.row.status_at === T);
  k = await cls(C.pending(1), null, { consumer: false, pages: { [rid(1)]: true } });
  check('T23', "row 4, a pending copy: error_code 'submission_failed'", k.row.code === 'submission_failed');
  // row 5 (and T32)
  await both('row 5, success', C.success, 'identity_succeeded');
  k = await cls(C.success(1), null);
  check('T32', 'a KV-only success: marker NULL, repair_record keeps the KV copy', k.row.status_at === null && JSON.parse(k.row.import_json).repair_record?.status === 'success' && k.row.artifact === 'published');
  k = await cls(C.success(1), C.running(1));
  check('T32', 'a KV success with a stale R2 copy (running): marker NULL, repair_record kept', k.cls === 'identity_succeeded' && k.row.status_at === null && !!JSON.parse(k.row.import_json).repair_record);
  k = await cls(C.success(1), C.success(1));
  check('T32', 'an R2 copy that already holds the success: imported with the marker set, no repair_record', k.row.status_at === T && JSON.parse(k.row.import_json).repair_record === undefined);
  check('T23', 'row 5 with a pending copy beside it: succeeded', (await cls(C.success(1), C.pending(1))).cls === 'identity_succeeded');
  check('T23', "row 5, the record's rescued flag: outcome 'rescued'", (await cls(C.success(1, { rescued: true }), null)).row.outcome === 'rescued');
  // row 6
  await both("row 6, a plain unrefunded error without the consumer's key", C.plain, 'hold_unexplained');
  // row 7
  await both('row 7, refund due with no cost', (n) => C.due(n, { tokenCost: undefined }), 'hold_no_cost');
  await both('row 7, pending with no cost and no requestId (S0 adds both or neither)', (n) => C.pending(n, { tokenCost: undefined, requestId: undefined }), 'hold_no_cost');
  await both('row 7, pending with a cost but no requestId', (n) => C.pending(n, { requestId: undefined }), 'hold_no_cost');
  // row 8
  await both('row 8, owed', C.owed, 'owed_ambiguous');
  k = await cls(C.owed(1), null);
  check('T23', "row 8: payable, 'refund_credit_failed', the owed cost", k.row.state === 'claimed' && k.row.due_code === 'refund_credit_failed' && k.row.cost === 16 && k.row.hold === null);
  check('T23', 'row 8, a running copy beside an owed copy (after the barrier): owed', (await cls(C.running(1), C.owed(1))).cls === 'owed_ambiguous');
  // row 9
  await both('row 9, refund due with a cost', C.due, 'refund_due_ambiguous');
  k = await cls(C.due(1), null);
  check('T23', "row 9: the refundDue code and message, the record's cost", k.row.due_code === 'provider_error' && k.row.msg === 'rd 500' && k.row.cost === 10);
  check('T23', 'row 9, a running copy beside a refund-due copy: refund due', (await cls(C.running(1), C.due(1))).cls === 'refund_due_ambiguous');
  // row 10
  await both('row 10, pending with its cost and requestId', C.pending, 'pending');
  k = await cls(C.pending(1), null);
  check('T23', "row 10: 'enqueued', its cost, enqueued_at = enqueuedAt", k.row.state === 'enqueued' && k.row.cost === 5 && k.row.enqueued === C.pending(1).enqueuedAt);
  // common to every row
  k = await cls(C.owed(1), null);
  const ij = JSON.parse(k.row.import_json);
  check('T23', "every row: provenance 'kv' fields, created_at = enqueuedAt, import_json's class, copies, evidence and fingerprint",
    k.row.created === C.owed(1).enqueuedAt && ij.class === 'owed_ambiguous' && ij.copies.kv === 'error' && ij.copies.r2 === null && Array.isArray(ij.evidence) && /^[0-9a-f]{64}$/.test(ij.fingerprint));
  check('T23', 'the fingerprint changes with any input and not without one',
    (await cls(C.owed(1), null)).fingerprint === k.fingerprint && (await cls(C.owed(1, { failedAt: 1 }), null)).fingerprint !== k.fingerprint
    && (await cls(C.owed(1), null, { consumer: true, pages: {} })).fingerprint !== k.fingerprint);
  check('T23', 'no copy at all: nothing to import', (await cls(null, null)).cls === null);
  // the carried job (S2 028 amendment 3)
  const carry = (epoch, x = {}) => ({ pause_epoch_ms: epoch, user_id: U(1), mode: 'create', token_cost: 9, evidence_json: '{"fingerprint":"f"}', paid_evidence_json: null, decided_at_ms: 1, ...x });
  k = await cls(null, null, noKeys, { carried: [carry(1), carry(2, { token_cost: 11 })] });
  check('T23', "a carried job with no scan evidence: held 'carried', the latest carry's cost, every carry named", k.cls === 'hold_carried' && k.row.hold === 'carried' && k.row.cost === 11 && JSON.parse(k.row.import_json).carried.length === 2);
  k = await cls(C.refunded(1), null, noKeys, { carried: [carry(1)] });
  check('T23', 'a carried job whose scan shows payment: row 4', k.cls === 'refunded_by_evidence');
  k = await cls(C.success(1), null, noKeys, { carried: [carry(1)] });
  check('T23', 'a carried job whose scan shows delivery: row 5', k.cls === 'identity_succeeded');
  k = await cls(C.owed(1), null, noKeys, { carried: [carry(1)] });
  check('T23', "a carried job whose scan shows neither payment nor delivery (an owed copy): held 'carried', never paid as row 8", k.cls === 'hold_carried');
  k = await cls(C.owed(1, { neutralized: { epoch: 1 } }), null, noKeys, { carried: [carry(1)] });
  check('T23', 'a carried job whose KV copy carries the neutralized marker: read as absent, held carried', k.cls === 'hold_carried');
  k = await cls(C.refunded(1, { neutralized: { epoch: 1 } }), null, noKeys, { carried: [carry(1)] });
  check('T23', "a neutralized copy is never the scan's payment evidence", k.cls === 'hold_carried');
  k = await cls(null, null, noKeys, { carried: [carry(1, { paid_evidence_json: '{"found":"gen: index"}' })] });
  check('T23', "the payment evidence George recorded on a carry is in import_json", JSON.parse(k.row.import_json).carried[0].paid_evidence.found === 'gen: index');
  check('T23', 'kv_payable: an owed KV copy and a running copy with a cost; not a refunded or costless one',
    M.kvPayable(C.owed(1)) === 1 && M.kvPayable(C.running(1)) === 1 && M.kvPayable(C.due(1)) === 1 && M.kvPayable(C.refunded(1)) === 0
    && M.kvPayable(C.running(1, { tokenCost: undefined })) === 0 && M.kvPayable(null) === 0);
}

// Every class imports through the spec's own statement, then every imported canceller meets it.
{
  const db = fresh();
  setCtl(db, 'money_pause', '1'); setCtl(db, 'migration_open', 'snapshot');
  const cases = [
    ['hold_running', C.running(1), null, noKeys], ['hold_contradictory', C.success(2), C.owed(2), noKeys], ['refunded_by_evidence', C.refunded(3), null, noKeys],
    ['identity_succeeded', C.success(4), null, noKeys], ['hold_unexplained', C.plain(5), null, noKeys], ['hold_no_cost', C.due(6, { tokenCost: undefined }), null, noKeys],
    ['owed_ambiguous', C.owed(7), null, noKeys], ['refund_due_ambiguous', C.due(8), null, noKeys], ['pending', C.pending(9), null, noKeys],
  ];
  const ids = [];
  for (const [want, kv, r2, keys] of cases) {
    const id = `ledgertest_job_c${ids.length}`;
    const k = await M.classifyJob(id, { kv, r2, keys }, { now: T, barrierProven: true });
    let ok = k.cls === want;
    try { runStmt(db, S(1528, 0), k.row); } catch (e) { ok = false; console.log('   import', want, e.message); }
    const rb = runStmt(db, S(1528, 1), { jobs: JSON.stringify([id]) }).rows[0];
    check('T23', `${want}: imported by 4.17's statement as written (every CHECK holds), fingerprint read back`, ok && rb.fingerprint === k.fingerprint && rb.class === want);
    ids.push([id, want]);
  }
  const kc = await M.classifyJob('ledgertest_job_carry', { kv: null, r2: null, keys: noKeys }, { now: T, barrierProven: true, carried: [{ pause_epoch_ms: 1, user_id: U(1), mode: 'create', token_cost: 9, evidence_json: '{}', paid_evidence_json: null, decided_at_ms: 1 }] });
  runStmt(db, S(1528, 0), kc.row); ids.push(['ledgertest_job_carry', 'hold_carried']);
  check('T23', 'hold_carried: imported as written', one(db, "SELECT hold_reason FROM jobs WHERE job_id = 'ledgertest_job_carry'").hold_reason === 'carried');
  // the unpause, then the cancellers
  setCtl(db, 'migration_open', '0'); setCtl(db, 'money_pause', '0');
  db.prepare("INSERT INTO balances VALUES (?, 100, 1, 1, 'snapshot', NULL)").run(U(1));
  T += 2 * HOUR;
  const ctx = { db: { prepare(sql) { const st = { sql, values: [] }; st.bind = (...a) => { st.values = a; return st; }; return st; },
    async batch(stmts) { db.exec('BEGIN'); try { const r = stmts.map((s) => { const p = db.prepare(s.sql); const vals = s.values.map(toBind); if (isRead(s.sql)) return { results: p.all(...vals).map((x) => ({ ...x })), meta: { changes: 0 } }; const rr = p.run(...vals); return { results: [], meta: { changes: Number(rr.changes) } }; }); db.exec('COMMIT'); return r; } catch (e) { db.exec('ROLLBACK'); throw e; } } },
    now: () => T };
  const wantFence = {
    hold_running: 'held', hold_contradictory: 'held', hold_unexplained: 'held', hold_no_cost: 'held', hold_carried: 'held',
    refunded_by_evidence: 'already_refunded_legacy', identity_succeeded: 'already_finished',
    owed_ambiguous: 'refunded', refund_due_ambiguous: 'refunded', pending: 'refunded',
  };
  for (const [id, c] of ids) {
    const r = await L.refundAndFinish(ctx, { job: id, fence: 'canceller', code: 'imported_unfinished' });
    const r2 = await L.refundAndFinish(ctx, { job: id, fence: 'canceller', code: 'imported_unfinished' });
    check('T23', `the canceller over an imported ${c} row: ${wantFence[c]}, then never again`, r.outcome === wantFence[c]
      && (r.outcome !== 'refunded' || r2.outcome === 'already_finished'));
  }
  check('T23', 'no positive-evidence row was ever paid; each payable row paid once',
    one(db, "SELECT COUNT(*) AS n FROM ledger WHERE idem_key LIKE 'refund:%'").n === 3
    && !one(db, "SELECT 1 AS x FROM ledger WHERE idem_key IN ('refund:ledgertest_job_c2', 'refund:ledgertest_job_c3')"));
  T -= 2 * HOUR;
}

// ════════════════════════════════════════════════════════════════════════
// T22 and T31: the scans, the kept content, the barrier
// ════════════════════════════════════════════════════════════════════════
const EPOCH0 = T;
{
  // Step 1b (money open): the report, and no kept content.
  const w = world({ phase: 'scan', pause: '0' });
  w.kv.m.set('job:ledgertest_job_a', { value: JSON.stringify(C.running(1, { startedAt: T - 20 * MIN, taskId: undefined })) });
  w.kv.m.set('job:ledgertest_job_b', { value: JSON.stringify(C.owed(2, { failedAt: T - 30 * MIN })) });
  w.kv.m.set('job:ledgertest_job_c', { value: JSON.stringify(C.due(3, { refundDue: { errorCode: 'x', at: T - 25 * MIN } })) });
  w.kv.m.set('job:ledgertest_job_d', { value: JSON.stringify(C.running(4, { startedAt: T - 5 * MIN })) });
  w.r2.m.set('jobs/ledgertest_job_e.json', { body: JSON.stringify(C.running(5)) });
  const a = await drain(w, '/admin/ledger/scan');
  const chunks = a.map((x) => x.body.chunk);
  const list = (f) => chunks.flatMap((c) => c[f] ?? []).sort();
  check('T22', "step 1b: stale KV running records with no task are listed (the refund-due one too); a fresh one is not", list('running_no_task_over_15m').join() === 'ledgertest_job_a,ledgertest_job_c');
  check('T22', 'step 1b: KV-visible debts older than 20 minutes are listed (owed and refund due)', list('debts_over_20m').join() === 'ledgertest_job_b,ledgertest_job_c');
  check('T22', 'step 1b: an R2-only running copy is listed, not waited for', list('r2_only_running').join() === 'ledgertest_job_e');
  check('T22', 'step 1b (money open): no kept content written', ![...w.r2.m.keys()].some((k) => k.startsWith('migrator/')) && last(a).body.writing_kept_content === false);
  check('T22', 'step 1b: every job counted once across the KV and R2 pages', last(a).body.counts.jobs === 5);
}

/** A switch on synthetic data, through the migrator's own endpoints and the
 *  spec's hand-run statements. */
async function switchWorld() {
  T = EPOCH0;
  const w = world({ phase: '0', pause: '0' });
  // step 2: the pause and the cutoff, one literal (section 8, as written)
  for (const s of splitBlock(FIX.blocks.find((b) => b.line === 1912).text)) runStmt(w.db, s, { cutoff: T });
  const move = (from, to) => runStmt(w.db, S(1883, 0), { to, from, now: T, who: 'george' }).changes;
  return { w, move, epoch: T };
}
{
  const { w, move, epoch } = await switchWorld();
  check('T31', "step 2's two commands, as written: paused, the cutoff recorded", one(w.db, "SELECT value FROM control WHERE key = 'money_pause'").value === '1'
    && one(w.db, 'SELECT value_ms FROM switch_marks').value_ms === epoch);
  w.kv.m.set('job:ledgertest_job_k1', { value: JSON.stringify(C.owed(1)), exp: Math.floor((T + 10 * MIN) / 1000) });
  w.r2.m.set('jobs/ledgertest_job_r1.json', { body: JSON.stringify(C.running(2)) });
  w.r2.m.set('jobs/ledgertest_job_r2.json', { body: JSON.stringify(C.running(3, { tokenCost: undefined })), exp: T + 20 * MIN });
  w.r2.m.set('jobs/ledgertest_job_r3.json', { body: JSON.stringify(C.owed(4)) });
  move('0', 'scan');
  let a = await drain(w, '/admin/ledger/scan');
  check('T31', 'scan 1 (paused): kept content written under migrator/scan/{epoch}/', last(a).body.writing_kept_content === true && w.r2.m.has(`migrator/scan/${epoch}/jobs/ledgertest_job_k1.json`) && last(a).body.counts.new === 4);
  check('T31', 'after one scan the barrier is not proven', last(a).body.barrier_proven_after === false);
  T += 10 * MIN;
  a = await drain(w, '/admin/ledger/scan');
  check('T31', 'two scans less than 15 minutes apart never pass', last(a).body.barrier_proven_after === false && (last(a).body.counts.changed ?? 0) === 0);
  T += 16 * MIN; // the KV copy and one R2 copy have expired now
  a = await drain(w, '/admin/ledger/scan');
  check('T31', 'a KV record expiring between the scans is not a change: the scan keeps its last content', (last(a).body.counts.changed ?? 0) === 0 && last(a).body.counts.kept_only === 2);
  check('T31', 'the third scan, 16 minutes after the second, nothing changed or new: the barrier proven', last(a).body.barrier_proven_after === true);
  // step 4: the dry run
  w.kv.m.set('token_balance:ledgertest_1', { value: JSON.stringify({ balance: 40 }) });
  w.kv.m.set('token_balance:baduser_1', { value: JSON.stringify({ balance: 1 }) });
  a = await drain(w, '/admin/ledger/scan?classify=1');
  const holds = a.flatMap((x) => x.body.chunk.holds ?? []);
  check('T22', 'step 4: an R2-only running copy with its cost and one without are holds (7.2 row 1), with the cost when known',
    holds.find((h) => h.job_id === 'ledgertest_job_r1')?.cost === 12 && holds.some((h) => h.job_id === 'ledgertest_job_r2' && h.cost === null));
  check('T22', "step 4: the R2-only owed copy is payable (row 8), not a hold", !holds.some((h) => h.job_id === 'ledgertest_job_r3') && last(a).body.counts.class_owed_ambiguous === 2);
  check('T26', 'step 4: a non-conforming token_balance key is counted (the switch stops)', last(a).body.counts.nonconforming_balance_keys === 1);
  w.kv.m.delete('token_balance:baduser_1');
  // a new record after the final clean scan
  w.kv.m.set('job:ledgertest_job_late', { value: JSON.stringify(C.pending(9)) });
  a = await drain(w, '/admin/ledger/scan');
  check('T31', 'an arrival after the final clean scan: the next scan finds it new, and the barrier is no longer proven', last(a).body.counts.new === 1 && last(a).body.barrier_proven_after === false);
  T += 16 * MIN;
  a = await drain(w, '/admin/ledger/scan');
  check('T31', 'and a scan 16 minutes later that finds nothing new: proven again', last(a).body.barrier_proven_after === true);
  T += 16 * MIN;
  w.kv.m.set('job:ledgertest_job_late2', { value: JSON.stringify(C.pending(10)) });
  a = await drain(w, '/admin/ledger/scan');
  check('T31', 'a scan 16 minutes after the last that finds a new record: not proven', last(a).body.counts.new === 1 && last(a).body.barrier_proven_after === false);
  T += 16 * MIN;
  a = await drain(w, '/admin/ledger/scan');
  T += 16 * MIN;
  w.kv.m.set('job:ledgertest_job_late2', { value: JSON.stringify(C.pending(10, { tokenCost: 6 })) });
  a = await drain(w, '/admin/ledger/scan');
  check('T31', 'a scan 16 minutes after the last that finds a changed record: not proven', last(a).body.counts.changed === 1 && last(a).body.barrier_proven_after === false);
  w.kv.m.delete('job:ledgertest_job_late');
  w.kv.m.delete('job:ledgertest_job_late2');
  check('O14', 'a verify scan in phase scan writes no kept content', await (async () => {
    const before = [...w.r2.m.keys()].join();
    const r = await drain(w, '/admin/ledger/scan?verify=1');
    return last(r).status === 200 && [...w.r2.m.keys()].join() === before;
  })());
}

// ════════════════════════════════════════════════════════════════════════
// T26 and T6 (import): the snapshot
// ════════════════════════════════════════════════════════════════════════
async function readyForSnapshot({ users = 60, keys = 250 } = {}) {
  const { w, move, epoch } = await switchWorld();
  let sum = 0;
  for (let i = 1; i <= users; i++) { const b = i === 7 ? -10 : i * 3; sum += b; w.kv.m.set(`token_balance:${U(1000 + i)}`, { value: JSON.stringify({ balance: b, created_at: 'x', last_updated: `t${i}` }) }); }
  for (let i = 0; i < keys; i++) w.kv.m.set(`token_idempotency:gen:${U(1)}:${i}`, { value: '1', exp: Math.floor((T + 7 * DAY) / 1000) });
  w.kv.m.set('token_idempotency:evt_applied', { value: '1' });
  w.kv.m.set('webhook:stripe:evt_applied', { value: '1' });
  w.kv.m.set('job:ledgertest_job_s1', { value: JSON.stringify(C.success(1)) });
  w.r2.m.set('jobs/ledgertest_job_s2.json', { body: JSON.stringify(C.owed(2)) });
  w.kv.m.set('job:ledgertest_job_s3', { value: JSON.stringify(C.pending(3)) });
  // carried rows from an earlier abort: two events (one release 1 since applied) and a job
  for (const [kind, id, extra] of [['event', 'evt_applied', {}], ['event', 'evt_carried', {}], ['job', 'ledgertest_job_old', { user_id: U(1), mode: 'create', token_cost: 7 }]]) {
    w.db.prepare(`INSERT INTO switch_obligations (pause_epoch_ms, subject_kind, subject_id, user_id, mode, source, evidence_json, token_cost, kv_payable, handed_off_at_ms, disposition, decision_note, decided_at_ms)
      VALUES (1, ?, ?, ?, ?, ?, '{}', ?, 0, 1, 'carry', 'n', 1)`).run(kind, id, extra.user_id ?? null, extra.mode ?? null, kind === 'event' ? 'stripe_refusal' : 'hold', toBind(extra.token_cost ?? null));
  }
  move('0', 'scan');
  await drain(w, '/admin/ledger/scan'); T += 16 * MIN; await drain(w, '/admin/ledger/scan');
  move('scan', 'snapshot');
  return { w, move, epoch, sum, users, keys };
}
{
  const { w, sum, users, keys } = await readyForSnapshot();
  const a = await drain(w, '/admin/ledger/snapshot', 400);
  const fin = last(a);
  check('T26', 'a full snapshot on synthetic KV: every chunk imported, done', fin.status === 200 && fin.body.done === true && !a.some((x) => x.body?.stopped));
  check('T26', `balances in chunks of 25 (${users} users, ${Math.ceil(users / 25)} chunks): the KV count and sum equal D1's`,
    one(w.db, 'SELECT COUNT(*) AS n, SUM(balance) AS s FROM balances').n === users && one(w.db, 'SELECT SUM(balance) AS s FROM balances').s === sum
    && fin.body.counts.balances_imported === users && fin.body.counts.balances_sum === sum
    && a.filter((x) => x.body.chunk?.kv_count !== undefined).length === Math.ceil(users / 25));
  check('T6', 'a -10 KV balance imported as -10', one(w.db, "SELECT balance FROM balances WHERE user_id = 'ledgertest_1007'").balance === -10);
  check('T26', `legacy evidence: every token_idempotency: and webhook:stripe: key copied with its kind and expiry (${keys + 2})`,
    one(w.db, 'SELECT COUNT(*) AS n FROM legacy_idem').n === keys + 2 && one(w.db, "SELECT kind FROM legacy_idem WHERE key = 'webhook:stripe:evt_applied'").kind === 'stripe_event'
    && one(w.db, "SELECT kind, kv_expires_ms FROM legacy_idem WHERE key LIKE 'token_idempotency:gen:%' LIMIT 1").kv_expires_ms !== null);
  check('T26', 'the carried events after the legacy chunks: a pending row each, the one release 1 applied not',
    !!one(w.db, "SELECT 1 AS x FROM stripe_pending WHERE event_id = 'evt_carried'") && !one(w.db, "SELECT 1 AS x FROM stripe_pending WHERE event_id = 'evt_applied'"));
  check('T26', "the jobs: each scanned job imported with its class, the carried job held 'carried'",
    one(w.db, "SELECT json_extract(import_json, '$.class') AS c FROM jobs WHERE job_id = 'ledgertest_job_s1'").c === 'identity_succeeded'
    && one(w.db, "SELECT json_extract(import_json, '$.class') AS c FROM jobs WHERE job_id = 'ledgertest_job_s2'").c === 'owed_ambiguous'
    && one(w.db, "SELECT state FROM jobs WHERE job_id = 'ledgertest_job_s3'").state === 'enqueued'
    && one(w.db, "SELECT hold_reason FROM jobs WHERE job_id = 'ledgertest_job_old'").hold_reason === 'carried');
  // a retried chunk of each kind
  const before = dump(w.db);
  const rerun = await drain(w, '/admin/ledger/snapshot', 400);
  check('T26', 'the whole snapshot run again (a retry of every chunk of every kind): imported again, no new rows', last(rerun).body.done === true && dump(w.db) === before);
  // a job copy changed between attempts: the stored fingerprint differs, error, stop
  w.r2.m.set('jobs/ledgertest_job_s2.json', { body: JSON.stringify(C.owed(2, { failedAt: 5 })) });
  const changed = await drain(w, '/admin/ledger/snapshot', 400);
  check('T26', 'a job record changed between attempts: its stored fingerprint differs, error, stop', /fingerprint differs/.test(last(changed).body.stopped ?? ''));
  w.r2.m.set('jobs/ledgertest_job_s2.json', { body: JSON.stringify(C.owed(2)) });
  // step 6 reconciliation reads, as written
  check('T26', "step 6's counts, as written: balances and their sum", runStmt(w.db, FIX.inline.find((x) => x.text === 'SELECT COUNT(*) FROM balances').text).rows[0]['COUNT(*)'] === users
    && runStmt(w.db, FIX.inline.find((x) => x.text === 'SELECT SUM(balance) FROM balances').text).rows[0]['SUM(balance)'] === sum);
  // R4-17: outside 'snapshot' a chunk is refused by the admission, nothing written
  runStmt(w.db, S(1883, 0), { to: 'verify', from: 'snapshot', now: T, who: 'george' });
  const b2 = dump(w.db);
  check('T26', "a chunk in 'verify' (an already imported chunk read outside 'snapshot'): 404, never taken as imported, nothing written", (await call(w, '/admin/ledger/snapshot')).status === 404 && dump(w.db) === b2);
}
{
  // the cutoff check at step 5
  const { w } = await readyForSnapshot({ users: 3, keys: 3 });
  w.db.prepare("UPDATE switch_marks SET value_ms = value_ms + 1").run();
  const b = dump(w.db);
  const r = await call(w, '/admin/ledger/snapshot');
  check('T26', "step 5's cutoff check: a mismatch answers the abort and writes nothing", r.status === 200 && r.body.abort === true && dump(w.db) === b);
  w.db.prepare("UPDATE switch_marks SET value_ms = value_ms - 1").run();
  check('T26', 'equal: the snapshot goes on', (await call(w, '/admin/ledger/snapshot')).body.abort === undefined);
}
{
  // a KV value changed between attempts: error, stop
  const { w } = await readyForSnapshot({ users: 3, keys: 3 });
  const first = await call(w, '/admin/ledger/snapshot');
  w.kv.m.set(`token_balance:${U(1001)}`, { value: JSON.stringify({ balance: 999 }) });
  const again = await call(w, '/admin/ledger/snapshot');
  check('T26', 'a KV value changed between attempts of one chunk: error, stop', first.body.chunk.kv_count === 3 && /error/.test(again.body.stopped ?? '') && again.body.cursor === null);
  // a non-conforming balance key: the snapshot stops too
  const n = await readyForSnapshot({ users: 2, keys: 1 });
  n.w.kv.m.set('token_balance:Another_1', { value: JSON.stringify({ balance: 1 }) });
  const s = await call(n.w, '/admin/ledger/snapshot');
  check('T26', 'a non-conforming balance key at the snapshot: stopped, nothing of it written', /non-conforming/.test(s.body.stopped ?? '') && one(n.w.db, 'SELECT COUNT(*) AS n FROM balances').n === 0);
  // a running copy whose barrier was never proven stops the snapshot
  const { w: w2, move } = await switchWorld();
  w2.kv.m.set('job:ledgertest_job_live', { value: JSON.stringify(C.running(1)) });
  move('0', 'scan'); await drain(w2, '/admin/ledger/scan'); move('scan', 'snapshot');
  const a = await drain(w2, '/admin/ledger/snapshot', 50);
  check('T23', 'before the barrier (one scan only), a running copy stops the snapshot at the jobs stage', /barrier/.test(last(a).body.stopped ?? '') && !one(w2.db, "SELECT 1 AS x FROM jobs WHERE job_id = 'ledgertest_job_live'"));
}
{
  // T22: a copy that expires between step 3 and step 5 still imports from the kept content
  const { w, move, epoch } = await switchWorld();
  w.r2.m.set('jobs/ledgertest_job_x1.json', { body: JSON.stringify(C.running(1)), exp: T + 20 * MIN });
  w.r2.m.set('jobs/ledgertest_job_x2.json', { body: JSON.stringify(C.owed(2)), exp: T + 20 * MIN });
  move('0', 'scan'); await drain(w, '/admin/ledger/scan'); T += 16 * MIN; await drain(w, '/admin/ledger/scan');
  T += 10 * MIN; // both objects expire by their lifecycle before step 5
  move('scan', 'snapshot');
  const a = await drain(w, '/admin/ledger/snapshot', 50);
  const x1 = one(w.db, "SELECT hold_reason, token_cost FROM jobs WHERE job_id = 'ledgertest_job_x1'");
  const x2 = one(w.db, "SELECT refund_due_code, token_cost FROM jobs WHERE job_id = 'ledgertest_job_x2'");
  check('T22', 'an R2-only running copy that expired before the snapshot: a held row with its cost, from the kept content', last(a).body.done && x1?.hold_reason === 'unexplained' && x1.token_cost === 12);
  check('T22', 'an R2-only owed copy that expired: payable as row 8 from the kept content', x2?.refund_due_code === 'refund_credit_failed' && x2.token_cost === 16);
  check('T31', 'its kept content stays under the pause epoch', w.r2.m.has(`migrator/scan/${epoch}/jobs/ledgertest_job_x1.json`));
  // T22's barrier through the snapshot, and the held row survives the object's expiry
  T += DAY;
  check('T22', "the held row survives the object's expiry until George settles it", one(w.db, "SELECT finished_at_ms FROM jobs WHERE job_id = 'ledgertest_job_x1'").finished_at_ms === null);
}

// The second fence: the spec's guards hold when the control rows change
// between the migrator's read and its batch.
for (const [stage, match, flip] of [
  ['balances', /INSERT INTO ledger/, 'pause'], ['balances', /INSERT INTO ledger/, 'phase'],
  ['legacy', /INSERT INTO legacy_idem/, 'pause'], ['legacy', /INSERT INTO legacy_idem/, 'phase'],
  ['carried events', /INSERT INTO stripe_pending/, 'pause'], ['carried events', /INSERT INTO stripe_pending/, 'phase'],
  ['jobs', /INSERT INTO jobs/, 'pause'], ['jobs', /INSERT INTO jobs/, 'phase'],
]) {
  const { w } = await readyForSnapshot({ users: 2, keys: 2 });
  const before = { ledger: one(w.db, 'SELECT COUNT(*) AS n FROM ledger').n };
  d1.race = { match, run: (db) => flip === 'pause'
    ? db.prepare("UPDATE control SET value = '0' WHERE key = 'money_pause'").run()
    : db.prepare("UPDATE control SET value = 'verify' WHERE key = 'migration_open'").run() };
  const a = await drain(w, '/admin/ledger/snapshot', 100);
  d1.race = null;
  const table = { balances: 'balances', legacy: 'legacy_idem', 'carried events': 'stripe_pending', jobs: 'jobs' }[stage];
  check('T26', `a ${flip === 'pause' ? 'reopen' : 'phase move'} between the gate and the ${stage} batch: the spec's guard refuses, nothing written, the snapshot stops 'refused'`,
    /refused/.test(last(a).body?.stopped ?? '') && one(w.db, `SELECT COUNT(*) AS n FROM ${table}`).n === 0
    && (stage !== 'balances' || one(w.db, 'SELECT COUNT(*) AS n FROM ledger').n === before.ledger));
}
{
  // the cutoff check reads the epoch only while paused
  const { w } = await readyForSnapshot({ users: 1, keys: 1 });
  d1.race = { match: /switch_marks/, run: (db) => db.prepare("UPDATE control SET value = '0' WHERE key = 'money_pause'").run() };
  w.env.LEDGER_DB.prepare = ((orig) => function (sql) { if (d1.race && d1.race.match.test(sql)) { const r = d1.race; d1.race = null; r.run(w.db); } return orig.call(this, sql); })(w.env.LEDGER_DB.prepare);
  const r = await call(w, '/admin/ledger/snapshot');
  d1.race = null;
  check('T26', "money reopened between the gate and the cutoff check: no pause epoch, the abort (never an import)", r.body?.abort === true && one(w.db, 'SELECT COUNT(*) AS n FROM balances').n === 0);
}
for (const flip of ['pause', 'phase']) {
  const { w, move } = await switchWorld();
  w.r2.m.set('jobs/ledgertest_job_hr.json', { body: JSON.stringify(C.owed(1)) });
  move('0', 'scan');
  d1.race = { match: /INSERT INTO switch_obligations/, run: (db) => flip === 'pause'
    ? db.prepare("UPDATE control SET value = '0' WHERE key = 'money_pause'").run()
    : db.prepare("UPDATE control SET value = '0' WHERE key = 'migration_open'").run() };
  const a = await drain(w, '/admin/ledger/scan?handoff=1');
  d1.race = null;
  check('T33', `a ${flip === 'pause' ? 'reopen' : 'phase move'} between the gate and the handoff batch: nothing written, the read-back not equal`,
    one(w.db, 'SELECT COUNT(*) AS n FROM switch_obligations').n === 0 && a.some((x) => x.body?.all_equal_so_far === false));
}

// ════════════════════════════════════════════════════════════════════════
// T33 (part): the abort's handoff, read back by content
// ════════════════════════════════════════════════════════════════════════
{
  const { w, move, epoch } = await switchWorld();
  w.kv.m.set('job:ledgertest_job_h1', { value: JSON.stringify(C.success(1)) });
  w.r2.m.set('jobs/ledgertest_job_h1.json', { body: JSON.stringify(C.owed(1)) });
  w.r2.m.set('jobs/ledgertest_job_h2.json', { body: JSON.stringify(C.owed(2)) });
  w.kv.m.set('job:ledgertest_job_h3', { value: JSON.stringify(C.owed(3)) });
  w.kv.m.set('job:ledgertest_job_h4', { value: JSON.stringify(C.running(4)) });
  w.kv.m.set('job:ledgertest_job_h5', { value: JSON.stringify(C.success(5)) });
  move('0', 'scan'); await drain(w, '/admin/ledger/scan');
  const a = await drain(w, '/admin/ledger/scan?handoff=1');
  const obs = a.flatMap((x) => x.body.chunk.obligations);
  const by = Object.fromEntries(obs.map((o) => [o.subject, o]));
  check('T33', "the handoff: the contradiction 'hold', the R2-only owed copy 'kept_copy'; a KV-payable owed copy and a success are release 1's, not listed",
    by.ledgertest_job_h1?.source === 'hold' && by.ledgertest_job_h2?.source === 'kept_copy' && !by.ledgertest_job_h3 && !by.ledgertest_job_h5);
  check('T33', 'kv_payable: 1 for a running KV copy with a cost (held, release 1 would pay it), 0 for an R2-only copy',
    by.ledgertest_job_h4?.kv_payable === 1 && by.ledgertest_job_h2?.kv_payable === 0);
  check('T33', 'every listed subject read back equal (fingerprint and kv_payable), so the DELETE may run', last(a).body.all_equal_so_far === true && obs.every((o) => o.read_back_equal));
  check('T33', "the rows: this pause's epoch, the spec's INSERT, no disposition yet", one(w.db, 'SELECT COUNT(*) AS n FROM switch_obligations WHERE pause_epoch_ms = ? AND disposition IS NULL', epoch).n === obs.length);
  // a KV copy that changes after the handoff: the re-handoff replaces the row and clears its decision
  const fBefore = one(w.db, "SELECT json_extract(evidence_json, '$.fingerprint') AS f FROM switch_obligations WHERE subject_id = 'ledgertest_job_h2'").f;
  runStmt(w.db, S(2163, 2), { disposition: 'nothing_owed', note: 'checked', who: 'george', now: T, cost: null, neutralized_ms: null, epoch, kind: 'job', subject: 'ledgertest_job_h2' });
  w.r2.m.set('jobs/ledgertest_job_h2.json', { body: JSON.stringify(C.owed(2, { failedAt: 1 })) });
  const b = await drain(w, '/admin/ledger/scan?handoff=1');
  const h2 = one(w.db, "SELECT disposition, json_extract(evidence_json, '$.fingerprint') AS f FROM switch_obligations WHERE subject_id = 'ledgertest_job_h2'");
  check('T33', 'a changed copy: the re-handoff replaces the row, its new fingerprint read back, its decision cleared',
    h2.disposition === null && h2.f !== fBefore && b.flatMap((x) => x.body.chunk.obligations).find((o) => o.subject === 'ledgertest_job_h2')?.read_back_equal === true);
  // a read-back that differs from what the handoff read: never all equal
  w.db.exec("CREATE TRIGGER t_alter AFTER INSERT ON switch_obligations BEGIN UPDATE switch_obligations SET evidence_json = json_set(evidence_json, '$.fingerprint', 'tampered') WHERE rowid = NEW.rowid; END");
  w.db.exec("CREATE TRIGGER t_alter2 AFTER UPDATE OF kv_payable ON switch_obligations BEGIN UPDATE switch_obligations SET evidence_json = json_set(evidence_json, '$.fingerprint', 'tampered') WHERE rowid = NEW.rowid; END");
  w.kv.m.set('job:ledgertest_job_h6', { value: JSON.stringify(C.plain(6)) });
  const c = await drain(w, '/admin/ledger/scan?handoff=1');
  check('T33', 'a handoff read-back that differs from what the handoff read: reported, not all equal (no DELETE)', last(c).body.all_equal_so_far === false
    && c.flatMap((x) => x.body.chunk.obligations).some((o) => o.subject === 'ledgertest_job_h6' && o.read_back_equal === false));
}

// ════════════════════════════════════════════════════════════════════════
// T33 (part) and step 8: the verify scan and the backstop run
// ════════════════════════════════════════════════════════════════════════
async function switched() {
  const r = await readyForSnapshot({ users: 4, keys: 3 });
  await drain(r.w, '/admin/ledger/snapshot', 100);
  return r;
}
const runRecord = (db, run, purpose) => runStmt(db, S(2061, 0), { run, purpose, now: T, who: 'george' }).changes;
const runDone = (db, run, t) => runStmt(db, S(2061, 3), {
  run, scan_started: t.started_at_ms, scan_finished: t.completed_at_ms, bl: t.balances_listed, br: t.balances_read, kl: t.keys_listed, kr: t.keys_read,
  jl: t.jobs_listed, jr: t.jobs_read, drift: t.drift_found, bal_done: t.balances_exhausted, idem_done: t.idem_exhausted, marks_done: t.marks_exhausted,
  jobs_done: t.jobs_exhausted, snap_read: t.snapshot_read, snap_missing: t.snapshot_missing,
});
const driftRows = (db, epoch, run, list) => list.forEach((d) => runStmt(db, S(2061, 5), { epoch, kind: d.kind, subject: d.subject, uid: d.user_id, snapshot: d.snapshot_json, found: d.found_json, now: T, run }));
{
  const { w, move, epoch } = await switched();
  move('snapshot', 'verify'); move('verify', '0');
  // step 8: the verify scan, in 'scan', with no difference
  move('0', 'scan');
  let a = await drain(w, '/admin/ledger/scan?verify=1');
  let drift = a.flatMap((x) => x.body.chunk.drift);
  check('T33', 'step 8: the verify scan with no difference: no drift, every list exhausted, every snapshot customer read', last(a).body.done && drift.length === 0
    && last(a).body.totals.snapshot_missing === 0 && last(a).body.totals.snapshot_read === 4 && last(a).body.totals.balances_exhausted === 1 && last(a).body.totals.jobs_exhausted === 1);
  w.db.prepare("INSERT INTO money_admissions (admission_id, route, subject_kind, subject_id, user_id, meta_json, admitted_at_ms, completed_at_ms) VALUES ('ad1', 'daily_reward', 'user', ?, ?, NULL, ?, ?)").run(U(1002), U(1002), toBind(epoch - HOUR), toBind(epoch - HOUR + 5));
  w.kv.m.set(`token_balance:${U(1002)}`, { value: JSON.stringify({ balance: 9999 }) });
  a = await drain(w, '/admin/ledger/scan?verify=1');
  drift = a.flatMap((x) => x.body.chunk.drift);
  check('T33', 'step 8: a balance changed in KV after the snapshot is a difference (the rollback abort)', drift.length === 1 && drift[0].kind === 'balance' && drift[0].subject === U(1002));
  check('T33', 'step 8: the customer with an admission record in the 24 hours before the pause is named, with the difference', last(a).body.named_customers?.some((n) => n.user_id === U(1002) && n.equal === false));
  w.kv.m.set(`token_balance:${U(1002)}`, { value: JSON.stringify({ balance: 6 }) });
  move('scan', '0');
  // 4.15, then step 9b's backstop run recorded from the scan's answers
  runStmt(w.db, S(1413, 0), { now: T }); // the unpause, as written
  check('T33', 'the unpause applied (phase 0, both rows)', one(w.db, "SELECT value FROM control WHERE key = 'money_pause'").value === '0');
  T += MIN;
  check('T33', "step 9b: the run recorded before its scan", runRecord(w.db, 'run_9b', 'step_9b') === 1);
  a = await drain(w, '/admin/ledger/scan?verify=1');
  let t = last(a).body.totals;
  runDone(w.db, 'run_9b', t);
  check('T33', 'a backstop run with no difference, its answers transcribed into the spec statement: complete', one(w.db, "SELECT complete FROM backstop_runs WHERE run_id = 'run_9b'").complete === 1);
  // a snapshot customer whose balance key is gone from KV: not read, so the run is incomplete
  {
    const saved = w.kv.m.get(`token_balance:${U(1003)}`);
    w.kv.m.delete(`token_balance:${U(1003)}`);
    T += MIN;
    runRecord(w.db, 'run_missing', 'reopen');
    a = await drain(w, '/admin/ledger/scan?verify=1');
    t = last(a).body.totals;
    driftRows(w.db, epoch, 'run_missing', a.flatMap((x) => x.body.chunk.drift));
    runDone(w.db, 'run_missing', t);
    check('T33', 'a snapshot customer not read (its key gone): snapshot_missing 1, the run incomplete', t.snapshot_missing === 1 && t.snapshot_read === 3
      && one(w.db, "SELECT complete FROM backstop_runs WHERE run_id = 'run_missing'").complete === 0);
    w.kv.m.set(`token_balance:${U(1003)}`, saved);
  }
  // S2 034's fixture: a new customer's key (drift) and a snapshot customer left behind a cursor
  T += MIN;
  runRecord(w.db, 'run_034', 'reopen');
  w.kv.m.set(`token_balance:${U(5000)}`, { value: JSON.stringify({ balance: 3 }) });
  const p1 = await call(w, '/admin/ledger/scan?verify=1');
  // the run stops after its first page (an interruption after every key on that page was read)
  const n = p1.body.counts;
  const partial = { started_at_ms: T, completed_at_ms: T, balances_listed: n.balances_listed, balances_read: n.balances_read, keys_listed: 0, keys_read: 0, jobs_listed: 0, jobs_read: 0,
    drift_found: n.drift_found ?? 0, balances_exhausted: 0, idem_exhausted: 0, marks_exhausted: 0, jobs_exhausted: 0, snapshot_read: n.snapshot_read ?? 0, snapshot_missing: 4 - (n.snapshot_read ?? 0) };
  driftRows(w.db, epoch, 'run_034', p1.body.chunk.drift);
  runDone(w.db, 'run_034', partial);
  check('T33', "S2 034's fixture: equal listed and read counts, the new customer's key a drift row, a snapshot customer left behind the cursor: incomplete",
    p1.body.chunk.drift.some((d) => d.subject === U(5000)) && n.balances_listed === n.balances_read && one(w.db, "SELECT complete FROM backstop_runs WHERE run_id = 'run_034'").complete === 0);
  // a failed key listing that returns zero counts: incomplete
  T += MIN;
  runRecord(w.db, 'run_fail', 'reopen');
  w.kv.failList = 'token_idempotency:';
  a = await drain(w, '/admin/ledger/scan?verify=1');
  w.kv.failList = null;
  t = last(a).body.totals;
  driftRows(w.db, epoch, 'run_fail', a.flatMap((x) => x.body.chunk.drift));
  runDone(w.db, 'run_fail', t);
  check('T33', 'a failed key listing (zero keys read from it): its prefix not exhausted, the run incomplete', t.idem_exhausted === 0 && one(w.db, "SELECT complete FROM backstop_runs WHERE run_id = 'run_fail'").complete === 0);
  // a complete run that reports drift, its rows written, and a planted key and job record
  T += MIN;
  runRecord(w.db, 'run_drift', 'reopen');
  w.kv.m.set('token_idempotency:planted', { value: '1' });
  w.kv.m.set('webhook:stripe:evt_nonmoney', { value: '1' });
  w.kv.m.set('job:ledgertest_job_s3', { value: JSON.stringify(C.running(3)) }); // a changed job record
  w.kv.m.set('job:ledgertest_job_expired_gone', { value: JSON.stringify(C.success(9)), exp: Math.floor((T - 1) / 1000) }); // gone by its TTL: not drift
  a = await drain(w, '/admin/ledger/scan?verify=1');
  t = last(a).body.totals;
  drift = a.flatMap((x) => x.body.chunk.drift);
  driftRows(w.db, epoch, 'run_drift', drift);
  runDone(w.db, 'run_drift', t);
  const kinds = drift.map((d) => `${d.kind}:${d.subject}`).sort();
  check('T33', 'the backstop finds a planted token_idempotency: key, a non-money webhook:stripe: mark and a changed job record; an expired record is none',
    kinds.includes('key:token_idempotency:planted') && kinds.includes('key:webhook:stripe:evt_nonmoney') && kinds.includes('job_record:ledgertest_job_s3') && !kinds.some((k) => k.includes('expired_gone')));
  check('T33', "release 2's own mark for an event it moved is none", (() => {
    w.db.prepare("INSERT INTO ledger (id, user_id, type, amount, reason, source, balance_after, idem_key, created_at_ms) VALUES ('m1', ?, 'credit', 1, 'x', 'x', 1, 'stripe:evt_moved', 1)").run(U(1001));
    return true;
  })());
  w.kv.m.set('webhook:stripe:evt_moved', { value: '1' });
  a = await drain(w, '/admin/ledger/scan?verify=1');
  check('T33', "release 2's own mark (a stripe: row exists) is not drift", !a.flatMap((x) => x.body.chunk.drift).some((d) => d.subject === 'webhook:stripe:evt_moved'));
  check('T33', 'a complete run whose drift rows name it: complete, and the drift waits for George', one(w.db, "SELECT complete FROM backstop_runs WHERE run_id = 'run_drift'").complete === 1
    && one(w.db, 'SELECT COUNT(*) AS n FROM switch_drift WHERE disposition IS NULL').n === drift.length);
  {
    const dbBefore = dump(w.db), r2Before = [...w.r2.m.keys()].join(), kvBefore = w.kv.puts, r2Puts = w.r2.puts;
    const v = await drain(w, '/admin/ledger/scan?verify=1');
    check('O14', "after the unpause, phase '0': a full scan?verify=1 run writes nothing (every table, every R2 key, no KV or R2 put)",
      last(v).body.done && dump(w.db) === dbBefore && [...w.r2.m.keys()].join() === r2Before && w.kv.puts === kvBefore && w.r2.puts === r2Puts);
  }
}

// ════════════════════════════════════════════════════════════════════════
// T27 (S2's offline part): dev after S2, every path 404 in phase '0' but verify
// ════════════════════════════════════════════════════════════════════════
{
  const w = world({ phase: '0', pause: '0' });
  const ctl = q(w.db, 'SELECT key, value FROM control ORDER BY key').map((r) => `${r.key}=${r.value}`).join();
  check('T27', "after 0003, dev's control rows: money_pause '0', the phase '0', the sentinel", ctl === 'migration_open=0,money_pause=0,switch_at_ms=99999999999999');
  const codes = await Promise.all(['/admin/ledger/scan', '/admin/ledger/scan?classify=1', '/admin/ledger/scan?handoff=1', '/admin/ledger/snapshot'].map(async (pq) => (await call(w, pq)).status));
  check('T27', "S2 deployed in phase '0': the migrator answers 404 on every path but scan?verify=1", codes.every((c) => c === 404) && (await call(w, '/admin/ledger/scan?verify=1')).status === 200);
  const src = readFileSync(path.join(ROOT, 'src/migrator.ts'), 'utf8');
  check('T27', 'the migrator binds no queue and sends nothing', !/\.send\(|sendBatch|RD_QUEUE|Queue\b/.test(src));
}

Date.now = realNow;
const order = ['spec', 'O14', 'T6', 'T22', 'T23', 'T26', 'T27', 'T31', 'T32', 'T33'];
let total = 0;
for (const t of order) {
  const c = counts.get(t) ?? { pass: 0, fail: 0 };
  total += c.pass + c.fail;
  console.log(`[ledger-s2-test] ${t}: ${c.pass}/${c.pass + c.fail}`);
}
console.log(`[ledger-s2-test] ${failed === 0 ? 'PASS' : 'FAIL'}: ${total - failed}/${total}`);
process.exit(failed === 0 ? 0 : 1);
