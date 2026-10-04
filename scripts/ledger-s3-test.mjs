// scripts/ledger-s3-test.mjs
//
// S3's offline harness: the consumer on D1 (n1-release-2-spec.md revision 9:
// section 11's S3 row; 5.2 to 5.4, 4.8 to 4.12; 10.1's T8, T8b, T11, T11c,
// T14, T15, T17 to T21, T24, T25, T28, T29, T32, T34, T35). Run from the repo
// root: `node scripts/ledger-s3-test.mjs`.
//
// src/index.ts is bundled with esbuild into dist/.ledger-s3-test (gitignored)
// and driven through its `queue` and `scheduled` handlers. LEDGER_DB is
// node:sqlite with migrations-ledger 0001 to 0003; EVENTS_DB is node:sqlite
// with migrations/0001 and 0002; KV and R2 are in-memory stubs with metadata,
// expiry and failure hooks; `fetch` is a Retro Diffusion stub (create, async
// submit, poll, status). Every sleep advances the test clock, so poll budgets,
// leases and floors behave as on the platform. Jobs are debited the way Pages
// debits them, through S1's library (4.2, 4.3, 4.4).
//
// It also carries the paused-answer rows of release 1's money-pause-test
// (retired with its code in S3), ported to release 2 as T11.
//
// LEDGER_MUTATION='{"file":"settle.ts","from":"...","to":"..."}' mutates one
// src file at bundle time (exit 1 caught, 0 survived, 3 not found once).

import { build } from 'esbuild';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

process.removeAllListeners('warning');
const { DatabaseSync } = await import('node:sqlite');
const ROOT = process.cwd();
const OUT = path.join(ROOT, 'dist', '.ledger-s3-test');
const MUT = process.env.LEDGER_MUTATION ? JSON.parse(process.env.LEDGER_MUTATION) : null;

const mutationPlugin = {
  name: 'mutation',
  setup(b) {
    if (!MUT) return;
    const re = new RegExp(`src[\\\\/]${MUT.file.replace('.', '\\.')}$`);
    b.onLoad({ filter: re }, (args) => {
      const src = readFileSync(args.path, 'utf8');
      const n = src.split(MUT.from).length - 1;
      if (n !== 1) { console.log(`[ledger-s3-test] mutation target found ${n} times`); process.exit(3); }
      return { contents: src.replace(MUT.from, MUT.to), loader: 'ts' };
    });
  },
};
await build({
  entryPoints: { index: path.join(ROOT, 'src/index.ts'), ledger: path.join(ROOT, 'src/ledger.ts') },
  bundle: true, platform: 'neutral', format: 'esm', outdir: OUT, logLevel: 'error',
  outExtension: { '.js': '.mjs' }, plugins: [mutationPlugin],
});
const v = `?v=${Date.now()}`;
const worker = (await import(pathToFileURL(path.join(OUT, 'index.mjs')).href + v)).default;
const L = await import(pathToFileURL(path.join(OUT, 'ledger.mjs')).href + v);

const counts = new Map();
let failed = 0;
function check(tag, name, ok) {
  const c = counts.get(tag) ?? { pass: 0, fail: 0 };
  if (ok) c.pass++; else { c.fail++; failed++; console.log(`[ledger-s3-test] FAIL ${tag}: ${name}`); }
  counts.set(tag, c);
}

// ── Clock and logs: every sleep advances the clock ──

let T = 1_800_000_000_000;
const MIN = 60_000, HOUR = 3_600_000, DAY = 86_400_000;
const realNow = Date.now;
Date.now = () => T;
const realSetTimeout = globalThis.setTimeout;
globalThis.setTimeout = (fn, ms = 0, ...a) => { T += Math.max(0, Number(ms) || 0); return realSetTimeout(() => fn(...a), 0); };
const logs = [];
for (const lvl of ['log', 'info', 'warn', 'error']) console[lvl] = ((orig) => (...a) => {
  const line = a.map(String).join(' ');
  if (line.startsWith('[ledger-s3-test]')) orig(...a); else logs.push(line);
})(console[lvl]);

// ── SQLite as D1 ──

const toBind = (x) => (typeof x === 'number' && Number.isInteger(x) ? BigInt(x) : typeof x === 'boolean' ? (x ? 1n : 0n) : x ?? null);
const isRead = (sql) => /^\s*(?:--[^\n]*\n\s*)*(SELECT|WITH)\b/i.test(sql);
function sqlite(files) {
  const db = new DatabaseSync(':memory:');
  for (const f of files) db.exec(readFileSync(path.join(ROOT, f), 'utf8'));
  return db;
}
function d1(db, hooks = {}) {
  const exec = (s) => {
    const p = db.prepare(s.sql);
    const vals = s.values.map(toBind);
    if (isRead(s.sql)) return { results: p.all(...vals).map((r) => ({ ...r })), meta: { changes: 0 }, success: true };
    const r = p.run(...vals);
    return { results: [], meta: { changes: Number(r.changes) }, success: true };
  };
  const api = {
    prepare(sql) {
      const st = { sql, values: [] };
      st.bind = (...a) => { st.values = a; return st; };
      st.all = async () => { hooks.before?.([sql]); return exec(st); };
      st.first = async (col) => { hooks.before?.([sql]); const r = exec(st).results[0] ?? null; return col && r ? r[col] : r; };
      st.run = async () => { hooks.before?.([sql]); return exec(st); };
      return st;
    },
    async batch(stmts) {
      hooks.before?.(stmts.map((s) => s.sql));
      db.exec('BEGIN');
      let r;
      try { r = stmts.map(exec); db.exec('COMMIT'); } catch (e) { db.exec('ROLLBACK'); throw e; }
      hooks.after?.(stmts.map((s) => s.sql));
      return r;
    },
  };
  return api;
}
const q = (db, sql, ...a) => db.prepare(sql).all(...a.map(toBind)).map((r) => ({ ...r }));
const one = (db, sql, ...a) => q(db, sql, ...a)[0];

// ── KV and R2 ──

function kvStub() {
  const m = new Map();
  const live = (k) => { const e = m.get(k); return e && (e.exp === undefined || e.exp * 1000 > T) ? e : null; };
  const kv = {
    m, failGet: null, failList: null, failPut: null, puts: [],
    async get(k) { if (kv.failGet?.(k)) throw new Error('KV get failed'); return live(k)?.value ?? null; },
    async put(k, value, opts = {}) {
      if (kv.failPut?.(k)) throw new Error('KV put failed');
      kv.puts.push(k);
      m.set(k, { value: String(value), metadata: opts.metadata, exp: opts.expiration ?? (opts.expirationTtl ? Math.floor(T / 1000) + opts.expirationTtl : undefined) });
    },
    async delete(k) { m.delete(k); },
    async list({ prefix = '', cursor, limit = 1000 } = {}) {
      if (kv.failList?.(prefix)) throw new Error('KV list failed');
      const names = [...m.keys()].filter((k) => k.startsWith(prefix) && live(k)).sort();
      const s = cursor ? Number(cursor) : 0;
      const page = names.slice(s, s + limit);
      const done = s + limit >= names.length;
      return { keys: page.map((name) => ({ name, metadata: m.get(name).metadata })), list_complete: done, ...(done ? {} : { cursor: String(s + limit) }) };
    },
  };
  return kv;
}
function r2Stub() {
  const m = new Map();
  const r2 = {
    m, failPut: null, failHead: null, landThenThrow: null, puts: [], deletes: [],
    async get(k) { const e = m.get(k); return e ? { text: async () => e.text, arrayBuffer: async () => e.bytes.buffer.slice(e.bytes.byteOffset, e.bytes.byteOffset + e.bytes.byteLength) } : null; },
    async head(k) { if (r2.failHead?.(k)) throw new Error('R2 head failed'); return m.has(k) ? {} : null; },
    async put(k, body) {
      const bytes = typeof body === 'string' ? new TextEncoder().encode(body) : new Uint8Array(body);
      if (r2.landThenThrow?.(k)) { m.set(k, { bytes, text: typeof body === 'string' ? body : '' }); throw new Error('R2 put response lost'); }
      if (r2.failPut?.(k)) throw new Error('R2 put failed');
      r2.puts.push(k);
      m.set(k, { bytes, text: typeof body === 'string' ? body : '' });
      return {};
    },
    async delete(k) { r2.deletes.push(k); m.delete(k); },
    async list({ prefix = '' } = {}) { return { objects: [...m.keys()].filter((k) => k.startsWith(prefix)).map((key) => ({ key })), truncated: false }; },
  };
  return r2;
}

// ── A PNG and Retro Diffusion ──

function pngB64(w, h) {
  const b = new Uint8Array(40);
  b.set([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13, 0x49, 0x48, 0x44, 0x52]);
  new DataView(b.buffer).setUint32(16, w); new DataView(b.buffer).setUint32(20, h);
  return Buffer.from(b).toString('base64');
}
const PNG = pngB64(64, 64);
let W = null; // the current world, for the fetch stub
const json = (o, status = 200) => new Response(JSON.stringify(o), { status, headers: { 'Content-Type': 'application/json' } });
globalThis.fetch = async (url, init = {}) => {
  const u = String(url);
  const rd = W.rd;
  if (u.endsWith('/v1/status')) { rd.calls.push('status'); return rd.status(); }
  if (u.includes('/v1/inferences/tasks/')) { const id = u.split('/').pop(); rd.calls.push(`poll ${id}`); rd.polls[id] = (rd.polls[id] ?? 0) + 1; return rd.poll(id, rd.polls[id]); }
  if (u.endsWith('/v1/inferences')) {
    const body = JSON.parse(init.body);
    if (body.async_process) { rd.calls.push(`submit ${body.prompt_style}`); return rd.submit(body); }
    rd.calls.push('create'); return rd.create(body);
  }
  rd.calls.push(`other ${u}`);
  return new Response('{}', { status: 404 });
};
function rdDefaults() {
  let n = 0;
  return {
    calls: [], polls: {},
    status: () => json({ status: { animations: 'ok' } }),
    create: () => json({ base64_images: [PNG], balance_cost: 0.02 }),
    submit: () => json({ task_id: `task_${++n}` }),
    poll: () => json({ status: 'succeeded', result: { base64_images: [PNG], balance_cost: 0.07 } }),
  };
}
const rdCalls = () => W.rd.calls.filter((c) => !c.startsWith('other'));
const billable = () => W.rd.calls.filter((c) => c === 'create' || c.startsWith('submit'));

// ── The world ──

function world() {
  const ledger = sqlite(['migrations-ledger/0001_control.sql', 'migrations-ledger/0002_stripe_refusals.sql', 'migrations-ledger/0003_ledger.sql']);
  const events = sqlite(['migrations/0001_events.sql', 'migrations/0002_digest_runs_attempts.sql']);
  const kv = kvStub();
  const r2 = r2Stub();
  const hooks = {};
  const w = { ledger, events, kv, r2, hooks, rd: rdDefaults(), env: null };
  w.env = { SPRITEBREW_KV: kv, GALLERY_BUCKET: r2, LEDGER_DB: d1(ledger, hooks), EVENTS_DB: d1(events), APP_ENV: 'dev', RETRO_DIFFUSION_API_KEY: 'test-key' };
  W = w;
  return w;
}
const ctxOf = (w) => ({ db: d1(w.ledger), now: () => T });
const setCtl = (w, key, value) => {
  if (value === undefined) { w.ledger.prepare('DELETE FROM control WHERE key = ?').run(key); return; }
  w.ledger.prepare("INSERT INTO control (key, value, updated_at_ms, updated_by) VALUES (?, ?, ?, 'test') ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at_ms = excluded.updated_at_ms").run(key, value, toBind(T));
};
const job = (w, id) => one(w.ledger, 'SELECT * FROM jobs WHERE job_id = ?', id);
const balanceOf = (w, uid) => one(w.ledger, 'SELECT balance FROM balances WHERE user_id = ?', uid)?.balance ?? null;
const refunds = (w, id) => q(w.ledger, "SELECT amount FROM ledger WHERE idem_key = 'refund:' || ?", id);
const eventsOf = (w, id, name) => q(w.events, 'SELECT * FROM events WHERE job_id = ? AND event_name = ?', id, name);
const alarmsOf = (w, kind) => q(w.events, "SELECT * FROM events WHERE event_name = 'ledger.alarm' AND error_code = ?", kind);
const status = (w, id) => { const e = w.r2.m.get(`jobs/${id}.json`); return e ? JSON.parse(e.text) : null; };
const kvStatus = (w, id) => { const e = w.kv.m.get(`job:${id}`); return e ? JSON.parse(e.value) : null; };

const USER = 'user_TESTTESTTESTTEST01';
let seq = 0;
/** A job debited the way Pages debits it, enqueued, with its message. */
async function debited(w, { mode = 'create', cost = 10, width = 128, height = 64, style, enqueue = true, user = USER, balance = 100, sendWH = true } = {}) {
  const jobId = `ledgertest_job_${++seq}`;
  const ctx = ctxOf(w);
  if (balanceOf(w, user) === null) await L.openBalance(ctx, { uid: user, amount: balance, reason: 'signup_bonus', source: 'signup', via: 'signup' });
  const ps = style ?? (mode === 'animate' ? 'rd_advanced_animation__walking' : 'rd_fast__no_style');
  const meta = sendWH ? JSON.stringify({ width, height }) : null;
  const d = await L.generationDebit(ctx, { uid: user, job: jobId, cost, mode, ckey: `k${seq}`, hash: `h${seq}`, style: ps, size: sendWH ? width : null, meta });
  if (d.outcome !== 'charged') throw new Error(`setup: ${d.outcome}`);
  if (enqueue) await L.markEnqueued(ctx, jobId);
  const body = { prompt: 'a red apple', prompt_style: ps, ...(sendWH ? { width, height } : {}), ...(mode === 'animate' ? { input_image: 'AAAA', frames_duration: 4 } : {}) };
  return { jobId, msg: { jobId, userId: user, idempotencyKey: `k${seq}`, tokenCost: cost, mode, body, enqueuedAt: T } };
}
let mid = 0;
function message(body, attempts = 1) {
  const m = { id: `msg_${++mid}`, body, attempts, timestamp: new Date(T), result: null };
  m.ack = () => { m.result = { ack: true }; };
  m.retry = (o) => { m.result = { retry: o?.delaySeconds ?? null }; };
  return m;
}
async function deliver(w, body, attempts = 1) { const m = message(body, attempts); W = w; await worker.queue({ queue: 'spritebrew-rd-jobs-dev', messages: [m] }, w.env, {}); return m.result; }
async function deadLetter(w, body, attempts = 1) { const m = message(body, attempts); W = w; await worker.queue({ queue: 'spritebrew-rd-jobs-dlq-dev', messages: [m] }, w.env, {}); return m.result; }
async function cron(w) { W = w; await worker.scheduled({}, w.env, {}); }
const acked = (r) => r?.ack === true;
const retried = (r, s) => r && 'retry' in r && (s === undefined || r.retry === s);
/** Test setup only: a row moved to a given state, as an earlier invocation would leave it. */
const setRow = (w, id, fields) => {
  const cols = Object.keys(fields);
  w.ledger.prepare(`UPDATE jobs SET ${cols.map((c) => `${c} = ?`).join(', ')} WHERE job_id = ?`).run(...cols.map((c) => toBind(fields[c])), id);
};

// ════════════════════════════════════════════════════════════════════════
// The normal path (the base the T rows stand on)
// ════════════════════════════════════════════════════════════════════════
{
  const w = world();
  const { jobId, msg } = await debited(w, { cost: 12 });
  const r = await deliver(w, msg);
  const j = job(w, jobId);
  check('base', 'a create: one RD call, acked, the row succeeded and published, the status written, no refund',
    acked(r) && billable().length === 1 && j.outcome === 'succeeded' && j.artifact === 'published' && j.status_written_at_ms !== null && refunds(w, jobId).length === 0);
  check('base', 'the PNG staged at {userId}/{jobId}.png and the gen: index published with the staged createdAt',
    w.r2.m.has(`${USER}/${jobId}.png`) && [...w.kv.m.keys()].some((k) => k.startsWith(`gen:${USER}:`) && k.endsWith(`:${jobId}`)));
  const s = status(w, jobId);
  check('base', 'the success record: status, the PNG as resultBase64, its completedAt, both stores', s?.status === 'success' && s.resultBase64 === PNG && kvStatus(w, jobId)?.status === 'success');
  check('base', "generation.succeeded written once", eventsOf(w, jobId, 'generation.succeeded').length === 1);
  check('base', "the running record never carried tokenCost (4.10)", !logs.some((l) => l.includes('"tokenCost"') && l.includes('"status":"running"')));
  const a = await debited(w, { mode: 'animate', width: 64, height: 64 });
  W.rd.calls = [];
  const r2 = await deliver(w, a.msg);
  check('base', 'an animate: the status pre-flight, one submit, a poll, succeeded', acked(r2) && W.rd.calls[0] === 'status' && billable().length === 1 && job(w, a.jobId).outcome === 'succeeded' && job(w, a.jobId).task_id === 'task_1');
}

// ════════════════════════════════════════════════════════════════════════
// T8 and T8b: claims
// ════════════════════════════════════════════════════════════════════════
{
  // two copies at once: the second meets a live owner
  const w = world();
  const { jobId, msg } = await debited(w);
  let inner = null;
  w.rd.create = () => { if (!inner) inner = deliver(w, msg); return json({ base64_images: [PNG] }); };
  const outer = await deliver(w, msg);
  const dup = await inner;
  check('T8', 'two copies at once: one RD call, the duplicate (attempt 1) acks as a live owner, one success', acked(outer) && acked(dup) && billable().length === 1 && job(w, jobId).outcome === 'succeeded');
}
{
  // owner killed after `submitted`: never RD again; the canceller refunds once it is stale past the floor
  const w = world();
  const { jobId, msg } = await debited(w, { cost: 7 });
  const ctx = ctxOf(w);
  const c = await L.claimJob(ctx, 'submit', { job: jobId, claim: 'dead', attempt: 1 });
  await L.ownerUpdate(ctx, 'submitted', { job: jobId, claim: 'dead' });
  let r = await deliver(w, msg, 2);
  check('T8', "owner killed after `submitted`, redelivered at once: a live owner, retry 30 s, no RD", c.outcome === 'won' && retried(r, 30) && billable().length === 0);
  T += 6 * MIN;
  r = await deliver(w, msg, 2);
  check('T8', 'stale (5 minutes) but under the 20-minute floor: the canceller finds a live owner, nothing moved, never RD', retried(r) && refunds(w, jobId).length === 0 && billable().length === 0);
  T += 15 * MIN;
  r = await deliver(w, msg, 3);
  const j = job(w, jobId);
  check('T8', "past the floor: refunded once, 'rd_create_outcome_unknown', never RD", acked(r) && j.outcome === 'refunded' && j.error_code === 'rd_create_outcome_unknown' && refunds(w, jobId).length === 1 && billable().length === 0);
  check('T8', 'its error status: refunded true', status(w, jobId)?.refunded === true && status(w, jobId)?.errorCode === 'rd_create_outcome_unknown');
}
{
  // a stale owner with nothing billable outstanding: the new delivery takes the claim and runs
  const w = world();
  const { jobId, msg } = await debited(w);
  await L.claimJob(ctxOf(w), 'submit', { job: jobId, claim: 'gone', attempt: 1 });
  T += 6 * MIN;
  const r = await deliver(w, msg, 2);
  check('T8', 'a stale owner (no submit on record): the redelivery reclaims and runs once', acked(r) && billable().length === 1 && job(w, jobId).outcome === 'succeeded' && job(w, jobId).claim_attempt === 2);
}
{
  // animate duplicates during the submit; T8b: a 590 s submit with a duplicate
  const w = world();
  const { jobId, msg } = await debited(w, { mode: 'animate', width: 64, height: 64 });
  let dup = null;
  w.rd.submit = () => { T += 590_000; if (!dup) dup = deliver(w, msg, 2); return json({ task_id: 'task_slow' }); };
  const r = await deliver(w, msg);
  const d = await dup;
  check('T8b', 'a 590 s animate submit with a duplicate: the duplicate is a live owner (retry 30 s), the owner keeps it, one submit',
    acked(r) && retried(d, 30) && W.rd.calls.filter((c) => c.startsWith('submit')).length === 1 && job(w, jobId).outcome === 'succeeded' && job(w, jobId).task_id === 'task_slow');
}
{
  // the dead-letter handler never claims and never calls RD
  const w = world();
  const { jobId, msg } = await debited(w);
  T += 41 * MIN;
  const r = await deadLetter(w, msg);
  check('T8', 'a dead letter is never claimed and never runs RD: refunded once by the canceller', acked(r) && rdCalls().length === 0 && job(w, jobId).claim_id === null && job(w, jobId).outcome === 'refunded' && refunds(w, jobId).length === 1);
}
{
  // 429: the create release, then a retry runs at once
  const w = world();
  const { jobId, msg } = await debited(w);
  let n = 0;
  w.rd.create = () => (++n === 1 ? new Response('busy', { status: 429 }) : json({ base64_images: [PNG] }));
  const r1 = await deliver(w, msg, 1);
  const j1 = job(w, jobId);
  check('T8', '429: the create release (released, submitted cleared) and a retry, no refund', retried(r1, null) && j1.released_at_ms !== null && j1.submitted_at_ms === null && refunds(w, jobId).length === 0);
  const r2 = await deliver(w, msg, 2);
  check('T8', '429 then a retry: the released claim is taken at once and succeeds', acked(r2) && job(w, jobId).outcome === 'succeeded' && billable().length === 2);
}
{
  // a `submitted` update whose response is lost: read back before the RD call (R3-17)
  const w = world();
  const { jobId, msg } = await debited(w);
  setCtl(w, 'dev_fault', 'batch_response_lost:submitted');
  const r = await deliver(w, msg);
  check('T8', 'a `submitted` update whose response is lost: read back committed, then exactly one RD call', acked(r) && billable().length === 1 && job(w, jobId).outcome === 'succeeded');
  const w2 = world();
  const b = await debited(w2);
  setCtl(w2, 'dev_fault', 'batch_throw_before:submitted');
  const r2 = await deliver(w2, b.msg);
  check('T8', 'a `submitted` update that never lands, twice: the message retries, no RD call', retried(r2, null) && billable().length === 0 && job(w2, b.jobId).submitted_at_ms === null);
}

// ════════════════════════════════════════════════════════════════════════
// T11, T11c, T28: paused answers (ported from release 1's money-pause-test)
// ════════════════════════════════════════════════════════════════════════
{
  const w = world();
  const { jobId, msg } = await debited(w);
  setCtl(w, 'money_pause', '1');
  const kvPuts = w.kv.puts.length, r2Puts = w.r2.puts.length;
  let r = await deliver(w, msg);
  check('T11', 'gate: a fresh delivery while paused retries in 900 s', retried(r, 900));
  check('T11', 'gate: no claim, no record written, no money, no RD call of any kind', job(w, jobId).claim_id === null && w.kv.puts.length === kvPuts && w.r2.puts.length === r2Puts && rdCalls().length === 0);
  r = await deliver(w, msg, 4);
  check('T11', 'gate: the last main-queue delivery while paused: a retry (to the dead-letter queue), nothing written', retried(r, 900) && job(w, jobId).state === 'enqueued');
  setCtl(w, 'dev_fault', 'paused_retry_60');
  check('T11', "gate: dev_fault 'paused_retry_60' on dev: 60 s", retried(await deliver(w, msg), 60));
  w.env.APP_ENV = 'production';
  check('T11', 'gate: the same dev_fault in production: still 900 s', retried(await deliver(w, msg), 900));
  w.env.APP_ENV = 'dev';
  setCtl(w, 'dev_fault', '');
  // T28: a paused animate delivery, before the pre-flight
  setCtl(w, 'money_pause', '0');
  const a = await debited(w, { mode: 'animate', width: 64, height: 64 });
  setCtl(w, 'money_pause', '1');
  W.rd.calls = [];
  r = await deliver(w, a.msg);
  check('T28', 'a paused animate delivery: no RD call of any kind (the status GET included), nothing written, retry 900 s', retried(r, 900) && W.rd.calls.length === 0 && job(w, a.jobId).claim_id === null);
  // T11c: the pause read fails
  w.hooks.before = (sqls) => { if (sqls.some((s) => s.includes("key = 'money_pause'") && !s.includes('EXISTS'))) throw new Error('D1 down'); };
  setCtl(w, 'money_pause', '0');
  r = await deliver(w, msg);
  check('T11c', 'the pause read fails: held (retry), nothing written, no RD call', retried(r) && billable().length === 0 && job(w, jobId).claim_id === null);
  check('T11c', 'the pause read fails: logged', logs.some((l) => l.includes('pause_read_failed')));
  w.hooks.before = undefined;
  const unbound = { ...w.env, LEDGER_DB: undefined };
  const m = message(msg); W = w;
  await worker.queue({ queue: 'spritebrew-rd-jobs-dev', messages: [m] }, unbound, {});
  check('T11c', 'LEDGER_DB unbound: retried, no RD call', retried(m.result) && billable().length === 0);
  // a finished row while paused: acked as before
  setCtl(w, 'money_pause', '0');
  const f = await debited(w);
  await deliver(w, f.msg);
  setCtl(w, 'money_pause', '1');
  check('T11', 'a finished row while paused: acked, nothing run', acked(await deliver(w, f.msg)) && billable().length === 1);
}
{
  // T11c's other half: every settling batch errors closed when the pause row is unreadable to the batch
  for (const [label, value] of [['absent', undefined], ["'x'", 'x'], ["''", '']]) {
    const w = world();
    const { jobId, msg } = await debited(w);
    T += 41 * MIN;
    setCtl(w, 'money_pause', value);
    const r = await deadLetter(w, msg);
    check('T11c', `money_pause ${label}: the dead letter's canceller refund answers paused (retry 900 s), nothing moved`, retried(r, 900) && refunds(w, jobId).length === 0 && job(w, jobId).finished_at_ms === null);
  }
}
{
  // T11: the resume under the pause, and a failure that owes a refund
  const w = world();
  const { jobId, msg } = await debited(w, { mode: 'animate', width: 64, height: 64 });
  const ctx = ctxOf(w);
  await L.claimJob(ctx, 'submit', { job: jobId, claim: 'o', attempt: 1 });
  await L.ownerUpdate(ctx, 'submitted', { job: jobId, claim: 'o' });
  await L.ownerUpdate(ctx, 'task', { job: jobId, claim: 'o', task: 'task_live' });
  T += 6 * MIN;
  setCtl(w, 'money_pause', '1');
  w.rd.poll = () => json({ status: 'failed', error: 'bad' });
  let r = await deliver(w, msg, 2);
  check('T11', 'the resume while paused: the poll runs (it bills nothing), no submit', W.rd.calls.some((c) => c.startsWith('poll')) && billable().length === 0);
  check('T11', 'its failure while paused: no refund, retry 900 s, the unfinished row is the debt', retried(r, 900) && refunds(w, jobId).length === 0 && job(w, jobId).finished_at_ms === null);
  setCtl(w, 'money_pause', '0');
  T += 6 * MIN;
  r = await deliver(w, msg, 3);
  check('T11', 'after the unpause: the failure refunds once, its error status refunded true', acked(r) && refunds(w, jobId).length === 1 && status(w, jobId)?.refunded === true);
  r = await deliver(w, msg, 3);
  check('T11', 'a later delivery: acked, no second refund', acked(r) && refunds(w, jobId).length === 1);
}
{
  // T11: the sweep while paused; the dead letter while paused, and through its last delivery
  const w = world();
  const { jobId, msg } = await debited(w);
  T += 61 * MIN;
  setCtl(w, 'money_pause', '1');
  await cron(w);
  check('T11', 'the sweep while paused: no candidate settled', refunds(w, jobId).length === 0 && logs.some((l) => l.includes('sweep candidates skipped')));
  let r = await deadLetter(w, msg, 1);
  check('T11', 'a dead letter while paused: retry 900 s, no refund', retried(r, 900) && refunds(w, jobId).length === 0);
  r = await deadLetter(w, msg, 6);
  check('T11', "a dead letter paused through its last delivery: the alarm row ('paused'), then ack", acked(r)
    && q(w.events, "SELECT 1 FROM events WHERE event_name = 'generation.unrefunded' AND job_id = ? AND json_extract(event_json, '$.extra.reason') = 'paused'", jobId).length === 1);
  check('T11', 'the row is the kept debt: unfinished, no refund', job(w, jobId).finished_at_ms === null && refunds(w, jobId).length === 0);
  setCtl(w, 'money_pause', '0');
  await cron(w);
  check('T11', 'after the unpause the sweep settles it once (candidate 5, alarm)', refunds(w, jobId).length === 1 && job(w, jobId).error_code === 'enqueued_never_claimed' && alarmsOf(w, 'enqueued_never_claimed').length === 1);
  await cron(w);
  check('T11', 'a second sweep: nothing more', refunds(w, jobId).length === 1);
  // the alarm insert fails at the last paused delivery: retry (the message is deleted), the row stays the debt
  const w2 = world();
  const b = await debited(w2);
  T += 61 * MIN;
  setCtl(w2, 'money_pause', '1');
  setCtl(w2, 'dev_fault', 'events_db_absent');
  r = await deadLetter(w2, b.msg, 6);
  check('T11', 'paused at the last delivery with the alarm row failing: retried (deleting the message), the jobs row the kept debt', retried(r, null) && job(w2, b.jobId).finished_at_ms === null);
  setCtl(w2, 'money_pause', '0'); setCtl(w2, 'dev_fault', '');
  await cron(w2);
  check('T11', 'and the sweep settles it once after the unpause', refunds(w2, b.jobId).length === 1);
}

// ════════════════════════════════════════════════════════════════════════
// T14, T20: the status write fails; the repair pass writes it
// ════════════════════════════════════════════════════════════════════════
{
  const w = world();
  const { jobId, msg } = await debited(w, { cost: 9 });
  w.rd.create = () => new Response('bad', { status: 400 });
  w.r2.failPut = (k) => k === `jobs/${jobId}.json`;
  let r = await deliver(w, msg);
  check('T14', 'a refund commits and its status write fails: acked, refunded once, the marker NULL', acked(r) && refunds(w, jobId).length === 1 && job(w, jobId).status_written_at_ms === null);
  check('T20', 'R2 first: a failed R2 status put leaves no terminal KV copy behind it', kvStatus(w, jobId)?.status !== 'error');
  w.r2.failPut = null;
  await cron(w);
  check('T14', 'the repair pass writes it: refunded true in both stores, the marker set', status(w, jobId)?.refunded === true && kvStatus(w, jobId)?.refunded === true && job(w, jobId).status_written_at_ms !== null);
  const b = await debited(w);
  w.rd.create = () => json({ base64_images: [PNG] });
  w.r2.failPut = (k) => k === `jobs/${b.jobId}.json`;
  r = await deliver(w, b.msg);
  check('T14', 'a success commits and its status write fails: acked, succeeded and published, the marker NULL', acked(r) && job(w, b.jobId).outcome === 'succeeded' && job(w, b.jobId).status_written_at_ms === null);
  w.r2.failPut = null;
  await cron(w);
  check('T14', 'repaired: the success record rebuilt from the PNG and artifact_meta_json, the marker set', status(w, b.jobId)?.resultBase64 === PNG && job(w, b.jobId).status_written_at_ms !== null);
  // T20: KV expires before the repair; R2 is written again from the row
  const c = await debited(w);
  w.r2.failPut = (k) => k === `jobs/${c.jobId}.json`;
  await deliver(w, c.msg);
  w.r2.failPut = null;
  T += 2 * HOUR;
  await cron(w);
  check('T20', 'a terminal status that failed in R2, KV long expired: the repair pass writes R2 from the row', status(w, c.jobId)?.status === 'success' && job(w, c.jobId).status_written_at_ms !== null);
  w.env.GALLERY_BUCKET = undefined;
  const d = await debited(w);
  w.env.GALLERY_BUCKET = w.r2;
  void d;
}

// ════════════════════════════════════════════════════════════════════════
// T15, T19: staged results
// ════════════════════════════════════════════════════════════════════════
{
  // T19: a result produced while paused; the success update answers paused; the first delivery after the unpause finalizes it
  const w = world();
  const { jobId, msg } = await debited(w);
  w.rd.create = () => { setCtl(w, 'money_pause', '1'); return json({ base64_images: [PNG] }); };
  let r = await deliver(w, msg);
  let j = job(w, jobId);
  check('T19', 'a result produced while paused: staged, the R2 object present, the success update paused (retry 900 s)', retried(r, 900) && j.artifact === 'staged' && j.finished_at_ms === null && w.r2.m.has(`${USER}/${jobId}.png`));
  T += 41 * MIN;
  r = await deadLetter(w, msg);
  check('T19', 'no canceller refunds it while paused: the dead letter cannot finalize either (retry), nothing moved', retried(r) && refunds(w, jobId).length === 0);
  setCtl(w, 'money_pause', '0');
  T += 6 * MIN;
  w.rd.create = () => json({ base64_images: [PNG] });
  r = await deliver(w, msg, 2);
  j = job(w, jobId);
  check('T19', 'after the unpause the first delivery finalizes it (c-finalize, PNG present): succeeded and published, no RD call, no refund',
    acked(r) && j.outcome === 'succeeded' && j.artifact === 'published' && billable().length === 1 && refunds(w, jobId).length === 0);
}
{
  // T19 and T15: the owner killed after the paused answer, no redelivery: candidate (6) finalizes after the unpause
  const w = world();
  const { jobId, msg } = await debited(w);
  w.rd.create = () => { setCtl(w, 'money_pause', '1'); return json({ base64_images: [PNG] }); };
  await deliver(w, msg);
  setCtl(w, 'money_pause', '0');
  T += 2 * MIN;
  await cron(w);
  check('T19', 'candidate (6) waits for the 300 s lease', job(w, jobId).artifact === 'staged');
  T += 4 * MIN;
  await cron(w);
  check('T19', 'candidate (6) finalizes on the first sweep after the unpause once the lease is 300 s old', job(w, jobId).outcome === 'succeeded' && job(w, jobId).artifact === 'published');
  check('T15', 'a canceller meeting a staged row never refunds it', refunds(w, jobId).length === 0);
}
{
  // T15 and T19: a staged row whose object is absent: one refund with result_store_failed
  const w = world();
  const { jobId, msg } = await debited(w, { cost: 6 });
  w.rd.create = () => { setCtl(w, 'money_pause', '1'); return json({ base64_images: [PNG] }); };
  await deliver(w, msg);
  w.r2.m.delete(`${USER}/${jobId}.png`);
  T += 6 * MIN;
  let r = await deliver(w, msg, 2);
  check('T19', 'a staged row whose object is absent, while paused: the finalizer refunds nothing and writes no status (retry 900 s)', retried(r, 900) && refunds(w, jobId).length === 0 && status(w, jobId)?.status !== 'error');
  setCtl(w, 'money_pause', '0');
  T += 6 * MIN;
  r = await deliver(w, msg, 3);
  check('T15', "the finalizer's absent branch after the unpause: one refund, 'result_store_failed'", acked(r) && refunds(w, jobId).length === 1 && job(w, jobId).error_code === 'result_store_failed');
}
{
  // T19: failed R2 puts, and a put whose response was lost
  const w = world();
  const { jobId, msg } = await debited(w, { cost: 5 });
  w.r2.failPut = (k) => k.endsWith('.png');
  let r = await deliver(w, msg);
  check('T19', "a failed R2 put (three attempts, head absent): one refund, 'result_store_failed'", acked(r) && refunds(w, jobId).length === 1 && job(w, jobId).error_code === 'result_store_failed');
  w.r2.failPut = null;
  const b = await debited(w);
  w.r2.landThenThrow = (k) => k.endsWith('.png');
  r = await deliver(w, b.msg);
  check('T19', 'a put whose response was lost (head finds it): succeeded, never a refund', acked(r) && job(w, b.jobId).outcome === 'succeeded' && refunds(w, b.jobId).length === 0);
  w.r2.landThenThrow = null;
  const a = await debited(w, { mode: 'animate', width: 64, height: 64 });
  w.r2.failPut = (k) => k.endsWith('.png');
  r = await deliver(w, a.msg);
  check('T19', "an animate's failed R2 put: one refund, 'result_store_failed'", acked(r) && job(w, a.jobId).error_code === 'result_store_failed');
  w.r2.failPut = null;
  // a stage update whose response is lost
  const s = await debited(w);
  setCtl(w, 'dev_fault', 'batch_response_lost:stage');
  r = await deliver(w, s.msg);
  check('T19', 'a stage update whose response is lost: resolved from its markers, then succeeded', acked(r) && job(w, s.jobId).outcome === 'succeeded');
  // a success update that throws after staging, on the last main-queue attempt: never refunds
  const t = await debited(w);
  setCtl(w, 'dev_fault', 'batch_throw_before:success');
  r = await deliver(w, t.msg, 4);
  check('T19', 'a success update that throws after staging, on the last attempt: retried, never refunded, the row staged', retried(r) && refunds(w, t.jobId).length === 0 && job(w, t.jobId).artifact === 'staged');
  setCtl(w, 'dev_fault', '');
  // a c-finalize that loses to a live owner, then wins once the owner is gone
  T += MIN;
  r = await deliver(w, t.msg, 5);
  check('T19', 'a c-finalize that meets a live owner: retry 900 s, never loops into RD', retried(r, 900) && billable().length === 5);
  T += 6 * MIN;
  r = await deliver(w, t.msg, 6);
  check('T19', 'then it wins and finalizes', acked(r) && job(w, t.jobId).outcome === 'succeeded');
  setCtl(w, 'dev_fault', 'batch_response_lost:claim');
  const u = await debited(w);
  w.rd.create = () => { setCtl(w, 'money_pause', '1'); return json({ base64_images: [PNG] }); };
  setCtl(w, 'dev_fault', '');
  await deliver(w, u.msg);
  setCtl(w, 'money_pause', '0');
  w.rd.create = () => json({ base64_images: [PNG] });
  T += 6 * MIN;
  setCtl(w, 'dev_fault', 'batch_response_lost:claim');
  r = await deliver(w, u.msg, 2);
  setCtl(w, 'dev_fault', '');
  check('T19', 'a c-finalize whose response is lost: its read-back decides, finalized once', acked(r) && job(w, u.jobId).outcome === 'succeeded');
}

// ════════════════════════════════════════════════════════════════════════
// T17, T32, T34: imported rows, their cancellers, and the status rebuild
// ════════════════════════════════════════════════════════════════════════
const KV_USER = 'user_IMPORTIMPORTIMP01';
function imported(w, extra = {}) {
  const jobId = `ledgertest_kvjob_${++seq}`;
  if (balanceOf(w, KV_USER) === null) w.ledger.prepare("INSERT INTO balances VALUES (?, 50, 1, 1, 'snapshot', NULL)").run(KV_USER);
  const r = { job_id: jobId, user_id: KV_USER, mode: 'create', token_cost: 8, provenance: 'kv', state: 'claimed', created_at_ms: T - DAY, import_json: '{"class":"pending","fingerprint":"f"}', ...extra };
  const cols = Object.keys(r);
  w.ledger.prepare(`INSERT INTO jobs (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`).run(...cols.map((c) => toBind(r[c])));
  return { jobId, msg: { jobId, userId: KV_USER, idempotencyKey: 'x', tokenCost: 8, mode: 'create', body: { prompt: 'p', prompt_style: 'rd_fast__no_style', width: 64, height: 64 }, enqueuedAt: T - DAY } };
}
const plant = (w, jobId, { png = false, index = false } = {}) => {
  const createdAt = T - DAY + 5;
  if (png) w.r2.m.set(`${KV_USER}/${jobId}.png`, { bytes: Buffer.from(PNG, 'base64'), text: '' });
  if (index) w.kv.m.set(`gen:${KV_USER}:900:${jobId}`, { value: '', metadata: { jobId, prompt: 'p', style: 'rd_fast__no_style', mode: 'create', createdAt, v: 1 } });
  return createdAt;
};
for (const [name, run] of [
  ['candidate (3), refund due', async (w, i) => { setRow(w, i.jobId, { refund_due_code: 'provider_error' }); await cron(w); }],
  ['candidate (2), a stale claim', async (w, i) => { setRow(w, i.jobId, { claim_id: 'x', claimed_at_ms: T - DAY, lease_at_ms: T - DAY }); await cron(w); }],
  ['candidate (4), unowned 30 minutes after the unpause', async (w, i) => { setRow(w, i.jobId, { state: 'enqueued' }); setCtl(w, 'switch_at_ms', String(T - HOUR)); await cron(w); }],
  ['the dead-letter handler', async (w, i) => { await deadLetter(w, i.msg); }],
  ['a main delivery', async (w, i) => { setRow(w, i.jobId, { refund_due_code: 'provider_error' }); await deliver(w, i.msg); }],
]) {
  for (const [what, p] of [['PNG and index', { png: true, index: true }], ['PNG only', { png: true }], ['index only', { index: true }], ['neither', {}]]) {
    const w = world();
    const i = imported(w);
    plant(w, i.jobId, p);
    await run(w, i);
    const j = job(w, i.jobId);
    let ok;
    if (p.png && p.index) ok = j.outcome === 'succeeded' && j.artifact === 'published' && refunds(w, i.jobId).length === 0 && status(w, i.jobId)?.resultBase64 === PNG;
    else if (p.png) ok = j.outcome === 'refunded' && refunds(w, i.jobId).length === 1 && !w.r2.m.has(`${KV_USER}/${i.jobId}.png`) && j.artifact === 'discarded';
    else if (p.index) ok = j.hold_reason === 'index_only' && refunds(w, i.jobId).length === 0 && alarmsOf(w, 'index_only').length === 1;
    else ok = j.outcome === 'refunded' && refunds(w, i.jobId).length === 1 && j.refunded_amount === 8;
    check('T17', `${name}, ${what}: as 4.12 decides`, ok);
  }
}
{
  // a consumer claims between the check and the refund: no delete, no discarded
  const w = world();
  const i = imported(w, { claim_id: 'x', claimed_at_ms: T - DAY, lease_at_ms: T - DAY });
  plant(w, i.jobId, { png: true });
  w.hooks.before = (sqls) => { if (sqls.some((s) => s.includes("'generation_failed_refund', 'generation_failed_refund'"))) { w.hooks.before = undefined; setRow(w, i.jobId, { claim_id: 'live', lease_at_ms: T, released_at_ms: null }); } };
  await cron(w);
  check('T17', 'a consumer claims between the check and the refund: nothing moved, no delete, not discarded',
    refunds(w, i.jobId).length === 0 && w.r2.m.has(`${KV_USER}/${i.jobId}.png`) && job(w, i.jobId).artifact === 'none');
  // a delivered finish that loses its fence: no status, no published
  const w2 = world();
  const i2 = imported(w2, { claim_id: 'x', claimed_at_ms: T - DAY, lease_at_ms: T - DAY });
  plant(w2, i2.jobId, { png: true, index: true });
  w2.hooks.before = (sqls) => { if (sqls.some((s) => s.includes("outcome = 'succeeded', artifact = 'staged'"))) { w2.hooks.before = undefined; setRow(w2, i2.jobId, { claim_id: 'live', lease_at_ms: T }); } };
  await cron(w2);
  check('T17', 'a delivered finish that loses its fence: no status, not published', job(w2, i2.jobId).finished_at_ms === null && !status(w2, i2.jobId));
  // refused by the pause: paused, not fence lost; no refund follows
  const w3 = world();
  const i3 = imported(w3);
  plant(w3, i3.jobId, { png: true, index: true });
  setRow(w3, i3.jobId, { refund_due_code: 'provider_error' });
  setCtl(w3, 'money_pause', '1');
  const r = await deliver(w3, i3.msg);
  check('T17', 'a delivered finish refused by the pause: retry 900 s, no refund follows', retried(r, 900) && refunds(w3, i3.jobId).length === 0 && job(w3, i3.jobId).finished_at_ms === null);
  // a delivered finish whose status write fails: rebuilt from the PNG and meta (T32, T34)
  const w4 = world();
  const i4 = imported(w4, { refund_due_code: 'provider_error' });
  const planted4 = plant(w4, i4.jobId, { png: true, index: true });
  w4.r2.failPut = (k) => k.startsWith('jobs/');
  await cron(w4);
  check('T32', 'a kv row finished by the delivered finish with a failed status write: the marker NULL', job(w4, i4.jobId).outcome === 'succeeded' && job(w4, i4.jobId).status_written_at_ms === null);
  w4.r2.failPut = null;
  await cron(w4);
  check('T34', 'a 4.12 delivered finish whose status write failed: rebuilt from the PNG and artifact_meta_json (no repair_record), the marker set',
    status(w4, i4.jobId)?.resultBase64 === PNG && status(w4, i4.jobId)?.completedAt === planted4 && job(w4, i4.jobId).status_written_at_ms !== null);
  // 4.18's delivered settlement ('succeeded') with a failed status write
  const w5 = world();
  const i5 = imported(w5, { hold_reason: 'index_only' });
  plant(w5, i5.jobId, { png: true });
  const settledAt5 = T - 7;
  setRow(w5, i5.jobId, { state: 'finished', outcome: 'succeeded', hold_reason: null, artifact: 'published', finished_at_ms: T,
    artifact_meta_json: JSON.stringify({ jobId: i5.jobId, prompt: 'p', style: 'rd_fast__no_style', mode: 'create', createdAt: settledAt5, v: 1 }) });
  await cron(w5);
  check('T34', "a 4.18 delivered settlement ('succeeded') with no status: the repair pass rebuilds it from the PNG and artifact_meta_json", status(w5, i5.jobId)?.resultBase64 === PNG
    && status(w5, i5.jobId)?.completedAt === settledAt5 && job(w5, i5.jobId).status_written_at_ms !== null);
  // T32: an imported success with repair_record: mirrored strictly, marker after the verified write
  const w6 = world();
  const rec = { status: 'success', userId: KV_USER, mode: 'create', enqueuedAt: 1, startedAt: 2, completedAt: 3, resultBase64: PNG };
  const i6 = imported(w6, { state: 'finished', outcome: 'succeeded', artifact: 'published', finished_at_ms: T, import_json: JSON.stringify({ class: 'identity_succeeded', repair_record: rec, fingerprint: 'f' }) });
  w6.r2.failPut = () => true;
  await cron(w6);
  check('T32', 'a KV-only imported success: the marker stays NULL until the repair pass writes R2 from repair_record', job(w6, i6.jobId).status_written_at_ms === null);
  w6.r2.failPut = null;
  await cron(w6);
  check('T32', 'then the R2 copy is repair_record and the marker set', status(w6, i6.jobId)?.completedAt === 3 && job(w6, i6.jobId).status_written_at_ms !== null);
}

// ════════════════════════════════════════════════════════════════════════
// T18, T21, T24, T29, T25
// ════════════════════════════════════════════════════════════════════════
{
  const w = world();
  const { jobId, msg } = await debited(w, { cost: 4 });
  T += 61 * MIN;
  await cron(w);
  check('T18', "enqueued, never claimed, 60 minutes: refunded once by candidate (5), 'enqueued_never_claimed', alarm", refunds(w, jobId).length === 1 && job(w, jobId).error_code === 'enqueued_never_claimed' && alarmsOf(w, 'enqueued_never_claimed').length === 1);
  const r = await deliver(w, msg);
  check('T18', 'the late delivery acks; no second refund, no RD call', acked(r) && refunds(w, jobId).length === 1 && billable().length === 0);
}
{
  const w = world();
  const a = imported(w, { hold_reason: 'contradictory', token_cost: null, created_at_ms: T - 2 * DAY });
  const b = await debited(w);
  w.r2.failPut = (k) => k === `jobs/${b.jobId}.json`;
  await deliver(w, b.msg);
  T += DAY + MIN;
  await cron(w);
  await cron(w);
  check('T21', 'an unfinished row past a day: one unfinished_overdue alarm (deduped across runs)', alarmsOf(w, 'unfinished_overdue').filter((x) => x.job_id === a.jobId).length === 1);
  check('T21', 'a status unwritten past a day: one repair_overdue alarm', alarmsOf(w, 'repair_overdue').filter((x) => x.job_id === b.jobId).length === 1);
  // the overdue alarms are part of the repair pass and wait for the phase with it
  const w2 = world();
  const c = imported(w2, { hold_reason: 'contradictory', token_cost: null, created_at_ms: T - 2 * DAY });
  setCtl(w2, 'migration_open', '1');
  await cron(w2);
  const whileOpen = alarmsOf(w2, 'unfinished_overdue').length;
  setCtl(w2, 'migration_open', '0');
  await cron(w2);
  check('T21', "the overdue alarms wait while the migrator's phase is open, then come once it reads '0'", whileOpen === 0 && alarmsOf(w2, 'unfinished_overdue').filter((x) => x.job_id === c.jobId).length === 1);
}
{
  const w = world();
  const { jobId, msg } = await debited(w, { cost: 3 });
  T += 41 * MIN;
  w.hooks.before = (sqls) => { if (sqls.some((s) => s.startsWith('SELECT * FROM jobs'))) throw new Error('D1 read failed'); };
  let r = await deadLetter(w, msg, 2);
  check('T24', 'a dead-letter delivery that throws, not the last: retried', retried(r, null));
  r = await deadLetter(w, msg, 6);
  check('T24', "retries_exhausted at the last delivery: the alarm row and ack; the row survives", acked(r)
    && q(w.events, "SELECT 1 FROM events WHERE event_name = 'generation.unrefunded' AND json_extract(event_json, '$.extra.reason') = 'retries_exhausted'").length === 1 && job(w, jobId).finished_at_ms === null);
  w.hooks.before = undefined;
  T += 20 * MIN;
  await cron(w);
  await cron(w);
  check('T24', 'the sweep settles it once', refunds(w, jobId).length === 1);
}
{
  const w = world();
  const msg = { jobId: 'ledgertest_job_ghost', userId: USER, idempotencyKey: 'x', tokenCost: 11, mode: 'create', body: { prompt: 'p', prompt_style: 'rd_fast__no_style', width: 64, height: 64 }, enqueuedAt: T };
  const r = await deliver(w, msg);
  const s = status(w, 'ledgertest_job_ghost');
  check('T29', "no row: the tombstone, alarm no_record (with the message's tokenCost), ack", acked(r) && job(w, 'ledgertest_job_ghost').outcome === 'no_record'
    && alarmsOf(w, 'no_record').some((x) => JSON.parse(x.event_json).extra?.tokenCost === 11));
  check('T29', "its status: the unresolved record (error, no_record, refunded false, unresolved), never pending, never a refund",
    s?.status === 'error' && s.errorCode === 'no_record' && s.refunded === false && s.unresolved === true && q(w.ledger, 'SELECT 1 FROM ledger WHERE job_id = ?', 'ledgertest_job_ghost').length === 0);
  check('T29', "its copy is HQ-3's Proposal, verbatim", s?.error === 'We could not confirm what happened to this generation. It has been flagged for review.');
  setCtl(w, 'money_pause', '1');
  const r2 = await deliver(w, { ...msg, jobId: 'ledgertest_job_ghost2' });
  check('T29', 'no row while paused: retry 900 s, no tombstone', retried(r2, 900) && !job(w, 'ledgertest_job_ghost2'));
  const r3 = await deadLetter(w, { ...msg, jobId: 'ledgertest_job_ghost3' }, 6);
  check('T29', "a dead letter with no row, paused at its last delivery: the no_record alarm (with tokenCost), then ack", acked(r3)
    && alarmsOf(w, 'no_record').some((x) => x.job_id === 'ledgertest_job_ghost3' && JSON.parse(x.event_json).extra?.tokenCost === 11));
}
for (const [label, value] of [["'1'", '1'], ['absent', undefined], ["'x'", 'x']]) {
  // T25 through the consumer: every settling transition reads back paused, distinct from fence lost or a row
  const w = world();
  const s = await debited(w);
  const r4 = await debited(w);
  w.ledger.prepare("INSERT INTO legacy_idem VALUES ('token_idempotency:refund:' || ?, 'refund_job', NULL, 1, ?)").run(r4.jobId, toBind(T + DAY));
  const rec = await debited(w);
  const i = imported(w, { refund_due_code: 'provider_error' });
  plant(w, i.jobId, { png: true, index: true });
  w.rd.create = () => { setCtl(w, 'money_pause', value); return json({ base64_images: [PNG] }); };
  const rs = await deliver(w, s.msg);
  T += 41 * MIN;
  const rr4 = await deadLetter(w, r4.msg);
  const rdf = await deliver(w, i.msg);
  const rtb = await deliver(w, { ...s.msg, jobId: 'ledgertest_job_none' });
  T += 20 * MIN;
  await cron(w);
  check('T25', `money_pause ${label}: the success update (staged, retry), r4, the delivered finish, the tombstone and the recovery each answer paused; nothing moved`,
    retried(rs, 900) && job(w, s.jobId).artifact === 'staged' && retried(rr4, 900) && job(w, r4.jobId).finished_at_ms === null
    && retried(rdf, 900) && job(w, i.jobId).finished_at_ms === null && retried(rtb, 900) && !job(w, 'ledgertest_job_none')
    && job(w, rec.jobId).finished_at_ms === null && q(w.ledger, "SELECT 1 FROM ledger WHERE idem_key LIKE 'refund:%'").length === 0);
}

// ════════════════════════════════════════════════════════════════════════
// T35: the digest audit's writers (findings 4, 6 and 7)
// ════════════════════════════════════════════════════════════════════════
{
  const w = world();
  const rect = await debited(w, { width: 128, height: 64 });
  const anim = await debited(w, { mode: 'animate', width: 96, height: 96 });
  const bare = await debited(w, { sendWH: false });
  const imp = imported(w, { state: 'enqueued', claim_id: 'x', claimed_at_ms: T - DAY, lease_at_ms: T - DAY });
  T += 61 * MIN;
  await cron(w);
  const failed = (id) => { const e = eventsOf(w, id, 'generation.failed')[0]; return e ? { style: e.style, size: e.requested_size, ev: JSON.parse(e.event_json) } : null; };
  check('T35', "the sweep's generation.failed for a rectangular create: the debit's style and WxH, never a square", failed(rect.jobId)?.style === 'rd_fast__no_style' && failed(rect.jobId)?.size === '128x64');
  check('T35', 'for an animate create: its square from the debit', failed(anim.jobId)?.size === '96x96' && failed(anim.jobId)?.style === 'rd_advanced_animation__walking');
  check('T35', 'for a create sent without width or height: the size unknown (no column), never a square', failed(bare.jobId)?.size === null && failed(bare.jobId)?.ev.extra.requestedSize === 'unknown');
  check('T35', 'for an imported row with no debit: no style, size unknown', failed(imp.jobId) !== null && failed(imp.jobId).style === null && failed(imp.jobId).size === null);
  check('T35', "a sweep row's age apart from its latency (ageMs, no latency)", failed(rect.jobId)?.ev.extra.ageMs >= 60 * MIN && eventsOf(w, rect.jobId, 'generation.failed')[0].latency_ms === null);
  // the dead-letter handler: the debit's, else the message's
  const w2 = world();
  const d = await debited(w2, { width: 32, height: 48 });
  T += 41 * MIN;
  await deadLetter(w2, d.msg);
  const e = eventsOf(w2, d.jobId, 'generation.failed')[0];
  check('T35', "the dead-letter handler's generation.failed: the debit's style and WxH", e?.style === 'rd_fast__no_style' && e?.requested_size === '32x48');
  const i2 = imported(w2);
  await deadLetter(w2, { ...i2.msg, body: { prompt: 'p', prompt_style: 'rd_pro__x', width: 40, height: 20 } });
  const e2 = eventsOf(w2, i2.jobId, 'generation.failed')[0];
  check('T35', "for an imported row (no debit): the message's style and size", e2?.style === 'rd_pro__x' && e2?.requested_size === '40x20');
  // a resumed job's success: the finishing attempt's poll start beside the first start
  const w3 = world();
  const a = await debited(w3, { mode: 'animate', width: 64, height: 64 });
  const ctx = ctxOf(w3);
  await L.claimJob(ctx, 'submit', { job: a.jobId, claim: 'o', attempt: 1 });
  await L.ownerUpdate(ctx, 'submitted', { job: a.jobId, claim: 'o' });
  await L.ownerUpdate(ctx, 'task', { job: a.jobId, claim: 'o', task: 'task_r' });
  T += 6 * MIN;
  const resumeAt = T;
  await deliver(w3, a.msg, 2);
  const sx = JSON.parse(eventsOf(w3, a.jobId, 'generation.succeeded')[0].event_json);
  check('T35', "a resumed job's success carries the finishing attempt's poll start beside the first start", sx.extra.finishingPollStartedAt >= resumeAt && typeof sx.extra.firstStartedAt === 'number');
  // a staged result finalized later records the finishing poll's own times, not its own later start
  const w4 = world();
  const st = await debited(w4);
  let pollEnd = null;
  w4.rd.create = () => { pollEnd = T; setCtl(w4, 'money_pause', '1'); return json({ base64_images: [PNG] }); };
  await deliver(w4, st.msg);
  setCtl(w4, 'money_pause', '0');
  T += HOUR;
  await cron(w4);
  const fx = JSON.parse(eventsOf(w4, st.jobId, 'generation.succeeded')[0].event_json);
  check('T35', "a staged result finalized later: the finishing poll's own times, not the finalizer's later start", fx.extra.finishingPollEndedAt !== null && fx.extra.finishingPollEndedAt <= pollEnd + 5 && fx.extra.finishingPollStartedAt < T - 50 * MIN);
  const render = readFileSync(path.join(ROOT, 'src/digest/render.ts'), 'utf8');
  check('T35', "the digest's money sentence names spritebrew-ledger", render.includes('Money truth is the D1 ledger, spritebrew-ledger') && !render.includes('KV token ledger'));
}

// ════════════════════════════════════════════════════════════════════════
// T27 and 4.10: nothing release 2 writes carries release 1's payable shapes
// ════════════════════════════════════════════════════════════════════════
{
  const all = [];
  const w = world();
  const a = await debited(w);
  w.rd.create = () => new Response('bad', { status: 400 });
  await deliver(w, a.msg);
  const b = await debited(w);
  w.rd.create = () => json({ base64_images: [PNG] });
  await deliver(w, b.msg);
  await deliver(w, { ...a.msg, jobId: 'ledgertest_job_zz' });
  for (const [k, e] of w.kv.m) if (k.startsWith('job:')) all.push(e.value);
  for (const [k, e] of w.r2.m) if (k.startsWith('jobs/')) all.push(e.text);
  check('4.10', 'no record release 2 wrote carries refundOwed, refundDue, or a running tokenCost', all.length >= 6 && all.every((t) => !t.includes('refundOwed') && !t.includes('refundDue') && !(t.includes('"running"') && t.includes('tokenCost'))));
  check('4.10', 'every terminal error record release 2 wrote before a tombstone says refunded: true', all.map((t) => JSON.parse(t)).filter((s) => s.status === 'error' && s.errorCode !== 'no_record').every((s) => s.refunded === true));
}

// ════════════════════════════════════════════════════════════════════════
// Further rows: each closes a guard the first mutation pass let through
// ════════════════════════════════════════════════════════════════════════
{
  // T11: held precedes the pause gate (5.2's order)
  const w = world();
  const i = imported(w, { hold_reason: 'contradictory', token_cost: null });
  setCtl(w, 'money_pause', '1');
  const r = await deliver(w, i.msg);
  check('T11', 'a held row while paused: acked (held precedes the gate), nothing run', acked(r) && rdCalls().length === 0 && job(w, i.jobId).claim_id === null);
}
{
  // T11: the pause lands between the gate and c-submit
  const w = world();
  const { jobId, msg } = await debited(w);
  let armed = false;
  w.hooks.before = (sqls) => {
    if (armed) { w.hooks.before = undefined; setCtl(w, 'money_pause', '1'); return; }
    if (sqls.some((s) => s.includes("key = 'money_pause'") && !s.includes('EXISTS'))) armed = true;
  };
  const r = await deliver(w, msg);
  check('T11', 'the pause lands between the gate and c-submit: retry 900 s (never the live-owner ack), no claim, no RD call', retried(r, 900) && job(w, jobId).claim_id === null && billable().length === 0);
}
{
  // T8: a live claim on an animate row answers before the pre-flight
  const w = world();
  const { jobId, msg } = await debited(w, { mode: 'animate', width: 64, height: 64 });
  await L.claimJob(ctxOf(w), 'submit', { job: jobId, claim: 'o', attempt: 1 });
  const r = await deliver(w, msg, 2);
  check('T8', 'a live claim (not yet submitted) on an animate row: retry 30 s, no RD call of any kind (the status GET included)', retried(r, 30) && W.rd.calls.length === 0);
}
{
  // T8: the poll budget on a non-final attempt: the keep-task release, then the resume wins at once
  const w = world();
  const { jobId, msg } = await debited(w, { mode: 'animate', width: 64, height: 64 });
  w.rd.poll = () => json({ status: 'processing' });
  let r = await deliver(w, msg, 1);
  const j = job(w, jobId);
  check('T8', 'the poll budget on attempt 1: the keep-task release (released, the task kept), retry 0, no refund', retried(r, 0) && j.released_at_ms !== null && j.task_id === 'task_1' && refunds(w, jobId).length === 0);
  w.rd.poll = () => json({ status: 'succeeded', result: { base64_images: [PNG] } });
  r = await deliver(w, msg, 2);
  check('T8', 'the redelivery resumes the same task at once: succeeded, one submit in all', acked(r) && job(w, jobId).outcome === 'succeeded' && W.rd.calls.filter((c) => c.startsWith('submit')).length === 1);
}
{
  // T19: a result arriving after its row finished
  const w = world();
  const { jobId, msg } = await debited(w);
  w.rd.create = () => { setRow(w, jobId, { state: 'finished', outcome: 'refunded', refunded_amount: 10, finished_at_ms: T }); return json({ base64_images: [PNG] }); };
  const r = await deliver(w, msg);
  check('T19', 'a result arriving after its row finished: generation.orphaned, acked, nothing staged or published', acked(r) && eventsOf(w, jobId, 'generation.orphaned').length === 1 && !w.r2.m.has(`${USER}/${jobId}.png`) && job(w, jobId).artifact === 'none');
}
{
  // T19: a step after the stage throws: retried, never the owner's refund
  const w = world();
  const { jobId, msg } = await debited(w, { cost: 6 });
  w.rd.create = () => json({ base64_images: ['@@not base64@@'] });
  let r = await deliver(w, msg);
  check('T19', 'a step after the stage throws (undecodable result bytes): retried, never the owner refund, the row staged', retried(r, null) && refunds(w, jobId).length === 0 && job(w, jobId).artifact === 'staged');
  T += 6 * MIN;
  r = await deliver(w, msg, 2);
  check('T19', "the next delivery's finalizer finds no PNG: one refund, 'result_store_failed', no second RD call", acked(r) && refunds(w, jobId).length === 1 && job(w, jobId).error_code === 'result_store_failed' && billable().length === 1);
}
{
  // T19: a finalizer whose head fails
  const w = world();
  const { jobId, msg } = await debited(w);
  w.rd.create = () => { setCtl(w, 'money_pause', '1'); return json({ base64_images: [PNG] }); };
  await deliver(w, msg);
  setCtl(w, 'money_pause', '0');
  T += 6 * MIN;
  w.r2.failHead = (k) => k.endsWith('.png');
  let r = await deliver(w, msg, 2);
  check('T19', 'a finalizer whose head fails: retried, never read as absent, no refund', retried(r, null) && refunds(w, jobId).length === 0 && job(w, jobId).artifact === 'staged' && job(w, jobId).finished_at_ms === null);
  w.r2.failHead = null;
  T += 6 * MIN;
  r = await deliver(w, msg, 3);
  check('T19', 'the head answers next time: finalized, succeeded', acked(r) && job(w, jobId).outcome === 'succeeded');
}
{
  // T19: the PNG's bounded retry
  const w = world();
  const { jobId, msg } = await debited(w);
  let fails = 0;
  w.r2.failPut = (k) => k.endsWith('.png') && ++fails <= 2;
  const r = await deliver(w, msg);
  check('T19', 'two failed PNG puts, the third lands: succeeded, no refund', acked(r) && fails >= 3 && job(w, jobId).outcome === 'succeeded' && refunds(w, jobId).length === 0);
}
{
  // T14: the repair pass publishes a finished staged success
  const w = world();
  const { jobId, msg } = await debited(w);
  w.kv.failPut = (k) => k.startsWith('gen:');
  const r = await deliver(w, msg);
  const j = job(w, jobId);
  check('T14', 'a publish that fails after the success update: acked, succeeded, the artifact still staged', acked(r) && j.outcome === 'succeeded' && j.artifact === 'staged');
  w.kv.failPut = null;
  await cron(w);
  check('T14', 'the repair pass publishes it: the gen: index written, published', job(w, jobId).artifact === 'published' && [...w.kv.m.keys()].some((k) => k.startsWith(`gen:${USER}:`) && k.endsWith(`:${jobId}`)));
}
{
  // T14: the repair pass discards a refunded staged row
  const w = world();
  const { jobId, msg } = await debited(w, { cost: 5 });
  w.r2.failPut = (k) => k.endsWith('.png');
  await deliver(w, msg);
  w.r2.failPut = null;
  const before = job(w, jobId).artifact;
  w.r2.m.set(`${USER}/${jobId}.png`, { bytes: Buffer.from(PNG, 'base64'), text: '' }); // a put that landed late
  await cron(w);
  check('T14', 'a refunded row left staged: the repair pass deletes the late PNG and marks it discarded', before === 'staged' && job(w, jobId).outcome === 'refunded' && job(w, jobId).artifact === 'discarded' && !w.r2.m.has(`${USER}/${jobId}.png`));
}
{
  // T14: the repair pass waits for the migrator's phase
  const w = world();
  const { jobId, msg } = await debited(w);
  w.rd.create = () => new Response('bad', { status: 400 });
  w.r2.failPut = (k) => k === `jobs/${jobId}.json`;
  await deliver(w, msg);
  w.r2.failPut = null;
  setCtl(w, 'migration_open', '1');
  await cron(w);
  check('T14', "the repair pass waits while the migrator's phase is open: the marker stays NULL", job(w, jobId).status_written_at_ms === null && status(w, jobId)?.status !== 'error');
  setCtl(w, 'migration_open', '0');
  await cron(w);
  check('T14', "once the phase reads '0': written, the marker set", status(w, jobId)?.refunded === true && job(w, jobId).status_written_at_ms !== null);
}
{
  // T18: candidate (1)
  const w = world();
  const { jobId } = await debited(w, { cost: 4, enqueue: false });
  T += 11 * MIN;
  await cron(w);
  check('T18', "debited, never enqueued, past 10 minutes: candidate (1), the recovery fence, refunded once, 'debited_never_enqueued'", refunds(w, jobId).length === 1 && job(w, jobId).error_code === 'debited_never_enqueued');
}
{
  // T17: PNG only, but the row finished succeeded just before the refund
  const w = world();
  const i = imported(w, { claim_id: 'x', claimed_at_ms: T - DAY, lease_at_ms: T - DAY });
  plant(w, i.jobId, { png: true });
  w.hooks.before = (sqls) => { if (sqls.some((s) => s.includes("'generation_failed_refund', 'generation_failed_refund'"))) { w.hooks.before = undefined; setRow(w, i.jobId, { state: 'finished', outcome: 'succeeded', finished_at_ms: T, artifact: 'published' }); } };
  await cron(w);
  check('T17', 'PNG only, but the row finished succeeded before the refund: already finished, the PNG kept, not discarded', refunds(w, i.jobId).length === 0 && w.r2.m.has(`${KV_USER}/${i.jobId}.png`) && job(w, i.jobId).artifact === 'published');
  // another job's gen: entry is not this job's
  const w2 = world();
  const i2 = imported(w2, { refund_due_code: 'provider_error' });
  plant(w2, i2.jobId, { png: true });
  w2.kv.m.set(`gen:${KV_USER}:100:ledgertest_other_job`, { value: '', metadata: { jobId: 'ledgertest_other_job', prompt: 'p', style: 'rd_fast__no_style', mode: 'create', createdAt: 1, v: 1 } });
  await cron(w2);
  check('T17', "another job's gen: entry for the same user is not this job's: PNG only, refunded once, discarded", job(w2, i2.jobId).outcome === 'refunded' && refunds(w2, i2.jobId).length === 1 && job(w2, i2.jobId).artifact === 'discarded');
  // no balance row
  const w3 = world();
  const NB = 'user_NOBALANCENOBAL01';
  const i3 = imported(w3, { user_id: NB, refund_due_code: 'provider_error' });
  await cron(w3);
  check('T17', 'a refund for a user with no balance row: opened at 0 with zero_alarm, then refunded once', job(w3, i3.jobId).outcome === 'refunded' && refunds(w3, i3.jobId).length === 1 && balanceOf(w3, NB) === 8 && alarmsOf(w3, 'zero_alarm').length === 1);
}
{
  // T24: the dead-letter guards
  const w = world();
  const { jobId, msg } = await debited(w);
  T += 41 * MIN;
  let r = await deadLetter(w, { ...msg, enqueuedAt: T - 7 * DAY });
  check('T24', 'a dead letter older than six days: the stale_message alarm row and ack, never a refund', acked(r) && refunds(w, jobId).length === 0
    && q(w.events, "SELECT 1 FROM events WHERE event_name = 'generation.unrefunded' AND job_id = ? AND json_extract(event_json, '$.extra.reason') = 'stale_message'", jobId).length === 1);
  const w2 = world();
  const y = await debited(w2);
  T += 30 * MIN;
  r = await deadLetter(w2, y.msg, 1);
  check('T24', 'a dead letter for a row 30 minutes old (no claim): retry with the remaining grace (about 600 s), no refund', retried(r) && r.retry > 500 && r.retry <= 600 && refunds(w2, y.jobId).length === 0);
  r = await deadLetter(w2, y.msg, 6);
  check('T24', 'the same at the last delivery: acked with a log, no refund; the row stays the debt', acked(r) && refunds(w2, y.jobId).length === 0 && job(w2, y.jobId).finished_at_ms === null);
  // a d1 row with no debit: corruption, its alarm, nothing moved
  const w3 = world();
  const id = 'ledgertest_job_nodebit';
  w3.ledger.prepare("INSERT INTO jobs (job_id, user_id, mode, token_cost, client_key, request_hash, provenance, state, created_at_ms, enqueued_at_ms) VALUES (?, ?, 'create', 5, 'k', 'h', 'd1', 'enqueued', ?, ?)").run(id, USER, toBind(T), toBind(T));
  T += 41 * MIN;
  r = await deadLetter(w3, { jobId: id, userId: USER, idempotencyKey: 'k', tokenCost: 5, mode: 'create', body: { prompt: 'p', prompt_style: 'rd_fast__no_style', width: 64, height: 64 }, enqueuedAt: T - 41 * MIN });
  check('T24', 'a d1 row with no debit: corruption, the debit_missing alarm, nothing moved', acked(r) && refunds(w3, id).length === 0 && job(w3, id).finished_at_ms === null && alarmsOf(w3, 'debit_missing').length === 1);
  // a 900 s grace at the last delivery is not a pause
  const w4 = world();
  const s = await debited(w4);
  w4.rd.create = () => { setCtl(w4, 'money_pause', '1'); return json({ base64_images: [PNG] }); };
  await deliver(w4, s.msg);
  setCtl(w4, 'money_pause', '0');
  setCtl(w4, 'migration_open', '1');
  setRow(w4, s.jobId, { lease_at_ms: T - 25 * MIN });
  r = await deadLetter(w4, s.msg, 6);
  check('T24', "a staged row at the last delivery, the phase open, a grace of exactly 900 s: retried, no 'paused' alarm (the pause is read again)", retried(r, 900)
    && q(w4.events, "SELECT 1 FROM events WHERE event_name = 'generation.unrefunded' AND job_id = ?", s.jobId).length === 0);
}
{
  // T35: generation.refunded only after a refund; the debit's context over the message's
  const w = world();
  const { jobId, msg } = await debited(w);
  w.ledger.prepare("INSERT INTO legacy_idem VALUES ('token_idempotency:refund:' || ?, 'refund_job', NULL, 1, ?)").run(jobId, toBind(T + DAY));
  T += 41 * MIN;
  const r = await deadLetter(w, msg);
  check('T35', 'a job refunded before the switch: generation.failed once, no generation.refunded, no new refund row, refunded true', acked(r) && job(w, jobId).outcome === 'refunded_legacy'
    && eventsOf(w, jobId, 'generation.failed').length === 1 && eventsOf(w, jobId, 'generation.refunded').length === 0 && refunds(w, jobId).length === 0 && status(w, jobId)?.refunded === true);
  const w2 = world();
  const d = await debited(w2, { width: 32, height: 48 });
  T += 41 * MIN;
  await deadLetter(w2, { ...d.msg, body: { prompt: 'p', prompt_style: 'rd_pro__x', width: 40, height: 20 } });
  const e = eventsOf(w2, d.jobId, 'generation.failed')[0];
  check('T35', "the dead-letter handler prefers the debit's style and size over a message that differs", e?.style === 'rd_fast__no_style' && e?.requested_size === '32x48');
}

Date.now = realNow;
const order =['base', 'T8', 'T8b', 'T11', 'T11c', 'T14', 'T15', 'T17', 'T18', 'T19', 'T20', 'T21', 'T24', 'T25', 'T28', 'T29', 'T32', 'T34', 'T35', '4.10'];
let total = 0;
for (const t of order) {
  const c = counts.get(t) ?? { pass: 0, fail: 0 };
  total += c.pass + c.fail;
  console.log(`[ledger-s3-test] ${t}: ${c.pass}/${c.pass + c.fail}`);
}
console.log(`[ledger-s3-test] ${failed === 0 ? 'PASS' : 'FAIL'}: ${total - failed}/${total}`);
process.exit(failed === 0 ? 0 : 1);
