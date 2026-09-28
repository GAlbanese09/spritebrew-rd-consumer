// scripts/money-pause-test.mjs
//
// Offline tests for release 1 of n1-ledger (005 section 4, 007 section 4,
// 008 rulings A and B), consumer side. Run from the repo root:
// `node scripts/money-pause-test.mjs`.
//
// src/index.ts is bundled with esbuild into dist/.money-pause-test/
// (gitignored) and driven through its queue and scheduled handlers with an
// in-memory KV and R2, a stub `control` table, a D1 events stub that answers
// the dead-letter handler's lookups, and a counting fetch. Output: case names
// and pass or fail only.

import { build } from 'esbuild';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const ROOT = process.cwd();
const OUT = path.join(ROOT, 'dist', '.money-pause-test');

await build({
  entryPoints: { index: path.join(ROOT, 'src/index.ts') },
  bundle: true, platform: 'neutral', format: 'esm', outdir: OUT, logLevel: 'error',
  outExtension: { '.js': '.mjs' },
});
const worker = (await import(pathToFileURL(path.join(OUT, 'index.mjs')).href)).default;

// ── Stubs ──

let calls, kv, r2, events, control, controlThrows, fetchStatus;
function reset() {
  calls = []; kv = new Map(); r2 = new Map(); events = [];
  control = { money_pause: '0' }; controlThrows = false; fetchStatus = 404;
}
const kvStub = {
  get: async (k) => { calls.push(`kv.get ${k}`); return kv.get(k) ?? null; },
  put: async (k, v) => { calls.push(`kv.put ${k}`); kv.set(k, v); },
  delete: async (k) => { kv.delete(k); },
  list: async ({ prefix, cursor } = {}) => {
    calls.push(`kv.list ${prefix}`);
    return { keys: [...kv.keys()].filter((k) => k.startsWith(prefix ?? '')).map((name) => ({ name })), list_complete: true };
  },
};
const r2Stub = {
  get: async (k) => { calls.push(`r2.get ${k}`); return r2.has(k) ? { text: async () => r2.get(k) } : null; },
  head: async (k) => { calls.push(`r2.head ${k}`); return r2.has(k) ? {} : null; },
  put: async (k, v) => { calls.push(`r2.put ${k}`); r2.set(k, v); return {}; },
};
const eventsStub = {
  prepare: (sql) => {
    const stmt = { args: [] };
    stmt.bind = (...a) => { stmt.args = a; return stmt; };
    stmt.run = async () => {
      const json = stmt.args.find((a) => typeof a === 'string' && a.startsWith('{') && a.includes('"eventName"'));
      if (json) events.push(JSON.parse(json));
      return { meta: { changes: 1 } };
    };
    stmt.first = async () => {
      if (/FROM events WHERE job_id = \?/.test(sql)) {
        const [jobId, ...names] = stmt.args;
        return events.some((e) => e.jobId === jobId && names.includes(e.eventName)) ? { 1: 1 } : null;
      }
      return null;
    };
    stmt.all = async () => ({ results: [] });
    return stmt;
  },
};
const ledgerStub = {
  prepare: (sql) => ({
    first: async () => {
      calls.push('ledger.first');
      if (controlThrows) throw new Error('stub: D1 unavailable');
      const key = /'(money_pause|dev_fault)'/.exec(sql)?.[1];
      return key && control[key] !== undefined ? { value: control[key] } : null;
    },
  }),
};
const makeEnv = ({ ledger = true } = {}) => ({
  SPRITEBREW_KV: kvStub, GALLERY_BUCKET: r2Stub, EVENTS_DB: eventsStub,
  ...(ledger ? { LEDGER_DB: ledgerStub } : {}),
  APP_ENV: 'dev', RETRO_DIFFUSION_API_KEY: 'placeholder',
});
globalThis.fetch = async (url) => {
  calls.push(`fetch ${String(url).replace(/^https?:\/\/[^/]+/, '')}`);
  return new Response(JSON.stringify({ detail: 'stub' }), { status: fetchStatus });
};
const logs = [];
for (const lvl of ['log', 'info', 'warn', 'error']) console[lvl] = (...a) => logs.push(a.map(String).join(' '));
const out = (...a) => process.stdout.write(`${a.join(' ')}\n`);

// ── Messages ──

const USER = 'user_MONEYPAUSETESTCONSUMER1';
const COST = 5;
function message(jobId, attempts = 1, mode = 'create') {
  const m = {
    id: `msg-${jobId}-${attempts}`, timestamp: new Date(), attempts, outcome: null,
    body: {
      jobId, userId: USER, idempotencyKey: 'idem-12345678', tokenCost: COST, mode, enqueuedAt: Date.now() - 1000,
      body: { prompt: 'a knight', prompt_style: 'rd_fast__default', width: 64, height: 64 },
    },
    ack() { m.outcome = { ack: true }; },
    retry(opts) { m.outcome = { retry: true, delaySeconds: opts?.delaySeconds }; },
  };
  return m;
}
const deliver = async (m, queue = 'spritebrew-rd-jobs-dev', env = makeEnv()) =>
  worker.queue({ queue, messages: [m] }, env, {});
const dlq = (m, env) => deliver(m, 'spritebrew-rd-jobs-dlq-dev', env);
const seed = (u, n) => kv.set(`token_balance:${u}`, JSON.stringify({ balance: n, created_at: 'x', last_updated: 'x' }));
const balance = () => JSON.parse(kv.get(`token_balance:${USER}`)).balance;
const record = (jobId) => { const r = kv.get(`job:${jobId}`); return r ? JSON.parse(r) : null; };
const jobWrites = () => calls.filter((c) => /^(kv|r2)\.put (job:|jobs\/)/.test(c));
const moneyWrites = () => calls.filter((c) => /^kv\.put token_/.test(c));
const rdCalls = () => calls.filter((c) => c.startsWith('fetch '));
const running = (over = {}) => JSON.stringify({ status: 'running', userId: USER, mode: 'create', enqueuedAt: Date.now() - 60_000,
  startedAt: Date.now() - 60_000, attempt: 1, tokenCost: COST, ...over });

let pass = 0, fail = 0;
const check = (name, ok) => { if (ok) pass++; else { fail++; out('FAIL', name); } };

// ── The gate before 0d (rulings A and B) ──

reset(); control.money_pause = '1';
let m = message('job_gate_fresh');
await deliver(m);
check('gate: fresh delivery while paused -> retry 900 s', m.outcome?.retry && m.outcome.delaySeconds === 900);
check('gate: fresh delivery while paused -> no job record, no money, no RD call',
  jobWrites().length === 0 && moneyWrites().length === 0 && rdCalls().length === 0);

reset(); control.money_pause = '1'; kv.set('job:job_gate_pending', JSON.stringify({ status: 'pending', userId: USER, mode: 'create', enqueuedAt: 1 }));
m = message('job_gate_pending', 4);
await deliver(m);
check('gate: last main-queue delivery while paused -> retry (to the dead-letter queue), nothing written',
  m.outcome?.retry && jobWrites().length === 0 && rdCalls().length === 0);

reset(); control.money_pause = '1'; control.dev_fault = 'paused_retry_60';
m = message('job_gate_dev60');
await deliver(m);
check("gate: dev_fault 'paused_retry_60' on dev -> retry 60 s", m.outcome?.retry && m.outcome.delaySeconds === 60);

reset(); control.money_pause = '1'; control.dev_fault = 'paused_retry_60';
m = message('job_gate_prod60');
await deliver(m, 'spritebrew-rd-jobs', { ...makeEnv(), APP_ENV: 'production' });
check('gate: the same dev_fault in production -> still 900 s', m.outcome?.retry && m.outcome.delaySeconds === 900);

reset(); controlThrows = true;
m = message('job_gate_readfail');
await deliver(m);
check('gate: pause read failing (T11c) -> held, nothing written, no RD call',
  m.outcome?.retry && m.outcome.delaySeconds === 900 && jobWrites().length === 0 && rdCalls().length === 0);
check('gate: pause read failing -> pause_read_failed logged', logs.some((l) => l.includes('pause_read_failed')));

reset();
m = message('job_gate_nobinding');
await deliver(m, 'spritebrew-rd-jobs-dev', makeEnv({ ledger: false }));
check('gate: LEDGER_DB unbound -> held, no RD call', m.outcome?.retry && rdCalls().length === 0 && jobWrites().length === 0);

// Terminal records still ack while paused (0a/0b come first).
reset(); control.money_pause = '1'; kv.set('job:job_term', JSON.stringify({ status: 'error', userId: USER, mode: 'create', enqueuedAt: 1, failedAt: 1, error: 'x', errorCode: 'x', attempts: 1, refunded: true }));
m = message('job_term', 2);
await deliver(m);
check('gate: a terminal record while paused -> acked as before', m.outcome?.ack === true);

// Open: the gate lets 0d run (an orphaned submit refunds).
reset(); seed(USER, 100); kv.set('job:job_open_0d', running({ mode: 'animate', submitAttemptedAt: Date.now() - 60_000 }));
m = message('job_open_0d', 2, 'animate');
await deliver(m);
check('open: 0d still refunds an orphaned submit', m.outcome?.ack && balance() === 100 + COST && record('job_open_0d')?.errorCode === 'rd_submit_orphaned_redelivery');

// ── 0c under the pause: a resume poll may run; its failure owes a refund ──

reset(); control.money_pause = '1'; seed(USER, 100);
kv.set('job:job_0c', running({ mode: 'animate', taskId: 'task_abc', submitAttemptedAt: Date.now() - 60_000 }));
m = message('job_0c', 2, 'animate');
await deliver(m);
const rec0c = record('job_0c');
check('0c while paused: the resume poll runs (bills nothing)', rdCalls().some((c) => c.includes('task_abc')));
check('0c while paused, poll fails terminally -> no refund, retry 900 s', m.outcome?.retry && m.outcome.delaySeconds === 900 && balance() === 100);
check('recordFailure while paused -> running record with refundDue', rec0c?.status === 'running' && rec0c.refundDue?.errorCode && typeof rec0c.refundDue.at === 'number' && rec0c.tokenCost === COST);

// ── 0f ──

reset(); control.money_pause = '1'; seed(USER, 100);
kv.set('job:job_0f', running({ refundDue: { errorCode: 'rd_http_500', at: Date.now(), error: 'RD returned 500' } }));
m = message('job_0f', 2);
await deliver(m);
check('0f while paused -> retry 900 s, nothing written, no RD call',
  m.outcome?.retry && m.outcome.delaySeconds === 900 && jobWrites().length === 0 && moneyWrites().length === 0 && rdCalls().length === 0);

control.money_pause = '0'; calls = [];
m = message('job_0f', 3);
await deliver(m);
const rec0f = record('job_0f');
check('0f after the unpause -> refunded once, acked, no RD call', m.outcome?.ack && balance() === 100 + COST && rdCalls().length === 0);
check('0f after the unpause -> error record keeps the owed code and message, refunded true',
  rec0f?.status === 'error' && rec0f.errorCode === 'rd_http_500' && rec0f.error === 'RD returned 500' && rec0f.refunded === true);
m = message('job_0f', 4);
await deliver(m);
check('0f: a later delivery -> acked, no second refund', m.outcome?.ack && balance() === 100 + COST);

// ── The sweep ──

reset(); control.money_pause = '1'; seed(USER, 100);
kv.set('job:job_sweep', running({ startedAt: Date.now() - 30 * 60_000, refundDue: { errorCode: 'rd_http_500', at: Date.now() } }));
await worker.scheduled({}, makeEnv(), {});
check('sweep while paused -> skipped (no job listing, no refund)', !calls.includes('kv.list job:') && balance() === 100);
control.money_pause = '0'; calls = [];
await worker.scheduled({}, makeEnv(), {});
check('sweep after the unpause -> refunds the owed job once, with its own code',
  balance() === 100 + COST && record('job_sweep')?.errorCode === 'rd_http_500');

// ── The dead-letter handler (ruling A) ──

reset(); control.money_pause = '1'; seed(USER, 100);
kv.set('job:job_dlq', JSON.stringify({ status: 'pending', userId: USER, mode: 'create', enqueuedAt: Date.now() - 60_000 }));
m = message('job_dlq', 1);
await dlq(m);
check('dead letter while paused -> retry 900 s, no refund', m.outcome?.retry && m.outcome.delaySeconds === 900 && balance() === 100 && moneyWrites().length === 0);

m = message('job_dlq', 6);
await dlq(m);
const alarm = events.find((e) => e.eventName === 'generation.unrefunded');
check("dead letter paused through its last delivery -> alarm reason 'paused', acked, no refund",
  m.outcome?.ack && alarm?.extra?.reason === 'paused' && balance() === 100);
check('the alarm carries the evidence: KV refund key absent, no D1 refunded row, token cost',
  alarm?.extra?.legacyRefundEvidence === 'kv:absent' && alarm?.extra?.d1RefundedEvent === false && alarm?.extra?.tokenCost === COST);

// A's test, offline: held past the main queue, dead-lettered, held, then one refund after the unpause.
reset(); control.money_pause = '1'; seed(USER, 100);
kv.set('job:job_A', JSON.stringify({ status: 'pending', userId: USER, mode: 'create', enqueuedAt: Date.now() - 60_000 }));
const outcomes = [];
for (let a = 1; a <= 4; a++) { m = message('job_A', a); await deliver(m); outcomes.push(m.outcome); }
for (let a = 1; a <= 3; a++) { m = message('job_A', a); await dlq(m); outcomes.push(m.outcome); }
check("A: four main deliveries and three dead-letter deliveries while paused -> all retried, no refund, no RD call",
  outcomes.every((o) => o?.retry) && balance() === 100 && rdCalls().length === 0);
control.money_pause = '0';
m = message('job_A', 4); await dlq(m);
const recA = record('job_A');
const r2A = r2.get('jobs/job_A.json') ? JSON.parse(r2.get('jobs/job_A.json')) : null;
check('A: the first dead-letter delivery after the unpause -> refunded once, acked', m.outcome?.ack && balance() === 100 + COST);
check('A: the status record says refunded (KV and R2)', recA?.status === 'error' && recA.refunded === true && r2A?.refunded === true);
m = message('job_A', 5); await dlq(m);
check('A: another delivery -> acked, no second refund', m.outcome?.ack && balance() === 100 + COST);
check('A: no generation.unrefunded alarm', !events.some((e) => e.eventName === 'generation.unrefunded'));

out(`${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
