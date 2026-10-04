// spritebrew-rd-consumer/src/migrator.ts
//
// Release 2's migrator (n1-release-2-spec.md revision 9: 6.1, 4.17, 7.2,
// section 8; section 11's S2 row). Built in S2; inert until the switch: in
// phase '0' every path answers 404 except POST /admin/ledger/scan?verify=1,
// which reads and writes nothing (the backstop runs).
//
// The gate, in order, every failure a 404 with no body:
//   1. the token: `Authorization: Bearer <MIGRATE_TOKEN>`, compared in
//      constant time first (SHA-256 of both, then a full XOR), and refused
//      outright when the secret is unset or shorter than 32 characters;
//   2. the method and path: POST to exactly /admin/ledger/scan or
//      /admin/ledger/snapshot, with only the query keys each takes;
//   3. the phase, read from `control` (a failed read refuses):
//        scan                  'scan'
//        scan?classify=1       'scan' (step 4's dry run)
//        scan?handoff=1        'scan', and money paused
//        scan?verify=1         'scan' (step 8) or '0' (the backstop runs)
//        snapshot              'snapshot', and money paused
//
// Writes: the scan's kept content to R2 under migrator/scan/{pause_epoch_ms}/
// (only while money is paused, so never at step 1b and never in a verify
// scan); the handoff's `switch_obligations` rows (paused, in 'scan'); and
// 4.17's import batches (paused, in 'snapshot'). Every import and handoff
// statement is the spec's text, guards included; the gate above is a second
// fence, not the only one.
//
// Every endpoint works in chunks: a call answers one chunk and a `cursor` for
// the next, until `done`. Counts that span chunks ride in the cursor; lists
// (holds, drift) are answered by the chunk that found them.

import type { Env } from './types';

// ── The spec's statements ──

/** 4.17 The snapshot's import batches, spec line 1454. */
const PAUSE_START_CHECK = `SELECT (SELECT value_ms FROM switch_marks WHERE name = 'pause_start_ms')               AS cutoff_ms,
       (SELECT updated_at_ms FROM control WHERE key = 'money_pause' AND value = '1')  AS pause_epoch_ms;`;

/** 4.17 The snapshot's import batches, spec line 1465. */
const IMPORT_BALANCE_LEDGER = `INSERT INTO ledger (id, user_id, type, amount, reason, source, balance_after, idem_key, created_at_ms, meta_json)
SELECT :id, :uid, 'opening', :amount, 'migrated_from_kv', 'snapshot', :amount, 'open:' || :uid, :now, :meta
 WHERE NOT EXISTS (SELECT 1 FROM balances WHERE user_id = :uid)
   AND EXISTS (SELECT 1 FROM control WHERE key = 'money_pause' AND value = '1')
   AND EXISTS (SELECT 1 FROM control WHERE key = 'migration_open' AND value = 'snapshot');`;

/** 4.17 The snapshot's import batches, spec line 1470. */
const IMPORT_BALANCE_ROW = `INSERT INTO balances (user_id, balance, created_at_ms, updated_at_ms, opened_via, kv_last_updated)
SELECT :uid, :amount, :now, :now, 'snapshot', :kv_last
 WHERE EXISTS (SELECT 1 FROM ledger WHERE id = :id);`;

/** 4.17 The snapshot's import batches, spec line 1478. */
const IMPORT_BALANCES_READ = `SELECT u.value AS user_id, b.balance, l.amount AS opened_amount, l.source AS opened_source
  FROM json_each(:uids) AS u
  LEFT JOIN balances AS b ON b.user_id = u.value
  LEFT JOIN ledger AS l ON l.idem_key = 'open:' || u.value;`;

/** 4.17 The snapshot's import batches, spec line 1489. */
const IMPORT_LEGACY = `INSERT INTO legacy_idem (key, kind, kv_expires_ms, copied_at_ms, keep_until_ms)
SELECT :key, :kind, :kv_exp, :now, :now + 3888000000
 WHERE EXISTS (SELECT 1 FROM control WHERE key = 'money_pause' AND value = '1')
   AND EXISTS (SELECT 1 FROM control WHERE key = 'migration_open' AND value = 'snapshot')
ON CONFLICT (key) DO NOTHING;`;

/** 4.17 The snapshot's import batches, spec line 1494. */
const IMPORT_LEGACY_READ = `SELECT k.value AS key, l.copied_at_ms
  FROM json_each(:keys) AS k LEFT JOIN legacy_idem AS l ON l.key = k.value;`;

/** 4.17 The snapshot's import batches, spec line 1505. */
const IMPORT_CARRIED_EVENTS = `INSERT INTO stripe_pending (event_id, reason, first_seen_ms)
SELECT o.subject_id, 'no_evidence', :now
  FROM switch_obligations AS o
 WHERE o.subject_kind = 'event' AND o.disposition = 'carry' AND o.settled_at_ms IS NULL
   AND NOT EXISTS (SELECT 1 FROM legacy_idem
                    WHERE key IN ('token_idempotency:' || o.subject_id, 'webhook:stripe:' || o.subject_id)
                      AND keep_until_ms > :now)
   AND EXISTS (SELECT 1 FROM control WHERE key = 'money_pause' AND value = '1')
   AND EXISTS (SELECT 1 FROM control WHERE key = 'migration_open' AND value = 'snapshot')
ON CONFLICT (event_id) DO NOTHING;`;

/** 4.17 The snapshot's import batches, spec line 1515. */
const IMPORT_CARRIED_EVENTS_READ = `SELECT o.subject_id, p.reason,
       EXISTS (SELECT 1 FROM legacy_idem
                WHERE key IN ('token_idempotency:' || o.subject_id, 'webhook:stripe:' || o.subject_id)
                  AND keep_until_ms > :now) AS legacy
  FROM switch_obligations AS o LEFT JOIN stripe_pending AS p ON p.event_id = o.subject_id
 WHERE o.subject_kind = 'event' AND o.disposition = 'carry' AND o.settled_at_ms IS NULL;`;

/** 4.17 The snapshot's import batches, spec line 1528. */
const IMPORT_JOB = `INSERT INTO jobs (job_id, user_id, mode, token_cost, provenance, state, refund_due_code, hold_reason, import_json,
                  outcome, error_code, error_message, artifact, status_written_at_ms,
                  created_at_ms, enqueued_at_ms, finished_at_ms)
SELECT :job, :uid, :mode, :cost, 'kv', :state, :due_code, :hold, :import_json,
       :outcome, :code, :msg, :artifact, :status_at, :created, :enqueued, :finished
 WHERE EXISTS (SELECT 1 FROM control WHERE key = 'money_pause' AND value = '1')
   AND EXISTS (SELECT 1 FROM control WHERE key = 'migration_open' AND value = 'snapshot')
ON CONFLICT (job_id) DO NOTHING;`;

/** 4.17 The snapshot's import batches, spec line 1536. */
const IMPORT_JOBS_READ = `SELECT u.value AS job_id, json_extract(j.import_json, '$.class') AS class,
       json_extract(j.import_json, '$.fingerprint') AS fingerprint
  FROM json_each(:jobs) AS u LEFT JOIN jobs AS j ON j.job_id = u.value;`;

/** Failure exits, spec line 2164. */
const HANDOFF_INSERT = `INSERT INTO switch_obligations (pause_epoch_ms, subject_kind, subject_id, user_id, mode, source, evidence_json,
                                token_cost, kv_payable, handed_off_at_ms)
SELECT :epoch, :kind, :subject, :uid, :mode, :source, :evidence, :cost, :kv_payable, :now
 WHERE EXISTS (SELECT 1 FROM control WHERE key = 'money_pause' AND value = '1')
   AND EXISTS (SELECT 1 FROM control WHERE key = 'migration_open' AND value = 'scan')
ON CONFLICT (pause_epoch_ms, subject_kind, subject_id) DO UPDATE
   SET user_id = excluded.user_id, mode = excluded.mode, source = excluded.source,
       evidence_json = excluded.evidence_json, token_cost = excluded.token_cost,
       kv_payable = excluded.kv_payable, handed_off_at_ms = excluded.handed_off_at_ms,
       disposition = NULL, decision_note = NULL, kv_neutralized_at_ms = NULL, found_unpaid_at_ms = NULL,
       decided_by = NULL, decided_at_ms = NULL
 WHERE switch_obligations.settled_at_ms IS NULL;`;

/** Failure exits, spec line 2177. */
const HANDOFF_READ = `SELECT json_extract(u.value, '$.kind') AS kind, json_extract(u.value, '$.id') AS subject_id,
       json_extract(o.evidence_json, '$.fingerprint') AS fingerprint, o.kv_payable, o.disposition
  FROM json_each(:subjects) AS u
  LEFT JOIN switch_obligations AS o
    ON o.pause_epoch_ms = :epoch AND o.subject_kind = json_extract(u.value, '$.kind')
   AND o.subject_id = json_extract(u.value, '$.id');`;

/** Every spec statement the migrator runs, by name (S2's tests compare each
 *  with the spec's text). */
export const MIGRATOR_SQL: Readonly<Record<string, string>> = {
  PAUSE_START_CHECK, IMPORT_BALANCE_LEDGER, IMPORT_BALANCE_ROW, IMPORT_BALANCES_READ,
  IMPORT_LEGACY, IMPORT_LEGACY_READ, IMPORT_CARRIED_EVENTS, IMPORT_CARRIED_EVENTS_READ,
  IMPORT_JOB, IMPORT_JOBS_READ, HANDOFF_INSERT, HANDOFF_READ,
};

// ── Constants ──

const KEPT_PREFIX = 'migrator/scan/';
const PAGE_JOBS = 50;
const PAGE_BALANCES = 25;
const PAGE_KEYS = 100;
const Q3_GAP_MS = 15 * 60 * 1000;
const RUNNING_NO_TASK_MS = 15 * 60 * 1000;
const DEBT_AGE_MS = 20 * 60 * 1000;
const DAY_MS = 86_400_000;
const USER_ID_OK = /^(user_|ledgertest_)/;

type Row = Record<string, unknown>;
type Copy = Record<string, unknown> & { status?: unknown };

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' } });
const notFound = (): Response => new Response(null, { status: 404 });

class MigratorStop extends Error {}

// ── The token gate ──

async function sha256(text: string): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text)));
}

/** Constant time: both sides are hashed to 32 bytes and every byte is
 *  compared, whatever the input; an unset or short secret never matches. */
export async function tokenMatches(header: string | null, secret: string | undefined): Promise<boolean> {
  const given = header !== null && header.startsWith('Bearer ') ? header.slice(7) : '';
  const usable = typeof secret === 'string' && secret.length >= 32;
  const a = await sha256(given);
  const b = await sha256(usable ? (secret as string) : '\u0000 no migrate token \u0000');
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
  return usable && diff === 0;
}

export type MigratorMode = 'scan' | 'classify' | 'handoff' | 'verify' | 'snapshot';

/** The exact paths and query keys; anything else is null (404). */
export function routeOf(request: Request): { mode: MigratorMode; cursor: string | null } | null {
  if (request.method !== 'POST') return null;
  let url: URL;
  try {
    url = new URL(request.url);
  } catch {
    return null;
  }
  const keys = [...url.searchParams.keys()];
  if (new Set(keys).size !== keys.length) return null;
  const cursor = url.searchParams.get('cursor');
  if (url.pathname === '/admin/ledger/snapshot') {
    return keys.every((k) => k === 'cursor') ? { mode: 'snapshot', cursor } : null;
  }
  if (url.pathname !== '/admin/ledger/scan') return null;
  const flags = keys.filter((k) => k !== 'cursor');
  if (flags.length > 1) return null;
  if (flags.length === 0) return { mode: 'scan', cursor };
  const flag = flags[0];
  if (url.searchParams.get(flag) !== '1') return null;
  if (flag === 'verify') return { mode: 'verify', cursor };
  if (flag === 'handoff') return { mode: 'handoff', cursor };
  if (flag === 'classify') return { mode: 'classify', cursor };
  return null;
}

interface ControlState {
  phase: string | null;
  pause: string | null;
  pauseEpoch: number | null;
}

/** Which phase admits which mode (section 8's phase contract). */
export function admitted(mode: MigratorMode, c: ControlState): boolean {
  switch (mode) {
    case 'scan':
    case 'classify':
      return c.phase === 'scan';
    case 'handoff':
      return c.phase === 'scan' && c.pause === '1';
    case 'verify':
      return c.phase === 'scan' || c.phase === '0';
    case 'snapshot':
      return c.phase === 'snapshot' && c.pause === '1';
  }
}

// ── D1 ──

type Params = Record<string, unknown>;

/** The spec writes :name parameters; D1 binds ?NNN. The same mapping as the
 *  library's (src/ledger.ts), kept here so the Worker's queue and scheduled
 *  paths never bundle the library before S3 (T27). */
function toPositional(sql: string): { sql: string; names: string[] } {
  const names: string[] = [];
  let out = '';
  let i = 0;
  while (i < sql.length) {
    const ch = sql[i];
    if (ch === "'") {
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === "'" && sql[j + 1] === "'") j += 2;
        else if (sql[j] === "'") break;
        else j++;
      }
      out += sql.slice(i, j + 1);
      i = j + 1;
    } else if (ch === '-' && sql[i + 1] === '-') {
      const j = sql.indexOf('\n', i);
      const end = j === -1 ? sql.length : j;
      out += sql.slice(i, end);
      i = end;
    } else if (ch === ':' && /[a-z_]/.test(sql[i + 1] ?? '')) {
      let j = i + 1;
      while (j < sql.length && /[a-z0-9_]/.test(sql[j])) j++;
      const name = sql.slice(i + 1, j);
      let n = names.indexOf(name);
      if (n === -1) n = names.push(name) - 1;
      out += `?${n + 1}`;
      i = j;
    } else {
      out += ch;
      i++;
    }
  }
  return { sql: out, names };
}

function bind(db: D1Database, sql: string, p: Params): D1PreparedStatement {
  const { sql: text, names } = toPositional(sql);
  return db.prepare(text).bind(...names.map((n) => {
    const v = p[n];
    if (v === undefined) throw new Error(`migrator: parameter :${n} not bound`);
    return v;
  }));
}

async function readControl(db: D1Database): Promise<ControlState> {
  const r = await db.prepare("SELECT key, value, updated_at_ms FROM control WHERE key IN ('migration_open', 'money_pause')").all<Row>();
  const by = Object.fromEntries((r.results ?? []).map((x) => [String(x.key), x]));
  return {
    phase: typeof by.migration_open?.value === 'string' ? (by.migration_open.value as string) : null,
    pause: typeof by.money_pause?.value === 'string' ? (by.money_pause.value as string) : null,
    pauseEpoch: typeof by.money_pause?.updated_at_ms === 'number' ? (by.money_pause.updated_at_ms as number) : null,
  };
}

// ── Canonical JSON and fingerprints ──

export function canonical(v: unknown): string {
  if (v === null || typeof v !== 'object') return JSON.stringify(v ?? null);
  if (Array.isArray(v)) return `[${v.map(canonical).join(',')}]`;
  const o = v as Record<string, unknown>;
  return `{${Object.keys(o).filter((k) => o[k] !== undefined).sort().map((k) => `${JSON.stringify(k)}:${canonical(o[k])}`).join(',')}}`;
}

export async function fingerprint(v: unknown): Promise<string> {
  return [...(await sha256(canonical(v)))].map((b) => b.toString(16).padStart(2, '0')).join('');
}

// ── Reading a job ──

export interface Keys {
  /** token_idempotency:refund:{jobId} present */
  consumer: boolean;
  /** token_idempotency:refund:{requestId} present, per requestId a copy names */
  pages: Record<string, boolean>;
}

export interface JobInputs {
  kv: Copy | null;
  r2: Copy | null;
  keys: Keys;
}

function parseCopy(raw: string | null, where: string): Copy | null {
  if (raw === null) return null;
  try {
    const v = JSON.parse(raw);
    if (v && typeof v === 'object') return v as Copy;
  } catch {
    // fall through
  }
  throw new MigratorStop(`${where}: not a JSON record`);
}

async function readCopies(env: Env, jobId: string): Promise<{ kv: Copy | null; r2: Copy | null }> {
  const [kvRaw, r2Obj] = await Promise.all([
    env.SPRITEBREW_KV.get(`job:${jobId}`),
    env.GALLERY_BUCKET.get(`jobs/${jobId}.json`),
  ]);
  return { kv: parseCopy(kvRaw, `job:${jobId}`), r2: parseCopy(r2Obj ? await r2Obj.text() : null, `jobs/${jobId}.json`) };
}

/** The requestIds a job's copies name (an owed copy's refundOwed, and a
 *  pending copy from S0 on, 7.2). */
export function requestIdsOf(copies: Array<Copy | null>): string[] {
  const ids = new Set<string>();
  for (const c of copies) {
    if (!c) continue;
    const owed = c.refundOwed as Row | undefined;
    if (owed && typeof owed.requestId === 'string') ids.add(owed.requestId);
    if (c.status === 'pending' && typeof c.requestId === 'string') ids.add(c.requestId);
  }
  return [...ids].sort();
}

async function readKeys(env: Env, jobId: string, copies: Array<Copy | null>): Promise<Keys> {
  const ids = requestIdsOf(copies);
  const [consumer, ...pages] = await Promise.all([
    env.SPRITEBREW_KV.get(`token_idempotency:refund:${jobId}`),
    ...ids.map((r) => env.SPRITEBREW_KV.get(`token_idempotency:refund:${r}`)),
  ]);
  return { consumer: consumer !== null, pages: Object.fromEntries(ids.map((r, i) => [r, pages[i] !== null])) };
}

// ── 7.2: evidence and classification ──

export interface CarriedRow {
  pause_epoch_ms: number;
  user_id: string | null;
  mode: string | null;
  token_cost: number | null;
  evidence_json: string;
  paid_evidence_json: string | null;
  decided_at_ms: number | null;
}

export interface ImportRow {
  job: string;
  uid: string;
  mode: string;
  cost: number | null;
  state: 'claimed' | 'enqueued' | 'finished';
  due_code: string | null;
  hold: string | null;
  outcome: string | null;
  code: string | null;
  msg: string | null;
  artifact: 'none' | 'published';
  status_at: number | null;
  created: number;
  enqueued: number | null;
  finished: number | null;
  import_json: string;
}

export type JobClass =
  | 'hold_running' | 'hold_contradictory' | 'refunded_by_evidence' | 'identity_succeeded' | 'hold_unexplained'
  | 'hold_no_cost' | 'owed_ambiguous' | 'refund_due_ambiguous' | 'pending' | 'hold_carried';

export interface Classified {
  /** null: the snapshot stops (a live running copy before the barrier, or a
   *  job no row of 7.2 describes). */
  cls: JobClass | null;
  stop?: string;
  fingerprint: string;
  row: ImportRow | null;
  evidence: string[];
}

const isError = (c: Copy) => c.status === 'error';
const owedOf = (c: Copy) => (c.refundOwed ?? null) as Row | null;
const num = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const str = (v: unknown): string | null => (typeof v === 'string' ? v : null);
const costOk = (v: number | null): number | null => (v !== null && Number.isInteger(v) && v >= 1 && v <= 50 ? v : null);

export function evidenceOf(kv: Copy | null, r2: Copy | null, keys: Keys) {
  const cs = [kv, r2].filter((c): c is Copy => c !== null);
  const refundedCopy = (c: Copy) => isError(c) && (c.refunded === true || c.refundSettled !== undefined);
  const owed = (c: Copy) => isError(c) && owedOf(c) !== null && c.refunded !== true;
  const plain = (c: Copy) => isError(c) && c.refunded === false && owedOf(c) === null;
  const e = {
    success: cs.some((c) => c.status === 'success'),
    refundedCopy: cs.some(refundedCopy),
    balanceWritten: cs.some((c) => owedOf(c)?.balanceWritten === true),
    consumerKey: keys.consumer,
    pagesKey: Object.values(keys.pages).some(Boolean),
    owed: cs.some(owed),
    refundDue: cs.some((c) => c.status === 'running' && c.refundDue !== undefined && c.refundDue !== null),
    pending: cs.some((c) => c.status === 'pending'),
    plain: cs.some(plain),
    running: cs.some((c) => c.status === 'running' && (c.refundDue === undefined || c.refundDue === null)),
    anyError: cs.some(isError),
    disagree: cs.some(refundedCopy) && cs.some((c) => owed(c) || plain(c)),
    cost: cs.map((c) => costOk(num(c.tokenCost)) ?? costOk(num(owedOf(c)?.tokenCost))).find((v) => v !== null) ?? null,
    namesRequestId: requestIdsOf(cs).length > 0,
  };
  return { ...e, refunded: e.refundedCopy || e.balanceWritten || e.consumerKey || e.pagesKey };
}

/** 7.2's classification, first match wins, with the carried job first. */
export async function classifyJob(
  jobId: string,
  input: JobInputs,
  opts: { now: number; barrierProven: boolean; carried?: CarriedRow[] },
): Promise<Classified> {
  const carried = (opts.carried ?? []).slice().sort((a, b) => a.pause_epoch_ms - b.pause_epoch_ms);
  // A neutralized copy (8) is read as absent for a carried job.
  const live = (c: Copy | null) => (carried.length && c && c.neutralized !== undefined ? null : c);
  const kv = live(input.kv);
  const r2 = live(input.r2);
  const fp = await fingerprint({ kv: input.kv, r2: input.r2, keys: input.keys, carried: carried.length ? carried : undefined });
  // Each copy's own fingerprint, so a backstop run can tell a changed record
  // from the one the snapshot read (8).
  const copyFingerprints = { kv: input.kv ? await fingerprint(input.kv) : null, r2: input.r2 ? await fingerprint(input.r2) : null };
  const e = evidenceOf(kv, r2, input.keys);
  const evidence = Object.entries(e).filter(([k, v]) => v === true && k !== 'namesRequestId').map(([k]) => k);
  const any = kv ?? r2;
  const uid = str(kv?.userId) ?? str(r2?.userId) ?? (carried.length ? carried[carried.length - 1].user_id : null);
  const mode = str(kv?.mode) ?? str(r2?.mode) ?? (carried.length ? carried[carried.length - 1].mode : null);
  const created = num(kv?.enqueuedAt) ?? num(r2?.enqueuedAt);
  const errCopy = [kv, r2].find((c): c is Copy => !!c && isError(c)) ?? null;
  const dueCopy = [kv, r2].find((c): c is Copy => !!c && c.status === 'running' && !!c.refundDue) ?? null;

  const base = (cls: JobClass, extra: Partial<ImportRow>, more: Row = {}): Classified => {
    const importJson = {
      class: cls,
      copies: { kv: str(input.kv?.status) ?? (input.kv ? 'unknown' : null), r2: str(input.r2?.status) ?? (input.r2 ? 'unknown' : null) },
      evidence,
      fingerprint: fp,
      copy_fingerprints: copyFingerprints,
      ...more,
    };
    return {
      cls, fingerprint: fp, evidence,
      row: {
        job: jobId, uid: uid as string, mode: mode as string, cost: null, state: 'claimed', due_code: null, hold: null,
        outcome: null, code: null, msg: null, artifact: 'none', status_at: null, created: created ?? opts.now,
        enqueued: null, finished: null, import_json: JSON.stringify(importJson), ...extra,
      },
    };
  };
  const stop = (why: string): Classified => ({ cls: null, stop: why, fingerprint: fp, row: null, evidence });

  const plainClass = (): Classified | null => {
    if (!any) return null;
    if (e.running && !e.success && !e.anyError && !e.refundDue && !e.refunded) {
      if (!opts.barrierProven) return stop('a running copy before the barrier proved drain (7.2 row 1)');
      return base('hold_running', { hold: 'unexplained', cost: [kv, r2].map((c) => costOk(num(c?.tokenCost))).find((v) => v !== null) ?? null });
    }
    if (e.success && (e.refunded || e.owed || e.refundDue || e.plain)) return base('hold_contradictory', { hold: 'contradictory' });
    if (e.disagree) return base('hold_contradictory', { hold: 'contradictory' });
    if (e.refunded) {
      const r2Refunded = !!r2 && isError(r2) && (r2.refunded === true || r2.refundSettled !== undefined);
      return base('refunded_by_evidence', {
        state: 'finished', outcome: 'refunded_legacy', finished: opts.now,
        code: str(errCopy?.errorCode) ?? 'submission_failed', msg: str(errCopy?.error),
        status_at: r2Refunded ? opts.now : null,
      });
    }
    if (e.success) {
      const succ = [kv, r2].find((c): c is Copy => !!c && c.status === 'success') as Copy;
      const r2Holds = !!r2 && r2.status === 'success';
      const kvSuccess = !!kv && kv.status === 'success';
      return base('identity_succeeded', {
        state: 'finished', outcome: succ.rescued === true ? 'rescued' : 'succeeded', finished: opts.now,
        artifact: 'published', status_at: r2Holds ? opts.now : null,
      }, r2Holds || !kvSuccess ? {} : { repair_record: kv });
    }
    if (e.plain) return base('hold_unexplained', { hold: 'unexplained' });
    if ((e.owed || e.refundDue || e.pending) && e.cost === null) return base('hold_no_cost', { hold: 'no_cost' });
    if (e.pending && !e.owed && !e.refundDue && !e.namesRequestId) return base('hold_no_cost', { hold: 'no_cost' });
    if (e.owed) {
      const owedCopy = [kv, r2].find((c): c is Copy => !!c && isError(c) && owedOf(c) !== null) as Copy;
      return base('owed_ambiguous', { due_code: 'refund_credit_failed', cost: costOk(num(owedOf(owedCopy)?.tokenCost)), msg: str(owedCopy.error) });
    }
    if (e.refundDue && dueCopy) {
      const due = dueCopy.refundDue as Row;
      return base('refund_due_ambiguous', { due_code: str(due.errorCode) ?? 'refund_due', msg: str(due.error), cost: e.cost });
    }
    if (e.pending) {
      const p = [kv, r2].find((c): c is Copy => !!c && c.status === 'pending') as Copy;
      return base('pending', { state: 'enqueued', cost: e.cost, enqueued: num(p.enqueuedAt) });
    }
    return stop('no row of 7.2 describes this job');
  };

  if (carried.length) {
    const scan = plainClass();
    if (scan && (scan.cls === 'refunded_by_evidence' || scan.cls === 'identity_succeeded')) return scan;
    const last = carried[carried.length - 1];
    const carriedEvidence = carried.map((c) => ({
      pause_epoch_ms: c.pause_epoch_ms, evidence: safeJson(c.evidence_json), paid_evidence: safeJson(c.paid_evidence_json),
    }));
    if (!uid || !mode) return stop('a carried job without its user or mode');
    return base('hold_carried', { hold: 'carried', cost: costOk(last.token_cost) }, { carried: carriedEvidence });
  }
  const c = plainClass();
  if (!c) return stop('no copy');
  if (c.row && (!c.row.uid || !c.row.mode)) return stop('a copy without userId or mode');
  return c;
}

function safeJson(s: string | null): unknown {
  if (s === null) return null;
  try {
    return JSON.parse(s);
  } catch {
    return s;
  }
}

/** Would release 1 pay this KV copy (8): an owed error not refunded, or a
 *  running copy with a tokenCost, refund due or not. */
export function kvPayable(kv: Copy | null): 0 | 1 {
  if (!kv) return 0;
  if (isError(kv) && owedOf(kv) !== null && kv.refunded !== true) return 1;
  if (kv.status === 'running' && num(kv.tokenCost) !== null) return 1;
  return 0;
}

// ── The kept content (amendment 6) ──

interface KeptJob {
  job_id: string;
  kv: Copy | null;
  r2: Copy | null;
  keys: Keys;
  fingerprint: string;
  first_seen_scan: string;
  last_scan: string;
}

interface ScanSummary {
  scan_id: string;
  started_at: number;
  finished_at: number;
  jobs: number;
  changed: number;
  new: number;
}

const keptKey = (epoch: number, jobId: string) => `${KEPT_PREFIX}${epoch}/jobs/${jobId}.json`;
const indexKey = (epoch: number) => `${KEPT_PREFIX}${epoch}/index.json`;

async function readKept(env: Env, epoch: number, jobId: string): Promise<KeptJob | null> {
  const o = await env.GALLERY_BUCKET.get(keptKey(epoch, jobId));
  return o ? (JSON.parse(await o.text()) as KeptJob) : null;
}

async function readIndex(env: Env, epoch: number): Promise<{ scans: ScanSummary[] }> {
  const o = await env.GALLERY_BUCKET.get(indexKey(epoch));
  return o ? (JSON.parse(await o.text()) as { scans: ScanSummary[] }) : { scans: [] };
}

/** Q3 as the kept content records it: the latest scan found nothing changed
 *  and nothing new, and began at least 15 minutes after the one before it. */
export function barrierProvenBy(index: { scans: ScanSummary[] }): boolean {
  const s = index.scans;
  if (s.length < 2) return false;
  const last = s[s.length - 1];
  const prev = s[s.length - 2];
  return last.changed === 0 && last.new === 0 && last.started_at - prev.started_at >= Q3_GAP_MS;
}

/** The union of what is in the stores now and what this pause's scans kept:
 *  a copy present now is read now; a copy since expired comes from the kept
 *  content (7.2). The keys are read now, and a key once seen stays seen. */
function mergeKept(now: { kv: Copy | null; r2: Copy | null; keys: Keys }, kept: KeptJob | null): JobInputs {
  if (!kept) return now;
  const pages: Record<string, boolean> = { ...kept.keys.pages };
  for (const [r, v] of Object.entries(now.keys.pages)) pages[r] = v || pages[r] === true;
  return {
    kv: now.kv ?? kept.kv,
    r2: now.r2 ?? kept.r2,
    keys: { consumer: now.keys.consumer || kept.keys.consumer, pages },
  };
}

// ── Cursors ──

interface Cursor {
  mode: MigratorMode;
  stage: string;
  /** A fresh id per scan, so a kept record shows whether this scan has
   *  already visited it. */
  scan_id: string;
  started_at: number;
  kv?: string;
  r2?: string;
  kept?: string;
  counts: Record<string, number>;
}

function encodeCursor(c: Cursor): string {
  return btoa(JSON.stringify(c)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}
function decodeCursor(s: string, mode: MigratorMode): Cursor {
  const b = s.replace(/-/g, '+').replace(/_/g, '/');
  const c = JSON.parse(atob(b + '='.repeat((4 - (b.length % 4)) % 4))) as Cursor;
  if (c.mode !== mode || typeof c.stage !== 'string' || typeof c.counts !== 'object') throw new Error('cursor for another mode');
  return c;
}
const add = (c: Cursor, k: string, n = 1) => { c.counts[k] = (c.counts[k] ?? 0) + n; };

/** One page of a KV prefix: its names, and the cursor (undefined at the end). */
async function kvPage(env: Env, prefix: string, cursor: string | undefined, limit: number) {
  const r = await env.SPRITEBREW_KV.list({ prefix, cursor, limit });
  return { keys: r.keys, next: r.list_complete ? undefined : (r as { cursor?: string }).cursor };
}
async function r2Page(env: Env, prefix: string, cursor: string | undefined, limit: number) {
  const r = await env.GALLERY_BUCKET.list({ prefix, cursor, limit });
  return { keys: r.objects.map((o) => o.key), next: r.truncated ? r.cursor : undefined };
}

async function carriedFor(db: D1Database, jobIds: string[]): Promise<Map<string, CarriedRow[]>> {
  const out = new Map<string, CarriedRow[]>();
  if (!jobIds.length) return out;
  const r = await db.prepare(
    `SELECT subject_id, pause_epoch_ms, user_id, mode, token_cost, evidence_json, paid_evidence_json, decided_at_ms
       FROM switch_obligations
      WHERE subject_kind = 'job' AND disposition = 'carry' AND settled_at_ms IS NULL
        AND subject_id IN (SELECT value FROM json_each(?1))`,
  ).bind(JSON.stringify(jobIds)).all<Row>();
  for (const x of r.results ?? []) {
    const id = String(x.subject_id);
    out.set(id, [...(out.get(id) ?? []), x as unknown as CarriedRow]);
  }
  return out;
}

// ── The scan (steps 1b, 3 and 4, and an abort's drain) ──

async function scan(env: Env, ctl: ControlState, cursorText: string | null, mode: 'scan' | 'classify', now: number): Promise<Response> {
  const writing = ctl.pause === '1' && ctl.pauseEpoch !== null;
  const epoch = ctl.pauseEpoch as number;
  const c: Cursor = cursorText ? decodeCursor(cursorText, mode) : { mode, stage: 'kv', scan_id: crypto.randomUUID(), started_at: now, counts: {} };
  const chunk: Row = { holds: [] as Row[], running_no_task_over_15m: [] as string[], debts_over_20m: [] as string[], r2_only_running: [] as string[] };
  const index = writing ? await readIndex(env, epoch) : { scans: [] as ScanSummary[] };
  const barrierProven = barrierProvenBy(index);

  const visit = async (jobId: string, from: 'kv' | 'r2' | 'kept') => {
    const copies = await readCopies(env, jobId);
    if (from === 'r2' && copies.kv) return;
    if (from === 'kept' && (copies.kv || copies.r2)) return;
    const keys = await readKeys(env, jobId, [copies.kv, copies.r2]);
    const kept = writing ? await readKept(env, epoch, jobId) : null;
    const merged = mergeKept({ ...copies, keys }, kept);
    const fp = await fingerprint(merged);
    add(c, 'jobs');
    if (copies.kv) add(c, 'kv_copies');
    if (copies.r2) add(c, 'r2_copies');
    if (!copies.kv && !copies.r2) add(c, 'kept_only');
    if (kept && kept.fingerprint !== fp) add(c, 'changed');
    if (writing && !kept) add(c, 'new');
    // Step 1b's report (7.3): KV-visible work that must settle before the pause.
    const kv = copies.kv;
    if (kv?.status === 'running' && !kv.taskId && (num(kv.startedAt) ?? now) < now - RUNNING_NO_TASK_MS) (chunk.running_no_task_over_15m as string[]).push(jobId);
    if (kv && ((isError(kv) && owedOf(kv) && kv.refunded !== true && (num(kv.failedAt) ?? now) < now - DEBT_AGE_MS)
      || (kv.status === 'running' && kv.refundDue && (num((kv.refundDue as Row).at) ?? now) < now - DEBT_AGE_MS))) {
      (chunk.debts_over_20m as string[]).push(jobId);
    }
    if (!copies.kv && copies.r2?.status === 'running') (chunk.r2_only_running as string[]).push(jobId);
    if (writing) {
      const next: KeptJob = { job_id: jobId, kv: merged.kv, r2: merged.r2, keys: merged.keys, fingerprint: fp, first_seen_scan: kept?.first_seen_scan ?? c.scan_id, last_scan: c.scan_id };
      await env.GALLERY_BUCKET.put(keptKey(epoch, jobId), JSON.stringify(next));
    }
    if (mode === 'classify') {
      const carried = ctl.pause === '1' ? (await carriedFor(env.LEDGER_DB as D1Database, [jobId])).get(jobId) : undefined;
      const k = await classifyJob(jobId, merged, { now, barrierProven, carried });
      add(c, `class_${k.cls ?? 'stop'}`);
      if (k.cls === null || k.row?.hold) (chunk.holds as Row[]).push({ job_id: jobId, class: k.cls, stop: k.stop ?? null, cost: k.row?.cost ?? null });
    }
  };

  if (c.stage === 'kv') {
    const p = await kvPage(env, 'job:', c.kv, PAGE_JOBS);
    for (const k of p.keys) await visit(k.name.slice('job:'.length), 'kv');
    if (p.next) c.kv = p.next; else c.stage = 'r2';
  } else if (c.stage === 'r2') {
    const p = await r2Page(env, 'jobs/', c.r2, PAGE_JOBS);
    for (const key of p.keys) if (key.endsWith('.json')) await visit(key.slice('jobs/'.length, -'.json'.length), 'r2');
    if (p.next) c.r2 = p.next; else c.stage = writing ? 'kept' : mode === 'classify' ? 'balances' : 'done';
  } else if (c.stage === 'kept') {
    const p = await r2Page(env, `${KEPT_PREFIX}${epoch}/jobs/`, c.kept, PAGE_JOBS);
    for (const key of p.keys) {
      const jobId = key.slice(`${KEPT_PREFIX}${epoch}/jobs/`.length, -'.json'.length);
      const kept = await readKept(env, epoch, jobId);
      if (kept && kept.last_scan === c.scan_id) continue;
      await visit(jobId, 'kept');
    }
    if (p.next) c.kept = p.next; else c.stage = mode === 'classify' ? 'balances' : 'done';
  } else if (c.stage === 'balances') {
    // Step 4: every token_balance: key that fails balances' user check.
    const p = await kvPage(env, 'token_balance:', c.kv, 1000);
    const bad = p.keys.map((k) => k.name.slice('token_balance:'.length)).filter((u) => !USER_ID_OK.test(u));
    add(c, 'balance_keys', p.keys.length);
    add(c, 'nonconforming_balance_keys', bad.length);
    chunk.nonconforming_balance_keys = bad;
    if (p.next) c.kv = p.next; else c.stage = 'done';
  }
  const done = c.stage === 'done';
  if (done && writing) {
    const idx = await readIndex(env, epoch);
    idx.scans.push({ scan_id: c.scan_id, started_at: c.started_at, finished_at: now, jobs: c.counts.jobs ?? 0, changed: c.counts.changed ?? 0, new: c.counts.new ?? 0 });
    await env.GALLERY_BUCKET.put(indexKey(epoch), JSON.stringify(idx));
  }
  return json({
    mode, done, writing_kept_content: writing, pause_epoch_ms: writing ? epoch : null, barrier_proven: barrierProven,
    counts: c.counts, chunk, cursor: done ? null : encodeCursor(c),
    ...(done && writing ? { scans_in_pause: (await readIndex(env, epoch)).scans.length, barrier_proven_after: barrierProvenBy(await readIndex(env, epoch)) } : {}),
  });
}

// ── The snapshot (step 5, 4.17) ──

const LEGACY_KINDS: Array<[RegExp, string]> = [
  [/^token_idempotency:refund:gen:/, 'refund_gen'],
  [/^token_idempotency:refund:/, 'refund_job'],
  [/^token_idempotency:gen:/, 'debit_gen'],
  [/^token_idempotency:signup:/, 'signup'],
  [/^token_idempotency:daily_login:/, 'daily_login'],
  [/^token_idempotency:email_list:/, 'email_list'],
  [/^token_idempotency:earnback:/, 'earnback'],
  [/^token_idempotency:evt_/, 'stripe_credit'],
  [/^webhook:stripe:/, 'stripe_event'],
];
export const legacyKind = (key: string): string => LEGACY_KINDS.find(([re]) => re.test(key))?.[1] ?? 'other';

async function snapshot(env: Env, cursorText: string | null, now: number): Promise<Response> {
  const db = env.LEDGER_DB as D1Database;
  // The pause-start check, first, every chunk (HQ-8 condition 1).
  const chk = (await bind(db, PAUSE_START_CHECK, {}).all<Row>()).results?.[0] ?? {};
  const cutoff = num(chk.cutoff_ms);
  const epoch = num(chk.pause_epoch_ms);
  if (cutoff === null || epoch === null || cutoff !== epoch) {
    return json({ mode: 'snapshot', abort: true, why: 'the cutoff does not equal this pause epoch', cutoff_ms: cutoff, pause_epoch_ms: epoch });
  }
  const c: Cursor = cursorText ? decodeCursor(cursorText, 'snapshot') : { mode: 'snapshot', stage: 'balances', scan_id: crypto.randomUUID(), started_at: now, counts: {} };
  const chunk: Row = {};
  const halt = (why: string, extra: Row = {}) =>
    json({ mode: 'snapshot', done: false, stopped: why, stage: c.stage, counts: c.counts, chunk: { ...chunk, ...extra }, cursor: null });

  if (c.stage === 'balances') {
    const p = await kvPage(env, 'token_balance:', c.kv, PAGE_BALANCES);
    const users: Array<{ uid: string; amount: number; kvLast: string | null }> = [];
    for (const k of p.keys) {
      const uid = k.name.slice('token_balance:'.length);
      if (!USER_ID_OK.test(uid)) return halt('a non-conforming token_balance key (step 4 should have stopped)', { key: k.name });
      const raw = await env.SPRITEBREW_KV.get(k.name);
      const rec = parseCopy(raw, k.name);
      const amount = rec ? num(rec.balance) : null;
      if (amount === null || !Number.isSafeInteger(amount)) return halt('a balance that is not an integer', { key: k.name });
      users.push({ uid, amount, kvLast: str(rec?.last_updated) });
    }
    if (users.length) {
      await db.batch(users.flatMap((u) => {
        const p0 = { id: crypto.randomUUID(), uid: u.uid, amount: u.amount, now, meta: null, kv_last: u.kvLast };
        return [bind(db, IMPORT_BALANCE_LEDGER, p0), bind(db, IMPORT_BALANCE_ROW, p0)];
      }));
      const rb = (await bind(db, IMPORT_BALANCES_READ, { uids: JSON.stringify(users.map((u) => u.uid)) }).all<Row>()).results ?? [];
      const by = new Map(rb.map((r) => [String(r.user_id), r]));
      for (const u of users) {
        const r = by.get(u.uid);
        if (!r || r.balance === null || r.balance === undefined) return halt('refused (the guards)', { user_id: u.uid });
        if (r.balance !== u.amount || r.opened_amount !== u.amount || r.opened_source !== 'snapshot') {
          return halt('error: D1 differs from KV', { user_id: u.uid, kv: u.amount, balance: r.balance, opened_amount: r.opened_amount, opened_source: r.opened_source });
        }
      }
    }
    chunk.kv_count = users.length;
    chunk.kv_sum = users.reduce((s, u) => s + u.amount, 0);
    chunk.d1_count = users.length;
    chunk.d1_sum = chunk.kv_sum;
    add(c, 'balances_imported', users.length);
    add(c, 'balances_sum', chunk.kv_sum as number);
    if (p.next) c.kv = p.next; else { c.stage = 'legacy_idem'; c.kv = undefined; }
  } else if (c.stage === 'legacy_idem' || c.stage === 'legacy_marks') {
    const prefix = c.stage === 'legacy_idem' ? 'token_idempotency:' : 'webhook:stripe:';
    const p = await kvPage(env, prefix, c.kv, PAGE_KEYS);
    if (p.keys.length) {
      await db.batch(p.keys.map((k) => bind(db, IMPORT_LEGACY, {
        key: k.name, kind: legacyKind(k.name), kv_exp: typeof k.expiration === 'number' ? k.expiration * 1000 : null, now,
      })));
      const rb = (await bind(db, IMPORT_LEGACY_READ, { keys: JSON.stringify(p.keys.map((k) => k.name)) }).all<Row>()).results ?? [];
      const missing = rb.filter((r) => r.copied_at_ms === null || r.copied_at_ms === undefined).map((r) => r.key);
      if (missing.length || rb.length !== p.keys.length) return halt('refused (the guards)', { keys: missing });
    }
    chunk.kv_listed = p.keys.length;
    chunk.imported = p.keys.length;
    add(c, 'legacy_imported', p.keys.length);
    if (p.next) c.kv = p.next;
    else { c.kv = undefined; c.stage = c.stage === 'legacy_idem' ? 'legacy_marks' : 'carried_events'; }
  } else if (c.stage === 'carried_events') {
    const [, rb] = await db.batch([bind(db, IMPORT_CARRIED_EVENTS, { now }), bind(db, IMPORT_CARRIED_EVENTS_READ, { now })]);
    const rows = (rb.results ?? []) as Row[];
    const refused = rows.filter((r) => (r.reason === null || r.reason === undefined) && Number(r.legacy) !== 1).map((r) => r.subject_id);
    if (refused.length) return halt('refused (the guards)', { events: refused });
    chunk.carried_events = rows.length;
    chunk.as_pending = rows.filter((r) => r.reason !== null && r.reason !== undefined && Number(r.legacy) !== 1).length;
    chunk.already_applied = rows.filter((r) => Number(r.legacy) === 1).length;
    add(c, 'carried_events', rows.length);
    c.stage = 'jobs_kept';
  } else {
    // Jobs: the kept content first, then what the stores hold that no scan
    // kept, then carried jobs with no copy at all.
    const index = await readIndex(env, epoch);
    const barrierProven = barrierProvenBy(index);
    let ids: string[] = [];
    let next: string | undefined;
    let nextStage = '';
    if (c.stage === 'jobs_kept') {
      const p = await r2Page(env, `${KEPT_PREFIX}${epoch}/jobs/`, c.kept, PAGE_BALANCES);
      ids = p.keys.map((k) => k.slice(`${KEPT_PREFIX}${epoch}/jobs/`.length, -'.json'.length));
      next = p.next;
      nextStage = 'jobs_kv';
    } else if (c.stage === 'jobs_kv') {
      const p = await kvPage(env, 'job:', c.kv, PAGE_BALANCES);
      ids = [];
      for (const k of p.keys) {
        const id = k.name.slice('job:'.length);
        if (!(await env.GALLERY_BUCKET.head(keptKey(epoch, id)))) ids.push(id);
      }
      next = p.next;
      nextStage = 'jobs_r2';
    } else if (c.stage === 'jobs_r2') {
      const p = await r2Page(env, 'jobs/', c.r2, PAGE_BALANCES);
      for (const key of p.keys) {
        if (!key.endsWith('.json')) continue;
        const id = key.slice('jobs/'.length, -'.json'.length);
        if (!(await env.GALLERY_BUCKET.head(keptKey(epoch, id))) && (await env.SPRITEBREW_KV.get(`job:${id}`)) === null) ids.push(id);
      }
      next = p.next;
      nextStage = 'jobs_carried';
    } else if (c.stage === 'jobs_carried') {
      const r = await db.prepare(
        `SELECT DISTINCT o.subject_id FROM switch_obligations AS o
          WHERE o.subject_kind = 'job' AND o.disposition = 'carry' AND o.settled_at_ms IS NULL
            AND NOT EXISTS (SELECT 1 FROM jobs AS j WHERE j.job_id = o.subject_id)`,
      ).all<Row>();
      ids = (r.results ?? []).map((x) => String(x.subject_id));
      nextStage = 'done';
    } else {
      return json({ mode: 'snapshot', done: true, counts: c.counts, chunk: {}, cursor: null });
    }
    const carried = await carriedFor(db, ids);
    const rows: Classified[] = [];
    for (const id of ids) {
      const copies = await readCopies(env, id);
      const keys = await readKeys(env, id, [copies.kv, copies.r2]);
      const kept = await readKept(env, epoch, id);
      const k = await classifyJob(id, mergeKept({ ...copies, keys }, kept), { now, barrierProven, carried: carried.get(id) });
      if (!k.row) return halt(k.stop ?? 'unclassified', { job_id: id });
      rows.push(k);
      add(c, `class_${k.cls}`);
    }
    if (rows.length) {
      await db.batch(rows.map((k) => bind(db, IMPORT_JOB, k.row as unknown as Params)));
      const rb = (await bind(db, IMPORT_JOBS_READ, { jobs: JSON.stringify(rows.map((k) => k.row!.job)) }).all<Row>()).results ?? [];
      const by = new Map(rb.map((r) => [String(r.job_id), r]));
      for (const k of rows) {
        const r = by.get(k.row!.job);
        if (!r || r.fingerprint === null || r.fingerprint === undefined) return halt('refused (the guards)', { job_id: k.row!.job });
        if (r.fingerprint !== k.fingerprint) return halt('error: the stored fingerprint differs', { job_id: k.row!.job });
      }
    }
    chunk.jobs = rows.length;
    add(c, 'jobs_imported', rows.length);
    if (next) {
      if (c.stage === 'jobs_kept') c.kept = next; else if (c.stage === 'jobs_kv') c.kv = next; else c.r2 = next;
    } else {
      c.stage = nextStage;
      c.kv = undefined;
    }
  }
  const done = c.stage === 'done';
  return json({ mode: 'snapshot', done, stage: c.stage, counts: c.counts, chunk, cursor: done ? null : encodeCursor(c) });
}

// ── The obligation handoff (?handoff=1, an abort, 8) ──

/** Strip a success copy's PNG from the stored evidence; the fingerprint is
 *  taken over the full content. */
function evidenceCopy(c: Copy | null): Copy | null {
  if (!c || typeof c.resultBase64 !== 'string') return c;
  const { resultBase64, ...rest } = c;
  return { ...rest, resultBase64_length: (resultBase64 as string).length };
}

async function handoff(env: Env, ctl: ControlState, cursorText: string | null, now: number): Promise<Response> {
  const db = env.LEDGER_DB as D1Database;
  const epoch = ctl.pauseEpoch as number;
  const marks = (await db.prepare("SELECT value_ms FROM switch_marks WHERE name = 'pause_start_ms'").all<Row>()).results ?? [];
  const switchEpoch = num(marks[0]?.value_ms);
  const c: Cursor = cursorText ? decodeCursor(cursorText, 'handoff') : { mode: 'handoff', stage: 'kept', scan_id: crypto.randomUUID(), started_at: now, counts: {} };
  const listed: Row[] = [];
  const index = await readIndex(env, epoch);
  const barrierProven = barrierProvenBy(index);

  const keptUnion = async (id: string) => (await readKept(env, epoch, id)) ?? (switchEpoch !== null && switchEpoch !== epoch ? await readKept(env, switchEpoch, id) : null);
  const consider = async (id: string) => {
    const copies = await readCopies(env, id);
    const keys = await readKeys(env, id, [copies.kv, copies.r2]);
    const merged = mergeKept({ ...copies, keys }, await keptUnion(id));
    const carried = (await carriedFor(db, [id])).get(id);
    const k = await classifyJob(id, merged, { now, barrierProven: true, carried });
    const held = k.row?.hold != null || k.cls === null;
    const keptCopy = !copies.kv && (k.cls === 'hold_running' || k.cls === 'owed_ambiguous' || k.cls === 'refund_due_ambiguous');
    if (!held && !keptCopy) return;
    const uid = k.row?.uid ?? str(merged.kv?.userId) ?? str(merged.r2?.userId);
    const mode = k.row?.mode ?? str(merged.kv?.mode) ?? str(merged.r2?.mode);
    if (!uid || !mode) throw new MigratorStop(`job ${id}: an obligation without its user or mode`);
    listed.push({
      kind: 'job', subject: id, uid, mode, source: held ? 'hold' : 'kept_copy', class: k.cls ?? 'stopped',
      cost: k.row?.cost ?? evidenceOf(merged.kv, merged.r2, merged.keys).cost, kv_payable: kvPayable(copies.kv),
      evidence: JSON.stringify({ fingerprint: k.fingerprint, class: k.cls, stop: k.stop ?? null, kv: evidenceCopy(merged.kv), r2: evidenceCopy(merged.r2), keys: merged.keys, barrier_proven: barrierProven }),
      fingerprint: k.fingerprint,
    });
  };

  if (c.stage === 'kept') {
    const prefixes = [epoch, ...(switchEpoch !== null && switchEpoch !== epoch ? [switchEpoch] : [])];
    const p = await r2Page(env, `${KEPT_PREFIX}${prefixes[c.counts.kept_prefix ?? 0]}/jobs/`, c.kept, PAGE_BALANCES);
    for (const key of p.keys) await consider(key.slice(key.lastIndexOf('/') + 1, -'.json'.length));
    if (p.next) c.kept = p.next;
    else if ((c.counts.kept_prefix ?? 0) + 1 < prefixes.length) { add(c, 'kept_prefix'); c.kept = undefined; }
    else { c.stage = 'kv'; c.kept = undefined; }
  } else if (c.stage === 'kv') {
    const p = await kvPage(env, 'job:', c.kv, PAGE_BALANCES);
    for (const k of p.keys) {
      const id = k.name.slice('job:'.length);
      if (!(await keptUnion(id))) await consider(id);
    }
    if (p.next) c.kv = p.next; else { c.stage = 'r2'; c.kv = undefined; }
  } else if (c.stage === 'r2') {
    const p = await r2Page(env, 'jobs/', c.r2, PAGE_BALANCES);
    for (const key of p.keys) {
      if (!key.endsWith('.json')) continue;
      const id = key.slice('jobs/'.length, -'.json'.length);
      if (!(await keptUnion(id)) && (await env.SPRITEBREW_KV.get(`job:${id}`)) === null) await consider(id);
    }
    if (p.next) c.r2 = p.next; else c.stage = 'holds';
  } else if (c.stage === 'holds') {
    // Imported rows already held in D1 (after a partial snapshot).
    const r = await db.prepare('SELECT job_id, user_id, mode, token_cost, import_json FROM jobs WHERE hold_reason IS NOT NULL').all<Row>();
    for (const x of r.results ?? []) {
      const id = String(x.job_id);
      if (listed.some((l) => l.subject === id)) continue;
      const imp = safeJson(str(x.import_json)) as Row | null;
      const fp = str(imp?.fingerprint) ?? (await fingerprint(x));
      const copies = await readCopies(env, id);
      listed.push({ kind: 'job', subject: id, uid: x.user_id, mode: x.mode, source: 'hold', class: str(imp?.class) ?? 'held_row',
        cost: x.token_cost ?? null, kv_payable: kvPayable(copies.kv), evidence: JSON.stringify({ fingerprint: fp, import_json: imp }), fingerprint: fp });
    }
    c.stage = 'done';
  }
  // Write and read back this chunk's obligations (8 step 2 and 3).
  let readback: Row[] = [];
  if (listed.length) {
    await db.batch(listed.map((o) => bind(db, HANDOFF_INSERT, {
      epoch, kind: o.kind, subject: o.subject, uid: o.uid, mode: o.mode, source: o.source, evidence: o.evidence,
      cost: o.cost ?? null, kv_payable: o.kv_payable, now,
    })));
    readback = (await bind(db, HANDOFF_READ, { epoch, subjects: JSON.stringify(listed.map((o) => ({ kind: o.kind, id: o.subject }))) }).all<Row>()).results ?? [];
  }
  const rows = listed.map((o) => {
    const r = readback.find((x) => x.subject_id === o.subject && x.kind === o.kind);
    const equal = !!r && r.fingerprint === o.fingerprint && Number(r.kv_payable) === o.kv_payable;
    if (!equal) add(c, 'differs');
    add(c, 'listed');
    return { kind: o.kind, subject: o.subject, source: o.source, class: o.class, cost: o.cost, kv_payable: o.kv_payable, read_back_equal: equal };
  });
  const done = c.stage === 'done';
  return json({ mode: 'handoff', done, pause_epoch_ms: epoch, switch_epoch_ms: switchEpoch, counts: c.counts, chunk: { obligations: rows },
    all_equal_so_far: (c.counts.differs ?? 0) === 0, cursor: done ? null : encodeCursor(c) });
}

// ── The verify scan (step 8) and the backstop runs (9b, before a reopen, 13) ──

async function verify(env: Env, ctl: ControlState, cursorText: string | null, now: number): Promise<Response> {
  const db = env.LEDGER_DB as D1Database;
  const c: Cursor = cursorText ? decodeCursor(cursorText, 'verify') : { mode: 'verify', stage: 'balances', scan_id: crypto.randomUUID(), started_at: now, counts: {} };
  const drift: Row[] = [];
  const failPage = (stage: string) => { c.counts[`${stage}_failed_page`] = 1; };

  if (c.stage === 'balances') {
    let p: Awaited<ReturnType<typeof kvPage>>;
    try {
      p = await kvPage(env, 'token_balance:', c.kv, 50);
    } catch {
      failPage('balances');
      c.stage = 'idem'; c.kv = undefined;
      return verifyAnswer(c, drift, now, db, ctl, env);
    }
    add(c, 'balances_listed', p.keys.length);
    const uids = p.keys.map((k) => k.name.slice('token_balance:'.length));
    const base = new Map(((await db.prepare(
      `SELECT u.value AS user_id, l.amount AS opened, b.opened_via
         FROM json_each(?1) AS u
         LEFT JOIN ledger AS l ON l.idem_key = 'open:' || u.value AND l.source = 'snapshot'
         LEFT JOIN balances AS b ON b.user_id = u.value`,
    ).bind(JSON.stringify(uids)).all<Row>()).results ?? []).map((r) => [String(r.user_id), r]));
    for (const uid of uids) {
      const rec = parseCopy(await env.SPRITEBREW_KV.get(`token_balance:${uid}`), `token_balance:${uid}`);
      add(c, 'balances_read');
      const b = base.get(uid);
      if (b?.opened_via === 'snapshot') add(c, 'snapshot_read');
      const value = num(rec?.balance);
      const opened = num(b?.opened);
      if (opened === null || value !== opened) {
        drift.push({ kind: 'balance', subject: uid, user_id: uid, snapshot_json: opened === null ? null : JSON.stringify({ balance: opened }), found_json: JSON.stringify({ balance: value }) });
      }
    }
    if (p.next) c.kv = p.next; else { c.counts.balances_exhausted = 1; c.stage = 'idem'; c.kv = undefined; }
  } else if (c.stage === 'idem' || c.stage === 'marks') {
    const prefix = c.stage === 'idem' ? 'token_idempotency:' : 'webhook:stripe:';
    let p: Awaited<ReturnType<typeof kvPage>>;
    try {
      p = await kvPage(env, prefix, c.kv, 1000);
    } catch {
      failPage(c.stage);
      c.stage = c.stage === 'idem' ? 'marks' : 'jobs_kv'; c.kv = undefined;
      return verifyAnswer(c, drift, now, db, ctl, env);
    }
    add(c, 'keys_listed', p.keys.length);
    const names = p.keys.map((k) => k.name);
    const r = (await db.prepare(
      `SELECT k.value AS key,
              EXISTS (SELECT 1 FROM legacy_idem WHERE key = k.value) AS kept,
              EXISTS (SELECT 1 FROM ledger WHERE idem_key = 'stripe:' || substr(k.value, 16)) AS moved,
              EXISTS (SELECT 1 FROM stripe_pending WHERE event_id = substr(k.value, 16) AND resolved_at_ms IS NOT NULL) AS resolved
         FROM json_each(?1) AS k`,
    ).bind(JSON.stringify(names)).all<Row>()).results ?? [];
    for (const x of r) {
      add(c, 'keys_read');
      const key = String(x.key);
      const isMark = key.startsWith('webhook:stripe:');
      const own = isMark && (Number(x.moved) === 1 || Number(x.resolved) === 1);
      if (Number(x.kept) !== 1 && !own) drift.push({ kind: 'key', subject: key, user_id: null, snapshot_json: null, found_json: JSON.stringify({ present: true }) });
    }
    if (p.next) c.kv = p.next;
    else {
      c.counts[c.stage === 'idem' ? 'idem_exhausted' : 'marks_exhausted'] = 1;
      c.stage = c.stage === 'idem' ? 'marks' : 'jobs_kv';
      c.kv = undefined;
    }
  } else if (c.stage === 'jobs_kv' || c.stage === 'jobs_r2') {
    let p: { keys: string[]; next: string | undefined };
    try {
      p = c.stage === 'jobs_kv'
        ? await kvPage(env, 'job:', c.kv, 50).then((x) => ({ keys: x.keys.map((k) => k.name.slice('job:'.length)), next: x.next }))
        : await r2Page(env, 'jobs/', c.r2, 50).then((x) => ({ keys: x.keys.filter((k) => k.endsWith('.json')).map((k) => k.slice('jobs/'.length, -'.json'.length)), next: x.next }));
    } catch {
      failPage(c.stage);
      c.stage = c.stage === 'jobs_kv' ? 'jobs_r2' : 'done';
      return verifyAnswer(c, drift, now, db, ctl, env);
    }
    add(c, 'jobs_listed', p.keys.length);
    const rows = new Map(((await db.prepare(
      `SELECT job_id, provenance, outcome, status_written_at_ms, import_json FROM jobs WHERE job_id IN (SELECT value FROM json_each(?1))`,
    ).bind(JSON.stringify(p.keys)).all<Row>()).results ?? []).map((r) => [String(r.job_id), r]));
    for (const id of p.keys) {
      const copy = c.stage === 'jobs_kv'
        ? parseCopy(await env.SPRITEBREW_KV.get(`job:${id}`), `job:${id}`)
        : parseCopy(await (await env.GALLERY_BUCKET.get(`jobs/${id}.json`))?.text() ?? null, `jobs/${id}.json`);
      add(c, 'jobs_read');
      if (!copy) continue; // gone by its TTL or lifecycle since the listing: not drift
      const row = rows.get(id);
      if (row && row.provenance !== 'kv') continue; // release 2's own job
      const imp = row ? (safeJson(str(row.import_json)) as Row | null) : null;
      const kept = imp?.copy_fingerprints as Row | undefined;
      const fp = await fingerprint(copy);
      const side = c.stage === 'jobs_kv' ? 'kv' : 'r2';
      if (kept && kept[side] === fp) continue;
      if (row && isOwnStatus(copy, row)) continue;
      drift.push({ kind: 'job_record', subject: id, user_id: str(copy.userId), snapshot_json: imp ? JSON.stringify({ class: imp.class, copies: imp.copies }) : null,
        found_json: JSON.stringify({ copy: side, status: copy.status ?? null, fingerprint: fp }) });
    }
    if (p.next) { if (c.stage === 'jobs_kv') c.kv = p.next; else c.r2 = p.next; }
    else {
      c.counts[c.stage === 'jobs_kv' ? 'jobs_kv_exhausted' : 'jobs_r2_exhausted'] = 1;
      c.stage = c.stage === 'jobs_kv' ? 'jobs_r2' : 'done';
      c.kv = undefined;
    }
  }
  return verifyAnswer(c, drift, now, db, ctl, env);
}

/** A status record release 2 wrote for a finished imported row: terminal and
 *  matching the row's outcome (4.10, 4.11). */
function isOwnStatus(copy: Copy, row: Row): boolean {
  if (row.status_written_at_ms === null || row.status_written_at_ms === undefined) return false;
  if (copy.status === 'success') return row.outcome === 'succeeded' || row.outcome === 'rescued';
  if (copy.status === 'error') {
    return (copy.refunded === true && (row.outcome === 'refunded' || row.outcome === 'refunded_legacy'))
      || (row.outcome === 'no_record' && copy.errorCode === 'no_record');
  }
  return false;
}

async function verifyAnswer(c: Cursor, drift: Row[], now: number, db: D1Database, ctl: ControlState, env: Env): Promise<Response> {
  add(c, 'drift_found', drift.length);
  const done = c.stage === 'done';
  let totals: Row | null = null;
  let named: Row[] | null = null;
  if (done) {
    const n = c.counts;
    // Coverage by identity (R8-1): the snapshot's customers read and not read.
    const snap = num(((await db.prepare("SELECT COUNT(*) AS n FROM balances WHERE opened_via = 'snapshot'").all<Row>()).results ?? [])[0]?.n) ?? 0;
    const snapRead = n.snapshot_read ?? 0;
    totals = {
      started_at_ms: c.started_at, completed_at_ms: now,
      balances_listed: n.balances_listed ?? 0, balances_read: n.balances_read ?? 0,
      keys_listed: n.keys_listed ?? 0, keys_read: n.keys_read ?? 0,
      jobs_listed: n.jobs_listed ?? 0, jobs_read: n.jobs_read ?? 0,
      balances_exhausted: n.balances_exhausted === 1 && !n.balances_failed_page ? 1 : 0,
      idem_exhausted: n.idem_exhausted === 1 && !n.idem_failed_page ? 1 : 0,
      marks_exhausted: n.marks_exhausted === 1 && !n.marks_failed_page ? 1 : 0,
      jobs_exhausted: n.jobs_kv_exhausted === 1 && n.jobs_r2_exhausted === 1 && !n.jobs_kv_failed_page && !n.jobs_r2_failed_page ? 1 : 0,
      snapshot_read: snapRead,
      snapshot_missing: Math.max(0, snap - snapRead),
      drift_found: n.drift_found ?? 0,
    };
    if (ctl.phase === 'scan' && ctl.pauseEpoch !== null) {
      // Step 8: every customer with an admission record admitted in the 24
      // hours before the pause, or closed by George's statement, by name.
      const users = ((await db.prepare(
        `SELECT DISTINCT user_id FROM money_admissions
          WHERE user_id IS NOT NULL AND ((admitted_at_ms >= ?1 - 86400000 AND admitted_at_ms <= ?1) OR closed_at_ms IS NOT NULL)`,
      ).bind(ctl.pauseEpoch).all<Row>()).results ?? []).map((r) => String(r.user_id));
      named = [];
      for (const uid of users) {
        const rec = parseCopy(await env.SPRITEBREW_KV.get(`token_balance:${uid}`), `token_balance:${uid}`);
        const opened = num(((await db.prepare("SELECT amount FROM ledger WHERE idem_key = 'open:' || ?1 AND source = 'snapshot'").bind(uid).all<Row>()).results ?? [])[0]?.amount);
        named.push({ user_id: uid, kv_balance: num(rec?.balance), snapshot_balance: opened, equal: opened !== null && num(rec?.balance) === opened });
      }
    }
  }
  return json({ mode: 'verify', phase: ctl.phase, done, counts: c.counts, chunk: { drift }, totals, named_customers: named, cursor: done ? null : encodeCursor(c) });
}

// ── Entry ──

export async function handleMigrator(request: Request, env: Env): Promise<Response> {
  if (!(await tokenMatches(request.headers.get('Authorization'), env.MIGRATE_TOKEN))) return notFound();
  const route = routeOf(request);
  if (!route) return notFound();
  const db = env.LEDGER_DB;
  if (!db) return notFound();
  let ctl: ControlState;
  try {
    ctl = await readControl(db);
  } catch {
    return notFound();
  }
  if (!admitted(route.mode, ctl)) return notFound();
  const now = Date.now();
  try {
    switch (route.mode) {
      case 'scan':
      case 'classify':
        return await scan(env, ctl, route.cursor, route.mode, now);
      case 'snapshot':
        return await snapshot(env, route.cursor, now);
      case 'handoff':
        return await handoff(env, ctl, route.cursor, now);
      case 'verify':
        return await verify(env, ctl, route.cursor, now);
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message.slice(0, 200) : 'error';
    return json({ mode: route.mode, error: msg }, err instanceof MigratorStop ? 409 : 500);
  }
}
