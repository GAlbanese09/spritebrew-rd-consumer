// spritebrew-rd-consumer/src/events.ts
//
// The one writer for the D1 event ledger (WD2a, 2026-09-19).
//
// STANDING RULE: the token_tx:* KV ledger is the money truth. This table is
// observability. A ledger write must never block, delay by more than a few
// hundred milliseconds, or fail a job. recordEvent therefore:
//   - wraps everything in try/catch and NEVER throws;
//   - logs 'ledger write failed' at warn and returns null on any failure;
//   - is idempotent through dedupe_key (INSERT OR IGNORE), so a queue retry
//     never double-counts and a redelivery can call it again freely.
//
// Schema: migrations/0001_events.sql. Typed columns are the query surface;
// the full canonical object lives in event_json (sorted keys) with its
// SHA-256 in event_sha256, so a row can be proven byte-for-byte later and a
// new field never needs a migration.

import type { Env } from './types';

/** Same shape as index.ts's Logger; declared here so this file imports
 *  nothing from index.ts. */
export type Logger = (
  level: 'info' | 'warn' | 'error',
  message: string,
  extra?: Record<string, unknown>
) => void;

export type LedgerEvent = {
  eventName: string;            // e.g. 'generation.failed'
  level: 'debug' | 'info' | 'warn' | 'error';
  dedupeKey: string;            // caller-supplied; see the call-site table in index.ts
  occurredAtMs?: number;        // default Date.now()
  userId?: string; jobId?: string; requestId?: string; queueMessageId?: string;
  provider?: string; providerJobId?: string; attempt?: number;
  style?: string; requestedSize?: string; finalSize?: string; outcome?: string;
  providerStatus?: string; errorCode?: string; failureStage?: string;
  httpStatus?: number; retryable?: boolean; refundExpected?: boolean;
  latencyMs?: number; queueWaitMs?: number; unitsDelta?: number;
  causedByEventId?: string;
  extra?: Record<string, unknown>;   // goes into event_json only
};

const SCHEMA_VERSION = 1;
const SOURCE_SERVICE = 'spritebrew-rd-consumer';

const defaultLogger: Logger = (level, message, extra = {}) => {
  console[level](JSON.stringify({ level, message, source: 'events', ...extra }));
};

// ─── Reporting day (New York calendar day) ─────────────────────────────────

// en-CA formats as YYYY-MM-DD. Never truncate a UTC ISO string for this: a
// 23:30 EDT job would land on tomorrow's day.
const NY_DAY = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/New_York',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

export function reportingDay(ms: number): string {
  return NY_DAY.format(new Date(ms));
}

// ─── Canonical serialization + hash ────────────────────────────────────────

/**
 * JSON.stringify with object keys sorted at every depth, undefined members
 * omitted (as JSON.stringify does), arrays in order. Deterministic, so the
 * same event always hashes the same.
 */
export function stableStringify(value: unknown): string {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      const v = (value as Record<string, unknown>)[key];
      if (v !== undefined) out[key] = sortKeys(v);
    }
    return out;
  }
  return value;
}

async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('');
}

// ─── Failure stage ─────────────────────────────────────────────────────────

/**
 * Where in the pipeline an errorCode from index.ts::classifyError (or the
 * two codes written directly: rd_submit_orphaned_redelivery and
 * stale_running_swept) originated. Pure; unknown codes map to 'unknown'.
 */
export function stageForErrorCode(code: string | undefined): string {
  if (!code) return 'unknown';
  if (code === 'rd_async_timeout') return 'poll';
  if (code.startsWith('rd_task_')) return 'poll';          // rd_task_success_no_image, rd_task_unknown_status
  if (code.startsWith('rd_submit_orphaned')) return 'submit';
  if (code === 'rd_sync_timeout') return 'sync';
  if (code === 'stale_running_swept') return 'sweep';
  if (code === 'dead_lettered') return 'queue';
  if (code.startsWith('rd_')) return 'provider';          // rd_<http status>
  return 'unknown';                                        // consumer_unknown and anything new
}

// ─── The writer ────────────────────────────────────────────────────────────

const INSERT_SQL = `INSERT OR IGNORE INTO events (
  event_id, dedupe_key, schema_version, event_name, level,
  occurred_at_ms, reporting_day, ingested_at_ms, environment, source_service,
  external_event_id, user_id, job_id, request_id, queue_message_id,
  provider, provider_job_id, attempt, style, requested_size,
  final_size, outcome, provider_status, error_code, failure_stage,
  http_status, retryable, refund_expected, latency_ms, queue_wait_ms,
  amount_minor, currency, units_delta, caused_by_event_id, event_json, event_sha256
) VALUES (
  ?1, ?2, ?3, ?4, ?5,
  ?6, ?7, ?8, ?9, ?10,
  ?11, ?12, ?13, ?14, ?15,
  ?16, ?17, ?18, ?19, ?20,
  ?21, ?22, ?23, ?24, ?25,
  ?26, ?27, ?28, ?29, ?30,
  ?31, ?32, ?33, ?34, ?35, ?36
)`;

/** Envs already warned about a missing EVENTS_DB binding. Keyed on the env
 *  object so the warning fires once per isolate rather than once per event. */
const warnedMissingBinding = new WeakSet<object>();

function bool01(v: boolean | undefined): number | null {
  return v === undefined ? null : v ? 1 : 0;
}

/**
 * Write one event. Returns the row's event_id on success (on a dedupe hit,
 * the id of the row that already holds this dedupe_key, so causedByEventId
 * chains stay honest across redeliveries), or null on any failure. Never
 * throws; never blocks a job on D1 being slow beyond the single insert.
 */
export async function recordEvent(
  env: Env,
  evt: LedgerEvent,
  log: Logger = defaultLogger
): Promise<string | null> {
  try {
    const db = env.EVENTS_DB as D1Database | undefined;
    if (!db) {
      if (!warnedMissingBinding.has(env)) {
        warnedMissingBinding.add(env);
        log('warn', 'ledger write failed', {
          eventName: evt.eventName,
          dedupeKey: evt.dedupeKey,
          error: 'EVENTS_DB binding is undefined on this deploy',
        });
      }
      return null;
    }

    const eventId = crypto.randomUUID();
    const occurredAtMs = evt.occurredAtMs ?? Date.now();
    const ingestedAtMs = Date.now();
    const day = reportingDay(occurredAtMs);
    const environment = typeof env.APP_ENV === 'string' && env.APP_ENV ? env.APP_ENV : 'unknown';

    // Everything, typed columns included, in one object: the row's typed
    // columns are a projection of this, never the other way round.
    const canonical = {
      schemaVersion: SCHEMA_VERSION,
      eventId,
      dedupeKey: evt.dedupeKey,
      eventName: evt.eventName,
      level: evt.level,
      occurredAtMs,
      occurredAt: new Date(occurredAtMs).toISOString(),
      reportingDay: day,
      ingestedAtMs,
      environment,
      sourceService: SOURCE_SERVICE,
      userId: evt.userId,
      jobId: evt.jobId,
      requestId: evt.requestId,
      queueMessageId: evt.queueMessageId,
      provider: evt.provider,
      providerJobId: evt.providerJobId,
      attempt: evt.attempt,
      style: evt.style,
      requestedSize: evt.requestedSize,
      finalSize: evt.finalSize,
      outcome: evt.outcome,
      providerStatus: evt.providerStatus,
      errorCode: evt.errorCode,
      failureStage: evt.failureStage,
      httpStatus: evt.httpStatus,
      retryable: evt.retryable,
      refundExpected: evt.refundExpected,
      latencyMs: evt.latencyMs,
      queueWaitMs: evt.queueWaitMs,
      unitsDelta: evt.unitsDelta,
      causedByEventId: evt.causedByEventId,
      extra: evt.extra,
    };
    const eventJson = stableStringify(canonical);
    const eventSha256 = await sha256Hex(eventJson);

    const result = await db
      .prepare(INSERT_SQL)
      .bind(
        eventId, evt.dedupeKey, SCHEMA_VERSION, evt.eventName, evt.level,
        occurredAtMs, day, ingestedAtMs, environment, SOURCE_SERVICE,
        null, evt.userId ?? null, evt.jobId ?? null, evt.requestId ?? null, evt.queueMessageId ?? null,
        evt.provider ?? null, evt.providerJobId ?? null, evt.attempt ?? null, evt.style ?? null, evt.requestedSize ?? null,
        evt.finalSize ?? null, evt.outcome ?? null, evt.providerStatus ?? null, evt.errorCode ?? null, evt.failureStage ?? null,
        evt.httpStatus ?? null, bool01(evt.retryable), bool01(evt.refundExpected), evt.latencyMs ?? null, evt.queueWaitMs ?? null,
        null, null, evt.unitsDelta ?? null, evt.causedByEventId ?? null, eventJson, eventSha256
      )
      .run();

    if (result.meta.changes === 0) {
      // Dedupe hit: a previous delivery already wrote this key. Return its
      // id so a follow-on causedByEventId points at the row that exists.
      const existing = await db
        .prepare('SELECT event_id FROM events WHERE dedupe_key = ?1')
        .bind(evt.dedupeKey)
        .first<{ event_id: string }>();
      return existing?.event_id ?? null;
    }
    return eventId;
  } catch (err) {
    log('warn', 'ledger write failed', {
      eventName: evt.eventName,
      dedupeKey: evt.dedupeKey,
      error: err instanceof Error ? err.message : String(err),
    });
    return null;
  }
}
