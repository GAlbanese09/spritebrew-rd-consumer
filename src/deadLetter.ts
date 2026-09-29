// spritebrew-rd-consumer/src/deadLetter.ts
//
// Consumer for the dead-letter queues (no-silent-failures.md 001 as amended
// by 002). A message lands here after the main queue gave up on it, usually
// because its invocation threw before or after the running write. Every such
// job must end delivered or refunded:
//
//   1. Validate the three fields this handler uses (jobId, userId, tokenCost).
//      Nothing else in the message is read: a dead letter may be dead because
//      it is malformed (a missing `body` throws in handleMessage).
//   2. Delivered? Strong stores first, then the permanent ones. Any positive:
//      ack, no refund.
//   3. Already refunded? A D1 generation.refunded row, or an error record
//      with refunded: true. Ack.
//   4. A running record younger than 40 minutes may be a live duplicate
//      delivery still working, or one the sweep is about to refund: retry the
//      message once that age is reached.
//   5. Money paused (n1-ledger 008 ruling A): no refund and no ack. Retry
//      every 900 s. The last paused delivery acks only once the alarm row
//      (reason 'paused') is proven written. If it is not, the debt goes into
//      the job record as `refundDue` with a 24-hour TTL, which the sweep
//      settles after the unpause; if that write fails too, the loss is
//      logged at error level (n1-ledger-02.md 002 ruling A).
//   6. Refund, then the D1 rows, then the terminal record, in recordFailure's
//      order, the record through writeStateUnlessTerminal.
//
// A throw retries through the queue (max_retries 5, retry_delay 60 s, no DLQ
// of its own). The last delivery writes an alarm instead of vanishing: an
// error log and a D1 generation.unrefunded row, which the digest lists.

import type { Env, JobMessage, JobMode, JobStateError, JobStateRunning } from './types';
import { refundTokens } from './refund';
import { recordEvent, recordEventOrThrow, stageForErrorCode } from './events';
import { DEBT_TTL_S, readJobState, writeStateUnlessTerminal } from './jobState';
import { devFaults, isMoneyPaused, pausedRetryDelayS } from './moneyPause';

/** Exact names, per environment. No prefix match: a future queue whose name
 *  starts the same way must never be refunded by accident. */
export const DEAD_LETTER_QUEUES: ReadonlySet<string> = new Set([
  'spritebrew-rd-jobs-dlq',
  'spritebrew-rd-jobs-dlq-dev',
]);

/** max_retries = 5 on the dead-letter consumers in wrangler.toml. Queues
 *  delivers max_retries + 1 times (the main queue, max_retries 3, shows
 *  attempt 4 in the ledger), so the last delivery is attempt 6. */
const DLQ_LAST_ATTEMPT = 6;

/** The sweep's 20-minute threshold, plus one 15-minute cron, plus 5. At 20 the
 *  handler would fire on the sweep's own boundary, and refundTokens' guard is
 *  a KV read then a write, so both could refund one job in the same seconds.
 *  At 40 the sweep has always refunded a stale running job first (the handler
 *  then finds it refunded and acks); the handler still refunds a running
 *  record the sweep skips (one without tokenCost). */
const RUNNING_GRACE_MS = 40 * 60 * 1000;

/** Older than this, a dead letter is not auto-refunded: an alarm row instead.
 *  Every auto-refund then stays inside the life of the refund key, including
 *  keys written with the old 7-day TTL, so "no evidence of a refund" still
 *  means "not refunded". Past it the evidence may have expired. */
const STALE_AFTER_MS = 6 * 24 * 60 * 60 * 1000;

/** Largest price today is 50 (styleRegistry.ts getTokenCost). */
const MAX_TOKEN_COST = 50;

const ERROR_CODE = 'dead_lettered';
const PROVIDER = 'retro-diffusion';

type Logger = (
  level: 'info' | 'warn' | 'error',
  message: string,
  extra?: Record<string, unknown>
) => void;

interface ValidFields {
  jobId: string;
  userId: string;
  tokenCost: number;
}

function validate(body: unknown): ValidFields | null {
  const b = (body ?? {}) as Partial<JobMessage>;
  const jobId = typeof b.jobId === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(b.jobId) ? b.jobId : null;
  const userId = typeof b.userId === 'string' && /^user_[A-Za-z0-9]{10,64}$/.test(b.userId) ? b.userId : null;
  const tokenCost =
    typeof b.tokenCost === 'number' && Number.isInteger(b.tokenCost) && b.tokenCost >= 1 && b.tokenCost <= MAX_TOKEN_COST
      ? b.tokenCost
      : null;
  if (!jobId || !userId || tokenCost === null) return null;
  return { jobId, userId, tokenCost };
}

async function d1HasRow(env: Env, jobId: string, eventNames: string[]): Promise<boolean> {
  if (!env.EVENTS_DB) return false;
  try {
    const placeholders = eventNames.map(() => '?').join(', ');
    const row = await env.EVENTS_DB
      .prepare(`SELECT 1 FROM events WHERE job_id = ? AND event_name IN (${placeholders}) LIMIT 1`)
      .bind(jobId, ...eventNames)
      .first();
    return row !== null;
  } catch {
    // Best effort: D1 is observability. Its absence proves nothing.
    return false;
  }
}

async function galleryKeyExists(env: Env, userId: string, jobId: string): Promise<boolean> {
  let cursor: string | undefined;
  for (let page = 0; page < 20; page++) {
    const res = await env.SPRITEBREW_KV.list({ prefix: `gen:${userId}:`, cursor });
    if (res.keys.some((k) => k.name.endsWith(`:${jobId}`))) return true;
    if (res.list_complete) return false;
    cursor = res.cursor;
  }
  return false;
}

/** Delivered, in order: R2 job record, KV job record, D1 terminal-success
 *  row, the gallery PNG in R2, the gen: index last. First positive wins. */
async function deliveredBy(
  env: Env,
  f: ValidFields,
  record: Awaited<ReturnType<typeof readJobState>>
): Promise<string | null> {
  if (record?.state.status === 'success') return `${record.source}_record`;
  // readJobState stops at R2 when it holds the record; a KV success written
  // after an older R2 copy is still checked here.
  if (record?.source === 'r2') {
    const raw = await env.SPRITEBREW_KV.get(`job:${f.jobId}`);
    if (raw && (JSON.parse(raw) as { status?: string }).status === 'success') return 'kv_record';
  }
  if (await d1HasRow(env, f.jobId, ['generation.succeeded', 'generation.rescued'])) return 'd1_row';
  if (await env.GALLERY_BUCKET.head(`${f.userId}/${f.jobId}.png`)) return 'gallery_png';
  if (await galleryKeyExists(env, f.userId, f.jobId)) return 'gen_key';
  return null;
}

async function alarm(
  env: Env,
  msg: Message<JobMessage>,
  reason: string,
  f: Partial<ValidFields>,
  log: Logger,
  detail?: string
): Promise<boolean> {
  const b = (msg.body ?? {}) as Partial<JobMessage>;
  const jobId = typeof b.jobId === 'string' ? b.jobId : undefined;
  // Evidence for settling by hand (n1-ledger 007 section 4): does KV hold the
  // refund key, and does D1 hold a generation.refunded row (evidence, not
  // proof). Only for a job id that passed validation.
  let legacyRefundEvidence: string | undefined;
  let d1RefundedEvent: boolean | undefined;
  if (f.jobId) {
    try {
      legacyRefundEvidence = (await env.SPRITEBREW_KV.get(`token_idempotency:refund:${f.jobId}`)) ? 'kv:present' : 'kv:absent';
    } catch {
      legacyRefundEvidence = 'kv:unreadable';
    }
    d1RefundedEvent = await d1HasRow(env, f.jobId, ['generation.refunded']);
  }
  // console.error with the fields is the record of last resort if D1 is the
  // thing failing.
  console.error(JSON.stringify({
    level: 'error',
    message: 'dead letter not refunded',
    source: 'dead-letter',
    reason,
    jobId,
    userId: typeof b.userId === 'string' ? b.userId : undefined,
    tokenCost: b.tokenCost,
    attempt: msg.attempts,
    detail,
    legacyRefundEvidence,
    d1RefundedEvent,
  }));
  // The row is the proof the digest lists, so this write is strict: the
  // caller learns whether it landed (n1-ledger-02.md 002 ruling A). The dev
  // fault 'events_db_absent' makes it behave as an unbound EVENTS_DB.
  const eventsEnv = (await devFaults(env)).includes('events_db_absent') ? { ...env, EVENTS_DB: undefined } : env;
  try {
    await recordEventOrThrow(eventsEnv as Env, {
      eventName: 'generation.unrefunded',
      level: 'error',
      dedupeKey: `${jobId ?? `msg:${msg.id}`}:generation.unrefunded`,
      userId: f.userId,
      jobId,
      queueMessageId: msg.id,
      attempt: msg.attempts,
      errorCode: ERROR_CODE,
      extra: { reason, tokenCost: b.tokenCost, detail: detail?.slice(0, 500), legacyRefundEvidence, d1RefundedEvent },
    });
    return true;
  } catch (err) {
    log('error', 'alarm row not written', { reason, error: err instanceof Error ? err.message : String(err) });
    return false;
  }
}

export async function handleDeadLetter(msg: Message<JobMessage>, env: Env): Promise<void> {
  const attempt = msg.attempts ?? 1;
  const b = (msg.body ?? {}) as Partial<JobMessage>;
  const log: Logger = (level, message, extra = {}) => {
    console[level](JSON.stringify({
      level, message, source: 'dead-letter',
      jobId: typeof b.jobId === 'string' ? b.jobId : undefined,
      attempt, ...extra,
    }));
  };

  const f = validate(msg.body);
  if (!f) {
    // Malformed: never refunded, never retried (it cannot become valid).
    await alarm(env, msg, 'invalid_fields', {}, log);
    msg.ack();
    return;
  }

  try {
    const record = await readJobState(env, f.jobId);

    const delivered = await deliveredBy(env, f, record);
    if (delivered) {
      log('info', 'dead letter already delivered; no refund', { by: delivered });
      msg.ack();
      return;
    }

    const refundedRecord = record?.state.status === 'error' && record.state.refunded === true;
    if (refundedRecord || (await d1HasRow(env, f.jobId, ['generation.refunded']))) {
      log('info', 'dead letter already refunded', { by: refundedRecord ? `${record?.source}_record` : 'd1_row' });
      msg.ack();
      return;
    }

    // Too old to trust the absence of refund evidence: alarm, never refund.
    const now = Date.now();
    const sentAtMs =
      typeof b.enqueuedAt === 'number' && Number.isFinite(b.enqueuedAt) && b.enqueuedAt <= now
        ? b.enqueuedAt
        : msg.timestamp.getTime();
    const messageAgeMs = now - sentAtMs;
    if (messageAgeMs > STALE_AFTER_MS) {
      await alarm(env, msg, 'stale_message', f, log, `age ${(messageAgeMs / 3_600_000).toFixed(1)} h`);
      msg.ack();
      return;
    }

    if (record?.state.status === 'running') {
      const ageMs = Date.now() - record.state.startedAt;
      if (ageMs < RUNNING_GRACE_MS) {
        const delaySeconds = Math.ceil((RUNNING_GRACE_MS - ageMs) / 1000);
        log('info', 'running record younger than 40 min; retrying later, no refund', { ageMs, delaySeconds });
        msg.retry({ delaySeconds });
        return;
      }
    }

    const mode: JobMode =
      b.mode === 'create' || b.mode === 'animate' ? b.mode : record?.state.mode ?? 'create';

    // Money paused: no refund, and no ack without a settlement. A retry at
    // the last delivery deletes the message (there is no queue behind this
    // one), so the last paused delivery acks only when the alarm row is
    // proven, and otherwise leaves the debt in the job record first.
    if (await isMoneyPaused(env)) {
      if (attempt >= DLQ_LAST_ATTEMPT) {
        if (await alarm(env, msg, 'paused', f, log, 'money paused through the last dead-letter delivery')) {
          msg.ack();
          return;
        }
        const now = Date.now();
        const owed: JobStateRunning = {
          status: 'running',
          userId: f.userId,
          mode,
          enqueuedAt: typeof b.enqueuedAt === 'number' ? b.enqueuedAt : now,
          startedAt: record?.state.status === 'running' ? record.state.startedAt : now,
          attempt,
          tokenCost: f.tokenCost,
          refundDue: { errorCode: ERROR_CODE, at: now, error: 'This generation could not be started.' },
        };
        let kept = false;
        try {
          kept = await writeStateUnlessTerminal(env, `job:${f.jobId}`, owed, log, DEBT_TTL_S);
        } catch (err) {
          log('error', 'refund-due record write failed', { error: err instanceof Error ? err.message : String(err) });
        }
        if (kept) {
          log('warn', 'alarm row not written; debt kept as a refund-due record (24 h) for the sweep', {
            tokenCost: f.tokenCost,
          });
        } else {
          // Neither the row nor the record exists: this line is the only
          // evidence left. Every field the alarm carries.
          console.error(JSON.stringify({
            level: 'error',
            message: 'dead letter lost unsettled: no alarm row and no refund-due record',
            source: 'dead-letter',
            reason: 'paused',
            jobId: f.jobId,
            userId: f.userId,
            tokenCost: f.tokenCost,
            mode,
            attempt,
            queueMessageId: msg.id,
            recordStatus: record?.state.status ?? 'absent',
          }));
        }
        msg.retry();
        return;
      }
      const delaySeconds = await pausedRetryDelayS(env);
      log('warn', 'money paused; dead letter held, no refund', { delaySeconds });
      msg.retry({ delaySeconds });
      return;
    }

    // Refund, then the D1 rows, then the terminal record (recordFailure's order).
    const refundResult = await refundTokens(env.SPRITEBREW_KV, f.userId, f.tokenCost, f.jobId, { mode });
    log('info', 'dead letter refunded', {
      alreadyApplied: refundResult.alreadyApplied,
      newBalance: refundResult.newBalance,
      recordStatus: record?.state.status ?? 'absent',
    });

    const failedEventId = await recordEvent(env, {
      eventName: 'generation.failed',
      level: 'error',
      dedupeKey: `${f.jobId}:generation.terminal`,
      userId: f.userId,
      jobId: f.jobId,
      queueMessageId: msg.id,
      attempt,
      provider: PROVIDER,
      outcome: 'failed',
      errorCode: ERROR_CODE,
      failureStage: stageForErrorCode(ERROR_CODE),
      retryable: false,
      refundExpected: true,
      extra: { mode, recordStatus: record?.state.status ?? 'absent' },
    }, log);
    await recordEvent(env, {
      eventName: 'generation.refunded',
      level: 'info',
      dedupeKey: `${f.jobId}:generation.refunded`,
      userId: f.userId,
      jobId: f.jobId,
      unitsDelta: f.tokenCost,
      causedByEventId: failedEventId ?? undefined,
      extra: { alreadyApplied: refundResult.alreadyApplied, newBalance: refundResult.newBalance },
    }, log);

    const errorState: JobStateError = {
      status: 'error',
      userId: f.userId,
      mode,
      enqueuedAt: typeof b.enqueuedAt === 'number' ? b.enqueuedAt : Date.now(),
      failedAt: Date.now(),
      // The client appends its own refund sentence when refunded is true.
      error: 'This generation could not be started.',
      errorCode: ERROR_CODE,
      attempts: attempt,
      refunded: true,
    };
    await writeStateUnlessTerminal(env, `job:${f.jobId}`, errorState, log);

    msg.ack();
  } catch (err) {
    const detail = err instanceof Error ? err.message : String(err);
    if (attempt >= DLQ_LAST_ATTEMPT) {
      // No DLQ behind this queue: a retry now would delete the message. Leave
      // an alarm the digest lists, then let it go.
      await alarm(env, msg, 'retries_exhausted', f, log, detail);
      msg.ack();
      return;
    }
    log('warn', 'dead letter handling threw; retrying', { error: detail.slice(0, 300) });
    msg.retry();
  }
}
