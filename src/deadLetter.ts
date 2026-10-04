// spritebrew-rd-consumer/src/deadLetter.ts
//
// Consumer for the dead-letter queues, on the D1 ledger (n1-release-2-spec.md
// revision 9, 5.4; no-silent-failures.md 001 as amended by 002). A message
// lands here after the main queue gave up on it. Every such job must end
// delivered or refunded, and the `jobs` row decides:
//
//   1. Validate the three fields this handler uses (jobId, userId, tokenCost);
//      a malformed message is alarmed and acked, never refunded.
//   2. Older than six days: alarm, never refund (kept in its place, before
//      the row's actions).
//   3. One read of the row, then:
//      - no row: the tombstone and its alarm; paused: retry 900 s, and at the
//        last delivery the `no_record` alarm (with the message's tokenCost)
//        and ack, as release 1 did;
//      - held: ack (its `hold` alarm exists);
//      - finished: ack, after the repair pass for that row;
//      - staged, unfinished: c-finalize, then 4.9's finish; not won (owner
//        live, or the migrator's phase open): retry with the remaining grace;
//      - a live claim (not stale), or COALESCE(lease, created) within 40
//        minutes: retry with the remaining grace; at the last delivery, ack
//        with a log (D7, a **Proposal**): the row is the durable debt and the
//        sweep settles it;
//      - an imported (`kv`) row: the delivered checks (4.12);
//      - otherwise: 4.8 `'canceller'`, `dead_lettered`, then its r5 branch.
//   4. Paused at the last delivery: ack once the `generation.unrefunded` alarm
//      row is proven written; if it is not, retry, which deletes the message
//      (no queue behind it), and the row stays the kept debt for the sweep.
//   5. A throw retries; at the last delivery, the `retries_exhausted` alarm
//      and ack: a debited job's row outlives the message.
//
// The handler's `generation.failed` carries the style from the job's debit
// ledger row, else the message, and `requested_size` as `WxH` from the debit's
// `meta_json`, else the message's width and height; unknown when neither has
// both, never a square (`L3 007` ruling 2; the digest audit's finding 4).

import type { Env, JobMessage } from './types';
import { recordEventOrThrow } from './events';
import { devFaults, isMoneyPaused } from './moneyPause';
import * as L from './ledger';
import { refundTokens } from './refund';
import {
  ACK, PAUSED_RETRY_S, afterCanceller, debitContext, deliveredChecks, finishStaged, isStale, ledgerAlarm, ledgerCtx,
  messageContext, readJobRow, repairOne, retryIn, tombstoneJob, unrefundedAlarmStrict, type JobRow, type Next,
} from './settle';

/** Exact names, per environment. No prefix match: a future queue whose name
 *  starts the same way must never be refunded by accident. */
export const DEAD_LETTER_QUEUES: ReadonlySet<string> = new Set([
  'spritebrew-rd-jobs-dlq',
  'spritebrew-rd-jobs-dlq-dev',
]);

/** max_retries = 5 on the dead-letter consumers in wrangler.toml: the last
 *  delivery is attempt 6. */
const DLQ_LAST_ATTEMPT = 6;

/** A live claim, or a row this young, gets the remaining grace: the sweep's
 *  20-minute threshold, one 15-minute cron, plus 5. */
const RUNNING_GRACE_MS = 40 * 60 * 1000;
const MAX_GRACE_S = 2400;

/** Older than this, a dead letter is not auto-refunded: an alarm row instead. */
const STALE_AFTER_MS = 6 * 24 * 60 * 60 * 1000;

/** Largest price today is 50 (styleRegistry.ts getTokenCost). */
const MAX_TOKEN_COST = 50;

const ERROR_CODE = 'dead_lettered';

type Logger = (level: 'info' | 'warn' | 'error', message: string, extra?: Record<string, unknown>) => void;

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

/** The strict alarm for a message that ends unsettled: an error line, and a
 *  `generation.unrefunded` row the digest lists. Answers whether the row
 *  landed. The dev fault 'events_db_absent' makes it behave as an unbound
 *  EVENTS_DB. */
async function alarm(env: Env, msg: Message<JobMessage>, reason: string, f: Partial<ValidFields>, log: Logger, detail?: string): Promise<boolean> {
  const b = (msg.body ?? {}) as Partial<JobMessage>;
  const jobId = typeof b.jobId === 'string' ? b.jobId : undefined;
  console.error(JSON.stringify({
    level: 'error', message: 'dead letter not settled', source: 'dead-letter', reason, jobId,
    userId: typeof b.userId === 'string' ? b.userId : undefined, tokenCost: b.tokenCost, attempt: msg.attempts, detail,
  }));
  const eventsEnv = (await devFaults(env)).includes('events_db_absent') ? { ...env, EVENTS_DB: undefined } : env;
  if (jobId && f.userId) {
    return unrefundedAlarmStrict(eventsEnv as Env, jobId, f.userId, { reason, tokenCost: b.tokenCost, detail: detail?.slice(0, 500) }, msg.id, msg.attempts ?? 1);
  }
  try {
    await recordEventOrThrow(eventsEnv as Env, {
      eventName: 'generation.unrefunded',
      level: 'error',
      dedupeKey: `${jobId ?? `msg:${msg.id}`}:generation.unrefunded`,
      jobId,
      queueMessageId: msg.id,
      attempt: msg.attempts,
      errorCode: ERROR_CODE,
      extra: { reason, tokenCost: b.tokenCost, detail: detail?.slice(0, 500) },
    });
    return true;
  } catch (err) {
    log('error', 'alarm row not written', { reason, error: err instanceof Error ? err.message : String(err) });
    return false;
  }
}

function apply(msg: Message<JobMessage>, next: Next): void {
  if ('ack' in next) msg.ack();
  else if (next.retry === null) msg.retry();
  else msg.retry({ delaySeconds: next.retry });
}

/** The remaining grace, 1 to 2,400 s. */
function graceS(row: JobRow, now: number): number {
  const from = row.lease_at_ms ?? row.created_at_ms;
  return Math.min(MAX_GRACE_S, Math.max(1, Math.ceil((from + RUNNING_GRACE_MS - now) / 1000)));
}

export async function handleDeadLetter(msg: Message<JobMessage>, env: Env): Promise<void> {
  const attempt = msg.attempts ?? 1;
  const b = (msg.body ?? {}) as Partial<JobMessage>;
  const log: Logger = (level, message, extra = {}) => {
    console[level](JSON.stringify({ level, message, source: 'dead-letter', jobId: typeof b.jobId === 'string' ? b.jobId : undefined, attempt, ...extra }));
  };

  const f = validate(msg.body);
  if (!f) {
    await alarm(env, msg, 'invalid_fields', {}, log);
    msg.ack();
    return;
  }
  try {
    const now = Date.now();
    const sentAtMs = typeof b.enqueuedAt === 'number' && Number.isFinite(b.enqueuedAt) && b.enqueuedAt <= now ? b.enqueuedAt : msg.timestamp.getTime();
    if (now - sentAtMs > STALE_AFTER_MS) {
      await alarm(env, msg, 'stale_message', f, log, `age ${((now - sentAtMs) / 3_600_000).toFixed(1)} h`);
      msg.ack();
      return;
    }
    const next = await deadLetterRow(msg, env, f, log);
    const last = attempt >= DLQ_LAST_ATTEMPT;
    if (last && 'retry' in next && next.retry === PAUSED_RETRY_S && (await isMoneyPaused(env))) {
      // Paused through the last delivery: ack only once the alarm row is
      // proven; else retry, which deletes the message, and the row stays the
      // kept debt for the sweep. The pause is read again here: a grace retry
      // that happens to be 900 s is not a pause.
      if (await alarm(env, msg, 'paused', f, log, 'money paused through the last dead-letter delivery')) {
        msg.ack();
      } else {
        log('warn', 'alarm row not written; the jobs row is the kept debt for the sweep');
        msg.retry();
      }
      return;
    }
    apply(msg, next);
  } catch (err) {
    const detail = err instanceof Error ? err.message : String(err);
    if (attempt >= DLQ_LAST_ATTEMPT) {
      await alarm(env, msg, 'retries_exhausted', f, log, detail);
      msg.ack();
      return;
    }
    log('warn', 'dead letter handling threw; retrying', { error: detail.slice(0, 300) });
    msg.retry();
  }
}

async function deadLetterRow(msg: Message<JobMessage>, env: Env, f: ValidFields, log: Logger): Promise<Next> {
  const attempt = msg.attempts ?? 1;
  const last = attempt >= DLQ_LAST_ATTEMPT;
  const ctx = ledgerCtx(env);
  const ev = { attempt, queueMessageId: msg.id, ctx: { ...messageContext(msg.body), ...(await debitContext(env, f.jobId)) } };

  let row = await readJobRow(env, f.jobId);
  if (!row) {
    const t = await tombstoneJob(env, msg.body, log);
    if (t === 'tombstoned') return ACK;
    if (t === 'paused') {
      if (!last) return retryIn(PAUSED_RETRY_S);
      await ledgerAlarm(env, 'no_record', f.jobId, { tokenCost: f.tokenCost, paused: true }, log, f.userId);
      return ACK;
    }
    if (t === 'error') return retryIn(null);
    row = await readJobRow(env, f.jobId);
    if (!row) return retryIn(null);
  }
  if (row.hold_reason !== null) return ACK;
  const now = Date.now();
  if (row.finished_at_ms !== null) {
    const list = await L.repairList(ctx);
    if (list.outcome === 'phase_open') return retryIn(graceS(row, now));
    await repairOne(env, row as unknown as Record<string, unknown>, log);
    return ACK;
  }
  if (row.artifact === 'staged') {
    const claim = crypto.randomUUID();
    const c = await L.claimJob(ctx, 'finalize', { job: f.jobId, claim, attempt });
    if (c.outcome === 'won') return finishStaged(env, row, claim, ev, log);
    if (c.outcome === 'held') return ACK;
    return retryIn(graceS(row, now));
  }
  const young = (row.lease_at_ms ?? row.created_at_ms) > now - RUNNING_GRACE_MS;
  if ((row.claim_id !== null && !isStale(row, now)) || young) {
    if (last) {
      log('info', 'a live claim or a young row at the last delivery: acked; the row is the debt the sweep settles', { grace: graceS(row, now) });
      return ACK;
    }
    return retryIn(graceS(row, now));
  }
  if (row.provenance === 'kv') return deliveredChecks(env, row, ev, log);
  const r = await refundTokens(env, { job: f.jobId, fence: 'canceller', code: ERROR_CODE }, log);
  if (r.outcome === 'live_owner') return last ? ACK : retryIn(graceS(row, now));
  return afterCanceller(env, row, r, ERROR_CODE, ev, log);
}
