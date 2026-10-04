// spritebrew-rd-consumer/src/settle.ts
//
// Release 2's settling paths, shared by the queue handler (5.2), the sweep
// (5.3, 4.12) and the dead-letter handler (5.4) (n1-release-2-spec.md
// revision 9). Money moves only through S1's library (src/ledger.ts); this
// file decides what follows each verified outcome:
//   - after a refund (4.8's r5): the events, the strict error status record,
//     then the marker (4.10); side effects only after a verified outcome (A9);
//   - the finalizer for a staged result (4.9) and the owner's failure path;
//   - the delivered checks for an imported row (4.12, O7);
//   - the repair pass and the overdue alarms (4.11);
//   - the alarm rows (O8).

import type { Env, JobMessage, JobMode } from './types';
import * as L from './ledger';
import { recordEvent, recordEventOrThrow, stageForErrorCode } from './events';
import { refundTokens } from './refund';
import { findIndexEntry, galleryR2Key, pngPresent, publishIndex } from './gallery';
import {
  bytesToBase64, metaFromIndexRow, parseMeta, putStatusStrict, refundedRecord, successRecord, unresolvedRecord,
  type ArtifactMeta,
} from './status';

export type Logger = (level: 'info' | 'warn' | 'error', message: string, extra?: Record<string, unknown>) => void;

export const PROVIDER = 'retro-diffusion';
export const PAUSED_RETRY_S = 900;

/** What a delivery does next. `retry` with no delay is the queue's own. */
export type Next = { ack: true } | { retry: number | null };
export const ACK: Next = { ack: true };
export const retryIn = (s: number | null): Next => ({ retry: s });

export function ledgerCtx(env: Env): L.LedgerCtx {
  if (!env.LEDGER_DB) throw new Error('LEDGER_DB binding missing');
  return { db: env.LEDGER_DB as unknown as L.LedgerDb, appEnv: env.APP_ENV, timeoutMs: 10_000 };
}

export interface JobRow {
  job_id: string;
  user_id: string;
  mode: JobMode;
  token_cost: number | null;
  provenance: 'd1' | 'kv' | 'tombstone';
  state: 'debited' | 'enqueued' | 'claimed' | 'finished';
  phase: 'primary' | 'fallback';
  claim_id: string | null;
  claim_attempt: number | null;
  lease_at_ms: number | null;
  released_at_ms: number | null;
  submitted_at_ms: number | null;
  task_id: string | null;
  refund_due_code: string | null;
  hold_reason: string | null;
  import_json: string | null;
  outcome: string | null;
  error_code: string | null;
  error_message: string | null;
  refunded_amount: number | null;
  artifact: 'none' | 'staged' | 'published' | 'discarded';
  artifact_meta_json: string | null;
  status_written_at_ms: number | null;
  created_at_ms: number;
  enqueued_at_ms: number | null;
  finished_at_ms: number | null;
}

/** The job's row, read once (5.2). A failed read throws: never read as
 *  absent. Not a spec statement: the library's batches each read back what
 *  they need; this is the delivery's own first look. */
export async function readJobRow(env: Env, jobId: string): Promise<JobRow | null> {
  if (!env.LEDGER_DB) throw new Error('LEDGER_DB binding missing');
  const r = await env.LEDGER_DB.prepare('SELECT * FROM jobs WHERE job_id = ?1').bind(jobId).first<JobRow>();
  return r ?? null;
}

/** 4.5's stale rule, in code, for the delivery's own branching. */
export function isStale(r: JobRow, now: number): boolean {
  if (r.claim_id === null || r.released_at_ms !== null) return true;
  const animateWaiting = r.mode === 'animate' && r.task_id === null && r.submitted_at_ms !== null;
  if (animateWaiting) return (r.submitted_at_ms as number) < now - 900_000;
  return (r.lease_at_ms ?? 0) < now - 300_000;
}

// ── Alarm rows (O8) ──

export type AlarmKind =
  | 'zero_alarm' | 'no_record' | 'debit_missing' | 'enqueued_never_claimed' | 'index_only' | 'hold'
  | 'repair_overdue' | 'unfinished_overdue' | 'unique_mismatch';

/** One `ledger.alarm` row, deduped by kind and subject. Best effort, never
 *  money proof (`S2 010` 6): the debt lives in a jobs or ledger row. */
export async function ledgerAlarm(env: Env, kind: AlarmKind, subject: string, fields: Record<string, unknown>, log: Logger, userId?: string): Promise<void> {
  await recordEvent(env, {
    eventName: 'ledger.alarm',
    level: 'error',
    dedupeKey: `${kind}:${subject}`,
    userId,
    jobId: kind === 'zero_alarm' ? undefined : subject,
    errorCode: kind,
    extra: fields,
  }, log);
}

/** The tombstone's alarm carries what George needs (`L 006` point 5). */
async function noRecordAlarm(env: Env, jobId: string, userId: string, tokenCost: number | null, log: Logger): Promise<void> {
  let legacyRefundKey: string = 'unknown';
  let d1Refunded: boolean | null = null;
  try {
    const r = await env.LEDGER_DB!.prepare("SELECT 1 AS x FROM legacy_idem WHERE key = 'token_idempotency:refund:' || ?1").bind(jobId).first();
    legacyRefundKey = r ? 'present' : 'absent';
  } catch {
    legacyRefundKey = 'unreadable';
  }
  try {
    d1Refunded = !!(await env.EVENTS_DB.prepare("SELECT 1 FROM events WHERE job_id = ?1 AND event_name = 'generation.refunded' LIMIT 1").bind(jobId).first());
  } catch {
    d1Refunded = null;
  }
  await ledgerAlarm(env, 'no_record', jobId, { legacyRefundKey, d1Refunded, tokenCost }, log, userId);
}

// ── The debit's style and size, for the events (S3; the digest audit's finding 4) ──

export interface EventContext {
  style?: string;
  requestedSize?: string;
}

/** The style from the job's debit ledger row and `requested_size` as `WxH`
 *  from its `meta_json` width and height (4.3); none for a row without them
 *  (an imported row, a tombstone), never a square. */
export async function debitContext(env: Env, jobId: string): Promise<EventContext> {
  try {
    const r = await env.LEDGER_DB!.prepare("SELECT style, meta_json FROM ledger WHERE idem_key = 'debit:' || ?1").bind(jobId).first<{ style: string | null; meta_json: string | null }>();
    if (!r) return {};
    let size: string | undefined;
    try {
      const m = r.meta_json ? JSON.parse(r.meta_json) : null;
      if (m && Number.isInteger(m.width) && Number.isInteger(m.height)) size = `${m.width}x${m.height}`;
    } catch {
      size = undefined;
    }
    return { ...(r.style ? { style: r.style } : {}), ...(size ? { requestedSize: size } : {}) };
  } catch {
    return {};
  }
}

/** A message's own style and size (the dead-letter handler's fallback, and
 *  the owner's), with both dimensions or none. */
export function messageContext(body: Partial<JobMessage> | undefined): EventContext {
  const b = (body?.body ?? {}) as { prompt_style?: unknown; width?: unknown; height?: unknown };
  const style = typeof b.prompt_style === 'string' ? b.prompt_style : undefined;
  const size = Number.isInteger(b.width) && Number.isInteger(b.height) ? `${b.width}x${b.height}` : undefined;
  return { ...(style ? { style } : {}), ...(size ? { requestedSize: size } : {}) };
}

// ── After a verified refund ──

export interface RefundEventInput {
  attempt?: number;
  queueMessageId?: string;
  ctx: EventContext;
  /** Owner paths: the run's latency. */
  latencyMs?: number;
  /** Sweep rows: the row's age, apart from its latency (finding 6). */
  ageMs?: number;
  retryable?: boolean;
  errMsg?: string;
}

/** The events, the strict error status record, the marker (A9: side effects
 *  only after refunded, refunded before the switch, or finished refunded).
 *  A failed status write leaves the marker NULL for the repair pass; the
 *  money outcome stands. */
export async function afterRefund(env: Env, jobId: string, res: L.RefundResult, code: string, ev: RefundEventInput, log: Logger): Promise<void> {
  const row = res.row as Record<string, unknown> | null;
  const job = await readJobRow(env, jobId);
  const userId = String(job?.user_id ?? row?.user_id ?? '');
  const failedEventId = await recordEvent(env, {
    eventName: 'generation.failed',
    level: 'error',
    dedupeKey: `${jobId}:generation.terminal`,
    userId,
    jobId,
    attempt: ev.attempt,
    queueMessageId: ev.queueMessageId,
    provider: PROVIDER,
    ...(ev.ctx.style ? { style: ev.ctx.style } : {}),
    ...(ev.ctx.requestedSize ? { requestedSize: ev.ctx.requestedSize } : {}),
    outcome: 'failed',
    errorCode: code,
    failureStage: stageForErrorCode(code),
    retryable: ev.retryable ?? false,
    refundExpected: true,
    ...(ev.latencyMs !== undefined ? { latencyMs: ev.latencyMs } : {}),
    extra: {
      mode: job?.mode, ledgerOutcome: res.outcome,
      ...(ev.ageMs !== undefined ? { ageMs: ev.ageMs } : {}),
      ...(ev.ctx.requestedSize ? {} : { requestedSize: 'unknown' }),
      ...(ev.errMsg ? { errMsg: ev.errMsg.slice(0, 500) } : {}),
    },
  }, log);
  if (res.outcome === 'refunded') {
    await recordEvent(env, {
      eventName: 'generation.refunded',
      level: 'info',
      dedupeKey: `${jobId}:generation.refunded`,
      userId,
      jobId,
      unitsDelta: res.amount ?? undefined,
      causedByEventId: failedEventId ?? undefined,
      extra: { ledger: 'spritebrew-ledger', balance: (row?.balance as number | undefined) ?? null },
    }, log);
  }
  if (job) await writeTerminalStatus(env, job, log);
}

/** The terminal status record for a finished row, strictly, then the marker.
 *  Answers whether the marker is set. Shared by every settling path and the
 *  repair pass (4.11's table). */
export async function writeTerminalStatus(env: Env, job: JobRow, log: Logger): Promise<boolean> {
  if (job.finished_at_ms === null) return false;
  try {
    if (job.outcome === 'refunded' || job.outcome === 'refunded_legacy') {
      await putStatusStrict(env, job.job_id, refundedRecord({
        userId: job.user_id, mode: job.mode, enqueuedAt: job.enqueued_at_ms ?? job.created_at_ms,
        errorCode: job.error_code, error: job.error_message, refundedAmount: job.refunded_amount,
      }), log);
    } else if (job.outcome === 'no_record') {
      await putStatusStrict(env, job.job_id, unresolvedRecord(job.user_id, job.mode, job.created_at_ms), log);
    } else if (job.outcome === 'succeeded' || job.outcome === 'rescued') {
      const repair = repairRecordOf(job);
      if (repair) {
        await putStatusStrict(env, job.job_id, repair as never, log);
      } else {
        // Rebuilt from the R2 PNG and artifact_meta_json (4.11, R4-10).
        const meta = parseMeta(job.artifact_meta_json, job.finished_at_ms);
        const obj = await env.GALLERY_BUCKET.get(galleryR2Key(job.user_id, job.job_id));
        if (!obj) throw new Error('the staged PNG is absent; no success record to rebuild');
        const bytes = new Uint8Array(await obj.arrayBuffer());
        await putStatusStrict(env, job.job_id, successRecord(job.user_id, meta, bytesToBase64(bytes)), log);
      }
    } else {
      return false;
    }
  } catch (err) {
    log('warn', 'terminal status write failed; the marker stays NULL for the repair pass', { jobId: job.job_id, error: err instanceof Error ? err.message : String(err) });
    return false;
  }
  return L.markStatusWritten(ledgerCtx(env), job.job_id);
}

function repairRecordOf(job: JobRow): Record<string, unknown> | null {
  if (job.provenance !== 'kv' || !job.import_json) return null;
  try {
    const r = JSON.parse(job.import_json).repair_record;
    return r && typeof r === 'object' ? r : null;
  } catch {
    return null;
  }
}

// ── 4.8 with the no-balance rerun, and the owner's r5 branch ──

/** The owner's failure path (4.8 `'owner'`, then r5's branch; 4.9 step 4). */
export async function ownerFails(
  env: Env, job: string, claim: string, code: string, errMsg: string, ev: RefundEventInput, log: Logger,
): Promise<Next> {
  const r = await refundTokens(env, { job, fence: 'owner', claim, code, msg: errMsg.slice(0, 500) }, log);
  switch (r.outcome) {
    case 'refunded':
    case 'already_refunded_legacy':
      await afterRefund(env, job, r, code, { ...ev, errMsg }, log);
      return ACK;
    case 'already_finished': {
      const row = await readJobRow(env, job);
      if (row) await writeTerminalStatus(env, row, log);
      return ACK;
    }
    case 'paused':
      log('warn', 'refund paused; the unfinished row is the kept debt', { code });
      return retryIn(PAUSED_RETRY_S);
    case 'error':
      return retryIn(null);
    default:
      // live owner, ownership lost, held, corruption (its alarm written), no record
      log('warn', 'owner refund settled nothing', { outcome: r.outcome, code });
      return ACK;
  }
}

// ── 4.9: finalizing a staged result ──

/** After c-finalize won (or the owner's own staged row): `head` the PNG;
 *  present: the success update with the outcome from the meta, then publish,
 *  the status and the marker; absent: 4.8 `'owner'`, `result_store_failed`. */
export async function finishStaged(env: Env, job: JobRow, claim: string, ev: RefundEventInput, log: Logger): Promise<Next> {
  let present: boolean;
  try {
    present = await pngPresent(env, job.user_id, job.job_id);
  } catch (err) {
    log('warn', 'staged PNG head failed; retrying', { error: err instanceof Error ? err.message : String(err) });
    return retryIn(null);
  }
  if (!present) return ownerFails(env, job.job_id, claim, 'result_store_failed', 'the staged result was not stored', ev, log);
  const meta = parseMeta(job.artifact_meta_json, job.created_at_ms);
  const outcome = meta.rescue ? 'rescued' : 'succeeded';
  const s = await successOrDoubt(env, job.job_id, claim, outcome);
  if (s === 'paused') return retryIn(PAUSED_RETRY_S);
  if (s === 'error') return retryIn(null);
  if (s === 'ownership_lost') {
    log('warn', 'finalize lost its claim; nothing published');
    return ACK;
  }
  return publishAndRecord(env, job.job_id, meta, log, { attempt: ev.attempt });
}

/** The success update (4.7), with one more read when it answers "ownership
 *  lost": the library's read-back after a thrown or uncertain update maps a
 *  row that is unfinished, unreleased and still on this claim to ownership
 *  lost, but that is a doubt, not a loss. 4.9 retries the message then; the
 *  staged row stays for c-finalize. */
export async function successOrDoubt(env: Env, job: string, claim: string, outcome: 'succeeded' | 'rescued'): Promise<L.SuccessOutcome> {
  const s = await L.successUpdate(ledgerCtx(env), { job, claim, outcome });
  if (s !== 'ownership_lost') return s;
  const row = await readJobRow(env, job);
  if (row && row.finished_at_ms === null && row.claim_id === claim && row.released_at_ms === null) return 'error';
  return s;
}

/** 4.9 steps 6 and 7: the `gen:` index, `published`, the strict status, the
 *  marker, and the terminal event. Finalizing records the finishing poll's
 *  own times from the meta, not its own later start (`L3 007` ruling 2). */
export async function publishAndRecord(env: Env, jobId: string, meta: ArtifactMeta, log: Logger, ev: { attempt?: number; requestedSize?: string; finalSize?: string }): Promise<Next> {
  const job = await readJobRow(env, jobId);
  if (!job) return retryIn(null);
  try {
    await publishIndex(env, { jobId, userId: job.user_id, prompt: meta.prompt, style: meta.style, mode: meta.mode, createdAt: meta.createdAt, ...(meta.rescue ? { rescued: true as const } : {}) }, log);
    await L.markPublished(ledgerCtx(env), jobId);
  } catch (err) {
    // The row is finished and staged: the repair pass publishes it.
    log('warn', 'publish failed; the repair pass will publish', { error: err instanceof Error ? err.message : String(err) });
  }
  const after = await readJobRow(env, jobId);
  if (after) await writeTerminalStatus(env, after, log);
  await recordEvent(env, {
    eventName: meta.rescue ? 'generation.rescued' : 'generation.succeeded',
    level: 'info',
    dedupeKey: `${jobId}:generation.terminal`,
    userId: job.user_id,
    jobId,
    attempt: ev.attempt,
    provider: PROVIDER,
    style: meta.style,
    ...(ev.requestedSize ? { requestedSize: ev.requestedSize } : {}),
    ...(ev.finalSize ? { finalSize: ev.finalSize } : {}),
    outcome: meta.rescue ? 'rescued' : 'succeeded',
    latencyMs: meta.createdAt - meta.startedAt,
    extra: {
      mode: meta.mode, rdBalanceCost: meta.rdBalanceCost, rescue: meta.rescue,
      firstStartedAt: meta.startedAt, finishingPollStartedAt: meta.pollStartedAt ?? null, finishingPollEndedAt: meta.pollEndedAt ?? null,
    },
  }, log);
  return ACK;
}

// ── 4.12: the delivered checks for an imported row ──

/** Both the `gen:` index and the PNG: the delivered finish; the PNG only: the
 *  canceller refund, then the PNG discarded; the index only: the hold and its
 *  alarm; neither: the canceller refund. A failed check retries; nothing is
 *  guessed. */
export async function deliveredChecks(env: Env, job: JobRow, ev: RefundEventInput, log: Logger): Promise<Next> {
  let index: Record<string, unknown> | null;
  let png: boolean;
  try {
    index = await findIndexEntry(env, job.user_id, job.job_id);
    png = await pngPresent(env, job.user_id, job.job_id);
  } catch (err) {
    log('warn', 'delivered checks failed to read; retrying', { error: err instanceof Error ? err.message : String(err) });
    return retryIn(null);
  }
  const code = job.refund_due_code ?? 'imported_unfinished';
  if (index && png) {
    const meta = metaFromIndexRow(index, job.created_at_ms);
    const d = await L.deliveredFinish(ledgerCtx(env), { job: job.job_id, meta: JSON.stringify(meta) });
    if (d === 'paused') return retryIn(PAUSED_RETRY_S);
    if (d === 'error') return retryIn(null);
    if (d === 'fence_lost') return ACK;
    const after = await readJobRow(env, job.job_id);
    if (after && (await writeTerminalStatus(env, after, log))) await L.markPublished(ledgerCtx(env), job.job_id);
    return ACK;
  }
  if (index && !png) {
    const h = await L.holdIndexOnly(ledgerCtx(env), job.job_id);
    if (h === 'held') await ledgerAlarm(env, 'index_only', job.job_id, { userId: job.user_id }, log, job.user_id);
    return h === 'error' ? retryIn(null) : ACK;
  }
  const r = await refundTokens(env, { job: job.job_id, fence: 'canceller', code }, log);
  return afterCanceller(env, job, r, code, ev, log, png);
}

/** A canceller's r5 branch; with `pngOnly`, the PNG is deleted and the row
 *  marked `discarded` only after a refunded outcome (4.12). */
export async function afterCanceller(env: Env, job: JobRow, r: L.RefundResult, code: string, ev: RefundEventInput, log: Logger, pngOnly = false): Promise<Next> {
  switch (r.outcome) {
    case 'refunded':
    case 'already_refunded_legacy':
    case 'already_finished': {
      const refundedOutcome = r.outcome !== 'already_finished' || ['refunded', 'refunded_legacy'].includes(String((r.row as Record<string, unknown> | null)?.outcome));
      if (pngOnly && refundedOutcome) {
        try {
          await env.GALLERY_BUCKET.delete(galleryR2Key(job.user_id, job.job_id));
          await L.discardPngOnly(ledgerCtx(env), job.job_id);
        } catch (err) {
          log('warn', 'PNG discard failed; the repair pass retries it', { error: err instanceof Error ? err.message : String(err) });
        }
      }
      if (r.outcome === 'already_finished') {
        const row = await readJobRow(env, job.job_id);
        if (row) await writeTerminalStatus(env, row, log);
      } else {
        await afterRefund(env, job.job_id, r, code, ev, log);
      }
      return ACK;
    }
    case 'paused':
      return retryIn(PAUSED_RETRY_S);
    case 'staged_result':
      return retryIn(null);
    case 'live_owner':
      return retryIn(null);
    case 'error':
      return retryIn(null);
    default:
      return ACK;
  }
}

// ── The tombstone (4.8) ──

export async function tombstoneJob(env: Env, msgBody: Partial<JobMessage>, log: Logger): Promise<'tombstoned' | 'row_exists' | 'paused' | 'error'> {
  const jobId = String(msgBody.jobId);
  const cost = Number.isInteger(msgBody.tokenCost) && (msgBody.tokenCost as number) >= 1 && (msgBody.tokenCost as number) <= 50 ? (msgBody.tokenCost as number) : 1;
  const t = await L.tombstone(ledgerCtx(env), {
    job: jobId, uid: String(msgBody.userId), mode: msgBody.mode === 'animate' ? 'animate' : 'create', cost, code: 'no_record',
  });
  if (t.alarm === 'no_record') await noRecordAlarm(env, jobId, String(msgBody.userId), msgBody.tokenCost ?? null, log);
  if (t.outcome === 'tombstoned') {
    const row = await readJobRow(env, jobId);
    if (row) await writeTerminalStatus(env, row, log);
  }
  return t.outcome;
}

// ── 4.11: the repair pass and the overdue alarms ──

export interface RepairCounts {
  phase_open: boolean;
  examined: number;
  published: number;
  discarded: number;
  statuses: number;
  failed: number;
  overdue: number;
}

/** One finished row through 4.11's table. */
export async function repairOne(env: Env, row: Record<string, unknown>, log: Logger): Promise<'published' | 'discarded' | 'status' | 'failed' | 'none'> {
  const job = (await readJobRow(env, String(row.job_id))) as JobRow | null;
  if (!job || job.finished_at_ms === null) return 'none';
  const ctx = ledgerCtx(env);
  try {
    if ((job.outcome === 'succeeded' || job.outcome === 'rescued') && job.artifact === 'staged') {
      const meta = parseMeta(job.artifact_meta_json, job.finished_at_ms);
      await publishIndex(env, { jobId: job.job_id, userId: job.user_id, prompt: meta.prompt, style: meta.style, mode: meta.mode, createdAt: meta.createdAt, ...(meta.rescue ? { rescued: true as const } : {}) }, log);
      await L.markPublished(ctx, job.job_id);
      if (job.status_written_at_ms === null) await writeTerminalStatus(env, (await readJobRow(env, job.job_id)) as JobRow, log);
      return 'published';
    }
    if ((job.outcome === 'refunded' || job.outcome === 'refunded_legacy' || job.outcome === 'no_record') && job.artifact === 'staged') {
      await env.GALLERY_BUCKET.delete(galleryR2Key(job.user_id, job.job_id));
      await L.discardStaged(ctx, job.job_id);
      if (job.status_written_at_ms === null) await writeTerminalStatus(env, (await readJobRow(env, job.job_id)) as JobRow, log);
      return 'discarded';
    }
    if (job.status_written_at_ms === null) return (await writeTerminalStatus(env, job, log)) ? 'status' : 'failed';
  } catch (err) {
    log('warn', 'repair failed for a row; the next pass retries', { jobId: job.job_id, error: err instanceof Error ? err.message : String(err) });
    return 'failed';
  }
  return 'none';
}

export async function repairPass(env: Env, log: Logger): Promise<RepairCounts> {
  const c: RepairCounts = { phase_open: false, examined: 0, published: 0, discarded: 0, statuses: 0, failed: 0, overdue: 0 };
  const ctx = ledgerCtx(env);
  const list = await L.repairList(ctx);
  if (list.outcome === 'phase_open') {
    c.phase_open = true;
    return c;
  }
  for (const row of list.rows) {
    c.examined++;
    const r = await repairOne(env, row, log);
    if (r === 'published') c.published++;
    else if (r === 'discarded') c.discarded++;
    else if (r === 'status') c.statuses++;
    else if (r === 'failed') c.failed++;
  }
  for (const row of await L.overdueList(ctx)) {
    const finished = row.finished_at_ms !== null && row.finished_at_ms !== undefined;
    await ledgerAlarm(env, finished ? 'repair_overdue' : 'unfinished_overdue', String(row.job_id), {
      finishedAtMs: row.finished_at_ms ?? null, createdAtMs: row.created_at_ms,
    }, log, String(row.user_id));
    c.overdue++;
  }
  return c;
}

/** The alarm row the dead-letter handler must prove written (release 1's
 *  strict alarm, 5.4): `generation.unrefunded`. */
export async function unrefundedAlarmStrict(env: Env, jobId: string, userId: string, fields: Record<string, unknown>, queueMessageId: string, attempt: number): Promise<boolean> {
  try {
    await recordEventOrThrow(env, {
      eventName: 'generation.unrefunded',
      level: 'error',
      dedupeKey: `${jobId}:generation.unrefunded`,
      userId,
      jobId,
      queueMessageId,
      attempt,
      errorCode: 'dead_lettered',
      extra: fields,
    });
    return true;
  } catch {
    return false;
  }
}

export { noRecordAlarm };
