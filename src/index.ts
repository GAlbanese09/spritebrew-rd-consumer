// spritebrew-rd-consumer/src/index.ts
//
// Cloudflare Queues consumer for SpriteBrew RD generation jobs, on the D1
// ledger (n1-release-2-spec.md revision 9: 5.2 to 5.4, 4.6 to 4.12; S3).
// Architectural reference: Confluence 87490562 (queue-and-poll) and 87588866
// (inline-refund), as release 1 built them; money is now D1's.
//
// One delivery (5.2): the job's `jobs` row is read once and decides, in order:
//   - no row: the tombstone (guarded) and its `no_record` alarm;
//   - held: ack, nothing run, nothing paid;
//   - finished: ack, after the repair pass for that row;
//   - staged, unfinished: c-finalize, then 4.9's finish (owner live: retry);
//   - an imported debt (`refund_due_code`): the delivered checks, never RD;
//   - a task on record: c-resume, poll first, then 4.9 or the owner's refund;
//   - the money pause gate (release 1's place): paused, retry, nothing written;
//   - a submit with no task: stale, the canceller's refund, never RD; else a
//     live owner;
//   - a live claim: a live owner (attempt 1 acks, later attempts retry);
//   - otherwise the fresh path: the animate pre-flight, c-submit, the
//     `running` record (no tokenCost), then the phase: `submitted` before any
//     billable RD call, `task` before any poll, `fallback` before a rescue.
// Success is 4.9: stage, the PNG durably, the success update, publish, the
// strict status, the marker. Failure is 4.8 `'owner'` and its r5 branch.
// Every money movement is one guarded D1 batch through src/ledger.ts.
//
// The cron (5.3): 4.12's sweep candidates, then the repair pass (4.11).
// Create mode stays synchronous; animate uses RD's async job API.

import type { JobMessage, JobMode, Env } from './types';
import type { RdAnimateBody, RdSuccessResponse } from './rdClient';
import {
  callRd,
  submitAsyncTask,
  pollAsyncTask,
  checkRdAnimationsStatus,
  probeRdStatus,
  RdError,
} from './rdClient';
import * as L from './ledger';
import { refundTokens } from './refund';
import { base64ToBytes, stagePng } from './gallery';
import { recordEvent } from './events';
import { runDigestIfDue } from './digest';
import { DEAD_LETTER_QUEUES, handleDeadLetter } from './deadLetter';
import { handleMigrator } from './migrator';
import { isMoneyPaused, pausedRetryDelayS } from './moneyPause';
import { putRunningBestEffort, type ArtifactMeta } from './status';
import {
  ACK, PAUSED_RETRY_S, PROVIDER, afterCanceller, debitContext, deliveredChecks, finishStaged, isStale, ledgerAlarm,
  ledgerCtx, messageContext, ownerFails, publishAndRecord, successOrDoubt, readJobRow, repairOne, repairPass, retryIn,
  tombstoneJob, type JobRow, type Next,
} from './settle';

const MAX_ATTEMPTS = 3;              // matches max_retries in wrangler.toml
const STATUS_RETRY_DELAY_S = 60;     // pre-flight backoff between attempts
const FALLBACK_CELL_SIZE = 64;       // animation__any_animation is 64×64-locked (probe C5)
const PROBE_SLOT_MS = 15 * 60 * 1000; // provider.status dedupe slot; matches the */15 cron

type Logger = (
  level: 'info' | 'warn' | 'error',
  message: string,
  extra?: Record<string, unknown>
) => void;

/**
 * Readable text for an arbitrary thrown value. `String(err)` on a plain
 * object yields "[object Object]", which is how a primary RD failure could
 * reach the logs with its cause erased. Errors and strings behave exactly as
 * before; only the non-string, non-Error case changes.
 */
function errText(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === 'string') return err;
  try {
    const json = JSON.stringify(err);
    if (json !== undefined) return json;
  } catch {
    // Circular / non-serializable — fall through to String().
  }
  return String(err);
}

/**
 * The half of a rescue that is known at fallback-SUBMIT time: what the user
 * asked for, and what we clamped to. Release 2 keeps the fallback phase on the
 * `jobs` row (`phase 'fallback'`), so a resumed fallback task rebuilds this
 * from the message, and the full descriptor rides in `artifact_meta_json`.
 */
interface RescueMarker {
  requestedWidth: number;
  requestedHeight: number;
  deliveredCellSize: number;
}

/**
 * Rescue descriptor for a delivery that came from the animate fallback: the
 * persisted marker plus the geometry that only exists once the sheet lands.
 * Threaded into recordSuccess, which writes it onto the success record. The
 * client reads `rescued` explicitly rather than inferring it from geometry.
 */
interface RescueInfo extends RescueMarker {
  rescued: true;
  deliveredFrames?: number;
}

/** What runAnimateAsync hands back: the RD result plus, iff the fallback
 *  served it, the rescue descriptor. */
interface AnimateOutcome {
  result: RdSuccessResponse;
  rescue?: RescueInfo;
  /** The finishing poll's own start and end (`L3 007` ruling 2). */
  pollStartedAt?: number;
  pollEndedAt?: number;
}

// ─── PNG header parsing (delivered-geometry read) ──────────────────────────

// A PNG's IHDR is fixed-offset: 8-byte signature, then the first chunk's
// 4-byte length, the 4-byte type tag "IHDR", then width and height as
// big-endian uint32s at byte offsets 16 and 20. 24 bytes total, which is
// exactly the first 32 base64 characters — so we decode only that prefix
// rather than the whole multi-hundred-KB sheet.
//
// Scope note (July 7 lesson): that lesson was about the COLOR-TYPE byte,
// where RD's declared value disagreed with the actual pixel data. It says
// nothing about dimensions, which are structural — a decoder that misread
// them could not produce a displayable image at all. Dimensions are safe to
// trust here; color type still is not.
const PNG_HEADER_B64_CHARS = 32;
const PNG_SIGNATURE = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];

function readPngDimensions(base64: string): { width: number; height: number } | null {
  let bytes: Uint8Array;
  try {
    if (base64.length < PNG_HEADER_B64_CHARS) return null;
    bytes = base64ToBytes(base64.slice(0, PNG_HEADER_B64_CHARS));
  } catch {
    return null;  // Not decodable base64 — caller treats geometry as unknown.
  }
  if (bytes.length < 24) return null;
  for (let i = 0; i < PNG_SIGNATURE.length; i++) {
    if (bytes[i] !== PNG_SIGNATURE[i]) return null;
  }
  // IHDR is required by spec to be the first chunk; if it isn't, this isn't a
  // shape we understand and we decline rather than guess.
  const chunkType = String.fromCharCode(bytes[12], bytes[13], bytes[14], bytes[15]);
  if (chunkType !== 'IHDR') return null;

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const width = view.getUint32(16);
  const height = view.getUint32(20);
  if (width === 0 || height === 0) return null;
  return { width, height };
}

/**
 * Frames in a delivered spritesheet: (W/cell)*(H/cell). Returns undefined
 * when the header is unreadable or the sheet isn't an exact multiple of the
 * cell — a wrong frame count would slice the animation visibly wrong, so an
 * absent field (client falls back to its own guess) beats a confident lie.
 */
function deliveredFramesFromSheet(
  base64: string,
  cellSize: number
): { frames?: number; width?: number; height?: number } {
  const dims = readPngDimensions(base64);
  if (!dims) return {};
  const { width, height } = dims;
  if (width % cellSize !== 0 || height % cellSize !== 0) {
    return { width, height };
  }
  const frames = (width / cellSize) * (height / cellSize);
  return frames > 0 ? { frames, width, height } : { width, height };
}

/**
 * Complete a persisted RescueMarker into the full descriptor by measuring the
 * sheet that actually arrived. Shared by the fresh-fallback path and the
 * resume-poll path (guard 0c) so a redelivered rescue is described exactly
 * like a first-delivery one — the whole point of persisting the marker.
 */
function buildRescueInfo(
  marker: RescueMarker,
  base64: string,
  taskId: string,
  requestedFrames: number | undefined,
  log: Logger
): RescueInfo {
  const sheet = deliveredFramesFromSheet(base64, marker.deliveredCellSize);

  if (sheet.frames === undefined) {
    log('warn', 'rescue delivered but frame count unreadable; deliveredFrames omitted', {
      taskId,
      sheetWidth: sheet.width,
      sheetHeight: sheet.height,
      cellSize: marker.deliveredCellSize,
      reason: sheet.width === undefined
        ? 'PNG header unreadable'
        : 'sheet not an exact multiple of cell size',
    });
  } else {
    log('info', 'rescue delivered', {
      taskId,
      requestedWidth: marker.requestedWidth,
      requestedHeight: marker.requestedHeight,
      sheetWidth: sheet.width,
      sheetHeight: sheet.height,
      deliveredCellSize: marker.deliveredCellSize,
      deliveredFrames: sheet.frames,
      requestedFrames,
    });
  }

  return {
    rescued: true,
    ...marker,
    ...(sheet.frames !== undefined ? { deliveredFrames: sheet.frames } : {}),
  };
}

/**
 * True for the poll-budget-exhaustion RdError specifically (status 0, message
 * "RD async poll exceeded budget ..."). This case is BILLING-SPECIAL: the RD
 * task is STILL LIVE and WILL bill, and we hold its taskId. Falling back would
 * submit a second billable job for one request AND discard a retrievable
 * result. Policy: on this error, redeliver the message so guard 0c can resume
 * polling the SAME task with a fresh budget (no new submit, no new bill, no
 * fallback). Only on the final attempt do we terminal-fail with rd_async_timeout.
 * Distinct from a submit orphan (also status 0) by its message prefix.
 */
function isPollBudgetExceeded(err: unknown): boolean {
  return (
    err instanceof RdError &&
    err.status === 0 &&
    err.message.startsWith('RD async poll exceeded budget')
  );
}

/**
 * The taskId a poll-budget error names ("... for task <id>"). The fresh-run
 * handler does not hold the taskId in a local (runAnimateAsync persisted it
 * to KV and threw), so the ledger row reads it off the message rather than
 * spending a KV get. Undefined if the message shape ever changes.
 */
function taskIdFromPollError(err: unknown): string | undefined {
  if (!(err instanceof RdError)) return undefined;
  const m = /for task (\S+)$/.exec(err.message);
  return m ? m[1] : undefined;
}

export default {
  // The migrator (S2, 6.1): token-gated, exact POST paths, uniform 404; in
  // phase '0' only the read-only scan?verify=1 answers.
  async fetch(request: Request, env: Env): Promise<Response> {
    return handleMigrator(request, env);
  },
  async queue(batch: MessageBatch<JobMessage>, env: Env, _ctx: ExecutionContext): Promise<void> {
    // Dead letters: exact queue names only (deadLetter.ts).
    if (DEAD_LETTER_QUEUES.has(batch.queue)) {
      for (const msg of batch.messages) {
        await handleDeadLetter(msg, env);
      }
      return;
    }
    for (const msg of batch.messages) {
      await handleMessage(msg, env);
    }
  },
  // Cron: every 15 min (see wrangler.toml). 4.12's sweep settles the rows the
  // queue path left unfinished (the create isolate-kill orphan class among
  // them) through the guarded batches, then the repair pass (4.11) writes any
  // status record a settlement could not.
  async scheduled(
    _controller: ScheduledController,
    env: Env,
    _ctx: ExecutionContext
  ): Promise<void> {
    await sweep(env);
    // Provider status ledger row every 15 min (WD2a). Own try/catch inside;
    // cannot affect the sweep.
    await probeAndRecordProviderStatus(env);
    // Morning digest (WD2b): runs only in the 08:00 New York hour, once per
    // reporting day, capped at 3 attempts. Own try/catch inside.
    await runDigestIfDue(env);
  },
} satisfies ExportedHandler<Env, JobMessage>;

// ─── The delivery (n1-release-2-spec.md revision 9, 5.2) ───────────────────

const LIVE_OWNER_RETRY_S = 30;

function apply(msg: Message<JobMessage>, next: Next): void {
  if ('ack' in next) msg.ack();
  else if (next.retry === null) msg.retry();
  else msg.retry({ delaySeconds: next.retry });
}

const liveOwner = (attempt: number): Next => (attempt === 1 ? ACK : retryIn(LIVE_OWNER_RETRY_S));

async function handleMessage(
  msg: Message<JobMessage>,
  env: Env
): Promise<void> {
  const { jobId, userId, mode, body } = msg.body;
  const attempt = msg.attempts ?? 1;

  const log: Logger = (level, message, extra = {}) => {
    console[level](
      JSON.stringify({
        level,
        message,
        jobId,
        userId,
        attempt,
        mode,
        ...extra,
      })
    );
  };

  log('info', 'message received');
  await recordEvent(env, {
    eventName: 'queue.message_received',
    level: 'info',
    dedupeKey: `${jobId}:queue.message_received:attempt_${attempt}`,
    userId,
    jobId,
    attempt,
    queueMessageId: msg.id,
    queueWaitMs: Date.now() - msg.body.enqueuedAt,
    style: body.prompt_style,
    requestedSize: `${body.width}x${body.height}`,
    extra: { mode },
  }, log);

  let next: Next;
  try {
    next = await deliver(msg, env, log);
  } catch (err) {
    // A throw never refunds: the row is the durable debt, and the message
    // comes back (a staged result goes to c-finalize, 4.9).
    log('error', 'delivery threw; retrying', { error: errText(err).slice(0, 300) });
    next = retryIn(null);
  }
  apply(msg, next);
}

/** 5.2's table, in order. The row is read once; release 1's pause gate keeps
 *  its place, after the resume branch and before 0d, 0e and the pre-flight. */
async function deliver(msg: Message<JobMessage>, env: Env, log: Logger): Promise<Next> {
  const { jobId, mode } = msg.body;
  const attempt = msg.attempts ?? 1;
  const ctx = ledgerCtx(env);
  const ev = { attempt, queueMessageId: msg.id, ctx: messageContext(msg.body) };

  let row = await readJobRow(env, jobId);
  if (!row) {
    // No row: the tombstone (guarded) and its alarm.
    const t = await tombstoneJob(env, msg.body, log);
    if (t === 'tombstoned') return ACK;
    if (t === 'paused') return retryIn(await pausedRetryDelayS(env));
    if (t === 'error') return retryIn(null);
    row = await readJobRow(env, jobId);
    if (!row) return retryIn(null);
  }
  if (row.hold_reason !== null) {
    log('info', 'held row: nothing run, nothing paid', { holdReason: row.hold_reason });
    return ACK;
  }
  if (row.finished_at_ms !== null) {
    // Finished: ack, after the repair pass for this row (phase '0' only).
    const list = await L.repairList(ctx);
    if (list.outcome === 'phase_open') return retryIn(PAUSED_RETRY_S);
    await repairOne(env, row as unknown as Record<string, unknown>, log);
    return ACK;
  }
  if (row.artifact === 'staged') {
    const claim = crypto.randomUUID();
    const c = await L.claimJob(ctx, 'finalize', { job: jobId, claim, attempt });
    if (c.outcome === 'won') return finishStaged(env, row, claim, ev, log);
    if (c.outcome === 'held') return ACK;
    if (c.outcome === 'error' || c.outcome === 'no_row') return retryIn(null);
    // owner live, or the migrator's phase open
    return retryIn(PAUSED_RETRY_S);
  }
  if (row.refund_due_code !== null) {
    // An imported debt: the delivered checks decide; never RD.
    return deliveredChecks(env, row, ev, log);
  }
  if (row.task_id !== null) {
    const claim = crypto.randomUUID();
    const c = await L.claimJob(ctx, 'resume', { job: jobId, claim, attempt });
    if (c.outcome === 'won') return resumeTask(env, msg, row, claim, log);
    if (c.outcome === 'phase_open') return retryIn(PAUSED_RETRY_S);
    if (c.outcome === 'held') return ACK;
    if (c.outcome === 'error' || c.outcome === 'no_row') return retryIn(null);
    return liveOwner(attempt);
  }

  // === The pause gate (release 1's place: A4, `L 006` A, `L 008` B). While
  //     paused, nothing is written and no RD call of any kind is made.
  if (await isMoneyPaused(env)) {
    const delaySeconds = await pausedRetryDelayS(env);
    log('warn', 'money paused; holding message, no RD call', { delaySeconds, lastDelivery: attempt > MAX_ATTEMPTS });
    return retryIn(delaySeconds);
  }

  const now = Date.now();
  if (row.submitted_at_ms !== null) {
    if (!isStale(row, now)) return liveOwner(attempt);
    if (row.provenance === 'kv') return deliveredChecks(env, row, ev, log);
    const code = row.mode === 'animate' ? 'rd_submit_orphaned_redelivery' : 'rd_create_outcome_unknown';
    log('error', 'a submit with no outcome on record; refunding, never RD', { code });
    const r = await refundTokens(env, { job: jobId, fence: 'canceller', code }, log);
    return afterCanceller(env, row, r, code, ev, log);
  }
  if (row.claim_id !== null && !isStale(row, now)) return liveOwner(attempt);

  // === The fresh path: the pre-flight (animate), then c-submit (guarded).
  if (mode === 'animate') {
    const rdStatus = await checkRdAnimationsStatus(log);
    const shouldRetry = rdStatus === 'degraded' && attempt < MAX_ATTEMPTS;
    log('info', 'rd status pre-flight', { rdStatus, decision: shouldRetry ? `retry in ${STATUS_RETRY_DELAY_S}s` : 'proceed' });
    if (shouldRetry) return retryIn(STATUS_RETRY_DELAY_S);
  }
  const claim = crypto.randomUUID();
  const c = await L.claimJob(ctx, 'submit', { job: jobId, claim, attempt });
  if (c.outcome === 'paused') return retryIn(await pausedRetryDelayS(env));
  if (c.outcome === 'held') return ACK;
  if (c.outcome !== 'won') return c.outcome === 'error' || c.outcome === 'no_row' ? retryIn(null) : liveOwner(attempt);
  return runFresh(env, msg, claim, log);
}

/** After c-submit won: the `running` record (without tokenCost, 4.10), then
 *  the phase (5.2). */
async function runFresh(env: Env, msg: Message<JobMessage>, claim: string, log: Logger): Promise<Next> {
  const { jobId, userId, mode, body } = msg.body;
  const attempt = msg.attempts ?? 1;
  const ctx = ledgerCtx(env);
  const startedAt = Date.now();
  await putRunningBestEffort(env, jobId, { userId, mode, enqueuedAt: msg.body.enqueuedAt, startedAt, attempt }, log);
  const ev = { attempt, queueMessageId: msg.id, ctx: messageContext(msg.body) };

  if (mode === 'create') {
    const u = await L.ownerUpdate(ctx, 'submitted', { job: jobId, claim });
    if (u === 'ownership_lost') return ACK;
    if (u === 'retry_message') return retryIn(null);
    const pollStartedAt = Date.now();
    let result: RdSuccessResponse;
    try {
      result = await callRd(env.RETRO_DIFFUSION_API_KEY, 'create', body);
    } catch (err) {
      const errorCode = classifyError(err);
      const retryable = err instanceof RdError ? err.retryable : true;
      log('error', 'rd call failed', { errMsg: errText(err), errorCode, retryable, willRetry: retryable && attempt < MAX_ATTEMPTS });
      // The 240 s create timeout and a 524 are not retryable: they keep
      // submitted_at_ms (no release, `L 004` ruling 7) and go to the refund.
      if (retryable && attempt < MAX_ATTEMPTS) {
        const rel = await L.ownerUpdate(ctx, 'release_create', { job: jobId, claim });
        return rel === 'ownership_lost' ? ACK : retryIn(null);
      }
      return ownerFails(env, jobId, claim, errorCode, errText(err), { ...ev, latencyMs: Date.now() - startedAt, retryable }, log);
    }
    return succeed(env, msg, claim, result, { startedAt, pollStartedAt, pollEndedAt: Date.now() }, undefined, log);
  }

  let outcome: AnimateOutcome;
  try {
    outcome = await runAnimateAsync(env, ctx, claim, msg, log);
  } catch (err) {
    if (err instanceof OwnershipStop) return err.next;
    return animateFailed(env, msg, claim, err, startedAt, log);
  }
  return succeed(env, msg, claim, outcome.result, { startedAt, pollStartedAt: outcome.pollStartedAt, pollEndedAt: outcome.pollEndedAt }, outcome.rescue, log);
}

/** An owner update that lost its claim, or a second doubt: stop the run,
 *  call nothing billable (4.7). */
class OwnershipStop extends Error {
  constructor(public next: Next) {
    super('ownership stop');
  }
}

async function ownerStep(ctx: L.LedgerCtx, kind: L.OwnerUpdateKind, u: L.OwnerUpdateInput): Promise<void> {
  const r = await L.ownerUpdate(ctx, kind, u);
  if (r === 'ownership_lost') throw new OwnershipStop(ACK);
  if (r === 'retry_message') throw new OwnershipStop(retryIn(null));
}

async function animateFailed(env: Env, msg: Message<JobMessage>, claim: string, err: unknown, startedAt: number, log: Logger): Promise<Next> {
  const { jobId, userId } = msg.body;
  const attempt = msg.attempts ?? 1;
  const ctx = ledgerCtx(env);
  const errorCode = classifyError(err);
  const pollTaskId = isPollBudgetExceeded(err) ? taskIdFromPollError(err) : undefined;
  if (isPollBudgetExceeded(err)) {
    await recordEvent(env, {
      eventName: 'provider.poll_budget_exhausted',
      level: 'warn',
      dedupeKey: `${jobId}:provider.poll_budget_exhausted:attempt_${attempt}`,
      userId, jobId, attempt,
      provider: PROVIDER,
      providerJobId: pollTaskId,
      extra: { finalAttempt: attempt >= MAX_ATTEMPTS },
    }, log);
    if (attempt < MAX_ATTEMPTS) {
      // The task stays: the keep-task release, then redeliver at once to
      // resume polling the same task (c-resume).
      const rel = await L.ownerUpdate(ctx, 'release_animate', { job: jobId, claim });
      return rel === 'ownership_lost' ? ACK : retryIn(0);
    }
    log('error', 'poll budget exhausted on the final attempt; refunding despite a live task (bounded orphan)', { taskId: pollTaskId });
  }
  log('error', 'rd call failed', { errMsg: errText(err), errorCode });
  return ownerFails(env, jobId, claim, errorCode, errText(err), {
    attempt, queueMessageId: msg.id, ctx: messageContext(msg.body), latencyMs: Date.now() - startedAt,
    retryable: err instanceof RdError ? err.retryable : true,
  }, log);
}

/** c-resume won: poll first; then 4.9 or 4.8 `'owner'` (5.2). */
async function resumeTask(env: Env, msg: Message<JobMessage>, row: JobRow, claim: string, log: Logger): Promise<Next> {
  const startedAt = Date.now();
  log('info', 'resuming the poll of an existing task', { taskId: row.task_id, phase: row.phase });
  const pollStartedAt = Date.now();
  try {
    const result = await pollAsyncTask(env.RETRO_DIFFUSION_API_KEY, row.task_id as string, undefined, { pollFirst: true });
    const rescue = row.phase === 'fallback'
      ? buildRescueInfo(
          { requestedWidth: msg.body.body.width, requestedHeight: msg.body.body.height, deliveredCellSize: FALLBACK_CELL_SIZE },
          result.base64_images[0], row.task_id as string, (msg.body.body as RdAnimateBody).frames_duration, log)
      : undefined;
    return succeed(env, msg, claim, result, { startedAt, pollStartedAt, pollEndedAt: Date.now() }, rescue, log);
  } catch (err) {
    return animateFailed(env, msg, claim, err, startedAt, log);
  }
}

// ─── Animate async orchestration ───────────────────────────────────────────

/**
 * Full animate flow on the claim: `submitted`, then the primary async submit,
 * the `task` update before any poll, then the poll; on a primary failure of
 * rd_advanced_animation__*, the `fallback` update first (no reclaim), its
 * submit, `task`, poll (5.2). WHEN a rescue happens is unchanged.
 */
async function runAnimateAsync(
  env: Env,
  ctx: L.LedgerCtx,
  claim: string,
  msg: Message<JobMessage>,
  log: Logger
): Promise<AnimateOutcome> {
  const { jobId, userId } = msg.body;
  const body = msg.body.body as RdAnimateBody;
  const fallbackInputImage = msg.body.fallbackInputImage;
  const attempt = msg.attempts ?? 1;
  const requestedWidth = body.width;
  const requestedHeight = body.height;
  const forceFallback = env.FORCE_ANIMATE_FALLBACK === 'true';

  let primaryErr: unknown;
  if (forceFallback) {
    log('warn', 'FORCE_ANIMATE_FALLBACK active - dev testing only', {
      promptStyle: body.prompt_style, requestedWidth, requestedHeight,
      effect: 'primary submit skipped; going straight to fallback',
    });
    primaryErr = new RdError('FORCE_ANIMATE_FALLBACK: primary submit skipped (dev testing only)', 0, false, '');
  } else {
    // `submitted`: immediately before the billable call, and read back
    // committed before it is made (4.7, R3-17).
    await ownerStep(ctx, 'submitted', { job: jobId, claim });
    try {
      const { taskId, submitElapsedMs } = await submitAsyncTask(env.RETRO_DIFFUSION_API_KEY, body);
      log('info', 'primary async submit accepted', { taskId, submitElapsedMs, promptStyle: body.prompt_style, width: body.width, height: body.height });
      await recordEvent(env, {
        eventName: 'provider.submit_accepted',
        level: 'info',
        dedupeKey: `${jobId}:provider.submit_accepted:${taskId}`,
        userId, jobId, attempt,
        provider: PROVIDER,
        providerJobId: taskId,
        latencyMs: submitElapsedMs,
        style: body.prompt_style,
        requestedSize: `${requestedWidth}x${requestedHeight}`,
      }, log);
      await ownerStep(ctx, 'task', { job: jobId, claim, task: taskId });
      const pollStartedAt = Date.now();
      const result = await pollAsyncTask(env.RETRO_DIFFUSION_API_KEY, taskId);
      return { result, pollStartedAt, pollEndedAt: Date.now() };
    } catch (err) {
      if (err instanceof OwnershipStop) throw err;
      primaryErr = err;
    }
  }

  // Poll-budget exhaustion is not a fallback trigger: the task is live and
  // will bill; the caller releases it and redelivers to resume the poll.
  if (isPollBudgetExceeded(primaryErr)) throw primaryErr;
  if (!(primaryErr instanceof RdError) || !body.prompt_style.startsWith('rd_advanced_animation__')) throw primaryErr;

  const hasEnvelopeInput = typeof fallbackInputImage === 'string' && fallbackInputImage.length > 0;
  const canUseOriginalInput = requestedWidth <= FALLBACK_CELL_SIZE && requestedHeight <= FALLBACK_CELL_SIZE;
  if (!hasEnvelopeInput && !canUseOriginalInput) {
    log('warn', 'fallback unavailable: no 64px input in envelope; primary failure will propagate', {
      originalWidth: requestedWidth, originalHeight: requestedHeight, primaryError: errText(primaryErr),
    });
    throw primaryErr;
  }
  const fallbackBody: RdAnimateBody = {
    ...body,
    prompt_style: 'animation__any_animation',
    width: FALLBACK_CELL_SIZE,
    height: FALLBACK_CELL_SIZE,
    input_image: hasEnvelopeInput ? fallbackInputImage : body.input_image,
  };
  log('info', 'attempting fallback', {
    reason: forceFallback ? 'FORCE_ANIMATE_FALLBACK (dev testing only)' : 'primary rd_advanced_animation__ failed',
    primaryStatus: primaryErr.status, primaryMessage: errText(primaryErr),
    fallbackShape: `${FALLBACK_CELL_SIZE}x${FALLBACK_CELL_SIZE}`, usedEnvelopeInput: hasEnvelopeInput,
  });
  // The same owner moves to the fallback phase first (4.7): no reclaim.
  await ownerStep(ctx, 'fallback', { job: jobId, claim });
  const { taskId: fallbackTaskId, submitElapsedMs: fallbackSubmitMs } = await submitAsyncTask(env.RETRO_DIFFUSION_API_KEY, fallbackBody);
  log('info', 'fallback async submit accepted', { taskId: fallbackTaskId, submitElapsedMs: fallbackSubmitMs });
  await recordEvent(env, {
    eventName: 'provider.submit_accepted',
    level: 'info',
    dedupeKey: `${jobId}:provider.submit_accepted:${fallbackTaskId}`,
    userId, jobId, attempt,
    provider: PROVIDER,
    providerJobId: fallbackTaskId,
    latencyMs: fallbackSubmitMs,
    style: fallbackBody.prompt_style,
    requestedSize: `${FALLBACK_CELL_SIZE}x${FALLBACK_CELL_SIZE}`,
    extra: { fallback: true, primaryError: errText(primaryErr).slice(0, 300) },
  }, log);
  await ownerStep(ctx, 'task', { job: jobId, claim, task: fallbackTaskId });
  const pollStartedAt = Date.now();
  const fallbackResult = await pollAsyncTask(env.RETRO_DIFFUSION_API_KEY, fallbackTaskId);
  const rescueMarker: RescueMarker = { requestedWidth, requestedHeight, deliveredCellSize: FALLBACK_CELL_SIZE };
  const rescue = buildRescueInfo(rescueMarker, fallbackResult.base64_images[0], fallbackTaskId, body.frames_duration, log);
  return { result: fallbackResult, rescue, pollStartedAt, pollEndedAt: Date.now() };
}

// ─── Success (4.9) ─────────────────────────────────────────────────────────

/**
 * 4.9's steps for the owner: stage (the update, then the PNG with its bounded
 * retry and `head`), the success update, publish, the status, the marker.
 * After the stage, a staged result is never refunded by this owner's failure
 * path: any throw or doubt retries the message, and the staged row goes to
 * c-finalize (A4). The only refund here is step 4's verified "cannot be made
 * durable", with `result_store_failed`.
 */
async function succeed(
  env: Env,
  msg: Message<JobMessage>,
  claim: string,
  result: RdSuccessResponse,
  times: { startedAt: number; pollStartedAt?: number; pollEndedAt?: number },
  rescue: RescueInfo | undefined,
  log: Logger
): Promise<Next> {
  const { jobId, userId, mode, body } = msg.body;
  const attempt = msg.attempts ?? 1;
  const ctx = ledgerCtx(env);
  const completedAt = Date.now();
  const meta: ArtifactMeta = {
    v: 1,
    createdAt: completedAt,
    prompt: body.prompt,
    style: body.prompt_style,
    mode,
    enqueuedAt: msg.body.enqueuedAt,
    startedAt: times.startedAt,
    ...(times.pollStartedAt !== undefined ? { pollStartedAt: times.pollStartedAt } : {}),
    ...(times.pollEndedAt !== undefined ? { pollEndedAt: times.pollEndedAt } : {}),
    ...(result.balance_cost !== undefined ? { rdBalanceCost: result.balance_cost } : {}),
    ...(rescue
      ? { rescue: { requestedWidth: rescue.requestedWidth, requestedHeight: rescue.requestedHeight, deliveredCellSize: rescue.deliveredCellSize, ...(rescue.deliveredFrames !== undefined ? { deliveredFrames: rescue.deliveredFrames } : {}) } }
      : {}),
  };

  // 1. Read the row: finished means the orphan path (no publish), ack.
  const before = await readJobRow(env, jobId);
  if (!before || before.finished_at_ms !== null) {
    log('error', 'result arrived after the row finished', { outcome: before?.outcome ?? null, rdBalanceCost: result.balance_cost });
    await recordEvent(env, {
      eventName: 'generation.orphaned',
      level: 'error',
      dedupeKey: `${jobId}:generation.orphaned`,
      userId, jobId, attempt,
      provider: PROVIDER,
      style: body.prompt_style,
      requestedSize: `${body.width}x${body.height}`,
      outcome: 'orphaned',
      extra: { mode, rowOutcome: before?.outcome ?? null, rdBalanceCost: result.balance_cost },
    }, log);
    return ACK;
  }
  // 2. Stage.
  const st = await L.ownerUpdate(ctx, 'stage', { job: jobId, claim, meta: JSON.stringify(meta) });
  if (st === 'ownership_lost') return ACK;
  if (st === 'retry_message') return retryIn(null);
  try {
    // 3. The PNG, durably.
    const durable = await stagePng(env, userId, jobId, base64ToBytes(result.base64_images[0]), log);
    if (!durable) {
      // 4. It cannot be made durable: the owner's refund, result_store_failed.
      return ownerFails(env, jobId, claim, 'result_store_failed', 'the result could not be stored', {
        attempt, queueMessageId: msg.id, ctx: messageContext(msg.body), latencyMs: completedAt - times.startedAt,
      }, log);
    }
    // 5. The success update.
    const s = await successOrDoubt(env, jobId, claim, rescue ? 'rescued' : 'succeeded');
    if (s === 'paused') {
      log('warn', 'result staged durably while money is paused; retrying later', {});
      return retryIn(await pausedRetryDelayS(env));
    }
    if (s === 'error') return retryIn(null);
    if (s === 'ownership_lost') {
      log('error', 'success update lost its claim; nothing published', {});
      await recordEvent(env, {
        eventName: 'generation.orphaned',
        level: 'error',
        dedupeKey: `${jobId}:generation.orphaned`,
        userId, jobId, attempt,
        provider: PROVIDER,
        style: body.prompt_style,
        outcome: 'orphaned',
        extra: { mode, stage: 'success_update' },
      }, log);
      return ACK;
    }
    // 6 to 8.
    const requestedSize = `${body.width}x${body.height}`;
    return await publishAndRecord(env, jobId, meta, log, {
      attempt, requestedSize, finalSize: rescue ? `${rescue.deliveredCellSize}x${rescue.deliveredCellSize}` : requestedSize,
    });
  } catch (err) {
    // After the stage: never the owner's refund (A4). The message returns.
    log('error', 'after staging, a step threw; retrying (the staged row is finalized later)', { error: errText(err).slice(0, 300) });
    return retryIn(null);
  }
}

// ─── Error classification ──────────────────────────────────────────────────

/**
 * Map an error into an errorCode string for the JobStateError record.
 * Order matters: async-specific codes come first so RdError messages that
 * happen to contain 'submit'/'poll' substrings land on the right bucket.
 */
function classifyError(err: unknown): string {
  if (err instanceof RdError) {
    if (err.status === 0 && err.message.startsWith('RD async submit')) {
      return 'rd_submit_orphaned';
    }
    if (err.status === 0 && err.message.startsWith('RD async poll exceeded budget')) {
      return 'rd_async_timeout';
    }
    // Sync create-path timeout (fix 1). Status 0 like the async-0 cases, so it
    // must be matched by message prefix before the rd_${status} fallback would
    // emit a bare 'rd_0'.
    if (err.status === 0 && err.message.startsWith('RD sync call timed out')) {
      return 'rd_sync_timeout';
    }
    // Fix 3: pollAsyncTask throws these two with resp.status=200, so the
    // default `rd_${err.status}` fallback would emit the misleading
    // 'rd_200' — a "successful HTTP but broken payload" is not the same
    // ops signal as an HTTP 200. Normalize BEFORE the fallback.
    if (err.message.includes('no base64_images')) {
      return 'rd_task_success_no_image';
    }
    if (err.message.startsWith('RD async task returned unknown status')) {
      return 'rd_task_unknown_status';
    }
    return `rd_${err.status}`;
  }
  return 'consumer_unknown';
}

// ─── The sweep (5.3, 4.12) and the repair pass (4.11) ─────────────────────

/**
 * The cron: 4.12's candidates replace release 1's KV listing, its refundOwed
 * branch and sweepOne. The paused early return stays for the settling part;
 * the repair pass follows on every run (its status writes settle nothing,
 * and it waits only for the migrator's phase, 4.11).
 */
async function sweep(env: Env): Promise<void> {
  const log: Logger = (level, message, extra = {}) =>
    console[level](JSON.stringify({ level, message, source: 'sweep', ...extra }));
  try {
    if (await isMoneyPaused(env)) {
      log('info', 'money paused; sweep candidates skipped');
    } else {
      await sweepCandidates(env, log);
    }
  } catch (err) {
    log('error', 'sweep failed; the next run retries', { error: errText(err).slice(0, 300) });
  }
  try {
    const counts = await repairPass(env, log);
    log('info', 'repair pass', { ...counts });
  } catch (err) {
    log('error', 'repair pass failed; the next run retries', { error: errText(err).slice(0, 300) });
  }
}

async function sweepCandidates(env: Env, log: Logger): Promise<void> {
  const ctx = ledgerCtx(env);
  const rows = await L.sweepList(ctx);
  const done: Record<string, number> = {};
  for (const r of rows) {
    const job = await readJobRow(env, String(r.job_id));
    if (!job) continue;
    const candidate = L.sweepCandidate(r);
    // Every generation.failed the sweep writes: the debit's style and size,
    // and the row's age apart from its latency (finding 4 and 6).
    const ev = { ctx: await debitContext(env, job.job_id), ageMs: Date.now() - job.created_at_ms };
    try {
      if (candidate === 6) {
        const claim = `sweep:${crypto.randomUUID()}`;
        const c = await L.claimJob(ctx, 'finalize', { job: job.job_id, claim, attempt: 0 });
        if (c.outcome === 'won') await finishStaged(env, job, claim, ev, log);
      } else if (candidate === 3 || candidate === 4 || (candidate === 2 && job.provenance === 'kv')) {
        await deliveredChecks(env, job, ev, log);
      } else if (candidate === 1) {
        const code = 'debited_never_enqueued';
        const res = await refundTokens(env, { job: job.job_id, fence: 'recovery', code }, log);
        await afterCanceller(env, job, res, code, ev, log);
      } else if (candidate === 5) {
        const code = 'enqueued_never_claimed';
        const res = await refundTokens(env, { job: job.job_id, fence: 'canceller', code }, log);
        if (res.outcome === 'refunded') await ledgerAlarm(env, 'enqueued_never_claimed', job.job_id, { amount: res.amount }, log, job.user_id);
        await afterCanceller(env, job, res, code, ev, log);
      } else {
        const code = 'stale_running_swept';
        const res = await refundTokens(env, { job: job.job_id, fence: 'canceller', code }, log);
        await afterCanceller(env, job, res, code, ev, log);
      }
      done[`candidate_${candidate}`] = (done[`candidate_${candidate}`] ?? 0) + 1;
    } catch (err) {
      log('error', 'sweep candidate failed; left for the next run', { jobId: job.job_id, candidate, error: errText(err).slice(0, 300) });
    }
  }
  log('info', 'sweep complete', { listed: rows.length, ...done });
}

// ─── Provider status probe (cron, WD2a) ──────────────────────────────────

/**
 * One provider.status ledger row per 15-minute slot: operational, degraded or
 * down, with latency, ON FAILURE TOO. A missing row must mean the cron did not
 * run, never that the probe threw. probeRdStatus never throws and recordEvent
 * never throws; the try/catch is the last line of defence so nothing here can
 * reach the sweep that runs before it.
 */
async function probeAndRecordProviderStatus(env: Env): Promise<void> {
  const log: Logger = (level, message, extra = {}) =>
    console[level](JSON.stringify({ level, message, source: 'provider-status-probe', ...extra }));

  const now = Date.now();
  // Floor to the quarter hour so a cron that fires twice in one slot writes once.
  const slot = new Date(Math.floor(now / PROBE_SLOT_MS) * PROBE_SLOT_MS).toISOString();

  try {
    const probe = await probeRdStatus();
    const level = probe.verdict === 'operational' ? 'info' : 'warn';
    log(level, 'provider status probe', {
      verdict: probe.verdict,
      httpStatus: probe.httpStatus,
      latencyMs: probe.latencyMs,
      slot,
    });
    await recordEvent(env, {
      eventName: 'provider.status',
      level,
      dedupeKey: `${PROVIDER}:provider.status:${slot}`,
      occurredAtMs: now,
      provider: PROVIDER,
      providerStatus: probe.verdict,
      httpStatus: probe.httpStatus ?? undefined,
      latencyMs: probe.latencyMs,
      errorCode: probe.verdict === 'down' ? 'probe_down' : undefined,
      extra: { raw: probe.raw, slot },
    }, log);
  } catch (err) {
    // Should be unreachable (neither callee throws). Still emit the row so
    // the slot is not silently empty.
    const error = errText(err);
    log('warn', 'provider status probe threw', { error, slot });
    await recordEvent(env, {
      eventName: 'provider.status',
      level: 'warn',
      dedupeKey: `${PROVIDER}:provider.status:${slot}`,
      occurredAtMs: now,
      provider: PROVIDER,
      providerStatus: 'down',
      errorCode: 'probe_down',
      extra: { raw: { error }, slot },
    }, log);
  }
}

// Silence tsc for unused imports that are here for type-only reference in
// docstrings above (JobMode). Removing them would require inline type refs
// in comments that the reader can't jump to.
export type { JobMode };
