// spritebrew-rd-consumer/src/index.ts
//
// Cloudflare Queues consumer for SpriteBrew RD generation jobs.
//
// Architectural reference: Confluence 87490562 (queue-and-poll) + 87588866 (inline-refund).
// Established 2026-05-07 — Session 15 continued morning.
// July-15 rewrite: animate mode migrated to RD's async job API.
//   Create mode stays synchronous — the sync endpoint's happy path is fast
//   enough that async_process adds no value and risks the same billing
//   orphan class we now guard against on animate.
//
// Lifecycle of one message (post July-15):
//   0. Read job:{jobId} from KV.
//      - Terminal (success/error)                              → ack (idempotent).
//      - Running + taskId (any mode wired async)               → resume poll.
//      - Running + submitAttemptedAt + !taskId                 → orphaned submit
//        (RD may have accepted our body but we lost the id; there is NO
//        recovery path — GET /v1/inferences/tasks 404s per probe). Refund
//        with errorCode 'rd_submit_orphaned_redelivery' and ack.
//      - Legacy running (no taskId, no submitAttemptedAt, within
//        RUNNING_TIMEOUT_MS)                                   → ack duplicate.
//   1. Status pre-flight (animate only): GET /v1/status. On non-ok + attempt<3,
//      msg.retry({delaySeconds:60}). At attempt≥3, proceed regardless.
//   2. Write running state (with submitAttemptedAt for animate).
//   3. RD call:
//      - create   → callRd (sync, unchanged).
//      - animate  → submitAsyncTask → persist taskId → pollAsyncTask.
//        On primary failure of rd_advanced_animation__*: fallback via
//        animation__any_animation clamped to 64×64 (probe C5). If fallback
//        input isn't available and original >64px, skip fallback and let
//        the primary error propagate.
//   4a. SUCCESS         → gallery + KV success + ack.
//   4b. RETRYABLE FAIL  → msg.retry(), no refund yet.
//   4c. TERMINAL FAIL   → refund + KV error + ack.
//
// ============================================================================
// CONCURRENT-WRITE RISK (KNOWN, INHERITED FROM PAGES, NOT MITIGATED HERE)
// ============================================================================
// Cloudflare KV has no compare-and-swap. The consumer's refund-on-failure
// performs read-modify-write on token_balance:{userId}. A concurrent Stripe
// purchase webhook can collide on the same record. Worst case: a sub-second
// timing window where both read pre-update balance and the slower writer
// overwrites the faster writer's update — losing one operation.
//
// This race exists on the synchronous code path today (Pages-side creditTokens
// has the same read-modify-write pattern). The consumer does not introduce
// new risk; it slightly extends the window because the refund now runs after
// the queue+consumer roundtrip rather than synchronously inside the producer
// request. Acceptable inheritance for v1.
//
// Future fix: migrate token_balance to Durable Objects (atomic per-userId
// instance) or D1 (transactional writes). Tracked separately. Do NOT block
// this build on it.
// ============================================================================

import type { JobMessage, JobState, JobStateRunning, JobStateSuccess, JobStateError, JobMode, Env } from './types';
import type { RdAnimateBody, RdSuccessResponse } from './rdClient';
import {
  callRd,
  submitAsyncTask,
  pollAsyncTask,
  checkRdAnimationsStatus,
  RdError,
} from './rdClient';
import { refundTokens } from './refund';
import { base64ToBytes, writeGalleryEntry } from './gallery';

const JOB_TTL_S = 60 * 60;           // 1h — long enough that a refresh recovers; short enough to bound storage.
const RUNNING_TIMEOUT_MS = 180_000;  // 3 min — if a legacy 'running' job is older than this, treat as orphaned.
const MAX_ATTEMPTS = 3;              // matches max_retries in wrangler.toml
const STATUS_RETRY_DELAY_S = 60;     // pre-flight backoff between attempts

type Logger = (
  level: 'info' | 'warn' | 'error',
  message: string,
  extra?: Record<string, unknown>
) => void;

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
 * CAS-approximating state write. KV has no compare-and-swap, but re-reading
 * immediately before the put shrinks the redelivery overwrite window from the
 * full handler duration (~150s+) to a few ms. If the state is already terminal
 * (success or error), we skip the write and log the attempted transition — a
 * slower invocation cannot then clobber a terminal state a faster concurrent
 * redelivery already committed. Route every non-initial state write through
 * this: taskId-persist after submit, recordSuccess, recordFailure.
 */
async function writeStateUnlessTerminal(
  env: Env,
  stateKey: string,
  nextState: JobState,
  log: Logger
): Promise<void> {
  const raw = await env.SPRITEBREW_KV.get(stateKey);
  const cur = raw ? (JSON.parse(raw) as JobState) : null;
  if (cur?.status === 'success' || cur?.status === 'error') {
    log('warn', 'state already terminal - write skipped', {
      currentStatus: cur.status,
      attemptedStatus: nextState.status,
    });
    return;
  }
  await env.SPRITEBREW_KV.put(stateKey, JSON.stringify(nextState), {
    expirationTtl: JOB_TTL_S,
  });
}

export default {
  async queue(batch: MessageBatch<JobMessage>, env: Env, _ctx: ExecutionContext): Promise<void> {
    for (const msg of batch.messages) {
      await handleMessage(msg, env);
    }
  },
} satisfies ExportedHandler<Env, JobMessage>;

async function handleMessage(
  msg: Message<JobMessage>,
  env: Env
): Promise<void> {
  const { jobId, userId, tokenCost, mode, body } = msg.body;
  const fallbackInputImage = msg.body.fallbackInputImage;
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

  const stateKey = `job:${jobId}`;
  const stateRaw = await env.SPRITEBREW_KV.get(stateKey);
  const state = stateRaw ? (JSON.parse(stateRaw) as JobState) : null;

  // === 0a. Terminal — success (with self-heal for gallery-write race).
  if (state?.status === 'success') {
    try {
      const pngBytes = base64ToBytes(state.resultBase64);
      await writeGalleryEntry(
        env,
        {
          jobId,
          userId,
          pngBytes,
          prompt: body.prompt,
          style: body.prompt_style,
          mode,
          createdAt: state.completedAt,
        },
        log
      );
    } catch (galleryErr) {
      log('error', 'self-healing gallery write failed; retrying message', {
        error: galleryErr instanceof Error ? galleryErr.message : String(galleryErr),
      });
      msg.retry();
      return;
    }
    log('info', 'job already terminal (success); self-heal complete; acking');
    msg.ack();
    return;
  }

  // === 0b. Terminal — error.
  if (state?.status === 'error') {
    log('info', 'job already terminal (error); acking', {
      existingStatus: state.status,
    });
    msg.ack();
    return;
  }

  // === 0c. Redelivery with taskId → resume polling that task.
  //     Applies to any running-state that captured a taskId (animate today).
  //     We SKIP status pre-flight and SKIP submission entirely: the task
  //     already exists on RD's side. Poll-budget exhaustion here uses the
  //     SAME retry-not-refund policy as fix 1 below: RD task is still live
  //     and billing; redelivery re-enters this branch with a fresh budget.
  //     Only on the final attempt do we terminal-fail with rd_async_timeout.
  if (state?.status === 'running' && state.taskId) {
    log('info', 'redelivery: resuming poll of existing task', {
      taskId: state.taskId,
      originalStartedAt: state.startedAt,
      originalSubmitAttemptedAt: state.submitAttemptedAt,
    });
    try {
      const result = await pollAsyncTask(env.RETRO_DIFFUSION_API_KEY, state.taskId);
      log('info', 'resume-poll succeeded', {
        taskId: state.taskId,
        balance_cost: result.balance_cost,
        remaining_balance: result.remaining_balance,
      });
      await recordSuccess(env, msg, state.startedAt, result, log);
    } catch (err) {
      if (isPollBudgetExceeded(err) && attempt < MAX_ATTEMPTS) {
        log('warn', 'resume-poll budget exhausted; task still live; redelivering to re-poll same taskId', {
          taskId: state.taskId,
          attempt,
          nextAttempt: attempt + 1,
        });
        msg.retry({ delaySeconds: 30 });
        return;
      }
      // Final attempt or non-budget error → terminal.
      // For a poll-budget final failure we log the bounded orphan class: the
      // refund we're about to fire may precede a late RD completion (bill
      // lands, we already refunded — visible in ops as duplicate cost).
      if (isPollBudgetExceeded(err)) {
        log('error', 'resume-poll budget exhausted on final attempt; refunding despite live task (bounded orphan)', {
          taskId: state.taskId,
          attempt,
        });
      }
      await recordFailure(env, msg, state.startedAt, err, classifyError(err), log);
    }
    return;
  }

  // === 0d. Redelivery with submitAttemptedAt + no taskId → orphaned submit.
  //     RD may or may not have created a task from the previous invocation's
  //     submit; there is no recovery (no task-list endpoint per probe). Fail
  //     terminally with the marker code so ops can identify the class.
  if (state?.status === 'running' && state.submitAttemptedAt && !state.taskId) {
    log('error', 'redelivery: submit orphaned (no task_id captured); no recovery available', {
      submitAttemptedAt: state.submitAttemptedAt,
    });
    await recordFailure(
      env,
      msg,
      state.startedAt,
      new Error('submit orphaned on redelivery — RD may have created a task we lost'),
      'rd_submit_orphaned_redelivery',
      log
    );
    return;
  }

  // === 0e. Legacy running (no async fields) within timeout → treat as
  //     concurrent duplicate. Backwards-compat: old pre-July-15 running
  //     records lack the new markers and land here.
  if (
    state?.status === 'running' &&
    !state.taskId &&
    !state.submitAttemptedAt &&
    Date.now() - state.startedAt < RUNNING_TIMEOUT_MS
  ) {
    log('warn', 'another invocation is running this job; acking duplicate', {
      startedAt: state.startedAt,
    });
    msg.ack();
    return;
  }

  // === 1. Status pre-flight (animate only). GET is best-effort; a fetch
  //     error is treated as 'unknown' → fail open → proceed.
  if (mode === 'animate') {
    const rdStatus = await checkRdAnimationsStatus();
    const shouldRetry = rdStatus === 'degraded' && attempt < MAX_ATTEMPTS;
    log('info', 'rd status pre-flight', {
      rdStatus,
      decision: shouldRetry ? `retry in ${STATUS_RETRY_DELAY_S}s` : 'proceed',
    });
    if (shouldRetry) {
      msg.retry({ delaySeconds: STATUS_RETRY_DELAY_S });
      return;
    }
  }

  // === 2. Write running state. For animate we ALSO mark submitAttemptedAt
  //     before the submit fires, so a mid-submit crash is detectable on
  //     redelivery. Create keeps the legacy shape (no async fields).
  const startedAt = Date.now();
  const runningState: JobStateRunning = {
    status: 'running',
    userId,
    mode,
    enqueuedAt: msg.body.enqueuedAt,
    startedAt,
    attempt,
  };
  if (mode === 'animate') {
    runningState.submitAttemptedAt = startedAt;
  }
  await env.SPRITEBREW_KV.put(stateKey, JSON.stringify(runningState), {
    expirationTtl: JOB_TTL_S,
  });

  // === 3. Call RD.
  try {
    const result: RdSuccessResponse =
      mode === 'animate'
        ? await runAnimateAsync(
            env,
            stateKey,
            runningState,
            body as RdAnimateBody,
            fallbackInputImage,
            log
          )
        : await callRd(env.RETRO_DIFFUSION_API_KEY, 'create', body);

    log('info', 'rd success', {
      rdLatencyMs: Date.now() - startedAt,
      rdBalanceCost: result.balance_cost,
      rdRemainingBalance: result.remaining_balance,
    });

    await recordSuccess(env, msg, startedAt, result, log);
    return;
  } catch (err) {
    const errorCode = classifyError(err);
    const isRdError = err instanceof RdError;
    const retryable = isRdError ? err.retryable : true;
    const errMsg = err instanceof Error ? err.message : String(err);

    // Fix 1: poll-budget exhaustion on the FRESH run's primary poll.
    // runAnimateAsync must NOT fallback (see comment there); the task is
    // still live and holds the taskId in state.taskId. Redeliver so guard
    // 0c resumes polling the same task with a fresh budget. No new submit,
    // no new bill, no fallback. Only the final attempt terminals.
    if (isPollBudgetExceeded(err) && attempt < MAX_ATTEMPTS) {
      log('warn', 'primary poll budget exhausted; task still live; redelivering to re-poll same taskId', {
        errMsg,
        attempt,
        nextAttempt: attempt + 1,
      });
      msg.retry({ delaySeconds: 30 });
      return;
    }
    if (isPollBudgetExceeded(err)) {
      log('error', 'primary poll budget exhausted on final attempt; refunding despite live task (bounded orphan)', {
        errMsg,
        attempt,
      });
    }

    log('error', 'rd call failed', {
      errMsg,
      errorCode,
      retryable,
      willRetry: retryable && attempt < MAX_ATTEMPTS,
    });

    // Retryable + retries remain → don't refund yet, just retry.
    // Async submit errors are NON-retryable by construction (billing
    // safety: submit orphans, per receipt 6, must not resubmit) so they'll
    // skip this branch and fall through to refund.
    if (retryable && attempt < MAX_ATTEMPTS) {
      msg.retry();
      return;
    }

    await recordFailure(env, msg, startedAt, err, errorCode, log);
  }
}

// ─── Animate async orchestration ───────────────────────────────────────────

/**
 * Full animate flow: primary async submit + poll, with a single-attempt
 * fallback on failure. Persists the task_id to KV immediately after each
 * submit so redelivery can resume polling instead of resubmitting (billing
 * safety per receipt 6: no task-list recovery endpoint).
 */
async function runAnimateAsync(
  env: Env,
  stateKey: string,
  runningState: JobStateRunning,
  body: RdAnimateBody,
  fallbackInputImage: string | undefined,
  log: Logger
): Promise<RdSuccessResponse> {
  // Primary attempt.
  let primaryErr: unknown;
  try {
    const { taskId, submitElapsedMs } = await submitAsyncTask(
      env.RETRO_DIFFUSION_API_KEY,
      body
    );
    log('info', 'primary async submit accepted', {
      taskId,
      submitElapsedMs,
      promptStyle: body.prompt_style,
      width: body.width,
      height: body.height,
    });

    // Persist task_id BEFORE polling so a poll-time crash resumes correctly.
    // Routed through writeStateUnlessTerminal (fix 2): a concurrent
    // redelivery may have already committed a terminal state; we must not
    // overwrite it here with a stale 'running' record.
    const withTaskId: JobStateRunning = { ...runningState, taskId };
    await writeStateUnlessTerminal(env, stateKey, withTaskId, log);

    return await pollAsyncTask(env.RETRO_DIFFUSION_API_KEY, taskId);
  } catch (err) {
    primaryErr = err;
  }

  // Fix 1: poll-budget exhaustion is NOT a fallback trigger. The RD task
  // is still live and will bill; falling back would submit a second job for
  // one request AND discard a retrievable result. Re-throw and let the
  // outer handler redeliver so guard 0c can resume-poll the same taskId.
  // Fallback stays eligible ONLY for terminal task-failure statuses, poll
  // 4xx (task rejected/vanished), and submit orphans — cases where the
  // primary is genuinely unrecoverable.
  if (isPollBudgetExceeded(primaryErr)) {
    throw primaryErr;
  }

  // Fallback consideration. Only for RdErrors on rd_advanced_animation__* —
  // preserves the pre-existing semantics (fallback is a rescue for that
  // specific style family). Other errors propagate unchanged.
  if (
    !(primaryErr instanceof RdError) ||
    !body.prompt_style.startsWith('rd_advanced_animation__')
  ) {
    throw primaryErr;
  }

  // Fallback shape (probe C3/C4/C5/C6):
  //   - prompt_style: animation__any_animation
  //   - width/height CLAMPED to 64 (C5: >64 is deterministic 400 on this style)
  //   - frames_duration KEPT (C4: tolerated at 64px)
  //   - remove_bg KEPT (C6: honored on any_animation, hard 1-bit alpha)
  //   - input_image: envelope's fallbackInputImage if present; otherwise
  //     reuse body.input_image only when the original request was already
  //     64px. Oversized reuse = deterministic 400, so we skip fallback.
  const originalSize = body.width;
  const hasEnvelopeInput = typeof fallbackInputImage === 'string' && fallbackInputImage.length > 0;
  const canUseOriginalInput = originalSize <= 64 && body.height <= 64;

  if (!hasEnvelopeInput && !canUseOriginalInput) {
    log('warn', 'fallback unavailable: no 64px input in envelope; primary failure will propagate', {
      originalWidth: body.width,
      originalHeight: body.height,
      primaryError: primaryErr.message,
    });
    throw primaryErr;
  }

  const fallbackBody: RdAnimateBody = {
    ...body,
    prompt_style: 'animation__any_animation',
    width: 64,
    height: 64,
    input_image: hasEnvelopeInput ? fallbackInputImage : body.input_image,
    // frames_duration and remove_bg carried via spread — no delete.
  };

  log('info', 'attempting fallback', {
    reason: 'primary rd_advanced_animation__ failed',
    primaryStatus: primaryErr.status,
    primaryMessage: primaryErr.message,
    fallbackShape: '64x64',
    usedEnvelopeInput: hasEnvelopeInput,
    fallbackKeepsRemoveBg: body.remove_bg === true,
  });

  const { taskId: fallbackTaskId, submitElapsedMs: fallbackSubmitMs } =
    await submitAsyncTask(env.RETRO_DIFFUSION_API_KEY, fallbackBody);

  log('info', 'fallback async submit accepted', {
    taskId: fallbackTaskId,
    submitElapsedMs: fallbackSubmitMs,
  });

  // Overwrite primary's taskId — one taskId in the running state at a time.
  // On redelivery, we resume THIS task (the last one attempted).
  // Same terminal-guard as the primary persist (fix 2).
  const withFallbackTaskId: JobStateRunning = { ...runningState, taskId: fallbackTaskId };
  await writeStateUnlessTerminal(env, stateKey, withFallbackTaskId, log);

  return await pollAsyncTask(env.RETRO_DIFFUSION_API_KEY, fallbackTaskId);
}

// ─── Success / failure recorders (shared by fresh and resume paths) ────────

async function recordSuccess(
  env: Env,
  msg: Message<JobMessage>,
  startedAt: number,
  result: RdSuccessResponse,
  log: Logger
): Promise<void> {
  const { jobId, userId, mode, body } = msg.body;
  const stateKey = `job:${jobId}`;
  const completedAt = Date.now();

  // Fix 2 special case: we hold a fresh RD result, but the KV state may
  // ALREADY be terminal 'error' because a concurrent redelivery hit the
  // final-attempt poll-timeout branch and refunded. Detect that here:
  // skip the gallery write AND the state write, still ack, log loudly.
  // Ops metric: 'result arrived after refund' counts the bounded orphan
  // class — RD delivered the result we already paid for and refunded.
  const preRaw = await env.SPRITEBREW_KV.get(stateKey);
  const pre = preRaw ? (JSON.parse(preRaw) as JobState) : null;
  if (pre?.status === 'error') {
    log('error', 'result arrived after refund', {
      preExistingErrorCode: pre.errorCode,
      preExistingRefunded: pre.refunded,
      rdBalanceCost: result.balance_cost,
    });
    msg.ack();
    return;
  }
  if (pre?.status === 'success') {
    // Redelivery re-race: another invocation already wrote success + gallery.
    // Ack without duplicating the R2 write.
    log('info', 'success state already recorded by concurrent invocation; acking');
    msg.ack();
    return;
  }

  // Gallery write FIRST (R2 PNG + KV gen: index). Tight nested try/catch:
  // a failure here calls msg.retry() directly and MUST NOT fall through
  // to the outer catch — the outer catch can refund tokens, and a
  // post-RD failure on a generation RD already produced must not be
  // double-refunded on the eventual retry.
  try {
    const pngBytes = base64ToBytes(result.base64_images[0]);
    await writeGalleryEntry(
      env,
      {
        jobId,
        userId,
        pngBytes,
        prompt: body.prompt,
        style: body.prompt_style,
        mode,
        createdAt: completedAt,
      },
      log
    );
  } catch (galleryErr) {
    log('error', 'gallery write failed; retrying message', {
      error: galleryErr instanceof Error ? galleryErr.message : String(galleryErr),
    });
    msg.retry();
    return;
  }

  const successState: JobStateSuccess = {
    status: 'success',
    userId,
    mode,
    enqueuedAt: msg.body.enqueuedAt,
    startedAt,
    completedAt,
    resultBase64: result.base64_images[0],
    rdBalanceCost: result.balance_cost,
  };
  // writeStateUnlessTerminal covers the very-narrow race where a
  // concurrent invocation transitioned to terminal between the pre-check
  // above and here. Rare but possible: the pre-check is milliseconds ago,
  // the write is milliseconds from now, both interleavable across two
  // Workers. Terminal state (either success or error) wins.
  await writeStateUnlessTerminal(env, stateKey, successState, log);

  msg.ack();
}

async function recordFailure(
  env: Env,
  msg: Message<JobMessage>,
  startedAt: number,
  err: unknown,
  errorCode: string,
  log: Logger
): Promise<void> {
  const { jobId, userId, mode, tokenCost } = msg.body;
  const stateKey = `job:${jobId}`;
  const errMsg = err instanceof Error ? err.message : String(err);
  const attempt = msg.attempts ?? 1;

  try {
    const refundResult = await refundTokens(env.SPRITEBREW_KV, userId, tokenCost, jobId);
    log('info', 'refund applied', {
      alreadyApplied: refundResult.alreadyApplied,
      newBalance: refundResult.newBalance,
      errorCode,
    });

    const errorState: JobStateError = {
      status: 'error',
      userId,
      mode,
      enqueuedAt: msg.body.enqueuedAt,
      failedAt: Date.now(),
      error: errMsg,
      errorCode,
      attempts: attempt,
      refunded: !refundResult.alreadyApplied,
    };
    // Reference startedAt for latency telemetry when useful (kept in log
    // rather than in the state record — schema stays byte-identical).
    log('info', 'terminal failure recorded', {
      errorCode,
      latencyMs: Date.now() - startedAt,
    });
    // writeStateUnlessTerminal: if a concurrent invocation already wrote
    // 'success' (RD result arrived between our refund and this write), we
    // must NOT overwrite that success with an error record. Success wins.
    await writeStateUnlessTerminal(env, stateKey, errorState, log);

    msg.ack();
  } catch (refundErr) {
    const refundErrMsg = refundErr instanceof Error ? refundErr.message : String(refundErr);
    log('error', 'refund failed; retrying message', { refundErrMsg });
    // If the refund itself fails (e.g., transient KV blip), retry the whole
    // message. Idempotency on token_idempotency:refund:{jobId} ensures no
    // double-refund on the next attempt.
    msg.retry();
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

// Silence tsc for unused imports that are here for type-only reference in
// docstrings above (JobMode). Removing them would require inline type refs
// in comments that the reader can't jump to.
export type { JobMode };
