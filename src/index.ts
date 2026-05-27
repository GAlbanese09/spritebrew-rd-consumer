// spritebrew-rd-consumer/src/index.ts
//
// Cloudflare Queues consumer for SpriteBrew RD generation jobs.
//
// Architectural reference: Confluence 87490562 (queue-and-poll) + 87588866 (inline-refund).
// Established 2026-05-07 — Session 15 continued morning.
//
// Lifecycle of one message:
//   1. Read job:{jobId} from KV (idempotency check — skip if already terminal).
//   2. Write job:{jobId} = running.
//   3. Call RD with the body.
//   4a. SUCCESS         → write job:{jobId} = success with resultBase64; ack message.
//   4b. RETRYABLE FAIL  → don't refund yet; retry the message (up to max_retries).
//   4c. TERMINAL FAIL   → refund tokens, write job:{jobId} = error; ack message.
//   On final retry exhaustion handled by Cloudflare → message lands in DLQ.
//   DLQ consumer (future work) will refund there; for now ops watches the DLQ.

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

import type { JobMessage, JobState, Env } from './types';
import { callRd, callRdAnimateWithFallback, RdError } from './rdClient';
import { refundTokens } from './refund';
import { base64ToBytes, writeGalleryEntry } from './gallery';

const JOB_TTL_S = 60 * 60;           // 1h — long enough that a refresh recovers; short enough to bound storage.
const RUNNING_TIMEOUT_MS = 180_000;  // 3 min — if a 'running' job is older than this, treat as orphaned.
const MAX_ATTEMPTS = 3;              // matches max_retries in wrangler.toml

/**
 * Exponential backoff with jitter for queue-level retries on transient
 * upstream failures.
 *
 * Cloudflare Queues' msg.retry() without delaySeconds redelivers the
 * message in the next batch, which can be seconds. On a degraded
 * upstream (e.g. RD's animation model returning inference_failed in
 * waves), immediate redelivery means we hammer the same degraded
 * endpoint instead of giving it time to recover.
 *
 * Schedule given the current constants (with jitter):
 *   attempt 1 (first retry):  10-12s
 *   attempt 2 (second retry): 20-25s
 *   attempt 3 (third retry):  40-51s
 *
 * Worst-case total customer wait across all 3 retries is approximately
 * 88 seconds before the final-failure refund + friendly error message
 * is shown. This is well within acceptable customer UX and is
 * materially better than retry-with-hammering on a degraded upstream.
 *
 * If post-ship observation shows RD's degraded windows last longer than
 * the 88s worst-case gives the upstream time to recover, increase
 * `baseSeconds` from 10 to 15 or 20 in a follow-up adjustment.
 */
function calculateRetryDelaySeconds(attempt: number): number {
  const baseSeconds = 10;
  const capSeconds = 120;
  const exponential = Math.min(capSeconds, baseSeconds * Math.pow(2, attempt - 1));
  const jitterCap = Math.min(15, Math.floor(exponential * 0.3));
  const jitter = Math.floor(Math.random() * jitterCap);
  return exponential + jitter;
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
  const attempt = msg.attempts ?? 1;

  const log = (
    level: 'info' | 'warn' | 'error',
    message: string,
    extra: Record<string, unknown> = {}
  ): void => {
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

  // === Idempotency guard: read current state.
  const stateKey = `job:${jobId}`;
  const stateRaw = await env.SPRITEBREW_KV.get(stateKey);
  const state = stateRaw ? (JSON.parse(stateRaw) as JobState) : null;

  if (state?.status === 'success') {
    // Self-healing: re-write the gallery entry in case a previous attempt
    // wrote `job:` but the gallery write didn't land (or only partially did).
    // R2 put + KV put on jobId-derived keys are both overwrite-safe, so this
    // is a no-op when the gallery already exists and corrective when it
    // doesn't. Failure → msg.retry() so the gallery eventually catches up;
    // never falls through to the outer catch (which would refund a job RD
    // already produced).
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

  if (state?.status === 'error') {
    log('info', 'job already terminal (error); acking', {
      existingStatus: state.status,
    });
    msg.ack();
    return;
  }

  if (
    state?.status === 'running' &&
    Date.now() - state.startedAt < RUNNING_TIMEOUT_MS
  ) {
    log('warn', 'another invocation is running this job; acking duplicate', {
      startedAt: state.startedAt,
    });
    msg.ack();
    return;
  }

  // === Mark as running before the expensive call.
  const startedAt = Date.now();
  const runningState: JobState = {
    status: 'running',
    userId,
    mode,
    enqueuedAt: msg.body.enqueuedAt,
    startedAt,
    attempt,
  };
  await env.SPRITEBREW_KV.put(stateKey, JSON.stringify(runningState), {
    expirationTtl: JOB_TTL_S,
  });

  // === Call RD.
  try {
    const result =
      mode === 'animate'
        ? await callRdAnimateWithFallback(env.RETRO_DIFFUSION_API_KEY, body as Parameters<typeof callRdAnimateWithFallback>[1])
        : await callRd(env.RETRO_DIFFUSION_API_KEY, 'create', body);

    const completedAt = Date.now();
    log('info', 'rd success', {
      rdLatencyMs: completedAt - startedAt,
      rdBalanceCost: result.balance_cost,
    });

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

    const successState: JobState = {
      status: 'success',
      userId,
      mode,
      enqueuedAt: msg.body.enqueuedAt,
      startedAt,
      completedAt,
      resultBase64: result.base64_images[0],
      rdBalanceCost: result.balance_cost,
    };
    await env.SPRITEBREW_KV.put(stateKey, JSON.stringify(successState), {
      expirationTtl: JOB_TTL_S,
    });

    msg.ack();
    return;
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    const isRdError = err instanceof RdError;
    const retryable = isRdError ? err.retryable : true;
    const errorCode = isRdError ? `rd_${err.status}` : 'consumer_unknown';

    log('error', 'rd call failed', {
      errMsg,
      errorCode,
      retryable,
      willRetry: retryable && attempt < MAX_ATTEMPTS,
    });

    // === Retryable + retries remain → don't refund yet, just retry.
    if (retryable && attempt < MAX_ATTEMPTS) {
      const delaySeconds = calculateRetryDelaySeconds(attempt);
      log('info', 'rd call retryable, scheduling retry with backoff', {
        delaySeconds,
        nextAttempt: attempt + 1,
      });
      msg.retry({ delaySeconds });
      return;
    }

    // === Terminal failure → refund and write error state.
    try {
      const refundResult = await refundTokens(env.SPRITEBREW_KV, userId, tokenCost, jobId);
      log('info', 'refund applied', {
        alreadyApplied: refundResult.alreadyApplied,
        newBalance: refundResult.newBalance,
      });

      const errorState: JobState = {
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
      await env.SPRITEBREW_KV.put(stateKey, JSON.stringify(errorState), {
        expirationTtl: JOB_TTL_S,
      });

      msg.ack();
    } catch (refundErr) {
      const refundErrMsg = refundErr instanceof Error ? refundErr.message : String(refundErr);
      log('error', 'refund failed; retrying message', { refundErrMsg });
      // If the refund itself fails (e.g., transient KV blip), retry the whole message.
      // Idempotency on token_idempotency:refund:{jobId} ensures no double-refund on the next attempt.
      msg.retry();
    }
  }
}
