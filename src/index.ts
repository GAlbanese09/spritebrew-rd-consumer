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

const JOB_TTL_S = 60 * 60;           // 1h — long enough that a refresh recovers; short enough to bound storage.
const RUNNING_TIMEOUT_MS = 180_000;  // 3 min — if a 'running' job is older than this, treat as orphaned.
const MAX_ATTEMPTS = 3;              // matches max_retries in wrangler.toml

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

  if (state?.status === 'success' || state?.status === 'error') {
    log('info', 'job already terminal; acking', { existingStatus: state.status });
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
      msg.retry();
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
