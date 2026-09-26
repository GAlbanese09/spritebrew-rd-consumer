// spritebrew-rd-consumer/src/jobState.ts
//
// One write path for the job record (status-store.md 001, step 2). The Pages
// status route reads R2 first because R2 reads are strongly consistent, while
// KV serves this key from an edge cache for 30 to 60 s. So the full record,
// the exact JSON that goes to KV, is mirrored to R2 at jobs/{jobId}.json
// first, then written to KV exactly as before (same key, same TTL).
//
// The R2 write is best effort, the same contract as recordEvent: a failure
// logs at warn and never fails, delays beyond the one retry, or refunds a job.
// The route falls back to KV when R2 has nothing, so a lost mirror degrades
// to the old behaviour. KV stays the source for the consumer's own guard reads
// (phase 2 moves them).

import type { Env, JobState } from './types';

export const JOB_TTL_S = 60 * 60; // 1h: long enough that a refresh recovers; short enough to bound storage.

/** R2 allows about one write per second per key and answers a faster write
 *  with error 10058 ("Reduce your concurrent request rate for the same
 *  object"). The taskId persist lands about 150 ms after `running`, so
 *  expect this retry on most animate jobs. */
const R2_RETRY_DELAY_MS = 1_100;

type Logger = (
  level: 'info' | 'warn' | 'error',
  message: string,
  extra?: Record<string, unknown>
) => void;

export function jobStateR2Key(jobId: string): string {
  return `jobs/${jobId}.json`;
}

function errText(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

async function putJobStateR2(env: Env, jobId: string, body: string, log: Logger): Promise<void> {
  const key = jobStateR2Key(jobId);
  const put = () =>
    env.GALLERY_BUCKET.put(key, body, { httpMetadata: { contentType: 'application/json' } });
  try {
    await put();
    return;
  } catch (err) {
    const msg = errText(err);
    log('warn', 'job state R2 put failed, retrying once', {
      r2Key: key,
      error: msg,
      rateLimited: /10058|concurrent request rate/i.test(msg),
    });
  }
  await new Promise((resolve) => setTimeout(resolve, R2_RETRY_DELAY_MS));
  try {
    await put();
  } catch (err) {
    log('warn', 'job state R2 put failed after retry; KV only for this write', {
      r2Key: key,
      error: errText(err),
    });
  }
}

/**
 * Writes the job record to R2 (jobs/{jobId}.json), then to KV (job:{jobId}),
 * byte for byte the same JSON. Unconditional: callers that must not overwrite
 * a terminal state go through writeStateUnlessTerminal below.
 */
export async function putJobState(env: Env, jobId: string, state: JobState, log: Logger): Promise<void> {
  const body = JSON.stringify(state);
  await putJobStateR2(env, jobId, body, log);
  await env.SPRITEBREW_KV.put(`job:${jobId}`, body, { expirationTtl: JOB_TTL_S });
}

/**
 * CAS-approximating state write. KV has no compare-and-swap, but re-reading
 * immediately before the put shrinks the redelivery overwrite window from the
 * full handler duration (~150s+) to a few ms. If the state is already terminal
 * (success or error), we skip the write and log the attempted transition: a
 * slower invocation cannot then clobber a terminal state a faster concurrent
 * redelivery already committed. Route every non-initial state write through
 * this: taskId-persist after submit, recordSuccess, recordFailure, and the
 * dead-letter handler.
 */
export async function writeStateUnlessTerminal(
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
  // Every caller builds stateKey as `job:{jobId}` (the sweep through
  // SWEEP_KEY_PREFIX); putJobState rebuilds it from the jobId.
  await putJobState(env, stateKey.slice('job:'.length), nextState, log);
}

/**
 * The job record, strong store first: the R2 mirror, then KV. Returns the
 * parsed record and where it came from, or null when neither holds one (or
 * neither parses). Read failures count as a miss.
 */
export async function readJobState(
  env: Env,
  jobId: string
): Promise<{ state: JobState; source: 'r2' | 'kv' } | null> {
  try {
    const obj = await env.GALLERY_BUCKET.get(jobStateR2Key(jobId));
    if (obj) return { state: JSON.parse(await obj.text()) as JobState, source: 'r2' };
  } catch { /* fall through to KV */ }
  try {
    const raw = await env.SPRITEBREW_KV.get(`job:${jobId}`);
    if (raw) return { state: JSON.parse(raw) as JobState, source: 'kv' };
  } catch { /* miss */ }
  return null;
}
