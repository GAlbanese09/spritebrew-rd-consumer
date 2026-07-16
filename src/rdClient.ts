// spritebrew-rd-consumer/src/rdClient.ts
//
// Wrapper around Retro Diffusion's /v1/inferences endpoint.
// Mirrors the request shape used by spritebrew/src/app/api/generate/route.ts
// (runCreate / runAnimate), but the consumer never imports from that repo.
// Per Confluence 87588866 the consumer is intentionally self-contained.
//
// July-15 rewrite: animate mode migrated to RD's async job API.
//   - Create mode: still synchronous via callRd (unchanged).
//   - Animate mode: submitAsyncTask → poll GET /v1/inferences/tasks/{id}.
// Redelivery-safety orchestration + status pre-flight + fallback shape
// clamp live in ../index.ts; this file exposes the primitives.

const RD_API_URL = 'https://api.retrodiffusion.ai/v1/inferences';
const RD_STATUS_URL = 'https://api.retrodiffusion.ai/v1/status';

const HTTP_USER_AGENT = 'spritebrew-rd-consumer/0.1.0 (+https://spritebrew.com)';
const HTTP_ACCEPT = 'application/json, */*';

export type RdMode = 'create' | 'animate';

export interface RdCreateBody {
  prompt: string;
  prompt_style: string;
  width: number;
  height: number;
  num_images: 1;
  remove_bg?: boolean;
  return_spritesheet?: boolean;
  reference_images?: string[];
}

export interface RdAnimateBody {
  prompt: string;
  prompt_style: string;
  width: number;
  height: number;
  num_images: 1;
  frames_duration: number;
  return_spritesheet: true;
  input_image: string;
  /** July 15: KEPT on fallback per probe C6 — animation__any_animation honors
   *  remove_bg (pixel-verified hard 1-bit alpha). Optional so producer bodies
   *  without it type-check unchanged. */
  remove_bg?: boolean;
}

/** Sync response shape (used by callRd for create) and the projection we
 *  hand back from pollAsyncTask (animate). Fields match what the existing
 *  index.ts success pipeline consumes. */
export interface RdSuccessResponse {
  base64_images: string[];
  balance_cost?: number;
  remaining_balance?: number;
}

export class RdError extends Error {
  constructor(
    message: string,
    public status: number,
    public retryable: boolean,
    public bodyText: string
  ) {
    super(message);
    this.name = 'RdError';
  }
}

/** Sync create-path call (unchanged). Animate no longer uses this — see
 *  submitAsyncTask + pollAsyncTask below. */
export async function callRd(
  apiKey: string,
  _mode: RdMode,
  body: RdCreateBody | RdAnimateBody
): Promise<RdSuccessResponse> {
  const resp = await fetch(RD_API_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-RD-Token': apiKey,
      // Explicit UA + Accept: RD's CF edge 403s the default Worker UA.
      // Diagnostic confirmed cf-ray + server:cloudflare on the 403 page.
      'User-Agent': HTTP_USER_AGENT,
      'Accept': HTTP_ACCEPT,
    },
    body: JSON.stringify(body),
  });

  const text = await resp.text();

  if (!resp.ok) {
    // 429 and 5xx are retryable; 4xx (except 429) are not — likely a bad payload.
    const retryable = resp.status === 429 || resp.status >= 500;
    throw new RdError(
      `RD ${resp.status}: ${text.slice(0, 500)}`,
      resp.status,
      retryable,
      text
    );
  }

  let parsed: RdSuccessResponse;
  try {
    parsed = JSON.parse(text) as RdSuccessResponse;
  } catch {
    throw new RdError(
      `RD returned non-JSON success response: ${text.slice(0, 500)}`,
      resp.status,
      true,  // unexpected; retry once
      text
    );
  }

  if (!parsed.base64_images || parsed.base64_images.length === 0) {
    throw new RdError(
      `RD returned empty base64_images array`,
      resp.status,
      true,
      text
    );
  }

  return parsed;
}

// ─── Async job API (animate) ───────────────────────────────────────────────

// Receipt 1 (July 15 probe): async submit response shape.
//   { "status": "accepted",
//     "task_id": "1a39b69c-576b-4d6c-adff-e7267a91b66e",
//     "message": "Inference accepted. Poll GET /v1/inferences/tasks/{task_id} for status." }
interface RdAsyncSubmitResponse {
  status: string;
  task_id: string;
  message?: string;
}

// Receipt 2 (July 15 probe): terminal-success poll response shape.
//   { "status": "succeeded", "task_id": "...", "created_at": ..., "updated_at": ...,
//     "result": { "created_at": ..., "credit_cost": 1, "balance_cost": 0.14,
//                 "base64_images": ["<base64>"], "model": "rd_fast",
//                 "remaining_credits": 0, "remaining_balance": 42.139 } }
// Non-terminal shape (pending/running/queued) has result null/absent.
// Failure record shape NOT observed on file; defensive extraction below.
interface RdTaskPollResponse {
  status: string;
  task_id?: string;
  created_at?: number;
  updated_at?: number;
  result?: {
    created_at?: number;
    credit_cost?: number;
    balance_cost?: number;
    base64_images?: string[];
    model?: string;
    remaining_credits?: number;
    remaining_balance?: number;
    error?: string;
  } | null;
  error?: string;
  message?: string;
}

const TERMINAL_SUCCESS = /^succeeded$/i;
const TERMINAL_FAILURE = /^(failed|error|errored|cancelled|canceled)$/i;
const NON_TERMINAL = /^(pending|running|queued|processing|in_progress|inprogress)$/i;

/**
 * Submit an animate job asynchronously. Returns the task_id and the submit
 * wall-clock elapsed ms (for latency telemetry).
 *
 * BILLING SAFETY (receipt 6, July 15 probe): GET /v1/inferences/tasks returns
 * 404 — RD exposes no task-list endpoint. If we lose the task_id we can never
 * recover it. Therefore this function NEVER retries: any throw here surfaces
 * as a terminal failure to the caller, which MUST persist a `submitAttemptedAt`
 * marker BEFORE calling us so the caller's redelivery guard can detect the
 * orphan and refund without resubmitting.
 *
 * BEHAVIORAL RECEIPT (July 15): async_process does NOT make the submit fast.
 * The submit HOLDS the connection for the full job duration (measured 125s at
 * 256px/8f, ~150s at 64px/8f under load) and returns "accepted" at completion.
 * Timeout budget is 10 min (Queues wall limit 15 min; leaves buffer for
 * pre-flight, poll, and pipeline).
 */
export async function submitAsyncTask(
  apiKey: string,
  body: RdAnimateBody
): Promise<{ taskId: string; submitElapsedMs: number }> {
  const startedAt = Date.now();
  let resp: Response;
  try {
    resp = await fetch(RD_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-RD-Token': apiKey,
        'User-Agent': HTTP_USER_AGENT,
        'Accept': HTTP_ACCEPT,
      },
      body: JSON.stringify({ ...body, async_process: true }),
      signal: AbortSignal.timeout(600_000),
    });
  } catch (err) {
    // Any throw (abort, network, headers timeout) may mean RD created a task
    // whose id we lost. Caller's submitAttemptedAt guard handles this.
    const elapsedMs = Date.now() - startedAt;
    const name = err instanceof Error ? err.name : 'Error';
    const message = err instanceof Error ? err.message : String(err);
    throw new RdError(
      `RD async submit threw after ${elapsedMs}ms: ${name}: ${message}`,
      0,
      false,
      message
    );
  }
  const submitElapsedMs = Date.now() - startedAt;
  const text = await resp.text();

  if (!resp.ok) {
    // Non-2xx here is treated as non-retryable because RD may still have
    // created a task. Same billing safety as the throw path.
    throw new RdError(
      `RD async submit ${resp.status}: ${text.slice(0, 500)}`,
      resp.status,
      false,
      text
    );
  }

  let parsed: RdAsyncSubmitResponse;
  try {
    parsed = JSON.parse(text) as RdAsyncSubmitResponse;
  } catch {
    throw new RdError(
      `RD async submit non-JSON: ${text.slice(0, 500)}`,
      resp.status,
      false,
      text
    );
  }

  if (typeof parsed.task_id !== 'string' || !parsed.task_id) {
    throw new RdError(
      `RD async submit missing task_id: ${text.slice(0, 500)}`,
      resp.status,
      false,
      text
    );
  }

  return { taskId: parsed.task_id, submitElapsedMs };
}

/**
 * Poll GET /v1/inferences/tasks/{taskId} until terminal or budget exhaustion.
 *
 * Cadence: 5s ±1s jitter between polls (submit blocks until completion per
 * behavioral receipt, so task is normally terminal on poll #1 anyway).
 * Budget: 180s wall clock — margin for cases where the blocking submit did
 * NOT hold for the full duration.
 *
 * Transient poll failures (network throw, 5xx, 429, JSON garbage) DO NOT
 * fail the job — we keep polling within budget. A 4xx (non-429) is terminal.
 * Unrecognized statuses count toward a 3-in-a-row streak before terminal.
 *
 * On terminal success: returns RdSuccessResponse extracted from result.
 * On terminal failure: throws RdError with message extracted defensively
 * (result.error → error → message → `rd_task_${status}`).
 * On budget exhaustion: throws RdError with message containing
 * "exceeded budget" so the caller can map to errorCode 'rd_async_timeout'.
 */
export async function pollAsyncTask(
  apiKey: string,
  taskId: string,
  budgetMs = 180_000
): Promise<RdSuccessResponse> {
  const startedAt = Date.now();
  const pollUrl = `${RD_API_URL}/tasks/${taskId}`;
  let unknownStatusStreak = 0;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (Date.now() - startedAt > budgetMs) {
      throw new RdError(
        `RD async poll exceeded budget ${budgetMs}ms for task ${taskId}`,
        0,
        false,
        ''
      );
    }
    // 5s ±1s jitter — 4000..6000ms.
    const delayMs = 4000 + Math.floor(Math.random() * 2000);
    await new Promise((r) => setTimeout(r, delayMs));

    let resp: Response;
    try {
      resp = await fetch(pollUrl, {
        method: 'GET',
        headers: {
          'X-RD-Token': apiKey,
          'User-Agent': HTTP_USER_AGENT,
          'Accept': HTTP_ACCEPT,
        },
        signal: AbortSignal.timeout(30_000),
      });
    } catch {
      // Transient network — keep polling within budget.
      continue;
    }

    if (resp.status === 429 || resp.status >= 500) {
      // Transient — keep polling.
      continue;
    }

    const text = await resp.text();
    if (!resp.ok) {
      // 4xx (non-429) is terminal.
      throw new RdError(
        `RD async poll ${resp.status} for task ${taskId}: ${text.slice(0, 500)}`,
        resp.status,
        false,
        text
      );
    }

    let record: RdTaskPollResponse;
    try {
      record = JSON.parse(text) as RdTaskPollResponse;
    } catch {
      // Non-JSON garbage — keep polling within budget.
      continue;
    }

    const status = String(record?.status ?? '');

    if (TERMINAL_SUCCESS.test(status)) {
      const images = record.result?.base64_images;
      if (!images || images.length === 0) {
        throw new RdError(
          `RD async task ${status} but no base64_images in result for task ${taskId}`,
          resp.status,
          false,
          text
        );
      }
      return {
        base64_images: images,
        balance_cost: record.result?.balance_cost,
        remaining_balance: record.result?.remaining_balance,
      };
    }

    if (TERMINAL_FAILURE.test(status)) {
      // Defensive error extraction: shape wasn't observed on file.
      const errMsg =
        record.result?.error ??
        record.error ??
        record.message ??
        `rd_task_${status}`;
      throw new RdError(
        `RD async task ${status} for task ${taskId}: ${errMsg}`,
        resp.status,
        false,
        text
      );
    }

    if (NON_TERMINAL.test(status)) {
      unknownStatusStreak = 0;
      continue;
    }

    // Unrecognized status — count toward the 3-in-a-row failure trigger.
    unknownStatusStreak++;
    if (unknownStatusStreak >= 3) {
      throw new RdError(
        `RD async task returned unknown status "${status}" for 3 consecutive polls (task ${taskId})`,
        resp.status,
        false,
        text
      );
    }
  }
}

// ─── Status pre-flight ─────────────────────────────────────────────────────

/**
 * Best-effort GET https://api.retrodiffusion.ai/v1/status. Returns:
 *   - 'ok'         when response.animations === 'ok'
 *   - 'degraded'   when response.animations is present but not 'ok'
 *   - 'unknown'    on fetch throw, non-2xx, or unparseable JSON (fail open)
 *
 * No auth required by RD for /v1/status; sends UA/Accept anyway for
 * consistency with other requests. 10s timeout — a status check must never
 * hold the worker long enough to matter.
 */
export async function checkRdAnimationsStatus(): Promise<'ok' | 'degraded' | 'unknown'> {
  try {
    const resp = await fetch(RD_STATUS_URL, {
      method: 'GET',
      headers: {
        'User-Agent': HTTP_USER_AGENT,
        'Accept': HTTP_ACCEPT,
      },
      signal: AbortSignal.timeout(10_000),
    });
    if (!resp.ok) return 'unknown';
    const data = (await resp.json()) as { animations?: unknown };
    return data.animations === 'ok' ? 'ok' : 'degraded';
  } catch {
    return 'unknown';
  }
}
