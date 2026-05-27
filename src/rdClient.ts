// spritebrew-rd-consumer/src/rdClient.ts
//
// Wrapper around Retro Diffusion's /v1/inferences endpoint.
// Mirrors the request shape used by spritebrew/src/app/api/generate/route.ts
// (runCreate / runAnimate), but the consumer never imports from that repo.
// Per Confluence 87588866 the consumer is intentionally self-contained.

const RD_API_URL = 'https://api.retrodiffusion.ai/v1/inferences';

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
}

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

export async function callRd(
  apiKey: string,
  _mode: RdMode,
  body: RdCreateBody | RdAnimateBody
): Promise<RdSuccessResponse> {
  let resp: Response;
  let text: string;
  try {
    resp = await fetch(RD_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-RD-Token': apiKey,
        // Explicit UA + Accept: RD's CF edge 403s the default Worker UA.
        // Diagnostic confirmed cf-ray + server:cloudflare on the 403 page.
        'User-Agent': 'spritebrew-rd-consumer/0.1.0 (+https://spritebrew.com)',
        'Accept': 'application/json, */*',
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(60_000), // 60s fail-fast on hung connections
    });
    text = await resp.text();
  } catch (err) {
    if (err instanceof DOMException && err.name === 'TimeoutError') {
      throw new RdError('RD request timed out after 60s', 0, true, '');
    }
    throw err; // re-throw network errors, etc.
  }

  if (!resp.ok) {
    // 429 and 5xx are retryable; 4xx (except 429) are not — likely a bad payload.
    let retryable = resp.status === 429 || resp.status >= 500;

    // Narrow rule: RD's "inference_failed" 400s are observed to be transient
    // upstream failures (RD's own model worker pool returning an error), not
    // genuine client-side input errors. Per the RD Reliability + Escalation
    // Runbook, retry these.
    if (resp.status === 400 && !retryable) {
      try {
        const parsed = JSON.parse(text);
        if (
          parsed?.detail?.code === 'inference_failed' &&
          parsed?.detail?.message === 'Unable to run inference.'
        ) {
          retryable = true;
        }
      } catch {
        // text wasn't JSON — leave retryable as-is (false for 400)
      }
    }

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

// Retry-with-fallback pattern preserved from spritebrew/src/app/api/generate/route.ts
// (runAnimate's catch block, ~lines 445-454). If rd_advanced_animation__* fails on
// certain frames_duration values, fall back to animation__any_animation. This must
// NOT be lost in the refactor.
export async function callRdAnimateWithFallback(
  apiKey: string,
  body: RdAnimateBody
): Promise<RdSuccessResponse> {
  try {
    return await callRd(apiKey, 'animate', body);
  } catch (err) {
    if (err instanceof RdError && body.prompt_style.startsWith('rd_advanced_animation__')) {
      console.warn(JSON.stringify({
        level: 'warn',
        message: 'rd_advanced_animation__ failed; falling back to animation__any_animation',
        originalStyle: body.prompt_style,
        fallbackStyle: 'animation__any_animation',
        errMsg: err instanceof Error ? err.message : String(err),
      }));
      const fallbackBody: RdAnimateBody = {
        ...body,
        prompt_style: 'animation__any_animation',
      };
      return await callRd(apiKey, 'animate', fallbackBody);
    }
    throw err;
  }
}
