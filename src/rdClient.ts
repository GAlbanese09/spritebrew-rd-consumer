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
  const resp = await fetch(RD_API_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-RD-Token': apiKey,
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
      console.warn(
        '[rdClient] rd_advanced_animation__ failed; falling back to animation__any_animation',
        JSON.stringify({
          originalStyle: body.prompt_style,
          status: err.status,
          message: err.message,
        })
      );
      const fallbackBody: RdAnimateBody = {
        ...body,
        prompt_style: 'animation__any_animation',
      };
      return await callRd(apiKey, 'animate', fallbackBody);
    }
    throw err;
  }
}
