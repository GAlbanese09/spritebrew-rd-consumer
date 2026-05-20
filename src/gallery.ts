// spritebrew-rd-consumer/src/gallery.ts
//
// Phase 2 of the Generation Gallery Backend (Confluence 93028353).
// Writes the generated PNG to R2 and an index row to KV under the
// `gen:{userId}:{invTs}:{jobId}` key. The index row is stored in KV
// metadata (≤1024 bytes); the KV value is an empty string so `KV.list()`
// returns rows without a per-entry GET.
//
// All writes are idempotent on jobId-derived keys: R2.put overwrites by
// default, and the KV key is deterministic from `(userId, createdAt, jobId)`
// with `createdAt = completedAt` on both the success and self-healing
// re-delivery paths — so the same slot is rewritten, never duplicated.

import type { Env, GalleryEntryV1, JobMode } from './types';

const PROMPT_MAX_LEN = 300;
const ANIMATE_STYLE_PREFIX = 'rd_advanced_animation__';

export function base64ToBytes(base64: string): Uint8Array {
  return Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));
}

export function truncatePrompt(prompt: string): string {
  return prompt.slice(0, PROMPT_MAX_LEN);
}

/**
 * For animate mode, strip the `rd_advanced_animation__` prefix to recover
 * the bare action (e.g. `walking`). Pretty-printing happens on the read
 * side; we just preserve the raw suffix here.
 */
export function actionFromPromptStyle(promptStyle: string): string | undefined {
  if (!promptStyle.startsWith(ANIMATE_STYLE_PREFIX)) return undefined;
  const suffix = promptStyle.slice(ANIMATE_STYLE_PREFIX.length);
  return suffix.length > 0 ? suffix : undefined;
}

/**
 * Inverted timestamp for lexicographic newest-first ordering via `KV.list()`
 * prefix scans. String form keeps key segments stable regardless of any
 * future numeric-coercion surprises in tooling.
 */
export function buildInvTs(createdAt: number): string {
  return (Number.MAX_SAFE_INTEGER - createdAt).toString();
}

export function galleryKvKey(
  userId: string,
  createdAt: number,
  jobId: string
): string {
  return `gen:${userId}:${buildInvTs(createdAt)}:${jobId}`;
}

/**
 * R2 object key. No `gallery/` prefix — the bucket name itself
 * (`spritebrew-gallery` / `-dev`) provides the namespace.
 */
export function galleryR2Key(userId: string, jobId: string): string {
  return `${userId}/${jobId}.png`;
}

export interface GalleryWriteParams {
  jobId: string;
  userId: string;
  pngBytes: Uint8Array;
  /** Raw prompt — truncated internally. */
  prompt: string;
  /** RD wire-format style, e.g. `rd_pro__fantasy` or `rd_advanced_animation__walking`. */
  style: string;
  mode: JobMode;
  /** ms epoch, the stable timestamp used for invTs. */
  createdAt: number;
}

export type Logger = (
  level: 'info' | 'warn' | 'error',
  message: string,
  extra?: Record<string, unknown>
) => void;

export async function writeGalleryEntry(
  env: Env,
  params: GalleryWriteParams,
  log: Logger
): Promise<void> {
  const { jobId, userId, pngBytes, prompt, style, mode, createdAt } = params;

  const r2Key = galleryR2Key(userId, jobId);
  const kvKey = galleryKvKey(userId, createdAt, jobId);

  const action = mode === 'animate' ? actionFromPromptStyle(style) : undefined;

  const row: GalleryEntryV1 = {
    jobId,
    prompt: truncatePrompt(prompt),
    style,
    mode,
    ...(action !== undefined ? { action } : {}),
    createdAt,
    v: 1,
  };

  // R2 first — blob must exist before the index entry references it.
  await env.GALLERY_BUCKET.put(r2Key, pngBytes, {
    httpMetadata: { contentType: 'image/png' },
  });

  // Then KV gen: index with the row in metadata, value left empty.
  await env.SPRITEBREW_KV.put(kvKey, '', { metadata: row });

  log('info', 'gallery entry written', {
    r2Key,
    kvKey,
    pngBytes: pngBytes.byteLength,
    promptTruncatedLen: row.prompt.length,
  });
}
