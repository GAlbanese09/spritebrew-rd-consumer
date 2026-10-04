// spritebrew-rd-consumer/src/gallery.ts
//
// Phase 2 of the Generation Gallery Backend (Confluence 93028353), split by
// release 2 (S3) into stage (the PNG) and publish (the gen: index).
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

/** The `gen:` index row's inputs (4.9 step 6, 4.11). */
export interface GalleryIndexParams {
  jobId: string;
  userId: string;
  /** Raw prompt; truncated internally. */
  prompt: string;
  /** RD wire-format style, e.g. `rd_pro__fantasy` or `rd_advanced_animation__walking`. */
  style: string;
  mode: JobMode;
  /** ms epoch, the stable timestamp used for invTs: the staging meta's
   *  createdAt, so a repair rewrites the same slot, never a second one. */
  createdAt: number;
  /** Animate-only. `true` iff the fallback produced this sheet; omitted from
   *  the row entirely otherwise, so normal rows are unchanged. */
  rescued?: true;
}

export type Logger = (
  level: 'info' | 'warn' | 'error',
  message: string,
  extra?: Record<string, unknown>
) => void;

const STAGE_ATTEMPTS = 3;
const STAGE_BACKOFF_MS = [250, 1_000];

/**
 * Release 2's gallery split (n1-release-2-spec.md revision 9, 6.1, 4.9).
 * Stage: the R2 PNG at `{userId}/{jobId}.png`, with no `gen:` index yet, under
 * a bounded retry (three attempts with backoff). Before each retry and before
 * giving up it `head`s the key: present means the put landed (`L 008` C).
 * Answers whether the object is durable; never throws for a failed put.
 */
export async function stagePng(env: Env, userId: string, jobId: string, pngBytes: Uint8Array, log: Logger): Promise<boolean> {
  const key = galleryR2Key(userId, jobId);
  for (let attempt = 1; attempt <= STAGE_ATTEMPTS; attempt++) {
    try {
      await env.GALLERY_BUCKET.put(key, pngBytes, { httpMetadata: { contentType: 'image/png' } });
      return true;
    } catch (err) {
      log('warn', 'gallery PNG put failed', { r2Key: key, attempt, error: err instanceof Error ? err.message : String(err) });
    }
    try {
      if (await env.GALLERY_BUCKET.head(key)) return true;
    } catch {
      // a failed head is not evidence either way; the next attempt decides
    }
    if (attempt < STAGE_ATTEMPTS) await new Promise((r) => setTimeout(r, STAGE_BACKOFF_MS[attempt - 1] ?? 1_000));
  }
  return false;
}

/** Is the staged PNG there (4.9's finalizer, 4.12's delivered checks)? A
 *  failed head throws: it is never read as absent. */
export async function pngPresent(env: Env, userId: string, jobId: string): Promise<boolean> {
  return (await env.GALLERY_BUCKET.head(galleryR2Key(userId, jobId))) !== null;
}

/** The job's `gen:` index entry, by a list on `gen:{userId}:` with a suffix
 *  match (as release 1's dead-letter handler did); its metadata row, or null.
 *  A failed list throws. */
export async function findIndexEntry(env: Env, userId: string, jobId: string): Promise<Record<string, unknown> | null> {
  let cursor: string | undefined;
  for (let page = 0; page < 20; page++) {
    const res = await env.SPRITEBREW_KV.list({ prefix: `gen:${userId}:`, cursor });
    const hit = res.keys.find((k) => k.name.endsWith(`:${jobId}`));
    if (hit) return (hit.metadata as Record<string, unknown> | undefined) ?? {};
    if (res.list_complete) return null;
    cursor = (res as { cursor?: string }).cursor;
  }
  throw new Error('gen: index listing did not finish');
}

/** Publish: the `gen:` index row, after the success update. Idempotent: the
 *  key is fixed by the user, the staged createdAt and the job. */
export async function publishIndex(env: Env, params: GalleryIndexParams, log: Logger): Promise<void> {
  const { jobId, userId, prompt, style, mode, createdAt, rescued } = params;
  const kvKey = galleryKvKey(userId, createdAt, jobId);
  const action = mode === 'animate' ? actionFromPromptStyle(style) : undefined;
  const row: GalleryEntryV1 = {
    jobId,
    prompt: truncatePrompt(prompt),
    style,
    mode,
    ...(action !== undefined ? { action } : {}),
    createdAt,
    ...(rescued ? { rescued: true as const } : {}),
    v: 1,
  };
  await env.SPRITEBREW_KV.put(kvKey, '', { metadata: row });
  log('info', 'gallery index published', { kvKey, promptTruncatedLen: row.prompt.length });
}
