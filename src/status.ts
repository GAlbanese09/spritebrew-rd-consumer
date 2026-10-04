// spritebrew-rd-consumer/src/status.ts
//
// The status record (n1-release-2-spec.md revision 9, 4.10), release 2's
// writer. A terminal record is written strictly: R2 first, then KV, and a
// missing binding or a failed write (after R2's one retry) throws, so the
// caller sets `jobs.status_written_at_ms` only after both landed. Non-terminal
// records (`running`) stay best effort.
//
// No record release 2 writes carries release 1's payable shapes (4.10,
// `L3 005` point 8): no `refundOwed`, no `refundDue`, and a `running` record
// never carries `tokenCost`. So release 1's sweep and dead-letter handler can
// pay nothing from it after a rollback abort.

import type { Env, JobMode, JobState, JobStateError, JobStateRunning, JobStateSuccess } from './types';
import { base64ToBytes } from './gallery';
import { JOB_TTL_S, jobStateR2Key } from './jobState';

type Logger = (level: 'info' | 'warn' | 'error', message: string, extra?: Record<string, unknown>) => void;

const R2_RETRY_DELAY_MS = 1_100;

/** What staging keeps with a result (4.9, `artifact_meta_json`): enough to
 *  write the `gen:` index and to rebuild the success record from the PNG. */
export interface ArtifactMeta {
  v: 1;
  /** The stable time of the `gen:` index key and the record's completedAt. */
  createdAt: number;
  prompt: string;
  style: string;
  mode: JobMode;
  enqueuedAt: number;
  /** The first attempt's start. */
  startedAt: number;
  /** The finishing poll's own start and end (`L3 007` ruling 2). */
  pollStartedAt?: number;
  pollEndedAt?: number;
  rdBalanceCost?: number;
  rescue?: {
    requestedWidth: number;
    requestedHeight: number;
    deliveredCellSize: number;
    deliveredFrames?: number;
  };
}

export function bytesToBase64(bytes: Uint8Array): string {
  let s = '';
  const CHUNK = 0x8000;
  for (let i = 0; i < bytes.length; i += CHUNK) s += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  return btoa(s);
}

async function putR2Strict(env: Env, key: string, body: string, log: Logger): Promise<void> {
  if (!env.GALLERY_BUCKET) throw new Error('GALLERY_BUCKET binding missing');
  const put = () => env.GALLERY_BUCKET.put(key, body, { httpMetadata: { contentType: 'application/json' } });
  try {
    await put();
    return;
  } catch (err) {
    log('warn', 'status R2 put failed, retrying once', { r2Key: key, error: err instanceof Error ? err.message : String(err) });
  }
  await new Promise((r) => setTimeout(r, R2_RETRY_DELAY_MS));
  await put();
}

/** A terminal status record, strictly: throws unless both stores took it. */
export async function putStatusStrict(env: Env, jobId: string, state: JobState, log: Logger): Promise<void> {
  const body = JSON.stringify(state);
  await putR2Strict(env, jobStateR2Key(jobId), body, log);
  if (!env.SPRITEBREW_KV) throw new Error('SPRITEBREW_KV binding missing');
  await env.SPRITEBREW_KV.put(`job:${jobId}`, body, { expirationTtl: JOB_TTL_S });
}

/** The `running` record, best effort and never with `tokenCost` (4.10). */
export async function putRunningBestEffort(env: Env, jobId: string, state: Omit<JobStateRunning, 'status'>, log: Logger): Promise<void> {
  const body = JSON.stringify({ status: 'running', ...state });
  try {
    await env.GALLERY_BUCKET.put(jobStateR2Key(jobId), body, { httpMetadata: { contentType: 'application/json' } });
  } catch (err) {
    log('warn', 'running record R2 put failed (best effort)', { error: err instanceof Error ? err.message : String(err) });
  }
  try {
    await env.SPRITEBREW_KV.put(`job:${jobId}`, body, { expirationTtl: JOB_TTL_S });
  } catch (err) {
    log('warn', 'running record KV put failed (best effort)', { error: err instanceof Error ? err.message : String(err) });
  }
}

/** The success record, from the staged PNG's bytes and the staging meta. */
export function successRecord(userId: string, meta: ArtifactMeta, resultBase64: string): JobStateSuccess {
  return {
    status: 'success',
    userId,
    mode: meta.mode,
    enqueuedAt: meta.enqueuedAt,
    startedAt: meta.startedAt,
    completedAt: meta.createdAt,
    resultBase64,
    ...(meta.rdBalanceCost !== undefined ? { rdBalanceCost: meta.rdBalanceCost } : {}),
    ...(meta.rescue
      ? {
          rescued: true as const,
          requestedWidth: meta.rescue.requestedWidth,
          requestedHeight: meta.rescue.requestedHeight,
          deliveredCellSize: meta.rescue.deliveredCellSize,
          ...(meta.rescue.deliveredFrames !== undefined ? { deliveredFrames: meta.rescue.deliveredFrames } : {}),
        }
      : {}),
  };
}

/** A staging meta from a `gen:` index row (a delivered finish, 4.12, or a
 *  delivered settlement, 4.18, R4-10): the index row carries its createdAt,
 *  prompt, style, mode and the rescue flag, not the geometry. */
export function metaFromIndexRow(row: Record<string, unknown>, fallbackCreatedAt: number): ArtifactMeta {
  const num = (v: unknown) => (typeof v === 'number' && Number.isFinite(v) ? v : undefined);
  const createdAt = num(row.createdAt) ?? fallbackCreatedAt;
  return {
    v: 1,
    createdAt,
    prompt: typeof row.prompt === 'string' ? row.prompt : '',
    style: typeof row.style === 'string' ? row.style : '',
    mode: row.mode === 'animate' ? 'animate' : 'create',
    enqueuedAt: num(row.enqueuedAt) ?? createdAt,
    startedAt: num(row.startedAt) ?? createdAt,
    ...(row.rescued === true || (row.rescue && typeof row.rescue === 'object')
      ? { rescue: (row.rescue as ArtifactMeta['rescue']) ?? { requestedWidth: 0, requestedHeight: 0, deliveredCellSize: 64 } }
      : {}),
  };
}

export function parseMeta(json: unknown, fallbackCreatedAt: number): ArtifactMeta {
  let row: Record<string, unknown> = {};
  if (typeof json === 'string') {
    try {
      const v = JSON.parse(json);
      if (v && typeof v === 'object') row = v as Record<string, unknown>;
    } catch {
      // an unreadable meta rebuilds from the fallback time
    }
  }
  return row.v === 1 && typeof row.createdAt === 'number' ? (row as unknown as ArtifactMeta) : metaFromIndexRow(row, fallbackCreatedAt);
}

/** The error record for a refunded job: `refunded: true` for both refunded
 *  outcomes (4.10, 4.11). */
export function refundedRecord(r: {
  userId: string; mode: JobMode; enqueuedAt: number; errorCode: string | null; error: string | null;
  refundedAmount: number | null; attempts?: number;
}): JobStateError & { refundedAmount?: number } {
  return {
    status: 'error',
    userId: r.userId,
    mode: r.mode,
    enqueuedAt: r.enqueuedAt,
    failedAt: Date.now(),
    error: r.error ?? 'This generation could not be completed.',
    ...(r.errorCode ? { errorCode: r.errorCode } : {}),
    attempts: r.attempts ?? 1,
    refunded: true,
    ...(r.refundedAmount !== null ? { refundedAmount: r.refundedAmount } : {}),
  };
}

/** Approved by HQ, `2026-10-04-004` (HQ-3). The honest unresolved record of a `no_record`
 *  tombstone (A9): never pending forever, never a claimed refund. */
export const UNRESOLVED_COPY = 'We could not confirm what happened to this generation. It has been flagged for review. If your balance looks wrong, email support@spritebrew.com.';

export function unresolvedRecord(userId: string, mode: JobMode, at: number): JobStateError & { unresolved: true } {
  return {
    status: 'error',
    userId,
    mode,
    enqueuedAt: at,
    failedAt: at,
    error: UNRESOLVED_COPY,
    errorCode: 'no_record',
    attempts: 0,
    refunded: false,
    unresolved: true,
  };
}

export { base64ToBytes };
