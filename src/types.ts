// spritebrew-rd-consumer/src/types.ts
//
// Wire-format types shared between the Pages-side producer (Build Prompt #2)
// and this consumer Worker. The Pages app does NOT import these directly —
// it must keep its own copy in sync. If you change anything here, update
// the Pages-side equivalent in the same PR.

import type { RdCreateBody, RdAnimateBody } from './rdClient';

export type JobMode = 'create' | 'animate';

/** Message body the producer publishes onto the queue. */
export interface JobMessage {
  jobId: string;
  userId: string;
  /** Client-supplied UUID. Used as the debit idempotency key on the producer
   *  side and as the refund idempotency key here on terminal failure. */
  idempotencyKey: string;
  /** Tokens to refund on terminal failure. The producer has already debited
   *  this amount before publishing. */
  tokenCost: number;
  mode: JobMode;
  /** RD body, shape varies by mode. Validated by the Pages-side producer
   *  before publishing — this Worker treats it as opaque and forwards it
   *  to RD. Typed as a union so callRd accepts it without further casting. */
  body: RdCreateBody | RdAnimateBody;
  enqueuedAt: number;
  /** Optional 64×64 opaque PNG base64 (no data: prefix). When present AND
   *  the animate primary attempt fails on rd_advanced_animation__*, the
   *  fallback uses this instead of body.input_image because
   *  animation__any_animation is 64×64-locked (probe C5: deterministic 400
   *  on oversized inputs). When absent AND body.width > 64, the fallback
   *  is skipped entirely and the primary's error propagates unchanged.
   *  Producer ships this in a later deploy; today it is always undefined
   *  and the >64px case simply loses fallback coverage. */
  fallbackInputImage?: string;
}

export type JobStatus = 'pending' | 'running' | 'success' | 'error';

export interface JobStatePending {
  status: 'pending';
  userId: string;
  mode: JobMode;
  enqueuedAt: number;
}

export interface JobStateRunning {
  status: 'running';
  userId: string;
  mode: JobMode;
  enqueuedAt: number;
  startedAt: number;
  attempt: number;
  /**
   * Set immediately BEFORE the async submit fetch fires (animate/async path
   * only). Purpose is redelivery-safety billing: if a message is redelivered
   * and this marker is present but taskId is absent, the previous invocation
   * threw during submit — RD may or may not have created a task, and there
   * is no recovery path (probe: GET /v1/inferences/tasks returns 404, no
   * listing endpoint; RD documents no idempotency keys). We MUST NOT
   * resubmit. Absent on the create path (which is still sync).
   */
  submitAttemptedAt?: number;
  /**
   * Set immediately AFTER the async submit response yields a task_id
   * (animate/async path only). On redelivery with this present + no terminal
   * status, resume polling this exact task; do not resubmit.
   */
  taskId?: string;
}

export interface JobStateSuccess {
  status: 'success';
  userId: string;
  mode: JobMode;
  enqueuedAt: number;
  startedAt: number;
  completedAt: number;
  resultBase64: string;
  rdBalanceCost?: number;
}

export interface JobStateError {
  status: 'error';
  userId: string;
  mode: JobMode;
  enqueuedAt: number;
  failedAt: number;
  error: string;
  errorCode?: string;
  attempts: number;
  refunded: boolean;
}

export type JobState =
  | JobStatePending
  | JobStateRunning
  | JobStateSuccess
  | JobStateError;

/** Bindings injected by Cloudflare Workers runtime. */
export interface Env {
  SPRITEBREW_KV: KVNamespace;
  GALLERY_BUCKET: R2Bucket;
  RETRO_DIFFUSION_API_KEY: string;
}

/**
 * SCHEMA CONTRACT — GalleryEntryV1
 *
 * KV row stored as METADATA on `gen:{userId}:{invTs}:{jobId}` keys. Value of
 * the KV entry is the empty string; the row lives in the metadata field so
 * `KV.list()` can return rows without a second `get` per entry. Metadata
 * must serialize to ≤1024 bytes — the row at max prompt length is well
 * under that.
 *
 * Cross-repo: the Pages app's gallery read endpoint (Phase 3) will have its
 * own copy of this type and must stay in sync. If you change the shape on
 * either side, update both files in the same PR and bump the "Last schema
 * review" date below.
 *
 * Last schema review: 2026-05-20 (Day-9, Phase 2)
 */
export interface GalleryEntryV1 {
  jobId: string;
  prompt: string;              // truncated to first 300 chars
  style: string;               // RD wire form, e.g. rd_pro__fantasy
  mode: JobMode;               // 'create' | 'animate'
  action?: string;             // animate-only; suffix of prompt_style
  createdAt: number;           // ms epoch; sourced from completedAt
  v: 1;                        // schema version
}
