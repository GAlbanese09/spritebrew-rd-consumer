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
  RETRO_DIFFUSION_API_KEY: string;
}
