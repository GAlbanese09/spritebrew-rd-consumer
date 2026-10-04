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

/** Release 2's `running` record (n1-release-2-spec.md revision 9, 4.10):
 *  best effort, for the status route only. It never carries `tokenCost`,
 *  `refundDue`, a task or a rescue marker: the `jobs` row holds the claim, the
 *  task and the phase, and release 1's payable shapes are retired (6.1). */
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
  /**
   * RESCUE MARKER (July 16). Present ONLY when the delivered sheet came from
   * the animate fallback (animation__any_animation, 64×64-clamped) after the
   * primary rd_advanced_animation__* attempt failed. The client must not infer
   * a rescue from geometry — a 64px request that succeeds normally delivers
   * the same geometry as a 64px rescue. `rescued` is the only signal.
   *
   * All five fields are absent on a normal success, so those records stay
   * byte-identical to pre-July-16 ones.
   */
  rescued?: true;
  /** Width the user actually asked for, pre-clamp (e.g. 128, 256). */
  requestedWidth?: number;
  /** Height the user actually asked for, pre-clamp. */
  requestedHeight?: number;
  /** Cell size actually delivered. Always 64 today — the fallback style is
   *  64×64-locked (probe C5). Recorded rather than assumed so the client
   *  doesn't hardcode it. */
  deliveredCellSize?: number;
  /**
   * Frame count actually delivered, derived from the PNG's IHDR dimensions:
   * (W/cell)*(H/cell). Absent when the header couldn't be read or the sheet
   * isn't an exact multiple of the cell size — the rescue is still real and
   * still marked, the client just falls back to its own slicing guess.
   */
  deliveredFrames?: number;
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
  /** The refunded amount (4.11), on a record release 2 wrote. */
  refundedAmount?: number;
  /** A `no_record` tombstone's honest unresolved record (A9, 4.10). */
  unresolved?: true;
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
  /** D1 event ledger (WD2a). Observability, not money. Typed as required
   *  because wrangler.toml binds it in both envs; recordEvent still guards
   *  against a deploy that lacks it and degrades to a warn line. */
  EVENTS_DB: D1Database;
  /** `spritebrew-ledger` (n1-ledger.md 005). Release 1 reads only its
   *  `control` table: the money pause, and `dev_fault` on dev. Optional at the
   *  type level; a deploy without it reads as paused (fail closed). */
  LEDGER_DB?: D1Database;
  /** 'production' | 'dev', from [env.*.vars] in wrangler.toml. Stamped on
   *  every ledger row because bindings alone do not say which env this is. */
  APP_ENV: string;
  /** The migrator's bearer token (S2, 4.17): a secret, set only for the switch
   *  (`wrangler secret put MIGRATE_TOKEN`). Unset, every migrator path is 404. */
  MIGRATE_TOKEN?: string;
  RETRO_DIFFUSION_API_KEY: string;
  /** Morning digest (WD2b). RESEND_API_KEY and DIGEST_TO are Worker secrets
   *  (`wrangler secret put`, per environment); DIGEST_TO is a comma-separated
   *  recipient list and never appears in a file. DIGEST_FROM is a plain var in
   *  wrangler.toml. All optional at the type level so a deploy without them
   *  degrades to a logged outcome rather than a type error. */
  RESEND_API_KEY?: string;
  DIGEST_TO?: string;
  DIGEST_FROM?: string;
  /** "1" bypasses the 08:00 New York window. Declared under [env.preview.vars]
   *  only, and the runner ignores it when APP_ENV is 'production'. */
  DIGEST_FORCE_RUN?: string;
  /**
   * DEV TESTING ONLY. When exactly 'true', animate jobs skip the primary
   * submit and go straight to the fallback, so the rescue path can be
   * exercised without waiting for a real provider failure. Set manually on
   * the dev worker via the dashboard and REMOVED after testing — deliberately
   * absent from wrangler.toml so it can never ride a deploy to prod. Any
   * other value (including unset) leaves the code inert.
   */
  FORCE_ANIMATE_FALLBACK?: string;
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
 * Last schema review: 2026-07-16 (rescue marker added; v stays 1 — the field
 * is optional and additive, so existing readers are unaffected)
 */
export interface GalleryEntryV1 {
  jobId: string;
  prompt: string;              // truncated to first 300 chars
  style: string;               // RD wire form, e.g. rd_pro__fantasy
  mode: JobMode;               // 'create' | 'animate'
  action?: string;             // animate-only; suffix of prompt_style
  createdAt: number;           // ms epoch; sourced from completedAt
  rescued?: true;              // animate-only; present iff delivered by the fallback
  v: 1;                        // schema version
}
