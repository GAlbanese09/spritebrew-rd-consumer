// spritebrew-rd-consumer/src/refundOwed.ts
//
// Settling a refund the Pages enqueue catch could not confirm: an `error`
// record carrying `refundOwed` (n1-ledger-02.md 002 rulings B and C). One
// settlement, two callers (n1-ledger-02.md 006 ruling A): the stale-running
// sweep and the dead-letter handler. Evidence first, never blind:
//   - `balanceWritten` true: Pages' credit already moved the balance. No credit.
//   - Pages' refund key (`token_idempotency:refund:{requestId}`) present. No credit.
//   - Otherwise one refundTokens keyed on the job, which any later settlement
//     finds already applied.
// Then both copies of the record, R2 `jobs/{jobId}.json` and KV `job:{jobId}`,
// are re-read and rewritten `refunded: true` with `refundSettled`, without
// `refundOwed`, when either still owes (ruling B).
//
// A failure throws to the caller: the sweep logs it and leaves the record for
// its next run; the dead-letter handler retries through its own catch.

import type { Env, JobState, JobStateError } from './types';
import { recordEvent, type Logger } from './events';
import { refundTokens } from './refund';
import { jobStateR2Key, putJobState } from './jobState';

export type RefundOwedSettler = 'sweep' | 'dead_letter';

/** An error record that still owes the refund Pages could not confirm. */
export function owesPagesRefund(state: JobState | null | undefined): state is JobStateError {
  return state?.status === 'error' && !!state.refundOwed && state.refunded !== true;
}

/** The R2 copy; a read failure counts as a miss, as in readJobState. */
async function readR2Copy(env: Env, jobId: string): Promise<JobState | null> {
  try {
    const obj = await env.GALLERY_BUCKET.get(jobStateR2Key(jobId));
    return obj ? (JSON.parse(await obj.text()) as JobState) : null;
  } catch {
    return null;
  }
}

export async function settleRefundOwed(
  env: Env,
  jobId: string,
  state: JobStateError,
  by: RefundOwedSettler,
  log: Logger
): Promise<{ evidence: string; newBalance?: number }> {
  const owed = state.refundOwed!;
  const { userId, mode } = state;

  let evidence: string;
  let newBalance: number | undefined;
  if (owed.balanceWritten) {
    evidence = 'balance_written_at_failure';
  } else if (await env.SPRITEBREW_KV.get(`token_idempotency:${owed.idempotencyKey}`)) {
    evidence = 'pages_refund_key_present';
  } else {
    const refundResult = await refundTokens(env.SPRITEBREW_KV, userId, owed.tokenCost, jobId, { mode });
    evidence = refundResult.alreadyApplied
      ? 'job_refund_key_present'
      : by === 'sweep' ? 'refunded_by_sweep' : 'refunded_by_dead_letter';
    newBalance = refundResult.newBalance;
  }

  await recordEvent(env, {
    eventName: 'generation.refunded',
    level: 'info',
    dedupeKey: `${jobId}:generation.refunded`,
    userId,
    jobId,
    requestId: owed.requestId,
    unitsDelta: owed.tokenCost,
    extra: { settledBy: by, evidence, reason: owed.reason, newBalance },
  }, log);

  // Re-read both copies; rewrite both when either still owes.
  const kvRaw = await env.SPRITEBREW_KV.get(`job:${jobId}`);
  const kvCopy = kvRaw ? (JSON.parse(kvRaw) as JobState) : null;
  const r2Copy = await readR2Copy(env, jobId);
  const base = owesPagesRefund(kvCopy) ? kvCopy : owesPagesRefund(r2Copy) ? r2Copy : null;
  if (base) {
    const { refundOwed: _settled, ...rest } = base;
    await putJobState(env, jobId, { ...rest, refunded: true, refundSettled: { by, at: Date.now(), evidence } }, log);
  }

  return { evidence, newBalance };
}
