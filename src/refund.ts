// spritebrew-rd-consumer/src/refund.ts
//
// A failed generation's refund, on the D1 ledger (n1-release-2-spec.md
// revision 9, 4.8; 6.1: "4.8 behind refundTokens(env, ...)"). Release 1's KV
// read-modify-write and its 30-day `token_idempotency:refund:{jobId}` key are
// retired: the refund is one guarded batch keyed `refund:{jobId}`, and a key
// release 1 wrote before the switch is honored through `legacy_idem` (r1, r4).

import type { Env } from './types';
import * as L from './ledger';
import { ledgerAlarm, ledgerCtx, readJobRow, type Logger } from './settle';

export interface RefundTokensResult extends L.RefundResult {
  /** True when nothing moved because the job was already refunded (here or
   *  before the switch). */
  alreadyApplied: boolean;
  /** The balance the batch read back, when it moved money. */
  newBalance: number | null;
}

/**
 * 4.8 with its fence. A missing balance is opened at 0 with `zero_alarm`
 * (4.2, `L 004` ruling 1) and the batch runs once more with a fresh :id; a
 * debit that is absent or mismatched raises `debit_missing` and refunds
 * nothing (`L 004` ruling 8).
 */
export async function refundTokens(env: Env, input: L.RefundInput, log: Logger): Promise<RefundTokensResult> {
  const ctx = ledgerCtx(env);
  let r = await L.refundAndFinish(ctx, input);
  if (r.outcome === 'no_balance') {
    const uid = String((r.row as Record<string, unknown> | null)?.user_id ?? (await readJobRow(env, input.job))?.user_id ?? '');
    const o = await L.openBalance(ctx, { uid, amount: 0, reason: 'refund_no_balance', source: 'generation_failed_refund', via: 'zero_alarm' });
    if (o.outcome === 'opened') await ledgerAlarm(env, 'zero_alarm', uid, { jobId: input.job }, log, uid);
    r = await L.refundAndFinish(ctx, input);
  }
  if (r.outcome === 'corruption') await ledgerAlarm(env, 'debit_missing', input.job, { fence: input.fence }, log);
  const balance = (r.row as Record<string, unknown> | null)?.balance;
  return {
    ...r,
    alreadyApplied: r.outcome === 'already_refunded_legacy' || (r.outcome === 'already_finished' && ['refunded', 'refunded_legacy'].includes(String((r.row as Record<string, unknown> | null)?.outcome))),
    newBalance: r.outcome === 'refunded' && typeof balance === 'number' ? balance : null,
  };
}
