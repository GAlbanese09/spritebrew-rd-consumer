// spritebrew-rd-consumer/src/refund.ts
//
// SCHEMA CONTRACT
// ===============
// Source of truth: spritebrew/src/lib/tokenBalance.ts (creditTokens function)
// Last schema review: 2026-05-07 (Session 15 continued morning)
//
// KV keys this file writes:
//   - token_balance:{userId} → { balance: number, updatedAt: number }
//   - token_idempotency:refund:{jobId} → { amount: number, ts: number }   TTL 7d
//   - token_tx:{userId}:{ts}:{uid} → TokenTransaction                      TTL 90d
//
// If any of these schemas change in tokenBalance.ts, update this file in
// the same PR and bump the "Last schema review" date.
//
// Architectural rationale: Confluence 87588866. Inline KV writes (Option F)
// chosen over Service Binding (does not exist Worker→Pages), npm package
// (overkill for ~25 LOC), git submodule (Pages-build friction).

const REFUND_IDEMPOTENCY_TTL_S = 60 * 60 * 24 * 7;  // 7 days
const TX_LOG_TTL_S = 60 * 60 * 24 * 90;              // 90 days

export interface RefundResult {
  alreadyApplied: boolean;
  newBalance?: number;
}

export async function refundTokens(
  kv: KVNamespace,
  userId: string,
  amount: number,
  jobId: string
): Promise<RefundResult> {
  const idemKey = `token_idempotency:refund:${jobId}`;
  const idemExisting = await kv.get(idemKey);
  if (idemExisting) {
    return { alreadyApplied: true };
  }

  const balanceKey = `token_balance:${userId}`;
  const currentRaw = await kv.get(balanceKey);
  const current = currentRaw
    ? (JSON.parse(currentRaw) as { balance: number; updatedAt: number })
    : { balance: 0, updatedAt: 0 };
  const newBalance = current.balance + amount;
  const now = Date.now();

  await kv.put(
    balanceKey,
    JSON.stringify({ balance: newBalance, updatedAt: now })
  );

  await kv.put(
    idemKey,
    JSON.stringify({ amount, ts: now }),
    { expirationTtl: REFUND_IDEMPOTENCY_TTL_S }
  );

  const tx = {
    userId,
    amount,
    reason: 'generation_failed_refund' as const,
    idempotencyKey: `refund:${jobId}`,
    ts: now,
  };
  const txKey = `token_tx:${userId}:${now}:${crypto.randomUUID()}`;
  await kv.put(txKey, JSON.stringify(tx), { expirationTtl: TX_LOG_TTL_S });

  return { alreadyApplied: false, newBalance };
}
