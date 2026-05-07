// spritebrew-rd-consumer/src/refund.ts
//
// SCHEMA CONTRACT
// ===============
// Source of truth: spritebrew/src/lib/tokenBalance.ts (creditTokens function)
// Canonical interfaces: BalanceRecord (lines 72-76), TransactionRecord (lines 92-101)
// Last schema review: 2026-05-07 (Session 15 continued morning, post-schema-recon)
//
// KV keys this file writes (matching Pages-side canonical exactly):
//   - token_balance:{userId}            → BalanceRecord (3-field, ISO date strings, merge-preserve created_at)
//   - token_idempotency:refund:{jobId}  → literal string '1'                                 TTL 7d
//   - token_tx:{userId}:{ts}:{uid}      → TransactionRecord (full canonical shape)           TTL 90d
//
// Where ts = Date.now() numeric, uid = Math.random().toString(36).slice(2,8) (6-char base36).
//
// Write ordering matches Pages-side creditTokens (tokenBalance.ts:331-348):
//   1. Read current balance.
//   2. Compute newBalance.
//   3. Write balance (merge-preserving created_at).
//   4. Write idempotency flag.
//   5. Write tx log.
//
// Trade-off documented: a crash between steps 3 and 4 risks a double-credit on
// queue redelivery (idempotency check would fail to short-circuit). Consumer
// matches Pages canonical ordering for consistency; the alternative
// (idempotency-first) trades double-credit risk for lost-refund risk and is
// asymmetric vs the Pages side. Net inherited risk is unchanged from current
// production behavior on the synchronous code path.
//
// If any of these schemas change in tokenBalance.ts, update this file in
// the same PR and bump the "Last schema review" date.

interface BalanceRecord {
  balance: number;
  created_at: string;
  last_updated: string;
}

interface TransactionRecord {
  type: 'credit' | 'debit';
  amount: number;
  reason: string;
  source?: string;
  balance_after: number;
  timestamp: string;
}

const REFUND_IDEMPOTENCY_TTL_S = 60 * 60 * 24 * 7;  // 7 days, matches IDEMPOTENCY_TTL on Pages
const TX_LOG_TTL_S = 60 * 60 * 24 * 90;              // 90 days, matches TX_TTL on Pages

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
  const nowIso = new Date().toISOString();
  const tsMillis = Date.now();

  // Defensive: producer's debitTokens lazy-inits via initBalance, so
  // by the time consumer runs the record SHOULD exist with all 3 fields.
  // The fallback synthesizes a canonical 3-field shape for robustness.
  const current: BalanceRecord = currentRaw
    ? (JSON.parse(currentRaw) as BalanceRecord)
    : { balance: 0, created_at: nowIso, last_updated: nowIso };

  const newBalance = current.balance + amount;

  const updated: BalanceRecord = {
    balance: newBalance,
    created_at: current.created_at ?? nowIso,  // belt-and-braces: preserve if present, synthesize if somehow missing
    last_updated: nowIso,
  };

  await kv.put(balanceKey, JSON.stringify(updated));

  await kv.put(idemKey, '1', { expirationTtl: REFUND_IDEMPOTENCY_TTL_S });

  const uid = Math.random().toString(36).slice(2, 8);
  const txKey = `token_tx:${userId}:${tsMillis}:${uid}`;
  const tx: TransactionRecord = {
    type: 'credit',
    amount,
    reason: 'generation_failed_refund',
    source: 'generation_failed_refund',
    balance_after: newBalance,
    timestamp: nowIso,
  };
  await kv.put(txKey, JSON.stringify(tx), { expirationTtl: TX_LOG_TTL_S });

  return { alreadyApplied: false, newBalance };
}
