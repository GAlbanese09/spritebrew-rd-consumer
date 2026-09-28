// spritebrew-rd-consumer/src/moneyPause.ts
//
// The money-write pause (n1-ledger.md 005 section 4, as amended by 007 and
// 008): one row in the `control` table of `spritebrew-ledger`, read before
// any refund, any fresh RD call and the sweep. It FAILS CLOSED: a missing
// binding, a read error, a timeout or an unexpected value all count as
// paused, so a D1 fault can never let an old-money write through during a
// switch.
//
// Pages copy: spritebrew/src/lib/moneyPause.ts. Keep them in step.

import type { Env } from './types';

/** Every paused retry (ruling A): a held fresh message is delivered at about
 *  0, 15, 30 and 45 minutes, then dead-letters; the dead-letter handler waits
 *  the same. */
export const PAUSED_RETRY_DELAY_S = 900;

/** Dev only (`dev_fault` = 'paused_retry_60'): the same paths at 60 s, so
 *  ruling A's dead-letter test runs in minutes instead of two hours. */
const DEV_PAUSED_RETRY_DELAY_S = 60;

const PAUSE_READ_TIMEOUT_MS = 2_000;

export async function isMoneyPaused(env: Env): Promise<boolean> {
  const started = Date.now();
  let paused = true;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    if (!env.LEDGER_DB) throw new Error('LEDGER_DB binding missing');
    const row = await Promise.race([
      env.LEDGER_DB.prepare("SELECT value FROM control WHERE key = 'money_pause'").first<{ value: string }>(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('pause read timed out')), PAUSE_READ_TIMEOUT_MS);
      }),
    ]);
    // Only an explicit '0' opens the gate; a missing row or any other value
    // is logged below and read as paused.
    if (row?.value !== '0' && row?.value !== '1') {
      throw new Error(row ? 'unexpected money_pause value' : 'money_pause row missing');
    }
    paused = row.value !== '0';
  } catch (err) {
    console.error(JSON.stringify({
      level: 'error',
      source: 'money-pause',
      event: 'pause_read_failed',
      error: err instanceof Error ? err.message.slice(0, 120) : 'unknown',
    }));
    paused = true;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
  if (env.APP_ENV === 'dev') {
    // T11d: the read's latency, dev only.
    console.log(JSON.stringify({ source: 'money-pause', event: 'pause_read', ms: Date.now() - started, paused }));
  }
  return paused;
}

/**
 * Dev-only fault injection for the release 1 tests: the `dev_fault` row of
 * the dev `control` table, a comma-separated list of fault names. Read only
 * when APP_ENV is 'dev'; production never reads it, and no migration inserts
 * it.
 */
export async function devFaults(env: Env): Promise<string[]> {
  if (env.APP_ENV !== 'dev' || !env.LEDGER_DB) return [];
  try {
    const row = await env.LEDGER_DB
      .prepare("SELECT value FROM control WHERE key = 'dev_fault'")
      .first<{ value: string }>();
    return row?.value ? row.value.split(',') : [];
  } catch {
    return [];
  }
}

export async function pausedRetryDelayS(env: Env): Promise<number> {
  return (await devFaults(env)).includes('paused_retry_60') ? DEV_PAUSED_RETRY_DELAY_S : PAUSED_RETRY_DELAY_S;
}
