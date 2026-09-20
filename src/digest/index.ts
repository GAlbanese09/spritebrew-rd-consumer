// spritebrew-rd-consumer/src/digest/index.ts
//
// The morning digest runner (WD2b). Third job of the */15 scheduled handler,
// after the stale-running sweep and the provider probe. Nothing on the job
// path; recordEvent is not involved.
//
// When: the 08:00 hour in America/New_York (so the 08:00, 08:15, 08:30 and
// 08:45 slots are candidates), for the New York day that just ended.
// Idempotency: digest_runs keyed on reporting_day. The row is written AFTER
// the attempt, never before: a successful send writes state 'sent' and the
// Resend message id; a failed attempt writes state 'failed' with the error
// and the attempt count, so the next slot retries. After 3 failed attempts
// the day is left alone (a dead Resend key must not turn into 96 failed
// requests a day). Every attempt is logged.
//
// Zero rows is not a reason to skip: a quiet morning still gets an email
// saying so, because a missing email must mean the cron did not run.
//
// DIGEST_FORCE_RUN = "1" bypasses the 08:00 window only. It is declared under
// [env.preview.vars] in wrangler.toml and nowhere else, and this file ignores
// it when APP_ENV is 'production', so it cannot act in production even if
// someone sets it there by hand.
//
// DIGEST_TO is a Worker SECRET (wrangler secret put DIGEST_TO --env <env>), a
// comma-separated list of recipients, never a var and never in a file. Unset
// or empty: log an error, send nothing, write no digest_runs row (it is a
// configuration gap, not an attempt).

import type { Env } from '../types';
import { gatherDigest, nyDay, nyHour, previousDay } from './queries';
import { digestHtml, digestSubject, digestText } from './render';

type Logger = (level: 'info' | 'warn' | 'error', message: string, extra?: Record<string, unknown>) => void;

const DIGEST_HOUR_NY = 8;
const MAX_ATTEMPTS_PER_DAY = 3;
const RESEND_URL = 'https://api.resend.com/emails';

interface DigestRunRow {
  state: 'started' | 'sent' | 'failed';
  attempts: number;
}

async function readRun(db: D1Database, day: string): Promise<DigestRunRow | null> {
  return db
    .prepare('SELECT state, attempts FROM digest_runs WHERE reporting_day = ?1')
    .bind(day)
    .first<DigestRunRow>();
}

/** Upsert the day's row. started_at_ms is kept from the first attempt. */
async function writeRun(
  db: D1Database,
  day: string,
  state: 'sent' | 'failed',
  attempts: number,
  nowMs: number,
  resendMessageId: string | null,
  errorCode: string | null
): Promise<void> {
  await db
    .prepare(
      `INSERT INTO digest_runs (reporting_day, state, started_at_ms, sent_at_ms, resend_message_id, error_code, attempts)
       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
       ON CONFLICT(reporting_day) DO UPDATE SET
         state = excluded.state,
         sent_at_ms = excluded.sent_at_ms,
         resend_message_id = excluded.resend_message_id,
         error_code = excluded.error_code,
         attempts = excluded.attempts`
    )
    .bind(day, state, nowMs, state === 'sent' ? nowMs : null, resendMessageId, errorCode, attempts)
    .run();
}

/** DIGEST_TO as a trimmed, de-duplicated list; empty when unset. */
export function parseRecipients(raw: string | undefined): string[] {
  if (!raw) return [];
  return Array.from(new Set(raw.split(',').map((s) => s.trim()).filter((s) => s.length > 0)));
}

/** POST to Resend. Returns the message id. Throws with a bounded message on
 *  any non-2xx; the caller records the error code and retries next slot. */
async function sendViaResend(env: Env, to: string[], subject: string, html: string, textBody: string): Promise<string> {
  if (!env.RESEND_API_KEY) throw new Error('resend_key_missing');
  if (!env.DIGEST_FROM) throw new Error('digest_from_missing');
  const resp = await fetch(RESEND_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${env.RESEND_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ from: env.DIGEST_FROM, to, subject, html, text: textBody }),
    signal: AbortSignal.timeout(15_000),
  });
  const text = await resp.text();
  if (!resp.ok) throw new Error(`resend_${resp.status}: ${text.slice(0, 300)}`);
  let id: string | undefined;
  try {
    id = (JSON.parse(text) as { id?: string }).id;
  } catch {
    // fall through; a 2xx without an id is still a send
  }
  return id ?? 'unknown';
}

/** Error text to a short code that fits error_code without leaking bodies. */
function errorCodeOf(err: unknown): string {
  const msg = err instanceof Error ? err.message : String(err);
  return msg.slice(0, 120);
}

export async function runDigestIfDue(env: Env): Promise<void> {
  const log: Logger = (level, message, extra = {}) =>
    console[level](JSON.stringify({ level, message, source: 'morning-digest', ...extra }));

  try {
    const db = env.EVENTS_DB as D1Database | undefined;
    if (!db) {
      log('warn', 'digest skipped: EVENTS_DB binding is undefined');
      return;
    }

    const nowMs = Date.now();
    const today = nyDay(nowMs);
    const day = previousDay(today);
    const hour = nyHour(nowMs);
    const forced = env.DIGEST_FORCE_RUN === '1' && env.APP_ENV !== 'production';

    if (!forced && hour !== DIGEST_HOUR_NY) return;

    const recipients = parseRecipients(env.DIGEST_TO);
    if (recipients.length === 0) {
      // Configuration gap, not an attempt: no send, no digest_runs row.
      log('error', 'digest skipped: DIGEST_TO secret is unset or empty', { day, forced });
      return;
    }

    const existing = await readRun(db, day);
    if (existing?.state === 'sent') {
      log('info', 'digest already sent for day; skipping', { day, forced });
      return;
    }
    const priorAttempts = existing?.attempts ?? 0;
    if (priorAttempts >= MAX_ATTEMPTS_PER_DAY) {
      log('warn', 'digest attempts exhausted for day; not retrying', { day, attempts: priorAttempts });
      return;
    }
    const attempt = priorAttempts + 1;
    log('info', 'digest attempt', { day, attempt, forced, recipients: recipients.length, nyHour: hour });

    try {
      const data = await gatherDigest(db, day, nowMs);
      const environment = env.APP_ENV ?? 'unknown';
      const subject = digestSubject(data, environment);
      const html = digestHtml(data, environment);
      const text = digestText(html);
      if (forced) {
        // Preview only (forced is false in production): the exact document
        // handed to Resend, so a dev run can be verified from the tail.
        log('info', 'digest html (forced run only)', { day, subject, htmlLength: html.length, html });
      }
      const messageId = await sendViaResend(env, recipients, subject, html, text);
      await writeRun(db, day, 'sent', attempt, nowMs, messageId, null);
      log('info', 'digest sent', {
        day, attempt, resendMessageId: messageId, subject, recipients: recipients.length,
        terminal: data.trend.yesterday.total, failed: data.trend.yesterday.failed,
        abandonedPaid: data.abandoned.count, missingRefunds: data.refunds.missing,
        tripwires: data.provider.tripwires.fired,
      });
    } catch (err) {
      const code = errorCodeOf(err);
      try {
        await writeRun(db, day, 'failed', attempt, nowMs, null, code);
      } catch (writeErr) {
        log('error', 'digest failure row write failed', { day, attempt, error: errorCodeOf(writeErr) });
      }
      log('error', 'digest attempt failed', { day, attempt, error: code, retriesLeft: MAX_ATTEMPTS_PER_DAY - attempt });
    }
  } catch (err) {
    log('error', 'digest runner threw', { error: errorCodeOf(err) });
  }
}
