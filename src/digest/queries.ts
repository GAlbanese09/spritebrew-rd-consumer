// spritebrew-rd-consumer/src/digest/queries.ts
//
// CANONICAL COPY. The Pages repo (WD2c, /admin/ops) carries a verbatim copy of
// this file; edit here first and copy, never the other way round. Two repos
// cannot share a module and six functions do not justify a package.
//
// Every query over the D1 event ledger that the morning digest needs, as pure
// functions: a D1Database in, plain objects out. No rendering, no env, no
// logging, no dependencies. Percentiles are computed here because SQLite has
// no percentile function and a day of rows is small (a few hundred terminal
// rows, at most 100 probe rows).
//
// Day bucketing uses the `reporting_day` column, which the writer stamped with
// the same America/New_York formatter used below, so a day's rows are exactly
// the rows the writer put in that day, on 23-hour and 25-hour days too. The
// half-open UTC range of a day (nyDayRangeUtc) is used only where instants
// matter: the expected probe-slot count and the gap detection at the edges of
// the day. Never a fixed offset.
//
// Event names and columns are those of src/events.ts and
// migrations/0001_events.sql. `finalAttempt` has no typed column; it lives in
// event_json at $.extra.finalAttempt.

// ─── Time: New York calendar days ──────────────────────────────────────────

const NY_TZ = 'America/New_York';

const NY_DAY_FMT = new Intl.DateTimeFormat('en-CA', {
  timeZone: NY_TZ,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

const NY_PARTS_FMT = new Intl.DateTimeFormat('en-US', {
  timeZone: NY_TZ,
  hourCycle: 'h23',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});

/** YYYY-MM-DD in New York for an instant. Same formatter as reporting_day. */
export function nyDay(ms: number): string {
  return NY_DAY_FMT.format(new Date(ms));
}

/** Hour of day (0..23) in New York for an instant. */
export function nyHour(ms: number): number {
  const h = NY_PARTS_FMT.formatToParts(new Date(ms)).find((p) => p.type === 'hour')?.value;
  return h === undefined ? -1 : Number(h);
}

/** The calendar day before a YYYY-MM-DD string (pure calendar arithmetic). */
export function previousDay(day: string, n = 1): string {
  const [y, m, d] = day.split('-').map(Number);
  const t = Date.UTC(y, m - 1, d - n);
  const dt = new Date(t);
  const yy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(dt.getUTCDate()).padStart(2, '0');
  return `${yy}-${mm}-${dd}`;
}

/** New York wall-clock offset from UTC, in minutes, at an instant. */
function nyOffsetMinutesAt(ms: number): number {
  const parts = NY_PARTS_FMT.formatToParts(new Date(ms));
  const get = (t: string) => Number(parts.find((p) => p.type === t)?.value);
  const asUtc = Date.UTC(get('year'), get('month') - 1, get('day'), get('hour'), get('minute'), get('second'));
  return Math.round((asUtc - Math.floor(ms / 1000) * 1000) / 60000);
}

/** UTC instant of local midnight starting the given New York calendar day. */
function nyMidnightUtc(day: string): number {
  const [y, m, d] = day.split('-').map(Number);
  const naive = Date.UTC(y, m - 1, d, 0, 0, 0);
  // Two passes converge across a DST boundary: the offset at the naive guess
  // may differ from the offset at the true midnight.
  let guess = naive - nyOffsetMinutesAt(naive) * 60000;
  guess = naive - nyOffsetMinutesAt(guess) * 60000;
  return guess;
}

/**
 * Half-open UTC range [startMs, endMs) covering one New York calendar day.
 * 23 hours on the spring-forward day, 25 on the fall-back day.
 */
export function nyDayRangeUtc(day: string): { startMs: number; endMs: number } {
  const startMs = nyMidnightUtc(day);
  const [y, m, d] = day.split('-').map(Number);
  const next = new Date(Date.UTC(y, m - 1, d + 1));
  const nextDay = `${next.getUTCFullYear()}-${String(next.getUTCMonth() + 1).padStart(2, '0')}-${String(next.getUTCDate()).padStart(2, '0')}`;
  return { startMs, endMs: nyMidnightUtc(nextDay) };
}

// ─── Shared ────────────────────────────────────────────────────────────────

const TERMINAL = "('generation.succeeded','generation.rescued','generation.failed')";

export const POLL_BUDGET_MS = 180_000;
export const PROBE_SLOT_MS = 15 * 60 * 1000;
/** RD's per-call price in USD as observed on rdBalanceCost (0.14 on every
 *  receipt to date). A parameter, not a constant, so a price change is a
 *  call-site edit. */
export const RD_PRICE_USD_DEFAULT = 0.14;

function pct(num: number, den: number): number | null {
  return den > 0 ? (num / den) * 100 : null;
}

/** Nearest-rank percentile of an ascending array; null on empty. */
export function percentile(sorted: number[], p: number): number | null {
  if (sorted.length === 0) return null;
  const rank = Math.max(1, Math.ceil((p / 100) * sorted.length));
  return sorted[rank - 1];
}

// ─── 1. Yesterday by style and size ────────────────────────────────────────

export interface StyleSizeRow {
  style: string;
  requestedSize: string;
  total: number;
  succeeded: number;
  rescued: number;
  failed: number;
  failureRatePct: number | null;
}

export async function byStyleAndSize(db: D1Database, day: string): Promise<StyleSizeRow[]> {
  const { results } = await db
    .prepare(
      `SELECT COALESCE(style, '(none)') AS style, COALESCE(requested_size, '(none)') AS requested_size,
              COUNT(*) AS total,
              SUM(event_name = 'generation.succeeded') AS succeeded,
              SUM(event_name = 'generation.rescued') AS rescued,
              SUM(event_name = 'generation.failed') AS failed
         FROM events
        WHERE reporting_day = ?1 AND event_name IN ${TERMINAL}
        GROUP BY 1, 2
        ORDER BY total DESC, style, requested_size`
    )
    .bind(day)
    .all<{ style: string; requested_size: string; total: number; succeeded: number; rescued: number; failed: number }>();
  return (results ?? []).map((r) => ({
    style: r.style,
    requestedSize: r.requested_size,
    total: r.total,
    succeeded: r.succeeded,
    rescued: r.rescued,
    failed: r.failed,
    failureRatePct: pct(r.failed, r.total),
  }));
}

// ─── 2. Trend ──────────────────────────────────────────────────────────────

export interface TrendWindow {
  label: string;
  fromDay: string;
  toDay: string;
  total: number;
  failed: number;
  rescued: number;
  failureRatePct: number | null;
  rescueRatePct: number | null;
}

export interface Trend {
  yesterday: TrendWindow;
  last7: TrendWindow;
  prior7: TrendWindow;
  /** Percentage points, last7 minus prior7; null when either window is empty. */
  failureRateDeltaPp: number | null;
  rescueRateDeltaPp: number | null;
}

export async function trend(db: D1Database, day: string): Promise<Trend> {
  const l7From = previousDay(day, 6);
  const p7To = previousDay(day, 7);
  const p7From = previousDay(day, 13);
  const row = await db
    .prepare(
      `SELECT
         SUM(reporting_day = ?1) AS y_total,
         SUM(reporting_day = ?1 AND event_name = 'generation.failed') AS y_failed,
         SUM(reporting_day = ?1 AND event_name = 'generation.rescued') AS y_rescued,
         SUM(reporting_day BETWEEN ?2 AND ?1) AS l7_total,
         SUM(reporting_day BETWEEN ?2 AND ?1 AND event_name = 'generation.failed') AS l7_failed,
         SUM(reporting_day BETWEEN ?2 AND ?1 AND event_name = 'generation.rescued') AS l7_rescued,
         SUM(reporting_day BETWEEN ?3 AND ?4) AS p7_total,
         SUM(reporting_day BETWEEN ?3 AND ?4 AND event_name = 'generation.failed') AS p7_failed,
         SUM(reporting_day BETWEEN ?3 AND ?4 AND event_name = 'generation.rescued') AS p7_rescued
       FROM events
       WHERE event_name IN ${TERMINAL} AND reporting_day BETWEEN ?3 AND ?1`
    )
    .bind(day, l7From, p7From, p7To)
    .first<Record<string, number | null>>();
  const n = (v: number | null | undefined) => v ?? 0;
  const win = (label: string, fromDay: string, toDay: string, t: number, f: number, r: number): TrendWindow => ({
    label, fromDay, toDay, total: t, failed: f, rescued: r,
    failureRatePct: pct(f, t), rescueRatePct: pct(r, t),
  });
  const yesterday = win('yesterday', day, day, n(row?.y_total), n(row?.y_failed), n(row?.y_rescued));
  const last7 = win('last 7 days', l7From, day, n(row?.l7_total), n(row?.l7_failed), n(row?.l7_rescued));
  const prior7 = win('prior 7 days', p7From, p7To, n(row?.p7_total), n(row?.p7_failed), n(row?.p7_rescued));
  const delta = (a: number | null, b: number | null) => (a === null || b === null ? null : a - b);
  return {
    yesterday, last7, prior7,
    failureRateDeltaPp: delta(last7.failureRatePct, prior7.failureRatePct),
    rescueRateDeltaPp: delta(last7.rescueRatePct, prior7.rescueRatePct),
  };
}

// ─── 3. Refunds expected versus issued ─────────────────────────────────────

export interface RefundReconciliation {
  expected: number;
  issued: number;
  /** Failures older than the grace window with no generation.refunded row. */
  missing: number;
  missingJobIds: string[];
  /** Failures younger than the grace window with no refund row yet. */
  inGrace: number;
  graceMinutes: number;
}

export async function refundsExpectedVsIssued(
  db: D1Database,
  day: string,
  nowMs: number,
  graceMs = 60 * 60 * 1000
): Promise<RefundReconciliation> {
  const { results } = await db
    .prepare(
      `SELECT f.job_id AS job_id, f.occurred_at_ms AS failed_at_ms,
              (SELECT r.event_id FROM events r
                WHERE r.job_id = f.job_id AND r.event_name = 'generation.refunded'
                LIMIT 1) AS refund_event_id
         FROM events f
        WHERE f.event_name = 'generation.failed' AND f.reporting_day = ?1
        ORDER BY f.occurred_at_ms`
    )
    .bind(day)
    .all<{ job_id: string; failed_at_ms: number; refund_event_id: string | null }>();
  const rows = results ?? [];
  const cutoff = nowMs - graceMs;
  const missingJobIds = rows.filter((r) => !r.refund_event_id && r.failed_at_ms <= cutoff).map((r) => r.job_id);
  const inGrace = rows.filter((r) => !r.refund_event_id && r.failed_at_ms > cutoff).length;
  return {
    expected: rows.length,
    issued: rows.filter((r) => !!r.refund_event_id).length,
    missing: missingJobIds.length,
    missingJobIds,
    inGrace,
    graceMinutes: Math.round(graceMs / 60000),
  };
}

// ─── 4. Abandoned paid tasks ───────────────────────────────────────────────

export interface AbandonedPaidTask {
  jobId: string;
  rdTaskId: string | null;
  attempt: number | null;
  tokensRefunded: number | null;
}

export interface AbandonedPaid {
  count: number;
  tasks: AbandonedPaidTask[];
  pricePerCallUsd: number;
  totalUsd: number;
}

/**
 * A job whose final attempt exhausted the poll budget AND that was refunded:
 * RD finished (and billed) a task nobody collected. Its own number, never
 * folded into the failure rate: the customer outcome and the money wasted are
 * different numbers.
 */
export async function abandonedPaidTasks(
  db: D1Database,
  day: string,
  pricePerCallUsd = RD_PRICE_USD_DEFAULT
): Promise<AbandonedPaid> {
  const { results } = await db
    .prepare(
      `SELECT p.job_id AS job_id, MAX(p.attempt) AS attempt,
              (SELECT s.provider_job_id FROM events s
                WHERE s.job_id = p.job_id AND s.event_name = 'provider.submit_accepted'
                ORDER BY s.occurred_at_ms DESC LIMIT 1) AS rd_task_id,
              (SELECT r.units_delta FROM events r
                WHERE r.job_id = p.job_id AND r.event_name = 'generation.refunded'
                LIMIT 1) AS tokens_refunded
         FROM events p
        WHERE p.event_name = 'provider.poll_budget_exhausted'
          AND p.reporting_day = ?1
          AND json_extract(p.event_json, '$.extra.finalAttempt') = 1
          AND EXISTS (SELECT 1 FROM events r
                       WHERE r.job_id = p.job_id AND r.event_name = 'generation.refunded')
        GROUP BY p.job_id
        ORDER BY p.job_id`
    )
    .bind(day)
    .all<{ job_id: string; attempt: number | null; rd_task_id: string | null; tokens_refunded: number | null }>();
  const tasks = (results ?? []).map((r) => ({
    jobId: r.job_id,
    rdTaskId: r.rd_task_id,
    attempt: r.attempt,
    tokensRefunded: r.tokens_refunded,
  }));
  return {
    count: tasks.length,
    tasks,
    pricePerCallUsd,
    totalUsd: Math.round(tasks.length * pricePerCallUsd * 100) / 100,
  };
}

// ─── 5. Poll budget headroom ───────────────────────────────────────────────

export interface PollHeadroom {
  budgetMs: number;
  jobs: number;
  p50Ms: number | null;
  p95Ms: number | null;
  maxMs: number | null;
  p95PctOfBudget: number | null;
  maxPctOfBudget: number | null;
  /** Terminal rows whose latency_ms exceeded 80% of the budget. */
  over80Pct: number;
}

/** latency_ms on terminal rows is the poll duration (ruling wd2 006). */
export async function pollHeadroom(db: D1Database, day: string, budgetMs = POLL_BUDGET_MS): Promise<PollHeadroom> {
  const { results } = await db
    .prepare(
      `SELECT latency_ms FROM events
        WHERE reporting_day = ?1 AND event_name IN ${TERMINAL} AND latency_ms IS NOT NULL
        ORDER BY latency_ms`
    )
    .bind(day)
    .all<{ latency_ms: number }>();
  const sorted = (results ?? []).map((r) => r.latency_ms);
  const p95 = percentile(sorted, 95);
  const max = sorted.length ? sorted[sorted.length - 1] : null;
  return {
    budgetMs,
    jobs: sorted.length,
    p50Ms: percentile(sorted, 50),
    p95Ms: p95,
    maxMs: max,
    p95PctOfBudget: p95 === null ? null : (p95 / budgetMs) * 100,
    maxPctOfBudget: max === null ? null : (max / budgetMs) * 100,
    over80Pct: sorted.filter((v) => v > budgetMs * 0.8).length,
  };
}

// ─── 6. Provider health ────────────────────────────────────────────────────

export interface ProbeGap {
  fromMs: number;
  toMs: number;
  minutes: number;
}

export interface FlagNotOk {
  atMs: number;
  flag: string;
  value: string;
}

export interface AnimationsNotOk {
  atMs: number;
  /** RD's value for status.animations, or 'absent' when the key was missing. */
  value: string;
}

export interface Tripwires {
  warnTwoConsecutiveOver3s: boolean;
  alertSingleOver10s: boolean;
  alertGapOver20m: boolean;
  /** status.animations anything other than the string 'ok', absence
   *  included. Separate from the generic flag count because an absent or
   *  non-ok animations flag is the TF-004 pre-flight failure shape. */
  alertAnimationsNotOk: boolean;
  fired: string[];
}

export interface ProviderHealth {
  probes: number;
  expectedSlots: number;
  coveragePct: number | null;
  operational: number;
  uptimePct: number | null;
  minMs: number | null;
  medianMs: number | null;
  p95Ms: number | null;
  maxMs: number | null;
  gapsOver20m: ProbeGap[];
  flagsNotOk: FlagNotOk[];
  animationsNotOk: AnimationsNotOk[];
  tripwires: Tripwires;
}

interface ProbeRow {
  occurred_at_ms: number;
  latency_ms: number | null;
  provider_status: string | null;
  event_json: string;
}

/**
 * Coverage (probes seen / slots expected) is printed beside uptime so a dead
 * canary can never read as 100%. Expected slots come from the day's half-open
 * UTC range, so a 23 or 25 hour day expects 92 or 100, not 96. For today's
 * (still running) day pass nowMs and the range is clipped to now. Gaps are
 * measured only between consecutive observed probes; the day's edges belong
 * to coverage, not to the gap list.
 */
export async function providerHealth(
  db: D1Database,
  day: string,
  nowMs: number,
  slotMs = PROBE_SLOT_MS
): Promise<ProviderHealth> {
  const { results } = await db
    .prepare(
      `SELECT occurred_at_ms, latency_ms, provider_status, event_json FROM events
        WHERE reporting_day = ?1 AND event_name = 'provider.status'
        ORDER BY occurred_at_ms`
    )
    .bind(day)
    .all<ProbeRow>();
  const rows = results ?? [];

  const range = nyDayRangeUtc(day);
  const endMs = Math.min(range.endMs, nowMs);
  const expectedSlots = Math.max(0, Math.floor((endMs - range.startMs) / slotMs));

  const latencies = rows.map((r) => r.latency_ms).filter((v): v is number => typeof v === 'number').sort((a, b) => a - b);
  const operational = rows.filter((r) => r.provider_status === 'operational').length;

  // Gaps: the interval between two CONSECUTIVE OBSERVED probes only. The
  // edges of the day are not gaps (coverage reports those, and better); a day
  // with fewer than two probes therefore has zero gaps. Synthesizing an edge
  // interval fabricated a 1,440 minute alert on a day with no probes at all
  // (dev, 2026-09-18) and would have opened production's first digest with a
  // false alert for the hours before the probe existed.
  const gapLimitMs = 20 * 60 * 1000;
  const gapsOver20m: ProbeGap[] = [];
  for (let i = 1; i < rows.length; i++) {
    const span = rows[i].occurred_at_ms - rows[i - 1].occurred_at_ms;
    if (span > gapLimitMs) {
      gapsOver20m.push({ fromMs: rows[i - 1].occurred_at_ms, toMs: rows[i].occurred_at_ms, minutes: Math.round(span / 60000) });
    }
  }

  // Flags: every value under extra.raw.status that is not the string 'ok'.
  // A value of 'unknown' here is what RD RETURNED (seen 2026-09-19 18:00 and
  // 18:15 on background_removal); a key RD omitted never reaches this loop.
  // The animations flag is therefore checked explicitly, presence included.
  const flagsNotOk: FlagNotOk[] = [];
  const animationsNotOk: AnimationsNotOk[] = [];
  for (const r of rows) {
    try {
      const raw = (JSON.parse(r.event_json) as { extra?: { raw?: { status?: Record<string, unknown> } } }).extra?.raw?.status;
      if (raw && typeof raw === 'object') {
        for (const [flag, value] of Object.entries(raw)) {
          if (value !== 'ok') flagsNotOk.push({ atMs: r.occurred_at_ms, flag, value: String(value) });
        }
        if (raw.animations !== 'ok') {
          animationsNotOk.push({ atMs: r.occurred_at_ms, value: raw.animations === undefined ? 'absent' : String(raw.animations) });
        }
      } else {
        if (r.provider_status !== 'operational') {
          flagsNotOk.push({ atMs: r.occurred_at_ms, flag: 'probe', value: r.provider_status ?? 'unknown' });
        }
        animationsNotOk.push({ atMs: r.occurred_at_ms, value: 'absent' });
      }
    } catch {
      flagsNotOk.push({ atMs: r.occurred_at_ms, flag: 'event_json', value: 'unparseable' });
      animationsNotOk.push({ atMs: r.occurred_at_ms, value: 'absent' });
    }
  }

  // Tripwires (provisional; retune from the real p95 after seven days).
  const series = rows.map((r) => r.latency_ms ?? 0);
  let warnTwo = false;
  for (let i = 1; i < series.length; i++) {
    if (series[i - 1] > 3000 && series[i] > 3000) { warnTwo = true; break; }
  }
  const alertSingle = series.some((v) => v > 10_000);
  const alertGap = gapsOver20m.length > 0;
  const alertAnimations = animationsNotOk.length > 0;
  const fired: string[] = [];
  if (warnTwo) fired.push('warn: two consecutive probes over 3,000 ms');
  if (alertSingle) fired.push('alert: a probe over 10,000 ms');
  if (alertGap) fired.push(`alert: ${gapsOver20m.length} gap${gapsOver20m.length === 1 ? '' : 's'} over 20 minutes between consecutive probes`);
  if (alertAnimations) {
    const values = Array.from(new Set(animationsNotOk.map((a) => a.value))).join(', ');
    fired.push(`alert: animations flag not ok on ${animationsNotOk.length} probe${animationsNotOk.length === 1 ? '' : 's'} (${values})`);
  }

  return {
    probes: rows.length,
    expectedSlots,
    coveragePct: pct(rows.length, expectedSlots),
    operational,
    uptimePct: pct(operational, rows.length),
    minMs: latencies.length ? latencies[0] : null,
    medianMs: percentile(latencies, 50),
    p95Ms: percentile(latencies, 95),
    maxMs: latencies.length ? latencies[latencies.length - 1] : null,
    gapsOver20m,
    flagsNotOk,
    animationsNotOk,
    tripwires: {
      warnTwoConsecutiveOver3s: warnTwo,
      alertSingleOver10s: alertSingle,
      alertGapOver20m: alertGap,
      alertAnimationsNotOk: alertAnimations,
      fired,
    },
  };
}

// ─── 7. Missed digests (the one case a missing email reaches nobody) ───────

export interface MissedDigest {
  day: string;
  /** 'failed' with attempts and the last error, or 'no row' when no slot ever
   *  ran for that day (Worker down, cron not firing). */
  state: string;
  attempts: number | null;
  errorCode: string | null;
}

/**
 * Prior days in the last seven whose digest never went out: rows in
 * digest_runs whose state is not 'sent', plus days with no row at all once the
 * digest existed (days before the first row ever are not missed, they are
 * pre-launch). Never throws; on any error returns [] so it can never block the
 * current day's send.
 */
export async function missedDigests(db: D1Database, day: string): Promise<MissedDigest[]> {
  try {
    const from = previousDay(day, 7);
    const to = previousDay(day, 1);
    const [{ results }, first] = await Promise.all([
      db
        .prepare('SELECT reporting_day, state, attempts, error_code FROM digest_runs WHERE reporting_day BETWEEN ?1 AND ?2 ORDER BY reporting_day')
        .bind(from, to)
        .all<{ reporting_day: string; state: string; attempts: number | null; error_code: string | null }>(),
      db.prepare('SELECT MIN(reporting_day) AS first_day FROM digest_runs').first<{ first_day: string | null }>(),
    ]);
    const rows = new Map((results ?? []).map((r) => [r.reporting_day, r]));
    const firstDay = first?.first_day ?? null;
    const missed: MissedDigest[] = [];
    for (let n = 7; n >= 1; n--) {
      const d = previousDay(day, n);
      const r = rows.get(d);
      if (r) {
        if (r.state !== 'sent') missed.push({ day: d, state: r.state, attempts: r.attempts, errorCode: r.error_code });
      } else if (firstDay !== null && d > firstDay) {
        missed.push({ day: d, state: 'no row', attempts: null, errorCode: null });
      }
    }
    return missed;
  } catch {
    return [];
  }
}

// ─── All six, for one reporting day ────────────────────────────────────────

export interface DigestData {
  day: string;
  generatedAtMs: number;
  styleSize: StyleSizeRow[];
  trend: Trend;
  refunds: RefundReconciliation;
  abandoned: AbandonedPaid;
  poll: PollHeadroom;
  provider: ProviderHealth;
  missed: MissedDigest[];
}

export async function gatherDigest(db: D1Database, day: string, nowMs: number): Promise<DigestData> {
  const [styleSize, trendData, refunds, abandoned, poll, provider, missed] = await Promise.all([
    byStyleAndSize(db, day),
    trend(db, day),
    refundsExpectedVsIssued(db, day, nowMs),
    abandonedPaidTasks(db, day),
    pollHeadroom(db, day),
    providerHealth(db, day, nowMs),
    missedDigests(db, day),
  ]);
  return { day, generatedAtMs: nowMs, styleSize, trend: trendData, refunds, abandoned, poll, provider, missed };
}
