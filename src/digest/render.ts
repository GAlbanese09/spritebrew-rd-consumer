// spritebrew-rd-consumer/src/digest/render.ts
//
// The morning digest as one plain HTML document: inline styles only, no
// images, no chart, no external CSS. Body order is most alarming first:
// abandoned paid tasks, missing refunds, tripwires fired, failure rate with
// trend, poll headroom, provider health, then the style and size table.
// The subject carries the two numbers that matter so it reads from a lock
// screen: failed of total, and abandoned paid.

import type { DigestData } from './queries';

function esc(v: unknown): string {
  return String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function pctStr(v: number | null, digits = 1): string {
  return v === null ? 'n/a' : `${v.toFixed(digits)}%`;
}

function ppStr(v: number | null): string {
  if (v === null) return 'n/a';
  const sign = v > 0 ? '+' : '';
  return `${sign}${v.toFixed(1)} pp`;
}

function msStr(v: number | null): string {
  return v === null ? 'n/a' : `${Math.round(v).toLocaleString('en-US')} ms`;
}

function nyTime(ms: number): string {
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York', hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit',
  }).format(new Date(ms));
}

const TD = 'padding:4px 10px;border:1px solid #ddd;text-align:left;font:13px/1.4 -apple-system,Segoe UI,Helvetica,Arial,sans-serif';
const TH = `${TD};background:#f3f3f3;font-weight:600`;
const H2 = 'font:600 15px/1.3 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:22px 0 6px';
const P = 'font:13px/1.5 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:4px 0';
const ALARM = 'color:#b00020;font-weight:600';
const OK = 'color:#1a7f37';

function table(headers: string[], rows: string[][]): string {
  const head = headers.map((h) => `<th style="${TH}">${esc(h)}</th>`).join('');
  const body = rows.length
    ? rows.map((r) => `<tr>${r.map((c) => `<td style="${TD}">${c}</td>`).join('')}</tr>`).join('')
    : `<tr><td style="${TD}" colspan="${headers.length}">none</td></tr>`;
  return `<table cellspacing="0" cellpadding="0" style="border-collapse:collapse;margin:4px 0 8px"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

export function digestSubject(d: DigestData): string {
  const t = d.trend.yesterday;
  return `SpriteBrew Daily ${d.day}: ${t.failed} of ${t.total} failed, ${d.abandoned.count} abandoned paid`;
}

export function digestHtml(d: DigestData, environment: string): string {
  const t = d.trend;
  const parts: string[] = [];

  parts.push(`<h1 style="font:600 18px/1.3 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:0 0 4px">SpriteBrew Daily ${esc(d.day)}</h1>`);
  parts.push(`<p style="${P};color:#666">Reporting day ${esc(d.day)} America/New_York. Generated ${esc(new Date(d.generatedAtMs).toISOString())} from the ${esc(environment)} event ledger. Money truth stays in the KV token ledger; this is observability.</p>`);

  // 1. Abandoned paid tasks (money wasted). Own number, never in the failure rate.
  const ab = d.abandoned;
  parts.push(`<h2 style="${H2}">Abandoned paid tasks: <span style="${ab.count ? ALARM : OK}">${ab.count}</span> (${esc(ab.totalUsd.toFixed(2))} USD at ${esc(ab.pricePerCallUsd.toFixed(3))} per call)</h2>`);
  parts.push(`<p style="${P}">A job whose final attempt exhausted the poll budget and that was refunded: Retro Diffusion finished and billed a task nobody collected. This is the money-wasted number and it is separate from the customer-outcome failure rate below.</p>`);
  parts.push(table(['job_id', 'RD task id', 'attempt', 'tokens refunded'],
    ab.tasks.map((x) => [esc(x.jobId), esc(x.rdTaskId ?? 'unknown'), esc(x.attempt ?? ''), esc(x.tokensRefunded ?? '')])));

  // 2. Missing refunds.
  const rf = d.refunds;
  parts.push(`<h2 style="${H2}">Refunds: expected ${rf.expected}, issued ${rf.issued}, <span style="${rf.missing ? ALARM : OK}">missing ${rf.missing}</span>${rf.inGrace ? `, ${rf.inGrace} inside the ${rf.graceMinutes} minute grace` : ''}</h2>`);
  parts.push(table(['missing refund: job_id'], rf.missingJobIds.map((j) => [esc(j)])));

  // 3. Tripwires.
  const tw = d.provider.tripwires;
  parts.push(`<h2 style="${H2}">Tripwires: <span style="${tw.fired.length ? ALARM : OK}">${tw.fired.length ? `${tw.fired.length} fired` : 'none fired'}</span></h2>`);
  if (tw.fired.length) parts.push(`<ul style="${P}">${tw.fired.map((f) => `<li>${esc(f)}</li>`).join('')}</ul>`);
  parts.push(`<p style="${P};color:#666">Thresholds are provisional (warn: two consecutive probes over 3,000 ms; alert: any probe over 10,000 ms; alert: any slot gap over 20 minutes). They were set from seven samples on 2026-09-19 and are due for retuning from the real p95 after seven days.</p>`);

  // 4. Failure rate with trend.
  parts.push(`<h2 style="${H2}">Failure rate: ${pctStr(t.yesterday.failureRatePct)} yesterday (${t.yesterday.failed} of ${t.yesterday.total})</h2>`);
  parts.push(table(['window', 'days', 'terminal', 'failed', 'rescued', 'failure rate', 'rescue rate'],
    [t.yesterday, t.last7, t.prior7].map((w) => [
      esc(w.label), esc(w.fromDay === w.toDay ? w.fromDay : `${w.fromDay} to ${w.toDay}`), esc(w.total), esc(w.failed), esc(w.rescued), pctStr(w.failureRatePct), pctStr(w.rescueRatePct),
    ])));
  parts.push(`<p style="${P}">Last 7 versus prior 7: failure rate ${esc(ppStr(t.failureRateDeltaPp))}, rescue rate ${esc(ppStr(t.rescueRateDeltaPp))} (percentage points).</p>`);

  // 5. Poll budget headroom.
  const po = d.poll;
  parts.push(`<h2 style="${H2}">Poll budget headroom: p95 ${pctStr(po.p95PctOfBudget)} of ${po.budgetMs.toLocaleString('en-US')} ms, <span style="${po.over80Pct ? ALARM : OK}">${po.over80Pct} job${po.over80Pct === 1 ? '' : 's'} over 80%</span></h2>`);
  parts.push(table(['jobs', 'p50', 'p95', 'max', 'max % of budget'],
    [[esc(po.jobs), msStr(po.p50Ms), msStr(po.p95Ms), msStr(po.maxMs), pctStr(po.maxPctOfBudget)]]));
  parts.push(`<p style="${P};color:#666">Terminal latency_ms is the poll duration of the operation the event names. The leading indicator for the next refund wave.</p>`);

  // 6. Provider health, coverage beside uptime.
  const pv = d.provider;
  parts.push(`<h2 style="${H2}">Provider health: uptime ${pctStr(pv.uptimePct)} on coverage <span style="${pv.coveragePct !== null && pv.coveragePct < 95 ? ALARM : OK}">${pctStr(pv.coveragePct)}</span> (${pv.probes} of ${pv.expectedSlots} slots)</h2>`);
  parts.push(table(['probes', 'expected', 'operational', 'min', 'median', 'p95', 'max'],
    [[esc(pv.probes), esc(pv.expectedSlots), esc(pv.operational), msStr(pv.minMs), msStr(pv.medianMs), msStr(pv.p95Ms), msStr(pv.maxMs)]]));
  parts.push(`<p style="${P}">Slot gaps over 20 minutes: ${pv.gapsOver20m.length}</p>`);
  if (pv.gapsOver20m.length) parts.push(table(['from (ET)', 'to (ET)', 'minutes'], pv.gapsOver20m.map((g) => [esc(nyTime(g.fromMs)), esc(nyTime(g.toMs)), esc(g.minutes)])));
  parts.push(`<p style="${P}">Flags not ok: ${pv.flagsNotOk.length}</p>`);
  if (pv.flagsNotOk.length) parts.push(table(['at (ET)', 'flag', 'value'], pv.flagsNotOk.map((f) => [esc(nyTime(f.atMs)), esc(f.flag), esc(f.value)])));

  // 7. Style and size.
  parts.push(`<h2 style="${H2}">Yesterday by style and size</h2>`);
  parts.push(table(['style', 'size', 'total', 'succeeded', 'rescued', 'failed', 'failure rate'],
    d.styleSize.map((r) => [esc(r.style), esc(r.requestedSize), esc(r.total), esc(r.succeeded), esc(r.rescued), esc(r.failed), pctStr(r.failureRatePct)])));

  return `<!doctype html><html><body style="margin:16px;background:#fff;color:#111">${parts.join('\n')}</body></html>`;
}
