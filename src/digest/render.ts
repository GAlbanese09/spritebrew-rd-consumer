// spritebrew-rd-consumer/src/digest/render.ts
//
// The morning digest as one plain HTML document read on a phone: inline
// styles only, no images, no chart, no external CSS. Body order is most
// alarming first: abandoned paid tasks, missing refunds, tripwires fired,
// failure rate with trend, poll headroom, provider health, then the style and
// size table. The subject carries the two numbers that matter so it reads
// from a lock screen: failed of total, and abandoned paid. Outside production
// the subject is tagged with the environment, since every environment sends
// from the same address.
//
// Layout rules (390pt phone): no table exceeds four columns; a count and its
// percentage share one cell; ids are truncated to their first 8 characters;
// tables are width:100% inside a max-width body; colours are declared
// explicitly with color-scheme light dark rather than left to the client.

import type { DigestData } from './queries';

function esc(v: unknown): string {
  return String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function pctStr(v: number | null, digits = 1): string {
  return v === null ? 'n/a' : `${v.toFixed(digits)}%`;
}

/** "count (share%)" in one cell. */
function countPct(count: number, pctv: number | null): string {
  return `${count} (${pctStr(pctv)})`;
}

function ppStr(v: number | null): string {
  if (v === null) return 'n/a';
  const sign = v > 0 ? '+' : '';
  return `${sign}${v.toFixed(1)} pp`;
}

function msStr(v: number | null): string {
  return v === null ? 'n/a' : `${Math.round(v).toLocaleString('en-US')} ms`;
}

function id8(v: string | null | undefined): string {
  return v ? v.slice(0, 8) : 'unknown';
}

/** Display form of a style: the shared animation prefix carries no
 *  information on a phone and is the largest single cause of overflow. */
function styleLabel(style: string): string {
  return style.startsWith('rd_advanced_animation__') ? style.slice('rd_advanced_animation__'.length) : style;
}

const NY_TIME = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York', hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit',
});
const NY_DATETIME = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/New_York', hour12: false, year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit',
});

function nyTime(ms: number): string {
  return NY_TIME.format(new Date(ms));
}

function nyDateTime(ms: number): string {
  return `${NY_DATETIME.format(new Date(ms)).replace(',', '')} ET`;
}

const FONT = "font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif";
const TD = `padding:5px 8px;border:1px solid #d9d9d9;text-align:left;${FONT};font-size:13px;line-height:1.4;color:#111111;vertical-align:top;word-break:break-word`;
const TH = `${TD};background:#f2f2f2;font-weight:600`;
const H2 = `${FONT};font-size:15px;line-height:1.3;font-weight:600;margin:22px 0 6px;color:#111111`;
const P = `${FONT};font-size:13px;line-height:1.5;margin:4px 0;color:#111111`;
const MUTED = `${P};color:#555555`;
const ALARM = 'color:#b00020;font-weight:600';
const OK = 'color:#1a7f37';

function table(headers: string[], rows: string[][]): string {
  const head = headers.map((h) => `<th style="${TH}">${esc(h)}</th>`).join('');
  const body = rows.length
    ? rows.map((r) => `<tr>${r.map((c) => `<td style="${TD}">${c}</td>`).join('')}</tr>`).join('')
    : `<tr><td style="${TD}" colspan="${headers.length}">none</td></tr>`;
  return `<table role="presentation" cellspacing="0" cellpadding="0" style="border-collapse:collapse;width:100%;margin:4px 0 8px;table-layout:auto"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

export function digestSubject(d: DigestData, environment: string): string {
  const t = d.trend.yesterday;
  const tag = environment === 'production' ? '' : `[${environment}] `;
  return `${tag}SpriteBrew Daily ${d.day}: ${t.failed} of ${t.total} failed, ${d.abandoned.count} abandoned paid`;
}

export function digestHtml(d: DigestData, environment: string): string {
  const t = d.trend;
  const parts: string[] = [];

  parts.push(`<h1 style="${FONT};font-size:18px;line-height:1.3;font-weight:600;margin:0 0 4px;color:#111111">SpriteBrew Daily ${esc(d.day)}${environment === 'production' ? '' : ` <span style="color:#555555;font-weight:400">[${esc(environment)}]</span>`}</h1>`);
  parts.push(`<p style="${MUTED}">Reporting day ${esc(d.day)} America/New_York. Generated ${esc(nyDateTime(d.generatedAtMs))} from the ${esc(environment)} event ledger. Money truth stays in the KV token ledger; this is observability.</p>`);

  // 0. Missed digests: the one case where a missing email reaches nobody.
  for (const m of d.missed) {
    const detail = m.state === 'no row'
      ? 'no slot ran'
      : `${m.attempts ?? 0} attempt${m.attempts === 1 ? '' : 's'}, last error ${esc(m.errorCode ?? 'none recorded')}`;
    parts.push(`<p style="${P}"><span style="${ALARM}">No digest was sent for ${esc(m.day)}</span> (${detail}).</p>`);
  }

  // 1. Abandoned paid tasks (money wasted). Own number, never in the failure rate.
  const ab = d.abandoned;
  parts.push(`<h2 style="${H2}">Abandoned paid tasks: <span style="${ab.count ? ALARM : OK}">${ab.count}</span> (${esc(ab.totalUsd.toFixed(2))} USD at ${esc(ab.pricePerCallUsd.toFixed(3))} per call)</h2>`);
  parts.push(`<p style="${P}">A job whose final attempt exhausted the poll budget and that was refunded: Retro Diffusion finished and billed a task nobody collected. Separate from the customer-outcome failure rate below.</p>`);
  parts.push(table(['job', 'RD task', 'attempt', 'tokens refunded'],
    ab.tasks.map((x) => [esc(id8(x.jobId)), esc(id8(x.rdTaskId)), esc(x.attempt ?? ''), esc(x.tokensRefunded ?? '')])));

  // 2. Missing refunds.
  const rf = d.refunds;
  parts.push(`<h2 style="${H2}">Refunds: expected ${rf.expected}, issued ${rf.issued}, <span style="${rf.missing ? ALARM : OK}">missing ${rf.missing}</span>${rf.inGrace ? `, ${rf.inGrace} inside the ${rf.graceMinutes} minute grace` : ''}</h2>`);
  parts.push(table(['missing refund: job'], rf.missingJobIds.map((j) => [esc(id8(j))])));

  // 3. Tripwires.
  const tw = d.provider.tripwires;
  parts.push(`<h2 style="${H2}">Tripwires: <span style="${tw.fired.length ? ALARM : OK}">${tw.fired.length ? `${tw.fired.length} fired` : 'none fired'}</span></h2>`);
  if (tw.fired.length) parts.push(`<ul style="${P};padding-left:18px">${tw.fired.map((f) => `<li>${esc(f)}</li>`).join('')}</ul>`);
  parts.push(`<p style="${MUTED}">Thresholds are provisional (warn: two consecutive probes over 3,000 ms; alert: any probe over 10,000 ms; alert: any gap over 20 minutes between consecutive probes; alert: the animations flag anything but ok, absence included). Set from seven samples on 2026-09-19 and due for retuning from the real p95 after seven days.</p>`);

  // 4. Failure rate with trend: four columns, count and share in one cell.
  parts.push(`<h2 style="${H2}">Failure rate: ${pctStr(t.yesterday.failureRatePct)} yesterday (${t.yesterday.failed} of ${t.yesterday.total})</h2>`);
  parts.push(table(['window', 'terminal', 'failed', 'rescued'],
    [t.yesterday, t.last7, t.prior7].map((w) => [
      esc(w.label), esc(w.total), countPct(w.failed, w.failureRatePct), countPct(w.rescued, w.rescueRatePct),
    ])));
  parts.push(`<p style="${MUTED}">Windows: yesterday ${esc(t.yesterday.fromDay)}; last 7 days ${esc(t.last7.fromDay)} to ${esc(t.last7.toDay)}; prior 7 days ${esc(t.prior7.fromDay)} to ${esc(t.prior7.toDay)}.</p>`);
  parts.push(`<p style="${P}">Last 7 versus prior 7: failure rate ${esc(ppStr(t.failureRateDeltaPp))}, rescue rate ${esc(ppStr(t.rescueRateDeltaPp))} (percentage points).</p>`);

  // 5. Poll budget headroom.
  const po = d.poll;
  parts.push(`<h2 style="${H2}">Poll budget headroom: <span style="${po.over80Pct ? ALARM : OK}">${po.over80Pct} job${po.over80Pct === 1 ? '' : 's'} over 80%</span> of ${po.budgetMs.toLocaleString('en-US')} ms</h2>`);
  parts.push(table(['jobs', 'p50', 'p95', 'max'],
    [[esc(po.jobs), msStr(po.p50Ms), po.p95Ms === null ? 'n/a' : `${msStr(po.p95Ms)} (${pctStr(po.p95PctOfBudget)})`, po.maxMs === null ? 'n/a' : `${msStr(po.maxMs)} (${pctStr(po.maxPctOfBudget)})`]]));
  parts.push(`<p style="${MUTED}">On a terminal row, latency_ms is how long the job spent waiting on Retro Diffusion for its result. Percentages are of the 180,000 ms poll budget. This is the leading indicator for the next refund wave.</p>`);

  // 6. Provider health: two lines of text, coverage beside uptime.
  const pv = d.provider;
  parts.push(`<h2 style="${H2}">Provider health</h2>`);
  parts.push(`<p style="${P}">Coverage <span style="${pv.coveragePct !== null && pv.coveragePct < 95 ? ALARM : OK}">${pctStr(pv.coveragePct)}</span> (${pv.probes} of ${pv.expectedSlots} slots), uptime ${pctStr(pv.uptimePct)} (${pv.operational} of ${pv.probes} probes operational).</p>`);
  parts.push(`<p style="${P}">Probe latency: min ${msStr(pv.minMs)}, median ${msStr(pv.medianMs)}, p95 ${msStr(pv.p95Ms)}, max ${msStr(pv.maxMs)}.</p>`);
  if (pv.probes < 2) {
    parts.push(`<p style="${P}">Gaps: fewer than two probes, so no interval to measure; coverage above is the signal.</p>`);
  } else if (pv.gapsOver20m.length === 0) {
    parts.push(`<p style="${P}">Gaps over 20 minutes between consecutive probes: none.</p>`);
  } else {
    parts.push(`<p style="${P}">Gaps over 20 minutes between consecutive probes: <span style="${ALARM}">${pv.gapsOver20m.length}</span></p>`);
    parts.push(table(['from (ET)', 'to (ET)', 'minutes'], pv.gapsOver20m.map((g) => [esc(nyTime(g.fromMs)), esc(nyTime(g.toMs)), esc(g.minutes)])));
  }
  if (pv.flagsNotOk.length === 0) {
    parts.push(`<p style="${P}">Flags not ok: none.</p>`);
  } else {
    parts.push(`<p style="${P}">Flags not ok: <span style="${ALARM}">${pv.flagsNotOk.length}</span></p>`);
    parts.push(table(['at (ET)', 'flag', 'value'], pv.flagsNotOk.map((f) => [esc(nyTime(f.atMs)), esc(f.flag), esc(f.value)])));
  }

  // 7. Style and size: style and size in one cell; rescued folded into succeeded.
  parts.push(`<h2 style="${H2}">Yesterday by style and size</h2>`);
  parts.push(table(['style / size', 'total', 'succeeded', 'failed'],
    d.styleSize.map((r) => [
      `${esc(styleLabel(r.style))} ${esc(r.requestedSize)}`,
      esc(r.total),
      r.rescued ? `${r.succeeded + r.rescued} (${r.rescued} rescued)` : esc(r.succeeded),
      countPct(r.failed, r.failureRatePct),
    ])));

  return [
    '<!doctype html><html><head><meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width,initial-scale=1">',
    '<meta name="color-scheme" content="light dark"><meta name="supported-color-schemes" content="light dark">',
    '</head>',
    '<body style="margin:0;padding:16px;background-color:#ffffff;color:#111111">',
    '<div style="max-width:640px;margin:0 auto">',
    parts.join('\n'),
    '</div></body></html>',
  ].join('');
}

/**
 * Plain-text alternative derived from the HTML, so both parts always say the
 * same thing: cells become " | ", block ends become newlines, tags go, the
 * four entities come back.
 */
export function digestText(html: string): string {
  let t = html.replace(/<\/t[dh]>/g, ' | ');
  t = t.replace(/<br\s*\/?>/g, ' ');
  t = t.replace(/<\/(tr|h1|h2|p|li)>/g, '\n');
  t = t.replace(/<[^>]+>/g, '');
  t = t.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"');
  return t.split('\n').map((l) => l.trim()).filter((l) => l.length > 0).join('\n') + '\n';
}
