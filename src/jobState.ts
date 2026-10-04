// spritebrew-rd-consumer/src/jobState.ts
//
// The job record's keys and lifetime. Each record lives in two stores, the R2
// mirror `jobs/{jobId}.json` (read first by the status route) and KV
// `job:{jobId}`, byte for byte the same JSON. Release 2 writes them through
// src/status.ts: terminal records strictly (4.10), the `running` record best
// effort. Release 1's unconditional writer, its terminal-skipping writer and
// the 24-hour debt lifetime (`DEBT_TTL_S`) are retired with the KV-era debts
// (n1-release-2-spec.md revision 9, 6.1).

/** 1h: long enough that a refresh recovers; short enough to bound storage. */
export const JOB_TTL_S = 60 * 60;

export function jobStateR2Key(jobId: string): string {
  return `jobs/${jobId}.json`;
}
