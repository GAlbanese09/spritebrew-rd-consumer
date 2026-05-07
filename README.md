# spritebrew-rd-consumer

Cloudflare Queues consumer for SpriteBrew sprite-generation jobs. Pulls a job message off the queue, calls Retro Diffusion's `/v1/inferences` endpoint, writes the result to shared KV, and refunds the customer's tokens inline if the call ultimately fails.

This is the **consumer half** of the queue-and-poll architecture that replaces the synchronous `/api/generate` route in the [spritebrew Pages app](https://github.com/GAlbanese09/spritebrew). The producer half (Pages Function that enqueues jobs and exposes `/api/generation-status/[jobId]`) ships in a follow-up PR.

## Why this exists

Synchronous `await fetch()` to RD took 90–120s and was hitting Cloudflare's ~100s upstream timeout (524 errors), affecting ~40% of paying customers. Architectural fix:

- **Producer (Pages Function)** debits tokens, enqueues `{ jobId, userId, tokenCost, mode, body }`, returns `{ jobId }` in <1s.
- **Consumer (this Worker)** spends 90–120s in I/O wait calling RD, writes terminal state to `job:{jobId}` KV.
- **Browser** polls `/api/generation-status/[jobId]` every 3s until terminal.

Refund-on-failure runs **inline in this Worker** (Confluence 87588866) rather than via a Service Binding (does not exist Worker→Pages) or shared package (overkill for ~25 LOC).

## Architectural references

- Confluence **87490562** — Async-Kickoff Architecture Research (full design + KV schema + polling)
- Confluence **87588866** — Token-Lib Sharing Decision (why inline KV writes)
- Confluence **86933506** — Cloudflare Pages Per-Env Config Runbook (env shape)

## File layout

```
spritebrew-rd-consumer/
├── src/
│   ├── index.ts        # Queue consumer entry — refund-first failure ordering
│   ├── rdClient.ts     # RD API wrapper + RdError + animate fallback
│   ├── refund.ts       # Inline KV refund (mirrors Pages-side schema)
│   └── types.ts        # JobMessage / JobState / Env wire types
├── wrangler.toml       # Per-env queue + KV bindings
├── tsconfig.json
└── package.json
```

## Setup (one-time per environment)

Before the first deploy, in this folder:

```bash
# Create the queues (production + dev each get a main + DLQ)
npx wrangler queues create spritebrew-rd-jobs
npx wrangler queues create spritebrew-rd-jobs-dlq
npx wrangler queues create spritebrew-rd-jobs-dev
npx wrangler queues create spritebrew-rd-jobs-dlq-dev

# Set the RD API key as a secret (paste the same key as production — RD has no separate dev env)
npx wrangler secret put RETRO_DIFFUSION_API_KEY --env preview
# When ready to ship to prod:
# npx wrangler secret put RETRO_DIFFUSION_API_KEY --env production
```

## Deploy

```bash
npm install
npm run types        # tsc --noEmit must pass clean
npm run deploy:dev   # ships to spritebrew-rd-consumer-dev (preview env)
npm run deploy:prod  # ships to spritebrew-rd-consumer (production env)
```

## Verifying dev

```bash
npm run tail:dev
# In another shell, publish a test message via wrangler:
#   npx wrangler queues producer ...  (or trigger from the Pages dev branch)
```

Watch the structured logs — every line is JSON with `jobId`, `userId`, `attempt`, `mode`. Look for:
- `"message received"` followed by either `"rd success"` or `"rd call failed"` then either `"refund applied"` or `"job already terminal; acking"`.

## Debugging a stuck job

1. Read the KV state directly:
   ```bash
   npx wrangler kv key get "job:<jobId>" --binding SPRITEBREW_KV --env preview
   ```
2. If `status: "running"` and `startedAt` is older than 3 minutes, the job is orphaned. Next message attempt will reclaim it (the `RUNNING_TIMEOUT_MS = 180_000` ms guard in `index.ts`).
3. If the job hit terminal `error` and `refunded: true`, check the user's balance via `token_balance:{userId}`.
4. If a customer reports a failed gen with no refund, check `token_idempotency:refund:{jobId}` — present means refund landed; absent means the failure happened before reaching the refund branch (look for DLQ messages).

## KV keys this Worker writes

| Key                                          | Shape                                                          | TTL  |
|----------------------------------------------|----------------------------------------------------------------|------|
| `job:{jobId}`                                | `JobState` (running / success / error)                         | 1h   |
| `token_balance:{userId}`                     | `{ balance: number, updatedAt: number }`                       | none |
| `token_idempotency:refund:{jobId}`           | `{ amount: number, ts: number }`                               | 7d   |
| `token_tx:{userId}:{ts}:{uid}`               | TokenTransaction (refund log)                                  | 90d  |

The producer (Pages) writes `job:{jobId}` initially as `pending` and writes `token_idempotency:gen:{...}` for the debit half. This Worker only writes the refund half.

## Schema contract

`src/refund.ts` writes KV keys whose schema is shared with [spritebrew/src/lib/tokenBalance.ts](https://github.com/GAlbanese09/spritebrew/blob/main/src/lib/tokenBalance.ts) (`creditTokens`). If you change the on-disk shape on either side, **update both files in the same PR** and bump the "Last schema review" date in `src/refund.ts`.

## What this Worker does NOT do

- **No tests.** Inheriting the Pages app's "no automated tests" posture. Smoke testing is manual on the dev URL.
- **No DLQ consumer.** If a job exhausts retries and lands in `spritebrew-rd-jobs-dlq[-dev]`, it sits there until ops triages. A future Worker could subscribe to the DLQ and force-refund + force-error those jobs.
- **No producer logic.** Job creation, debit, and `/api/generation-status` polling endpoint live in the spritebrew Pages app (Build Prompt #2).
- **No imports from the Pages repo.** Self-contained per Confluence 87588866.
