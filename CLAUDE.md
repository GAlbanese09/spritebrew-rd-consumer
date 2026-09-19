# spritebrew-rd-consumer: working rules for Claude Code

This is the Cloudflare Queues consumer Worker for SpriteBrew. It takes generation jobs from `spritebrew-rd-jobs`, calls the Retro Diffusion API, refunds tokens on failure, and runs a 15-minute stale-job sweep. The Pages app lives in the sibling repo `../SpriteBrew`.

All working rules, including the hub-to-Claude-Code room, are in `../SpriteBrew/CLAUDE.md` and `../SpriteBrew/room/README.md`. Read both. Tasks that touch this repo arrive as files in `../SpriteBrew/room/`.

Consumer-specific facts:
- One branch, `main`. Environments are wrangler `preview` (Worker `spritebrew-rd-consumer-dev`) and `production` (`spritebrew-rd-consumer`). Deploy with `npm run deploy:dev` / `npm run deploy:prod`; watch with `npm run tail:dev` / `npm run tail:prod`. Confirm a production deploy with `npx wrangler deployments list --env production`.
- Workers Observability (logs) is on since Sep 18, 2026 and declared in `wrangler.toml`; keep those blocks.
- `RETRO_DIFFUSION_API_KEY` is a Worker secret. Never put it in a file.
- Retro Diffusion's `/v1/status` returns `{"status":{"rd_fast","rd_pro","rd_plus","animations","background_removal"},"updated_at"}`; the fields are nested under `status`.
- The D1 `events` table (`EVENTS_DB`, `src/events.ts`, migrations in `migrations/`) is observability, not money. `recordEvent` never throws and never blocks or fails a job; `token_tx:` in KV stays the source of truth for balances.
