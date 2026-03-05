# Project Guidelines

## Architecture

- Monorepo with three primary services.
- `src/app`: Next.js 16 frontend (React 19) deployed with OpenNext/Cloudflare.
- `src/server-worker`: Cloudflare Worker (Hono) that manages a Cloudflare Container and proxies requests to the Python API.
- `src/server`: FastAPI service that validates uploaded plaintext logs, calls OpenRouter for schema inference, and stores rows in SQLite via SQLAlchemy.
- Request path is frontend -> worker -> containerized FastAPI (`src/server-worker/index.ts`, `src/server/main.py`).

## Build And Run

- Use Bun for JavaScript/TypeScript projects in this repo.
- Frontend (`src/app`): install deps with `bun install`, run dev with `bun run dev`, and build with `bun run build`.
- Worker (`src/server-worker`): install deps with `bun install`, run local worker with `bun run dev`.
- Python API (`src/server`): requires Python `>=3.12,<3.14` (`src/server/pyproject.toml`).
- Python container installs with `uv pip install --system -r pyproject.toml` (see `src/server/Dockerfile`).
- Python API start command: `uvicorn main:app --host 0.0.0.0 --port 8000`.
- No dedicated test or lint scripts are currently defined in `src/app/package.json`, `src/server-worker/package.json`, or `src/server/pyproject.toml`.

## Conventions

- Frontend code follows shadcn/ui-style component patterns in `src/app/src/components/ui`.
- Reuse `cn()` from `src/app/src/lib/utils.ts` for class composition.
- Prefer CVA variant patterns for reusable UI primitives (example: `src/app/src/components/ui/button.tsx`).
- Keep frontend imports aligned with `@/*` path alias from `src/app/tsconfig.json`.
- Worker logic should keep container lifecycle handling in `src/server-worker/index.ts` (`getState()`, `startAndWaitForPorts()`, then proxy via `instance.fetch`).
- FastAPI endpoints should raise explicit `HTTPException` on validation or upstream failures, mirroring `src/server/main.py`.

## Pitfalls

- Keep package managers scoped by area: Bun for `src/app` and `src/server-worker`, Python tooling for `src/server`.
- Both JS projects rely on `postinstall` generating Wrangler types (`bun run types`); missing generated env types can break TypeScript checks.
- The worker passes `process.env` into container startup; environment consistency matters for local and deployed behavior.
