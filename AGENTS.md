# Agent Instructions

## Tech Architecture

This repository is composed of four projects.

- App (Frontend) @ `src/app`: Next.js 16 w/ OpenNext.js Adapters
- Server (Backend) @ `src/server`: Python FastAPI backend
- Parser (Binary preprocessing) @ `src/parser`: C# (.NET) CLI tool for Windows Event Log (`.evtx`/`.evt`) and binary/blob files; invoked by the server before the main parser pipelines when handling those file types.
- Server Worker @ `src/server-worker`: Cloudflare Worker (Hono) that proxies all requests to the containerised FastAPI server via `@cloudflare/containers`.

## Coding Conventions

- Use existing components under `src/app/src/components/ui/*` instead of introducing new UI primitives.
- Follow shadcn/ui's best practices when designing pages.
- FastAPI endpoints should raise explicit `HTTPException` on validation or upstream failures, mirroring `src/server/main.py`.
- Update the frontend API endpoints in `src/app/src/lib/api/index.ts` (with Better Fetch) accordingly to the server endpoints.
- Keep Zod input and output schemas in sync with the actual server input and responses.

## Database Architecture (Server)

The server uses two separate databases:

- **App database** (`lib/database.py`): Stores users, log groups, files, tables, and processes. Defaults to a local SQLite file (`store/database.db`). In production, set `DATABASE_URL` (libsql://) and `DATABASE_TOKEN` to use a Turso database. A PostgreSQL URL is also accepted when no token is provided.
- **Swarm database** (`lib/database_swarm.py`): One SQLite database per log group, holding the parsed table data. Defaults to `store/<log_group_id>.sqlite3` on disk. In production (same env vars as above), each log group gets its own Turso database managed via the Turso REST API.
- **File storage** (`lib/storage.py`): Uploaded raw files are stored locally under `store/<asset_id>` or in an S3-compatible bucket when `BUCKET_ENDPOINT_URL`, `BUCKET_ACCESS_KEY`, `BUCKET_SECRET_KEY`, and `BUCKET_NAME` are all set.

## Auth System (Server)

- Auth is JWT-based with HTTP-only cookies: a short-lived access token (60 min) and a long-lived refresh token (7 days).
- The `get_current_user` dependency in `lib/auth.py` validates the access token cookie; use it on every protected route.
- The frontend `AuthProvider` (`src/app/src/components/auth-provider.tsx`) is the single source of truth for the authenticated user. On mount it calls `/auth/me`, falling back to `/auth/refresh` once before giving up.

## Data Models (Server)

Key SQLAlchemy models in `lib/models.py`:

| Model | Table | Notes |
|---|---|---|
| `User` | `users` | Email + bcrypt password. Owns log groups and files. |
| `LogGroup` | `logs` | Container for a set of uploaded log files. |
| `LogGroupFile` | `log_files` | Join between a `LogGroup` and a raw `Asset`. |
| `Asset` | `assets` | Metadata for an uploaded file; bytes are in file storage. |
| `LogGroupTable` | `log_tables` | Metadata for a parsed table stored in the swarm DB. |
| `LogGroupProcess` | `log_processes` | Ingestion job record: status, classification JSON, and result JSON. |
| `LogGroupSwarmCredential` | `log_swarm_credentials` | Turso credentials for a log group's swarm DB (production only). |

## Parser Architecture (Server)

- General parser pipelines live under `src/server/lib/parsers/*` and currently include:
  - `structured` for structured sources (including JSON, CSV, and XML-style payloads).
  - `semi_structured` for mixed text + field-like patterns.
  - `unstructured` as the broad fallback for plain text or binary-like decoded content.
- Ignore `keoni` parser when implementing the general parser routing/fallback flow unless explicitly requested.
- Use common parser entry points through the `ParserPipeline` interface in `src/server/lib/parsers/registry.py`:
  - `supports(...)` for file/content capability scoring.
  - `ingest(...)` for pipeline ingestion execution.
  - `parse(...)` for core parser implementation.
- Keep parser outputs aligned to the shared `ParserPipelineResult` contract in `src/server/lib/parsers/contracts.py`.
- File uploads can be mixed-format. Prefer per-file parser resolution with score-based fallback instead of forcing a single parser for the whole batch.
- Fallback order for general parsers is `structured -> semi_structured -> unstructured` when support scores are close or weak.
- Keep baseline columns contract-compatible across parsers; parser-specific fields should be additive.
- Call `register_pipelines()` from `lib/parsers/orchestrator.py` exactly once at startup (already done in `main.py` lifespan). Background ingestion jobs are dispatched via `run_ingestion_job(...)` in the same module.

## Testing (Server)

- Tests live in `src/server/tests/` and use **pytest**.
- Run from the `src/server` directory: `uv run pytest` (or `pytest` with the virtualenv active).
- Test files are prefixed `test_*`. Add new tests to that directory; mirror the style of `test_parser_registry.py`.

## When Making Changes

- Keep auth/org behavior centralized in the auth provider; avoid duplicating auth fetch logic inside pages/layouts.
- Prefer extending existing route groups/layout shells over creating parallel navigation/auth patterns.
- Server routes are organised under `src/server/routes/` (`auth.py`, `logs.py`, `parser.py`, `stats.py`). Add new routers there and register them in `main.py`.
