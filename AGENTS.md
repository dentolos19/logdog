# Agent Instructions

## Tech Architecture

This repository is composed of two projects.

- App (Frontend) @ `src/app`: TanStack Start
- Server (Backend) @ `src/server`: Python FastAPI

## Project Conventions

### App (`src/app`)

- Use `bun` for app scripts and package operations.
- Use the `#/*` import alias for app source imports.
- Prefer existing UI components under `src/app/src/components/ui/*` instead of introducing new primitives.
- TanStack file routes live under `src/app/src/routes/*`.
- Any non-route files inside route folders must be prefixed with `-` (for example `-page-header.tsx`) to avoid route generation warnings.
- Keep API access in `src/app/src/lib/server/*` and call backend endpoints through the app proxy path (`/api/*`).
- Do not import `cloudflare:workers` from browser-shared/client modules.

### Server (`src/server`)

- Keep backend modules under `src/server/src/*`.
- Keep `/auth/*` as public routes and protect non-auth routes with `get_current_user`.
- Container startup expects Uvicorn module path `src.main:app` from `/app`.

## Validation Commands

### App

- `bun run types`
- `bunx biome check src`
- `bun run build`

### Server

- `uv run ruff check src`
- `python -m compileall src`

## Coding Conventions

- Use existing components under `src/app/src/components/ui/*` instead of introducing new UI primitives.

## Parsing Pipeline Notes

- The preprocessor now supports profile-aware adaptive cache lookup before heuristic and LLM format classification.
- Known structured formats are routed to deterministic parser keys (`json_lines`, `csv`, `syslog`, `apache_access`, `nginx_access`, `logfmt`, `key_value`) and unknown formats fall back to `unified`.
- Archive payloads (`zip`, `gzip`, `tar`) are expanded into synthetic file names (`outer:inner`) before classification and parsing.

## Evaluation CLI

- Run parser quality evaluation with gold data from the server project root:
  - `python -m tools.eval_logs --input ./samples/eval`
- Supported case layouts:
  - `name.<raw_ext>` paired with `name.gold.json`
  - `<case_dir>/raw.<ext>` paired with `<case_dir>/gold.json`

## Filtered Export API

- Server-side filtered exports are available at `POST /logs/{entry_id}/tables/{table_name}/download/filtered`.
- Supported export formats: `csv`, `json`.
- Supported filters: `search`, `levels`, `field_filters`, `timestamp_from`, `timestamp_to`.
