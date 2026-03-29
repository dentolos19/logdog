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
