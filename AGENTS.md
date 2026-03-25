# Agent Instructions

## Tech Architecture

This repository is composed of a Node.js project and a Python project.

- App (Frontend) @ `src/app`: Next.js 16 w/ OpenNext.js Adapters
- Server (Backend) @ `src/server`: Python FastAPI backend

## Coding Conventions

- Use existing components under `src/app/src/components/ui/*` instead of introducing new UI primitives.
- Follow shadcn/ui's best practices when designing pages.
- FastAPI endpoints should raise explicit `HTTPException` on validation or upstream failures, mirroring `src/server/main.py`.
- Update the frontend API endpoints in `src/app/src/lib/api/index.ts` (with Better Fetch) accordingly to the server endpoints.
- Keep Zod input and output schemas in sync with the actual server input and responses.

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

## When Making Changes

- Keep auth/org behavior centralized in the auth provider; avoid duplicating auth fetch logic inside pages/layouts.
- Prefer extending existing route groups/layout shells over creating parallel navigation/auth patterns.
