# Dependency Upgrade

## October 15, 2026

The application uses TanStack Start 1.169.2 and Table 9.3.1. Table row models are registered explicitly, preserving pagination, filtering, sorting, and faceting. The chat client uses the native TanStack AI fetcher to preserve UI message parts and forwards run/thread identifiers.

The backend uses PyJWT instead of python-jose because the old dependency chain has advisories without published fixes. HS256 signing, the existing secret, claims, expiration rules, token types, and refresh revocation are preserved. Existing HS256 tokens remain valid. No database migration is needed. Token compatibility, expiration, invalid signatures, and revocation are covered by `uv run python -m unittest discover -s tests` in `src/server`.

Pydantic is updated to the current stable v2 release. Type checks pass with the updated Python lockfile.

Run `just check`, `just build`, and `just test` from the repository root to verify both applications and the migration regression tests.
