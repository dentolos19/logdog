set dotenv-load

setup mode="": install compose
    just --no-deps migrate
    if [ "{{ mode }}" != "prerun" ]; then just decompose; fi

install:
    cd src/app && bun install --frozen-lockfile
    cd src/server && uv sync --frozen

start: compose
    cd src/app && bun run dev & \
    cd src/server && uv run src/main.py & \
    wait
    just decompose

compose:
    if [ "${CI:-false}" != "true" ]; then docker compose up --detach --wait; fi

decompose:
    if [ "${CI:-false}" != "true" ]; then docker compose down; fi

build:
    cd src/app && bun run build

check:
    cd src/app && bun run check
    cd src/server && uv run ruff check --fix && uv run ruff format && uv run ty check

test:
    cd src/app && bun test
    cd src/server && uv run python -m unittest discover -s tests

migrate: compose
    if [ "${CI:-false}" != "true" ]; then docker compose exec -T database psql -v ON_ERROR_STOP=1 -U logdog -d postgres -f /docker-entrypoint-initdb.d/init.sql; fi
    cd src/server && uv run alembic upgrade head

deploy: install build
    #!/usr/bin/env bash
    set -euo pipefail
    cd src/app
    secrets=(
        AWS_ACCESS_KEY_ID
        AWS_ENDPOINT_URL_S3
        AWS_REGION
        AWS_SECRET_ACCESS_KEY
        DATABASE_URL
        MEGABASE_URL
        OPENROUTER_API_KEY
        OPENROUTER_MODEL
        OPENROUTER_REFERER
        OPENROUTER_TITLE
        SECRET_KEY
    )
    for secret in "${secrets[@]}"; do if [[ -z "${!secret:-}" ]]; then echo "Missing secret value: $secret" >&2; exit 1; fi; done
    node -e 'process.stdout.write(JSON.stringify(Object.fromEntries(process.argv.slice(1).map((name) => [name, process.env[name]]))))' "${secrets[@]}" | bun wrangler secret bulk
    bun wrangler deploy
