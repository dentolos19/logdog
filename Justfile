set dotenv-load

setup mode="": install
    just compose
    just migrate
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
    docker compose up --detach --wait
    docker compose exec -T database psql -U logdog -d postgres -f /docker-entrypoint-initdb.d/init.sql

decompose:
    docker compose down

build:
    cd src/app && bun run build

check:
    cd src/app && bun run check
    cd src/server && uv run ruff check --fix && uv run ruff format && uv run ty check

migrate:
    cd src/server && uv run alembic upgrade head

deploy: install build
    #!/usr/bin/env bash
    set -euo pipefail
    cd src/app
    names=(
        DATABASE_URL
        MEGABASE_URL
        OPENROUTER_API_KEY
        OPENROUTER_MODEL
        OPENROUTER_REFERER
        OPENROUTER_TITLE
        SECRET_KEY
    )
    for name in "${names[@]}"; do if [[ -z "${!name:-}" ]]; then echo "Missing worker secret value: $name" >&2; exit 1; fi; done
    node -e 'process.stdout.write(JSON.stringify(Object.fromEntries(process.argv.slice(1).map((name) => [name, process.env[name]]))))' "${names[@]}" | bun wrangler secret bulk
    bun wrangler deploy
