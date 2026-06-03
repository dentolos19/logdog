setup:
    cd src/app && bun install
    cd src/server && uv sync
    just migrate

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

check:
    cd src/app && bun run check
    cd src/server && uv run ruff check --fix && uv run ruff format && uv run ty check

migrate:
    cd src/server && uv run alembic upgrade head
