.PHONY: setup start compose decompose check migrate

setup:
	cd src/app && bun install
	cd src/server && uv sync

start:
	cd src/app && bun run dev & \
	cd src/server && uv run src/main.py & \
	wait

compose:
	docker compose up -d --wait
	docker compose exec -T database psql -U logdog -d postgres -f /docker-entrypoint-initdb.d/init.sql

decompose:
	docker compose down

check:
	cd src/app && bun run check
	cd src/server && uv run ruff check --fix && uv run ruff format && uv run ty check

migrate:
	cd src/server && uv run alembic upgrade head
