.PHONY: setup start check

setup:
	cd src/app && bun install
	cd src/server && uv sync

start:
	cd src/app && bun run dev & \
	cd src/server && uv run main.py & \
  wait

check:
	cd src/app && bun run check
	cd src/server && uv run ruff check --fix
	cd src/server && uv run ruff format
