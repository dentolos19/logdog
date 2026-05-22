.PHONY: dev test check

dev:
	uv run main.py

test:
	uv run pytest

check:
	uv run ruff check --fix
	uv run ruff format
