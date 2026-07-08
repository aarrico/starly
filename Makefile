.PHONY: up down lint fmt test test-unit test-integration

up:
	docker compose up -d --wait

down:
	docker compose down

lint:
	uv run ruff format --check .
	uv run ruff check .

fmt:
	uv run ruff format .
	uv run ruff check --fix .

test: test-unit test-integration

test-unit:
	uv run pytest

test-integration:
	uv run pytest -m integration
