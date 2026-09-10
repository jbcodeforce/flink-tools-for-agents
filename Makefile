.PHONY: install test lint docs docs-build clean

# ── Installation ──────────────────────────────────────────────────────────────
install:
	uv sync --extra all --extra dev

install-flink:
	uv sync --extra flink --extra dev

install-dbt:
	uv sync --extra dbt --extra dev

install-kafka:
	uv sync --extra kafka --extra dev

# ── Testing ───────────────────────────────────────────────────────────────────
test:
	uv run pytest

test-flink:
	uv run pytest tools/flink/tests/

test-dbt:
	uv run pytest tools/dbt/tests/

test-kafka:
	uv run pytest tools/kafka/tests/

test-verbose:
	uv run pytest -v

# ── Linting ───────────────────────────────────────────────────────────────────
lint:
	uv run ruff check .

lint-fix:
	uv run ruff check --fix .

# ── Documentation ─────────────────────────────────────────────────────────────
docs:
	uv run mkdocs serve

docs-build:
	uv run mkdocs build --strict

# ── Utilities ─────────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf site/
