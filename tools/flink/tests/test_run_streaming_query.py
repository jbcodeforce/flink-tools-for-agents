"""CLI tests for tools.flink.cc_deploy.run_streaming_query."""

from __future__ import annotations

from typing import Any

import pytest
from typer.testing import CliRunner

from tools.flink.cc_deploy import run_streaming_query as cli
from tools.flink.cc_deploy.run_streaming_query import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _no_real_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "load_dotenv_file", lambda: False)


def test_requires_table_or_sql() -> None:
    result = runner.invoke(app, [])
    assert result.exit_code == 1
    assert "one of --table or --sql is required" in result.output


def test_table_and_sql_are_mutually_exclusive() -> None:
    result = runner.invoke(app, ["--table", "orders", "--sql", "SELECT 1"])
    assert result.exit_code == 1
    assert "mutually exclusive" in result.output


def test_invalid_table_builder_args_surface_value_error() -> None:
    result = runner.invoke(app, ["--table", "orders", "--limit", "-1"])
    assert result.exit_code == 1
    assert "limit must be a positive integer" in result.output


def test_happy_path_with_table_builds_select(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        cli,
        "run_streaming_query",
        lambda sql, **kw: captured.update(sql=sql, kw=kw),
    )

    result = runner.invoke(app, ["--table", "orders", "--max-rows", "3", "--output", "json"])

    assert result.exit_code == 0, result.output
    assert captured["sql"] == "SELECT * FROM orders"
    assert captured["kw"]["max_rows"] == 3
    assert captured["kw"]["output"] == "json"
    assert captured["kw"]["delete_statement"] is True


def test_happy_path_with_raw_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(cli, "run_streaming_query", lambda sql, **kw: captured.update(sql=sql))

    result = runner.invoke(app, ["--sql", "SELECT * FROM orders WHERE amount > 100"])

    assert result.exit_code == 0
    assert captured["sql"] == "SELECT * FROM orders WHERE amount > 100"


def test_keep_statement_and_quiet_meta_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(cli, "run_streaming_query", lambda sql, **kw: captured.update(kw=kw))

    result = runner.invoke(app, ["--table", "orders", "--keep-statement", "--quiet-meta"])

    assert result.exit_code == 0
    assert captured["kw"]["delete_statement"] is False
    assert captured["kw"]["show_meta"] is False


def test_runtime_error_from_query_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(sql: str, **kw: Any) -> None:
        raise RuntimeError("Streaming query failed: boom")

    monkeypatch.setattr(cli, "run_streaming_query", _raise)

    result = runner.invoke(app, ["--table", "orders"])

    assert result.exit_code == 1
    assert "Streaming query failed" in result.output
