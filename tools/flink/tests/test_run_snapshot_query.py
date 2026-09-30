"""CLI tests for tools.flink.cc_deploy.run_snapshot_query."""

from __future__ import annotations

from typing import Any

import pytest
from typer.testing import CliRunner

from tools.flink.cc_deploy import run_snapshot_query as cli
from tools.flink.cc_deploy.flink_deploy import SnapshotQueryResult
from tools.flink.cc_deploy.run_snapshot_query import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _no_real_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "load_dotenv_file", lambda: False)


def _fake_result() -> SnapshotQueryResult:
    return SnapshotQueryResult(
        statement_name="snapshot-orders-1",
        sql="SELECT * FROM orders",
        columns=["id"],
        rows=[(1,)],
        rowcount=1,
        elapsed_sec=0.1,
    )


def test_requires_table_or_sql() -> None:
    result = runner.invoke(app, [])
    assert result.exit_code == 1
    assert "one of --table or --sql is required" in result.output


def test_table_and_sql_are_mutually_exclusive() -> None:
    result = runner.invoke(app, ["--table", "orders", "--sql", "SELECT 1"])
    assert result.exit_code == 1
    assert "mutually exclusive" in result.output


def test_invalid_table_builder_args_surface_value_error() -> None:
    result = runner.invoke(app, ["--table", "orders", "--limit", "0"])
    assert result.exit_code == 1
    assert "limit must be a positive integer" in result.output


def test_happy_path_with_table_builds_select_and_prints(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        cli,
        "run_snapshot_query",
        lambda sql, **kw: captured.update(sql=sql, kw=kw) or _fake_result(),
    )

    result = runner.invoke(app, ["--table", "orders", "--limit", "5"])

    assert result.exit_code == 0, result.output
    assert captured["sql"] == "SELECT * FROM orders LIMIT 5"
    assert captured["kw"]["delete_statement"] is True


def test_happy_path_with_raw_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        cli,
        "run_snapshot_query",
        lambda sql, **kw: captured.update(sql=sql) or _fake_result(),
    )

    result = runner.invoke(app, ["--sql", "SELECT COUNT(*) FROM orders"])

    assert result.exit_code == 0
    assert captured["sql"] == "SELECT COUNT(*) FROM orders"


def test_keep_statement_flag_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        cli,
        "run_snapshot_query",
        lambda sql, **kw: captured.update(kw=kw) or _fake_result(),
    )

    result = runner.invoke(app, ["--table", "orders", "--keep-statement"])

    assert result.exit_code == 0
    assert captured["kw"]["delete_statement"] is False


def test_runtime_error_from_query_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(sql: str, **kw: Any) -> None:
        raise RuntimeError("Snapshot query failed: boom")

    monkeypatch.setattr(cli, "run_snapshot_query", _raise)

    result = runner.invoke(app, ["--table", "orders"])

    assert result.exit_code == 1
    assert "Snapshot query failed" in result.output


def test_json_shorthand_selects_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(cli, "run_snapshot_query", lambda sql, **kw: _fake_result())
    monkeypatch.setattr(
        cli,
        "print_snapshot_result",
        lambda result, output, show_meta: captured.update(output=output),
    )

    result = runner.invoke(app, ["--table", "orders", "--json"])

    assert result.exit_code == 0
    assert captured["output"] == "json"
