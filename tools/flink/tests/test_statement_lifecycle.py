"""Unit tests for tools.flink.cc_deploy.statement_lifecycle."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from confluent_sql.exceptions import OperationalError, StatementNotFoundError

from tools.flink.cc_deploy import statement_lifecycle as lifecycle
from tools.flink.cc_deploy.statement_lifecycle import (
    StatementLifecycleError,
    check_statement_health,
    classify_sql,
    create_statement,
    delete_statement,
    drop_table,
    get_statement_exceptions,
    list_statements,
    statement_properties,
    statement_status,
    submit_statement,
    wait_for_phase,
)

CONFIG = {"FLINK_COMPUTE_POOL_ID": "pool-1"}


def _noop_sleep(_seconds: float) -> None:
    return None


# ---------------------------------------------------------------------------
# classify_sql
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("INSERT INTO t SELECT * FROM s", "streaming_dml"),
        ("insert into t values (1)", "batch_dml"),
        ("CREATE MATERIALIZED TABLE t AS SELECT * FROM s", "streaming_ddl"),
        ("CREATE OR ALTER MATERIALIZED TABLE t AS SELECT * FROM s", "streaming_ddl"),
        ("CREATE TABLE t AS SELECT * FROM s", "streaming_ddl"),
        ("CREATE TABLE t (id STRING)", "snapshot_ddl"),
        ("DROP TABLE t", "snapshot_ddl"),
        ("SELECT * FROM t", "snapshot_ddl"),
    ],
)
def test_classify_sql(sql: str, expected: str) -> None:
    assert classify_sql(sql) == expected


def test_classify_sql_skips_leading_line_comments() -> None:
    sql = "-- a comment\n-- another\nINSERT INTO t SELECT * FROM s"
    assert classify_sql(sql) == "streaming_dml"


def test_classify_sql_comment_only_defaults_to_snapshot_ddl() -> None:
    assert classify_sql("-- just a comment, no newline") == "snapshot_ddl"


def test_statement_properties_ignores_config() -> None:
    assert statement_properties({"anything": "x"}) == {}


# ---------------------------------------------------------------------------
# _phase_from_stmt / _detail_from_stmt (via statement_status)
# ---------------------------------------------------------------------------


def test_phase_from_stmt_dict_status() -> None:
    stmt = SimpleNamespace(status={"phase": "RUNNING", "detail": "ok"})
    assert lifecycle._phase_from_stmt(stmt) == "RUNNING"
    assert lifecycle._detail_from_stmt(stmt) == "ok"


def test_phase_from_stmt_enum_like_phase() -> None:
    stmt = SimpleNamespace(status=None, phase=SimpleNamespace(name="COMPLETED"))
    assert lifecycle._phase_from_stmt(stmt) == "COMPLETED"


def test_phase_from_stmt_plain_phase_value() -> None:
    stmt = SimpleNamespace(status=None, phase="RUNNING")
    assert lifecycle._phase_from_stmt(stmt) == "RUNNING"


def test_phase_from_stmt_unknown() -> None:
    stmt = SimpleNamespace(status=None, phase=None)
    assert lifecycle._phase_from_stmt(stmt) == "UNKNOWN"
    assert lifecycle._detail_from_stmt(stmt) == ""


def test_statement_status_found() -> None:
    conn = SimpleNamespace(
        get_statement=lambda name: SimpleNamespace(status={"phase": "RUNNING", "detail": ""})
    )
    status = statement_status(conn, "stmt-1")
    assert status == {"name": "stmt-1", "phase": "RUNNING", "detail": ""}


def test_statement_status_not_found() -> None:
    def _raise(name: str) -> Any:
        raise StatementNotFoundError("missing", statement_name=name)

    conn = SimpleNamespace(get_statement=_raise)
    status = statement_status(conn, "stmt-1")
    assert status["phase"] == "NOT_FOUND"


# ---------------------------------------------------------------------------
# list_statements
# ---------------------------------------------------------------------------


def test_list_statements_normalizes_payload() -> None:
    payload = {
        "data": [
            {"name": "a", "status": {"phase": "RUNNING", "detail": ""}},
            {"statementName": "b", "status": {"phase": "COMPLETED", "detail": "done"}},
            "not-a-dict",
        ]
    }
    conn = SimpleNamespace(_request=lambda path, params=None: SimpleNamespace(json=lambda: payload))
    result = list_statements(conn)
    assert result["count"] == 2
    assert result["statements"][0] == {"name": "a", "phase": "RUNNING", "detail": ""}
    assert result["statements"][1] == {"name": "b", "phase": "COMPLETED", "detail": "done"}


def test_list_statements_handles_non_list_payload() -> None:
    conn = SimpleNamespace(_request=lambda path, params=None: SimpleNamespace(json=lambda: {"data": "oops"}))
    result = list_statements(conn)
    assert result == {"statements": [], "count": 0}


# ---------------------------------------------------------------------------
# get_statement_exceptions
# ---------------------------------------------------------------------------


def test_get_statement_exceptions_not_found() -> None:
    resp = SimpleNamespace(status_code=404)
    conn = SimpleNamespace(_request=lambda *a, **k: resp)
    assert get_statement_exceptions(conn, "s") == {"name": "s", "exceptions": []}


def test_get_statement_exceptions_http_error() -> None:
    resp = SimpleNamespace(status_code=500, text="boom")
    conn = SimpleNamespace(_request=lambda *a, **k: resp)
    result = get_statement_exceptions(conn, "s")
    assert result == {"name": "s", "error": "HTTP 500", "body": "boom"}


def test_get_statement_exceptions_success() -> None:
    resp = SimpleNamespace(status_code=200, json=lambda: {"exceptions": ["x"]})
    conn = SimpleNamespace(_request=lambda *a, **k: resp)
    assert get_statement_exceptions(conn, "s") == {"exceptions": ["x"]}


def test_get_statement_exceptions_json_failure_falls_back_to_raw() -> None:
    def _bad_json() -> None:
        raise ValueError("not json")

    resp = SimpleNamespace(status_code=200, json=_bad_json)
    conn = SimpleNamespace(_request=lambda *a, **k: resp)
    result = get_statement_exceptions(conn, "s")
    assert result["name"] == "s"
    assert "raw" in result


# ---------------------------------------------------------------------------
# check_statement_health
# ---------------------------------------------------------------------------


def test_check_statement_health_healthy() -> None:
    conn = SimpleNamespace(get_statement=lambda name: SimpleNamespace(status={"phase": "RUNNING", "detail": ""}))
    health = check_statement_health(conn, "s")
    assert health["healthy"] is True
    assert health["phase"] == "RUNNING"


def test_check_statement_health_unhealthy() -> None:
    conn = SimpleNamespace(get_statement=lambda name: SimpleNamespace(status={"phase": "FAILED", "detail": "bad"}))
    health = check_statement_health(conn, "s")
    assert health["healthy"] is False
    assert health["detail"] == "bad"


# ---------------------------------------------------------------------------
# submit_statement
# ---------------------------------------------------------------------------


def test_submit_statement_snapshot_ddl_records_call() -> None:
    calls: list[dict[str, Any]] = []

    def _execute_snapshot_ddl(sql: str, **kwargs: Any) -> SimpleNamespace:
        calls.append({"sql": sql, **kwargs})
        return SimpleNamespace(status={"phase": "COMPLETED", "detail": ""})

    conn = SimpleNamespace(execute_snapshot_ddl=_execute_snapshot_ddl)
    result = submit_statement(conn, CONFIG, "s1", "DROP TABLE t")
    assert result == {"name": "s1", "phase": "COMPLETED", "detail": "", "kind": "snapshot_ddl"}
    assert calls[0]["compute_pool_id"] == "pool-1"


def test_submit_statement_dry_run_sets_properties() -> None:
    captured: dict[str, Any] = {}

    def _execute_snapshot_ddl(sql: str, **kwargs: Any) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(status={"phase": "COMPLETED", "detail": ""})

    conn = SimpleNamespace(execute_snapshot_ddl=_execute_snapshot_ddl)
    submit_statement(conn, CONFIG, "s1", "DROP TABLE t", dry_run=True)
    assert captured["properties"]["sql.dry-run"] == "true"
    assert captured["properties"]["sql.inline-result"] == "false"


def test_submit_statement_streaming_dml_uses_cursor() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.statement = None

        def execute(self, sql: str, **kwargs: Any) -> None:
            self.statement = SimpleNamespace(status={"phase": "RUNNING", "detail": ""})

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

    cursor = FakeCursor()
    conn = SimpleNamespace(closing_streaming_cursor=lambda: cursor)
    result = submit_statement(conn, CONFIG, "s1", "INSERT INTO t SELECT * FROM s")
    assert result["phase"] == "RUNNING"
    assert result["kind"] == "streaming_dml"


def test_submit_statement_unsupported_kind_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lifecycle, "classify_sql", lambda sql: "bogus_kind")
    conn = SimpleNamespace()
    with pytest.raises(StatementLifecycleError, match="Unsupported SQL kind"):
        submit_statement(conn, CONFIG, "s1", "irrelevant")


# ---------------------------------------------------------------------------
# delete_statement
# ---------------------------------------------------------------------------


def test_delete_statement_not_found() -> None:
    def _raise(name: str) -> None:
        raise StatementNotFoundError("missing", statement_name=name)

    conn = SimpleNamespace(delete_statement=_raise)
    assert delete_statement(conn, "s1", sleep=_noop_sleep) == {"name": "s1", "status": "not_found"}


def test_delete_statement_deleted_after_poll() -> None:
    def _get_statement(name: str) -> None:
        raise StatementNotFoundError("gone", statement_name=name)

    conn = SimpleNamespace(delete_statement=lambda name: None, get_statement=_get_statement)
    result = delete_statement(conn, "s1", timeout=5, sleep=_noop_sleep)
    assert result == {"name": "s1", "status": "deleted"}


def test_delete_statement_times_out() -> None:
    conn = SimpleNamespace(
        delete_statement=lambda name: None,
        get_statement=lambda name: SimpleNamespace(status={"phase": "RUNNING", "detail": ""}),
    )
    with pytest.raises(StatementLifecycleError, match="still present after delete timeout"):
        delete_statement(conn, "s1", timeout=0, sleep=_noop_sleep)


# ---------------------------------------------------------------------------
# create_statement
# ---------------------------------------------------------------------------


def test_create_statement_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        lifecycle,
        "submit_statement",
        lambda conn, config, name, sql, **kw: {"name": name, "phase": "COMPLETED", "detail": "", "kind": "snapshot_ddl"},
    )
    conn = SimpleNamespace()
    result = create_statement(conn, CONFIG, "s1", "DROP TABLE t", sleep=_noop_sleep)
    assert result["phase"] == "COMPLETED"


def test_create_statement_retries_on_409(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def _submit(conn: Any, config: Any, name: str, sql: str, **kw: Any) -> dict[str, Any]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError("conflict", http_status_code=409)
        return {"name": name, "phase": "COMPLETED", "detail": "", "kind": "snapshot_ddl"}

    monkeypatch.setattr(lifecycle, "submit_statement", _submit)

    def _get_statement(name: str) -> None:
        raise StatementNotFoundError("gone", statement_name=name)

    conn = SimpleNamespace(delete_statement=lambda name: None, get_statement=_get_statement)
    result = create_statement(conn, CONFIG, "s1", "DROP TABLE t", timeout=5, sleep=_noop_sleep)
    assert calls["n"] == 2
    assert result["phase"] == "COMPLETED"


def test_create_statement_non_409_operational_error_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def _submit(*a: Any, **kw: Any) -> None:
        raise OperationalError("bad request", http_status_code=400)

    monkeypatch.setattr(lifecycle, "submit_statement", _submit)
    conn = SimpleNamespace()
    with pytest.raises(StatementLifecycleError, match="HTTP 400"):
        create_statement(conn, CONFIG, "s1", "DROP TABLE t", sleep=_noop_sleep)


def test_create_statement_conflict_delete_not_found_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def _submit(conn: Any, config: Any, name: str, sql: str, **kw: Any) -> dict[str, Any]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError("conflict", http_status_code=409)
        return {"name": name, "phase": "COMPLETED", "detail": "", "kind": "snapshot_ddl"}

    monkeypatch.setattr(lifecycle, "submit_statement", _submit)

    def _delete_statement(name: str) -> None:
        raise StatementNotFoundError("already gone", statement_name=name)

    def _get_statement(name: str) -> None:
        raise StatementNotFoundError("gone", statement_name=name)

    conn = SimpleNamespace(delete_statement=_delete_statement, get_statement=_get_statement)
    result = create_statement(conn, CONFIG, "s1", "DROP TABLE t", timeout=5, sleep=_noop_sleep)
    assert result["phase"] == "COMPLETED"


def test_create_statement_times_out_waiting_for_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    def _submit(*a: Any, **kw: Any) -> None:
        raise OperationalError("conflict", http_status_code=409)

    monkeypatch.setattr(lifecycle, "submit_statement", _submit)
    conn = SimpleNamespace(
        delete_statement=lambda name: None,
        get_statement=lambda name: SimpleNamespace(status={"phase": "RUNNING", "detail": ""}),
    )
    with pytest.raises(StatementLifecycleError, match="still exists after delete before retry"):
        create_statement(conn, CONFIG, "s1", "DROP TABLE t", timeout=0, sleep=_noop_sleep)


# ---------------------------------------------------------------------------
# wait_for_phase
# ---------------------------------------------------------------------------


def test_wait_for_phase_reaches_accepted_phase() -> None:
    conn = SimpleNamespace(get_statement=lambda name: SimpleNamespace(status={"phase": "RUNNING", "detail": ""}))
    result = wait_for_phase(conn, "s1", {"RUNNING"}, timeout=5, sleep=_noop_sleep)
    assert result["phase"] == "RUNNING"


def test_wait_for_phase_treats_failure_as_terminal() -> None:
    conn = SimpleNamespace(get_statement=lambda name: SimpleNamespace(status={"phase": "FAILED", "detail": "boom"}))
    result = wait_for_phase(conn, "s1", {"RUNNING"}, timeout=5, sleep=_noop_sleep)
    assert result["phase"] == "FAILED"


def test_wait_for_phase_not_found_is_terminal() -> None:
    def _raise(name: str) -> None:
        raise StatementNotFoundError("missing", statement_name=name)

    conn = SimpleNamespace(get_statement=_raise)
    result = wait_for_phase(conn, "s1", {"RUNNING"}, timeout=5, sleep=_noop_sleep)
    assert result["phase"] == "NOT_FOUND"


def test_wait_for_phase_times_out() -> None:
    conn = SimpleNamespace(get_statement=lambda name: SimpleNamespace(status={"phase": "PENDING", "detail": ""}))
    with pytest.raises(StatementLifecycleError, match="Timeout waiting"):
        wait_for_phase(conn, "s1", {"RUNNING"}, timeout=0, sleep=_noop_sleep)


# ---------------------------------------------------------------------------
# drop_table
# ---------------------------------------------------------------------------


def test_drop_table_submits_and_deletes(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: dict[str, Any] = {}
    deleted: dict[str, Any] = {}

    def _create_statement(conn: Any, config: Any, name: str, sql: str, **kw: Any) -> dict[str, Any]:
        submitted["name"] = name
        submitted["sql"] = sql
        return {"name": name, "phase": "COMPLETED", "detail": "", "kind": "snapshot_ddl"}

    def _delete_statement(conn: Any, name: str, **kw: Any) -> dict[str, Any]:
        deleted["name"] = name
        return {"name": name, "status": "deleted"}

    monkeypatch.setattr(lifecycle, "create_statement", _create_statement)
    monkeypatch.setattr(lifecycle, "delete_statement", _delete_statement)

    conn = SimpleNamespace()
    drop_table(conn, CONFIG, "orders", "drop-orders", sleep=_noop_sleep)

    assert submitted["sql"] == "DROP TABLE IF EXISTS `orders`"
    assert deleted["name"] == "drop-orders"


def test_drop_table_materialized(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: dict[str, Any] = {}

    def _create_statement(conn: Any, config: Any, name: str, sql: str, **kw: Any) -> dict[str, Any]:
        submitted["sql"] = sql
        return {}

    monkeypatch.setattr(lifecycle, "create_statement", _create_statement)
    monkeypatch.setattr(lifecycle, "delete_statement", lambda conn, name, **kw: {"status": "deleted"})

    conn = SimpleNamespace()
    drop_table(conn, CONFIG, "orders_mt", "drop-orders-mt", materialized=True, sleep=_noop_sleep)
    assert submitted["sql"] == "DROP MATERIALIZED TABLE IF EXISTS `orders_mt`"
