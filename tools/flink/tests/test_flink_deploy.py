"""Unit tests for tools.flink.cc_deploy.flink_deploy."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from confluent_sql.exceptions import OperationalError

from tools.flink.cc_deploy import flink_deploy
from tools.flink.cc_deploy.flink_deploy import (
    SnapshotQueryResult,
    _csv_escape,
    _format_snapshot_table,
    _sanitize_statement_name,
    build_select_sql,
    default_snapshot_statement_name,
    default_streaming_statement_name,
    deploy_statements,
    drop_statement_name_for_table,
    drop_tables,
    drop_tables_by_name,
    format_snapshot_rows,
    format_streaming_row,
    full_undeploy,
    get_config,
    print_snapshot_result,
    read_sql,
    run_create,
    run_delete,
    run_drop_table,
    run_snapshot_query,
    run_streaming_query,
    submit_statement,
    undeploy_statements,
    wait_for_phases,
)
from tools.flink.cc_deploy.statement_lifecycle import StatementLifecycleError
from tools.flink.manifest.manifest import DeployManifest, DropTableRef, StatementRef

REQUIRED_ENV_VARS = [
    "FLINK_API_KEY",
    "FLINK_API_SECRET",
    "CONFLUENT_CLOUD_API_KEY",
    "CONFLUENT_CLOUD_API_SECRET",
    "FLINK_ORG_ID",
    "ORGANIZATION_ID",
    "ORG_ID",
    "FLINK_ENV_ID",
    "CC_ENV_ID",
    "ENVIRONMENT_ID",
    "ENV_ID",
    "FLINK_COMPUTE_POOL_ID",
    "CPOOLID",
    "FLINK_DATABASE_NAME",
    "CLOUD_PROVIDER",
    "CLOUD_REGION",
    "FLINK_BASE_URL",
    "FLINK_REST_ENDPOINT",
]


@pytest.fixture(autouse=True)
def _clean_flink_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure no real Confluent Cloud credentials leak into these tests."""
    for var in REQUIRED_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _fake_flink_connection(conn: Any):
    @contextmanager
    def _cm(config: dict[str, str], *, user_agent: str = ""):
        yield conn

    return _cm


def _fake_conn_with_cursor(cursor: Any) -> Any:
    """Build a fake connection whose closing_cursor()/closing_streaming_cursor() return *cursor*."""

    class _Conn:
        def closing_cursor(self, **kwargs: Any) -> Any:
            return cursor

    return _Conn()


CONFIG_FOR_SNAPSHOT = {"FLINK_COMPUTE_POOL_ID": "pool-1"}


# ---------------------------------------------------------------------------
# get_config
# ---------------------------------------------------------------------------


def test_get_config_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLINK_API_KEY", "key")
    monkeypatch.setenv("FLINK_API_SECRET", "secret")
    monkeypatch.setenv("FLINK_ORG_ID", "org")
    monkeypatch.setenv("FLINK_ENV_ID", "env")
    monkeypatch.setenv("FLINK_COMPUTE_POOL_ID", "pool")
    monkeypatch.setenv("FLINK_DATABASE_NAME", "db")

    cfg = get_config()

    assert cfg["FLINK_API_KEY"] == "key"
    assert cfg["CLOUD_PROVIDER"] == "aws"
    assert cfg["CLOUD_REGION"] == "us-west-2"
    assert "FLINK_REST_ENDPOINT" not in cfg


def test_get_config_uses_fallback_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONFLUENT_CLOUD_API_KEY", "key")
    monkeypatch.setenv("CONFLUENT_CLOUD_API_SECRET", "secret")
    monkeypatch.setenv("ORG_ID", "org")
    monkeypatch.setenv("ENV_ID", "env")
    monkeypatch.setenv("CPOOLID", "pool")
    monkeypatch.setenv("FLINK_DATABASE_NAME", "db")
    monkeypatch.setenv("FLINK_BASE_URL", "https://example.com/")

    cfg = get_config()

    assert cfg["FLINK_REST_ENDPOINT"] == "https://example.com"


def test_get_config_missing_vars_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(SystemExit) as exc_info:
        get_config()
    assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# read_sql
# ---------------------------------------------------------------------------


def test_read_sql_strips_content(tmp_path: Path) -> None:
    (tmp_path / "a.sql").write_text("\n  SELECT 1  \n", encoding="utf-8")
    assert read_sql(tmp_path, "a.sql") == "SELECT 1"


def test_read_sql_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_sql(tmp_path, "missing.sql")


# ---------------------------------------------------------------------------
# wait_for_phases (CLI wrapper)
# ---------------------------------------------------------------------------


def test_wait_for_phases_success(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(
        flink_deploy,
        "lifecycle_wait_for_phase",
        lambda *a, **kw: {"phase": "RUNNING", "detail": ""},
    )
    wait_for_phases(object(), "s1", {"RUNNING"})
    assert "s1: RUNNING" in capsys.readouterr().out


def test_wait_for_phases_failed_raises_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        flink_deploy,
        "lifecycle_wait_for_phase",
        lambda *a, **kw: {"phase": "FAILED", "detail": "bad sql"},
    )
    with pytest.raises(RuntimeError, match="bad sql"):
        wait_for_phases(object(), "s1", {"RUNNING"})


def test_wait_for_phases_unaccepted_phase_raises_timeout() -> None:
    def _fake_wait(*a: Any, **kw: Any) -> dict[str, Any]:
        return {"phase": "PENDING", "detail": ""}

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(flink_deploy, "lifecycle_wait_for_phase", _fake_wait)
        with pytest.raises(TimeoutError):
            wait_for_phases(object(), "s1", {"RUNNING"}, timeout=5)


def test_wait_for_phases_converts_lifecycle_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*a: Any, **kw: Any) -> None:
        raise StatementLifecycleError("boom")

    monkeypatch.setattr(flink_deploy, "lifecycle_wait_for_phase", _raise)
    with pytest.raises(TimeoutError, match="boom"):
        wait_for_phases(object(), "s1", {"RUNNING"})


# ---------------------------------------------------------------------------
# submit_statement (CLI wrapper) / run_create / run_delete
# ---------------------------------------------------------------------------


def test_submit_statement_forwards_dry_run_false(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        flink_deploy,
        "lifecycle_submit_statement",
        lambda conn, config, name, sql, dry_run: captured.update(dry_run=dry_run),
    )
    submit_statement(object(), {}, "s1", "SELECT 1")
    assert captured["dry_run"] is False


def test_run_create_success(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(flink_deploy, "lifecycle_submit_statement", lambda *a, **kw: None)
    run_create(object(), {}, "s1", "SELECT 1")
    assert "Creating statement: s1" in capsys.readouterr().out


def test_run_create_conflict_retries_after_delete(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls = {"n": 0}

    def _submit(conn: Any, config: Any, name: str, sql: str, dry_run: bool) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError("conflict", http_status_code=409)

    monkeypatch.setattr(flink_deploy, "lifecycle_submit_statement", _submit)
    monkeypatch.setattr(flink_deploy, "lifecycle_delete_statement", lambda conn, name: {"status": "deleted"})

    run_create(object(), {}, "s1", "SELECT 1")

    assert calls["n"] == 2
    assert "already exists (409), deleting and retrying" in capsys.readouterr().out


def test_run_create_non_409_raises_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*a: Any, **kw: Any) -> None:
        raise OperationalError("bad", http_status_code=400)

    monkeypatch.setattr(flink_deploy, "lifecycle_submit_statement", _raise)
    with pytest.raises(RuntimeError, match="Failed to create s1"):
        run_create(object(), {}, "s1", "SELECT 1")


def test_run_create_lifecycle_error_raises_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*a: Any, **kw: Any) -> None:
        raise StatementLifecycleError("boom")

    monkeypatch.setattr(flink_deploy, "lifecycle_submit_statement", _raise)
    with pytest.raises(RuntimeError, match="boom"):
        run_create(object(), {}, "s1", "SELECT 1")


def test_run_delete_quiet_suppresses_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(flink_deploy, "lifecycle_delete_statement", lambda conn, name: {"status": "deleted"})
    run_delete(object(), "s1", quiet=True)
    assert capsys.readouterr().out == ""


def test_run_delete_reports_not_found(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(flink_deploy, "lifecycle_delete_statement", lambda conn, name: {"status": "not_found"})
    run_delete(object(), "s1")
    assert "not found" in capsys.readouterr().out


def test_run_delete_converts_lifecycle_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(conn: Any, name: str) -> None:
        raise StatementLifecycleError("stuck")

    monkeypatch.setattr(flink_deploy, "lifecycle_delete_statement", _raise)
    with pytest.raises(TimeoutError, match="stuck"):
        run_delete(object(), "s1")


# ---------------------------------------------------------------------------
# deploy_statements / undeploy_statements
# ---------------------------------------------------------------------------


def test_deploy_statements_reads_sql_and_creates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "ddl.orders.sql").write_text("CREATE TABLE orders (id STRING)", encoding="utf-8")
    created: list[tuple[str, str]] = []

    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(object()))
    monkeypatch.setattr(
        flink_deploy,
        "run_create",
        lambda conn, config, name, sql: created.append((name, sql)),
    )

    statements = [StatementRef(name="orders-ddl", file="ddl.orders.sql")]
    deploy_statements(statements, sql_dir=tmp_path, config={})

    assert created == [("orders-ddl", "CREATE TABLE orders (id STRING)")]


def test_deploy_statements_records_ledger_entry(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "ddl.orders.sql").write_text("CREATE TABLE orders (id STRING)", encoding="utf-8")
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(object()))
    monkeypatch.setattr(flink_deploy, "run_create", lambda conn, config, name, sql: None)

    statements = [StatementRef(name="orders-ddl", file="ddl.orders.sql")]
    deploy_statements(statements, sql_dir=tmp_path, config=CONFIG_FOR_SNAPSHOT)

    from tools.flink.cc_deploy.deploy_state import load_state

    state = load_state(tmp_path)
    assert "orders-ddl" in state


def test_deploy_statements_skips_unchanged_and_reruns_on_flag(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "ddl.orders.sql").write_text("CREATE TABLE orders (id STRING)", encoding="utf-8")
    created: list[str] = []
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(object()))
    monkeypatch.setattr(flink_deploy, "run_create", lambda conn, config, name, sql: created.append(name))

    statements = [StatementRef(name="orders-ddl", file="ddl.orders.sql")]

    deploy_statements(statements, sql_dir=tmp_path, config=CONFIG_FOR_SNAPSHOT)
    assert created == ["orders-ddl"]

    # Second deploy with unchanged SQL: skipped, no new create.
    deploy_statements(statements, sql_dir=tmp_path, config=CONFIG_FOR_SNAPSHOT)
    assert created == ["orders-ddl"]

    # --rerun forces a redeploy even though nothing changed.
    deploy_statements(statements, sql_dir=tmp_path, config=CONFIG_FOR_SNAPSHOT, rerun=True)
    assert created == ["orders-ddl", "orders-ddl"]


def test_deploy_statements_auto_reruns_when_sql_changes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sql_file = tmp_path / "ddl.orders.sql"
    sql_file.write_text("CREATE TABLE orders (id STRING)", encoding="utf-8")
    created: list[str] = []
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(object()))
    monkeypatch.setattr(flink_deploy, "run_create", lambda conn, config, name, sql: created.append(name))

    statements = [StatementRef(name="orders-ddl", file="ddl.orders.sql")]
    deploy_statements(statements, sql_dir=tmp_path, config=CONFIG_FOR_SNAPSHOT)
    assert created == ["orders-ddl"]

    sql_file.write_text("CREATE TABLE orders (id STRING, amount DECIMAL(10, 2))", encoding="utf-8")
    deploy_statements(statements, sql_dir=tmp_path, config=CONFIG_FOR_SNAPSHOT)
    assert created == ["orders-ddl", "orders-ddl"]


def test_deploy_statements_skips_connection_when_all_unchanged(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "ddl.orders.sql").write_text("CREATE TABLE orders (id STRING)", encoding="utf-8")
    monkeypatch.setattr(flink_deploy, "run_create", lambda conn, config, name, sql: None)
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(object()))

    statements = [StatementRef(name="orders-ddl", file="ddl.orders.sql")]
    deploy_statements(statements, sql_dir=tmp_path, config=CONFIG_FOR_SNAPSHOT)

    def _fail_connection(*a: Any, **kw: Any) -> None:
        raise AssertionError("flink_connection should not be called when everything is skipped")

    monkeypatch.setattr(flink_deploy, "flink_connection", _fail_connection)
    deploy_statements(statements, sql_dir=tmp_path, config=CONFIG_FOR_SNAPSHOT)


def test_undeploy_statements_deletes_each(monkeypatch: pytest.MonkeyPatch) -> None:
    deleted: list[str] = []
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(object()))
    monkeypatch.setattr(flink_deploy, "run_delete", lambda conn, name: deleted.append(name))

    statements = [StatementRef(name="a", file="a.sql"), StatementRef(name="b", file="b.sql")]
    undeploy_statements(statements, config={})

    assert deleted == ["a", "b"]


def test_undeploy_statements_forgets_ledger_entries(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "ddl.orders.sql").write_text("CREATE TABLE orders (id STRING)", encoding="utf-8")
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(object()))
    monkeypatch.setattr(flink_deploy, "run_create", lambda conn, config, name, sql: None)
    statements = [StatementRef(name="orders-ddl", file="ddl.orders.sql")]
    deploy_statements(statements, sql_dir=tmp_path, config=CONFIG_FOR_SNAPSHOT)

    from tools.flink.cc_deploy.deploy_state import load_state

    assert "orders-ddl" in load_state(tmp_path)

    monkeypatch.setattr(flink_deploy, "run_delete", lambda conn, name: None)
    undeploy_statements(statements, config=CONFIG_FOR_SNAPSHOT, sql_dir=tmp_path)

    assert "orders-ddl" not in load_state(tmp_path)


# ---------------------------------------------------------------------------
# run_drop_table / drop_tables / drop_tables_by_name
# ---------------------------------------------------------------------------


def test_run_drop_table_success(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(
        "tools.flink.cc_deploy.statement_lifecycle.drop_table",
        lambda conn, config, table, name, materialized=False: None,
    )
    run_drop_table(object(), {}, "orders", "drop-orders")
    assert "Dropping table: orders" in capsys.readouterr().out


def test_run_drop_table_warns_on_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _raise(*a: Any, **kw: Any) -> None:
        raise RuntimeError("could not drop")

    monkeypatch.setattr("tools.flink.cc_deploy.statement_lifecycle.drop_table", _raise)
    run_drop_table(object(), {}, "orders", "drop-orders")
    assert "warning: could not drop orders" in capsys.readouterr().err


def test_drop_tables_noop_when_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail_connection(*a: Any, **kw: Any) -> None:
        raise AssertionError("flink_connection should not be called")

    monkeypatch.setattr(flink_deploy, "flink_connection", _fail_connection)
    manifest = DeployManifest()
    drop_tables([], manifest=manifest, config={})


def test_drop_tables_calls_run_drop_table_per_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    dropped: list[tuple[str, str, bool]] = []
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(object()))
    monkeypatch.setattr(
        flink_deploy,
        "run_drop_table",
        lambda conn, config, table, name, materialized=False: dropped.append((table, name, materialized)),
    )
    manifest = DeployManifest(drop_statement_prefix="demo-drop")
    refs = [DropTableRef(table="orders", materialized=False), DropTableRef(table="orders_mt", materialized=True)]

    drop_tables(refs, manifest=manifest, config={})

    assert dropped == [
        ("orders", "demo-drop-orders", False),
        ("orders_mt", "demo-drop-orders-mt", True),
    ]


def test_drop_tables_by_name_uses_default_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    dropped: list[tuple[str, str]] = []
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(object()))
    monkeypatch.setattr(
        flink_deploy,
        "run_drop_table",
        lambda conn, config, table, name, materialized=False: dropped.append((table, name)),
    )
    drop_tables_by_name(["orders"], config={})
    assert dropped == [("orders", "cleanup-drop-orders")]


def test_drop_tables_by_name_noop_when_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail_connection(*a: Any, **kw: Any) -> None:
        raise AssertionError("flink_connection should not be called")

    monkeypatch.setattr(flink_deploy, "flink_connection", _fail_connection)
    drop_tables_by_name([], config={})


# ---------------------------------------------------------------------------
# drop_statement_name_for_table / _sanitize_statement_name
# ---------------------------------------------------------------------------


def test_drop_statement_name_for_table_sanitizes_dots() -> None:
    assert drop_statement_name_for_table("drop", "db.orders") == "drop-db-orders"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Orders", "orders"),
        ("my table!!", "my-table"),
        ("--leading-and-trailing--", "leading-and-trailing"),
        ("", "table"),
    ],
)
def test_sanitize_statement_name(raw: str, expected: str) -> None:
    assert _sanitize_statement_name(raw) == expected


def test_sanitize_statement_name_truncates_to_48_chars() -> None:
    long_name = "x" * 100
    result = _sanitize_statement_name(long_name)
    assert len(result) == 48


# ---------------------------------------------------------------------------
# full_undeploy
# ---------------------------------------------------------------------------


def test_full_undeploy_calls_undeploy_and_drop(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        flink_deploy,
        "undeploy_statements",
        lambda statements, config, user_agent, sql_dir=None: calls.append("undeploy"),
    )
    monkeypatch.setattr(
        flink_deploy,
        "drop_tables",
        lambda tables, manifest, config: calls.append("drop"),
    )
    manifest = DeployManifest(
        groups={"pipeline": [StatementRef(name="p1", file="p1.sql")]},
        drop_tables=[DropTableRef(table="orders")],
    )
    full_undeploy(manifest, config={})
    assert calls == ["undeploy", "drop"]


def test_full_undeploy_skips_drop_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        flink_deploy,
        "undeploy_statements",
        lambda statements, config, user_agent, sql_dir=None: calls.append("undeploy"),
    )
    monkeypatch.setattr(flink_deploy, "drop_tables", lambda *a, **kw: calls.append("drop"))
    manifest = DeployManifest(
        groups={"pipeline": [StatementRef(name="p1", file="p1.sql")]},
        drop_tables=[DropTableRef(table="orders")],
    )
    full_undeploy(manifest, config={}, drop_tables_after=False)
    assert calls == ["undeploy"]


def test_full_undeploy_skips_undeploy_when_no_statements(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(flink_deploy, "undeploy_statements", lambda *a, **kw: calls.append("undeploy"))
    monkeypatch.setattr(flink_deploy, "drop_tables", lambda *a, **kw: calls.append("drop"))
    manifest = DeployManifest(drop_tables=[DropTableRef(table="orders")])
    full_undeploy(manifest, config={})
    assert calls == ["drop"]


def test_full_undeploy_forwards_sql_dir_to_undeploy_statements(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(
        flink_deploy,
        "undeploy_statements",
        lambda statements, config, user_agent, sql_dir=None: recorded.update(sql_dir=sql_dir),
    )
    manifest = DeployManifest(groups={"pipeline": [StatementRef(name="p1", file="p1.sql")]})
    full_undeploy(manifest, config={}, sql_dir=tmp_path)
    assert recorded["sql_dir"] == tmp_path


# ---------------------------------------------------------------------------
# build_select_sql / default_*_statement_name
# ---------------------------------------------------------------------------


def test_build_select_sql_defaults() -> None:
    assert build_select_sql("orders") == "SELECT * FROM orders"


def test_build_select_sql_with_where_and_limit() -> None:
    sql = build_select_sql("orders", columns="id, amount", where="amount > 100", limit=10)
    assert sql == "SELECT id, amount FROM orders WHERE amount > 100 LIMIT 10"


def test_build_select_sql_requires_table() -> None:
    with pytest.raises(ValueError, match="table name is required"):
        build_select_sql("   ")


def test_build_select_sql_rejects_non_positive_limit() -> None:
    with pytest.raises(ValueError, match="limit must be a positive integer"):
        build_select_sql("orders", limit=0)


def test_default_snapshot_statement_name_format() -> None:
    name = default_snapshot_statement_name("Orders Table")
    assert name.startswith("snapshot-orders-table-")
    assert name.rsplit("-", 1)[-1].isdigit()


def test_default_streaming_statement_name_format() -> None:
    name = default_streaming_statement_name("orders")
    assert name.startswith("stream-orders-")


# ---------------------------------------------------------------------------
# format_snapshot_rows / _csv_escape / _format_snapshot_table / print_snapshot_result
# ---------------------------------------------------------------------------


def _sample_result(rows: list[Any], columns: list[str]) -> SnapshotQueryResult:
    return SnapshotQueryResult(
        statement_name="s1",
        sql="SELECT * FROM orders",
        columns=columns,
        rows=rows,
        rowcount=len(rows),
        elapsed_sec=0.5,
    )


def test_format_snapshot_rows_json_from_tuples() -> None:
    result = _sample_result([(1, "a")], ["id", "name"])
    payload = format_snapshot_rows(result, output="json")
    assert '"id": 1' in payload
    assert '"name": "a"' in payload


def test_format_snapshot_rows_json_from_dicts() -> None:
    result = _sample_result([{"id": 1, "name": "a"}], ["id", "name"])
    payload = format_snapshot_rows(result, output="json")
    assert '"id": 1' in payload


def test_format_snapshot_rows_csv_escapes_commas() -> None:
    result = _sample_result([("1", "a,b")], ["id", "name"])
    csv_text = format_snapshot_rows(result, output="csv")
    assert csv_text.splitlines()[0] == "id,name"
    assert csv_text.splitlines()[1] == '1,"a,b"'


def test_format_snapshot_rows_table_default() -> None:
    result = _sample_result([("1", "a")], ["id", "name"])
    table_text = format_snapshot_rows(result)
    assert "id" in table_text and "name" in table_text


def test_csv_escape_none_and_quoting() -> None:
    assert _csv_escape(None) == ""
    assert _csv_escape("plain") == "plain"
    assert _csv_escape('has "quote"') == '"has ""quote"""'


def test_format_snapshot_table_empty_rows() -> None:
    assert _format_snapshot_table(["id"], []) == ""


def test_format_snapshot_table_generates_column_names_for_tuples() -> None:
    text = _format_snapshot_table([], [(1, 2)])
    assert "col1" in text and "col2" in text


def test_print_snapshot_result_meta_on_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    result = _sample_result([("1", "a")], ["id", "name"])
    print_snapshot_result(result)
    captured = capsys.readouterr()
    assert "Statement: s1" in captured.err
    assert "id" in captured.out


def test_print_snapshot_result_quiet_meta(capsys: pytest.CaptureFixture[str]) -> None:
    result = _sample_result([], ["id"])
    print_snapshot_result(result, show_meta=False)
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


# ---------------------------------------------------------------------------
# format_streaming_row
# ---------------------------------------------------------------------------


def test_format_streaming_row_dict_table_output() -> None:
    assert format_streaming_row({"id": 1, "name": "a"}) == "1 | a"


def test_format_streaming_row_tuple_json_output() -> None:
    payload = format_streaming_row((1, "a"), columns=["id", "name"], output="json")
    assert payload == '{"id": 1, "name": "a"}'


def test_format_streaming_row_tuple_json_without_columns() -> None:
    payload = format_streaming_row((1, "a"), output="json")
    assert payload == "[1, \"a\"]"


def test_format_streaming_row_changelog_table_output() -> None:
    row = SimpleNamespace(op="INSERT", row={"id": 1})
    assert format_streaming_row(row, returns_changelog=True) == "INSERT | 1"


def test_format_streaming_row_changelog_json_output() -> None:
    row = SimpleNamespace(op="DELETE", row={"id": 1})
    payload = format_streaming_row(row, returns_changelog=True, output="json")
    assert payload == '{"op": "DELETE", "row": {"id": 1}}'


def test_format_streaming_row_csv_output_with_op() -> None:
    row = SimpleNamespace(op="INSERT", row=(1, "a,b"))
    payload = format_streaming_row(row, returns_changelog=True, output="csv")
    assert payload == 'INSERT,1,"a,b"'


# ---------------------------------------------------------------------------
# run_snapshot_query
# ---------------------------------------------------------------------------


class _FakeSnapshotCursor:
    def __init__(self, rows: list[Any], description: list[tuple], raise_on_execute: Exception | None = None) -> None:
        self._rows = rows
        self.description = description
        self._raise_on_execute = raise_on_execute
        self.is_closed = False
        self.deleted = False
        self.executed: dict[str, Any] = {}

    def execute(self, sql: str, **kwargs: Any) -> None:
        self.executed = {"sql": sql, **kwargs}
        if self._raise_on_execute:
            raise self._raise_on_execute

    def fetchall(self) -> list[Any]:
        return self._rows

    def delete_statement(self) -> None:
        self.deleted = True

    def __enter__(self) -> "_FakeSnapshotCursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.is_closed = True


def test_run_snapshot_query_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeSnapshotCursor(rows=[(1, "a")], description=[("id",), ("name",)])
    conn = _fake_conn_with_cursor(cursor)
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(conn))

    result = run_snapshot_query("SELECT * FROM orders", config=CONFIG_FOR_SNAPSHOT)

    assert result.columns == ["id", "name"]
    assert result.rowcount == 1
    assert cursor.deleted is True


def test_run_snapshot_query_requires_sql() -> None:
    with pytest.raises(ValueError, match="sql is required"):
        run_snapshot_query("   ")


def test_run_snapshot_query_wraps_operational_error(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeSnapshotCursor(
        rows=[], description=[], raise_on_execute=OperationalError("boom", http_status_code=500)
    )
    conn = _fake_conn_with_cursor(cursor)
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(conn))

    with pytest.raises(RuntimeError, match="Snapshot query failed.*HTTP 500"):
        run_snapshot_query("SELECT 1", config=CONFIG_FOR_SNAPSHOT)


def test_run_snapshot_query_keeps_statement_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeSnapshotCursor(rows=[], description=[])
    conn = _fake_conn_with_cursor(cursor)
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(conn))

    run_snapshot_query("SELECT 1", config=CONFIG_FOR_SNAPSHOT, delete_statement=False)

    assert cursor.deleted is False


# ---------------------------------------------------------------------------
# run_streaming_query
# ---------------------------------------------------------------------------


class _FakeStreamingCursor:
    def __init__(self, sequence: list[Any], description: list[tuple], returns_changelog: bool = False) -> None:
        self._sequence = list(sequence)
        self.description = description
        self.returns_changelog = returns_changelog
        self.is_closed = False
        self.deleted = False

    @property
    def may_have_results(self) -> bool:
        return bool(self._sequence)

    def fetchone(self) -> Any:
        return self._sequence.pop(0)

    def execute(self, sql: str, **kwargs: Any) -> None:
        self.executed = {"sql": sql, **kwargs}

    def delete_statement(self) -> None:
        self.deleted = True

    def __enter__(self) -> "_FakeStreamingCursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.is_closed = True


def test_run_streaming_query_stops_at_max_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeStreamingCursor(
        sequence=[(1, "a"), (2, "b"), (3, "c"), (4, "d")],
        description=[("id",), ("name",)],
    )
    conn = _fake_conn_with_cursor(cursor)
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(conn))

    stats = run_streaming_query("SELECT * FROM orders", config=CONFIG_FOR_SNAPSHOT, max_rows=2, show_meta=False)

    assert stats.rowcount == 2
    assert cursor.deleted is True


def test_run_streaming_query_polls_on_empty_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeStreamingCursor(
        sequence=[(1, "a"), None, (2, "b")],
        description=[("id",), ("name",)],
    )
    conn = _fake_conn_with_cursor(cursor)
    monkeypatch.setattr(flink_deploy, "flink_connection", _fake_flink_connection(conn))
    sleeps: list[float] = []
    monkeypatch.setattr(flink_deploy.time, "sleep", lambda seconds: sleeps.append(seconds))

    stats = run_streaming_query("SELECT * FROM orders", config=CONFIG_FOR_SNAPSHOT, show_meta=False)

    assert stats.rowcount == 2
    assert len(sleeps) == 1


def test_run_streaming_query_requires_sql() -> None:
    with pytest.raises(ValueError, match="sql is required"):
        run_streaming_query("  ")


def test_run_streaming_query_rejects_non_positive_max_rows() -> None:
    with pytest.raises(ValueError, match="max_rows must be a positive integer"):
        run_streaming_query("SELECT 1", max_rows=0)
