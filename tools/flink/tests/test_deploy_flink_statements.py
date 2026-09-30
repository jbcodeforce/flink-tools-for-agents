"""Unit and CLI tests for tools.flink.cc_deploy.deploy_flink_statements."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from tools.flink.cc_deploy import deploy_flink_statements as cli
from tools.flink.cc_deploy.deploy_flink_statements import (
    app,
    find_repo_root,
    load_dotenv_file,
    print_groups,
    resolve_dotenv_path,
)
from tools.flink.manifest.manifest import DeployManifest, DropTableRef, StatementRef, write_manifest

runner = CliRunner()


def _write_manifest(sql_dir: Path, manifest: DeployManifest) -> None:
    write_manifest(manifest, sql_dir / "deploy_manifest.json")


# ---------------------------------------------------------------------------
# find_repo_root / resolve_dotenv_path / load_dotenv_file
# ---------------------------------------------------------------------------


def test_find_repo_root_walks_up_to_marker(tmp_path: Path) -> None:
    (tmp_path / "references" / "flink" / "valid").mkdir(parents=True)
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    assert find_repo_root(nested) == tmp_path


def test_find_repo_root_raises_when_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        find_repo_root(tmp_path)


def test_resolve_dotenv_path_explicit_relative(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "custom.env").write_text("X=1", encoding="utf-8")
    monkeypatch.setenv("DOTENV_FILE", "custom.env")
    assert resolve_dotenv_path(tmp_path) == tmp_path / "custom.env"


def test_resolve_dotenv_path_defaults_to_repo_root_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    (tmp_path / ".env").write_text("X=1", encoding="utf-8")
    assert resolve_dotenv_path(tmp_path) == tmp_path / ".env"


def test_resolve_dotenv_path_returns_none_when_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    assert resolve_dotenv_path(tmp_path) is None


def test_load_dotenv_file_prefers_confluent_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / "creds.env"
    env_file.write_text("FLINK_TEST_MARKER=explicit\n", encoding="utf-8")
    monkeypatch.setenv("CONFLUENT_ENV_FILE", str(env_file))
    monkeypatch.delenv("FLINK_TEST_MARKER", raising=False)

    assert load_dotenv_file() is True
    assert __import__("os").environ["FLINK_TEST_MARKER"] == "explicit"


def test_load_dotenv_file_falls_back_to_repo_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Path.expanduser() reads $HOME directly (not Path.home()), so redirect it to
    # a directory with no .confluent/.env — otherwise this could load the real
    # developer credentials file described in CLAUDE.md.
    monkeypatch.delenv("CONFLUENT_ENV_FILE", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "no-such-home"))
    (tmp_path / "references" / "flink" / "valid").mkdir(parents=True)
    (tmp_path / ".env").write_text("FLINK_TEST_MARKER2=fromrepo\n", encoding="utf-8")
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    monkeypatch.delenv("FLINK_TEST_MARKER2", raising=False)

    assert load_dotenv_file(start=tmp_path) is True
    assert __import__("os").environ["FLINK_TEST_MARKER2"] == "fromrepo"


def test_load_dotenv_file_returns_false_when_nothing_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONFLUENT_ENV_FILE", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "no-such-home"))
    empty_root = tmp_path / "empty"
    (empty_root / "references" / "flink" / "valid").mkdir(parents=True)

    assert load_dotenv_file(start=empty_root) is False


# ---------------------------------------------------------------------------
# print_groups
# ---------------------------------------------------------------------------


def test_print_groups_shows_flags(capsys: pytest.CaptureFixture[str]) -> None:
    manifest = DeployManifest(
        groups={
            "ddl": [StatementRef(name="d1", file="ddl.sql")],
            "pipeline": [StatementRef(name="p1", file="p.sql"), StatementRef(name="p2", file="p2.sql")],
        },
        deploy_all=["ddl", "pipeline"],
        undeploy_all=["pipeline"],
    )
    print_groups(manifest)
    out = capsys.readouterr().out
    assert "ddl: 1 statement(s) (deploy_all)" in out
    assert "pipeline: 2 statement(s) (deploy_all, undeploy_all)" in out


# ---------------------------------------------------------------------------
# CLI: groups / deploy / undeploy / drop-tables
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_real_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "load_dotenv_file", lambda: False)


def test_cli_missing_sql_dir_errors() -> None:
    result = runner.invoke(app, ["--sql-dir", "/no/such/dir", "groups"])
    assert result.exit_code == 1
    assert "sql-dir not found" in result.output


def test_cli_missing_manifest_errors(tmp_path: Path) -> None:
    result = runner.invoke(app, ["--sql-dir", str(tmp_path), "groups"])
    assert result.exit_code == 1
    assert "Manifest not found" in result.output


def test_cli_groups_happy_path(tmp_path: Path) -> None:
    manifest = DeployManifest(
        groups={"ddl": [StatementRef(name="d1", file="ddl.sql")]},
        deploy_all=["ddl"],
    )
    _write_manifest(tmp_path, manifest)

    result = runner.invoke(app, ["--sql-dir", str(tmp_path), "groups"])

    assert result.exit_code == 0
    assert "ddl: 1 statement(s) (deploy_all)" in result.output


def test_cli_deploy_calls_deploy_statements(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "ddl.sql").write_text("CREATE TABLE t (id STRING)", encoding="utf-8")
    manifest = DeployManifest(
        groups={"ddl": [StatementRef(name="d1", file="ddl.sql")]},
        deploy_all=["ddl"],
    )
    _write_manifest(tmp_path, manifest)

    monkeypatch.setattr(cli, "get_config", lambda: {"FLINK_COMPUTE_POOL_ID": "pool"})
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(
        cli,
        "deploy_statements",
        lambda statements, sql_dir, config, rerun=False: recorded.update(
            statements=statements, sql_dir=sql_dir, rerun=rerun
        ),
    )

    result = runner.invoke(app, ["--sql-dir", str(tmp_path), "deploy", "--group", "ddl"])

    assert result.exit_code == 0, result.output
    assert "deploy --group ddl complete." in result.output
    assert recorded["statements"][0].name == "d1"
    assert recorded["rerun"] is False


def test_cli_deploy_rerun_flag_forwarded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "ddl.sql").write_text("CREATE TABLE t (id STRING)", encoding="utf-8")
    manifest = DeployManifest(groups={"ddl": [StatementRef(name="d1", file="ddl.sql")]}, deploy_all=["ddl"])
    _write_manifest(tmp_path, manifest)

    monkeypatch.setattr(cli, "get_config", lambda: {})
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(
        cli,
        "deploy_statements",
        lambda statements, sql_dir, config, rerun=False: recorded.update(rerun=rerun),
    )

    result = runner.invoke(app, ["--sql-dir", str(tmp_path), "deploy", "--group", "ddl", "--rerun"])

    assert result.exit_code == 0, result.output
    assert recorded["rerun"] is True


def test_cli_deploy_unknown_group_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = DeployManifest(groups={"ddl": [StatementRef(name="d1", file="ddl.sql")]}, deploy_all=["ddl"])
    _write_manifest(tmp_path, manifest)
    monkeypatch.setattr(cli, "get_config", lambda: {})

    result = runner.invoke(app, ["--sql-dir", str(tmp_path), "deploy", "--group", "nope"])

    assert result.exit_code == 1
    assert "Unknown group" in result.output


def test_cli_undeploy_all_calls_full_undeploy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = DeployManifest(
        groups={"pipeline": [StatementRef(name="p1", file="p.sql")]},
        drop_tables=[DropTableRef(table="orders")],
    )
    _write_manifest(tmp_path, manifest)
    monkeypatch.setattr(cli, "get_config", lambda: {})
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(
        cli,
        "full_undeploy",
        lambda manifest, config, drop_tables_after, sql_dir=None: recorded.update(
            drop_tables_after=drop_tables_after
        ),
    )

    result = runner.invoke(app, ["--sql-dir", str(tmp_path), "undeploy", "--no-drop-tables"])

    assert result.exit_code == 0
    assert recorded["drop_tables_after"] is False


def test_cli_undeploy_single_group_calls_undeploy_statements(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = DeployManifest(groups={"pipeline": [StatementRef(name="p1", file="p.sql")]})
    _write_manifest(tmp_path, manifest)
    monkeypatch.setattr(cli, "get_config", lambda: {})
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(
        cli,
        "undeploy_statements",
        lambda statements, config, sql_dir=None: recorded.update(statements=statements),
    )

    result = runner.invoke(app, ["--sql-dir", str(tmp_path), "undeploy", "--group", "pipeline"])

    assert result.exit_code == 0
    assert recorded["statements"][0].name == "p1"


def test_cli_drop_tables_requires_manifest_entries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = DeployManifest(groups={"pipeline": [StatementRef(name="p1", file="p.sql")]})
    _write_manifest(tmp_path, manifest)
    monkeypatch.setattr(cli, "get_config", lambda: {})

    result = runner.invoke(app, ["--sql-dir", str(tmp_path), "drop-tables"])

    assert result.exit_code == 1
    assert "No drop_tables defined" in result.output


def test_cli_drop_tables_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = DeployManifest(drop_tables=[DropTableRef(table="orders")])
    _write_manifest(tmp_path, manifest)
    monkeypatch.setattr(cli, "get_config", lambda: {})
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(
        cli,
        "flink_drop_tables",
        lambda tables, manifest, config: recorded.update(tables=tables),
    )

    result = runner.invoke(app, ["--sql-dir", str(tmp_path), "drop-tables"])

    assert result.exit_code == 0
    assert "drop-tables complete." in result.output
    assert recorded["tables"][0].table == "orders"
