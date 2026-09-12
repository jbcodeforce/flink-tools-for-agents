"""Tests for sl_dbt CLI."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from tools.dbt.sl_dbt import app, _upsert_sources_yaml


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


def test_init_creates_pyproject_toml(cli_runner: CliRunner, tmp_path: Path) -> None:
    """init should write pipelines/pyproject.toml with uv-compatible content."""
    project_root = tmp_path / "my_project"

    result = cli_runner.invoke(
        app,
        ["init", str(project_root)],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output

    pyproject = project_root / "pipelines" / "pyproject.toml"
    assert pyproject.exists(), "pyproject.toml was not created"

    content = pyproject.read_text()
    # project name injected correctly
    assert 'name = "pipelines"' in content
    # dbt dependencies present
    assert "dbt-confluent" in content
    assert "dbt-core" in content


def test_init_idempotent_preserves_existing_files(cli_runner: CliRunner, tmp_path: Path) -> None:
    """Re-running init without --force must not overwrite files the user has edited."""
    project_root = tmp_path / "my_project"

    # First init
    cli_runner.invoke(app, ["init", str(project_root)], catch_exceptions=False)

    # Simulate a user edit
    pyproject = project_root / "pipelines" / "pyproject.toml"
    pyproject.write_text("# my custom content")

    # Second init — should leave the file alone
    result = cli_runner.invoke(app, ["init", str(project_root)], catch_exceptions=False)
    assert result.exit_code == 0
    assert pyproject.read_text() == "# my custom content"


def test_init_force_overwrites_existing_files(cli_runner: CliRunner, tmp_path: Path) -> None:
    """--force must regenerate files even when they already exist."""
    project_root = tmp_path / "my_project"

    cli_runner.invoke(app, ["init", str(project_root)], catch_exceptions=False)

    pyproject = project_root / "pipelines" / "pyproject.toml"
    pyproject.write_text("# my custom content")

    result = cli_runner.invoke(app, ["init", str(project_root), "--force"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "# my custom content" not in pyproject.read_text()
    assert "dbt-confluent" in pyproject.read_text()


# ---------------------------------------------------------------------------
# add-raw-topic tests
# ---------------------------------------------------------------------------

@pytest.fixture
def initialized_project(cli_runner: CliRunner, tmp_path: Path) -> Path:
    """Return a project_root that has already been `init`-ed."""
    project_root = tmp_path / "my_project"
    result = cli_runner.invoke(app, ["init", str(project_root)], catch_exceptions=False)
    assert result.exit_code == 0
    return project_root


def test_add_raw_topic_creates_ddl_and_dml(cli_runner: CliRunner, initialized_project: Path) -> None:
    """add-raw-topic should create ddl and dml SQL files under raws/<topic>/."""
    result = cli_runner.invoke(
        app,
        ["add-raw-topic", str(initialized_project), "raw_customers"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    raws_dir = initialized_project / "pipelines" / "models" / "raws" / "raw_customers"
    ddl = raws_dir / "ddl.raw_customers.sql"
    dml = raws_dir / "dml.raw_customers.sql"

    assert ddl.exists(), "DDL file not created"
    assert dml.exists(), "DML file not created"

    ddl_text = ddl.read_text()
    assert "CREATE TABLE IF NOT EXISTS raw_customers" in ddl_text
    assert "'kafka.topic'              = 'raw_customers'" in ddl_text

    dml_text = dml.read_text()
    assert "INSERT INTO raw_customers" in dml_text
    assert "synth-001" in dml_text


def test_add_raw_topic_registers_in_sources_yaml(cli_runner: CliRunner, initialized_project: Path) -> None:
    """add-raw-topic should register the topic in raws/sources.yaml."""
    cli_runner.invoke(
        app,
        ["add-raw-topic", str(initialized_project), "raw_orders"],
        catch_exceptions=False,
    )

    sources_path = initialized_project / "pipelines" / "models" / "raws" / "sources.yaml"
    assert sources_path.exists()

    data = yaml.safe_load(sources_path.read_text())
    tables = data["sources"][0]["tables"]
    names = [t["name"] for t in tables]
    assert "raw_orders" in names


def test_add_raw_topic_idempotent(cli_runner: CliRunner, initialized_project: Path) -> None:
    """Running add-raw-topic twice must not duplicate files or sources entries."""
    for _ in range(2):
        result = cli_runner.invoke(
            app,
            ["add-raw-topic", str(initialized_project), "raw_events"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

    sources_path = initialized_project / "pipelines" / "models" / "raws" / "sources.yaml"
    data = yaml.safe_load(sources_path.read_text())
    tables = data["sources"][0]["tables"]
    assert sum(1 for t in tables if t["name"] == "raw_events") == 1


def test_upsert_sources_yaml_multiple_topics(tmp_path: Path) -> None:
    """_upsert_sources_yaml accumulates multiple topics under the same source block."""
    sources_path = tmp_path / "sources.yaml"
    _upsert_sources_yaml(sources_path, "cc_flink", "raw_customers")
    _upsert_sources_yaml(sources_path, "cc_flink", "raw_orders")

    data = yaml.safe_load(sources_path.read_text())
    tables = data["sources"][0]["tables"]
    names = {t["name"] for t in tables}
    assert names == {"raw_customers", "raw_orders"}
    # Only one source block for cc_flink
    assert len(data["sources"]) == 1
