"""Tests for add_tags helpers and the add-tags CLI command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tools.dbt.flink_dbt_migrate.add_tags import inject_tags_into_config, read_product_name
from tools.dbt.flink_dbt_migrate.migrate_dml_to_dbt import app

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_PIPELINE_DIR = (
    Path(__file__).resolve().parent / "fixtures" / "flink-project" / "pipelines"
)

_MULTILINE_SQL = """\
{{ config(
    materialized='streaming_table',
    distributed_by='tenant_id',
    with={
        'changelog.mode': 'upsert'
    }
) }}

select 1
"""

_ONELINER_SQL = "{{ config(materialized='streaming_table') }}\n\nselect 1\n"

_ALREADY_TAGGED_SQL = """\
{{ config(
    materialized='streaming_table',
    tags=['c360']
) }}

select 1
"""


def _make_tracking_yml(tmp_path: Path, entries: list[dict]) -> Path:
    """Write a minimal tracking.yml with the given table entries."""
    import yaml

    tables = {}
    for e in entries:
        tables[e["table_name"]] = {
            "relative_path": e["relative_path"],
            "dml_sha256": "abc123",
            "status": "done",
            "error": None,
            "migrated_at": "2024-01-01T00:00:00",
        }
    path = tmp_path / "tracking.yml"
    path.write_text(
        yaml.dump({"tables": tables}, default_flow_style=False, allow_unicode=True),
        encoding="utf-8",
    )
    return path


# ---------------------------------------------------------------------------
# Unit tests — inject_tags_into_config
# ---------------------------------------------------------------------------


def test_inject_tags_multiline():
    patched, changed = inject_tags_into_config(_MULTILINE_SQL, "c360")
    assert changed is True
    assert "tags=['c360']" in patched
    # config block still intact
    assert "materialized='streaming_table'" in patched
    assert "select 1" in patched


def test_inject_tags_oneliner():
    patched, changed = inject_tags_into_config(_ONELINER_SQL, "mx")
    assert changed is True
    assert "tags=['mx']" in patched


def test_inject_tags_idempotent_already_tagged():
    patched, changed = inject_tags_into_config(_ALREADY_TAGGED_SQL, "c360")
    assert changed is False
    assert patched == _ALREADY_TAGGED_SQL


def test_inject_tags_idempotent_second_call():
    patched, changed = inject_tags_into_config(_MULTILINE_SQL, "c360")
    assert changed is True
    patched2, changed2 = inject_tags_into_config(patched, "c360")
    assert changed2 is False
    assert patched2 == patched


def test_inject_tags_no_config_block():
    sql = "select 1\n"
    patched, changed = inject_tags_into_config(sql, "x")
    assert changed is False
    assert patched == sql


def test_inject_tags_preserves_content_after_config():
    """Content after the config block must be preserved verbatim."""
    patched, changed = inject_tags_into_config(_MULTILINE_SQL, "c360")
    assert changed is True
    assert patched.endswith("\nselect 1\n")


# ---------------------------------------------------------------------------
# Unit tests — read_product_name
# ---------------------------------------------------------------------------


def test_read_product_name_from_fixture():
    pipeline_def = _PIPELINE_DIR / "dimensions" / "c360" / "dim_groups" / "pipeline_definition.json"
    assert read_product_name(pipeline_def) == "c360"


def test_read_product_name_missing_key(tmp_path: Path):
    p = tmp_path / "pipeline_definition.json"
    p.write_text(json.dumps({"table_name": "foo"}), encoding="utf-8")
    with pytest.raises(KeyError):
        read_product_name(p)


def test_read_product_name_invalid_json(tmp_path: Path):
    p = tmp_path / "pipeline_definition.json"
    p.write_text("not json", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        read_product_name(p)


# ---------------------------------------------------------------------------
# CLI integration tests — add-tags command
# ---------------------------------------------------------------------------


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _make_project(tmp_path: Path, table_name: str, relative_path: str, sql: str) -> tuple[Path, Path]:
    """Create a minimal dbt project + pipelines tree for CLI tests."""
    pipelines_dir = tmp_path / "pipelines"
    dbt_dir = tmp_path / "dbt"

    # Pipeline definition
    pd_path = pipelines_dir / relative_path / "pipeline_definition.json"
    pd_path.parent.mkdir(parents=True, exist_ok=True)
    pd_path.write_text(
        json.dumps({"table_name": table_name, "product_name": "c360"}), encoding="utf-8"
    )

    # SQL model
    sql_path = dbt_dir / "models" / relative_path / f"{table_name}.sql"
    sql_path.parent.mkdir(parents=True, exist_ok=True)
    sql_path.write_text(sql, encoding="utf-8")

    # tracking.yml
    _make_tracking_yml(dbt_dir, [{"table_name": table_name, "relative_path": relative_path}])

    return pipelines_dir, dbt_dir


def test_add_tags_dry_run(runner: CliRunner, tmp_path: Path):
    pipelines_dir, dbt_dir = _make_project(
        tmp_path, "sl_c360_dim_groups", "dimensions/c360/dim_groups", _MULTILINE_SQL
    )
    result = runner.invoke(
        app,
        ["add-tags", str(pipelines_dir), str(dbt_dir)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "would tag" in result.output
    assert "Run with --write" in result.output
    # File must NOT be modified in dry-run
    sql_path = dbt_dir / "models" / "dimensions/c360/dim_groups" / "sl_c360_dim_groups.sql"
    assert sql_path.read_text(encoding="utf-8") == _MULTILINE_SQL


def test_add_tags_write(runner: CliRunner, tmp_path: Path):
    pipelines_dir, dbt_dir = _make_project(
        tmp_path, "sl_c360_dim_groups", "dimensions/c360/dim_groups", _MULTILINE_SQL
    )
    result = runner.invoke(
        app,
        ["add-tags", str(pipelines_dir), str(dbt_dir), "--write"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "tagged with 'c360'" in result.output
    sql_path = dbt_dir / "models" / "dimensions/c360/dim_groups" / "sl_c360_dim_groups.sql"
    content = sql_path.read_text(encoding="utf-8")
    assert "tags=['c360']" in content


def test_add_tags_idempotent(runner: CliRunner, tmp_path: Path):
    pipelines_dir, dbt_dir = _make_project(
        tmp_path, "sl_c360_dim_groups", "dimensions/c360/dim_groups", _ALREADY_TAGGED_SQL
    )
    result = runner.invoke(
        app,
        ["add-tags", str(pipelines_dir), str(dbt_dir), "--write"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "already tagged" in result.output
    assert "0 patched" in result.output


def test_add_tags_missing_pipeline_def(runner: CliRunner, tmp_path: Path):
    """Tables with no pipeline_definition.json should be skipped with a warning."""
    pipelines_dir, dbt_dir = _make_project(
        tmp_path, "sl_c360_dim_groups", "dimensions/c360/dim_groups", _MULTILINE_SQL
    )
    # Remove the pipeline_definition.json
    (pipelines_dir / "dimensions" / "c360" / "dim_groups" / "pipeline_definition.json").unlink()

    result = runner.invoke(
        app,
        ["add-tags", str(pipelines_dir), str(dbt_dir), "--write"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "skipped" in result.output
    assert "1 skipped" in result.output


def test_add_tags_missing_tracking_yml(runner: CliRunner, tmp_path: Path):
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    result = runner.invoke(
        app,
        ["add-tags", str(pipelines_dir), str(dbt_dir)],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
