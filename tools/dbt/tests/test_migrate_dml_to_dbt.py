"""Tests for Flink DML → dbt migration."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from tools.dbt.flink_dbt_migrate.migrate_dml_to_dbt import app

# tests/ → tools/ → dbt/ → code/ → repo root, then into code/flink-sql
tests_path =  Path(__file__).resolve().parent # current tests folder

FLINK_SQL = Path(__file__).resolve().parent / "fixtures" / "flink-sql"
CART_UPDATE = FLINK_SQL / "11-puzzles/cart_update"
BASIC_PATH = FLINK_SQL / "00-basic-sql" / "cc-flink"
ROLLING = FLINK_SQL / "10-windowing/tumble_then_hop_rolling"
JOINS_CC_FLINK = FLINK_SQL / "04-joins/cc-flink"
JOINS_CC_DBT = FLINK_SQL / "04-joins/cc_dbt"
JOIN_04 = FLINK_SQL / "04-joins"


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


def test_cli_dry_run(cli_runner: CliRunner) -> None:
    dbt_project_path = tests_path / "dbt_out"
    target_path : Path = dbt_project_path / "models" / "hr" / "employees"
    result = cli_runner.invoke(
        app,
        ["migrate-one-file", str(BASIC_PATH / "dml.employee_count.sql"), str(target_path), "--dbt-project-dir", str(dbt_project_path)],
         catch_exceptions=False, 
    )
    print(result.stdout)
    assert result.exit_code == 0, f"CLI invocation failed (exit_code={result.exit_code}):\n{result.output}"
    assert "# --- model ---" in result.stdout
    assert "materialized='streaming_table'" in result.stdout
    assert "# --- schema.yml ---" in result.stdout
    assert "name: employee" in result.stdout


def test_cli_basic_sql_processing(cli_runner: CliRunner) -> None:
    dbt_project_path = tests_path / "dbt_out"
    target_path : Path = dbt_project_path / "models" / "crm" / "employees"
   
    result = cli_runner.invoke(
        app,
        ["migrate-one-file", str(BASIC_PATH / "dml.employee_count.sql"), str(target_path), "--dbt-project-dir", str(dbt_project_path), "--write", "--force"],
    )
    print(result.stdout)
    assert result.exit_code == 0, f"CLI invocation failed (exit_code={result.exit_code}):\n{result.output}"
    assert (target_path / "employee_count.sql").exists()
    assert (target_path / "schema.yml").exists()
    assert "Wrote" in result.stdout


def test_migrate_one_file_resolves_parent_pipeline_model_as_ref(tmp_path: Path, cli_runner: CliRunner) -> None:
    pipelines = tmp_path / "pipelines"
    # Create parent pipeline with dml
    parent_dir = pipelines / "dimensions/dim_users"
    parent_scripts = parent_dir / "sql-scripts"
    parent_scripts.mkdir(parents=True)
    (parent_scripts / "ddl.dim_users.sql").write_text("CREATE TABLE dim_users (id BIGINT, name STRING, PRIMARY KEY (id) NOT ENFORCED);")
    (parent_scripts / "dml.dim_users.sql").write_text("INSERT INTO dim_users SELECT id, name FROM src_users;")

    # Create child pipeline that references parent in pipeline_definition.json
    child_dir = pipelines / "facts/fct_orders"
    child_scripts = child_dir / "sql-scripts"
    child_scripts.mkdir(parents=True)
    (child_scripts / "ddl.fct_orders.sql").write_text("CREATE TABLE fct_orders (id BIGINT, user_id BIGINT, PRIMARY KEY (id) NOT ENFORCED);")
    (child_scripts / "dml.fct_orders.sql").write_text("INSERT INTO fct_orders SELECT id, user_id FROM dim_users;")

    import json
    pipeline_def = {
        "table_name": "fct_orders",
        "parents": [
            {
                "table_name": "dim_users",
                "ddl_ref": "pipelines/dimensions/dim_users/sql-scripts/ddl.dim_users.sql",
            }
        ],
    }
    (child_dir / "pipeline_definition.json").write_text(json.dumps(pipeline_def))

    dbt_project_path = tmp_path / "dbt_project"
    target_path = dbt_project_path / "models/facts/fct_orders"

    result = cli_runner.invoke(
        app,
        [
            "migrate-one-file",
            str(child_scripts / "dml.fct_orders.sql"),
            str(target_path),
            "--dbt-project-dir",
            str(dbt_project_path),
        ],
    )
    assert result.exit_code == 0, f"CLI error: {result.stdout}"
    assert "{{ ref('dim_users') }}" in result.stdout
    assert "{{ source(" not in result.stdout


def test_migrate_fct_user_per_group(cli_runner: CliRunner) -> None:
    sl_test_pipeline_path = tests_path / "fixtures" / "flink-project" / "pipelines"
    dbt_project_path = tests_path / "dbt_out"
    target_path : Path = dbt_project_path / "models" / "crm"
   
    result = cli_runner.invoke(
        app,
        ["migrate-one-file", str(sl_test_pipeline_path / "facts" / "c360" / "fct_user_per_group" / "sql-scripts" / "dml.c360_fct_user_per_group.sql"),
              str(target_path),
              "--dbt-project-dir", str(dbt_project_path),
              "--write", "--force"],
    )
    print(result.stdout)
    assert result.exit_code == 0, f"CLI invocation failed (exit_code={result.exit_code}):\n{result.output}"
    assert (target_path / "fct_user_per_group" / "schema.yml").exists()
    assert "Wrote" in result.stdout


def test_migrate_one_file_with_distributed_by_and_primary_key(tmp_path: Path, cli_runner: CliRunner) -> None:
    ddl_content = """\
CREATE TABLE customer_orders (
    customer_id INT NOT NULL,
    order_id BIGINT NOT NULL,
    total DOUBLE,
    PRIMARY KEY (customer_id, order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 2 BUCKETS;
"""
    dml_content = """\
INSERT INTO customer_orders
SELECT customer_id, order_id, total FROM raw_orders;
"""
    pipeline_dir = tmp_path / "pipelines" / "facts" / "customer_orders"
    sql_scripts = pipeline_dir / "sql-scripts"
    sql_scripts.mkdir(parents=True)
    (sql_scripts / "ddl.customer_orders.sql").write_text(ddl_content, encoding="utf-8")
    (sql_scripts / "dml.customer_orders.sql").write_text(dml_content, encoding="utf-8")

    dbt_project = tmp_path / "dbt_project"
    target_dir = dbt_project / "models" / "facts" / "customer_orders"

    result = cli_runner.invoke(
        app,
        [
            "migrate-one-file",
            str(sql_scripts / "dml.customer_orders.sql"),
            str(target_dir),
            "--dbt-project-dir",
            str(dbt_project),
            "--write",
        ],
    )
    assert result.exit_code == 0, f"CLI error: {result.output}"

    model_sql = (target_dir / "customer_orders.sql").read_text(encoding="utf-8")
    assert "distributed_by={" in model_sql
    assert "'columns': ['customer_id']," in model_sql
    assert "'buckets': 2" in model_sql

    schema_yml = (target_dir / "schema.yml").read_text(encoding="utf-8")
    assert "name: customer_id" in schema_yml
    assert "name: order_id" in schema_yml
    assert "type: not_null" in schema_yml
    assert "type: primary_key" in schema_yml
    assert 'expression: "not enforced"' in schema_yml or "expression: not enforced" in schema_yml


def test_migrate_one_file_with_watermark(tmp_path: Path, cli_runner: CliRunner) -> None:
    ddl_content = """\
CREATE TABLE tx_events (
    txn_id STRING NOT NULL,
    amount DECIMAL(10, 2),
    event_time TIMESTAMP(3),
    PRIMARY KEY (txn_id) NOT ENFORCED,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) DISTRIBUTED BY HASH(txn_id) INTO 4 BUCKETS;
"""
    dml_content = """\
INSERT INTO tx_events
SELECT txn_id, amount, event_time FROM raw_tx;
"""
    pipeline_dir = tmp_path / "pipelines" / "facts" / "tx_events"
    sql_scripts = pipeline_dir / "sql-scripts"
    sql_scripts.mkdir(parents=True)
    (sql_scripts / "ddl.tx_events.sql").write_text(ddl_content, encoding="utf-8")
    (sql_scripts / "dml.tx_events.sql").write_text(dml_content, encoding="utf-8")

    dbt_project = tmp_path / "dbt_project"
    target_dir = dbt_project / "models" / "facts" / "tx_events"

    result = cli_runner.invoke(
        app,
        [
            "migrate-one-file",
            str(sql_scripts / "dml.tx_events.sql"),
            str(target_dir),
            "--dbt-project-dir",
            str(dbt_project),
            "--write",
        ],
    )
    assert result.exit_code == 0, f"CLI error: {result.output}"

    schema_yml = (target_dir / "schema.yml").read_text(encoding="utf-8")
    assert "type: custom" in schema_yml
    assert "WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND" in schema_yml
