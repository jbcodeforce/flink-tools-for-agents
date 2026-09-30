"""Tests for add_constraints helper functions."""

from __future__ import annotations

from pathlib import Path
import yaml

from tools.dbt.flink_dbt_migrate.add_constraints import (
    extract_distributed_by,
    update_model_schema_constraints,
    scan_and_update_models_constraints,
)


def test_extract_distributed_by_single_col() -> None:
    sql = """
{{ config(
    materialized='streaming_table',
    distributed_by={
        'columns': ['order_id'],
        'buckets': 4
    }
) }}
SELECT 1;
"""
    assert extract_distributed_by(sql) == ["order_id"]


def test_extract_distributed_by_multiple_cols() -> None:
    sql = """
{{ config(
    materialized='streaming_table',
    distributed_by={
        'columns': ['tenant_id', 'record_id', 'node_id'],
        'buckets': 4
    }
) }}
SELECT 1;
"""
    assert extract_distributed_by(sql) == ["tenant_id", "record_id", "node_id"]


def test_extract_distributed_by_legacy_string_form() -> None:
    sql = """
{{ config(
    materialized='streaming_table',
    distributed_by='`tenant_id`, `record_id`, node_id'
) }}
SELECT 1;
"""
    assert extract_distributed_by(sql) == ["tenant_id", "record_id", "node_id"]


def test_extract_distributed_by_none() -> None:
    sql = """
{{ config(
    materialized='streaming_table'
) }}
SELECT 1;
"""
    assert extract_distributed_by(sql) == []


def test_update_model_schema_constraints_dry_run_vs_write(tmp_path: Path) -> None:
    schema_path = tmp_path / "schema.yml"
    schema_content = {
        "version": 2,
        "models": [
            {
                "name": "my_model",
                "columns": [
                    {"name": "id", "data_type": "VARCHAR"},
                    {"name": "val", "data_type": "INT"},
                ],
            }
        ],
    }
    schema_path.write_text(yaml.safe_dump(schema_content), encoding="utf-8")

    # Dry-run
    res_dry = update_model_schema_constraints(
        schema_path,
        "my_model",
        ["id"],
        write=False,
    )
    assert res_dry.updated_columns == ["id"]
    # File not changed yet
    data = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    assert "constraints" not in data["models"][0]["columns"][0]

    # Write
    res_write = update_model_schema_constraints(
        schema_path,
        "my_model",
        ["id"],
        write=True,
    )
    assert res_write.updated_columns == ["id"]
    data_written = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    col = data_written["models"][0]["columns"][0]
    assert "constraints" in col
    assert col["constraints"] == [
        {"type": "not_null"},
        {"type": "primary_key", "expression": "not enforced"},
    ]


def test_update_model_schema_constraints_idempotent(tmp_path: Path) -> None:
    schema_path = tmp_path / "schema.yml"
    schema_content = {
        "version": 2,
        "models": [
            {
                "name": "my_model",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "VARCHAR",
                        "constraints": [
                            {"type": "not_null"},
                            {"type": "primary_key", "expression": "not enforced"},
                        ],
                    },
                ],
            }
        ],
    }
    schema_path.write_text(yaml.safe_dump(schema_content), encoding="utf-8")

    res = update_model_schema_constraints(
        schema_path,
        "my_model",
        ["id"],
        write=True,
    )
    assert res.updated_columns == []
    assert res.skipped_columns == ["id"]


def test_scan_and_update_models_constraints(tmp_path: Path) -> None:
    model_dir = tmp_path / "models" / "orders"
    model_dir.mkdir(parents=True)
    sql_path = model_dir / "orders.sql"
    sql_path.write_text(
        "{{ config(distributed_by={'columns': ['order_id'], 'buckets': 4}) }}\nSELECT 1;",
        encoding="utf-8",
    )
    schema_path = model_dir / "schema.yml"
    schema_path.write_text(
        yaml.safe_dump({
            "version": 2,
            "models": [
                {
                    "name": "orders",
                    "columns": [{"name": "order_id", "data_type": "VARCHAR"}],
                }
            ],
        }),
        encoding="utf-8",
    )

    updates = scan_and_update_models_constraints(tmp_path / "models", write=True)
    assert len(updates) == 1
    assert updates[0].updated_columns == ["order_id"]

    data = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    col = data["models"][0]["columns"][0]
    assert "constraints" in col
