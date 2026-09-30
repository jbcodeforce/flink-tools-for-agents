"""Helpers for injecting key constraints into migrated dbt schema.yml files based on distributed_by config."""

from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple

import yaml

from tools.dbt.flink_dbt_migrate.dbt_element_mgr import (
    default_key_constraints,
    dump_schema_yml,
    load_schema_yml,
    parse_distributed_by_keys,
)
from tools.dbt.flink_dbt_migrate.flink_sql_processor import _get_logger

# Matches distributed_by={'columns': ['a', 'b'], ...} inside {{ config(...) }}
_DISTRIBUTED_BY_DICT_RE = re.compile(
    r"\bdistributed_by\s*=\s*\{[^}]*'columns'\s*:\s*\[([^\]]+)\]",
    re.IGNORECASE,
)
# Legacy: matches distributed_by='...' or distributed_by = "..." (old string form)
_DISTRIBUTED_BY_STR_RE = re.compile(
    r"\bdistributed_by\s*=\s*['\"]([^'\"]+)['\"]",
    re.IGNORECASE,
)


def extract_distributed_by(sql_text: str) -> list[str]:
    """Extract distributed_by column list from SQL model text."""
    match = _DISTRIBUTED_BY_DICT_RE.search(sql_text)
    if match:
        # Parse individual quoted column names from the list literal
        return [
            col.strip().strip("'\"")
            for col in match.group(1).split(",")
            if col.strip().strip("'\"")
        ]
    match = _DISTRIBUTED_BY_STR_RE.search(sql_text)
    if match:
        return parse_distributed_by_keys(match.group(1))
    return []


class ModelConstraintUpdate(NamedTuple):
    schema_path: Path
    model_name: str
    updated_columns: list[str]
    skipped_columns: list[str]


def update_model_schema_constraints(
    schema_path: Path,
    model_name: str,
    key_columns: list[str],
    *,
    write: bool = False,
) -> ModelConstraintUpdate:
    """Update schema.yml for a given model to add constraints to key_columns.

    Returns a ModelConstraintUpdate tracking which columns were updated or skipped.
    """
    if not key_columns or not schema_path.exists():
        return ModelConstraintUpdate(
            schema_path=schema_path,
            model_name=model_name,
            updated_columns=[],
            skipped_columns=[],
        )

    data = load_schema_yml(schema_path)
    models = data.get("models", [])

    # Find matching model entry (either exact match or if single model in schema.yml)
    target_entry = None
    for entry in models:
        if entry.get("name") == model_name:
            target_entry = entry
            break
    if target_entry is None and len(models) == 1:
        target_entry = models[0]

    if target_entry is None:
        return ModelConstraintUpdate(
            schema_path=schema_path,
            model_name=model_name,
            updated_columns=[],
            skipped_columns=[],
        )

    columns = target_entry.setdefault("columns", [])
    col_by_name = {c.get("name"): c for c in columns if isinstance(c, dict) and "name" in c}

    updated_cols: list[str] = []
    skipped_cols: list[str] = []

    for key in key_columns:
        if key in col_by_name:
            col = col_by_name[key]
            existing_constraints = col.get("constraints", [])
            # Check if primary_key constraint is already present
            has_pk = any(
                isinstance(c, dict) and c.get("type") == "primary_key"
                for c in existing_constraints
            )
            if not has_pk:
                col["constraints"] = default_key_constraints()
                updated_cols.append(key)
            else:
                skipped_cols.append(key)
        else:
            # Column listed in distributed_by is not yet in schema.yml columns list:
            # add it with default type and constraints
            new_col = {
                "name": key,
                "data_type": "VARCHAR",
                "constraints": default_key_constraints(),
            }
            columns.append(new_col)
            updated_cols.append(key)

    if updated_cols and write:
        schema_path.write_text(dump_schema_yml(data), encoding="utf-8")

    return ModelConstraintUpdate(
        schema_path=schema_path,
        model_name=model_name,
        updated_columns=updated_cols,
        skipped_columns=skipped_cols,
    )


def scan_and_update_models_constraints(
    models_dir: Path,
    *,
    write: bool = False,
) -> list[ModelConstraintUpdate]:
    """Walk models_dir, inspect all .sql files with distributed_by, and update sibling schema.yml."""
    results: list[ModelConstraintUpdate] = []
    if not models_dir.exists():
        return results

    # Find all .sql files under models_dir
    for sql_path in sorted(models_dir.rglob("*.sql")):
        sql_text = sql_path.read_text(encoding="utf-8")
        keys = extract_distributed_by(sql_text)
        if not keys:
            continue

        model_name = sql_path.stem
        parent_dir = sql_path.parent
        schema_path = parent_dir / "schema.yml"
        if not schema_path.exists() and (parent_dir / "schema.yaml").exists():
            schema_path = parent_dir / "schema.yaml"

        if not schema_path.exists():
            continue

        update_info = update_model_schema_constraints(
            schema_path=schema_path,
            model_name=model_name,
            key_columns=keys,
            write=write,
        )
        if update_info.updated_columns:
            results.append(update_info)

    return results
