"""Scaffold Flink DDL + synthetic DML for tables referenced in pipelines but never declared.

Workflow
--------
1. ``find_undeclared_tables(pipelines_root)``
   Scans every ``sql-scripts/dml*.sql`` file, extracts upstream table references, and
   returns those that have no matching ``CREATE TABLE`` DDL anywhere in the tree.

2. ``infer_columns_for_table(table_name, dml_paths, ddl_index)``
   For each DML that references the undeclared table, parses the SELECT list and infers
   Flink column types using the existing type_inferrer.  Merges results across files.

3. ``generate_ddl_sql / generate_dml_sql / generate_sources_yaml``
   Pure-text generators that produce ready-to-use Flink SQL / YAML.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import yaml

from tools.dbt.flink_dbt_migrate.discover_deps import (
    build_pipelines_ddl_index,
    collect_upstream_tables,
    _SQL_KEYWORD_BLOCKLIST,
)
from tools.dbt.flink_dbt_migrate.flink_sql_processor import (
    _get_logger,
    collect_cte_names,
    is_values_insert,
    parse_ddl,
    strip_identifier,
)
from tools.dbt.sql_parser import parse_select
from tools.dbt.type_inferrer import build_catalog, infer_type

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class InferredColumn:
    """One column inferred from the DML that consumes an undeclared table."""
    name: str
    flink_type: str          # Flink type string, e.g. "VARCHAR", "BIGINT", "TIMESTAMP(3)"
    needs_review: bool = False  # True when type fell back to VARCHAR or could not be inferred


# ---------------------------------------------------------------------------
# 1. Scanner: find undeclared tables
# ---------------------------------------------------------------------------

_INSERT_INTO_TARGET_RE = re.compile(
    r"\bINSERT\s+INTO\s+(`?[\w]+`?)",
    re.IGNORECASE,
)


def _dml_target(sql: str) -> str | None:
    """Return the INSERT INTO target table name, or None if not found."""
    m = _INSERT_INTO_TARGET_RE.search(sql)
    if m:
        return strip_identifier(m.group(1))
    return None


def find_undeclared_tables(pipelines_root: Path) -> dict[str, list[Path]]:
    """Scan *pipelines_root* and return tables referenced in DML but never declared.

    Returns a mapping ``{table_name: [dml_path, ...]}`` where each entry lists the
    DML files that reference that undeclared table.

    Only ``sql-scripts/`` directories are scanned; ``tests/`` subtrees are skipped.
    CTE names are excluded.  The INSERT INTO target of each DML is excluded (it has
    its own DDL in the same pipeline folder).
    """
    log = _get_logger()
    pipelines_root = pipelines_root.resolve()

    # Build the full table → ddl_path index for this pipelines tree.
    ddl_index = build_pipelines_ddl_index(pipelines_root)
    log.debug("find_undeclared_tables | ddl_index size=%d", len(ddl_index))

    undeclared: dict[str, list[Path]] = {}

    for sql_scripts_dir in sorted(pipelines_root.rglob("sql-scripts")):
        if not sql_scripts_dir.is_dir():
            continue
        if "tests" in sql_scripts_dir.parts:
            continue

        for dml_file in sorted(sql_scripts_dir.glob("dml*.sql")):
            try:
                sql = dml_file.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue

            # Skip INSERT INTO ... VALUES seeds — they don't reference upstream tables.
            if is_values_insert(sql):
                continue

            target = _dml_target(sql)
            cte_names = collect_cte_names(sql)

            # collect_upstream_tables expects just the SELECT body, but works on
            # full SQL too (it strips comments and matches FROM/JOIN patterns).
            upstream = collect_upstream_tables(sql, cte_names)
            log.debug("  dml=%s  target=%s  upstream=%s", dml_file.name, target, upstream)

            for table in upstream:
                if table == target:
                    continue
                if table in ddl_index:
                    continue
                undeclared.setdefault(table, []).append(dml_file)

    return undeclared


# ---------------------------------------------------------------------------
# 2. Column inference
# ---------------------------------------------------------------------------

class _TableRef(NamedTuple):
    """An alias → real table mapping found in a SQL query."""
    alias: str       # the alias or the bare name used in the SELECT
    table: str       # canonical table name


_FROM_ALIAS_RE = re.compile(
    r"\bFROM\s+(`?[\w]+`?)\s+(?:AS\s+)?(`?[\w]+`?)",
    re.IGNORECASE,
)
_JOIN_ALIAS_RE = re.compile(
    r"\bJOIN\s+(`?[\w]+`?)\s+(?:AS\s+)?(`?[\w]+`?)",
    re.IGNORECASE,
)
_FROM_NO_ALIAS_RE = re.compile(
    r"\bFROM\s+(`?[\w]+`?)(?:\s*(?:WHERE|JOIN|GROUP|ORDER|HAVING|LIMIT|$))",
    re.IGNORECASE,
)


def _collect_table_aliases(sql: str, target_table: str) -> set[str]:
    """Return all aliases (or the bare name) used for *target_table* in *sql*."""
    aliases: set[str] = set()
    for pattern in (_FROM_ALIAS_RE, _JOIN_ALIAS_RE):
        for m in pattern.finditer(sql):
            tbl = strip_identifier(m.group(1))
            alias = strip_identifier(m.group(2))
            if tbl == target_table and alias.lower() not in _SQL_KEYWORD_BLOCKLIST:
                aliases.add(alias)
    # If the table appears with no alias
    for m in _FROM_NO_ALIAS_RE.finditer(sql):
        tbl = strip_identifier(m.group(1))
        if tbl == target_table:
            aliases.add(target_table)
    if not aliases:
        # Fallback: treat the bare table name itself as the alias
        aliases.add(target_table)
    return aliases


def _flink_type_to_select_type(flink_type: str) -> str:
    """Convert a Flink DDL type string to the lowercase type key used by infer_type."""
    ft = flink_type.upper().strip()
    if ft.startswith("VARCHAR") or ft.startswith("STRING") or ft.startswith("CHAR"):
        return "string"
    if ft.startswith("TIMESTAMP"):
        return "timestamp(3)"
    if ft == "DATE":
        return "date"
    if ft in ("BOOLEAN", "BOOL"):
        return "boolean"
    if ft in ("BIGINT",):
        return "bigint"
    if ft in ("INT", "INTEGER"):
        return "int"
    if ft in ("DOUBLE", "FLOAT"):
        return "double"
    return flink_type.lower()


def _build_catalog_from_ddl_index(
    table_aliases: dict[str, str],   # alias → table_name
    ddl_index: dict[str, Path],
) -> dict[str, dict[str, str]]:
    """Build a type catalog ``{alias: {col: type}}`` for known tables in the query."""
    catalog: dict[str, dict[str, str]] = {}
    for alias, table_name in table_aliases.items():
        if table_name not in ddl_index:
            continue
        try:
            ddl = parse_ddl(ddl_index[table_name].read_text(encoding="utf-8"))
        except Exception:
            continue
        catalog[alias] = {
            col.name: _flink_type_to_select_type(col.flink_type)
            for col in ddl.columns
        }
        # Also register by table name itself so unqualified refs resolve
        if alias != table_name:
            catalog[table_name] = catalog[alias]
    return catalog


def infer_columns_for_table(
    table_name: str,
    dml_paths: list[Path],
    ddl_index: dict[str, Path],
) -> list[InferredColumn]:
    """Infer columns for *table_name* by analysing all DML files that reference it.

    Returns a deduplicated, ordered list of :class:`InferredColumn`.  When the
    same column appears in multiple files with conflicting types, the most specific
    non-VARCHAR type wins.
    """
    log = _get_logger()
    # col_name → (flink_type, needs_review)
    merged: dict[str, tuple[str, bool]] = {}

    for dml_path in dml_paths:
        try:
            sql = dml_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        # Strip the INSERT INTO … header to get just the SELECT body for parsing.
        _insert_re = re.compile(
            r"\bINSERT\s+INTO\s+`?[\w]+`?\s*(?:\([^)]*\))?\s*",
            re.IGNORECASE,
        )
        select_body = _insert_re.sub("", sql, count=1).strip()

        # Detect SELECT * from the undeclared table → emit placeholder column.
        if re.search(r"\bSELECT\s+\*", select_body, re.IGNORECASE):
            log.debug("infer_columns_for_table | SELECT * detected in %s", dml_path.name)
            merged.setdefault("_todo", ("VARCHAR", True))
            continue

        # Map every alias in this query to its real table name.
        all_alias_map: dict[str, str] = {}
        for pat in (_FROM_ALIAS_RE, _JOIN_ALIAS_RE):
            for m in pat.finditer(select_body):
                tbl = strip_identifier(m.group(1))
                alias = strip_identifier(m.group(2))
                if alias.lower() not in _SQL_KEYWORD_BLOCKLIST:
                    all_alias_map[alias] = tbl
                all_alias_map[tbl] = tbl  # bare name always maps to itself

        target_aliases = _collect_table_aliases(select_body, table_name)
        catalog = _build_catalog_from_ddl_index(all_alias_map, ddl_index)

        try:
            items = parse_select(select_body)
        except Exception:
            log.warning("infer_columns_for_table | parse_select failed for %s", dml_path)
            continue

        for item in items:
            # Include column if it is qualified with a target alias, or unqualified.
            if item.table_alias is not None and item.table_alias not in target_aliases:
                continue

            col_name = item.output_name
            if not col_name or col_name == "*":
                merged.setdefault("_todo", ("VARCHAR", True))
                continue

            inferred = infer_type(item, catalog)
            # Map type_inferrer's "string" back to Flink "VARCHAR"
            flink_type = "VARCHAR" if inferred == "string" else inferred.upper()
            needs_review = inferred == "string"

            existing = merged.get(col_name)
            if existing is None:
                merged[col_name] = (flink_type, needs_review)
            else:
                # Prefer a non-VARCHAR (more specific) type when there is a conflict.
                if existing[1] and not needs_review:
                    merged[col_name] = (flink_type, needs_review)

    if not merged:
        # Nothing could be inferred — emit a single placeholder.
        merged["_todo"] = ("VARCHAR", True)

    return [
        InferredColumn(name=name, flink_type=ftype, needs_review=review)
        for name, (ftype, review) in merged.items()
    ]


# ---------------------------------------------------------------------------
# 3. File generators
# ---------------------------------------------------------------------------

_DDL_TMPL = """\
-- DDL: raw table '{table_name}' — scaffolded by flink-sql-migrate-dbt scaffold-missing-raws.
-- This table was referenced in DML pipelines but had no CREATE TABLE declaration.
-- Review column types marked with TODO and update as needed, then run:
--   uv run dbt run --select raws.{table_name}
CREATE TABLE IF NOT EXISTS {table_name} (
{column_defs}
) WITH (
    'connector'            = 'confluent',
    'changelog.mode'       = 'append',
    'kafka.topic'          = '{table_name}',
    'scan.startup.mode'    = 'earliest-offset',
    'value.format'         = 'avro-registry',
    'kafka.cleanup-policy' = 'delete'
);
"""

_DML_TMPL = """\
-- DML: synthetic rows for '{table_name}' — for local / CI testing.
-- Run with:  uv run dbt run --select raws.{table_name}
INSERT INTO {table_name} ({col_list})
VALUES
{value_rows};
"""


def _placeholder_for(flink_type: str) -> str:
    """Return a SQL literal placeholder matching *flink_type*."""
    ft = flink_type.upper().strip()
    if ft.startswith("TIMESTAMP"):
        return "TIMESTAMP '2024-01-01 00:00:00'"
    if ft == "DATE":
        return "DATE '2024-01-01'"
    if ft in ("BOOLEAN", "BOOL"):
        return "true"
    if ft in ("BIGINT", "INT", "INTEGER", "DOUBLE", "FLOAT", "DECIMAL"):
        return "0"
    if ft.startswith("DECIMAL"):
        return "0"
    # VARCHAR / STRING / anything else
    return "'synth-001'"


def generate_ddl_sql(table_name: str, columns: list[InferredColumn]) -> str:
    """Return a ``CREATE TABLE`` SQL string for *table_name* with *columns*."""
    col_lines = []
    for col in columns:
        suffix = "  -- TODO: verify type" if col.needs_review else ""
        col_lines.append(f"    {col.name:<30} {col.flink_type}{suffix}")
    column_defs = ",\n".join(col_lines)
    return _DDL_TMPL.format(table_name=table_name, column_defs=column_defs)


def generate_dml_sql(table_name: str, columns: list[InferredColumn]) -> str:
    """Return an ``INSERT INTO … VALUES`` SQL string for *table_name*."""
    col_list = ", ".join(c.name for c in columns)
    placeholders = ", ".join(_placeholder_for(c.flink_type) for c in columns)
    row1 = f"    ({placeholders})"
    # Second row: use 'synth-002' for the first varchar column if any
    row2_vals = []
    first_varchar_done = False
    for col in columns:
        ph = _placeholder_for(col.flink_type)
        ft = col.flink_type.upper()
        if not first_varchar_done and (ft.startswith("VARCHAR") or ft.startswith("STRING")):
            ph = "'synth-002'"
            first_varchar_done = True
        row2_vals.append(ph)
    row2 = f"    ({', '.join(row2_vals)})"
    value_rows = f"{row1},\n{row2}"
    return _DML_TMPL.format(table_name=table_name, col_list=col_list, value_rows=value_rows)


def generate_sources_yaml(
    profile_name: str,
    table_entries: list[tuple[str, list[InferredColumn]]],
    existing_path: Path | None = None,
) -> str:
    """Return a ``sources.yaml`` YAML string registering all *table_entries*.

    Merges idempotently with any existing file at *existing_path*.
    """
    if existing_path is not None and existing_path.exists():
        data: dict = yaml.safe_load(existing_path.read_text(encoding="utf-8")) or {}
    else:
        data = {}

    data["version"] = 2
    sources: list = data.setdefault("sources", [])

    source_block = next((s for s in sources if s.get("name") == profile_name), None)
    if source_block is None:
        source_block = {"name": profile_name, "tables": []}
        sources.append(source_block)

    tables: list = source_block.setdefault("tables", [])
    existing_names = {t.get("name") for t in tables}

    for table_name, columns in table_entries:
        if table_name in existing_names:
            continue
        tables.append({
            "name": table_name,
            "identifier": table_name,
            "description": (
                f"Raw table '{table_name}' — scaffolded by scaffold-missing-raws."
            ),
            "columns": [
                {"name": col.name, "data_type": col.flink_type.lower()}
                for col in columns
            ],
        })

    return yaml.dump(data, sort_keys=False, allow_unicode=True)
