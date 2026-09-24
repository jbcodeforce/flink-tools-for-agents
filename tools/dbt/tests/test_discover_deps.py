"""Tests for upstream dependency discovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.dbt.flink_dbt_migrate.discover_deps import (
    collect_upstream_tables,
    default_source_name,
    discover_upstream_ddl,
    find_dbt_model,
    resolve_upstream_deps,
)
from tools.dbt.flink_dbt_migrate.flink_sql_processor import parse_dml, collect_cte_names

FLINK_SQL = Path(__file__).resolve().parent / "fixtures" / "flink-sql"
JOINS_CC_FLINK = FLINK_SQL / "04-joins/cc-flink"
JOINS_CC_DBT = FLINK_SQL / "04-joins/cc_dbt"


def test_collect_upstream_tables_enriched_orders() -> None:
    sql = (JOINS_CC_FLINK / "dml.enriched_orders.sql").read_text(encoding="utf-8")
    dml = parse_dml(sql)
    cte_names = collect_cte_names(dml.body)
    tables = collect_upstream_tables(dml.body, cte_names)
    assert tables == ["d04_orders", "d04_products"]


def test_discover_upstream_ddl_prefers_non_wm_variant() -> None:
    ddl_path = discover_upstream_ddl(JOINS_CC_FLINK, "d04_orders")
    assert ddl_path.name == "ddl.orders.sql"


def test_discover_upstream_ddl_products() -> None:
    ddl_path = discover_upstream_ddl(JOINS_CC_FLINK, "d04_products")
    assert ddl_path.name == "ddl.products.sql"


def test_find_dbt_model_missing() -> None:
    assert find_dbt_model(JOINS_CC_DBT, "d04_orders") is None


def test_default_source_name() -> None:
    assert default_source_name(JOINS_CC_FLINK) == "cc_flink"


def test_resolve_upstream_deps_classifies_sources() -> None:
    sql = (JOINS_CC_FLINK / "dml.enriched_orders.sql").read_text(encoding="utf-8")
    dml = parse_dml(sql)
    deps = resolve_upstream_deps(
        JOINS_CC_FLINK,
        JOINS_CC_DBT,
        dml,
        source_name="cc_flink",
    )
    assert [dep.table_name for dep in deps] == ["d04_orders", "d04_products"]
    assert all(dep.resolution == "source" for dep in deps)
    assert all(dep.source_name == "cc_flink" for dep in deps)
    assert all(dep.ddl_path is not None for dep in deps)


def test_resolve_upstream_deps_ref_override() -> None:
    sql = (JOINS_CC_FLINK / "dml.enriched_orders.sql").read_text(encoding="utf-8")
    dml = parse_dml(sql)
    deps = resolve_upstream_deps(
        JOINS_CC_FLINK,
        JOINS_CC_DBT,
        dml,
        ref_overrides={"d04_orders": "orders_model"},
    )
    orders = deps[0]
    assert orders.resolution == "ref"
    assert orders.ref_model == "orders_model"


def test_resolve_upstream_deps_no_sources_uses_ref() -> None:
    sql = (JOINS_CC_FLINK / "dml.enriched_orders.sql").read_text(encoding="utf-8")
    dml = parse_dml(sql)
    deps = resolve_upstream_deps(
        JOINS_CC_FLINK,
        JOINS_CC_DBT,
        dml,
        resolve_sources=False,
    )
    assert all(dep.resolution == "ref" for dep in deps)


def test_discover_upstream_ddl_missing_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="No DDL file found"):
        discover_upstream_ddl(tmp_path, "missing_table")


def test_resolve_upstream_deps_with_known_models() -> None:
    sql = (JOINS_CC_FLINK / "dml.enriched_orders.sql").read_text(encoding="utf-8")
    dml = parse_dml(sql)
    deps = resolve_upstream_deps(
        JOINS_CC_FLINK,
        JOINS_CC_DBT,
        dml,
        source_name="cc_flink",
        known_models={"d04_orders"},
    )
    assert [dep.table_name for dep in deps] == ["d04_orders", "d04_products"]
    # d04_orders is known model -> ref
    assert deps[0].resolution == "ref"
    assert deps[0].ref_model == "d04_orders"
    # d04_products is not known -> source
    assert deps[1].resolution == "source"
    assert deps[1].source_name == "cc_flink"


def test_resolve_upstream_deps_missing_ddl_skipped(tmp_path: Path) -> None:
    """An upstream table whose DDL cannot be found is skipped with a warning.

    The model and schema.yml must still be generated — only the unresolvable
    source entry is omitted from the returned dep list.
    """
    sql_scripts = tmp_path / "sql-scripts"
    sql_scripts.mkdir()
    # known_table has a DDL; unknown_table does not
    (sql_scripts / "ddl.known_table.sql").write_text(
        "CREATE TABLE known_table (id INT) WITH ('connector' = 'kafka');",
        encoding="utf-8",
    )

    dml_sql = (
        "INSERT INTO my_model\n"
        "SELECT a.id FROM known_table a JOIN unknown_table b ON a.id = b.id"
    )
    dml = parse_dml(dml_sql)

    deps = resolve_upstream_deps(
        sql_scripts,
        None,
        dml,
        source_name="my_source",
    )

    dep_names = [d.table_name for d in deps]
    # known_table resolved normally
    assert "known_table" in dep_names
    # unknown_table silently skipped — no exception raised
    assert "unknown_table" not in dep_names




def test_resolve_upstream_deps_cte_no_space_before_paren(tmp_path: Path) -> None:
    """CTE defined with no space between AS and ( must not be treated as an upstream table.

    Flink SQL (and common formatting) allows ``name as(`` without a space before the
    opening parenthesis.  collect_cte_names previously required at least one space,
    so it missed the CTE name and incorrectly passed it to the DDL lookup —
    causing a FileNotFoundError.
    """
    sql_scripts = tmp_path / "sql-scripts"
    sql_scripts.mkdir()
    (sql_scripts / "ddl.src_raw.sql").write_text(
        "CREATE TABLE src_raw (id INT) WITH ('connector' = 'kafka');",
        encoding="utf-8",
    )

    # Mirrors the real pattern: INSERT INTO ... WITH cte as( ... ) SELECT * FROM cte
    dml_sql = (
        "INSERT INTO stage_training_assignment\n"
        "WITH\n"
        "training_assignment as(\n"
        "    SELECT id FROM src_raw\n"
        "),\n"
        "final as (SELECT id FROM training_assignment)\n"
        "SELECT * FROM final"
    )
    dml = parse_dml(dml_sql)

    deps = resolve_upstream_deps(
        sql_scripts,
        None,
        dml,
        source_name="my_source",
    )

    dep_names = [d.table_name for d in deps]
    # Neither CTE name should appear as an upstream dep
    assert "training_assignment" not in dep_names
    assert "final" not in dep_names
    # The real upstream table inside the CTE body should be resolved
    assert "src_raw" in dep_names
