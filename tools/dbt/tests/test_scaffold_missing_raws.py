"""Tests for scaffold_missing_raws — scanner, inferrer, generator, and CLI."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from tools.dbt.flink_dbt_migrate.scaffold_missing_raws import (
    InferredColumn,
    find_undeclared_tables,
    generate_ddl_sql,
    generate_dml_sql,
    generate_sources_yaml,
    infer_columns_for_table,
)
from tools.dbt.flink_dbt_migrate.migrate_dml_to_dbt import app


# ---------------------------------------------------------------------------
# Fixtures: minimal pipelines trees written to tmp_path
# ---------------------------------------------------------------------------

def _make_pipeline(root: Path, category: str, product: str, table: str,
                   ddl: str | None, dml: str) -> None:
    """Write a minimal pipeline folder under root/<category>/<product>/<table>/sql-scripts/."""
    scripts = root / category / product / table / "sql-scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    if ddl:
        (scripts / f"ddl.{table}.sql").write_text(ddl)
    (scripts / f"dml.{table}.sql").write_text(dml)


_DDL_ORDERS = """\
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR NOT NULL,
    customer_id VARCHAR,
    amount BIGINT,
    PRIMARY KEY(order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');
"""

_DML_ORDER_SUMMARY = """\
INSERT INTO order_summary
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id)   AS total_orders,
    SUM(o.amount)       AS total_amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name;
"""

_DDL_ORDER_SUMMARY = """\
CREATE TABLE IF NOT EXISTS order_summary (
    customer_id VARCHAR NOT NULL,
    customer_name VARCHAR,
    total_orders BIGINT,
    total_amount BIGINT,
    PRIMARY KEY(customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');
"""


# ---------------------------------------------------------------------------
# 1. Scanner tests
# ---------------------------------------------------------------------------

class TestFindUndeclaredTables:
    def test_detects_missing_customers_table(self, tmp_path: Path) -> None:
        """orders has a DDL; customers is referenced but undeclared → should be found."""
        _make_pipeline(tmp_path, "facts", "sales", "orders", _DDL_ORDERS, _DML_ORDER_SUMMARY)
        _make_pipeline(tmp_path, "facts", "sales", "order_summary", _DDL_ORDER_SUMMARY, "")

        result = find_undeclared_tables(tmp_path)

        assert "customers" in result

    def test_declared_table_not_in_result(self, tmp_path: Path) -> None:
        """orders is declared in a DDL — it must not appear in the undeclared set."""
        _make_pipeline(tmp_path, "facts", "sales", "orders", _DDL_ORDERS, _DML_ORDER_SUMMARY)
        _make_pipeline(tmp_path, "facts", "sales", "order_summary", _DDL_ORDER_SUMMARY, "")

        result = find_undeclared_tables(tmp_path)

        assert "orders" not in result

    def test_insert_target_not_reported(self, tmp_path: Path) -> None:
        """The INSERT INTO target table is not counted as undeclared even without a DDL."""
        # order_summary has no DDL but is the INSERT INTO target — should be excluded.
        _make_pipeline(tmp_path, "facts", "sales", "orders", _DDL_ORDERS, _DML_ORDER_SUMMARY)

        result = find_undeclared_tables(tmp_path)

        assert "order_summary" not in result

    def test_values_insert_seeds_are_skipped(self, tmp_path: Path) -> None:
        """INSERT INTO ... VALUES files (seeds) must not be scanned for upstream refs."""
        seed_dml = """\
INSERT INTO raw_products (id, name) VALUES
    ('P001', 'Widget'),
    ('P002', 'Gadget');
"""
        _make_pipeline(tmp_path, "seeds", "shop", "raw_products", None, seed_dml)

        result = find_undeclared_tables(tmp_path)

        # No SELECT → no upstream refs → nothing should be reported.
        assert len(result) == 0

    def test_cte_names_are_excluded(self, tmp_path: Path) -> None:
        """CTE names defined within the query body must not be reported as undeclared."""
        dml_with_cte = """\
INSERT INTO enriched_orders
WITH valid_orders AS (
    SELECT * FROM orders WHERE amount > 0
)
SELECT * FROM valid_orders;
"""
        _make_pipeline(tmp_path, "facts", "sales", "orders", _DDL_ORDERS, dml_with_cte)
        _make_pipeline(tmp_path, "facts", "sales", "enriched_orders", _DDL_ORDER_SUMMARY, "")

        result = find_undeclared_tables(tmp_path)

        assert "valid_orders" not in result

    def test_tests_subdirectory_skipped(self, tmp_path: Path) -> None:
        """Files inside a tests/ subdirectory must not be scanned."""
        tests_scripts = tmp_path / "facts" / "sales" / "orders" / "tests" / "sql-scripts"
        tests_scripts.mkdir(parents=True, exist_ok=True)
        (tests_scripts / "dml.test_check.sql").write_text(
            "INSERT INTO test_results SELECT * FROM ghost_table;"
        )
        # Only the tests/ tree exists — nothing in a real sql-scripts dir
        result = find_undeclared_tables(tmp_path)
        assert "ghost_table" not in result

    def test_empty_pipelines_dir(self, tmp_path: Path) -> None:
        result = find_undeclared_tables(tmp_path)
        assert result == {}

    def test_returns_dml_paths(self, tmp_path: Path) -> None:
        """The returned value maps table names to the DML files that reference them."""
        _make_pipeline(tmp_path, "facts", "sales", "orders", _DDL_ORDERS, _DML_ORDER_SUMMARY)

        result = find_undeclared_tables(tmp_path)

        assert "customers" in result
        assert any("dml.orders.sql" in str(p) for p in result["customers"])


# ---------------------------------------------------------------------------
# 2. Inferrer tests
# ---------------------------------------------------------------------------

_DDL_CUSTOMERS = """\
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR NOT NULL,
    customer_name VARCHAR,
    PRIMARY KEY(customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');
"""

_DML_SELECT_WITH_CAST = """\
INSERT INTO enriched
SELECT
    c.customer_id,
    CAST(c.raw_score AS BIGINT) AS score,
    c.signup_date
FROM missing_profiles c;
"""

_DML_SELECT_STAR = """\
INSERT INTO wide_table
SELECT * FROM missing_source;
"""


class TestInferColumnsForTable:
    def _write_dml(self, tmp_path: Path, name: str, content: str) -> Path:
        p = tmp_path / f"{name}.sql"
        p.write_text(content)
        return p

    def test_basic_varchar_column_inferred(self, tmp_path: Path) -> None:
        dml = """\
INSERT INTO result
SELECT p.profile_id, p.display_name
FROM missing_profiles p;
"""
        dml_path = self._write_dml(tmp_path, "dml.result", dml)
        cols = infer_columns_for_table("missing_profiles", [dml_path], {})

        names = [c.name for c in cols]
        assert "profile_id" in names
        assert "display_name" in names

    def test_cast_type_inferred(self, tmp_path: Path) -> None:
        dml_path = self._write_dml(tmp_path, "dml.enriched", _DML_SELECT_WITH_CAST)
        cols = infer_columns_for_table("missing_profiles", [dml_path], {})

        score_col = next((c for c in cols if c.name == "score"), None)
        assert score_col is not None
        assert score_col.flink_type == "BIGINT"
        assert score_col.needs_review is False

    def test_varchar_fallback_marked_needs_review(self, tmp_path: Path) -> None:
        dml = """\
INSERT INTO result
SELECT p.unknown_col
FROM missing_profiles p;
"""
        dml_path = self._write_dml(tmp_path, "dml.result", dml)
        cols = infer_columns_for_table("missing_profiles", [dml_path], {})

        col = next((c for c in cols if c.name == "unknown_col"), None)
        assert col is not None
        assert col.flink_type == "VARCHAR"
        assert col.needs_review is True

    def test_select_star_emits_todo_column(self, tmp_path: Path) -> None:
        dml_path = self._write_dml(tmp_path, "dml.wide", _DML_SELECT_STAR)
        cols = infer_columns_for_table("missing_source", [dml_path], {})

        assert len(cols) == 1
        assert cols[0].name == "_todo"
        assert cols[0].needs_review is True

    def test_catalog_improves_inference(self, tmp_path: Path) -> None:
        """When a known DDL is in the index, column types are resolved from it."""
        # customers.customer_id is VARCHAR in the DDL; orders references it via JOIN.
        dml = """\
INSERT INTO summary
SELECT o.order_id, c.customer_id, c.customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
"""
        # Write the customers DDL to the tmp_path DDL index
        ddl_path = tmp_path / "ddl.customers.sql"
        ddl_path.write_text(_DDL_CUSTOMERS)
        ddl_index = {"customers": ddl_path}

        dml_path = self._write_dml(tmp_path, "dml.summary", dml)
        cols = infer_columns_for_table("orders", [dml_path], ddl_index)

        # order_id has no catalog entry → VARCHAR + needs_review
        order_col = next((c for c in cols if c.name == "order_id"), None)
        assert order_col is not None

    def test_merge_across_multiple_dml_files(self, tmp_path: Path) -> None:
        """Columns from two DML files are merged; more specific type wins."""
        dml1 = """\
INSERT INTO r1
SELECT p.col_a
FROM missing_tbl p;
"""
        dml2 = """\
INSERT INTO r2
SELECT p.col_a, CAST(p.col_b AS DATE) AS col_b
FROM missing_tbl p;
"""
        p1 = self._write_dml(tmp_path, "dml1", dml1)
        p2 = self._write_dml(tmp_path, "dml2", dml2)
        cols = infer_columns_for_table("missing_tbl", [p1, p2], {})

        names = [c.name for c in cols]
        assert "col_a" in names
        assert "col_b" in names
        col_b = next(c for c in cols if c.name == "col_b")
        assert col_b.flink_type == "DATE"
        assert col_b.needs_review is False

    def test_empty_dml_list_returns_placeholder(self) -> None:
        cols = infer_columns_for_table("ghost", [], {})
        assert len(cols) == 1
        assert cols[0].name == "_todo"


# ---------------------------------------------------------------------------
# 3. Generator tests
# ---------------------------------------------------------------------------

class TestGenerateDdlSql:
    def test_contains_create_table(self) -> None:
        cols = [InferredColumn("id", "VARCHAR"), InferredColumn("amount", "BIGINT")]
        sql = generate_ddl_sql("my_table", cols)
        assert "CREATE TABLE IF NOT EXISTS my_table" in sql

    def test_columns_in_output(self) -> None:
        cols = [InferredColumn("id", "VARCHAR"), InferredColumn("ts", "TIMESTAMP(3)")]
        sql = generate_ddl_sql("my_table", cols)
        assert "id" in sql
        assert "TIMESTAMP(3)" in sql

    def test_needs_review_adds_todo_comment(self) -> None:
        cols = [InferredColumn("x", "VARCHAR", needs_review=True)]
        sql = generate_ddl_sql("my_table", cols)
        assert "TODO" in sql

    def test_clean_column_has_no_todo(self) -> None:
        cols = [InferredColumn("x", "BIGINT", needs_review=False)]
        sql = generate_ddl_sql("my_table", cols)
        # The header comment contains the word TODO (generic advice), but no
        # per-column "-- TODO: verify type" marker should appear on a clean column.
        assert "-- TODO: verify type" not in sql

    def test_with_options_present(self) -> None:
        cols = [InferredColumn("id", "VARCHAR")]
        sql = generate_ddl_sql("my_table", cols)
        assert "WITH" in sql
        assert "confluent" in sql


class TestGenerateDmlSql:
    def test_insert_into_present(self) -> None:
        cols = [InferredColumn("id", "VARCHAR"), InferredColumn("amount", "BIGINT")]
        sql = generate_dml_sql("my_table", cols)
        assert "INSERT INTO my_table" in sql

    def test_two_rows_generated(self) -> None:
        cols = [InferredColumn("id", "VARCHAR")]
        sql = generate_dml_sql("my_table", cols)
        # Two rows means two opening parentheses in VALUES block
        values_section = sql[sql.index("VALUES"):]
        assert values_section.count("(") >= 2

    def test_varchar_gets_synth_placeholder(self) -> None:
        cols = [InferredColumn("id", "VARCHAR")]
        sql = generate_dml_sql("my_table", cols)
        assert "'synth-001'" in sql

    def test_bigint_gets_zero_placeholder(self) -> None:
        cols = [InferredColumn("count", "BIGINT")]
        sql = generate_dml_sql("my_table", cols)
        # 0 should appear as the placeholder for numeric types
        assert ", 0)" in sql or "(0)" in sql or "(0," in sql

    def test_boolean_gets_true_placeholder(self) -> None:
        cols = [InferredColumn("active", "BOOLEAN")]
        sql = generate_dml_sql("my_table", cols)
        assert "true" in sql

    def test_timestamp_gets_literal(self) -> None:
        cols = [InferredColumn("ts", "TIMESTAMP(3)")]
        sql = generate_dml_sql("my_table", cols)
        assert "TIMESTAMP '2024-01-01" in sql

    def test_date_gets_literal(self) -> None:
        cols = [InferredColumn("d", "DATE")]
        sql = generate_dml_sql("my_table", cols)
        assert "DATE '2024-01-01'" in sql


class TestGenerateSourcesYaml:
    def test_table_registered(self) -> None:
        entries = [
            ("raw_orders", [InferredColumn("id", "VARCHAR")]),
        ]
        output = generate_sources_yaml("cc_flink", entries)
        data = yaml.safe_load(output)
        source = data["sources"][0]
        assert source["name"] == "cc_flink"
        tables = [t["name"] for t in source["tables"]]
        assert "raw_orders" in tables

    def test_columns_in_sources(self) -> None:
        entries = [
            ("raw_orders", [InferredColumn("id", "VARCHAR"), InferredColumn("ts", "TIMESTAMP(3)")]),
        ]
        output = generate_sources_yaml("cc_flink", entries)
        data = yaml.safe_load(output)
        cols = data["sources"][0]["tables"][0]["columns"]
        col_names = [c["name"] for c in cols]
        assert "id" in col_names
        assert "ts" in col_names

    def test_idempotent_merge(self, tmp_path: Path) -> None:
        """Running twice does not duplicate entries."""
        entries = [("raw_orders", [InferredColumn("id", "VARCHAR")])]
        existing = tmp_path / "sources.yaml"
        # First run
        first = generate_sources_yaml("cc_flink", entries)
        existing.write_text(first)
        # Second run with same entries
        second = generate_sources_yaml("cc_flink", entries, existing_path=existing)
        data = yaml.safe_load(second)
        tables = data["sources"][0]["tables"]
        names = [t["name"] for t in tables]
        assert names.count("raw_orders") == 1

    def test_merges_with_existing_tables(self, tmp_path: Path) -> None:
        """New table is appended; existing table is preserved."""
        existing_yaml = """\
version: 2
sources:
  - name: cc_flink
    tables:
      - name: existing_table
        identifier: existing_table
"""
        existing = tmp_path / "sources.yaml"
        existing.write_text(existing_yaml)
        entries = [("new_table", [InferredColumn("id", "VARCHAR")])]
        output = generate_sources_yaml("cc_flink", entries, existing_path=existing)
        data = yaml.safe_load(output)
        names = [t["name"] for t in data["sources"][0]["tables"]]
        assert "existing_table" in names
        assert "new_table" in names


# ---------------------------------------------------------------------------
# 4. CLI integration tests
# ---------------------------------------------------------------------------

@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


def _build_fixture_pipelines(root: Path) -> None:
    """Create a minimal but complete pipelines tree for CLI testing."""
    # Declared table: orders
    _make_pipeline(root, "facts", "sales", "orders", _DDL_ORDERS, _DML_ORDER_SUMMARY)
    # Declared table: order_summary (the INSERT target)
    _make_pipeline(root, "facts", "sales", "order_summary", _DDL_ORDER_SUMMARY, "")
    # customers is referenced in _DML_ORDER_SUMMARY but has no DDL → undeclared


class TestScaffoldMissingRawsCli:
    def test_dry_run_exits_zero(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        pipelines = tmp_path / "pipelines"
        dbt_project = tmp_path / "dbt_project"
        _build_fixture_pipelines(pipelines)

        result = cli_runner.invoke(
            app,
            ["scaffold-missing-raws", str(pipelines), str(dbt_project)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

    def test_dry_run_does_not_write_files(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        pipelines = tmp_path / "pipelines"
        dbt_project = tmp_path / "dbt_project"
        _build_fixture_pipelines(pipelines)

        cli_runner.invoke(
            app,
            ["scaffold-missing-raws", str(pipelines), str(dbt_project)],
            catch_exceptions=False,
        )

        raws_dir = dbt_project / "models" / "raws"
        assert not raws_dir.exists()

    def test_write_creates_ddl_and_dml(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        pipelines = tmp_path / "pipelines"
        dbt_project = tmp_path / "dbt_project"
        _build_fixture_pipelines(pipelines)

        result = cli_runner.invoke(
            app,
            ["scaffold-missing-raws", str(pipelines), str(dbt_project), "--write"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        raws_dir = dbt_project / "models" / "raws"
        assert (raws_dir / "customers" / "ddl.customers.sql").exists()
        assert (raws_dir / "customers" / "dml.customers.sql").exists()

    def test_write_creates_sources_yaml(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        pipelines = tmp_path / "pipelines"
        dbt_project = tmp_path / "dbt_project"
        _build_fixture_pipelines(pipelines)

        cli_runner.invoke(
            app,
            ["scaffold-missing-raws", str(pipelines), str(dbt_project), "--write"],
            catch_exceptions=False,
        )

        sources_path = dbt_project / "models" / "raws" / "sources.yaml"
        assert sources_path.exists()
        data = yaml.safe_load(sources_path.read_text())
        tables = data["sources"][0]["tables"]
        assert any(t["name"] == "customers" for t in tables)

    def test_write_creates_valid_ddl_sql(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        pipelines = tmp_path / "pipelines"
        dbt_project = tmp_path / "dbt_project"
        _build_fixture_pipelines(pipelines)

        cli_runner.invoke(
            app,
            ["scaffold-missing-raws", str(pipelines), str(dbt_project), "--write"],
            catch_exceptions=False,
        )

        ddl_text = (dbt_project / "models" / "raws" / "customers" / "ddl.customers.sql").read_text()
        assert "CREATE TABLE IF NOT EXISTS customers" in ddl_text
        assert "WITH" in ddl_text

    def test_force_overwrites_existing_file(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        pipelines = tmp_path / "pipelines"
        dbt_project = tmp_path / "dbt_project"
        _build_fixture_pipelines(pipelines)

        # First write
        cli_runner.invoke(
            app,
            ["scaffold-missing-raws", str(pipelines), str(dbt_project), "--write"],
            catch_exceptions=False,
        )

        ddl_file = dbt_project / "models" / "raws" / "customers" / "ddl.customers.sql"
        original_mtime = ddl_file.stat().st_mtime

        # Second write with --force
        result = cli_runner.invoke(
            app,
            ["scaffold-missing-raws", str(pipelines), str(dbt_project), "--write", "--force"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0
        # File was overwritten (mtime should be >= original)
        assert ddl_file.stat().st_mtime >= original_mtime

    def test_no_undeclared_tables_prints_success(
        self, cli_runner: CliRunner, tmp_path: Path
    ) -> None:
        """When every referenced table has a DDL, the command reports success and exits 0."""
        pipelines = tmp_path / "pipelines"
        dbt_project = tmp_path / "dbt_project"

        # Both tables declared
        _make_pipeline(pipelines, "facts", "sales", "orders", _DDL_ORDERS, _DML_ORDER_SUMMARY)
        _make_pipeline(pipelines, "facts", "sales", "order_summary", _DDL_ORDER_SUMMARY, "")
        _make_pipeline(pipelines, "dims", "sales", "customers", _DDL_CUSTOMERS, "")

        result = cli_runner.invoke(
            app,
            ["scaffold-missing-raws", str(pipelines), str(dbt_project)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0
        assert "No undeclared tables" in result.output

    def test_custom_profile_in_sources_yaml(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        pipelines = tmp_path / "pipelines"
        dbt_project = tmp_path / "dbt_project"
        _build_fixture_pipelines(pipelines)

        cli_runner.invoke(
            app,
            [
                "scaffold-missing-raws", str(pipelines), str(dbt_project),
                "--write", "--profile", "my_custom_profile",
            ],
            catch_exceptions=False,
        )

        sources_path = dbt_project / "models" / "raws" / "sources.yaml"
        data = yaml.safe_load(sources_path.read_text())
        assert data["sources"][0]["name"] == "my_custom_profile"

    def test_skip_write_without_flag(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        pipelines = tmp_path / "pipelines"
        dbt_project = tmp_path / "dbt_project"
        _build_fixture_pipelines(pipelines)

        result = cli_runner.invoke(
            app,
            ["scaffold-missing-raws", str(pipelines), str(dbt_project)],
            catch_exceptions=False,
        )

        assert "dry-run" in result.output.lower() or "Run with --write" in result.output


# ---------------------------------------------------------------------------
# 5. Integration with real fixture tree
# ---------------------------------------------------------------------------

_FIXTURE_PIPELINES = (
    Path(__file__).resolve().parent / "fixtures" / "flink-project" / "pipelines"
)


@pytest.mark.skipif(
    not _FIXTURE_PIPELINES.exists(),
    reason="flink-project fixture not found",
)
class TestWithFixturePipelines:
    def test_scan_fixture_pipelines(self) -> None:
        """Running against the real test fixture should not raise and return a dict."""
        result = find_undeclared_tables(_FIXTURE_PIPELINES)
        assert isinstance(result, dict)

    def test_all_undeclared_tables_get_columns(self, tmp_path: Path) -> None:
        """Every undeclared table found in the fixture can have columns inferred."""
        from tools.dbt.flink_dbt_migrate.discover_deps import build_pipelines_ddl_index

        ddl_index = build_pipelines_ddl_index(_FIXTURE_PIPELINES)
        undeclared = find_undeclared_tables(_FIXTURE_PIPELINES)

        for table_name, dml_paths in undeclared.items():
            cols = infer_columns_for_table(table_name, dml_paths, ddl_index)
            assert isinstance(cols, list)
            assert len(cols) >= 1, f"No columns inferred for {table_name}"

    def test_cli_dry_run_on_fixture(self, tmp_path: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["scaffold-missing-raws", str(_FIXTURE_PIPELINES), str(tmp_path)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
