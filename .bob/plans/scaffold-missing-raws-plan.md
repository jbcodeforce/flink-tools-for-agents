# Plan: `scaffold-missing-raws` Command

## Overview

Add a `scaffold-missing-raws` subcommand to the existing `flink-sql-migrate-dbt` CLI
(`tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py`).

**Goal:** Given a `pipelines/` root and a dbt project root, the command scans every DML file
in the pipelines tree, collects all tables referenced in `FROM`/`JOIN` clauses, and identifies
those that have **no matching `CREATE TABLE` DDL file anywhere in the same pipelines tree**.
For each undeclared table it then infers column names and Flink types from the consuming
queries, generates a Flink DDL (`CREATE TABLE`) and a synthetic DML (`INSERT INTO ... VALUES`)
using generic placeholder values, and writes them under
`<dbt_project_root>/models/raws/<table_name>/`.

**Motivation:** Lets developers test dbt pipeline migrations end-to-end without needing live
Kafka topics — the scaffolded raws provide the table definitions and seed rows that upstream
models need to compile and run against Confluent Cloud Flink.

---

## Sub-task 1 — Core scanner: find undeclared tables

**Status:** `[ ] pending`

### Intent
Build a pure function that scans a `pipelines/` root, builds the DDL index (reusing
`build_pipelines_ddl_index` from `discover_deps.py`), walks every `dml*.sql` file, extracts
upstream table references (reusing `collect_upstream_tables` + `collect_cte_names` from
`flink_sql_processor.py` and `discover_deps.py`), and returns the set of table names that are
not present in the DDL index — excluding `INSERT INTO` targets (those are declared tables).

### Expected Outcomes
- A function `find_undeclared_tables(pipelines_root) -> dict[str, list[Path]]` that returns
  `{table_name: [dml_path, ...]}` — all DML files that reference that undeclared table.
- CTE names are excluded (already handled by `collect_cte_names`).
- The DDL index is built once and reused for all DML files.

### Todo List
1. Create `tools/dbt/flink_dbt_migrate/scaffold_missing_raws.py` as the new module.
2. Import `build_pipelines_ddl_index` from `discover_deps`, `collect_upstream_tables` and
   `collect_cte_names` from `flink_sql_processor`, and `parse_dml` from `flink_sql_processor`.
3. Walk all `sql-scripts/dml*.sql` files under `pipelines_root` (skip `tests/` dirs, same
   convention as `build_pipelines_ddl_index`).
4. For each DML file: parse the SQL, collect CTE names, collect upstream table refs, and filter
   against the DDL index.
5. Also exclude the `INSERT INTO` target table name from the missing set (it will have its own
   DDL in the same pipeline folder).
6. Return `{table_name: [dml_path, ...]}` grouped by undeclared table.

### Relevant Context
- `tools/dbt/flink_dbt_migrate/discover_deps.py` — `build_pipelines_ddl_index`,
  `collect_upstream_tables`, `_SQL_KEYWORD_BLOCKLIST`
- `tools/dbt/flink_dbt_migrate/flink_sql_processor.py` — `collect_cte_names`, `parse_dml`,
  `DmlStatement`
- Convention: skip paths containing `"tests"` in their parts (same as DDL index builder).

---

## Sub-task 2 — Column inference for undeclared tables

**Status:** `[ ] pending`

### Intent
For each undeclared table, look at the DML queries that consume it and infer the column names
and Flink types that the table must expose. The approach:

1. Parse each consuming DML with `parse_dml` to get a `DmlStatement`.
2. Use `sqlglot` to parse the SELECT body.
3. Walk `FROM` / `JOIN` references to that table's alias.
4. Collect all columns selected from that alias — using `sql_parser.parse_select` and
   `type_inferrer.infer_type` where the catalog is built from the DDL of the *other* tables in
   the same query (already present in the DDL index).
5. For columns where the type cannot be determined, default to `VARCHAR` and flag with a
   `TODO` comment in the generated DDL.

When multiple consuming DML files reference the same undeclared table, merge their column
sets (union of names; if the same column appears in two files with conflicting inferred types,
use the more specific non-VARCHAR type, or VARCHAR if both are VARCHAR).

### Expected Outcomes
- A function `infer_columns_for_table(table_name, dml_paths, ddl_index) -> list[InferredColumn]`
  where `InferredColumn` is a small dataclass: `name: str`, `flink_type: str`,
  `needs_review: bool`.
- Columns that could not be inferred carry `needs_review=True` and `flink_type="VARCHAR"`.

### Todo List
1. Add `InferredColumn` dataclass to `scaffold_missing_raws.py`.
2. For each consuming DML path: parse the SQL with `sqlglot` (Flink dialect fallback to
   generic), find the `FROM`/`JOIN` node referencing the target table or its alias.
3. Extract all column references that are qualified with that alias (or unambiguous unqualified
   refs), and for each run `type_inferrer.infer_type` using a catalog built from the DDL index
   entries for the other tables in the query.
4. Handle `SELECT *` from the undeclared table by emitting a single `_todo VARCHAR` column
   with `needs_review=True` and a prominent TODO comment — the table is still created so the
   dependency is covered; the developer can fill in the real schema later.
5. Merge results across multiple consuming DML files.

### Relevant Context
- `tools/dbt/sql_parser.py` — `parse_select`, `SelectItem`
- `tools/dbt/type_inferrer.py` — `infer_type`, `build_catalog`
- `tools/dbt/flink_dbt_migrate/flink_sql_processor.py` — `parse_ddl`, `DdlColumn`
- `tools/dbt/flink_dbt_migrate/discover_deps.py` — `collect_upstream_tables`

---

## Sub-task 3 — DDL and DML file generation

**Status:** `[ ] pending`

### Intent
For each undeclared table and its inferred columns, generate:

1. A Flink DDL file (`ddl.<table_name>.sql`) using `CREATE TABLE IF NOT EXISTS` with the
   inferred columns and standard `WITH` options (same defaults as `_RAW_DDL_TMPL` in
   `sl_dbt.py`). Columns with `needs_review=True` get an inline `-- TODO: verify type`
   comment.
2. A synthetic DML file (`dml.<table_name>.sql`) using `INSERT INTO ... VALUES` with two
   placeholder rows (same generic values as `_RAW_DML_TMPL`: `'synth-001'`, `TIMESTAMP
   '2024-01-01 00:00:00'`, etc.).

These are written to `<dbt_project_root>/models/raws/<table_name>/`.

### Expected Outcomes
- `generate_ddl_sql(table_name, columns, profile_name) -> str` — returns DDL text.
- `generate_dml_sql(table_name, columns) -> str` — returns DML text with two INSERT rows.
- Files are written only when `--write` is passed; without it the content is printed to stdout.

### Todo List
1. Add `generate_ddl_sql`, `generate_dml_sql`, and `generate_sources_yaml` functions to
   `scaffold_missing_raws.py`.
2. DDL template: `CREATE TABLE IF NOT EXISTS <table_name> (\n  <columns>\n) WITH (...)` using
   the standard raw-topic WITH options from `sl_dbt._RAW_DDL_TMPL`.
3. For each column: `<name>  <flink_type>[  -- TODO: verify type]`.
4. DML template: `INSERT INTO <table_name> (<col_list>) VALUES ('synth-001', ...)` — use
   type-appropriate placeholder literals: `VARCHAR` → `'synth-001'`, `TIMESTAMP` →
   `TIMESTAMP '2024-01-01 00:00:00'`, `DATE` → `DATE '2024-01-01'`, `BOOLEAN` → `true`,
   numeric types → `0`.
5. Generate a `sources.yaml` at `<dbt_project_dir>/models/raws/sources.yaml` that registers
   all scaffolded tables so `dbt run --select raws.*` resolves them without manual editing.
   Use the same YAML structure as `sl_dbt._RAW_SOURCES_FILE`. Merge idempotently with any
   existing `sources.yaml` (same pattern as `_upsert_sources_yaml` in `sl_dbt.py`).
6. Write all files using the existing `_write` helper from `sl_dbt.py` (or replicate the
   same guard logic in the new module).

### Relevant Context
- `tools/dbt/sl_dbt.py` — `_RAW_DDL_TMPL`, `_RAW_DML_TMPL`, `_RAW_SOURCES_FILE`,
  `_RAW_SOURCES_ENTRY`, `_upsert_sources_yaml`, `_write`
- `tools/dbt/flink_dbt_migrate/dbt_element_mgr.py` — `emit_seed_csv` pattern for reference

---

## Sub-task 4 — CLI command wiring

**Status:** `[ ] pending`

### Intent
Add the `scaffold-missing-raws` command to the existing `flink-sql-migrate-dbt` Typer app in
`migrate_dml_to_dbt.py`. The command takes:
- `pipelines_dir` (positional) — the Flink pipelines root to scan.
- `dbt_project_dir` (positional) — the dbt project root where `models/raws/` will be written.
- `--write` (flag, default `False`) — write files; without it print a dry-run report.
- `--force` (flag, default `False`) — overwrite existing files in `models/raws/`.

Output (dry-run or write): a Rich-formatted report listing each undeclared table, its
consuming DML file(s), and the inferred columns. On `--write`, also print the path of each
written file (DDL, DML, and `sources.yaml`).

### Expected Outcomes
- `flink-sql-migrate-dbt scaffold-missing-raws <pipelines_dir> <dbt_project_dir> [--write] [--force]`
  is callable and exits 0 on success.
- Dry-run prints the full report and all generated file content without writing anything.
- `--write` writes `ddl.<name>.sql` and `dml.<name>.sql` under
  `<dbt_project_dir>/models/raws/<table_name>/`, and a merged `sources.yaml` under
  `<dbt_project_dir>/models/raws/`.

### Todo List
1. Import the scanner and generator from `scaffold_missing_raws.py` into
   `migrate_dml_to_dbt.py`.
2. Add `@app.command()` for `scaffold_missing_raws` with the four parameters above.
3. Call `find_undeclared_tables(pipelines_dir)`, then for each entry call
   `infer_columns_for_table(...)` and `generate_ddl_sql` / `generate_dml_sql`.
4. Print a Rich table or plain-text report: table name | source DML file(s) | columns inferred.
5. If `--write`: write DDL + DML files, then call `generate_sources_yaml` to merge all
   scaffolded tables into `models/raws/sources.yaml`, and print each written path.

### Relevant Context
- `tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py` — `app`, `migrate_sl_folder`,
  `migrate_one_file` (existing command patterns to follow)
- `tools/dbt/sl_dbt.py` — `_write` helper

---

## Sub-task 5 — Tests

**Status:** `[ ] pending`

### Intent
Add a test module `tools/dbt/tests/test_scaffold_missing_raws.py` with unit tests for each
logical layer (scanner, inferrer, generator) and an integration test for the CLI command
using Typer's `CliRunner` against a synthetic fixture directory.

### Expected Outcomes
- Scanner test: given a fixture pipelines dir with one DML referencing an undeclared table,
  `find_undeclared_tables` returns exactly that table.
- Inferrer test: given a DML that selects `col_a VARCHAR`, `col_b` from undeclared table via
  `CAST`, `infer_columns_for_table` returns correct `InferredColumn` list.
- Generator test: `generate_ddl_sql` and `generate_dml_sql` produce well-formed SQL strings.
- CLI test: `CliRunner.invoke` with `--write` creates the expected files under `tmp_path`.

### Todo List
1. Create `tools/dbt/tests/test_scaffold_missing_raws.py`.
2. Write scanner unit test with a small inline fixture (two SQL strings, one DDL + one DML).
3. Write inferrer unit test with known DML and known DDL index.
4. Write generator unit test verifying column list appears in generated DDL.
5. Write CLI integration test using `tmp_path` and `CliRunner`.

### Relevant Context
- `tools/dbt/tests/test_sl_dbt.py` — `CliRunner` fixture pattern
- `tools/dbt/tests/test_seed_migration.py` — inline SQL fixture pattern
- `tools/dbt/tests/fixtures/flink-project/pipelines/` — reusable fixture root for integration test

---

## Implementation Notes

- **No new entry point** — the command is added to the existing `flink-sql-migrate-dbt` script.
- **Reuse aggressively** — `build_pipelines_ddl_index`, `collect_upstream_tables`,
  `collect_cte_names`, `parse_ddl`, `parse_select`, `infer_type` are all reused without
  modification.
- **`SELECT *` handling** — when the consuming query uses `SELECT *` from the undeclared
  table, the table is still scaffolded (dependency is covered) with a single `_todo VARCHAR`
  column and a prominent TODO comment directing the developer to fill in the real schema.
- **`sources.yaml`** — written to `models/raws/sources.yaml` alongside the generated table
  folders. This is separate from `add-raw-topic`'s per-topic registration; it covers all
  scaffolded tables in one pass so `dbt run --select raws.*` works immediately. The file is
  merged idempotently so re-running the command does not duplicate entries.
- **Minimal scope** — no changes to existing commands, no new CLI entry points.
