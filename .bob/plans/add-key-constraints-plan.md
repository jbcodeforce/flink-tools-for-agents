# Plan: Add Primary Key Constraints from `distributed_by` to dbt Schema YAML

## Top-Level Overview
When migrating Flink SQL models to dbt (`dbt-confluent`), tables distributed by specific keys should have matching primary key and not-null constraints recorded in the model's `schema.yml`. Specifically, columns listed in `distributed_by` (or `DISTRIBUTED BY HASH(...)` in DDL) need the following constraint structure:

```yaml
constraints:
  - type: not_null
  - type: primary_key
    expression: "not enforced"
```

This plan covers:
1. Updating the schema generator in `tools/dbt/flink_dbt_migrate/dbt_element_mgr.py` (in `flink-tools-for-agents`) so future migrations automatically add these constraints for `distributed_by` columns.
2. Creating a dedicated helper module (`tools/dbt/flink_dbt_migrate/add_constraints.py`) and a new CLI command in `flink-sql-migrate-dbt` (`add-key-constraints`) to inspect existing `.sql` model files in a dbt project directory, parse `distributed_by` from their `config(...)` blocks, and update the corresponding `schema.yml` files (with `--write` and default `--dry-run` modes).
3. Adding comprehensive unit tests in `tools/dbt/tests/`.
4. Executing the CLI against `dbt-pipelines/pipelines` to fix existing models.

---

## Sub-Tasks

### Sub-Task 1: Constraint Generation in `dbt_element_mgr.py`
- **Intent**: Automatically add `constraints` to column definitions when `emit_schema_yml` / `build_model_schema_entry` generates schema YAML from a `DdlTable` with `distributed_by` (or when `merge_model_schema` merges columns).
- **Expected Outcomes**:
  - `build_model_schema_entry` parses `ddl.distributed_by` into column names (stripping backticks and trimming whitespace).
  - Columns matching any key in `distributed_by` get:
    ```python
    "constraints": [
        {"type": "not_null"},
        {"type": "primary_key", "expression": "not enforced"},
    ]
    ```
  - `merge_model_schema` properly preserves or updates constraints without duplicating.
- **Todo List**:
  1. Add helper `parse_distributed_by_keys(distributed_by: str | None) -> list[str]` to parse comma-separated keys and strip backticks/whitespace.
  2. Update `build_model_schema_entry` to attach `constraints` to matching columns.
  3. Update `merge_model_schema` to ensure existing column entries get constraints merged if missing.
  4. Update existing tests in `test_dbt_element_mgr.py` and add new test cases for constraints.
- **Relevant Context**:
  - `tools/dbt/flink_dbt_migrate/dbt_element_mgr.py`
  - `tools/dbt/tests/test_dbt_element_mgr.py`
- **Status**: `[x] done`

---

### Sub-Task 2: Build `add_constraints.py` for Existing Migrated Models
- **Intent**: Provide a stand-alone scanner and updater that inspects all dbt models in a `models/` directory, extracts `distributed_by` from `{{ config(...) }}`, and updates the corresponding `schema.yml` files.
- **Expected Outcomes**:
  - Parses `distributed_by='...'` or `distributed_by = '...'` from `.sql` files.
  - Loads matching `schema.yml` in the model folder (or creates it if missing).
  - Injects constraints for key columns idempotently (does not duplicate if already present).
  - Supports `--write` (dry-run by default), reporting modified files and columns updated.
- **Todo List**:
  1. Implement `tools/dbt/flink_dbt_migrate/add_constraints.py` with:
     - `extract_distributed_by(sql_text: str) -> list[str]`
     - `update_model_schema_constraints(schema_path: Path, model_name: str, key_columns: list[str], write: bool = False) -> tuple[bool, list[str]]`
     - `scan_and_update_models_constraints(models_dir: Path, write: bool = False) -> list[ModelConstraintUpdate]`
  2. Implement unit tests in `tools/dbt/tests/test_add_constraints.py`.
- **Relevant Context**:
  - Pattern similar to `add_tags.py`
- **Status**: `[x] done`

---

### Sub-Task 3: Expose CLI Command in `migrate_dml_to_dbt.py`
- **Intent**: Add the Typer CLI command `add-key-constraints` to the `flink-sql-migrate-dbt` CLI so users can run it on existing dbt projects.
- **Expected Outcomes**:
  - CLI command `flink-sql-migrate-dbt add-key-constraints <dbt_project_dir> [--write]` available.
  - Clear dry-run report output and confirmation when written.
- **Todo List**:
  1. Register `@app.command(name="add-key-constraints")` in `migrate_dml_to_dbt.py`.
  2. Test CLI invocation with `CliRunner` in pytest.
- **Relevant Context**:
  - `tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py`
- **Status**: `[x] done`

---

### Sub-Task 4: Validation & Run Against Workspace Models
- **Intent**: Run test suite across `flink-tools-for-agents` and run `flink-sql-migrate-dbt add-key-constraints` on `dbt-pipelines/pipelines` (with dry-run first, then with `--write` if verified).
- **Expected Outcomes**:
  - All pytest tests pass without regression.
  - Schema YAMLs under `dbt-pipelines/pipelines/models/` are updated with the required constraints for their `distributed_by` keys.
- **Todo List**:
  1. Run `pytest` on `tools/dbt/tests`.
  2. Run the CLI tool on `dbt-pipelines/pipelines` in dry-run mode and inspect diffs.
  3. Verify against sample models like `manufacturing.correction.topic`.
- **Status**: `[x] done`
