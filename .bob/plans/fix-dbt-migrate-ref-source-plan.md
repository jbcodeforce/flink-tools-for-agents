# Plan: Fix Reference Resolution (`ref()` vs `source()`) in `flink_dbt_migrate`

## Overview
Currently, `flink_dbt_migrate` defaults to generating `{{ source(...) }}` references whenever an upstream table cannot be found as an already-existing `.sql` file inside `<dbt_project_dir>/models`. In a multi-table migration or when migrating tables with existing parent pipeline definitions (`pipeline_definition.json`), parent tables defined within the repository/pipeline tree are erroneously classified as `source()` instead of `ref()`.

This plan updates the reference resolution logic in `flink_dbt_migrate` to:
1. Distinguish between tables that are produced by pipelines in the repository (or known models being migrated) and external sources.
2. Inspect `pipeline_definition.json` parent entries to check if their referenced pipeline folder contains DML or corresponds to a known migratable model.
3. Correct the model search root and candidate model index during both batch migration (`migrate-sl-folder`) and single-file migration (`migrate`), ensuring that parent tables within the repository/pipeline tree resolve as `{{ ref('...') }}` and only truly external tables resolve as `{{ source('...', '...') }}`.

---

## Sub-Tasks

### Sub-Task 1: Enhance `sl_discovery_mgr.py` and `discover_deps.py` with Pipeline Model Awareness
- **Intent**: Provide mechanism to determine whether an upstream table is produced by a DML / pipeline folder in the source repository.
- **Expected Outcomes**:
  - `pipeline_definition.json` parents inspection can determine if a parent table has a DML or points to a pipeline folder with DML.
  - Helper functions to build an index of all produced tables (models/seeds) in the source pipeline repository.
  - `resolve_upstream_deps` checks whether an upstream table is in the set of known pipeline models / produced tables or existing dbt models, resolving it to `ref()` before falling back to external `source()`.
- **Todo List**:
  1. In `tools/dbt/flink_dbt_migrate/sl_discovery_mgr.py`, update `_upstream_ddl_map_from_pipeline_def` or add parent inspection logic to track which parent tables are pipeline-produced (contain DML or point to a pipeline directory with DML).
  2. In `tools/dbt/flink_dbt_migrate/discover_deps.py`, introduce support for a `known_models: set[str] | dict[str, str]` (or pipeline model index) in `resolve_upstream_deps`.
  3. Update `resolve_upstream_deps` to resolve upstream tables found in `known_models` or identified as pipeline-produced parents to `resolution="ref"`.
  4. Only fall back to `resolution="source"` if the table is truly an external source (i.e., only has DDL / no DML pipeline).
- **Relevant Context**:
  - [`tools/dbt/flink_dbt_migrate/discover_deps.py`](tools/dbt/flink_dbt_migrate/discover_deps.py)
  - [`tools/dbt/flink_dbt_migrate/sl_discovery_mgr.py`](tools/dbt/flink_dbt_migrate/sl_discovery_mgr.py)
- **Status**: `[ ] pending`

---

### Sub-Task 2: Wire Known Models Indexing into `migrate.py` and `migrate_dml_to_dbt.py`
- **Intent**: Ensure both CLI commands (`migrate-sl-folder` and `migrate`) construct and pass the known models index / repository pipeline model index to the dependency resolver.
- **Expected Outcomes**:
  - In batch migration (`migrate-sl-folder`), the discovered `TableEntry` list (all target tables being migrated) is passed to `migrate_dml_to_dbt` / `resolve_upstream_deps` as known models.
  - In single file migration (`migrate`), `pipeline_definition.json` parent information and repository pipeline scans (if inside a pipelines repository) correctly inform known model resolution.
  - Fix any target directory vs dbt project root path resolution discrepancies when searching for existing dbt models.
- **Todo List**:
  1. In `tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py`, build the set of known model names from `entries` during `migrate-sl-folder` and pass it down.
  2. In single-file `migrate`, inspect `pipeline_definition.json` and check if parent table directories contain `dml.*.sql`, populating known ref models.
  3. In `tools/dbt/flink_dbt_migrate/migrate.py`, ensure `known_models` is accepted and forwarded to `resolve_upstream_deps`.
- **Relevant Context**:
  - [`tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py`](tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py)
  - [`tools/dbt/flink_dbt_migrate/migrate.py`](tools/dbt/flink_dbt_migrate/migrate.py)
- **Status**: `[ ] pending`

---

### Sub-Task 3: Unit and Integration Testing
- **Intent**: Validate that references are accurately emitted as `{{ ref(...) }}` for existing parent tables and `{{ source(...) }}` only for external source tables.
- **Expected Outcomes**:
  - Existing tests continue to pass.
  - New test cases verifying:
    - Tables whose parents in `pipeline_definition.json` point to DML pipelines produce `{{ ref(...) }}`.
    - Tables whose parents only have DDL (external source) produce `{{ source(...) }}`.
    - Batch migration of interrelated pipeline folders correctly produces `{{ ref(...) }}` between models without requiring prior runs or manual `--ref-table` overrides.
- **Todo List**:
  1. Add tests in `tools/dbt/tests/test_discover_deps.py` covering `known_models` and parent pipeline resolution.
  2. Add/update tests in `tools/dbt/tests/test_migrate_dml_to_dbt.py` and `tools/dbt/tests/test_sl_discov_mgr.py` with pipeline structures having parent DMLs vs external source DDLs.
  3. Run the full pytest test suite across `tools/dbt/tests` to ensure no regressions.
- **Relevant Context**:
  - [`tools/dbt/tests/test_discover_deps.py`](tools/dbt/tests/test_discover_deps.py)
  - [`tools/dbt/tests/test_migrate_dml_to_dbt.py`](tools/dbt/tests/test_migrate_dml_to_dbt.py)
  - [`tools/dbt/tests/test_sl_discov_mgr.py`](tools/dbt/tests/test_sl_discov_mgr.py)
- **Status**: `[ ] pending`
