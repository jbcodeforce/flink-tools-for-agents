# Plan: Fix CTE-before-INSERT Detection in `collect_cte_names`

## Status: PENDING

## Top-Level Overview

When a Flink DML file places a `WITH ... AS (...)` CTE block **before** the `INSERT INTO`
statement (a valid Flink SQL pattern), `parse_dml` captures the CTE preamble as
`leading_comments` and the `SELECT` body as `body`.  `collect_cte_names` is called only on
`body`, so the CTE name is invisible to it.  `collect_upstream_tables` then sees the CTE name
as a plain table reference, tries to find its DDL, fails, and aborts the migration with:

```
No DDL file found for upstream table training_assignment in .../sql-scripts.
Tried ddl.training_assignment.sql and scanned ddl*.sql.
```

The fix is minimal: pass `leading_comments + body` (the full DML text) to
`collect_cte_names`, or extract CTEs from `leading_comments` before scanning `body` for
upstream tables.  No structural changes are needed.

---

## Sub-Task 1 — Fix `collect_cte_names` call site in `resolve_upstream_deps`

**Status:** `[ ] pending`

### Intent
`resolve_upstream_deps` calls `collect_cte_names(dml.body)` (line 232 of `discover_deps.py`).
This misses CTEs declared before `INSERT INTO`.  Pass the full DML text — `leading_comments`
concatenated with `body` — so the CTE scanner sees the entire statement.

### Expected Outcomes
- `collect_cte_names` returns CTE names declared either before or after `INSERT INTO`.
- `training_assignment` (and any other leading CTE) is excluded from upstream table lookup.
- The `stage_training_assignment` migration succeeds.

### Todo List
1. In `discover_deps.py` line 232, change:
   ```python
   cte_names = collect_cte_names(dml.body)
   ```
   to:
   ```python
   full_dml = (dml.leading_comments + "\n" + dml.body) if dml.leading_comments else dml.body
   cte_names = collect_cte_names(full_dml)
   ```
2. Verify there are no other call sites that pass only `dml.body` when CTEs before INSERT are
   possible. Check `dbt_element_mgr.py` line 119 (another `collect_cte_names` call) and apply
   the same fix if it uses `dml.body` alone.

### Relevant Context
- `discover_deps.py` line 232 — the call being changed
- `dbt_element_mgr.py` line 119 — second call site to verify
- `flink_sql_processor.py` lines 593-641 — `collect_cte_names` implementation (correct as-is)
- `DmlStatement` dataclass (line 39) — has both `.leading_comments` and `.body` fields

---

## Sub-Task 2 — Add regression test for CTE declared before INSERT INTO

**Status:** `[ ] pending`

### Intent
There is no test covering a DML where the CTE preamble precedes the `INSERT INTO` keyword.
Add one to `test_discover_deps.py` to lock in the fix.

### Expected Outcomes
- A new test `test_resolve_upstream_deps_cte_before_insert` passes.
- The test uses a DML whose CTE is in `leading_comments` and verifies that the CTE name is
  **not** treated as an upstream table.

### Todo List
1. In `test_discover_deps.py`, add a test that:
   - Defines a DML string like:
     ```sql
     WITH training_assignment AS (SELECT id FROM raw_ta)
     INSERT INTO stage_training_assignment
     SELECT id FROM training_assignment
     ```
   - Parses it with `parse_dml`.
   - Verifies `dml.leading_comments` contains the CTE.
   - Creates a minimal `tmp_path`-based source dir with a DDL for `raw_ta`.
   - Calls `resolve_upstream_deps` and asserts `training_assignment` does **not** appear in the
     returned deps (only `raw_ta` should).
2. Also add a unit test directly on `collect_cte_names` in `test_flink_sql_processor.py` for
   a string that starts with `WITH cte AS (...)` and has no `SELECT` following the WITH — to
   confirm it still returns `{"cte"}` even when the text ends before a SELECT.

### Relevant Context
- `test_discover_deps.py` lines 49-61 — existing `resolve_upstream_deps` test to use as a template
- `test_flink_sql_processor.py` `TestCollectCteNames` class (line 432) — add the new unit test there

---

## Sub-Task 3 — Run full test suite and validate

**Status:** `[ ] pending`

### Intent
Confirm all existing and new tests pass after the two-line change.

### Expected Outcomes
- `uv run pytest tools/dbt/tests/ -v` passes with no failures.

### Todo List
1. Run `uv run pytest tools/dbt/tests/ -v`.
2. Fix any unexpected breakage (none expected — the change only widens the set of CTEs seen).

### Relevant Context
- `tools/dbt/tests/` — full test suite
