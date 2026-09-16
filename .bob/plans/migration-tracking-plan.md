# Migration Tracking Plan — Large-Scale Flink-to-dbt Migration

## Overview

The real `data-platform-flink` project contains **898 tables** spread across multiple domains
(`facts`, `intermediates`, `dimensions`, `seeds`, `sources`, `views`) and data products
(e.g. `aqem`).  Running `migrate-sl-folder` today migrates all tables in one shot with no
ability to resume, skip already-done tables, or scope work to one product.

**Goal:** Add per-table status tracking (done / failed / skipped) and two new capabilities:

1. **Resume** — skip tables whose DML SHA hasn't changed and were previously successful.
2. **Product filter** — pass `--product <name>` to limit a run to one product subtree.

A single `tracking.yml` file lives in the dbt-pipelines output root and is the source of truth
for migration state across all runs and all product scopes.

---

## Sub-Task 1 — `TrackingStore` — read/write `tracking.yml`

**Status:** `[x] done`

### Intent
Introduce a lightweight module (`tracking_store.py`) that owns the `tracking.yml` schema and
all I/O.  No other file should read or write `tracking.yml` directly.

### Expected Outcomes
- `tracking.yml` is created on first run; subsequent runs merge new entries without losing
  existing ones.
- Each table entry records: `table_name`, `relative_path`, `dml_sha256`, `status`
  (`done` | `failed` | `skipped`), `error` (null or message string), `migrated_at` (ISO-8601
  timestamp).
- `TrackingStore` exposes:
  - `load(path) → TrackingStore` — reads or returns empty store if file absent.
  - `should_migrate(entry: TableEntry) → bool` — returns `True` when status is not `done` OR
    when `dml_sha256` differs from the stored value.
  - `record(table_name, status, sha256, error, relative_path)` — upserts one record.
  - `save(path)` — writes YAML atomically (write to `.tmp`, rename).
  - `summary() → dict` — counts by status.

### Todo List
1. Create `tools/dbt/flink_dbt_migrate/tracking_store.py` with `TrackingRecord` dataclass and
   `TrackingStore` class.
2. Use `pyyaml` (already a project dependency) for serialisation.
3. Write unit tests in `tools/dbt/tests/test_tracking_store.py` covering: create, load,
   should_migrate (sha match / sha change / failed status), save round-trip.

### Relevant Context
- `tools/dbt/flink_dbt_migrate/sl_discovery_mgr.py` — `TableEntry.dml_sha256` is the hash
  to compare against.
- `tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py` — `migrate_sl_folder` is the sole
  consumer; keep `TrackingStore` decoupled from the CLI.
- `tools/dbt/flink_dbt_migrate/temp_write.py` — existing atomic-write helper; reuse or
  replicate the pattern for the YAML save.

---

## Sub-Task 2 — `--product` filter in `crawl_pipeline_folder`

**Status:** `[x] done`

### Intent
Allow the user to pass `--product aqem` alongside the root `pipelines/` directory. The crawler
will only return `TableEntry` records whose `relative_path` contains a segment matching the
product name, so all domains for that product are captured in one run.

### Expected Outcomes
- `crawl_pipeline_folder` accepts an optional `product: str | None` parameter.
- When set, only table directories where `product` appears as a path segment in
  `relative_path` are included.
- `migrate-sl-folder` CLI exposes `--product/-p` option.
- Dry-run output shows `(filtered to product: aqem)` when filter is active.

### Todo List
1. Add `product: str | None = None` parameter to `crawl_pipeline_folder` in
   `sl_discovery_mgr.py`.
2. After computing `relative_path`, skip the entry when `product` is set and none of the path
   parts equals `product`.
3. Add `--product` / `-p` option to `migrate_sl_folder` in `migrate_dml_to_dbt.py`; pass
   through to `crawl_pipeline_folder`.
4. Update the dry-run inventory echo to show the active product filter.
5. Add test cases to `test_sl_discov_mgr.py` for the product filter.

### Relevant Context
- `tools/dbt/flink_dbt_migrate/sl_discovery_mgr.py:104-165` — `crawl_pipeline_folder`.
- `tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py:114-185` — `migrate_sl_folder` CLI.
- The pipeline hierarchy is `pipelines/<domain>/<product>/<table>/` — product is always the
  **third** path segment from the pipeline root, but matching by segment value (not position)
  is safer given real-world variation.

---

## Sub-Task 3 — Wire `TrackingStore` into `migrate_sl_folder`

**Status:** `[x] done`

### Intent
Integrate the tracking store into the migration loop so that:
- Tables already marked `done` with the same SHA are **skipped** (no re-migration).
- Each migration outcome (success / failure / skip) is written to `tracking.yml`.
- A summary line at the end reports migrated / skipped / failed counts.

### Expected Outcomes
- On first run, all tables are migrated; `tracking.yml` is created.
- On re-run with no DML changes, all tables are skipped; no files are re-written.
- On re-run after a fix, only previously `failed` or DML-changed tables are re-migrated.
- Failures in individual tables are caught, recorded as `failed` in `tracking.yml`, and the
  run continues; exit code is 1 if any failure occurred.
- Summary line: `N migrated, M skipped, K failed.`
- `tracking.yml` path is `<dbt_project_dir>/tracking.yml` (always in the dbt output root).

### Todo List
1. In `migrate_sl_folder`, load `TrackingStore` from `dbt_project_dir / "tracking.yml"` before
   the migration loop (even in `--write` mode only — dry-run never touches tracking).
2. In the loop, call `store.should_migrate(entry)` before invoking `migrate_dml_to_dbt`; if
   False, increment `skipped` counter and echo `  →  {table_name}: skipped (unchanged)`.
3. On success, call `store.record(..., status="done")`.
4. On exception, call `store.record(..., status="failed", error=str(exc))`.
5. After the loop, call `store.save(tracking_path)` once.
6. Update the summary echo to include `skipped` count.
7. Add `--force` behaviour: when `--force` is passed, `should_migrate` always returns `True`
   (override the skip logic, not the store itself).
8. Add integration test in `test_migrate_sl_pipeline.py` covering: first run writes
   `tracking.yml`; second run skips unchanged tables; after SHA change, table is re-migrated.

### Relevant Context
- `tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py:187-237` — the migration loop to modify.
- Sub-Task 1 must be complete before this task.
- Sub-Task 2 can be done in parallel (independent of tracking logic).

---

## Sub-Task 4 — Docs & skill update

**Status:** `[x] done`

### Intent
Keep the `dbt-project` skill and `docs/dbt/index.md` accurate so agents and users know about
`--product` and tracking behaviour.

### Expected Outcomes
- `skills/dbt-project/SKILL.md` documents `--product` flag and `tracking.yml` behaviour.
- `docs/dbt/index.md` has an updated CLI reference showing the new options.
- `uv run mkdocs build --strict` passes with no warnings.

### Todo List
1. Update `skills/dbt-project/SKILL.md` — add `--product` to the `migrate-sl-folder` example;
   add a `tracking.yml` section explaining resume behaviour.
2. Update `docs/dbt/index.md` — mirror the same CLI reference changes.
3. Run `uv run mkdocs build --strict` and fix any warnings.

### Relevant Context
- `skills/dbt-project/SKILL.md` — current skill file.
- `docs/dbt/index.md` — current user docs.

---

## tracking.yml Shape

```yaml
# Auto-generated by flink-sql-migrate-dbt — do not edit manually
tables:
  sl_fct_order:
    relative_path: facts/aqem/fct_order
    dml_sha256: "abc123..."
    status: done          # done | failed | skipped
    error: null
    migrated_at: "2025-07-10T14:23:01"
  sl_int_payments:
    relative_path: intermediates/aqem/int_payments
    dml_sha256: "def456..."
    status: failed
    error: "DDL not found for table sl_raw_payments"
    migrated_at: "2025-07-10T14:23:05"
```

---

## Implementation Order

```
Sub-Task 1 (TrackingStore)
      ↓
Sub-Task 3 (wire into loop)   ←—— Sub-Task 2 (--product filter, parallel)
      ↓
Sub-Task 4 (docs)
```

Sub-Tasks 1 and 2 can be started in parallel; Sub-Task 3 depends on Sub-Task 1.
