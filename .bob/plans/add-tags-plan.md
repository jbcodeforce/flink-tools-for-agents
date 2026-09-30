# Plan: Add `add-tags` Command to `flink-sql-migrate-dbt`

## Top-Level Overview

Add a new CLI command `add-tags` to the `flink-sql-migrate-dbt` tool (in the `flink-tools-for-agents` repo).

The command walks an existing migrated dbt project (`dbt-pipelines/pipelines/`), locates each model's original `pipeline_definition.json` in the source `pipelines/` tree, reads `product_name` from it, and injects `tags=['<product_name>']` into the `{{ config(...) }}` block of the already-migrated dbt SQL model. After running the command, users can do `dbt run --select tag:mx` (or any product name) to target a specific product's models.

The command must be **idempotent**: re-running it on a model that already has a `tags=` entry must not duplicate the tag.

**Scope**: only touches `.sql` model files (the `{{ config(...) }}` block); the `schema.yml` files do not need changes (dbt resolves tags from the config block).

---

## Sub-Tasks

---

### Sub-task 1: Understand the existing CLI entry point and code conventions

**Status**: `[ ] pending`

**Intent**  
Before writing any code, read the CLI module that registers `migrate-sl-folder` (and other commands) so the new command follows the same patterns — argument parsing, `--write` flag, error handling, logging.

**Expected Outcomes**  
- Know the exact file, function, and decorator/click-group where new commands are registered.
- Know whether the tool uses `click`, `argparse`, or another CLI library.
- Know how the tool currently reads `pipeline_definition.json` and resolves paths between source pipelines and dbt output.

**Todo List**
1. Read `pyproject.toml` in `flink-tools-for-agents` to find the `[project.scripts]` entry that defines `flink-sql-migrate-dbt`.
2. Follow the entry point to the main CLI module; read all command registrations.
3. Read the `migrate-sl-folder` command handler in full.
4. Read any shared utility that reads `pipeline_definition.json` or resolves model paths.

**Relevant Context**
- Tool lives at `/Users/jerome/Documents/Code/flink-tools-for-agents`
- Command is invoked as `uv run flink-sql-migrate-dbt`
- `tracking.yml` at `dbt-pipelines/pipelines/tracking.yml` maps `table_name → relative_path` (e.g. `dimensions/amx/dim_capture_type`)
- Source `pipeline_definition.json` lives at `pipelines/<relative_path>/pipeline_definition.json`
- Migrated SQL lives at `dbt-pipelines/pipelines/models/<relative_path>/<table_name>.sql`

---

### Sub-task 2: Implement the `add-tags` command

**Status**: `[ ] pending`

**Intent**  
Add a new CLI command `add-tags` that patches existing migrated SQL files by injecting `tags=['<product_name>']` into the `{{ config(...) }}` block. This command runs on an already-migrated dbt project — it does not re-migrate anything.

**Expected Outcomes**  
- Running `uv run flink-sql-migrate-dbt add-tags <pipelines_dir> <dbt_dir>` reads `product_name` from each table's `pipeline_definition.json` and patches the corresponding `{{ config(...) }}` block to include `tags=['<product_name>']`.
- A `--write` flag controls whether files are actually written (dry-run by default, consistent with other commands).
- The command is idempotent: re-running on an already-tagged file leaves it unchanged.
- Tables with no `pipeline_definition.json` (e.g. manually added sources, raws) are skipped with a warning.
- Summary is printed at the end: N patched, N skipped, N already tagged.

**Todo List**
1. In the CLI module, register a new command `add-tags` with the same arguments as other commands (`pipelines_dir`, `dbt_dir`, `--write`).
2. Write a helper `read_product_name(pipeline_def_path) -> str` that parses the JSON and returns the top-level `product_name`.
3. Write a helper `inject_tags_into_config(sql_content: str, tag: str) -> tuple[str, bool]` that:
   - Uses a regex to find the `{{ config(` block.
   - Returns `(unchanged_content, False)` if `tags=` already present.
   - Inserts `tags=['<tag>']` as a new parameter before the closing `)` of the config call.
   - Returns `(patched_content, True)`.
4. Implement the command body: iterate over `tracking.yml` entries, resolve source `pipeline_definition.json` path, read `product_name`, resolve dbt SQL path, call `inject_tags_into_config`, write if `--write`.
5. Add tests covering: tag injection, idempotency, missing pipeline_definition (skip), already-tagged model.

**Relevant Context**
- `{{ config(...) }}` block is always the first thing in the SQL file (lines 1–N).
- The config block format is multi-line with a closing `) }}` on its own line.
- `tracking.yml` is at `<dbt_dir>/tracking.yml`; its `tables.<table_name>.relative_path` gives the path segment.
- Source `pipeline_definition.json` is at `<pipelines_dir>/<relative_path>/pipeline_definition.json`.
- Dbt SQL model is at `<dbt_dir>/models/<relative_path>/<table_name>.sql`.
- Example of target SQL after patching:
  ```sql
  {{ config(
      materialized='streaming_table',
      distributed_by='`phase_sid`',
      tags=['mx'],
      with={...}
  ) }}
  ```

---

### Sub-task 3: Wire into the `flink-sql-migrate-dbt` migration flow (optional future)

**Status**: `[ ] pending`

**Intent**  
Optionally, also inject `tags=` during the initial `migrate-sl-folder` run so new migrations are already tagged. This is lower priority because the `add-tags` command handles the existing migrated project retroactively.

**Expected Outcomes**  
- The `migrate-sl-folder` command passes `product_name` (already present in `pipeline_definition.json` at migration time) into the config block writer.
- New migrations do not need a separate `add-tags` pass.

**Todo List**
1. Find the code that builds the `{{ config(...) }}` string during migration.
2. Add `product_name` as a parameter to the config builder.
3. Pass `tags=[product_name]` into the config call.

**Relevant Context**
- Only implement this after Sub-task 2 is working and tested.
- `pipeline_definition.json` is available at migration time in the source directory.

---

## Key File Locations

| File | Role |
|---|---|
| `flink-tools-for-agents/pyproject.toml` | Defines `flink-sql-migrate-dbt` entry point |
| `flink-tools-for-agents/src/.../cli.py` (TBD) | CLI command registration |
| `dbt-pipelines/pipelines/tracking.yml` | Maps table names → relative paths |
| `pipelines/<type>/<product>/<dir>/pipeline_definition.json` | Contains `product_name` |
| `dbt-pipelines/pipelines/models/<type>/<product>/<dir>/<table>.sql` | Target SQL to patch |

---

## Design Decisions

- **Tags in SQL `config()`, not in `schema.yml`**: dbt resolves tags from the `config()` Jinja call. Adding tags there is the standard pattern and consistent with the other config parameters (`materialized`, `distributed_by`, etc.).
- **Standalone command, not re-migration**: The user explicitly asked for a command that works on an existing migrated project. This avoids touching the migration logic.
- **Regex-based patching**: The config block has a predictable structure. A targeted regex insert before the closing `)` of the config call is simpler and safer than full SQL parsing.
- **Idempotency via `tags=` presence check**: Before patching, check if `tags=` already appears in the config block.
