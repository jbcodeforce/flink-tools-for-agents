# Plan: `flink-tools-for-agents` Repository

## Overview

Create a new standalone Python repository called `flink-tools-for-agents` that consolidates
reusable Flink, dbt, and Kafka CLI tools from `flink-studies` into a clean, agent-friendly
structure. Each domain is organized as a Python package under `tools/` and backed by a
Bob/Claude `SKILL.md` under `skills/` that lets agents invoke the CLI tools via shell commands.

**Source repository:** `flink-studies` (this repo)  
**Target repository:** `flink-tools-for-agents` (new repo, created locally then pushed to GitHub)  
**Package manager:** `uv`  
**Packaging:** Single root `pyproject.toml` with optional extras per domain (`[flink]`, `[dbt]`, `[kafka]`, `[all]`)  
**Skills framework:** Bob/Claude `SKILL.md` files — one per domain skill group

### What is migrated

| Source path (flink-studies) | Destination (flink-tools-for-agents) |
|---|---|
| `code/flink-sql/tools/cc_deploy/` | `tools/flink/cc_deploy/` |
| `code/flink-sql/tools/manifest/` | `tools/flink/manifest/` |
| `code/flink-sql/tools/tests/` (manifest tests) | `tools/flink/tests/` |
| `code/dbt/tools/flink_dbt_migrate/` | `tools/dbt/flink_dbt_migrate/` |
| `code/dbt/tools/sl_dbt.py` | `tools/dbt/sl_dbt.py` |
| `code/dbt/tools/sql_to_dbt_yaml.py` | `tools/dbt/sql_to_dbt_yaml.py` |
| `code/dbt/tools/sr_to_dbt_yaml.py` | `tools/dbt/sr_to_dbt_yaml.py` |
| `code/dbt/tools/sql_parser.py` | `tools/dbt/sql_parser.py` |
| `code/dbt/tools/type_inferrer.py` | `tools/dbt/type_inferrer.py` |
| `code/dbt/tools/model_resolver.py` | `tools/dbt/model_resolver.py` |
| `code/dbt/tools/tests/` | `tools/dbt/tests/` |
| `code/flink-sql/tools/kafka/` | `tools/kafka/` |

### What stays in flink-studies (not migrated)

- `code/tools/extract_sql_from_avro.py` — hard-coded demo script
- `code/flink-sql/tools/cp_flink_rest_client.py`, `cp_flink_utils.py` — CMF/K8s on-prem specific
- `code/flink-sql/tools/gen_flink_wide_table.py` — one-off generator
- `code/flink-sql/tools/sync-cc-tools.sh` — vendor sync for flink-studies only
- `code/dbt/airbnb*/`, `code/dbt/flink_workshop/` — study/demo projects

---

## Sub-Tasks

---

### Sub-Task 1 — Scaffold the repository skeleton

**Status:** `[x] done`

**Intent**  
Create the new `flink-tools-for-agents` directory with all top-level project files, the
directory tree, and the root `pyproject.toml`. No source code is migrated yet — just the
skeleton that every subsequent sub-task builds on.

**Expected Outcomes**
- `flink-tools-for-agents/` directory exists at the same level as `flink-studies/`
- `pyproject.toml` defines the package with extras `[flink]`, `[dbt]`, `[kafka]`, `[all]`
- Entry points wired for all CLI tools (stubs pointing to final module paths)
- `README.md`, `CLAUDE.md`, `AGENTS.md`, `Makefile` all present with correct content
- `docs/mkdocs.yml` pre-configured for MkDocs Material with nav placeholders
- `tools/flink/`, `tools/dbt/`, `tools/kafka/`, `skills/flink-deploy/`, `skills/dbt-migrate/`,
  `skills/kafka/`, `skills/manifest/` directories created (with `__init__.py` where needed)
- `.gitignore`, `.python-version`, `uv.lock` initialised via `uv init` + `uv sync`

**Todo List**
1. Create the top-level directory `flink-tools-for-agents/` (sibling of `flink-studies/`)
2. Run `git init` inside it
3. Write `pyproject.toml` with project metadata, all runtime dependencies (merged from both
   source pyproject files), and the optional extras
4. Write `README.md` with project overview, install instructions, and CLI quick-reference table
5. Write `CLAUDE.md` with project-specific Bob rules (domain layout, uv conventions, skill structure)
6. Write `AGENTS.md` describing repository structure and agent responsibilities per domain
7. Write top-level `Makefile` with targets: `install`, `test`, `lint`, `docs-serve`, `docs-build`
8. Write `docs/mkdocs.yml` pre-configured with MkDocs Material theme and nav stubs for each domain
9. Create `tools/flink/__init__.py`, `tools/dbt/__init__.py`, `tools/kafka/__init__.py`
10. Create `skills/flink-deploy/`, `skills/dbt-migrate/`, `skills/kafka/`, `skills/manifest/` dirs
11. Run `uv sync` to generate `uv.lock` and `.venv`
12. Create the GitHub repository with `gh repo create flink-tools-for-agents --public` and push

**Relevant Context**
- Source `pyproject.toml` files to merge:
  - `code/flink-sql/tools/pyproject.toml` (flink + kafka deps)
  - `code/dbt/tools/pyproject.toml` (dbt + sqlglot deps)
- Existing `AGENTS.md` in flink-studies is a good structural template
- MkDocs config pattern: check `docs/` in flink-studies for existing mkdocs.yml

---

### Sub-Task 2 — Migrate the Flink domain tools

**Status:** `[x] done`

**Intent**  
Copy the `cc_deploy` and `manifest` packages from `flink-studies/code/flink-sql/tools/` into
`tools/flink/`, fix any relative imports, remove the vendor-sync machinery, and migrate the
existing manifest tests.

**Expected Outcomes**
- `tools/flink/cc_deploy/` contains all 5 modules with no broken imports
- `tools/flink/manifest/` contains `manifest.py` and `manifest_cli.py` with no broken imports
- `tools/flink/tests/test_manifest_dbt.py` runs cleanly with `uv run pytest tools/flink/tests/`
- Entry points `flink-sql-deploy`, `flink-sql-snapshot`, `flink-sql-stream`,
  `flink-sql-create-manifest` resolve correctly

**Todo List**
1. Copy `code/flink-sql/tools/cc_deploy/` → `tools/flink/cc_deploy/`
2. Copy `code/flink-sql/tools/manifest/manifest.py` and `manifest_cli.py` → `tools/flink/manifest/`
3. Audit and fix any relative imports that break when moved (e.g. `from manifest import` →
   `from tools.flink.manifest import` or package-relative)
4. Copy `code/flink-sql/tools/tests/test_manifest_dbt.py` → `tools/flink/tests/`
5. Update imports in the test file to reflect the new package paths
6. Add `tools/flink/tests/__init__.py`
7. Run `uv run pytest tools/flink/tests/` and fix any failures

**Relevant Context**
- `cc_deploy/deploy_flink_statements.py` — argparse CLI, depends on `manifest` and `cc_deploy.flink_deploy`
- `cc_deploy/flink_deploy.py` — library, depends on `statement_lifecycle` and `manifest`
- `cc_deploy/statement_lifecycle.py` — library, no local deps
- `cc_deploy/run_snapshot_query.py` and `run_streaming_query.py` — argparse CLIs
- `manifest/manifest.py` — Pydantic models + generators; optional dbt support via `importlib`
- `manifest/manifest_cli.py` — Typer CLI wrapping `manifest.py`
- The `dbt-manifest-plan.md` file in manifest/ should NOT be migrated (design artifact)
- `sync-cc-tools.sh` and `cc-tools-sync.sha256` should NOT be migrated

---

### Sub-Task 3 — Migrate the dbt domain tools

**Status:** `[x] done`

**Intent**  
Copy the dbt tools from `flink-studies/code/dbt/tools/` into `tools/dbt/`, fix imports (remove
the dynamic `sys.path` hacks that reach into `flink-studies/code/flink-sql/cm_py_lib/`), and
migrate all dbt tests.

**Expected Outcomes**
- `tools/dbt/flink_dbt_migrate/` is a fully self-contained package
- `tools/dbt/sl_dbt.py`, `sql_to_dbt_yaml.py`, `sr_to_dbt_yaml.py`, `sql_parser.py`,
  `type_inferrer.py`, `model_resolver.py` all present with clean imports
- `tools/dbt/tests/` contains all migrated tests and they pass under `uv run pytest tools/dbt/tests/`
- Entry points `flink-sql-migrate-dbt`, `sl-dbt`, `sql-to-dbt-yaml`, `sr-to-dbt-yaml` resolve

**Todo List**
1. Copy `code/dbt/tools/flink_dbt_migrate/` → `tools/dbt/flink_dbt_migrate/`
2. Copy `code/dbt/tools/sl_dbt.py`, `sql_to_dbt_yaml.py`, `sr_to_dbt_yaml.py`,
   `sql_parser.py`, `type_inferrer.py`, `model_resolver.py` → `tools/dbt/`
3. Locate all `sys.path` manipulations that import from `cm_py_lib` and replace with direct
   package imports (the `cm_py_lib.schema_registry` utilities should be inlined or abstracted
   into a `tools/dbt/schema_registry_helpers.py` module)
4. Audit and fix all relative imports across the `flink_dbt_migrate` sub-package
5. Copy `code/dbt/tools/tests/` → `tools/dbt/tests/`
6. Update imports in all test files
7. Add `tools/dbt/__init__.py` and `tools/dbt/tests/__init__.py`
8. Run `uv run pytest tools/dbt/tests/` and fix any failures

**Relevant Context**
- `sql_to_dbt_yaml.py` has a dynamic path hack: locates `cm_py_lib/schema_registry.py` relative
  to the flink-studies repo root — this must be removed
- `sr_to_dbt_yaml.py` also imports from `cm_py_lib.schema_registry`
- `flink_dbt_migrate/migrate_dml_to_dbt.py` is the Typer CLI entry point
- `flink_dbt_migrate/` sub-modules: `migrate.py`, `flink_sql_processor.py`, `dbt_element_mgr.py`,
  `sl_discovery_mgr.py`, `discover_deps.py`, `rewrite_refs.py`, `type_map.py`,
  `validate_compile.py`, `compare_sql.py`, `temp_write.py`
- Test fixture data (dbt project files, SQL files) live in `tools/tests/` — keep co-located

---

### Sub-Task 4 — Migrate the Kafka domain tools

**Status:** `[x] done`

**Intent**  
Copy the Kafka/Schema Registry tools from `flink-studies/code/flink-sql/tools/kafka/` into
`tools/kafka/`, fix imports, and add a `tests/` directory.

**Expected Outcomes**
- `tools/kafka/` contains `kafka_client.py`, `register_schema.py`, `drop_tables_manifest.py`,
  `table_cleanup.py` with no broken imports
- Entry points `flink-sql-table-cleanup`, `flink-sql-register-schema` resolve correctly
- `tools/kafka/tests/__init__.py` exists (even if empty, ready for future tests)

**Todo List**
1. Copy `code/flink-sql/tools/kafka/kafka_client.py`, `register_schema.py`,
   `drop_tables_manifest.py`, `table_cleanup.py` → `tools/kafka/`
2. Audit imports: `register_schema.py` and `table_cleanup.py` reference `cc_deploy` helpers
   for dotenv loading — update to import from `tools.flink.cc_deploy`
3. Add `tools/kafka/__init__.py` and `tools/kafka/tests/__init__.py`
4. Run a smoke-test import check: `uv run python -c "from tools.kafka import kafka_client"`

**Relevant Context**
- `kafka_client.py` — only stdlib + `confluent-kafka`, no local deps
- `drop_tables_manifest.py` — only stdlib
- `register_schema.py` — depends on `cc_deploy` dotenv loader (`load_dotenv_file`)
- `table_cleanup.py` — depends on `kafka_client`, `drop_tables_manifest`, `cc_deploy.flink_deploy`

---

### Sub-Task 5 — Write the four SKILL.md files

**Status:** `[x] done`

**Intent**  
Author one `SKILL.md` per skill group following the Bob/Claude skill spec. Each skill describes
its purpose, trigger phrases, available CLI commands (with flags), required environment
variables, and usage examples. Agents invoke tools via shell commands using `uv run`.

**Expected Outcomes**
- `skills/flink-deploy/SKILL.md` — covers deploy, undeploy, snapshot, streaming, manifest generation
- `skills/dbt-migrate/SKILL.md` — covers Flink DML → dbt migration, dbt project scaffolding
- `skills/kafka/SKILL.md` — covers schema registration, topic listing, table cleanup
- `skills/manifest/SKILL.md` — covers manifest generation from SQL dirs and dbt projects
- Each SKILL.md includes: description, trigger phrases, prerequisites (env vars), CLI reference,
  step-by-step workflow, and 2+ usage examples

**Todo List**
1. Write `skills/flink-deploy/SKILL.md` — document all `cc_deploy` CLI commands and flags,
   required env vars (`FLINK_API_KEY`, `FLINK_API_SECRET`, org/env/pool/db variables),
   deploy/undeploy/snapshot/stream workflows
2. Write `skills/manifest/SKILL.md` — document `manifest_cli` commands, `--sql-dir`, `--dbt`,
   `--dry-run`, `--overwrite` flags; explain manifest JSON structure
3. Write `skills/dbt-migrate/SKILL.md` — document `migrate_dml_to_dbt` commands
   (`migrate-one-file`, `migrate-sl-folder`), `sl-dbt` init/add commands, `sql-to-dbt-yaml`,
   `sr-to-dbt-yaml`; explain dry-run → write → validate workflow
4. Write `skills/kafka/SKILL.md` — document `register_schema` (register/list/delete/debug-refs),
   `table_cleanup` (list/drop), required SR and Kafka env vars

**Relevant Context**
- Bob skill spec: frontmatter with `name`, `description`, `trigger phrases`
- Existing skill examples in `.bob/skills/` within this repo
- All skills invoke tools via: `uv run <entry-point> [args]` or `uv run python -m <module> [args]`
- Skills should reference the `.env` / `CONFLUENT_ENV_FILE` credential convention documented
  in `code/flink-sql/tools/README.md`

---

### Sub-Task 6 — Write documentation and finalize

**Status:** `[x] done`

**Intent**  
Populate the MkDocs documentation with per-domain pages, a getting-started guide, and
a skills reference. Validate that `mkdocs build` succeeds cleanly. Ensure the full test
suite passes and all entry points are reachable.

**Expected Outcomes**
- `mkdocs build` completes with no warnings
- `uv run pytest` runs all three domain test suites and passes
- All registered entry points are callable: `flink-sql-deploy --help`, `sl-dbt --help`, etc.
- `CLAUDE.md` and `AGENTS.md` accurately describe the final repo state

**Todo List**
1. Write `docs/index.md` — project overview, installation, quick-start
2. Write `docs/flink/index.md` — Flink deploy tool reference (mirror SKILL.md content as docs)
3. Write `docs/dbt/index.md` — dbt tools reference
4. Write `docs/kafka/index.md` — Kafka/Schema Registry tools reference
5. Write `docs/skills/index.md` — how to install and use the skills with Bob/Claude
6. Update `docs/mkdocs.yml` nav to match the written pages
7. Run `uv run mkdocs build --strict` and fix any broken links or missing pages
8. Run full test suite: `uv run pytest` from repo root
9. Smoke-test each entry point with `--help`
10. Review and finalize `CLAUDE.md` and `AGENTS.md` content to reflect actual final state
11. Commit all files and push to GitHub (`git add -A && git commit -m "feat: initial scaffold"`)
