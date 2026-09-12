# Agent Description — tools/dbt

## Domain Purpose

Convert Flink SQL DML statements into dbt streaming models, scaffold shift-left dbt projects, and generate dbt YAML from SQL models or Confluent Schema Registry schemas.

## Module Map

### `flink_dbt_migrate/` — DML-to-dbt Migration Pipeline

| File | Type | Role |
|---|---|---|
| `migrate_dml_to_dbt.py` | CLI | `migrate` command — migrate DML files or full pipeline folders |
| `migrate.py` | Library | Orchestrates the end-to-end migration workflow |
| `flink_sql_processor.py` | Library | Flink SQL parser — extracts table names, DML structure |
| `dbt_element_mgr.py` | Library | dbt file generator and merger (models, schema YAML) |
| `discover_deps.py` | Library | Dependency discovery across SQL files |
| `rewrite_refs.py` | Library | Rewrites SQL refs to dbt `{{ ref() }}` / `{{ source() }}` |
| `sl_discovery_mgr.py` | Library | Shift-left source discovery |
| `compare_sql.py` | Library | Before/after SQL comparison for dry-run output |
| `validate_compile.py` | Library | Runs `dbt compile` to validate migrated models |
| `type_map.py` | Library | Flink → dbt type mapping |
| `temp_write.py` | Library | Atomic temp-file write helpers |

### Top-level modules

| File | Type | Role |
|---|---|---|
| `sl_dbt.py` | CLI | Scaffold and manage shift-left dbt projects |
| `sql_to_dbt_yaml.py` | CLI | Generate dbt model YAML from SQL SELECT statements |
| `sr_to_dbt_yaml.py` | CLI | Generate dbt source/model YAML from Schema Registry schemas |
| `sql_parser.py` | Library | Shared SQL parsing utilities |
| `model_resolver.py` | Library | Resolves model names and paths within a dbt project |
| `schema_registry_helpers.py` | Library | Schema Registry API helpers used by `sr_to_dbt_yaml.py` |
| `type_inferrer.py` | Library | Infers dbt column types from SQL expressions |

## Agent Tasks

- Migrate Flink DML pipelines to dbt streaming models via `migrate_dml_to_dbt.py`
- Always run `--dry-run` first; only pass `--write` after reviewing dry-run output
- Scaffold new shift-left dbt projects via `sl_dbt.py`
- Generate and maintain dbt schema YAML via `sql_to_dbt_yaml.py` or `sr_to_dbt_yaml.py`
- Validate migrated models with `dbt compile` (wrapped by `validate_compile.py`)
- Keep `type_map.py` current when new Flink SQL types appear

## Key Conventions

- All CLIs use [Typer](https://typer.tiangolo.com/); invoke with `uv run <entry-point>`
- SQL parsing is centralised in `flink_sql_processor.py` and `sql_parser.py` — do not duplicate
- `dbt_element_mgr.py` handles all file writes; never write dbt files directly from other modules
- `tmp/` is a scratch directory for migration work — never commit its contents
- Tests live in `tests/`; run with `uv run pytest tools/dbt/`

## Success Checks

```bash
uv run migrate-dml-to-dbt --help
uv run sl-dbt --help
uv run sql-to-dbt-yaml --help
uv run sr-to-dbt-yaml --help
uv run pytest tools/dbt/ -q
```

## Test Driven Development

* Add pytest file under tests/ folder before writing the new method/function
* run wuth `uv run pytest -vs tests/test_....py`
