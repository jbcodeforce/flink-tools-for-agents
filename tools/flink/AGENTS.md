# Agent Description — tools/flink

## Domain Purpose

Deploy and undeploy Flink SQL statement groups on Confluent Cloud, run bounded snapshot queries,
run continuous streaming queries, and generate deployment manifests from SQL directories or dbt
projects.

## Module Map

### `cc_deploy/` — Deployment CLI & Library

| File | Type | Role |
|---|---|---|
| `deploy_flink_statements.py` | CLI | `deploy`, `undeploy`, `drop-tables`, `groups` commands |
| `run_snapshot_query.py` | CLI | Bounded table/SQL queries (returns when complete) |
| `run_streaming_query.py` | CLI | Continuous streaming queries (runs until stopped) |
| `flink_deploy.py` | Library | Confluent SQL REST API wrappers |
| `statement_lifecycle.py` | Library | Statement state polling and lifecycle management |

### `manifest/` — Manifest Generator

| File | Type | Role |
|---|---|---|
| `manifest_cli.py` | CLI | `generate` command — writes `deploy_manifest.json` |
| `manifest.py` | Library | Pydantic manifest models and generator logic |

## Agent Tasks

- Generate manifests from Flink SQL dirs or dbt projects via `manifest_cli.py`
- Deploy / undeploy statement groups via `deploy_flink_statements.py`
- Run diagnostic snapshot queries via `run_snapshot_query.py`
- Keep wrappers in `flink_deploy.py` in sync with Confluent SQL REST API changes
- Keep `statement_lifecycle.py` up-to-date with new statement terminal states

## Key Conventions

- All entry points use [Typer](https://typer.tiangolo.com/); invoke with `uv run <entry-point>`
- Statement lifecycle polling lives exclusively in `statement_lifecycle.py` — do not duplicate it
- Manifest models are Pydantic v2; keep validators strict
- Tests live in `tests/`; run with `uv run pytest tools/flink/`

## Success Checks

```bash
uv run deploy-flink-statements --help
uv run run-snapshot-query --help
uv run run-streaming-query --help
uv run generate-manifest --help
uv run pytest tools/flink/ -q
```
