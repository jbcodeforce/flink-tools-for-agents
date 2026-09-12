# Agent Description — tools/kafka

## Domain Purpose

Manage Kafka topics, register/list/delete schemas in Confluent Schema Registry, and clean up
stale Flink tables. Acts as the infrastructure layer that other tool domains (flink, dbt) rely
on before deploying pipelines.

## Module Map

| File | Type | Role |
|---|---|---|
| `register_schema.py` | CLI | `register`, `list`, `delete` schema commands against Schema Registry |
| `table_cleanup.py` | CLI | Flink table inventory and selective cleanup |
| `kafka_client.py` | Library | Confluent Kafka AdminClient helpers (topic listing, metadata) |
| `drop_tables_manifest.py` | Library | Builds a table-drop manifest from a deployment manifest |

## Agent Tasks

- Register schemas before deploying Flink pipelines via `register_schema.py`
- Audit and clean up stale Flink tables via `table_cleanup.py`
- List Kafka topics for manifest generation using helpers in `kafka_client.py`
- Build table-drop manifests via `drop_tables_manifest.py` before running `undeploy`

## Key Conventions

- All CLIs use [Typer](https://typer.tiangolo.com/); invoke with `uv run <entry-point>`
- `kafka_client.py` is a pure library — no CLI surface; import it from other modules
- Credentials are always read from environment variables; never hardcode them
- Tests live in `tests/`; run with `uv run pytest tools/kafka/`

## Success Checks

```bash
uv run register-schema --help
uv run table-cleanup --help
uv run pytest tools/kafka/ -q
```
