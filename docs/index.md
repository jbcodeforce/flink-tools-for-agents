# Flink Tools for Agents

Reusable CLI tools and Bob/Claude agent skills for managing Confluent Cloud Flink projects, SQL statements, dbt confluent projects, and Kafka on Confluent Cloud.

## Quick Start

```bash
# Install all domains
uv sync --extra all --extra dev

# Verify entry points
uv run flink-sql-deploy --help
uv run flink-sql-manifest --help
uv run flink-sql-migrate-dbt --help
uv run sl-dbt --help
uv run flink-sql-cleanup --help
uv run flink-sql-register --help
```

## Domains

- [Flink Deploy](flink/index.md) — deploy/undeploy Flink SQL statements, snapshot & streaming queries using [python confluent-sql]()
- [Manifest](manifest/index.md) — generate deployment manifests from SQL dirs or dbt projects
- [dbt Migrate](dbt/index.md) — convert Flink DML to dbt models, scaffold dbt projects
- [Kafka & Schema Registry](kafka/index.md) — register schemas, manage topics and Flink tables

## Using the Skills

See [Skills for Bob/Claude](skills/index.md) for instructions on installing and using the agent skills.

## Example of Prompts
