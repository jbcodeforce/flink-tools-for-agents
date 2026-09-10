# Flink Tools for Agents

Reusable CLI tools and Bob/Claude agent skills for managing Apache Flink SQL, dbt streaming migrations, and Kafka on Confluent Cloud.

## Quick Start

```bash
# Install all domains
uv sync --extra all --extra dev

# Verify entry points
flink-sql-deploy --help
flink-sql-manifest --help
flink-sql-migrate-dbt --help
sl-dbt --help
flink-sql-cleanup --help
flink-sql-register --help
```

## Domains

- [Flink Deploy](flink/index.md) — deploy/undeploy Flink SQL statements, snapshot & streaming queries
- [Manifest](manifest/index.md) — generate deployment manifests from SQL dirs or dbt projects
- [dbt Migrate](dbt/index.md) — convert Flink DML to dbt models, scaffold dbt projects
- [Kafka & Schema Registry](kafka/index.md) — register schemas, manage topics and Flink tables

## Using the Skills

See [Skills for Bob/Claude](skills/index.md) for instructions on installing and using the agent skills.
