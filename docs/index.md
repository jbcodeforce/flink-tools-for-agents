# Flink Tools for Agents

Reusable CLI tools and Bob/Claude agent skills for managing Confluent Cloud Flink projects, SQL statements, dbt confluent projects, and Kafka on Confluent Cloud.

User may interact with an existing AI harness liek Claude Code, Codex, IBM Bob, Cursor... which itself calls LLM models. The harness exposes tools that can be directly called by a human. 

![](./images/hl_architecture.drawio.png)

Those tools are in three groups: flink project management (using Confluent dbt), Confluent cloud deployment (using sql-confluent library), and Kafka/schema registry tools for development practices areound schema and tests.

This repository includes tool implementation, LLM skill definitions and how data engineers may use those with or without AI harness.

## Quick Start For Developers

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

## Quick Start for Data Engineer

See the [Data engineer day to day practice](./methodology/index.md)

## Domains

- [Flink Deploy](flink/index.md) — deploy/undeploy Flink SQL statements, snapshot & streaming queries using [python confluent-sql]()
- [Manifest](manifest/index.md) — generate deployment manifests from SQL dirs or dbt projects
- [dbt flink project management](dbt/index.md) — convert Flink DML to dbt models, scaffold and manage Flink projects
- [Kafka & Schema Registry](kafka/index.md) — register schemas, manage topics and Flink tables

## Using the Skills

See [Skills for Bob/Claude](skills/index.md) for instructions on installing and using the agent skills.

