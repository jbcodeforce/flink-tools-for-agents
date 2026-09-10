# flink-tools-for-agents

Reusable CLI tools for managing Apache Flink SQL deployments, dbt streaming migrations, and Kafka/Schema Registry operations on Confluent Cloud. Each tool domain is backed by a Bob/Claude agent **skill** so AI agents can invoke the tools via shell commands.

## Domains

| Domain | Tools | Skill |
|--------|-------|-------|
| **Flink deploy** | Deploy/undeploy statements, snapshot & streaming queries | `skills/flink-deploy/SKILL.md` |
| **Manifest** | Generate `deploy_manifest.json` from SQL dirs or dbt projects | `skills/manifest/SKILL.md` |
| **dbt migrate** | Convert Flink DML → dbt models, scaffold dbt projects | `skills/dbt-migrate/SKILL.md` |
| **Kafka** | Register schemas, list/drop Flink tables, manage topics | `skills/kafka/SKILL.md` |

## Installation

Requires Python ≥ 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
# Install all domains
uv sync --extra all

# Install a single domain
uv sync --extra flink
uv sync --extra dbt
uv sync --extra kafka
```

## CLI Quick Reference

### Flink Deploy

```bash
# Deploy all statement groups in a manifest
flink-sql-deploy --sql-dir <path> deploy --group <group>

# Undeploy a group (and optionally drop tables)
flink-sql-deploy --sql-dir <path> undeploy --group <group>

# Run a bounded snapshot query
flink-sql-snapshot --table <table> --output table

# Run a continuous streaming query
flink-sql-stream --sql "SELECT * FROM orders" --max-rows 100
```

### Manifest

```bash
# Generate manifest from a Flink SQL directory
flink-sql-manifest --sql-dir <path>

# Generate manifest from a dbt-confluent project
flink-sql-manifest --sql-dir <dbt-project-root> --dbt

# Dry-run (no files written)
flink-sql-manifest --sql-dir <path> --dry-run
```

### dbt Migrate

```bash
# Migrate a single Flink DML file to a dbt model
flink-sql-migrate-dbt migrate-one-file <dml.sql> <dbt-project-dir>

# Migrate an entire shift-left pipeline folder
flink-sql-migrate-dbt migrate-sl-folder <pipeline-dir> <dbt-project-dir> --write

# Scaffold a new dbt streaming project
sl-dbt init <project-root> --type data-product
sl-dbt add-data-product <project-root> <name>
sl-dbt add-table <project-root> <table> <data-product> --table-type fact
```

### Kafka / Schema Registry

```bash
# Register an Avro or JSON schema
flink-sql-register register <schema-file>

# List all registered schemas
flink-sql-register list

# List Kafka topics / Flink tables
flink-sql-cleanup list --output manifest.json

# Drop tables listed in a manifest
flink-sql-cleanup drop --manifest manifest.json
```

## Configuration

All tools load credentials from environment variables or a `.env` file.
Set `CONFLUENT_ENV_FILE=/path/to/.env` to point to a custom location, or place `.env` in the repo root.

Required variables per domain are documented in each skill's `SKILL.md` file.

## Development

```bash
make install   # uv sync --extra all --extra dev
make test      # uv run pytest
make lint      # uv run ruff check .
make docs      # uv run mkdocs serve
```

## Skills for Bob / Claude

Install the skills by copying or symlinking the `skills/` directory into your `.bob/skills/` folder,
or reference individual `SKILL.md` files from your Bob workspace configuration.

## License

Apache 2.0
