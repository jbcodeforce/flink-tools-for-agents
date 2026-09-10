---
name: flink-deploy
description: Use when the user wants to deploy, undeploy, or manage Flink SQL statements on Confluent Cloud — covers deploy/undeploy statement groups, run snapshot or streaming queries, and manage Flink SQL jobs via the flink-tools-for-agents CLI tools.
---

# Flink Deploy Skill

This skill guides you through deploying, undeploying, and querying Flink SQL statements on
Confluent Cloud using the `flink-sql-deploy`, `flink-sql-snapshot`, and `flink-sql-stream`
CLI tools from the **flink-tools-for-agents** repository.

## Prerequisites

Before running any command, verify the following environment variables are set (or present in `.env`):

```bash
FLINK_API_KEY=<key>
FLINK_API_SECRET=<secret>
FLINK_REST_ENDPOINT=<endpoint>        # e.g. https://flink.region.cloud.provider.confluent.cloud
FLINK_ENV_ID=<env-id>                 # e.g. env-abc123
FLINK_ORG_ID=<org-id>
FLINK_COMPUTE_POOL_ID=<pool-id>       # e.g. lfcp-abc123
FLINK_DATABASE_NAME=<database>        # Kafka cluster name used as Flink catalog database
CLOUD_PROVIDER=<aws|gcp|azure>
CLOUD_REGION=<region>                 # e.g. us-east-1
```

Set `CONFLUENT_ENV_FILE=/path/to/.env` to point to a custom credential file, or place `.env`
in the project root.

## Step 1 — Check the manifest

Every deploy operation requires a `deploy_manifest.json`. If one doesn't exist yet, run the
manifest skill first to generate it, or use:

```bash
uv run flink-sql-manifest --sql-dir <path-to-sql-dir>
```

To view available groups in a manifest:

```bash
uv run flink-sql-deploy --sql-dir <path> groups
```

## Step 2 — Deploy statement groups

Deploy in dependency order (sources first, then facts):

```bash
# Deploy a single group
uv run flink-sql-deploy --sql-dir <path> deploy --group sources
uv run flink-sql-deploy --sql-dir <path> deploy --group facts

# Deploy all groups defined in the manifest
uv run flink-sql-deploy --sql-dir <path> deploy
```

## Step 3 — Verify deployment (snapshot query)

After deploying, verify data is flowing with a bounded snapshot query:

```bash
# Query a table (auto-generates statement name)
uv run flink-sql-snapshot --table <table_name> --output table --limit 10

# Custom SQL
uv run flink-sql-snapshot --sql "SELECT count(*) as cnt FROM <table_name>" --output json

# Keep the Flink statement alive after completion
uv run flink-sql-snapshot --table <table_name> --keep-statement
```

## Step 4 — Monitor with streaming queries

To tail a live stream:

```bash
uv run flink-sql-stream --table <table_name> --max-rows 50
uv run flink-sql-stream --sql "SELECT * FROM orders WHERE amount > 100" --output csv --timeout 30
```

Stop with `Ctrl+C` or `--max-rows N`.

## Step 5 — Undeploy

```bash
# Undeploy a group (drops tables by default)
uv run flink-sql-deploy --sql-dir <path> undeploy --group facts

# Undeploy without dropping tables
uv run flink-sql-deploy --sql-dir <path> undeploy --group facts --no-drop-tables

# Drop remaining tables from a manifest
uv run flink-sql-deploy --sql-dir <path> drop-tables
```

## Common patterns

### Deploy a full pipeline end-to-end

```bash
uv run flink-sql-deploy --sql-dir ./pipeline deploy --group ddl
uv run flink-sql-deploy --sql-dir ./pipeline deploy --group sources
uv run flink-sql-deploy --sql-dir ./pipeline deploy --group facts
uv run flink-sql-snapshot --table fct_orders --output table --limit 5
```

### Tear down everything

```bash
uv run flink-sql-deploy --sql-dir ./pipeline undeploy
uv run flink-sql-deploy --sql-dir ./pipeline drop-tables
```

## Troubleshooting

- **"Statement not found"** — the statement may have already been deleted or never deployed.
  Run `groups` to check what the manifest defines.
- **Exit code 1 with timeout** — increase `--timeout` or check compute pool capacity in the
  Confluent Cloud UI.
- **Authentication error** — verify `FLINK_API_KEY` and `FLINK_API_SECRET` are correct and
  that the API key has sufficient permissions.
