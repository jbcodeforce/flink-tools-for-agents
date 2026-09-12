# Flink Deploy Tools

Deploy, undeploy, and query Flink SQL statements on Confluent Cloud using a manifest-driven workflow. It can work for dbt project or simple project with a list of flink SQL ddl and dml files.

## Entry Points

| Command | Description |
|---------|-------------|
| `flink-sql-deploy` | Deploy or undeploy statement groups |
| `flink-sql-snapshot` | Run a bounded snapshot query |
| `flink-sql-stream` | Run a continuous streaming query |

## Required Environment Variables

```bash
FLINK_API_KEY=<key>
FLINK_API_SECRET=<secret>
FLINK_REST_ENDPOINT=<endpoint>
FLINK_ENV_ID=<env-id>
FLINK_ORG_ID=<org-id>
FLINK_COMPUTE_POOL_ID=<pool-id>
FLINK_DATABASE_NAME=<database>
CLOUD_PROVIDER=<aws|gcp|azure>
CLOUD_REGION=<region>
```

## Usage

### Deploy a group

```bash
uv run flink-sql-deploy --sql-dir ./my-pipeline deploy --group sources
uv run flink-sql-deploy --sql-dir ./my-pipeline deploy --group facts
```

### Undeploy a group

```bash
uv run flink-sql-deploy --sql-dir ./my-pipeline undeploy --group facts
uv run flink-sql-deploy --sql-dir ./my-pipeline undeploy --group sources --no-drop-tables
```

### List groups in a manifest

```bash
uv run flink-sql-deploy --sql-dir ./my-pipeline groups
```

### Snapshot query

```bash
uv run flink-sql-snapshot --table orders --output table --limit 20
uv run flink-sql-snapshot --sql "SELECT count(*) FROM orders" --output json
```

### Streaming query

```bash
uv run flink-sql-stream --table orders --max-rows 50
uv run flink-sql-stream --sql "SELECT * FROM orders WHERE amount > 100" --output csv
```

## Manifest Format

See [Manifest Tools](../manifest/index.md) for how to generate `deploy_manifest.json`.
