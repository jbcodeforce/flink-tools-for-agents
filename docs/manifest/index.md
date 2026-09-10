# Manifest Tools

Generate `deploy_manifest.json` files that describe how Flink SQL statements should be deployed
in groups and in what order.

## Entry Points

| Command | Description |
|---------|-------------|
| `flink-sql-manifest` | Generate a deploy manifest from a SQL dir or dbt project |

## Usage

### From a Flink SQL directory

```bash
# Dry-run (print manifest without writing)
flink-sql-manifest --sql-dir ./my-pipeline --dry-run

# Write deploy_manifest.json
flink-sql-manifest --sql-dir ./my-pipeline

# Overwrite an existing manifest
flink-sql-manifest --sql-dir ./my-pipeline --overwrite
```

### From a dbt-confluent project

```bash
flink-sql-manifest --sql-dir ./my-dbt-project --dbt --dry-run
flink-sql-manifest --sql-dir ./my-dbt-project --dbt --overwrite
```

## Manifest Structure

```json
{
  "user_agent": "cc-sql-tools/0.1",
  "groups": [
    {
      "name": "sources",
      "statements": [
        { "name": "ddl.orders", "file": "sql_scripts/ddl.orders.sql", "type": "ddl" }
      ]
    },
    {
      "name": "facts",
      "statements": [
        { "name": "dml.orders_enriched", "file": "sql_scripts/dml.orders_enriched.sql", "type": "pipeline" }
      ]
    }
  ]
}
```

## SQL Classification

Files are classified automatically:

| Prefix / Pattern | Group |
|-----------------|-------|
| `ddl.*` | `ddl` / table definitions |
| `dml.*` | `pipeline` / streaming inserts |
| `data.*` | `data` / seed/static data |
| `scenario.*` | `scenario` / test scenarios |
