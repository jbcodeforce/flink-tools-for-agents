# dbt Migration Tools

Convert Flink SQL DML statements into dbt streaming models, scaffold shift-left dbt projects,
and generate dbt YAML from SQL files or Confluent Schema Registry schemas.

## Entry Points

| Command | Description |
|---------|-------------|
| `flink-sql-migrate-dbt` | Migrate Flink DML to dbt models |
| `sl-dbt` | Scaffold and manage shift-left dbt projects |

## Flink DML → dbt Migration

### Migrate a single file

```bash
# Dry-run (default)
flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./my-dbt-project

# Write output files
flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./my-dbt-project --write

# With explicit materialization and ref mapping
flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./my-dbt-project \
  --materialized streaming_table \
  --ref-table raw_orders=stg_orders \
  --write
```

### Migrate an entire shift-left pipeline folder

```bash
flink-sql-migrate-dbt migrate-sl-folder ./pipelines/facts/orders ./my-dbt-project --write --force
```

### Validate the migration

```bash
flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./my-dbt-project --check \
  --dbt-project-dir ./my-dbt-project \
  --dbt-target dev
```

## dbt Project Scaffolding

### Initialize a new project

```bash
sl-dbt init ./my-project --type data-product --profile cc_flink
```

### Add a data product

```bash
sl-dbt add-data-product ./my-project orders_domain
```

### Add a table to a data product

```bash
sl-dbt add-table ./my-project orders orders_domain --table-type fact
sl-dbt add-table ./my-project raw_orders orders_domain --table-type source
sl-dbt add-table ./my-project dim_customers orders_domain --table-type dimension
```

## YAML Generation

### From a dbt SQL model file

```bash
uv run python -m tools.dbt.sql_to_dbt_yaml model.sql --project-root ./my-project --yaml
```

### From a Confluent Schema Registry topic

```bash
uv run python -m tools.dbt.sr_to_dbt_yaml orders_topic \
  --output sources \
  --sr-url https://psrc-xxx.region.aws.confluent.cloud
```
