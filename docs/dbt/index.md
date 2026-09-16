# dbt Migration Tools


Tools to convert Flink SQL DML statements into dbt streaming models, scaffold shift-left dbt projects,
and generate dbt YAML from SQL files or Confluent Schema Registry schemas.

## Problem Statement

The classical use case for Confluent Cloud Flink development is to start writing Flink SQL in the Confluent Workspace, iterating incrementally, and then committing the SQL to a git repository. Adopting dbt requires developers to manually refactor that Flink SQL into dbt template syntax, update YAML files, and validate the translation — all of which is error-prone and time-consuming.

## Entry Points

| Command | Description |
|---------|-------------|
| `flink-sql-migrate-dbt` | Migrate Flink DML to dbt models |
| `sl-dbt` | Scaffold and manage shift-left dbt projects |

## Using with Agentic harness

Once you have installed the skills for your harness, you can use the following prompts, in one session of your flink project repository:


???+ info "using /dbt-project create a dbt project under @tmp folder. name it db-out use data as a product structure"
    Equivalent to the call to:
    ```sh
    uv run sl-dbt init ./tools/dbt/tmp/db-out --type data-product --profile cc_flink
    ```

    This will create the following folders:
    ```sh
    db-out
    ├── docs
    ├── IaC
    ├── pipelines
    │   ├── dbt_project.yml
    │   ├── macros
    │   ├── models
    │   ├── seeds
    │   └── tests
    ├── sl_dbt.yaml
    └── tools
    ```
    
???+ info  "add a data analytics product named crm in this project"
    Same as:
    ```sh
    uv run sl-dbt add-data-product ./tools/dbt/tmp/db-out crm
    ```

    Now the pipelines has:
    ```sh
    ├── pipelines
    │   ├── dbt_project.yml
    │   ├── macros
    │   ├── models
    │   │   └── crm
    │   │       ├── dimensions
    │   │       ├── facts
    │   │       └── sources
    ```


???+ info "add a table to deduplicate raw customer in sources, name it: src_customers for the crm data product"
    Same as:
    ```sh
    uv run sl-dbt add-table ./tools/dbt/tmp/db-out src_customers crm --table-type source
    ```

    Which adds a table in the models:
    ```sh
    │   ├── models
    │   │   └── crm
    │   │       ├── dimensions
    │   │       ├── facts
    │   │       ├── schema.yml
    │   │       └── sources
    │   │           ├── src_customers.sql
    │   │           └── src_customers.yml
    ```

## Flink DML → dbt Migration

This section is to present the commands for the classical data engineer's use cases.


### Migrate a single file

```bash
# Dry-run (default)
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./my-dbt-project

# Write output files
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./my-dbt-project --write

# With explicit materialization and ref mapping
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./my-dbt-project \
  --materialized streaming_table \
  --ref-table raw_orders=stg_orders \
  --write
```

### Migrate an entire shift-left pipeline folder

```bash
uv run flink-sql-migrate-dbt migrate-sl-folder ./pipelines/facts/orders ./my-dbt-project --write --force

# Scope run to one product subtree (only tables under .../aqem/...)
uv run flink-sql-migrate-dbt migrate-sl-folder ./pipelines ./my-dbt-project --product aqem --write

# With an exclusion file (one folder path per line)
uv run flink-sql-migrate-dbt migrate-sl-folder ./pipelines ./my-dbt-project --exclude-file ./excluded_folders.txt --write
```

#### Resume behaviour — `tracking.yml`

`migrate-sl-folder` writes a `tracking.yml` file to the dbt project root on every `--write`
run. It records each table's DML SHA-256, migration status (`done` / `failed` / `skipped`),
and an optional error message.

A table is **skipped** when it is already marked `done` and its DML SHA hasn't changed — so
large migrations can be safely interrupted and resumed without re-processing completed tables.
Pass `--force` to override the skip logic and re-migrate all tables regardless of status.

### Validate the migration

```bash
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./my-dbt-project --check \
  --dbt-project-dir ./my-dbt-project \
  --dbt-target dev
```

## dbt Project Scaffolding

### Initialize a new project

```bash
uv run sl-dbt init ./my-project --type data-product --profile cc_flink
```

### Add a data product

```bash
uv run sl-dbt add-data-product ./my-project orders_domain
```

### Add a table to a data product

```bash
uv run sl-dbt add-table ./my-project orders orders_domain --table-type fact
uv run sl-dbt add-table ./my-project raw_orders orders_domain --table-type source
uv run sl-dbt add-table ./my-project dim_customers orders_domain --table-type dimension
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
