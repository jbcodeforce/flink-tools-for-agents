---
name: dbt-project
description: Use when the user wants to migrate Flink SQL DML statements to dbt streaming models, scaffold a new shift-left dbt project, add data products or tables to an existing dbt project, or generate dbt YAML from SQL files or Confluent Schema Registry schemas.
---

# dbt Flink SQL Migration and dbt project Scaffolding Skill

This skill guides you through converting Flink SQL into dbt streaming models, and scaffolding
or managing shift-left dbt projects, using the CLI tools from **flink-tools-for-agents**.

## Tools covered

| Entry point | What it does |
|-------------|--------------|
| `flink-sql-migrate-dbt migrate-one-file` | Migrate a single Flink DML file to a dbt model |
| `flink-sql-migrate-dbt migrate-sl-folder` | Migrate an entire shift-left pipeline folder |
| `sl-dbt init` | Scaffold a new dbt streaming project |
| `sl-dbt add-data-product` | Add a data product subdirectory |
| `sl-dbt add-table` | Add a table (model/seed/source) to a data product |

## Migrate a single Flink DML file

### Step 1 — Dry-run (always start here)

```bash
uv run flink-sql-migrate-dbt migrate-one-file \
  path/to/dml.orders.sql \
  ./my-dbt-project/models/orders
```

This prints the generated model SQL, `schema.yml`, and `sources.yml` to stdout without writing
any files. Inspect the output carefully.

### Step 2 — Customize the migration (optional)

```bash
# Override the materialization
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./models/orders \
  --materialized streaming_table

# Map an upstream table to a dbt ref
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./models/orders \
  --ref-table raw_orders=stg_orders

# Specify an upstream DDL file explicitly
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./models/orders \
  --ddl-file ddl.orders.sql

# Suppress source generation (use {{ ref() }} only)
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./models/orders \
  --no-sources
```

### Step 3 — Write the files

```bash
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./models/orders \
  --dbt-project-dir ./my-dbt-project \
  --write

# Overwrite existing files
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./models/orders \
  --dbt-project-dir ./my-dbt-project \
  --write --force
```

### Step 4 — Validate the migration (optional)

Validates by compiling with dbt and comparing the compiled SQL to the original DML:

```bash
uv run flink-sql-migrate-dbt migrate-one-file dml.orders.sql ./models/orders \
  --dbt-project-dir ./my-dbt-project \
  --dbt-target dev \
  --check
```

## Migrate an entire shift-left pipeline folder

```bash
# Dry-run entire pipeline
uv run flink-sql-migrate-dbt migrate-sl-folder ./pipelines ./my-dbt-project

# Write all migrations
uv run flink-sql-migrate-dbt migrate-sl-folder ./pipelines ./my-dbt-project --write

# Overwrite existing files
uv run flink-sql-migrate-dbt migrate-sl-folder ./pipelines ./my-dbt-project --write --force

# Scope run to one product subtree (e.g. only tables under pipelines/.../aqem/...)
uv run flink-sql-migrate-dbt migrate-sl-folder ./pipelines ./my-dbt-project --product aqem --write

# Exclude folders listed in a text file (one folder path per line)
uv run flink-sql-migrate-dbt migrate-sl-folder ./pipelines ./my-dbt-project --exclude-file ./excluded_folders.txt --write
```

The tool discovers all `sql-scripts/dml.*.sql` files under the pipeline folder, infers their
DDL counterparts, and emits dbt models + seeds + schema files.

### Resume behaviour — `tracking.yml`

On every `--write` run, `migrate-sl-folder` maintains a `tracking.yml` file in the dbt project
root. Each entry records the table name, its DML SHA-256, migration status (`done` / `failed` /
`skipped`), and an optional error message.

**Skip logic:** a table is skipped when its entry in `tracking.yml` is already `done` *and* the
DML SHA hasn't changed since. This allows large migrations to be restarted safely after
interruptions — only new or changed tables are processed.

**Force re-migration:** pass `--force` to ignore the skip logic and re-migrate all tables
regardless of their tracked status.

```yaml
# tracking.yml — auto-generated; do not edit manually
tables:
  sl_fct_order:
    relative_path: facts/aqem/fct_order
    dml_sha256: "abc123..."
    status: done          # done | failed | skipped
    error: null
    migrated_at: "2025-07-10T14:23:01"
  sl_int_payments:
    relative_path: intermediates/aqem/int_payments
    dml_sha256: "def456..."
    status: failed
    error: "DDL not found for table sl_raw_payments"
    migrated_at: "2025-07-10T14:23:05"
```

## Scaffold a new dbt streaming project

### Initialize a project

```bash
uv run sl-dbt init ./my-project --type data-product --profile cc_flink
```

This creates:
- `dbt_project.yml` with streaming defaults
- `sl_dbt.yaml` with project metadata
- Standard model directories: `models/`, `seeds/`, `tests/`, `macros/`
- `.gitignore`

### Add a data product

```bash
uv run sl-dbt add-data-product ./my-project orders_domain
```

### Add a table to a data product

```bash
# Source table
uv run sl-dbt add-table ./my-project raw_orders orders_domain --table-type source

# Dimension
uv run sl-dbt add-table ./my-project dim_customers orders_domain --table-type dimension

# Fact
uv run sl-dbt add-table ./my-project fct_order_summary orders_domain --table-type fact
```

## Generate dbt YAML from Schema Registry

```bash
# sources: block for models/sources.yaml
uv run python -m tools.dbt.sr_to_dbt_yaml orders_topic \
  --output sources \
  --sr-url https://psrc-xxx.region.aws.confluent.cloud

# models: block for a staging model
uv run python -m tools.dbt.sr_to_dbt_yaml orders_topic \
  --output model \
  --schema-name stg_orders
```

Required env vars:
```bash
SCHEMA_REGISTRY_URL=<url>
SCHEMA_REGISTRY_API_KEY=<key>
SCHEMA_REGISTRY_API_SECRET=<secret>
```

## Common patterns

### Full pipeline migration workflow

```bash
# 1. Scaffold project
uv run sl-dbt init ./orders-dbt --type data-product

# 2. Migrate pipeline folder
uv run flink-sql-migrate-dbt migrate-sl-folder ./pipelines/orders ./orders-dbt --write

# 3. Compile and validate
cd ./orders-dbt && dbt compile && cd ..
uv run flink-sql-migrate-dbt migrate-sl-folder ./pipelines/orders ./orders-dbt --check
```

## Troubleshooting

- **"No DDL file found"** — pass `--ddl-file` explicitly or ensure a matching `ddl.<table>.sql`
  exists alongside the DML file.
- **"Statement file not found"** — verify the path to the DML `.sql` file is correct.
- **dbt compile fails** — check that the generated model SQL uses valid Jinja (`{{ ref() }}`,
  `{{ source() }}`) and that the referenced models exist in the dbt project.
