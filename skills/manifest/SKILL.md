---
name: manifest
description: Use when the user wants to generate a deploy_manifest.json for Flink SQL deployment — covers creating manifests from Flink SQL directories or dbt-confluent projects, understanding manifest structure, or regenerating manifests after SQL changes.
---

# Manifest Generation Skill

This skill guides you through generating a `deploy_manifest.json` using the
`flink-sql-manifest` CLI tool from **flink-tools-for-agents**.

A manifest describes which Flink SQL statements exist, how they are grouped (e.g. `ddl`,
`sources`, `facts`), and in what order they should be deployed and undeployed.

## When to generate a manifest

- First time deploying a new Flink SQL directory
- After adding, removing, or renaming SQL files in a pipeline
- Switching from raw SQL to a dbt-confluent project structure

## Step 1 — Identify the source

**Option A: Raw Flink SQL directory**

The directory should contain SQL files named following the convention:
- `ddl.<table>.sql` — DDL (CREATE TABLE) statements → group `ddl`
- `dml.<table>.sql` — DML (INSERT INTO) statements → group `pipeline`
- `data.<name>.sql` — static data inserts → group `data`
- `scenario.<name>.sql` — test scenarios → group `scenario`

**Option B: dbt-confluent project**

The project must have a compiled `manifest.json` in `target/` (run `dbt compile` first).

## Step 2 — Generate the manifest

```bash
# From a Flink SQL directory (dry-run first)
uv run flink-sql-manifest --sql-dir ./my-pipeline --dry-run

# Write deploy_manifest.json
uv run flink-sql-manifest --sql-dir ./my-pipeline

# Overwrite an existing manifest
uv run flink-sql-manifest --sql-dir ./my-pipeline --overwrite

# With a custom statement-name prefix
uv run flink-sql-manifest --sql-dir ./my-pipeline --prefix my-demo
```

```bash
# From a dbt-confluent project (compile first)
cd ./my-dbt-project && dbt compile
uv run flink-sql-manifest --sql-dir ./my-dbt-project --dbt --dry-run
uv run flink-sql-manifest --sql-dir ./my-dbt-project --dbt --overwrite
```

## Step 3 — Inspect the manifest

Open the generated `deploy_manifest.json` to verify the groups and statement order.

Example structure:
```json
{
  "user_agent": "cc-sql-tools/0.1",
  "groups": [
    {
      "name": "ddl",
      "statements": [
        { "name": "ddl.orders", "file": "sql_scripts/ddl.orders.sql", "type": "ddl" }
      ]
    },
    {
      "name": "pipeline",
      "statements": [
        { "name": "dml.orders_enriched", "file": "sql_scripts/dml.orders_enriched.sql", "type": "pipeline" }
      ]
    }
  ]
}
```

## Step 4 — Use the manifest for deployment

Once the manifest is generated, proceed to deployment:

```bash
uv run flink-sql-deploy --sql-dir ./my-pipeline groups     # verify groups
uv run flink-sql-deploy --sql-dir ./my-pipeline deploy --group ddl
uv run flink-sql-deploy --sql-dir ./my-pipeline deploy --group pipeline
```

## Common patterns

### Re-generate after SQL changes

```bash
uv run flink-sql-manifest --sql-dir ./my-pipeline --overwrite
uv run flink-sql-deploy --sql-dir ./my-pipeline groups
```

### dbt project: compile then manifest

```bash
cd ./my-dbt-project
dbt compile --target prod
cd ..
uv run flink-sql-manifest --sql-dir ./my-dbt-project --dbt --overwrite
```

## Troubleshooting

- **"No SQL files found"** — check that the directory contains `.sql` files with the correct naming convention.
- **"dbt manifest not found"** — run `dbt compile` inside the dbt project first; the tool reads `target/manifest.json`.
- **Groups look wrong** — inspect the SQL filenames; the prefix (`ddl.`, `dml.`, `data.`, `scenario.`) determines grouping.
