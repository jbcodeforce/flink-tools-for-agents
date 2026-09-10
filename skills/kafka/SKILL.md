---
name: kafka
description: Use when the user wants to manage Confluent Schema Registry schemas (register, list, delete, debug references) or manage Kafka topics and Flink tables (list topics, build a drop manifest, drop tables) using the flink-tools-for-agents CLI tools.
---

# Kafka & Schema Registry Skill

This skill guides you through managing Confluent Schema Registry schemas and Kafka/Flink table
lifecycle using the `flink-sql-register` and `flink-sql-cleanup` CLI tools from
**flink-tools-for-agents**.

## Prerequisites

Set the following environment variables (or place them in `.env`):

```bash
# Schema Registry
SCHEMA_REGISTRY_URL=<url>              # e.g. https://psrc-xxx.region.aws.confluent.cloud
SCHEMA_REGISTRY_API_KEY=<key>
SCHEMA_REGISTRY_API_SECRET=<secret>

# Kafka (for table cleanup)
KAFKA_BOOTSTRAP_SERVERS=<brokers>     # e.g. pkc-xxx.region.aws.confluent.cloud:9092
KAFKA_API_KEY=<key>
KAFKA_API_SECRET=<secret>
```

## Schema Registry — Register schemas

### Register an Avro schema

```bash
# Type inferred from .avsc extension
uv run flink-sql-register register ./schemas/orders.avsc

# Explicit subject name
uv run flink-sql-register register ./schemas/orders.avsc --subject orders-value

# JSON schema
uv run flink-sql-register register ./schemas/orders.json --type JSON
```

### List registered schemas

```bash
# Print to stdout
uv run flink-sql-register list

# Save to a schema manifest file for later deletion
uv run flink-sql-register list --output schema-manifest.json
```

### Delete schemas

Always use `--dry-run` first:

```bash
# Soft delete (mark as deleted, recoverable)
uv run flink-sql-register delete --manifest schema-manifest.json --dry-run
uv run flink-sql-register delete --manifest schema-manifest.json

# Permanent delete (purges from Schema Registry — irreversible)
uv run flink-sql-register delete --manifest schema-manifest.json --permanent --dry-run
uv run flink-sql-register delete --manifest schema-manifest.json --permanent

# Permanent delete, resolving schema references first
uv run flink-sql-register delete --manifest schema-manifest.json \
  --permanent --resolve-references
```

### Debug schema reference errors (error 42206)

```bash
uv run flink-sql-register debug-refs orders-value
```

## Kafka / Flink table cleanup

### Step 1 — List Kafka topics / Flink tables

```bash
# Print topic list to stdout
uv run flink-sql-cleanup list

# Save to a drop manifest for editing
uv run flink-sql-cleanup list --output drop_manifest.json

# Include internal topics (hidden by default)
uv run flink-sql-cleanup list --output drop_manifest.json --include-internal

# Add a prefix to all generated Flink statement names
uv run flink-sql-cleanup list --statement-prefix my-demo
```

### Step 2 — Edit the drop manifest

The generated `drop_manifest.json` has `"drop": false` for all topics by default (safety).
Edit it to set `"drop": true` for the topics/tables you want to remove:

```json
{
  "drop_tables": [
    { "topic": "orders", "drop": true },
    { "topic": "customers", "drop": false }
  ]
}
```

### Step 3 — Drop tables

```bash
# Dry-run (no changes)
uv run flink-sql-cleanup drop --manifest drop_manifest.json --dry-run

# Execute
uv run flink-sql-cleanup drop --manifest drop_manifest.json
```

## Common patterns

### Register all schemas before deploying a pipeline

```bash
find ./schemas -name "*.avsc" | while read f; do
  uv run flink-sql-register register "$f"
done
```

### Full cleanup after tearing down a demo

```bash
# 1. List topics to a manifest
uv run flink-sql-cleanup list --output teardown.json

# 2. Mark all pipeline topics for deletion (edit teardown.json)
#    Set "drop": true for the topics you own

# 3. Drop tables
uv run flink-sql-cleanup drop --manifest teardown.json --dry-run
uv run flink-sql-cleanup drop --manifest teardown.json

# 4. Delete schemas
uv run flink-sql-register list --output schema-teardown.json
uv run flink-sql-register delete --manifest schema-teardown.json --dry-run
uv run flink-sql-register delete --manifest schema-teardown.json
```

## Troubleshooting

- **"Schema already registered"** — the exact same schema is already present; no action needed.
- **"Subject not found"** — verify the subject name with `flink-sql-register list`.
- **"Reference not found" (error 42206)** — use `debug-refs <subject>` to identify missing schema
  references before permanent deletion.
- **"Topic not found"** — verify `KAFKA_BOOTSTRAP_SERVERS` is set correctly and the topic exists.
