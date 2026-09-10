# Kafka & Schema Registry Tools

Manage Kafka topics, register schemas in Confluent Schema Registry, and clean up
Flink tables on Confluent Cloud.

## Entry Points

| Command | Description |
|---------|-------------|
| `flink-sql-register` | Register, list, or delete schemas in Schema Registry |
| `flink-sql-cleanup` | List Kafka topics / drop Flink tables |

## Required Environment Variables

```bash
# Schema Registry
SCHEMA_REGISTRY_URL=<url>
SCHEMA_REGISTRY_API_KEY=<key>
SCHEMA_REGISTRY_API_SECRET=<secret>

# Kafka
KAFKA_BOOTSTRAP_SERVERS=<brokers>
KAFKA_API_KEY=<key>
KAFKA_API_SECRET=<secret>
```

## Schema Registry

### Register a schema

```bash
# Avro schema (type inferred from file extension)
flink-sql-register register ./schemas/orders.avsc

# JSON schema with explicit subject
flink-sql-register register ./schemas/orders.json --subject orders-value --type JSON
```

### List registered schemas

```bash
flink-sql-register list
flink-sql-register list --output schema-manifest.json
```

### Delete schemas

```bash
# Soft delete (from manifest)
flink-sql-register delete --manifest schema-manifest.json --dry-run
flink-sql-register delete --manifest schema-manifest.json

# Permanent delete (resolves references first)
flink-sql-register delete --manifest schema-manifest.json --permanent --resolve-references
```

### Debug reference errors

```bash
flink-sql-register debug-refs orders-value
```

## Table Cleanup

### List topics / Flink tables

```bash
flink-sql-cleanup list
flink-sql-cleanup list --output drop_manifest.json --include-internal
```

### Drop Flink tables

```bash
# Edit drop_manifest.json to set "drop": true for the tables you want to remove, then:
flink-sql-cleanup drop --manifest drop_manifest.json --dry-run
flink-sql-cleanup drop --manifest drop_manifest.json
```
