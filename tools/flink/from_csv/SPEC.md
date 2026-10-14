# SPEC.md: CSV → Avro → Kafka Producer & Flink Snapshot Query Generator

## 1. Overview
This specification defines the architecture, data processing rules, and output artifacts for a tool that:
1. Reads a CSV file (first row = column headers)
2. Infers column types and generates a valid Avro schema
3. Auto-generates a Python script to serialize CSV rows into Avro and produce them to a Kafka topic
4. Provides the corresponding Confluent Cloud Flink SQL syntax for point-in-time (snapshot) queries with filters

---

## 2. Input & Output Specifications

### 2.1 Input
- **File**: `.csv`
- **Format**: Standard CSV (RFC 4180 compliant)
- **Header**: First row contains column names (alphanumeric, underscores, no spaces)
- **Data**: Row-based values. Tool must handle empty fields, quoted strings, and numeric/date formats.

### 2.2 Outputs
| Artifact | Format | Description |
|----------|--------|-------------|
| `schema.avsc` | JSON | Avro schema definition |
| `producer.py` | Python 3 | Auto-generated Kafka producer script |
| `flink_snapshot_query.sql` | SQL | Flink SQL snapshot query template |
| `config.env` | INI/ENV | Runtime configuration placeholders |

---

## 3. Core Workflow
```
CSV File → Header Parsing → Type Inference → Avro Schema → Python Code Generator → Kafka Topic → Flink SQL Snapshot Template
```

---

## 4. Avro Schema Generation Rules

### 4.1 Schema Structure
```json
{
  "type": "record",
  "name": "<derived_from_topic_or_custom>",
  "namespace": "<custom_namespace_or_derived>",
  "fields": [
    { "name": "col1", "type": "type1" },
    ...
  ]
}
```

### 4.2 Type Inference Heuristic
For each column, scan a sample of values (default: first 100 non-null rows) and apply:
| Inferred Pattern | Avro Type |
|------------------|-----------|
| Matches `^\d+$` | `int` |
| Matches `^\d+\.\d+$` | `float` |
| Matches ISO date/time or common formats | `string` (or `date`/`timestamp` if specified) |
| `true`/`false` (case-insensitive) | `boolean` |
| Contains commas, quotes, or mixed formats | `string` |
| All values empty | `string` (optional: `null` union) |

**Fallback**: Default to `string` if inference fails or data is heterogeneous.

### 4.3 Schema Validation
- Must pass `avro.BlockIO.validate()` equivalent
- Field names must be valid Avro identifiers
- No duplicate field names
- Optional: Add `default` for nullable fields if required by schema evolution strategy

---

## 5. Python Producer Code Generation

### 5.1 Required Dependencies
```txt
confluent-kafka>=2.3.0
avro-python3>=0.132.0
pandas>=2.0.0  # optional, for CSV parsing
```

### 5.2 Generated Script Structure
```python
#!/usr/bin/env python3
"""Auto-generated Kafka Avro Producer from CSV"""
import os
import csv
import json
import avro
import avro.schema
from confluent_kafka import Producer, KafkaError, Errordict

# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "your-cluster.confluent.io:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "csv-ingestion-topic")
SCHEMA_FILE = os.getenv("SCHEMA_PATH", "schema.avsc")

# Load Schema
with open(SCHEMA_FILE, "r") as f:
    schema = avro.schema.Schema.parse(json.load(f))
SYSTEM = avro.schema.RelECTOR(schema)

def deserialize_avro(value: str, schema: avro.schema.Schema) -> bytes:
    decoder = avro.io.Decoder(schema)
    return avro.io.encode(Map(value, decoder), **dict(reject_unknown_keys=True))

def main():
    conf = {"bootstrap.servers": KAFKA_BOOTSTRAP}
    p = Producer(conf)

    def delivery_report(err, msg):
        if err:
            print(f"Kafka Error: {err}")
        else:
            print(f"Record {msg.topic()} delivered to {msg.offset()}")

    with open(os.getenv("CSV_INPUT_FILE", "input.csv"), "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                payload = deserialize_avro(json.dumps(row), schema)
                p.produce(
                    TOPIC_NAME,
                    key=row.get("id"),
                    value=payload,
                    callback=delivery_report
                )
            except Exception as e:
                print(f"Row error: {e}")

    p.flush()
    print("Producer finished.")

if __name__ == "__main__":
    main()
```

### 5.3 Generation Notes
- Replace placeholders with actual inferred types if strongly typed Avro is preferred
- Include JSON serialization bridge for dynamic CSV → Avro mapping
- Add retry logic & backoff for Kafka producer (optional but recommended)
- Store schema inline or load from Schema Registry (specify `DefaultURI` vs inline)

---

## 6. Confluent Cloud Flink SQL Snapshot Query

### 6.1 Prerequisites
- Flink cluster provisioned in Confluent Cloud
- Kafka table registered in Flink SQL environment:
```sql
CREATE TABLE kafka_input_table (
    col1 INT,
    col2 STRING,
    col3 TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'csv-ingestion-topic',
    'properties.bootstrap.servers' = 'cluster.confluent.io:9092',
    'properties.consumer.group.id' = 'flink-job-group',
    'table.resolve-field-name-sensitivity' = 'TRUE'
);
```

### 6.2 Snapshot Query Syntax
Flink SQL supports point-in-time (snapshot) queries on Kafka tables using `AS OF SYSTEM TIME`:
```sql
SELECT * 
FROM kafka_input_table
WHERE col2 = 'target_value' 
  AND col1 > 100
AS OF SYSTEM TIME '2024-12-01 12:00:00';
```

### 6.3 Filter & Snapshot Rules
- `AS OF SYSTEM TIME` requires the query to run at or after the specified timestamp
- Filters can reference any column in the table
- For low-cardinality filters, Flink may optimize via index scans if using sink-specific index (advanced)
- To query exactly-at-once or exactly-once snapshot, ensure Flink checkpointing is enabled in the jobmanager

---

## 7. Configuration & Dependencies

### 7.1 Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
| `CSV_INPUT_FILE` | Path to CSV file | `input.csv` |
| `KAFKA_BOOTSTRAP` | Kafka cluster bootstrap | `localhost:9092` |
| `KAFKA_TOPIC` | Target topic | `csv-stream` |
| `SCHEMA_NAMESPACE` | Avro namespace | `com.example.ingestion` |
| `FLINK_SNAPSHOT_TIME` | Snapshot timestamp | `SYSTEM_TIME` |

### 7.2 Required Python Packages
```txt
confluent-kafka
avro-python3
python-dateutil  # for flexible date parsing
pandas  # optional CSV handling
```

---

## 8. Example Workflow

### Input CSV (`users.csv`)
```csv
user_id,name,email,signup_date,active
1,Alice,alice@example.com,2023-01-15,true
2,Bob,bob@example.com,2023-02-20,false
3,Charlie,charlie@example.com,2023-03-05,true
```

### Generated Avro Schema (`schema.avsc`)
```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example.users",
  "fields": [
    { "name": "user_id", "type": "int" },
    { "name": "name", "type": "string" },
    { "name": "email", "type": "string" },
    { "name": "signup_date", "type": "string" },
    { "name": "active", "type": "boolean" }
  ]
}
```

### Flink Snapshot Query
```sql
SELECT user_id, name, email 
FROM kafka_input_table 
WHERE active = TRUE 
  AND signup_date >= '2023-01-01'
AS OF SYSTEM TIME '2023-04-01 00:00:00';
```

---

## 9. Error Handling & Validation

| Scenario | Handling Strategy |
|----------|-------------------|
| CSV encoding issues | Use `utf-8-sig` or detect BOM; fallback to `latin-1` |
| Type mismatch / heterogeneous column | Log warning, coerce to `string`, skip row with identifier |
| Kafka connection failure | Exponential backoff, retry 3x, fail gracefully with error report |
| Avro schema invalid | Block generation, output validation errors to `schema_errors.log` |
| Flink snapshot timestamp too future | Return empty result; document limitation |

---

## 10. Constraints & Notes
- Tool assumes CSV data fits in memory for type inference. For large files, use streaming sample-based inference.
- Avro schema evolution is out-of-scope; schema is static per generation run.
- Flink snapshot queries require `flink-catalog-connector-kafka` with compatible version.
- Python producer does not include Schema Registry lookup; use inline schema or extend `JSONDecoder` with registry client if needed.
- For production, add secret management (AWS Secrets Manager, HashiCorp Vault, or Confluent Cloud OAuth).

---
**Version**: 1.0  
**Status**: Draft / Ready for Implementation  
**Maintainer**: [Your Team/Name]  
**Last Updated**: 2024-06-15  

*End of Specification*