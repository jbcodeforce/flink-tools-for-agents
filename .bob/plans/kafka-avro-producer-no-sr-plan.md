# Plan: KafkaAvroProducer — Schemaless Avro Mode (no Schema Registry)

## Overview

`KafkaAvroProducer` in `tools/kafka/cm_lib/kafka_avro_producer.py` already has a `use_schema_registry: bool` flag, but the `else` branch in `send_record` is broken (typo `instanceOf`, incomplete `value_bytes`).

This plan fixes and completes the no-SR path so that:
- The Avro schema is always sourced from `.avsc` files via `specify_models_from_paths` (both SR and no-SR paths)
- A `namespace` parameter is added to `specify_models_from_paths` for use by `py_avro_schema.generate`
- Values are serialized with `fastavro.schemaless_writer` (no Confluent wire-format header) when no SR
- String keys are encoded as UTF-8 bytes; `BaseModel` keys use fastavro schemaless encoding too
- The existing SR path (`_install_serializers` / `AvroSerializer`) is unchanged

**Caller usage:**
```python
# No Schema Registry
producer = KafkaAvroProducer("raw_customers", use_schema_registry=False)
producer.specify_models_from_paths(key_schema_path, value_schema_path, namespace="com.example")
producer.send_record(current_key="C001", current_value=aboat)

# With Schema Registry (unchanged)
producer = KafkaAvroProducer("raw_customers", use_schema_registry=True)
producer.specify_models_from_paths(key_schema_path, value_schema_path)
producer.send_record(current_key=key_obj, current_value=value_obj)
```

---

## Sub-Task 1 — Extend `specify_models_from_paths` to support the no-SR path

**Intent**  
`specify_models_from_paths` currently only calls `_install_serializers` (SR path).
It needs to also build `_value_parsed_schema` and `_key_parsed_schema` when
`use_schema_registry=False`, using `fastavro.parse_schema` on the loaded `.avsc` content.
A `namespace` parameter is added so callers can pass it through to `py_avro_schema.generate`
if needed — but since schemas come from `.avsc` files (already full JSON), the namespace is
recorded on `self` for informational use only; `parse_schema` takes the dict directly.

**Expected Outcomes**
- `specify_models_from_paths(key_path, value_path, namespace="com.example")` works for both modes
- After the call with `use_schema_registry=False`, `self._value_parsed_schema` and
  `self._key_parsed_schema` are populated (fastavro parsed schema dicts)
- After the call with `use_schema_registry=True`, `self.key_serializer` and
  `self.value_serializer` are set as before (no regression)

**Todo List**
1. Add `namespace: str = ""` param to `specify_models_from_paths`
2. Store `self.namespace = namespace` on the instance
3. After reading both schema strings, branch on `self.use_schema_registry`:
   - `True` → call `_install_serializers` as before
   - `False` → call new private method `_install_parsed_schemas(key_schema_str, value_schema_str)`
4. Implement `_install_parsed_schemas(key_schema_str, value_schema_str)`:
   - `json.loads` each string → `parse_schema(...)` → assign to `self._key_parsed_schema` and `self._value_parsed_schema`
5. Add `self._key_parsed_schema = None` and `self._value_parsed_schema = None` to `__init__` as initial defaults
6. Add `import json` and `import io` to top-level imports (fastavro already imported)

**Relevant Context**
- File: `tools/kafka/cm_lib/kafka_avro_producer.py`
- `specify_models_from_paths` at lines 62–68; `_install_serializers` at lines 71–81
- `fastavro.parse_schema` already imported at line 24
- Pattern: `parse_schema(json.loads(schema_str))` — `schema_str` is the raw `.avsc` file content

**Status:** [ ] pending

---

## Sub-Task 2 — Fix `send_record` no-SR branch

**Intent**  
Replace the broken `else` block with correct fastavro schemaless serialization for both key and value.

**Expected Outcomes**
- String keys produce `key.encode("utf-8")`
- `BaseModel` keys produce fastavro-schemaless bytes using `_key_parsed_schema`
- `BaseModel` values produce fastavro-schemaless bytes using `_value_parsed_schema`
- The SR path (`if self.use_schema_registry`) is untouched

**Todo List**
1. Replace `instanceOf(current_key, BaseModel)` with `isinstance(current_key, BaseModel)`
2. For `BaseModel` keys: use `schemaless_writer` into a `BytesIO`, extract `.getvalue()`
   - Guard: if `self._key_parsed_schema is None`, raise `ValueError("call specify_models_from_paths first")`
3. For string keys: `key_bytes = current_key.encode("utf-8")`
4. For the value: use `schemaless_writer` into a `BytesIO`, extract `.getvalue()` using `self._value_parsed_schema`
   - Guard: if `self._value_parsed_schema is None`, raise `ValueError("call specify_models_from_paths first")`
5. Use `current_value.model_dump()` (Pydantic v2) — not `.__dict__` — for correct field serialization
6. Ensure `schemaless_writer` is included in the fastavro import at line 24

**Relevant Context**
- File: `tools/kafka/cm_lib/kafka_avro_producer.py`, `send_record` at lines 121–151
- fastavro pattern: `schemaless_writer(bytes_io, parsed_schema, record_dict)`
- `schemaless_writer` is already present in the import on line 24

**Status:** [ ] pending

---

## Sub-Task 3 — Update the disabled `_test_avro_producer` test

**Intent**  
The existing `_test_avro_producer` is disabled (leading `_`). Update it to use the
corrected API and re-enable it as a proper pytest test.

**Expected Outcomes**
- `test_avro_producer` runs with the `set_env` fixture against local Kafka
- Uses `specify_models_from_paths` to supply the schema (matching new API)
- `send_record` with a string key and a `Boat` value returns `True`

**Todo List**
1. Rename `_test_avro_producer` → `test_avro_producer`
2. After constructing `KafkaAvroProducer("raw_customers", False)`, call
   `specify_models_from_paths` with a test `.avsc` file path (or note that a
   test schema fixture is needed — see below)
3. Alternatively, if no `.avsc` file is available in tests, generate the schema inline
   with `pas.generate(Boat, ...)`, write it to a temp file via `tmp_path` fixture, and
   pass that path to `specify_models_from_paths`
4. Verify `send_record(current_key="C001", current_value=aboat)` returns `True`

**Relevant Context**
- File: `tools/kafka/tests/test_kafka_client.py`, lines 73–81
- `Boat` model defined at lines 66–70
- `tmp_path` is a built-in pytest fixture for temporary directories

**Status:** [ ] pending

---

## Implementation Notes

- `_install_serializers` (SR path) is not changed — it continues to expect schema strings and the SR client
- No model class params are added to `__init__`; schema always flows through `specify_models_from_paths`
- No changes to `flush_and_close`, `_delivery_report`, `_create_producer`, or `_read_schema`
