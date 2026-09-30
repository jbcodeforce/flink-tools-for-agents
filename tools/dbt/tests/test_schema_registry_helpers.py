"""Unit tests for tools.dbt.schema_registry_helpers.

Also guards against the specific regression this module once had: it imported
SCHEMA_REGISTRY_* constants from a ``cm_py_lib`` package that doesn't exist in
this repo, which meant simply importing this module raised ImportError.
"""

from __future__ import annotations

import pytest

from tools.dbt.schema_registry_helpers import (
    ColumnSpec,
    SchemaFetcher,
    is_debezium_envelope,
    render_model_yaml,
    render_sources_yaml,
    schema_to_columns,
)

REQUIRED_ENV_VARS = [
    "SCHEMA_REGISTRY_ENDPOINT",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_USER",
    "SCHEMA_REGISTRY_API_SECRET",
    "SCHEMA_REGISTRY_PASSWORD",
]


@pytest.fixture(autouse=True)
def _clean_sr_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure no real Schema Registry credentials leak into these tests."""
    for var in REQUIRED_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# SchemaFetcher construction / env var resolution
# ---------------------------------------------------------------------------


def test_schema_fetcher_requires_url_when_none_available() -> None:
    with pytest.raises(ValueError, match="Schema Registry URL is required"):
        SchemaFetcher()


def test_schema_fetcher_accepts_explicit_url() -> None:
    fetcher = SchemaFetcher(url="https://sr.example.com")
    assert fetcher is not None


def test_schema_fetcher_reads_canonical_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCHEMA_REGISTRY_ENDPOINT", "https://sr.example.com")
    monkeypatch.setenv("SCHEMA_REGISTRY_API_KEY", "key")
    monkeypatch.setenv("SCHEMA_REGISTRY_API_SECRET", "secret")
    SchemaFetcher()  # must not raise


def test_schema_fetcher_reads_legacy_env_var_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "https://sr.example.com")
    monkeypatch.setenv("SCHEMA_REGISTRY_USER", "key")
    monkeypatch.setenv("SCHEMA_REGISTRY_PASSWORD", "secret")
    SchemaFetcher()  # must not raise


def test_schema_fetcher_explicit_args_override_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCHEMA_REGISTRY_ENDPOINT", "https://env.example.com")
    fetcher = SchemaFetcher(url="https://explicit.example.com")
    assert fetcher is not None


# ---------------------------------------------------------------------------
# schema_to_columns
# ---------------------------------------------------------------------------


def test_schema_to_columns_avro() -> None:
    schema = {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "notes", "type": ["null", "string"]},
        ],
    }
    columns = schema_to_columns(schema, "AVRO")
    assert columns == [
        ColumnSpec(name="order_id", data_type="string"),
        ColumnSpec(name="amount", data_type="double"),
        ColumnSpec(name="created_at", data_type="timestamp(3)"),
        ColumnSpec(name="notes", data_type="string"),
    ]


def test_schema_to_columns_json() -> None:
    schema = {
        "properties": {
            "order_id": {"type": "string"},
            "amount": {"type": "number"},
            "created_at": {"type": "string", "format": "date-time"},
        }
    }
    columns = schema_to_columns(schema, "JSON")
    assert columns == [
        ColumnSpec(name="order_id", data_type="string"),
        ColumnSpec(name="amount", data_type="double"),
        ColumnSpec(name="created_at", data_type="timestamp(3)"),
    ]


def test_schema_to_columns_protobuf_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        schema_to_columns({}, "PROTOBUF")


def test_schema_to_columns_unknown_type_raises() -> None:
    with pytest.raises(ValueError, match="Unknown schema_type"):
        schema_to_columns({}, "XML")


# ---------------------------------------------------------------------------
# Debezium CDC envelope handling (Avro)
# ---------------------------------------------------------------------------

# Realistic shape: "before" fully defines the shared row record ("Value"),
# "after" references it back by name only (Avro's named-type-reuse shorthand)
# — this is exactly what Confluent's Debezium CDC connectors emit.
_DEBEZIUM_AVRO_ENVELOPE = {
    "type": "record",
    "name": "Envelope",
    "namespace": "io.debezium.connector.postgresql.orders",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Value",
                    "namespace": "io.debezium.connector.postgresql.orders",
                    "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "amount", "type": "double"},
                        {
                            "name": "address",
                            "type": [
                                "null",
                                {
                                    "type": "record",
                                    "name": "Address",
                                    "fields": [
                                        {"name": "street", "type": "string"},
                                        {"name": "city", "type": "string"},
                                    ],
                                },
                            ],
                            "default": None,
                        },
                    ],
                },
            ],
            "default": None,
        },
        {
            "name": "after",
            "type": ["null", "io.debezium.connector.postgresql.orders.Value"],
            "default": None,
        },
        {
            "name": "source",
            "type": {"type": "record", "name": "Source", "fields": [{"name": "table", "type": "string"}]},
        },
        {"name": "op", "type": "string"},
        {"name": "ts_ms", "type": ["null", "long"], "default": None},
    ],
}


def test_is_debezium_envelope_detects_avro() -> None:
    assert is_debezium_envelope(_DEBEZIUM_AVRO_ENVELOPE, "AVRO") is True


def test_is_debezium_envelope_false_for_plain_schema() -> None:
    plain = {"type": "record", "name": "Order", "fields": [{"name": "id", "type": "string"}]}
    assert is_debezium_envelope(plain, "AVRO") is False


def test_schema_to_columns_unwraps_debezium_after_and_resolves_named_type() -> None:
    columns = schema_to_columns(_DEBEZIUM_AVRO_ENVELOPE, "AVRO")
    names = [c.name for c in columns]
    # Columns come from 'after' (resolved via the 'Value' name reference
    # defined inside 'before'), not from the envelope's own before/after/
    # source/op/ts_ms fields.
    assert names == ["id", "amount", "address"]


def test_schema_to_columns_debezium_after_nested_record_is_hierarchical_row() -> None:
    columns = schema_to_columns(_DEBEZIUM_AVRO_ENVELOPE, "AVRO")
    address_col = next(c for c in columns if c.name == "address")
    assert address_col.data_type == "row<street string, city string>"


def test_avro_nested_record_outside_cdc_is_also_hierarchical() -> None:
    """Nested-record → row<...> resolution isn't CDC-specific."""
    schema = {
        "type": "record",
        "name": "Customer",
        "fields": [
            {"name": "id", "type": "string"},
            {
                "name": "address",
                "type": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                        {"name": "street", "type": "string"},
                        {"name": "zip", "type": "int"},
                    ],
                },
            },
        ],
    }
    columns = schema_to_columns(schema, "AVRO")
    address_col = next(c for c in columns if c.name == "address")
    assert address_col.data_type == "row<street string, zip int>"


def test_avro_array_of_records_is_array_of_row() -> None:
    schema = {
        "type": "record",
        "name": "Order",
        "fields": [
            {
                "name": "items",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "Item",
                        "fields": [{"name": "sku", "type": "string"}, {"name": "qty", "type": "int"}],
                    },
                },
            }
        ],
    }
    columns = schema_to_columns(schema, "AVRO")
    assert columns[0].data_type == "array<row<sku string, qty int>>"


# ---------------------------------------------------------------------------
# Debezium CDC envelope handling (JSON Schema)
# ---------------------------------------------------------------------------

_DEBEZIUM_JSON_ENVELOPE = {
    "properties": {
        "before": {"type": ["object", "null"], "properties": {"id": {"type": "string"}}},
        "after": {
            "type": ["object", "null"],
            "properties": {
                "id": {"type": "string"},
                "amount": {"type": "number"},
                "address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"},
                    },
                },
            },
        },
        "source": {"type": "object", "properties": {"table": {"type": "string"}}},
        "op": {"type": "string"},
    }
}


def test_is_debezium_envelope_detects_json() -> None:
    assert is_debezium_envelope(_DEBEZIUM_JSON_ENVELOPE, "JSON") is True


def test_schema_to_columns_unwraps_debezium_after_json() -> None:
    columns = schema_to_columns(_DEBEZIUM_JSON_ENVELOPE, "JSON")
    names = [c.name for c in columns]
    assert names == ["id", "amount", "address"]
    address_col = next(c for c in columns if c.name == "address")
    assert address_col.data_type == "row<street string, city string>"


# ---------------------------------------------------------------------------
# render_sources_yaml / render_model_yaml
# ---------------------------------------------------------------------------


def test_render_sources_yaml_shape() -> None:
    columns = [ColumnSpec(name="id", data_type="string")]
    yaml_text = render_sources_yaml("orders_topic", "my_schema", columns)
    assert "sources:" in yaml_text
    assert "name: orders_topic" in yaml_text
    assert "schema: my_schema" in yaml_text
    assert "enforced: true" in yaml_text


def test_render_model_yaml_shape() -> None:
    columns = [ColumnSpec(name="id", data_type="string")]
    yaml_text = render_model_yaml("stg_orders", columns)
    assert "models:" in yaml_text
    assert "name: stg_orders" in yaml_text
    assert "enforced: false" in yaml_text
