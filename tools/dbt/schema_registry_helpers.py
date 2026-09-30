"""
Reusable Schema Registry helpers: fetch schemas and convert them to dbt YAML.

Shared by any script in the repo that needs to inspect Confluent Schema Registry
subjects and produce dbt column definitions.

Three independent layers (each importable on its own):

1. ``SchemaFetcher``   — thin auth wrapper around SchemaRegistryClient
2. ``schema_to_columns`` — pure function: schema dict → list[ColumnSpec]
3. ``render_sources_yaml`` / ``render_model_yaml`` — pure YAML renderers

``schema_to_columns`` auto-detects a Debezium CDC envelope (a schema with
``before``/``after``/``op`` fields) and builds columns from the ``after``
record instead of the envelope itself, since ``after`` — not the envelope —
is the real table's row shape. Nested records/arrays inside it are resolved
recursively into Flink ``row<...>``/``array<...>`` type strings rather than
collapsing to a plain string, so a nested struct still produces a usable,
hierarchical column type. Use :func:`is_debezium_envelope` to detect this
case separately (e.g. to log a note about which schema was actually used).

Environment variables (same names used by ``tools.kafka.register_schema``, plus the
legacy aliases this module originally shipped with):

- ``SCHEMA_REGISTRY_ENDPOINT`` / ``SCHEMA_REGISTRY_URL`` — registry URL
- ``SCHEMA_REGISTRY_API_KEY`` / ``SCHEMA_REGISTRY_USER`` — basic-auth key
- ``SCHEMA_REGISTRY_API_SECRET`` / ``SCHEMA_REGISTRY_PASSWORD`` — basic-auth secret

Usage::

    from tools.dbt.schema_registry_helpers import SchemaFetcher, schema_to_columns, render_sources_yaml

    fetcher = SchemaFetcher()                                    # reads env vars
    schema, schema_type = fetcher.fetch("raw_hosts-value")
    columns = schema_to_columns(schema, schema_type)
    print(render_sources_yaml("raw_hosts", "j9r-kafka", columns))
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import yaml
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError


def _env_first(*names: str) -> str:
    """Return the value of the first set (non-empty) env var, or ''."""
    for name in names:
        value = os.environ.get(name, "")
        if value:
            return value
    return ""


# ── Schema Registry access ────────────────────────────────────────────────────

class SchemaFetcher:
    """Fetch a schema from Confluent Schema Registry by subject name.

    Credentials are resolved in priority order:
      1. Constructor arguments (``url``, ``key``, ``secret``)
      2. Environment variables, read at construction time (not import time) so
         a ``.env`` file loaded just before constructing this object is picked
         up: ``SCHEMA_REGISTRY_ENDPOINT``/``SCHEMA_REGISTRY_API_KEY``/
         ``SCHEMA_REGISTRY_API_SECRET`` (same names as
         ``tools.kafka.register_schema``), falling back to this module's
         legacy ``SCHEMA_REGISTRY_URL``/``SCHEMA_REGISTRY_USER``/
         ``SCHEMA_REGISTRY_PASSWORD`` aliases.

    Args:
        url:    Schema Registry base URL.  Overrides ``SCHEMA_REGISTRY_ENDPOINT``.
        key:    API key for basic auth.    Overrides ``SCHEMA_REGISTRY_API_KEY``.
        secret: API secret for basic auth. Overrides ``SCHEMA_REGISTRY_API_SECRET``.
    """

    def __init__(
        self,
        url: str | None = None,
        key: str | None = None,
        secret: str | None = None,
    ) -> None:
        effective_url = url or _env_first("SCHEMA_REGISTRY_ENDPOINT", "SCHEMA_REGISTRY_URL")
        effective_key = key or _env_first("SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_USER")
        effective_secret = secret or _env_first("SCHEMA_REGISTRY_API_SECRET", "SCHEMA_REGISTRY_PASSWORD")

        if not effective_url:
            raise ValueError(
                "Schema Registry URL is required. Set SCHEMA_REGISTRY_ENDPOINT "
                "or pass --sr-url."
            )

        conf: dict[str, str] = {"url": effective_url}
        if effective_key:
            conf["basic.auth.user.info"] = f"{effective_key}:{effective_secret}"

        self._client = SchemaRegistryClient(conf)

    def fetch(self, subject: str) -> tuple[dict[str, Any], str]:
        """Return ``(schema_dict, schema_type)`` for the latest version of *subject*.

        *schema_type* is one of ``"JSON"``, ``"AVRO"``, or ``"PROTOBUF"``.

        Raises:
            SchemaRegistryError: if the subject does not exist or the request fails.
        """
        try:
            metadata = self._client.get_latest_version(subject)
        except SchemaRegistryError as exc:
            raise SchemaRegistryError(
                exc.error_code,
                f"Subject '{subject}' not found or SR unreachable: {exc}",
            ) from exc

        schema_type: str = metadata.schema.schema_type or "JSON"
        schema_dict: dict[str, Any] = json.loads(metadata.schema.schema_str)
        return schema_dict, schema_type


# ── Type mapping tables ───────────────────────────────────────────────────────

# JSON Schema type (and optional format) → Flink / dbt type.
# Keyed as "type" or "type:format".
_JSON_TO_DBT: dict[str, str] = {
    "string": "string",
    "string:date-time": "timestamp(3)",
    "string:date": "date",
    "string:time": "string",
    "integer": "int",
    "number": "double",
    "boolean": "boolean",
    "array": "array<string>",
    "object": "row<string>",
    "null": "string",
}

# Avro primitive / complex type → Flink / dbt type.
_AVRO_TO_DBT: dict[str, str] = {
    "string": "string",
    "int": "int",
    "long": "bigint",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "bytes": "bytes",
    "fixed": "bytes",
    "null": "string",
}

# Avro logical type overrides.
_AVRO_LOGICAL_TO_DBT: dict[str, str] = {
    "timestamp-millis": "timestamp(3)",
    "timestamp-micros": "timestamp(3)",
    "local-timestamp-millis": "timestamp(3)",
    "local-timestamp-micros": "timestamp(3)",
    "date": "date",
    "time-millis": "string",
    "time-micros": "string",
    "decimal": "double",
    "uuid": "string",
}

# Avro primitive type names — anything else appearing as a bare string type is
# a reference to an already-defined named type (record/enum/fixed), Avro's
# shorthand for reusing a type instead of repeating its full definition.
_AVRO_PRIMITIVE_TYPES = frozenset(
    {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}
)

# Debezium CDC envelope marker fields. Requiring all three (rather than just
# before/after) avoids misfiring on a table that coincidentally has columns
# named "before"/"after" but isn't actually a CDC envelope.
_DEBEZIUM_ENVELOPE_FIELDS = frozenset({"before", "after", "op"})


# ── Schema → ColumnSpec ───────────────────────────────────────────────────────

@dataclass
class ColumnSpec:
    """One column entry in a dbt YAML file."""
    name: str
    data_type: str


def schema_to_columns(schema: dict[str, Any], schema_type: str) -> list[ColumnSpec]:
    """Convert a parsed schema dict to a list of :class:`ColumnSpec`.

    If *schema* is a Debezium CDC envelope (see :func:`is_debezium_envelope`),
    columns are built from the ``after`` record — the actual current row
    shape — instead of the envelope's own fields (``before``, ``after``,
    ``source``, ``op``, ``ts_ms``, ...).

    Args:
        schema:      Parsed schema dict as returned by :meth:`SchemaFetcher.fetch`.
        schema_type: ``"JSON"``, ``"AVRO"``, or ``"PROTOBUF"``.

    Returns:
        Ordered list of :class:`ColumnSpec` matching the schema fields.

    Raises:
        NotImplementedError: for Protobuf schemas (not yet supported).
        ValueError:           for unrecognised schema_type values.
    """
    if schema_type == "PROTOBUF":
        raise NotImplementedError(
            "Protobuf schema conversion is not yet supported. "
            "Register the schema as JSON or Avro to use this tool."
        )
    if schema_type == "AVRO":
        return _avro_to_columns(schema)
    if schema_type == "JSON":
        return _json_to_columns(schema)
    raise ValueError(f"Unknown schema_type '{schema_type}'. Expected JSON or AVRO.")


def is_debezium_envelope(schema: dict[str, Any], schema_type: str) -> bool:
    """True if *schema* looks like a Debezium CDC envelope (before/after/op fields)."""
    if schema_type == "AVRO":
        return _debezium_after_field(schema) is not None
    if schema_type == "JSON":
        return _debezium_after_property(schema) is not None
    return False


# ── JSON Schema → columns (with Debezium CDC + nested-object support) ────────

def _debezium_after_property(schema: dict[str, Any]) -> dict[str, Any] | None:
    """Return the raw ``after`` property dict if *schema* is a Debezium envelope."""
    props = schema.get("properties")
    if not isinstance(props, dict) or not _DEBEZIUM_ENVELOPE_FIELDS <= props.keys():
        return None
    return props.get("after")


def _unwrap_json_nullable(prop: dict[str, Any]) -> dict[str, Any]:
    """Unwrap a nullable anyOf/oneOf JSON Schema property to its non-null branch."""
    for key in ("anyOf", "oneOf"):
        if key in prop:
            non_null = [b for b in prop[key] if b.get("type") != "null" and b != {"type": "null"}]
            if non_null:
                return non_null[0]
    return prop


def _json_type(prop: dict[str, Any]) -> str:
    """Map a single JSON Schema property definition to a dbt/Flink type string.

    Nested ``object``/``array`` properties are resolved recursively into
    ``row<...>``/``array<...>`` type strings, so a nested struct (e.g. inside
    a Debezium ``after`` record) produces a hierarchical column type instead
    of collapsing to a plain string.
    """
    prop = _unwrap_json_nullable(prop)

    raw_type: str = prop.get("type", "string")
    if isinstance(raw_type, list):
        # e.g. ["null", "string"] — take the first non-null
        non_null = [t for t in raw_type if t != "null"]
        raw_type = non_null[0] if non_null else "null"

    if raw_type == "object" and isinstance(prop.get("properties"), dict):
        nested = ", ".join(f"{name} {_json_type(sub)}" for name, sub in prop["properties"].items())
        return f"row<{nested}>" if nested else "row<string>"

    if raw_type == "array":
        items = prop.get("items")
        if isinstance(items, dict):
            return f"array<{_json_type(items)}>"
        return "array<string>"

    fmt: str | None = prop.get("format")
    lookup = f"{raw_type}:{fmt}" if fmt else raw_type
    return _JSON_TO_DBT.get(lookup) or _JSON_TO_DBT.get(raw_type, "string")


def _json_to_columns(schema: dict[str, Any]) -> list[ColumnSpec]:
    after_prop = _debezium_after_property(schema)
    if after_prop is not None:
        after_props = _unwrap_json_nullable(after_prop).get("properties")
        if isinstance(after_props, dict):
            return [ColumnSpec(name=name, data_type=_json_type(prop)) for name, prop in after_props.items()]

    props: dict[str, Any] = schema.get("properties", {})
    return [ColumnSpec(name=name, data_type=_json_type(prop)) for name, prop in props.items()]


# ── Avro → columns (with Debezium CDC + named-type resolution) ───────────────

def _collect_avro_named_types(node: Any, registry: dict[str, dict[str, Any]]) -> None:
    """Recursively register every named Avro record in *node* by simple and
    fully-qualified (namespace.name) name.

    Avro lets a schema reference an already-defined named record by a bare
    string instead of repeating its full definition — exactly what Debezium
    does for ``after`` when ``before`` already defines the shared row record
    (conventionally named ``Value``). This registry lets :func:`_resolve_avro_type`
    look such references back up to their full definition.
    """
    if isinstance(node, dict):
        if node.get("type") == "record" and "name" in node:
            simple_name = node["name"]
            registry[simple_name] = node
            namespace = node.get("namespace")
            if namespace:
                registry[f"{namespace}.{simple_name}"] = node
        for value in node.values():
            _collect_avro_named_types(value, registry)
    elif isinstance(node, list):
        for item in node:
            _collect_avro_named_types(item, registry)


def _resolve_avro_type(avro_type: Any, registry: dict[str, dict[str, Any]]) -> Any:
    """Resolve a possibly-named-type-reference Avro type to its full definition."""
    if isinstance(avro_type, str) and avro_type not in _AVRO_PRIMITIVE_TYPES:
        return registry.get(avro_type, avro_type)
    return avro_type


def _unwrap_avro_nullable(avro_type: Any) -> Any:
    """Unwrap a nullable union (``["null", X]``) to ``X``."""
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        return non_null[0] if non_null else "null"
    return avro_type


def _debezium_after_field(schema: dict[str, Any]) -> dict[str, Any] | None:
    """Return the raw ``after`` field dict if *schema* is a Debezium envelope."""
    fields = schema.get("fields")
    if not isinstance(fields, list):
        return None
    field_names = {f.get("name") for f in fields if isinstance(f, dict)}
    if not _DEBEZIUM_ENVELOPE_FIELDS <= field_names:
        return None
    return next((f for f in fields if isinstance(f, dict) and f.get("name") == "after"), None)


def _avro_type_to_dbt(avro_type: Any, registry: dict[str, dict[str, Any]]) -> str:
    """Map an already-resolved (nullable-unwrapped, named-ref-resolved) Avro type
    to a dbt/Flink type string, recursing into records/arrays/maps."""
    if isinstance(avro_type, dict):
        logical = avro_type.get("logicalType")
        if logical and logical in _AVRO_LOGICAL_TO_DBT:
            return _AVRO_LOGICAL_TO_DBT[logical]

        base = avro_type.get("type", "string")
        if base == "record":
            nested = ", ".join(
                f"{f['name']} {_avro_type(f, registry)}" for f in avro_type.get("fields", [])
            )
            return f"row<{nested}>" if nested else "row<string>"
        if base == "array":
            item_type = _resolve_avro_type(_unwrap_avro_nullable(avro_type.get("items")), registry)
            return f"array<{_avro_type_to_dbt(item_type, registry)}>"
        if base == "map":
            value_type = _resolve_avro_type(_unwrap_avro_nullable(avro_type.get("values")), registry)
            return f"map<string,{_avro_type_to_dbt(value_type, registry)}>"
        if base == "enum":
            return "string"
        return _AVRO_TO_DBT.get(base, "string")

    if isinstance(avro_type, str):
        return _AVRO_TO_DBT.get(avro_type, "string")

    return "string"


def _avro_type(field: dict[str, Any], registry: dict[str, dict[str, Any]]) -> str:
    """Map a single Avro field definition to a dbt/Flink type string.

    Resolves named-type references (e.g. Debezium's before/after row-record
    reuse) via *registry* and recurses into nested records/arrays/maps to
    build hierarchical ``row<...>``/``array<...>`` types instead of collapsing
    complex fields to a plain string.
    """
    resolved = _resolve_avro_type(_unwrap_avro_nullable(field.get("type")), registry)

    # A field-level logicalType (sibling of "type" rather than nested inside
    # it) is a non-standard shape this module has historically supported —
    # keep honoring it when the resolved type is a bare primitive string.
    if isinstance(resolved, str):
        logical = field.get("logicalType")
        if logical and logical in _AVRO_LOGICAL_TO_DBT:
            return _AVRO_LOGICAL_TO_DBT[logical]

    return _avro_type_to_dbt(resolved, registry)


def _avro_record_fields_to_columns(
    fields: list[dict[str, Any]], registry: dict[str, dict[str, Any]]
) -> list[ColumnSpec]:
    return [ColumnSpec(name=f["name"], data_type=_avro_type(f, registry)) for f in fields]


def _avro_to_columns(schema: dict[str, Any]) -> list[ColumnSpec]:
    registry: dict[str, dict[str, Any]] = {}
    _collect_avro_named_types(schema, registry)

    after_field = _debezium_after_field(schema)
    if after_field is not None:
        after_type = _resolve_avro_type(_unwrap_avro_nullable(after_field.get("type")), registry)
        if isinstance(after_type, dict) and after_type.get("type") == "record":
            return _avro_record_fields_to_columns(after_type.get("fields", []), registry)
        # 'after' present but didn't resolve to a record (unexpected shape) —
        # fall through and convert the raw envelope so callers still get something.

    fields: list[dict[str, Any]] = schema.get("fields", [])
    return _avro_record_fields_to_columns(fields, registry)


# ── YAML renderers ────────────────────────────────────────────────────────────

def render_sources_yaml(topic: str, schema_name: str, columns: list[ColumnSpec]) -> str:
    """Render a dbt ``sources:`` YAML block for a Kafka topic.

    The output matches the shape of ``models/sources.yaml`` in the
    airbnb_streaming dbt project (``contract.enforced: true``).

    Args:
        topic:       Kafka topic name — used for both the source ``name`` and
                     the ``tables[].name``.
        schema_name: Value placed in ``schema:`` (typically the Kafka cluster /
                     environment identifier, e.g. ``j9r-kafka``).
        columns:     Column definitions as returned by :func:`schema_to_columns`.

    Returns:
        YAML string ready to paste into ``models/sources.yaml``.
    """
    data = {
        "sources": [
            {
                "name": topic,
                "schema": schema_name,
                "tables": [
                    {
                        "name": topic,
                        "columns": [
                            {"name": col.name, "data_type": col.data_type}
                            for col in columns
                        ],
                    }
                ],
                "config": {"contract": {"enforced": True}},
            }
        ]
    }
    return yaml.safe_dump(data, sort_keys=False, default_flow_style=False, allow_unicode=True)


def render_model_yaml(model_name: str, columns: list[ColumnSpec]) -> str:
    """Render a dbt ``models:`` YAML block for a staging / source model.

    The output matches the shape of ``src_hosts_models.yml`` in the
    airbnb_streaming dbt project (``contract.enforced: false``).

    Args:
        model_name: dbt model name (e.g. ``src_hosts``).
        columns:    Column definitions as returned by :func:`schema_to_columns`.

    Returns:
        YAML string ready to paste into a model YAML file.
    """
    data = {
        "models": [
            {
                "name": model_name,
                "config": {"contract": {"enforced": False}},
                "columns": [
                    {"name": col.name, "data_type": col.data_type}
                    for col in columns
                ],
            }
        ]
    }
    return yaml.safe_dump(data, sort_keys=False, default_flow_style=False, allow_unicode=True)
