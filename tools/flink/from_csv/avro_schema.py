"""Avro Schema Builder Module.

Converts inferred CSV column types into valid Avro schema definitions.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .csv_parser import InferredType, validate_columns


# ---------------------------------------------------------------------------
# Avro Field Type Mapping
# ---------------------------------------------------------------------------

_AVRO_TYPE_MAP: dict[str, str] = {
    "int": "int",
    "float": "float",
    "boolean": "boolean",
    "string": "string",
    "null": "null",
}


# ---------------------------------------------------------------------------
# Schema generation
# ---------------------------------------------------------------------------

def build_avro_schema(
    columns: list[InferredType],
    record_name: str | None = None,
    namespace: str | None = None,
) -> dict[str, Any]:
    """Build an Avro schema from inferred column types.

    Args:
        columns: List of InferredType from CSVParser.parse().
        record_name: Record name. Auto-derived from CSV filename if None.
        namespace: Avro namespace. Auto-derived if None.

    Returns:
        A dict representing a valid Avro schema.
    """
    # Validate column names first
    errors = validate_columns(columns)
    if errors:
        raise ValueError("Schema validation errors: " + "; ".join(errors))

    # Determine record name
    if not record_name:
        record_name = columns[0].name if columns else "record"
        record_name = record_name[0].upper() + record_name[1:] if record_name else "Record"

    # Determine namespace
    if not namespace:
        namespace = "com.example.ingestion"

    # Build field list
    fields = []
    for col in columns:
        avro_type = _AVRO_TYPE_MAP.get(col.avro_type, col.avro_type)
        field_entry: dict[str, Any] = {"name": col.name, "type": avro_type}
        if col.description:
            field_entry["description"] = col.description
        fields.append(field_entry)

    # Build schema
    return {
        "type": "record",
        "name": record_name,
        "namespace": namespace,
        "fields": fields,
    }


def validate_avro_schema(schema: dict[str, Any]) -> list[str]:
    """Basic validation of an Avro schema dict. Returns list of error messages."""
    errors: list[str] = []

    if schema.get("type") != "record":
        errors.append("Schema type must be 'record'")

    if not schema.get("name"):
        errors.append("Schema must have a 'name'")

    if not schema.get("namespace"):
        errors.append("Schema must have a 'namespace'")

    if not schema.get("fields"):
        errors.append("Schema must have at least one field")

    seen_names: set[str] = set()
    for i, field in enumerate(schema.get("fields", [])):
        if not field.get("name"):
            errors.append(f"Field {i} missing 'name'")
        elif not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", field["name"]):
            errors.append(f"Field {i} has invalid name: '{field.get('name')}'")
        else:
            if field["name"] in seen_names:
                errors.append(f"Duplicate field name: '{field['name']}'")
            seen_names.add(field["name"])

        valid_types = {
            "int", "float", "double", "long", "short", "boolean",
            "string", "null", "byte", "uint", "uuid",
        }
        field_type = field.get("type")
        if isinstance(field_type, list):
            if "null" not in field_type:
                errors.append(f"Field {i} union must include 'null' if nullable")
        elif field_type not in valid_types:
            errors.append(f"Field '{field.get('name')}' has invalid type: '{field_type}'")

    return errors


def write_schema(
    schema: dict[str, Any],
    output_path: Path,
    pretty: bool = True,
) -> None:
    """Write an Avro schema dict to a .avsc file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    indent = 2 if pretty else None
    with output_path.open("w") as fp:
        json.dump(schema, fp, indent=indent)

