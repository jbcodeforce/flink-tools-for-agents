"""CSV Parser Module.

Reads CSV files, parses headers, and infers column types for downstream
schema generation and Avro producer script scaffolding.
"""

from __future__ import annotations

import csv
import re
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type-inference patterns
# ---------------------------------------------------------------------------

# Integer: optional leading minus, digits only
_RE_INT = re.compile(r"^-?\d+$")

# Float: digits, optional decimal point with digits
_RE_FLOAT = re.compile(r"^[-+]?(?:\d+\.?\d*|\.\d+)$")

# Boolean (case-insensitive)
_RE_BOOL = re.compile(r"^(true|false|yes|no|1|0)$", re.IGNORECASE)

# ISO-like dates / times
_RE_DATE = re.compile(r"^\d{4}(-\d{2})?(-\d{2})?(T\d{2}:\d{2}:\d{2})?$")


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class InferredType:
    """Represents an inferred column type from CSV data."""

    name: str
    avro_type: str
    inference_score: float  # 0.0 (string) — 1.0 (homogeneous numeric)
    description: str = ""

    def __str__(self) -> str:
        return f"{self.name}: {self.avro_type} ({self.inference_score:.2f})"


# ---------------------------------------------------------------------------
# Column value analyser
# ---------------------------------------------------------------------------

def _analyse_column(column_name: str, values: list[str], sample_size: int = 100) -> InferredType:
    """Analyse a column's values and return an InferredType."""
    if not values:
        return InferredType(
            name=column_name,
            avro_type="string",
            inference_score=0.0,
            description=f"Empty column — defaulted to string",
        )

    non_empty = [v.strip() for v in values if v.strip() != ""]
    if not non_empty:
        return InferredType(
            name=column_name,
            avro_type="string",
            inference_score=0.0,
            description=f"All-empty column — defaulted to string",
        )

    sample = non_empty[:sample_size]

    int_count = sum(1 for v in sample if _RE_INT.match(v))
    float_count = sum(1 for v in sample if _RE_FLOAT.match(v))
    bool_count = sum(1 for v in sample if _RE_BOOL.match(v))
    date_count = sum(1 for v in sample if _RE_DATE.match(v))

    all_int = int_count == len(sample)
    all_float = float_count == len(sample)
    all_bool = bool_count == len(sample)
    all_date = date_count == len(sample)

    if all_int:
        return InferredType(
            name=column_name,
            avro_type="int",
            inference_score=1.0,
            description=f"All values are integers ({len(non_empty)} rows)",
        )
    if all_bool:
        return InferredType(
            name=column_name,
            avro_type="boolean",
            inference_score=1.0,
            description=f"All values are booleans ({len(non_empty)} rows)",
        )
    if all_float:
        return InferredType(
            name=column_name,
            avro_type="float",
            inference_score=1.0,
            description=f"All values are floats ({len(non_empty)} rows)",
        )
    if all_date:
        return InferredType(
            name=column_name,
            avro_type="string",
            inference_score=0.8,
            description=f"All values match date/time patterns ({len(non_empty)} rows) — stored as string",
        )

    float_pct = float_count / len(sample) if sample else 0
    desc = f"Mixed content ({float_pct:.0%} numeric) — defaulted to string"
    return InferredType(
        name=column_name,
        avro_type="string",
        inference_score=0.0,
        description=desc,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class CSVParser:
    """Parses a CSV file and returns inferred column types.

    Usage:
        parser = CSVParser("data.csv")
        columns = parser.parse()
    """

    def __init__(self, csv_path: Path | str, sample_size: int = 100, encoding: str = "utf-8-sig"):
        self._csv_path = Path(csv_path)
        self.sample_size = sample_size
        self.encoding = encoding
        self._headers: list[str] | None = None
        self._columns: list[InferredType] | None = None

    @property
    def csv_path(self) -> Path:
        return self._csv_path

    def _read_raw_rows(self) -> tuple[list[str], list[list[str]]]:
        """Read the CSV and return headers + all raw value rows."""
        with open(self._csv_path, newline="", encoding=self.encoding, errors="replace") as f:
            reader = csv.DictReader(f)
            headers = list(reader.fieldnames or [])
            rows = [list(row.values()) for row in reader]
        return headers, rows

    def parse(self) -> list[InferredType]:
        """Parse the CSV and return a list of InferredType objects."""
        if not self._csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self._csv_path}")

        headers, rows = self._read_raw_rows()
        if not headers:
            raise ValueError(f"CSV file is empty or has no headers: {self._csv_path}")

        self._headers = headers
        self._columns = [
            _analyse_column(
                h,
                [r[i] if i < len(r) else "" for r in rows],
                self.sample_size,
            )
            for i, h in enumerate(headers)
        ]
        return self._columns

    @property
    def headers(self) -> list[str] | None:
        if self._headers is None:
            self.parse()
        return self._headers

    @property
    def columns(self) -> list[InferredType] | None:
        if self._columns is None:
            self.parse()
        return self._columns

    @property
    def csv_as_dicts(self) -> list[dict[str, str]]:
        """Return all rows as a list of dicts."""
        headers, rows = self._read_raw_rows()
        return [dict(zip(headers, r)) for r in rows]

    @property
    def raw_values(self) -> list[list[str]]:
        """Return all rows as a list-of-lists of strings."""
        _, rows = self._read_raw_rows()
        return rows


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_column_name(name: str) -> None:
    """Ensure a column name is valid for Avro (and Flink SQL)."""
    if not name:
        raise ValueError(f"Column name cannot be empty")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise ValueError(
            f"Invalid Avro field name '{name}': must start with a letter or underscore, "
            "followed by letters, digits, or underscores only."
        )


def validate_columns(columns: list[InferredType]) -> list[str]:
    """Return a list of validation errors (empty if valid)."""
    errors: list[str] = []
    seen_names: set[str] = set()
    for col in columns:
        try:
            validate_column_name(col.name)
        except ValueError as exc:
            errors.append(str(exc))
        if col.name in seen_names:
            errors.append(f"Duplicate column name: '{col.name}'")
        seen_names.add(col.name)
    return errors

