"""Helpers for injecting tags into migrated dbt SQL config blocks."""

from __future__ import annotations

import json
import re
from pathlib import Path


def read_product_name(pipeline_def_path: Path) -> str:
    """Return the ``product_name`` field from a ``pipeline_definition.json`` file.

    Raises
    ------
    KeyError
        If ``product_name`` is absent from the JSON.
    json.JSONDecodeError
        If the file is not valid JSON.
    """
    data = json.loads(pipeline_def_path.read_text(encoding="utf-8"))
    return data["product_name"]


# Matches the opening {{ config( ... ) }} block (possibly multi-line).
# Group 1 captures everything up to but not including the closing `) }}`.
_CONFIG_BLOCK_RE = re.compile(
    r"(\{\{\s*config\s*\(.*?)(\)\s*\}\})",
    re.DOTALL,
)


def inject_tags_into_config(sql_content: str, tag: str) -> tuple[str, bool]:
    """Inject ``tags=['<tag>']`` into the ``{{ config(...) }}`` block.

    Returns
    -------
    (patched_content, was_changed)
        *was_changed* is ``False`` when ``tags=`` already appears inside the
        config block (idempotency guard) or when no config block was found.
    """
    m = _CONFIG_BLOCK_RE.search(sql_content)
    if m is None:
        return sql_content, False

    config_body = m.group(1)  # everything from {{ config( up to the last param

    # Idempotency: if tags= already present anywhere in the config block, skip.
    if re.search(r"\btags\s*=", config_body):
        return sql_content, False

    # Insert `    tags=['<tag>'],\n` before the closing `) }}`.
    # We place it after the last non-whitespace character of the body so the
    # indentation matches the surrounding params (4 spaces).
    new_config_body = config_body.rstrip() + f",\n    tags=['{tag}']"
    patched = sql_content[: m.start(1)] + new_config_body + "\n" + m.group(2) + sql_content[m.end(2) :]
    return patched, True
