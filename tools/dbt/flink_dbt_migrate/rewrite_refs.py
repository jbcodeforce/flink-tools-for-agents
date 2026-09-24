"""Rewrite bare Flink table names to dbt {{ ref() }} or {{ source() }} calls."""

from __future__ import annotations

import re


def strip_identifier(name: str) -> str:
    name = name.strip()
    if name.startswith("`") and name.endswith("`"):
        return name[1:-1]
    return name



def rewrite_refs(
    sql: str,
    cte_names: set[str],
    ref_overrides: dict[str, str] | None = None,
    ref_tables: set[str] | None = None,
    source_tables: dict[str, str] | None = None,
) -> str:
    ref_overrides = ref_overrides or {}
    ref_tables = ref_tables or set()
    source_tables = source_tables or {}

    def should_rewrite(table: str) -> bool:
        clean = strip_identifier(table)
        return clean not in cte_names

    def rewrite_target(table: str) -> str | None:
        clean = strip_identifier(table)
        if clean in source_tables:
            source_name = source_tables[clean]
            return f"{{{{ source('{source_name}', '{clean}') }}}}"
        if clean in ref_tables or clean in ref_overrides or not source_tables:
            model = ref_overrides.get(clean, clean)
            return f"{{{{ ref('{model}') }}}}"
        return None

    def replace_table_ref(match: re.Match[str]) -> str:
        prefix = match.group(1)
        table = match.group(2)
        if not should_rewrite(table):
            return match.group(0)
        rewritten = rewrite_target(table)
        if rewritten is None:
            return match.group(0)
        return f"{prefix}{rewritten}"

    _TABLE_TAIL = r"(?=[\s,\)]|$|\s+AS\b)"
    _NOT_SUBQUERY = r"(?!\s*\()"

    from_pattern = re.compile(
        rf"(\bFROM\s+)(`?[\w]+`?){_TABLE_TAIL}{_NOT_SUBQUERY}",
        re.IGNORECASE,
    )
    join_pattern = re.compile(
        rf"(\b(?:JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|"
        rf"FULL\s+JOIN|CROSS\s+JOIN)\s+)(`?[\w]+`?){_TABLE_TAIL}{_NOT_SUBQUERY}",
        re.IGNORECASE,
    )
    table_pattern = re.compile(r"(\bTABLE\s+)(`?[\w]+`?)\b", re.IGNORECASE)

    # Tokenize SQL into string literals, comments (line and block), and code segments.
    # Only rewrite table references in non-comment, non-string code segments.
    token_pattern = re.compile(
        r"('(?:[^']|'')*')"  # single-quoted string literal
        r"|(--[^\n]*)"       # line comment
        r"|(/\*[\s\S]*?\*/)" # block comment
    )

    def rewrite_segment(segment: str) -> str:
        segment = from_pattern.sub(replace_table_ref, segment)
        segment = join_pattern.sub(replace_table_ref, segment)
        segment = table_pattern.sub(replace_table_ref, segment)
        return segment

    parts: list[str] = []
    last_end = 0
    for match in token_pattern.finditer(sql):
        # Code before the matched token
        if match.start() > last_end:
            parts.append(rewrite_segment(sql[last_end : match.start()]))
        # Matched token (string literal or comment) is preserved as-is
        parts.append(match.group(0))
        last_end = match.end()

    # Remaining code after the last match
    if last_end < len(sql):
        parts.append(rewrite_segment(sql[last_end:]))

    return "".join(parts)
