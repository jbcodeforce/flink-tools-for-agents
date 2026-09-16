"""Discover upstream Flink table dependencies for dbt migration."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from tools.dbt.flink_dbt_migrate.flink_sql_processor import DdlTable, _get_logger, parse_ddl, DmlStatement, collect_cte_names, strip_identifier

# Matches CREATE TABLE, CREATE OR REPLACE TABLE, CREATE TABLE IF NOT EXISTS
_CREATE_TABLE_INDEX_PATTERN = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`]?([\w]+)[`]?\s*\(",
    re.IGNORECASE,
)


def build_pipelines_ddl_index(pipelines_root: Path) -> dict[str, Path]:
    """Scan *pipelines_root* and return a ``{table_name: ddl_path}`` index.

    Only ``sql-scripts/`` directories are scanned (skipping ``tests/`` folders)
    to mirror what the migration crawler considers authoritative.  When the same
    table name appears in multiple DDL files the first match wins (sorted path
    order is deterministic).
    """
    log = _get_logger()
    index: dict[str, Path] = {}
    pipelines_root = pipelines_root.resolve()
    for sql_scripts_dir in sorted(pipelines_root.rglob("sql-scripts")):
        if not sql_scripts_dir.is_dir():
            continue
        # Skip test fixture directories
        if "tests" in sql_scripts_dir.parts:
            continue
        for ddl_file in sorted(sql_scripts_dir.glob("ddl*.sql")):
            try:
                text = ddl_file.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for match in _CREATE_TABLE_INDEX_PATTERN.finditer(text):
                name = match.group(1)
                if name not in index:
                    index[name] = ddl_file
    log.debug("build_pipelines_ddl_index | root=%s  entries=%d", pipelines_root, len(index))
    return index

_TABLE_TAIL = r"(?=[\s,\)]|$|\s+AS\b)"
_NOT_SUBQUERY = r"(?!\s*\()"

_FROM_PATTERN = re.compile(
    rf"\bFROM\s+(`?[\w]+`?){_TABLE_TAIL}{_NOT_SUBQUERY}",
    re.IGNORECASE,
)
_JOIN_PATTERN = re.compile(
    rf"\b(?:JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|"
    rf"FULL\s+JOIN|CROSS\s+JOIN)\s+(`?[\w]+`?){_TABLE_TAIL}{_NOT_SUBQUERY}",
    re.IGNORECASE,
)
_TABLE_PATTERN = re.compile(rf"\bTABLE\s+(`?[\w]+`?)\b", re.IGNORECASE)
_CREATE_TABLE_PATTERN = re.compile(
    r"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(`?[\w]+`?)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class UpstreamDep:
    table_name: str
    ddl_path: Path | None
    ddl: DdlTable | None
    resolution: Literal["ref", "source"]
    ref_model: str | None = None
    source_name: str | None = None


def default_source_name(source_project_dir: Path) -> str:
    name = source_project_dir.name.replace("-", "_").replace(".", "_")
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    return name.lower()


# SQL keywords that are never valid table names but can appear after FROM/JOIN
# in comments, window functions, or other constructs.
_SQL_KEYWORD_BLOCKLIST: frozenset[str] = frozenset({
    "select", "where", "on", "and", "or", "not", "in", "as", "by",
    "with", "having", "group", "order", "limit", "offset", "union",
    "all", "distinct", "case", "when", "then", "else", "end",
    "true", "false", "null", "is", "between", "like", "exists",
    "to", "from", "the", "a", "an", "of", "for", "at", "into",
    "batch", "final", "removal", "ties", "only",
})


def _strip_line_comments(sql: str) -> str:
    """Remove ``-- ...`` line comments from SQL text."""
    return "\n".join(re.sub(r"--.*$", "", line) for line in sql.splitlines())


def collect_upstream_tables(body: str, cte_names: set[str]) -> list[str]:
    # Strip line comments so keywords inside comments are not matched as tables
    body = _strip_line_comments(body)

    tables: list[str] = []
    seen: set[str] = set()

    for pattern in (_FROM_PATTERN, _JOIN_PATTERN, _TABLE_PATTERN):
        for match in pattern.finditer(body):
            table = strip_identifier(match.group(1))
            if table in cte_names or table in seen:
                continue
            if table.lower() in _SQL_KEYWORD_BLOCKLIST:
                continue
            seen.add(table)
            tables.append(table)

    return tables


def _ddl_suffix_candidates(table_name: str) -> list[str]:
    candidates = [table_name]
    if "_" in table_name:
        prefix, suffix = table_name.split("_", 1)
        if prefix.isdigit() or (prefix.startswith("d") and prefix[1:].isdigit()):
            candidates.append(suffix)
    return list(dict.fromkeys(candidates))


def discover_upstream_ddl(
    source_dir: Path,
    table_name: str,
    global_ddl_index: dict[str, Path] | None = None,
) -> Path:
    log = _get_logger()
    source_dir = source_dir.resolve()
    log.debug("discover_upstream_ddl | table=%s  source_dir=%s", table_name, source_dir)

    for suffix in _ddl_suffix_candidates(table_name):
        candidate = source_dir / f"ddl.{suffix}.sql"
        log.debug("  suffix candidate: %s  exists=%s", candidate, candidate.is_file())
        if candidate.is_file():
            text = candidate.read_text(encoding="utf-8")
            for match in _CREATE_TABLE_PATTERN.finditer(text):
                found = strip_identifier(match.group(1))
                log.debug("    CREATE TABLE found in file: %s", found)
                if found == table_name:
                    log.debug("  -> matched via suffix candidate: %s", candidate)
                    return candidate

    direct = source_dir / f"ddl.{table_name}.sql"
    log.debug("  direct candidate: %s  exists=%s", direct, direct.is_file())
    if direct.is_file():
        log.debug("  -> matched via direct candidate: %s", direct)
        return direct

    all_ddl = sorted(source_dir.glob("ddl*.sql"))
    log.debug("  glob ddl*.sql in %s -> %s file(s): %s", source_dir, len(all_ddl), [p.name for p in all_ddl])
    matches: list[Path] = []
    for ddl_path in all_ddl:
        text = ddl_path.read_text(encoding="utf-8")
        for match in _CREATE_TABLE_PATTERN.finditer(text):
            found = strip_identifier(match.group(1))
            log.debug("    %s defines CREATE TABLE %s", ddl_path.name, found)
            if found == table_name:
                matches.append(ddl_path)
                break

    if matches:
        if len(matches) == 1:
            log.debug("  -> matched via glob scan: %s", matches[0])
            return matches[0]

        def rank(path: Path) -> tuple[int, int, str]:
            name = path.name.lower()
            wm_penalty = 1 if "_wm" in name else 0
            return (wm_penalty, len(name), name)

        best = sorted(matches, key=rank)[0]
        log.debug("  -> best match among %d: %s", len(matches), best)
        return best

    # Local search exhausted — try the global pipelines-tree index
    if global_ddl_index and table_name in global_ddl_index:
        found_path = global_ddl_index[table_name]
        log.debug("  -> matched via global DDL index: %s", found_path)
        return found_path

    log.warning(
        "discover_upstream_ddl FAILED | table=%s  source_dir=%s  "
        "glob_files=%s",
        table_name, source_dir, [p.name for p in all_ddl],
    )
    raise FileNotFoundError(
        f"No DDL file found for upstream table {table_name} in {source_dir}. "
        f"Tried ddl.{table_name}.sql and scanned ddl*.sql."
    )


def find_dbt_model(project_dir: Path, model_name: str) -> Path | None:
    models_dir = project_dir / "models"
    if not models_dir.is_dir():
        return None

    matches = [
        path
        for path in models_dir.rglob("*.sql")
        if path.stem == model_name
    ]
    if not matches:
        return None
    if len(matches) > 1:
        return matches[0]
    return matches[0]


def resolve_upstream_deps(
    source_project_dir: Path,
    dbt_project_dir: Path | None,
    dml: DmlStatement,
    *,
    ref_overrides: dict[str, str] | None = None,
    source_name: str | None = None,
    resolve_sources: bool = True,
    upstream_ddl_map: dict[str, Path] | None = None,
    global_ddl_index: dict[str, Path] | None = None,
    known_models: set[str] | None = None,
) -> list[UpstreamDep]:
    log = _get_logger()
    ref_overrides = ref_overrides or {}
    upstream_ddl_map = upstream_ddl_map or {}
    known_models = known_models or set()
    cte_names = collect_cte_names(dml.body)
    upstream_tables = collect_upstream_tables(dml.body, cte_names)
    resolved_source_name = source_name or default_source_name(source_project_dir)
    log.debug(
        "resolve_upstream_deps | target=%s  source_project_dir=%s  dbt_project_dir=%s  "
        "upstream_tables=%s  cte_names=%s  upstream_ddl_map_keys=%s  known_models=%s",
        dml.target_table, source_project_dir, dbt_project_dir,
        upstream_tables, cte_names, list(upstream_ddl_map.keys()), list(known_models),
    )

    deps: list[UpstreamDep] = []
    for table_name in upstream_tables:
        if table_name in ref_overrides:
            deps.append(
                UpstreamDep(
                    table_name=table_name,
                    ddl_path=None,
                    ddl=None,
                    resolution="ref",
                    ref_model=ref_overrides[table_name],
                )
            )
            continue

        if table_name in known_models:
            deps.append(
                UpstreamDep(
                    table_name=table_name,
                    ddl_path=None,
                    ddl=None,
                    resolution="ref",
                    ref_model=table_name,
                )
            )
            continue

        ref_model_path = (
            find_dbt_model(dbt_project_dir, table_name)
            if dbt_project_dir is not None
            else None
        )
        if ref_model_path is not None:
            deps.append(
                UpstreamDep(
                    table_name=table_name,
                    ddl_path=None,
                    ddl=None,
                    resolution="ref",
                    ref_model=table_name,
                )
            )
            continue

        if not resolve_sources:
            deps.append(
                UpstreamDep(
                    table_name=table_name,
                    ddl_path=None,
                    ddl=None,
                    resolution="ref",
                    ref_model=table_name,
                )
            )
            continue

        # Use pipeline_definition.json index first, fall back to filesystem scan,
        # then fall back to the global pipelines-tree DDL index
        if table_name in upstream_ddl_map:
            ddl_path = upstream_ddl_map[table_name]
        else:
            ddl_path = discover_upstream_ddl(
                source_project_dir, table_name, global_ddl_index=global_ddl_index
            )
        ddl = parse_ddl(ddl_path.read_text(encoding="utf-8"))
        deps.append(
            UpstreamDep(
                table_name=table_name,
                ddl_path=ddl_path,
                ddl=ddl,
                resolution="source",
                source_name=resolved_source_name,
            )
        )

    return deps
