"""
Shift left releated functions to work on existing shift_left utils Flink SQL repository
"""
import json
import hashlib
from pathlib import Path
from dataclasses import dataclass, field
from tools.dbt.flink_dbt_migrate.flink_sql_processor import (
    discover_ddl_path,
    is_values_insert,
    parse_dml,
    parse_values_dml,
)

@dataclass(frozen=True)
class TableEntry:
    """A single table discovered inside a shift_left pipeline folder."""

    table_name: str
    dml_path: Path
    ddl_path: Path
    dml_sha256: str
    relative_path: Path  # path from pipeline root to the table's parent dir
    # table_name → absolute DDL path for upstream tables, sourced from pipeline_definition.json
    upstream_ddl_map: dict[str, Path] = field(default_factory=dict)
    is_seed: bool = False


def _upstream_ddl_map_from_pipeline_def(
    table_dir: Path,
    pipelines_parent: Path,
) -> dict[str, Path]:
    """
    Read pipeline_definition.json
    returns {table_name: abs_ddl_path} for all parents from the pipeline_root folder
    """
    pipeline_def = table_dir / "pipeline_definition.json"
    if not pipeline_def.is_file():
        return {}
    try:
        data = json.loads(pipeline_def.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}

    ddl_map: dict[str, Path] = {}
    for parent in data.get("parents", []):
        name = parent.get("table_name", "")
        ddl_ref = parent.get("ddl_ref", "")
        if name and ddl_ref:
            abs_ddl = (pipelines_parent / ddl_ref).resolve()
            if abs_ddl.is_file():
                ddl_map[name] = abs_ddl
    return ddl_map

def _find_pipelines_parent(folder: Path) -> Path:
    """Walk up from *folder* to find the directory that contains a 'pipelines/' sub-tree.

    Falls back to *folder* itself if no such ancestor is found.
    """
    current = folder.resolve()
    for ancestor in [current, *current.parents]:
        if (ancestor / "pipelines").is_dir():
            return ancestor
    return current


def load_excluded_folders(exclude_file: Path, base_dir: Path | None = None) -> set[Path]:
    """Load folder paths to exclude from an exclusion text file.

    Each row is a folder path (relative or absolute).
    Blank lines and lines starting with '#' are ignored.
    Relative paths are resolved against *base_dir* if provided, otherwise against the parent of *exclude_file*.
    """
    exclude_file = exclude_file.resolve()
    if not exclude_file.is_file():
        raise FileNotFoundError(f"Exclusion file not found: {exclude_file}")

    root_dir = (base_dir or exclude_file.parent).resolve()
    excluded: set[Path] = set()

    for line in exclude_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        folder_path = Path(line)
        if not folder_path.is_absolute():
            folder_path = (root_dir / folder_path).resolve()
        else:
            folder_path = folder_path.resolve()
        excluded.add(folder_path)

    return excluded


def _is_excluded(path: Path, excluded_folders: set[Path]) -> bool:
    """Check if *path* is in *excluded_folders* or is a descendant of any excluded folder."""
    path = path.resolve()
    for excl in excluded_folders:
        if path == excl or excl in path.parents:
            return True
    return False


def crawl_pipeline_folder(
    folder: Path,
    excluded_folders: set[Path] | None = None,
    product: str | None = None,
) -> list[TableEntry]:
    """
    Recursively walk *folder* and return one TableEntry per discoverable table.

    A table is discoverable when:
    - a ``sql_scripts/`` subdirectory exists, AND
    - at least one ``dml.*.sql`` file is present, AND
    - a matching ``ddl.*.sql`` can be resolved via the standard discovery rules.
    - the table directory is not part of ``excluded_folders``.

    When *product* is set, only entries whose ``relative_path`` contains a segment
    equal to *product* are returned (matches by path segment value, not position).

    Tables whose DDL cannot be found are skipped with a warning on stderr.
    Upstream DDL paths are resolved from ``pipeline_definition.json`` when present.
    """
    folder = folder.resolve()
    pipelines_parent = _find_pipelines_parent(folder)
    excluded = {p.resolve() for p in excluded_folders} if excluded_folders else set()

    entries: list[TableEntry] = []
    for sql_scripts_dir in sorted(folder.rglob("sql-scripts")):
        if not sql_scripts_dir.is_dir():
            continue
        table_dir = sql_scripts_dir.parent
        if excluded and _is_excluded(table_dir, excluded):
            continue
        upstream_ddl_map = _upstream_ddl_map_from_pipeline_def(table_dir, pipelines_parent)

        for dml_file in sorted(sql_scripts_dir.glob("dml.*.sql")):
            try:
                dml_text = dml_file.read_text(encoding="utf-8")
                is_seed = is_values_insert(dml_text)
                if is_seed:
                    target_table = parse_values_dml(dml_text, source_file=dml_file.name).target_table
                else:
                    target_table = parse_dml(dml_text, source_file=dml_file.name).target_table
                ddl_file_path = Path(
                    discover_ddl_path(str(dml_file), target_table)
                )
            except FileNotFoundError as exc:
                print(f"WARNING: skipping {dml_file.name} — {exc}")
                continue
            except ValueError as exc:
                print(f"WARNING: skipping {dml_file.name} — {exc}")
                continue

            sha256 = hashlib.sha256(dml_file.read_bytes()).hexdigest()
            # relative_path: from folder root to the table directory (parent of sql_scripts)
            relative_path = table_dir.relative_to(folder)
            if product is not None and product not in relative_path.parts:
                continue
            entries.append(
                TableEntry(
                    table_name=target_table,
                    dml_path=dml_file,
                    ddl_path=ddl_file_path,
                    dml_sha256=sha256,
                    relative_path=relative_path,
                    upstream_ddl_map=upstream_ddl_map,
                    is_seed=is_seed,
                )
            )
    return entries