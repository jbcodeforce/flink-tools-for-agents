"""
Cli to manage a Flink dbt project to shift left from a batch processing.
Support the concept of star schema and data product or kimball with data product
"""

import enum
from pathlib import Path
from typing import Annotated

import typer
import yaml
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# dbt pipeline scaffold (mirrors `dbt init -s`)
# ---------------------------------------------------------------------------

_DBT_PROJECT_YML = """\

# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: '{project_name}'
version: '1.0.0'

# This setting configures which "profile" dbt uses for this project.
profile: '{profile_name}'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]


clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

models:
  {project_name}
"""

_GITIGNORE = """\

target/
dbt_packages/
logs/
"""

_PYPROJECT_TOML = """\
[project]
name = "{project_name}"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "dbt-confluent>=0.3.1",
    "dbt-core>=1.9.0",
]

"""

_SQL_TMPL = """
-- After editing this query, you MUST run `dbt run --full-refresh` to deploy the change.
-- Schema-drift detection only checks columns, types, and WITH options — query logic
-- changes are not detected and will be silently skipped on a normal `dbt run`.
{{ config(
    materialized = 'streaming_table',
    with= {
        'changelog.mode': 'append',
        'connector': 'confluent',
        'kafka.cleanup-policy': 'delete',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'value.format': 'avro-registry'
    }
) }}
--- to modify!
SELECT 1;
"""

_YML_TMPL= """
models:
- name: {table_name}
  description: ""
  config:
    contract:
      enforced: true
  columns:
  - name: _id
    data_type: varchar(2147483647)
    constraints:
      - type: not_null
      - type: primary_key
        expression: "not enforced"
  - name: is_superhero
    data_type: boolean
  - name: review_month
    data_type: date
    constraints:
      - type: not_null
  - name: big_value
    data_type: bigint
  - name: review_ts
    data_type: timestamp_ltz
  - name: price_as_double
    data_type: decimal(10,1)
  - name: small_int
    data_type: int
"""

# ---------------------------------------------------------------------------
# Raw-topic scaffold templates
# ---------------------------------------------------------------------------

_RAW_DDL_TMPL = """\
-- DDL: create the Kafka-backed source table for topic '{topic_name}'.
-- This must run (via `dbt run`) before any downstream model that references
-- {{ source('{profile_name}', '{topic_name}') }}.
-- Edit columns to match your actual topic schema, then run:
--   uv run dbt run --select raws.{topic_name}
CREATE TABLE IF NOT EXISTS {topic_name} (
    -- TODO: replace with the real columns of the topic
    id          VARCHAR,
    created_at  TIMESTAMP(3),
    payload     VARCHAR
) WITH (
    'connector'                = 'confluent',
    'changelog.mode'           = 'append',
    'scan.startup.mode'        = 'earliest-offset',
    'value.format'             = 'avro-registry',
    'kafka.cleanup-policy'     = 'delete'
);
"""

_RAW_DML_TMPL = """\
-- DML: insert synthetic rows into '{topic_name}' for local / CI testing.
-- Run with:  uv run dbt run --select raws.{topic_name}_seed
-- These rows let downstream models compile and run without a live Kafka topic.
INSERT INTO {topic_name} (id, created_at, payload)
VALUES
    ('synth-001', TIMESTAMP '2024-01-01 00:00:00', '{{"event": "bootstrap"}}'),
    ('synth-002', TIMESTAMP '2024-01-01 00:01:00', '{{"event": "bootstrap"}}');
"""

_RAW_SOURCES_ENTRY = """\
  - name: {topic_name}
    identifier: {topic_name}
    description: "Raw topic '{topic_name}' — auto-scaffolded by sl-dbt add-raw-topic."
    columns:
      - name: id
        data_type: varchar
      - name: created_at
        data_type: timestamp(3)
      - name: payload
        data_type: varchar
"""

_RAW_SOURCES_FILE = """\
version: 2
sources:
  - name: {profile_name}
    tables:
{entries}"""

class ProjectType(str, enum.Enum):
    data_product = "data-product"
    kimball = "kimball"

class TableType(str, enum.Enum):
    source    = "source"
    src       = "src"
    dimension = "dimension"
    dim       = "dim"
    fact      = "fact"
    other     = "other"

    def canonical(self) -> "TableType":
        """Resolve aliases to their canonical member."""
        _aliases = {TableType.src: TableType.source, TableType.dim: TableType.dimension}
        return _aliases.get(self, self)

def _write(path: Path, content: str, *, overwrite: bool = True) -> None:
    """Write *content* to *path*, creating parent directories as needed.

    When *overwrite* is ``False`` the file is skipped if it already exists.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and path.exists():
        return
    path.write_text(content)


def create_pipelines_hierarchy(
    pipelines_dir: Path, type: ProjectType, profile_name: str, *, force: bool = False
) -> None:
    """Create the standard dbt project structure under *pipelines_dir*.

    Produces the same layout as ``dbt init -s`` (skip profile setup):

        pipelines/
        ├── .gitignore
        ├── dbt_project.yml
        ├── pyproject.toml
        ├── macros/            (empty, tracked via .gitkeep)
        ├── models/
        ├── seeds/             (empty, tracked via .gitkeep)
        └── tests/             (empty, tracked via .gitkeep)

    When *force* is ``False`` (the default) existing files are left untouched,
    making repeated ``init`` calls safe for filling in missing files.
    """
    project_name = pipelines_dir.name  # folder name is the dbt project name
    ow = force  # shorthand: overwrite only when --force

    # Top-level files
    _write(pipelines_dir / ".gitignore", _GITIGNORE, overwrite=ow)
    _write(
        pipelines_dir / "dbt_project.yml",
        _DBT_PROJECT_YML.format(project_name=project_name, profile_name=profile_name),
        overwrite=ow,
    )
    _write(
        pipelines_dir / "pyproject.toml",
        _PYPROJECT_TOML.format(project_name=project_name),
        overwrite=ow,
    )

    # Empty directories tracked by .gitkeep
    for empty_dir in ("macros", "seeds", "tests"):
        _write(pipelines_dir / empty_dir / ".gitkeep", "", overwrite=ow)
    models_path = pipelines_dir / "models"
    models_path.mkdir(parents=True, exist_ok=True)
    if type == ProjectType.kimball:
        for kb_dir in ["sources", "intermediates", "marts"]:
            _write(pipelines_dir / "models" / kb_dir / ".gitkeep", "", overwrite=ow)

# ---------------------------------------------------------------------------
# Project metadata  (sl_dbt.yaml)
# ---------------------------------------------------------------------------

_METADATA_FILE = "sl_dbt.yaml"

class ProjectMetadata(BaseModel):
    """Persisted project settings written to sl_dbt.yaml."""

    pipelines_dir: str        # name of the folder holding dbt SQL (e.g. "pipelines")
    project_type: ProjectType
    dbt_profile_name: str

    def to_yaml(self) -> str:
        """Serialise the model to a YAML string (enum values as plain strings)."""
        return yaml.dump(self.model_dump(mode="json"), sort_keys=False)


def save_metadata(root: Path, meta: ProjectMetadata) -> None:
    """Persist *meta* to <root>/sl_dbt.yaml."""
    _write(root / _METADATA_FILE, meta.to_yaml())


def load_metadata(root: Path) -> ProjectMetadata:
    """Load project metadata from <root>/sl_dbt.yaml.

    Raises ``typer.Exit`` with an error message when the file is absent.
    """
    meta_path = root / _METADATA_FILE
    if not meta_path.exists():
        typer.echo(
            f"No {_METADATA_FILE} found in {root}. Run `init` first.", err=True
        )
        raise typer.Exit(code=1)
    return ProjectMetadata.model_validate(yaml.safe_load(meta_path.read_text()))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="sl-dbt",
    help=__doc__,
    no_args_is_help=True,
)


@app.command()
def init(
    project_root: Annotated[Path, typer.Argument(help="Project root folder path (e.g. ./crm_analytics)")],
    type: Annotated[
        ProjectType,
        typer.Option(
            help=(
                "'data-product': folder tree based on data product then star schema. "
                "'kimball': top level represents the medallion levels (sources, intermediates, marts)."
            ),
        ),
    ] = ProjectType.data_product,
    profile: Annotated[str, typer.Option(help="profile name in the .dbt/profiles.yml")] = "cc_flink",
    force: Annotated[bool, typer.Option("--force", help="Overwrite existing files.")] = False,
) -> None:
    """Initialise a new shift-left dbt project skeleton.

    Safe to re-run on an existing project: missing files are created and existing
    files are left untouched.  Pass ``--force`` to overwrite every file.
    """
    project_root.mkdir(parents=True, exist_ok=True)
    for sub in ("IaC", "docs", "tools"):
        (project_root / sub).mkdir(exist_ok=True)
    pipelines_name = "pipelines"
    create_pipelines_hierarchy(project_root / pipelines_name, type, profile, force=force)
    save_metadata(project_root, ProjectMetadata(pipelines_dir=pipelines_name, project_type=type, dbt_profile_name=profile))
    typer.echo(f"Project initialised at {project_root.resolve()}")


@app.command()
def add_data_product(
    project_root: Annotated[Path, typer.Argument(help="Existing project root folder path")],
    name: Annotated[str, typer.Argument(help="Data-product name (lowercase, underscores)")],
) -> None:
    """Add a new data-product sub-tree under <project_root>/<pipelines_dir>/models/<name>."""
    meta = load_metadata(project_root)
    if meta.project_type == ProjectType.data_product:
        dp_dir = project_root / meta.pipelines_dir / "models" / name
        dp_dir.mkdir(parents=True, exist_ok =True)
        _write(dp_dir / "schema.yml", f"version: 2\n\nmodels:\n  - name: {name}\n    description: \"\"\n")
        for sub in ["sources", "facts", "dimensions"]:
            _write(dp_dir / sub / ".gitkeep", "")

        typer.echo(f"Data product '{name}' created at {dp_dir.resolve()}")
    else:
        for sub in ["sources", "intermediates", "marts"]:
            dp_dir = project_root / meta.pipelines_dir / "models" / sub / name
            _write(dp_dir / ".gitkeep", "")
            gitkeep = project_root / meta.pipelines_dir / "models" / sub / ".gitkeep"
            try:
                gitkeep.unlink()
            except FileNotFoundError:
                pass
            except PermissionError:
                pass
    typer.echo(f"Data Product added {dp_dir.resolve()}")


@app.command()
def add_table(
    project_root: Annotated[Path, typer.Argument(help="Existing project root folder path")],
    name: Annotated[str, typer.Argument(help="table name (lowercase, underscores)")],
    data_product_name: Annotated[str, typer.Argument(help="Data-product name (lowercase, underscores)")],
    table_type: Annotated[TableType,
        typer.Option(
            help=(
                "'source': statement to process raw records as sources. "
                 "'src': statement to process raw records as sources. "
                "'dimension': statement to build star schema - dimension."
                "'dim': statement to build star schema - dimension."
                "'fact': statement to build star schema - fact."
                "'other': not in previous category"
            ),
        )] = TableType.source
    ):
    table_type = table_type.canonical()
    meta = load_metadata(project_root)
    if meta.project_type == ProjectType.data_product:
        dp_path = project_root / meta.pipelines_dir / "models" / data_product_name
        if table_type in [TableType.source, TableType.dimension, TableType.fact]:
            table_path = dp_path / f"{table_type.value}s"
        else:
            table_path = dp_path

    else:
        table_path = project_root / meta.pipelines_dir / "models" / "sources" / name

    _write(table_path / f"{name}.sql", _SQL_TMPL)
    _write(table_path / f"{name}.yml", _YML_TMPL.format(table_name=name))
    typer.echo(f"Statement/Table added as {name}.sql in {table_path.resolve()}")

def _upsert_sources_yaml(sources_path: Path, profile_name: str, topic_name: str) -> None:
    """Add *topic_name* to the sources.yaml under *sources_path*.

    If the file does not exist it is created from scratch.  If it already
    contains the topic it is left untouched (idempotent).
    """
    if sources_path.exists():
        data = yaml.safe_load(sources_path.read_text()) or {}
    else:
        data = {}

    sources: list = data.get("sources") or []

    # Find (or create) the source block matching profile_name
    source_block = next((s for s in sources if s.get("name") == profile_name), None)
    if source_block is None:
        source_block = {"name": profile_name, "tables": []}
        sources.append(source_block)

    tables: list = source_block.setdefault("tables", [])

    # Idempotent: skip if already registered
    if any(t.get("name") == topic_name for t in tables):
        return

    tables.append({
        "name": topic_name,
        "identifier": topic_name,
        "description": f"Raw topic '{topic_name}' — auto-scaffolded by sl-dbt add-raw-topic.",
        "columns": [
            {"name": "id",         "data_type": "varchar"},
            {"name": "created_at", "data_type": "timestamp(3)"},
            {"name": "payload",    "data_type": "varchar"},
        ],
    })

    data["version"] = 2
    data["sources"] = sources
    sources_path.write_text(yaml.dump(data, sort_keys=False, allow_unicode=True))


@app.command()
def add_raw_topic(
    project_root: Annotated[Path, typer.Argument(help="Existing project root folder path")],
    topic_name: Annotated[str, typer.Argument(help="Kafka topic name (e.g. raw_customers)")],
) -> None:
    """Scaffold a raw Kafka-topic table under pipelines/models/raws/<topic_name>/.

    Creates two files:

    \b
      ddl.<topic_name>.sql  — CREATE TABLE pointing at the Kafka topic.
      dml.<topic_name>.sql  — INSERT synthetic rows for local / CI testing.

    Also registers the topic in pipelines/models/raws/sources.yaml so that
    downstream {{ source(...) }} references resolve when running `uv run dbt run`.

    \b
    Example
    -------
      sl-dbt add-raw-topic ./my_project raw_customers
    """
    meta = load_metadata(project_root)
    raws_dir = project_root / meta.pipelines_dir / "raws" / topic_name
    profile_name = meta.dbt_profile_name

    _write(
        raws_dir / f"ddl.{topic_name}.sql",
        _RAW_DDL_TMPL.format(topic_name=topic_name, profile_name=profile_name),
        overwrite=False,
    )
    _write(
        raws_dir / f"dml.{topic_name}.sql",
        _RAW_DML_TMPL.format(topic_name=topic_name),
        overwrite=False,
    )

    sources_path = project_root / meta.pipelines_dir / "models" / "raws" / "sources.yaml"
    _upsert_sources_yaml(sources_path, profile_name, topic_name)

    typer.echo(f"Raw topic '{topic_name}' scaffolded at {raws_dir.resolve()}")
    typer.echo(f"  ddl.{topic_name}.sql  — edit columns to match your topic schema")
    typer.echo(f"  dml.{topic_name}.sql  — edit synthetic rows for local testing")
    typer.echo(f"  sources.yaml updated  — topic registered as source '{profile_name}'")


if __name__ == "__main__":
    app()
