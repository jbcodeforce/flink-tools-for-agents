"""
Cli to manage a Flink dbt project to shift left from a batch processing.
Support the concept of star schema and data product or kimball with data product
"""

import enum
import os
from pathlib import Path
from typing import Annotated

import typer
import yaml
from confluent_kafka.schema_registry.error import SchemaRegistryError
from pydantic import BaseModel

from tools.flink.cc_deploy.deploy_flink_statements import load_dotenv_file
from tools.dbt.schema_registry_helpers import (
    SchemaFetcher,
    is_debezium_envelope,
    render_model_yaml,
    render_sources_yaml,
    schema_to_columns,
)

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
    'kafka.topic'              = '{topic_name}',
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


class SchemaOutputMode(str, enum.Enum):
    sources = "sources"
    model = "model"

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
    print(root)
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

def _upsert_source_table(
    sources_path: Path,
    profile_name: str,
    topic_name: str,
    columns: list[dict[str, str]],
    *,
    description: str | None = None,
) -> None:
    """Add or replace *topic_name* as a table under *profile_name* in sources.yaml.

    If the file (or its parent directory) does not exist it is created from
    scratch. Unlike :func:`_upsert_sources_yaml`, this always overwrites an
    existing table entry's columns/description — used for schema-registry-
    derived columns, which should reflect the live schema on every run.
    """
    sources_path.parent.mkdir(parents=True, exist_ok=True)
    data = yaml.safe_load(sources_path.read_text()) if sources_path.exists() else {}
    data = data or {}

    sources: list = data.get("sources") or []

    # Find (or create) the source block matching profile_name
    source_block = next((s for s in sources if s.get("name") == profile_name), None)
    if source_block is None:
        source_block = {"name": profile_name, "tables": []}
        sources.append(source_block)

    tables: list = source_block.setdefault("tables", [])

    entry = {"name": topic_name, "identifier": topic_name}
    if description:
        entry["description"] = description
    entry["columns"] = columns

    existing = next((t for t in tables if t.get("name") == topic_name), None)
    if existing is not None:
        existing.update(entry)
    else:
        tables.append(entry)

    data["version"] = 2
    data["sources"] = sources
    sources_path.write_text(yaml.dump(data, sort_keys=False, allow_unicode=True))


def _upsert_sources_yaml(sources_path: Path, profile_name: str, topic_name: str) -> None:
    """Add *topic_name* to the sources.yaml under *sources_path* (raw-topic scaffold columns).

    If the file does not exist it is created from scratch.  If it already
    contains the topic it is left untouched (idempotent) — the scaffolded
    columns are TODO placeholders meant for manual editing, so re-running
    this must never clobber a user's edits.
    """
    data = yaml.safe_load(sources_path.read_text()) if sources_path.exists() else {}
    sources: list = (data or {}).get("sources") or []
    source_block = next((s for s in sources if s.get("name") == profile_name), None)
    already_registered = source_block is not None and any(
        t.get("name") == topic_name for t in source_block.get("tables", [])
    )
    if already_registered:
        return

    _upsert_source_table(
        sources_path,
        profile_name,
        topic_name,
        columns=[
            {"name": "id",         "data_type": "varchar"},
            {"name": "created_at", "data_type": "timestamp(3)"},
            {"name": "payload",    "data_type": "varchar"},
        ],
        description=f"Raw topic '{topic_name}' — auto-scaffolded by sl-dbt add-raw-topic.",
    )


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
    raws_dir = project_root / meta.pipelines_dir / "models" / "raws" / topic_name
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


# ---------------------------------------------------------------------------
# Schema Registry → dbt YAML
# ---------------------------------------------------------------------------

def _env_first(*names: str) -> str:
    """Return the value of the first set (non-empty) env var, or ''."""
    for name in names:
        value = os.environ.get(name, "")
        if value:
            return value
    return ""


@app.command()
def get_schema_existing_topic_to_dbt(
    project_root: Annotated[Path, typer.Argument(help="Existing project root folder path")],
    topic_name: Annotated[str, typer.Argument(help="Kafka topic name (e.g. raw_hosts).")],
    subject_suffix: Annotated[
        str,
        typer.Option(
            "--subject-suffix",
            help=(
                "Comma-separated subject suffixes to fetch: 'key', 'value', or "
                "'key,value' (default: value)."
            ),
        ),
    ] = "value",
    output: Annotated[
        SchemaOutputMode,
        typer.Option(
            "--output",
            help=(
                "'sources' emits a sources: block (contract.enforced: true); "
                "'model' emits a models: block (contract.enforced: false)."
            ),
        ),
    ] = SchemaOutputMode.sources,
    schema_name: Annotated[
        str,
        typer.Option(
            "--schema-name",
            help=(
                "Override the name used in the YAML output. "
                "For --output sources this sets schema: and source name (default: topic). "
                "For --output model this sets the model name (default: topic)."
            ),
        ),
    ] = "",
) -> None:
    """Fetch Confluent Schema Registry subjects for a Kafka topic and write dbt YAML.

    Reads the key and/or value schema registered under ``{topic}-key`` /
    ``{topic}-value`` and writes it into the project:

    \b
      --output sources (default): upserts a table entry for the topic into
        <project_root>/<pipelines_dir>/models/sources.yaml, under the
        project's dbt profile (same file/shape as `add-raw-topic`).
      --output model: writes a standalone
        <project_root>/<pipelines_dir>/models/<schema-name>.yml (overwritten
        on every run, so it always reflects the current schema).

    The YAML block is also printed to stdout.

    If the fetched schema is a Debezium CDC envelope (has before/after/op
    fields), columns are built from the `after` record — the actual current
    row shape — instead of the envelope's own fields. Nested structs inside
    it become hierarchical `row<...>` types rather than collapsing to a
    plain string.

    Schema Registry credentials are resolved in priority order:

    \b
      1. ~/.confluent/.env / repo-root .env (loaded automatically)
      2. Environment variables (SCHEMA_REGISTRY_ENDPOINT, SCHEMA_REGISTRY_API_KEY,
         SCHEMA_REGISTRY_API_SECRET)

    \b
    Examples
    --------
      sl-dbt get-schema-existing-topic-to-dbt ./my_project raw_hosts
      sl-dbt get-schema-existing-topic-to-dbt ./my_project raw_hosts --subject-suffix key,value
      sl-dbt get-schema-existing-topic-to-dbt ./my_project raw_hosts --output model --schema-name src_hosts
    """
    load_dotenv_file()
    meta = load_metadata(project_root)

    resolved_sr_url = _env_first("SCHEMA_REGISTRY_ENDPOINT", "SCHEMA_REGISTRY_URL")
    resolved_sr_key = _env_first("SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_USER")
    resolved_sr_secret = _env_first("SCHEMA_REGISTRY_API_SECRET", "SCHEMA_REGISTRY_PASSWORD")

    if not resolved_sr_url:
        typer.echo(
            "Error: Schema Registry URL is required. Set SCHEMA_REGISTRY_ENDPOINT.",
            err=True,
        )
        raise typer.Exit(code=1)

    suffixes = [s.strip() for s in subject_suffix.split(",") if s.strip()]
    invalid = [s for s in suffixes if s not in ("key", "value")]
    if invalid:
        typer.echo(
            f"Error: invalid --subject-suffix value(s): {invalid}. "
            "Use 'key', 'value', or 'key,value'.",
            err=True,
        )
        raise typer.Exit(code=1)

    resolved_schema_name = schema_name or topic_name

    try:
        fetcher = SchemaFetcher(url=resolved_sr_url, key=resolved_sr_key, secret=resolved_sr_secret)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    models_dir = project_root / meta.pipelines_dir / "models"
    output_blocks: list[str] = []
    written_path: Path | None = None

    for suffix in suffixes:
        subject = f"{topic_name}-{suffix}"
        try:
            schema, schema_type = fetcher.fetch(subject)
        except SchemaRegistryError as exc:
            typer.echo(f"Error fetching subject '{subject}': {exc}", err=True)
            raise typer.Exit(code=1) from exc

        if is_debezium_envelope(schema, schema_type):
            typer.echo(
                f"  {subject}: detected Debezium CDC envelope — using 'after' schema for columns.",
                err=True,
            )

        try:
            columns = schema_to_columns(schema, schema_type)
        except (NotImplementedError, ValueError) as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(code=1) from exc

        if len(suffixes) > 1:
            output_blocks.append(f"# --- subject: {subject} ---")

        if output == SchemaOutputMode.sources:
            output_blocks.append(render_sources_yaml(topic_name, resolved_schema_name, columns))
            written_path = models_dir / "sources.yaml"
            _upsert_source_table(
                written_path,
                meta.dbt_profile_name,
                topic_name,
                columns=[{"name": c.name, "data_type": c.data_type} for c in columns],
                description=f"Topic '{topic_name}' — registered via sl-dbt get-schema-existing-topic-to-dbt.",
            )
        else:
            output_blocks.append(render_model_yaml(resolved_schema_name, columns))
            written_path = models_dir / f"{resolved_schema_name}.yml"
            _write(written_path, output_blocks[-1])

    print("\n".join(output_blocks), end="")
    if written_path is not None:
        typer.echo(f"\nWrote {written_path.resolve()}", err=True)


if __name__ == "__main__":
    app()
