"""Tests for sl_dbt CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from confluent_kafka.schema_registry.error import SchemaRegistryError
from typer.testing import CliRunner

from tools.dbt import sl_dbt as cli
from tools.dbt.sl_dbt import ProjectMetadata, ProjectType, app, _upsert_sources_yaml, save_metadata

REQUIRED_SR_ENV_VARS = [
    "SCHEMA_REGISTRY_ENDPOINT",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_USER",
    "SCHEMA_REGISTRY_API_SECRET",
    "SCHEMA_REGISTRY_PASSWORD",
]


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(autouse=True)
def _no_real_dotenv_or_sr_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Never load the developer's real .env, and never leak real SR credentials."""
    monkeypatch.setattr(cli, "load_dotenv_file", lambda: False)
    for var in REQUIRED_SR_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def test_init_creates_pyproject_toml(cli_runner: CliRunner, tmp_path: Path) -> None:
    """init should write pipelines/pyproject.toml with uv-compatible content."""
    project_root = tmp_path / "my_project"

    result = cli_runner.invoke(
        app,
        ["init", str(project_root)],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output

    pyproject = project_root / "pipelines" / "pyproject.toml"
    assert pyproject.exists(), "pyproject.toml was not created"

    content = pyproject.read_text()
    # project name injected correctly
    assert 'name = "pipelines"' in content
    # dbt dependencies present
    assert "dbt-confluent" in content
    assert "dbt-core" in content


def test_init_idempotent_preserves_existing_files(cli_runner: CliRunner, tmp_path: Path) -> None:
    """Re-running init without --force must not overwrite files the user has edited."""
    project_root = tmp_path / "my_project"

    # First init
    cli_runner.invoke(app, ["init", str(project_root)], catch_exceptions=False)

    # Simulate a user edit
    pyproject = project_root / "pipelines" / "pyproject.toml"
    pyproject.write_text("# my custom content")

    # Second init — should leave the file alone
    result = cli_runner.invoke(app, ["init", str(project_root)], catch_exceptions=False)
    assert result.exit_code == 0
    assert pyproject.read_text() == "# my custom content"


def test_init_force_overwrites_existing_files(cli_runner: CliRunner, tmp_path: Path) -> None:
    """--force must regenerate files even when they already exist."""
    project_root = tmp_path / "my_project"

    cli_runner.invoke(app, ["init", str(project_root)], catch_exceptions=False)

    pyproject = project_root / "pipelines" / "pyproject.toml"
    pyproject.write_text("# my custom content")

    result = cli_runner.invoke(app, ["init", str(project_root), "--force"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "# my custom content" not in pyproject.read_text()
    assert "dbt-confluent" in pyproject.read_text()


# ---------------------------------------------------------------------------
# add-raw-topic tests
# ---------------------------------------------------------------------------

@pytest.fixture
def initialized_project(cli_runner: CliRunner, tmp_path: Path) -> Path:
    """Return a project_root that has already been `init`-ed."""
    project_root = tmp_path / "my_project"
    result = cli_runner.invoke(app, ["init", str(project_root)], catch_exceptions=False)
    assert result.exit_code == 0
    return project_root


def test_add_raw_topic_creates_ddl_and_dml(cli_runner: CliRunner, initialized_project: Path) -> None:
    """add-raw-topic should create ddl and dml SQL files under raws/<topic>/."""
    result = cli_runner.invoke(
        app,
        ["add-raw-topic", str(initialized_project), "raw_customers"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    raws_dir = initialized_project / "pipelines" / "models" / "raws" / "raw_customers"
    ddl = raws_dir / "ddl.raw_customers.sql"
    dml = raws_dir / "dml.raw_customers.sql"

    assert ddl.exists(), "DDL file not created"
    assert dml.exists(), "DML file not created"

    ddl_text = ddl.read_text()
    assert "CREATE TABLE IF NOT EXISTS raw_customers" in ddl_text
    assert "'kafka.topic'              = 'raw_customers'" in ddl_text

    dml_text = dml.read_text()
    assert "INSERT INTO raw_customers" in dml_text
    assert "synth-001" in dml_text


def test_add_raw_topic_registers_in_sources_yaml(cli_runner: CliRunner, initialized_project: Path) -> None:
    """add-raw-topic should register the topic in raws/sources.yaml."""
    cli_runner.invoke(
        app,
        ["add-raw-topic", str(initialized_project), "raw_orders"],
        catch_exceptions=False,
    )

    sources_path = initialized_project / "pipelines" / "models" / "raws" / "sources.yaml"
    assert sources_path.exists()

    data = yaml.safe_load(sources_path.read_text())
    tables = data["sources"][0]["tables"]
    names = [t["name"] for t in tables]
    assert "raw_orders" in names


def test_add_raw_topic_idempotent(cli_runner: CliRunner, initialized_project: Path) -> None:
    """Running add-raw-topic twice must not duplicate files or sources entries."""
    for _ in range(2):
        result = cli_runner.invoke(
            app,
            ["add-raw-topic", str(initialized_project), "raw_events"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

    sources_path = initialized_project / "pipelines" / "models" / "raws" / "sources.yaml"
    data = yaml.safe_load(sources_path.read_text())
    tables = data["sources"][0]["tables"]
    assert sum(1 for t in tables if t["name"] == "raw_events") == 1


def test_upsert_sources_yaml_multiple_topics(tmp_path: Path) -> None:
    """_upsert_sources_yaml accumulates multiple topics under the same source block."""
    sources_path = tmp_path / "sources.yaml"
    _upsert_sources_yaml(sources_path, "cc_flink", "raw_customers")
    _upsert_sources_yaml(sources_path, "cc_flink", "raw_orders")

    data = yaml.safe_load(sources_path.read_text())
    tables = data["sources"][0]["tables"]
    names = {t["name"] for t in tables}
    assert names == {"raw_customers", "raw_orders"}
    # Only one source block for cc_flink
    assert len(data["sources"]) == 1


# ---------------------------------------------------------------------------
# get-schema-existing-topic-to-dbt
# ---------------------------------------------------------------------------


class _FakeSchemaFetcher:
    """Stand-in for SchemaFetcher: no real Schema Registry / network access."""

    last_init_kwargs: dict[str, Any] = {}

    def __init__(self, url: str = "", key: str = "", secret: str = "") -> None:
        _FakeSchemaFetcher.last_init_kwargs = {"url": url, "key": key, "secret": secret}

    def fetch(self, subject: str) -> tuple[dict[str, Any], str]:
        return {"fields": [{"name": "id", "type": "string"}]}, "AVRO"


class _FailingFetchSchemaFetcher(_FakeSchemaFetcher):
    def fetch(self, subject: str) -> tuple[dict[str, Any], str]:
        raise SchemaRegistryError(404, 40401, f"Subject '{subject}' not found")


class _DebeziumSchemaFetcher(_FakeSchemaFetcher):
    """A CDC envelope where 'after' references the record 'before' defines."""

    def fetch(self, subject: str) -> tuple[dict[str, Any], str]:
        return {
            "type": "record",
            "name": "Envelope",
            "fields": [
                {
                    "name": "before",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "Value",
                            "fields": [
                                {"name": "id", "type": "string"},
                                {"name": "amount", "type": "double"},
                            ],
                        },
                    ],
                    "default": None,
                },
                {"name": "after", "type": ["null", "Value"], "default": None},
                {"name": "op", "type": "string"},
            ],
        }, "AVRO"


def _init_project(root: Path, *, profile: str = "cc_flink") -> Path:
    """Create a minimal initialized project at *root* (just sl_dbt.yaml)."""
    save_metadata(
        root,
        ProjectMetadata(
            pipelines_dir="pipelines", project_type=ProjectType.data_product, dbt_profile_name=profile
        ),
    )
    return root


def test_get_schema_requires_project_root(cli_runner: CliRunner, tmp_path: Path) -> None:
    project_root = tmp_path / "proj"  # never initialized: no sl_dbt.yaml
    result = cli_runner.invoke(app, ["get-schema-existing-topic-to-dbt", str(project_root), "orders_topic"])
    assert result.exit_code == 1
    assert "Run `init` first" in result.output


def test_get_schema_requires_sr_url(cli_runner: CliRunner, tmp_path: Path) -> None:
    project_root = _init_project(tmp_path / "proj")
    result = cli_runner.invoke(app, ["get-schema-existing-topic-to-dbt", str(project_root), "orders_topic"])
    assert result.exit_code == 1
    assert "Schema Registry URL is required" in result.output


def test_get_schema_rejects_invalid_subject_suffix(
    cli_runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = _init_project(tmp_path / "proj")
    monkeypatch.setenv("SCHEMA_REGISTRY_ENDPOINT", "https://sr.example.com")

    result = cli_runner.invoke(
        app,
        [
            "get-schema-existing-topic-to-dbt",
            str(project_root),
            "orders_topic",
            "--subject-suffix",
            "bogus",
        ],
    )
    assert result.exit_code == 1
    assert "invalid --subject-suffix" in result.output


def test_get_schema_sources_output_writes_and_merges_into_sources_yaml(
    cli_runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = _init_project(tmp_path / "proj")
    monkeypatch.setattr(cli, "SchemaFetcher", _FakeSchemaFetcher)
    monkeypatch.setenv("SCHEMA_REGISTRY_ENDPOINT", "https://sr.example.com")

    result = cli_runner.invoke(
        app, ["get-schema-existing-topic-to-dbt", str(project_root), "orders_topic"]
    )

    assert result.exit_code == 0, result.output
    assert "sources:" in result.output
    assert _FakeSchemaFetcher.last_init_kwargs["url"] == "https://sr.example.com"

    sources_path = project_root / "pipelines" / "models" / "sources.yaml"
    assert sources_path.exists()
    data = yaml.safe_load(sources_path.read_text())
    assert data["sources"][0]["name"] == "cc_flink"
    table = data["sources"][0]["tables"][0]
    assert table["name"] == "orders_topic"
    assert table["columns"] == [{"name": "id", "data_type": "string"}]


def test_get_schema_sources_output_updates_existing_table_entry(
    cli_runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Re-running with a changed schema updates the entry in place (no duplicate)."""
    project_root = _init_project(tmp_path / "proj")
    monkeypatch.setenv("SCHEMA_REGISTRY_ENDPOINT", "https://sr.example.com")

    monkeypatch.setattr(cli, "SchemaFetcher", _FakeSchemaFetcher)
    cli_runner.invoke(app, ["get-schema-existing-topic-to-dbt", str(project_root), "orders_topic"])

    class _ChangedSchemaFetcher(_FakeSchemaFetcher):
        def fetch(self, subject: str) -> tuple[dict[str, Any], str]:
            return {"fields": [{"name": "id", "type": "string"}, {"name": "amount", "type": "double"}]}, "AVRO"

    monkeypatch.setattr(cli, "SchemaFetcher", _ChangedSchemaFetcher)
    result = cli_runner.invoke(app, ["get-schema-existing-topic-to-dbt", str(project_root), "orders_topic"])
    assert result.exit_code == 0, result.output

    sources_path = project_root / "pipelines" / "models" / "sources.yaml"
    data = yaml.safe_load(sources_path.read_text())
    tables = data["sources"][0]["tables"]
    assert len(tables) == 1
    assert tables[0]["columns"] == [
        {"name": "id", "data_type": "string"},
        {"name": "amount", "data_type": "double"},
    ]


def test_get_schema_model_output_writes_standalone_yml(
    cli_runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = _init_project(tmp_path / "proj")
    monkeypatch.setattr(cli, "SchemaFetcher", _FakeSchemaFetcher)
    monkeypatch.setenv("SCHEMA_REGISTRY_ENDPOINT", "https://sr.example.com")

    result = cli_runner.invoke(
        app,
        [
            "get-schema-existing-topic-to-dbt",
            str(project_root),
            "orders_topic",
            "--output",
            "model",
            "--schema-name",
            "stg_orders",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "models:" in result.output
    assert "name: stg_orders" in result.output

    model_path = project_root / "pipelines" / "models" / "stg_orders.yml"
    assert model_path.exists()
    data = yaml.safe_load(model_path.read_text())
    assert data["models"][0]["name"] == "stg_orders"


def test_get_schema_key_and_value_adds_subject_markers(
    cli_runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = _init_project(tmp_path / "proj")
    monkeypatch.setattr(cli, "SchemaFetcher", _FakeSchemaFetcher)
    monkeypatch.setenv("SCHEMA_REGISTRY_ENDPOINT", "https://sr.example.com")

    result = cli_runner.invoke(
        app,
        [
            "get-schema-existing-topic-to-dbt",
            str(project_root),
            "orders_topic",
            "--subject-suffix",
            "key,value",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "# --- subject: orders_topic-key ---" in result.output
    assert "# --- subject: orders_topic-value ---" in result.output


def test_get_schema_uses_legacy_env_var_alias(
    cli_runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = _init_project(tmp_path / "proj")
    monkeypatch.setattr(cli, "SchemaFetcher", _FakeSchemaFetcher)
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "https://from-legacy-env.example.com")

    result = cli_runner.invoke(
        app, ["get-schema-existing-topic-to-dbt", str(project_root), "orders_topic"]
    )

    assert result.exit_code == 0, result.output
    assert _FakeSchemaFetcher.last_init_kwargs["url"] == "https://from-legacy-env.example.com"


def test_get_schema_fetch_error_exits_nonzero(
    cli_runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = _init_project(tmp_path / "proj")
    monkeypatch.setattr(cli, "SchemaFetcher", _FailingFetchSchemaFetcher)
    monkeypatch.setenv("SCHEMA_REGISTRY_ENDPOINT", "https://sr.example.com")

    result = cli_runner.invoke(
        app, ["get-schema-existing-topic-to-dbt", str(project_root), "orders_topic"]
    )

    assert result.exit_code == 1
    assert "Error fetching subject" in result.output


def test_get_schema_debezium_cdc_uses_after_and_notes_it(
    cli_runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Debezium CDC schema yields columns from 'after', with a note on stderr."""
    project_root = _init_project(tmp_path / "proj")
    monkeypatch.setattr(cli, "SchemaFetcher", _DebeziumSchemaFetcher)
    monkeypatch.setenv("SCHEMA_REGISTRY_ENDPOINT", "https://sr.example.com")

    result = cli_runner.invoke(
        app, ["get-schema-existing-topic-to-dbt", str(project_root), "orders_topic"]
    )

    assert result.exit_code == 0, result.output
    assert "detected Debezium CDC envelope" in result.output

    sources_path = project_root / "pipelines" / "models" / "sources.yaml"
    data = yaml.safe_load(sources_path.read_text())
    table = data["sources"][0]["tables"][0]
    # Columns come from 'after' (id, amount) — not the envelope's own
    # before/after/op fields.
    assert table["columns"] == [
        {"name": "id", "data_type": "string"},
        {"name": "amount", "data_type": "double"},
    ]
