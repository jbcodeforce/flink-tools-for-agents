# Agent Description — flink-tools-for-agents

## Repository Overview

This repository contains reusable CLI tools for managing Confluent Cloud Flink SQL projects, 
deployments, dbt streaming model migrations, and Kafka/Schema Registry operations on Confluent Cloud.

Each tool domain is packaged as a Python module under `tools/` and backed by a Bob/Claude agent
skill under `skills/`, enabling AI agents to invoke the tools via shell commands.

This is **not** a learning or demo repository. All code here is meant to be stable, testable,
and importable by other projects.

## Repository Structure & Agent Responsibilities

### `/tools/flink` — Flink SQL Deploy & Manifest Tools
- **Purpose:** Deploy and undeploy Flink SQL statement groups on Confluent Cloud, run snapshot
  and streaming queries, and generate deployment manifests.
- **Key modules:**
  - `cc_deploy/deploy_flink_statements.py` — CLI: deploy, undeploy, drop-tables, groups
  - `cc_deploy/run_snapshot_query.py` — CLI: bounded table/SQL queries
  - `cc_deploy/run_streaming_query.py` — CLI: continuous streaming queries
  - `cc_deploy/flink_deploy.py` — library: Confluent SQL API wrappers
  - `cc_deploy/statement_lifecycle.py` — library: statement state management
  - `manifest/manifest.py` — library: Pydantic manifest models + generators
  - `manifest/manifest_cli.py` — CLI: generate `deploy_manifest.json`
- **Agent Role:**
  - Generate manifests from Flink SQL dirs or dbt projects
  - Deploy / undeploy statement groups
  - Run diagnostic snapshot queries
  - Keep tool code in sync with `confluent-sql` API changes

### `/tools/dbt` — dbt Streaming Migration & Project Tools
- **Purpose:** Convert Flink SQL DML statements into dbt streaming models, scaffold
  shift-left dbt projects, and generate dbt YAML from SQL models or Schema Registry schemas.
- **Key modules:**
  - `flink_dbt_migrate/migrate_dml_to_dbt.py` — CLI: migrate DML files or pipeline folders
  - `sl_dbt.py` — CLI: scaffold and manage shift-left dbt projects
  - `sql_to_dbt_yaml.py` — CLI: generate dbt model YAML from SQL
  - `sr_to_dbt_yaml.py` — CLI: generate dbt source/model YAML from Schema Registry
  - `flink_sql_processor.py` — library: Flink SQL parser
  - `dbt_element_mgr.py` — library: dbt file generator/merger
- **Agent Role:**
  - Migrate Flink DML pipelines to dbt models
  - Scaffold new dbt streaming projects
  - Generate and maintain dbt schema YAML files
  - Validate migrations with `dbt compile`

### `/tools/kafka` — Kafka & Schema Registry Tools
- **Purpose:** Manage Kafka topics, register/list/delete schemas in Confluent Schema Registry,
  and clean up Flink tables.
- **Key modules:**
  - `kafka_client.py` — library: Kafka AdminClient helpers
  - `register_schema.py` — CLI: schema registration, listing, deletion
  - `drop_tables_manifest.py` — library: table-drop manifest builder
  - `table_cleanup.py` — CLI: Flink table inventory and cleanup
- **Agent Role:**
  - Register schemas before deploying Flink pipelines
  - Audit and clean up stale Flink tables
  - List Kafka topics for manifest generation

### `/skills` — Agent Skills (Bob/Claude SKILL.md)
- **Purpose:** Bob/Claude skill definitions that instruct agents how to invoke the CLI tools.
- **Agent Role:**
  - Keep SKILL.md files in sync with actual CLI interfaces
  - Update trigger phrases when new commands are added
  - Ensure usage examples reflect current entry point names

### `/docs` — MkDocs Documentation
- **Purpose:** User-facing documentation for all tools and skills.
- **Agent Role:**
  - Keep docs in sync with CLI changes
  - Run `uv run mkdocs build --strict` and fix any warnings
  - Add a new page whenever a new CLI command is introduced

## Agent Capabilities & Responsibilities

### Core Competencies Required
1. **Apache Flink & Confluent Cloud** — Flink SQL, statement lifecycle, Confluent SQL REST API
2. **dbt streaming** — dbt-confluent materialization, streaming tables, shift-left patterns
3. **Kafka & Schema Registry** — Confluent Kafka Python client, Avro/JSON schema management
4. **Python tooling** — Typer/argparse CLIs, Pydantic models, uv package management
5. **Documentation** — MkDocs Material, skill authoring

### Primary Agent Tasks
- **Code maintenance:** Keep all CLI tools functional and up-to-date with dependency changes
- **Skill maintenance:** Keep SKILL.md files accurate and complete
- **Test coverage:** Ensure every CLI command has at least one test
- **Documentation sync:** Ensure docs reflect current CLI interfaces
- **Dependency updates:** Review and apply package updates; never pin to EOL versions

### Agent Interaction Guidelines
- Use `uv run <cmd>` for all tool invocations — never activate the venv manually
- Always run `--dry-run` before `--write` when testing migration tools
- Never commit `.env` files or hardcoded credentials
- Keep the `tools/` namespace clean — no demo or study scripts here

## Success Metrics
- All entry points callable with `--help` after `uv sync --extra all`
- `uv run pytest` passes across all three domain test suites
- `uv run mkdocs build --strict` completes with no warnings
- Each skill has working examples verified against the installed CLIs
