# Agent Description — flink-tools-for-agents

## Repository Overview

This repository contains reusable CLI tools for managing Confluent Cloud Flink SQL projects,
deployments, dbt streaming model migrations, and Kafka/Schema Registry operations on Confluent Cloud.

Each tool domain is packaged as a Python module under `tools/` and backed by a Bob/Claude agent
skill under `skills/`, enabling AI agents to invoke the tools via shell commands.

This is **not** a learning or demo repository. All code here is meant to be stable, testable,
and importable by other projects.

## Repository Structure

| Path | Description | Detail |
|---|---|---|
| `tools/flink/` | Flink SQL deploy & manifest tools | See [`tools/flink/AGENTS.md`](tools/flink/AGENTS.md) |
| `tools/dbt/` | dbt streaming migration & project tools | See [`tools/dbt/AGENTS.md`](tools/dbt/AGENTS.md) |
| `tools/kafka/` | Kafka & Schema Registry tools | See [`tools/kafka/AGENTS.md`](tools/kafka/AGENTS.md) |
| `skills/` | Bob/Claude skill definitions (SKILL.md) | Keep in sync with CLI interfaces |
| `docs/` | MkDocs user-facing documentation | Keep in sync with CLI changes |

> **Narrowing context:** When working within a single domain, read only that domain's `AGENTS.md`.
> Return here only when the task spans multiple domains or touches `skills/` or `docs/`.

## Agent Capabilities & Responsibilities

### Core Competencies Required
1. **Apache Flink & Confluent Cloud** — Flink SQL, statement lifecycle, Confluent SQL REST API
2. **dbt streaming** — dbt-confluent materialization, streaming tables, shift-left patterns
3. **Kafka & Schema Registry** — Confluent Kafka Python client, Avro/JSON schema management
4. **Python tooling** — Typer CLIs, Pydantic models, uv package management
5. **Documentation** — MkDocs Material, skill authoring

### Primary Agent Tasks
- **Code maintenance:** Keep all CLI tools functional and up-to-date with dependency changes
- **Skill maintenance:** Keep `skills/*/SKILL.md` files accurate and complete
- **Test coverage:** Ensure every CLI command has at least one test
- **Documentation sync:** Ensure `docs/` reflects current CLI interfaces
- **Dependency updates:** Review and apply package updates; never pin to EOL versions

### Agent Interaction Guidelines
- Use `uv run <cmd>` for all tool invocations — never activate the venv manually
- Always run `--dry-run` before `--write` when testing migration tools
- Never commit `.env` files or hardcoded credentials
- Keep the `tools/` namespace clean — no demo or study scripts here

### Skills & Docs
- Skill changes: update trigger phrases when new commands are added; verify examples against installed CLIs
- Docs changes: run `uv run mkdocs build --strict` and fix any warnings; add a new page for every new CLI command

## Success Metrics
- All entry points callable with `--help` after `uv sync --extra all`
- `uv run pytest` passes across all three domain test suites
- `uv run mkdocs build --strict` completes with no warnings
- Each skill has working examples verified against the installed CLIs
