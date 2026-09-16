@AGENTS.md

## Development Conventions

- **Package manager:** always use `uv`. Never use `pip install` directly.
- **Virtual env:** always `uv sync` before running; use `uv run <cmd>` for all tool invocations.
- **Python version:** 3.12+. Check `.python-version` for the pinned version.
- **Imports:** use absolute package imports from the `tools` namespace (e.g. `from tools.flink.manifest import manifest`). Never use `sys.path` manipulation.
- **CLI framework:** Typer for CLI tools. 
- **Credentials:** always load from environment variables or `.env` via `python-dotenv`. Never hardcode secrets.
- **Dry-run first:** all write operations must support a `--dry-run` flag that prints the intended changes without applying them.

## Skill Conventions

- Each skill lives in `skills/<domain>/SKILL.md`.
- Skill frontmatter must include `name`, `description`, `trigger_phrases`, and `prerequisites`.
- Skills invoke CLI tools via `uv run <entry-point> [args]` shell commands.
- Document all required env vars in the skill's prerequisites section.
- Provide at least two concrete usage examples per skill.

## Testing

- Tests live under `tools/<domain>/tests/`.
- Run with `uv run pytest tools/<domain>/tests/`.
- Use `typer.testing.CliRunner` for Typer CLIs and standard `subprocess` for argparse CLIs.
- Every new CLI command needs at least one test covering the happy path.

## Documentation

- Documentation lives in `docs/` and is built with MkDocs Material.
- Each domain has its own page under `docs/<domain>/index.md`.
- Run `uv run mkdocs serve` to preview; `uv run mkdocs build --strict` must pass with no warnings.


