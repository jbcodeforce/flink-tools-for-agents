# Agent Description — tools/flink/cc_deploy

## Module Purpose

Deploy and undeploy Flink SQL statement groups on Confluent Cloud (via the `confluent-sql` REST API driver), run bounded snapshot queries, run continuous streaming queries, and manage Flink statement lifecycle (submit, poll phase, delete, drop table).

Consumes `deploy_manifest.json` produced by `tools/flink/manifest/` — see
[`tools/flink/AGENTS.md`](../AGENTS.md) for manifest structure and the domain-level module map.

## File Map

| File | Type | Role |
|---|---|---|
| `deploy_flink_statements.py` | CLI (Typer) | `deploy`, `undeploy`, `drop-tables`, `groups` subcommands, sharing `--sql-dir` via a `@app.callback()`. Entry point: `flink-sql-deploy` |
| `run_snapshot_query.py` | CLI (Typer) | Bounded snapshot query, returns when complete. Entry point: `flink-sql-snapshot` |
| `run_streaming_query.py` | CLI (Typer) | Continuous streaming query, runs until stopped/`--max-rows`. Entry point: `flink-sql-stream` |
| `flink_deploy.py` | Library | `confluent_sql` connection management, statement submit/deploy/undeploy, table drop, snapshot & streaming query builders and execution |
| `statement_lifecycle.py` | Library | Pure statement lifecycle API — submit, poll phase, delete, drop table, health check. No `print`/`sys.exit`; callers own UX |
| `deploy_state.py` | Library | Local per-`sql_dir` ledger of successfully-deployed statements (name + SQL hash + target), used to skip unchanged redeploys |

## Key Conventions

- Env loading order (`load_dotenv_file` in `deploy_flink_statements.py`, imported by the other two CLIs):
  1. `CONFLUENT_ENV_FILE` env var (explicit override)
  2. `~/.confluent/.env` (conventional Confluent Cloud credentials location — see root `CLAUDE.md`)
  3. `{repo_root}/.env`, where repo root is located by walking up for `references/flink/valid/`
- `statement_lifecycle.py` is the single source of truth for phase polling and terminal-state
  handling (`SUCCESS_PHASES`); `flink_deploy.py` imports from it rather than duplicating logic.
- Poll interval / timeout are configurable via `FLINK_POLL_INTERVAL` and `FLINK_STATEMENT_TIMEOUT`
  env vars (defaults 5s / 600s), read in `statement_lifecycle.py`.
- `deploy_flink_statements.py` depends on `tools.flink.manifest.manifest` (`DeployManifest`,
  `DEFAULT_MANIFEST`, `load_manifest`) for statement ordering, groups, and `drop_tables`.
- Snapshot queries force `SNAPSHOT` cursor mode; streaming queries run until `--max-rows` or
  interrupted — see docstrings in `flink_deploy.py` (`run_snapshot_query`, `run_streaming_query`).
- `deploy` skips a statement whose name + SQL file content hash was already recorded successful
  for the *same* Confluent Cloud target (env/pool/database) in `<sql_dir>/.deploy_state.json`
  (see `deploy_state.py`); editing the SQL file auto-forces a redeploy, and `--rerun` forces one
  unconditionally. `undeploy` removes the corresponding ledger entries so a later `deploy` doesn't
  wrongly skip recreating something that was just torn down. This file is machine-local and
  git-ignored — never treat it as a source of truth for what's actually running in Confluent Cloud.

## Agent Tasks

- Deploy/undeploy statement groups via `deploy_flink_statements.py` (`--sql-dir <dir> deploy|undeploy|drop-tables|groups --group <name|all>`, `deploy` also takes `--rerun`)
- Run diagnostic bounded queries via `run_snapshot_query.py` (`--table` or `--sql`, `--columns`, `--where`, `--output text|json`)
- Run diagnostic continuous queries via `run_streaming_query.py` (`--table` or `--sql`, `--max-rows`, `--output`)
- Keep `flink_deploy.py`'s `confluent_sql` wrappers in sync with Confluent SQL REST API / driver changes
- Keep `statement_lifecycle.py`'s phase constants (`SUCCESS_PHASES`) in sync with any new Flink statement terminal states
- When changing manifest fields consumed here (`groups`, `deploy_all`, `undeploy_all`, `drop_tables`), coordinate with `tools/flink/manifest/`

## Known Gaps (flag before extending)

- All three CLIs now use Typer, consistent with root `CLAUDE.md`. `deploy_flink_statements.py` shares
  `--sql-dir` across its `deploy`/`undeploy`/`drop-tables`/`groups` subcommands via a `@app.callback()`
  that resolves the manifest once into a `DeployContext` on `ctx.obj`.
- All three console-script entry points in `pyproject.toml` point at the module's Typer `app` object
  (`:app`), not a `main` function — keep that pattern for any new CLI added here.
- Because `--sql-dir` is validated eagerly in that callback, `<subcommand> --help` (e.g.
  `deploy --help`) still runs callback validation first and errors if `--sql-dir` is invalid; only
  top-level `--help` is guaranteed to work without a valid `--sql-dir`. This is an inherent
  Click/Typer group-callback limitation, not a bug to "fix" casually.

## Test Coverage

`tools/flink/tests/` covers this module across six files:

| Test file | Covers |
|---|---|
| `test_statement_lifecycle.py` | `statement_lifecycle.py` — phase classification, submit/delete/create/wait_for_phase/drop_table, using `SimpleNamespace` fakes for `conn` (no real `confluent_sql` connection) |
| `test_flink_deploy.py` | `flink_deploy.py` — config resolution, statement/table wrappers, formatting helpers, `run_snapshot_query`/`run_streaming_query` against fake cursor/connection doubles, and the deploy-ledger skip/rerun/forget wiring in `deploy_statements`/`undeploy_statements`/`full_undeploy` |
| `test_deploy_state.py` | `deploy_state.py` — hash/target key derivation, ledger load/save round-trips, `is_already_deployed`/`mark_deployed`/`forget_deployed` semantics |
| `test_deploy_flink_statements.py` | env/dotenv resolution helpers and the `deploy`/`undeploy`/`drop-tables`/`groups` Typer subcommands (including `--rerun`) via `CliRunner` |
| `test_run_snapshot_query.py` | the `run_snapshot_query` Typer CLI's argument validation and dispatch, via `CliRunner` |
| `test_run_streaming_query.py` | the `run_streaming_query` Typer CLI's argument validation and dispatch, via `CliRunner` |

All library-level tests mock the `confluent_sql` connection/cursor rather than hitting Confluent
Cloud — no real credentials are required to run `uv run pytest tools/flink/`. Tests that touch
`load_dotenv_file`'s `~/.confluent/.env` fallback path redirect `$HOME` to a temp directory so they
never read the developer's real credentials file.

## Success Checks

```bash
uv run flink-sql-deploy --help
uv run flink-sql-snapshot --help
uv run flink-sql-stream --help
uv run pytest tools/flink/tests/test_statement_lifecycle.py tools/flink/tests/test_flink_deploy.py \
  tools/flink/tests/test_deploy_state.py tools/flink/tests/test_deploy_flink_statements.py \
  tools/flink/tests/test_run_snapshot_query.py tools/flink/tests/test_run_streaming_query.py -q
```
