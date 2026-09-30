#!/usr/bin/env python3
"""
Generic CLI to deploy Flink SQL statement groups via confluent-sql (REST API).

Usage:
  uv run flink-sql-deploy --sql-dir ../11-puzzles/cart_update deploy --group all
  uv run flink-sql-deploy --sql-dir ../11-puzzles/cart_update undeploy --group all
  uv run flink-sql-deploy --sql-dir ../11-puzzles/cart_update drop-tables
  uv run flink-sql-deploy --sql-dir ../04-joins/cc groups

Each pipeline folder supplies deploy_manifest.json listing statement groups, SQL files,
undeploy_all order, and drop_tables for full teardown.
Environment: loads ``DOTENV_FILE`` or ``{repo_root}/.env`` (same convention as the monorepo skills).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import typer
from dotenv import load_dotenv

from tools.flink.manifest.manifest import DEFAULT_MANIFEST, DeployManifest, load_manifest

from tools.flink.cc_deploy.flink_deploy import (
    deploy_statements,
    drop_tables as flink_drop_tables,
    full_undeploy,
    get_config,
    undeploy_statements,
)

_DOTENV_ENV_VAR = "DOTENV_FILE"


def find_repo_root(start: Path | None = None) -> Path:
    """Walk parents until ``references/flink/valid`` exists."""
    here = (start or Path(__file__)).resolve()
    search = here if here.is_dir() else here.parent
    for parent in [search, *search.parents]:
        if (parent / "references" / "flink" / "valid").is_dir():
            return parent
    raise FileNotFoundError(
        "Could not locate repo root containing references/flink/valid "
        f"(started from {here})"
    )


def resolve_dotenv_path(repo_root: Path | None = None) -> Path | None:
    """Resolve ``DOTENV_FILE`` or ``{repo_root}/.env``."""
    root = repo_root or find_repo_root()
    raw = os.environ.get(_DOTENV_ENV_VAR)
    if raw:
        path = Path(raw)
        if not path.is_absolute():
            path = (root / path).resolve()
    else:
        path = root / ".env"
    return path if path.is_file() else None


def load_dotenv_file(*, start: Path | None = None) -> bool:
    """Load env from ``CONFLUENT_ENV_FILE``, ``~/.confluent/.env``, or repo-root ``.env``."""
    # 1. Explicit override via CONFLUENT_ENV_FILE env var
    explicit = os.environ.get("CONFLUENT_ENV_FILE")
    if explicit:
        p = Path(explicit).expanduser()
        if p.is_file():
            return load_dotenv(p, override=True)

    # 2. ~/.confluent/.env (conventional Confluent Cloud credentials location)
    default_home = Path("~/.confluent/.env").expanduser()
    if default_home.is_file():
        return load_dotenv(default_home, override=True)

    # 3. Repo-root .env (original behaviour)
    try:
        root = find_repo_root(start)
    except FileNotFoundError:
        return False
    path = resolve_dotenv_path(root)
    if path is None:
        return False
    return load_dotenv(path, override=True)


def print_groups(manifest: DeployManifest) -> None:
    """Print group names, sizes, and deploy_all / undeploy_all membership."""
    deploy_set = set(manifest.deploy_all)
    undeploy_set = set(manifest.undeploy_all)
    for name in sorted(manifest.groups):
        count = len(manifest.groups[name])
        flags: list[str] = []
        if name in deploy_set:
            flags.append("deploy_all")
        if name in undeploy_set:
            flags.append("undeploy_all")
        flag_text = f" ({', '.join(flags)})" if flags else ""
        print(f"{name}: {count} statement(s){flag_text}")


def deploy_flink_statements(
    manifest: DeployManifest,
    group: str,
    sql_dir: Path,
    config: dict[str, str],
    *,
    rerun: bool = False,
) -> None:
    """Deploy Flink SQL statements for a given group."""
    statements = manifest.statements_for(group)
    deploy_statements(
        statements,
        sql_dir=sql_dir,
        config=config,
        rerun=rerun,
    )


@dataclass
class DeployContext:
    """Shared state resolved once from ``--sql-dir`` and passed to every subcommand."""

    sql_dir: Path
    manifest: DeployManifest


app = typer.Typer(
    add_completion=False,
    help="Deploy Flink SQL statement groups to Confluent Cloud (confluent-sql REST API).",
)


@app.callback()
def _load_context(
    ctx: typer.Context,
    sql_dir: Path = typer.Option(
        ...,
        "--sql-dir",
        help="Demo folder containing SQL files and deploy_manifest.json",
    ),
) -> None:
    """Load env, resolve --sql-dir, and load its deploy_manifest.json."""
    load_dotenv_file()

    resolved = sql_dir.resolve()
    if not resolved.is_dir():
        typer.echo(f"sql-dir not found: {resolved}", err=True)
        raise typer.Exit(code=1)

    manifest_path = (resolved / DEFAULT_MANIFEST).resolve()
    if not manifest_path.is_file():
        typer.echo(f"Manifest not found: {manifest_path}", err=True)
        raise typer.Exit(code=1)

    ctx.obj = DeployContext(sql_dir=resolved, manifest=load_manifest(manifest_path))


@app.command()
def deploy(
    ctx: typer.Context,
    group: str = typer.Option(
        "all",
        "--group",
        help="Manifest group name, or 'all' (default: all)",
    ),
    rerun: bool = typer.Option(
        False,
        "--rerun",
        help=(
            "Redeploy every statement in this group even if it was already "
            "deployed unchanged (by default, unchanged statements are skipped)"
        ),
    ),
) -> None:
    """Create statements in manifest order, skipping ones already deployed unchanged."""
    context: DeployContext = ctx.obj
    config = get_config()
    try:
        deploy_flink_statements(context.manifest, group, context.sql_dir, config, rerun=rerun)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    print(f"deploy --group {group} complete.")


@app.command()
def undeploy(
    ctx: typer.Context,
    group: str = typer.Option(
        "all",
        "--group",
        help="Manifest group name, or 'all' for full teardown (default: all)",
    ),
    no_drop_tables: bool = typer.Option(
        False,
        "--no-drop-tables",
        help="With --group all, delete statements only (skip drop_tables)",
    ),
) -> None:
    """Delete statements; with --group all also drops tables from manifest."""
    context: DeployContext = ctx.obj
    config = get_config()
    try:
        if group == "all":
            full_undeploy(
                context.manifest,
                config=config,
                drop_tables_after=not no_drop_tables,
                sql_dir=context.sql_dir,
            )
        else:
            statements = context.manifest.undeploy_order(group)
            undeploy_statements(statements, config=config, sql_dir=context.sql_dir)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    print(f"undeploy --group {group} complete.")


@app.command("drop-tables")
def drop_tables_command(ctx: typer.Context) -> None:
    """Drop tables listed in manifest drop_tables (no statement deletes)."""
    context: DeployContext = ctx.obj
    if not context.manifest.drop_tables:
        typer.echo("No drop_tables defined in manifest.", err=True)
        raise typer.Exit(code=1)
    config = get_config()
    flink_drop_tables(context.manifest.drop_tables, manifest=context.manifest, config=config)
    print("drop-tables complete.")


@app.command()
def groups(ctx: typer.Context) -> None:
    """List manifest groups and statement counts."""
    context: DeployContext = ctx.obj
    print_groups(context.manifest)


if __name__ == "__main__":
    app()
