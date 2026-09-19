#!/usr/bin/env python3
"""Migrate Flink INSERT INTO DML statements to dbt streaming_table models."""

from __future__ import annotations

from dataclasses import asdict
import sys
import json
from pathlib import Path
from typing import Annotated
import typer

from tools.dbt.flink_dbt_migrate.sl_discovery_mgr import (
    crawl_pipeline_folder,
    load_excluded_folders,
    _upstream_ddl_map_from_pipeline_def,
    _pipeline_models_from_pipeline_def,
    _find_pipelines_parent,
)
from tools.dbt.flink_dbt_migrate.discover_deps import build_pipelines_ddl_index
from tools.dbt.flink_dbt_migrate.scaffold_missing_raws import (
    find_undeclared_tables,
    infer_columns_for_table,
    generate_ddl_sql,
    generate_dml_sql,
    generate_sources_yaml,
)
from tools.dbt.flink_dbt_migrate.tracking_store import TrackingStore
from tools.dbt.flink_dbt_migrate.migrate import (
    migrate_dml_to_dbt,
    migrate_values_dml_to_seed,
)
from tools.dbt.flink_dbt_migrate.flink_sql_processor import (
    is_values_insert,
        _get_logger,
)


app = typer.Typer(add_completion=False)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_ref_table(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise typer.BadParameter(f"Expected TABLE=MODEL mapping, got: {value!r}")
    table, model = value.split("=", 1)
    table = table.strip()
    model = model.strip()
    if not table or not model:
        raise typer.BadParameter(f"Expected TABLE=MODEL mapping, got: {value!r}")
    return table, model


def run_migrate_seed(
    statement_file: Path,
    seeds_dir: Path,
    *,
    ddl_file: Path | None,
    seed_name: str | None,
    write: bool,
    force: bool,
    check: bool,
) -> None:
    _get_logger().info(
        "run_migrate_seed | statement_file=%s seeds_dir=%s seed_name=%s write=%s force=%s check=%s",
        statement_file, seeds_dir, seed_name, write, force, check,
    )
    try:
        result = migrate_values_dml_to_seed(
            statement_file,
            seeds_dir,
            ddl_file=ddl_file,
            seed_name=seed_name,
            force=force,
        )
    except (ValueError, FileNotFoundError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    existing_csv = (
        result.csv_path.read_text(encoding="utf-8") if result.csv_path.exists() else ""
    )
    existing_schema = (
        result.schema_path.read_text(encoding="utf-8")
        if result.schema_path.exists()
        else ""
    )
    would_change = existing_csv != result.csv_text or existing_schema != result.schema_yml

    if check and would_change:
        typer.echo(
            f"Output differs from {result.csv_path} / {result.schema_path}; run with --write",
            err=True,
        )
        raise typer.Exit(1)

    if write:
        seeds_dir.mkdir(parents=True, exist_ok=True)
        if result.csv_path.exists() and not force:
            typer.echo(
                f"Seed already exists: {result.csv_path} (use --force to overwrite)",
                err=True,
            )
            raise typer.Exit(1)
        result.csv_path.write_text(result.csv_text, encoding="utf-8")
        result.schema_path.write_text(result.schema_yml, encoding="utf-8")
        typer.echo(f"Wrote {result.csv_path}")
        typer.echo(f"Wrote {result.schema_path}")
        typer.echo(f"DDL source: {result.ddl_path}", err=True)
    else:
        print("# --- seed csv ---")
        print(result.csv_text, end="")
        print("# --- schema.yml ---")
        print(result.schema_yml, end="")
        print(f"# DDL source: {result.ddl_path}", file=sys.stderr)


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

@app.command()
def migrate_sl_folder(
    pipeline_dir: Annotated[
        Path,
        typer.Argument(help="Shift-left pipelines folder (or sub-folder) to crawl"),
    ],
    dbt_project_dir: Annotated[
        Path,
        typer.Argument(help="dbt project root; models are written under models/"),
    ],
    write: Annotated[
        bool,
        typer.Option("--write", help="Write output files (default: dry-run)"),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Overwrite existing models and schema.yml entries"),
    ] = False,
    exclude_file: Annotated[
        Path | None,
        typer.Option(
            "--exclude-file",
            "-e",
            help="Path to text file containing folder paths to exclude from migration",
        ),
    ] = None,
    product: Annotated[
        str | None,
        typer.Option("--product", "-p", help="Filter migration to a single product by name"),
    ] = None,
) -> None:
    """Crawl a shift-left pipelines folder and migrate every table to dbt.

    Each table must have a ``sql_scripts/`` directory containing ``ddl.{table}.sql``
    and ``dml.{table}.sql``.  The source hierarchy is mirrored into the dbt project:

        pipelines/dimensions/customer/  →  dbt_project/models/dimensions/customer/
    """
    _get_logger().info(
        "migrate_sl_folder | pipeline_dir=%s dbt_project_dir=%s write=%s force=%s exclude_file=%s product=%s",
        pipeline_dir, dbt_project_dir, write, force, exclude_file, product,
    )
    pipeline_dir = pipeline_dir.resolve()
    dbt_project_dir = dbt_project_dir.resolve()

    if not pipeline_dir.is_dir():
        typer.echo(f"Pipeline directory not found: {pipeline_dir}", err=True)
        raise typer.Exit(1)

    excluded_folders: set[Path] = set()
    if exclude_file is not None:
        exclude_file = exclude_file.resolve()
        try:
            excluded_folders = load_excluded_folders(exclude_file, base_dir=pipeline_dir)
        except FileNotFoundError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(1)
        typer.echo(f"Loaded {len(excluded_folders)} excluded folder path(s) from {exclude_file}")

    typer.echo(f"Crawling {pipeline_dir} ...")
    entries = crawl_pipeline_folder(pipeline_dir, excluded_folders=excluded_folders, product=product)

    if not entries:
        typer.echo("No tables discovered (no sql_scripts/ directories with dml.*.sql found).")
        raise typer.Exit(0)

    # Print inventory
    product_suffix = f" (filtered to product: {product})" if product else ""
    typer.echo(f"\nDiscovered {len(entries)} table(s){product_suffix}:")
    col = max(len(e.table_name) for e in entries)
    for e in entries:
        typer.echo(f"  {e.table_name:<{col}} ({e.relative_path}) seed: {e.is_seed}")
    typer.echo("")

    if not write:
        typer.echo("Dry-run mode — pass --write to generate dbt model files.")
        raise typer.Exit(0)

    models_root = dbt_project_dir / "models"
    seeds_root = dbt_project_dir / "seeds"
    migrated = 0
    skipped = 0
    failures: list[tuple[str, str]] = []

    tracking_path = dbt_project_dir / "tracking.yml"
    store = TrackingStore.load(tracking_path)

    typer.echo("Building global DDL index ...")
    global_ddl_index = build_pipelines_ddl_index(pipeline_dir)
    typer.echo(f"Global DDL index: {len(global_ddl_index)} tables indexed.")

    known_models: set[str] = {entry.table_name for entry in entries}
    for entry in entries:
        known_models.update(entry.pipeline_models)

    for entry in entries:
        if not store.should_migrate(entry, force=force):
            skipped += 1
            typer.echo(f"  →  {entry.table_name}: skipped (unchanged)")
            continue

        if entry.is_seed:
            try:
                seed_result = migrate_values_dml_to_seed(
                    entry.dml_path,
                    seeds_root,
                    ddl_file=entry.ddl_path,
                    force=force,
                )
                seeds_root.mkdir(parents=True, exist_ok=True)
                seed_result.csv_path.write_text(seed_result.csv_text, encoding="utf-8")
                seed_result.schema_path.write_text(seed_result.schema_yml, encoding="utf-8")
                typer.echo(
                    f"  ✓  {entry.table_name}: wrote {seed_result.csv_path.relative_to(dbt_project_dir)}"
                )
                store.record(
                    entry.table_name,
                    status="done",
                    sha256=entry.dml_sha256,
                    relative_path=str(entry.relative_path),
                )
                migrated += 1
            except Exception as exc:  # noqa: BLE001
                failures.append((entry.table_name, str(exc)))
                store.record(
                    entry.table_name,
                    status="failed",
                    sha256=entry.dml_sha256,
                    relative_path=str(entry.relative_path),
                    error=str(exc),
                )
                typer.echo(f"  ✗  {entry.table_name}: {exc}", err=True)
            continue

        target_dir = models_root / entry.relative_path
        try:
            result = migrate_dml_to_dbt(
                statement_file=entry.dml_path,
                target_dir=target_dir,
                dbt_project_dir=dbt_project_dir,
                ddl_file=entry.ddl_path,
                force=force,
                upstream_ddl_map=entry.upstream_ddl_map,
                global_ddl_index=global_ddl_index,
                known_models=known_models,
            )
            target_dir.mkdir(parents=True, exist_ok=True)
            result.model_path.write_text(result.model_sql, encoding="utf-8")
            result.schema_path.write_text(result.schema_yml, encoding="utf-8")
            typer.echo(f"  ✓  {entry.table_name}: wrote {result.model_path.relative_to(dbt_project_dir)}")
            if result.sources_yml is not None and result.sources_path is not None:
                result.sources_path.parent.mkdir(parents=True, exist_ok=True)
                result.sources_path.write_text(result.sources_yml, encoding="utf-8")
            store.record(
                entry.table_name,
                status="done",
                sha256=entry.dml_sha256,
                relative_path=str(entry.relative_path),
            )
            migrated += 1
        except Exception as exc:  # noqa: BLE001
            failures.append((entry.table_name, str(exc)))
            store.record(
                entry.table_name,
                status="failed",
                sha256=entry.dml_sha256,
                relative_path=str(entry.relative_path),
                error=str(exc),
            )
            typer.echo(f"  ✗  {entry.table_name}: {exc}", err=True)

    store.save(tracking_path)
    typer.echo(f"\n{migrated} migrated, {skipped} skipped, {len(failures)} failed.")
    if failures:
        raise typer.Exit(1)


@app.command()
def migrate_one_file(
    statement_file: Annotated[
        Path,
        typer.Argument(help="Flink DML file (INSERT INTO ... SELECT)"),
    ],
    target_dir: Annotated[
        Path,
        typer.Argument(help="dbt models subfolder to write {model}.sql and schema.yml"),
    ],
    ddl_file: Annotated[
        Path | None,
        typer.Option("--ddl-file", help="Override auto-discovered DDL file"),
    ] = None,
    model_name: Annotated[
        str | None,
        typer.Option(
            "--model-name",
            help="Output model name (default: INSERT INTO target table)",
        ),
    ] = None,
    materialized: Annotated[
        str,
        typer.Option(
            "--materialized",
            help="dbt materialization (default: streaming_table)",
        ),
    ] = "streaming_table",
    ref_table: Annotated[
        list[str],
        typer.Option(
            "--ref-table",
            metavar="TABLE=MODEL",
            help="Override {{ ref() }} mapping for an upstream table",
        ),
    ] = [],
    write: Annotated[
        bool,
        typer.Option("--write", help="Write output files (default: dry-run to stdout)"),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite existing model and replace schema.yml entry",
        ),
    ] = False,
    check: Annotated[
        bool,
        typer.Option("--check", help="Exit 1 if output would change (for CI)"),
    ] = False,
    dbt_project_dir: Annotated[
        Path | None,
        typer.Option(
            "--dbt-project-dir",
            help="dbt project root (default: auto-discover from target_dir)",
        ),
    ] = None,
    dbt_target: Annotated[
        str,
        typer.Option("--dbt-target", help="dbt target name (default: dev)"),
    ] = "dev",
    dbt_profiles_dir: Annotated[
        Path | None,
        typer.Option(
            "--dbt-profiles-dir",
            help="dbt profiles directory (default: ~/.dbt)",
        ),
    ] = None,
    source_project_dir: Annotated[
        Path | None,
        typer.Option(
            "--source-project-dir",
            help="Flink SQL project to search for upstream DDLs (default: DML file directory)",
        ),
    ] = None,
    source_name: Annotated[
        str | None,
        typer.Option(
            "--source-name",
            help="dbt source group name in sources.yaml (default: sanitized DML folder name)",
        ),
    ] = None,
    no_sources: Annotated[
        bool,
        typer.Option(
            "--no-sources",
            help="Skip upstream source discovery; keep ref()-only rewrite",
        ),
    ] = False,
    seed_name: Annotated[
        str | None,
        typer.Option(
            "--seed-name",
            help="Output seed name for INSERT INTO ... VALUES files "
            "(default: INSERT INTO target table)",
        ),
    ] = None,
) -> None:
    _get_logger().info(
        "migrate_one_file | statement_file=%s target_dir=%s model_name=%s "
        "materialized=%s write=%s force=%s check=%s no_sources=%s",
        statement_file, target_dir, model_name, materialized, write, force, check, no_sources,
    )
    if not statement_file.is_file():
        typer.echo(f"Statement file not found: {statement_file}", err=True)
        raise typer.Exit(1)
    print()
    print('=' * 40, " INPUT ", "=" * 20)
    print(f"Running migrate_dml_to_dbt with:")
    print(f"  statement_file: {statement_file}")
    print(f"  target_dir: {target_dir}")
    print(f"  ddl_file: {ddl_file}")
    mn=model_name if model_name else 'auto-derived from statement file'
    print(f"  model_name: {mn}")
    print(f"  materialized: {materialized}")
    print(f"  force: {force}")
    print(f"  check: {check}")
    print(f"  dbt_project_dir: {dbt_project_dir}")
    print(f"  dbt_target: {dbt_target}")
    print(f"  dbt_profiles_dir: {dbt_profiles_dir}")
    print(f"  source_project_dir: {source_project_dir}")
    print(f"  source_name: {source_name}")
    print(f"  no_sources: {no_sources}")
    print(f"  seed_name: {seed_name}")
    if is_values_insert(statement_file.read_text(encoding="utf-8")):
        run_migrate_seed(
            statement_file,
            target_dir,
            ddl_file=ddl_file,
            seed_name=seed_name or model_name,
            write=write,
            force=force,
            check=check,
        )
        return

    ref_overrides = dict(parse_ref_table(item) for item in ref_table)
    profiles_dir = dbt_profiles_dir.expanduser() if dbt_profiles_dir else None

    # Auto-load upstream DDL map from pipeline_definition.json when present.
    # table_dir is the pipeline folder that contains sql-scripts/ (e.g. fct_user_per_group/)
    table_dir = statement_file.resolve().parent.parent
    pipelines_parent = _find_pipelines_parent(table_dir)
    auto_upstream_ddl_map = _upstream_ddl_map_from_pipeline_def(table_dir, pipelines_parent)
    known_pipeline_models = _pipeline_models_from_pipeline_def(table_dir, pipelines_parent)

    # If target_dir doesn't already end with the pipeline folder name, append it so
    # the dbt hierarchy mirrors the source hierarchy.
    # e.g. models/crm  →  models/crm/fct_user_per_group
    if target_dir.resolve().name != table_dir.name:
        target_dir = target_dir / table_dir.name
        _get_logger().debug(
            "migrate_one_file | target_dir adjusted to include table folder: %s", target_dir
        )

    _get_logger().debug(
        "migrate_one_file | auto_upstream_ddl_map=%s  effective_target_dir=%s",
        list(auto_upstream_ddl_map.keys()), target_dir,
    )

    print(f"  ref_overrides: {ref_overrides}")
    print(f"  effective_target_dir: {target_dir}")
    print(f"  auto_upstream_ddl_map keys: {list(auto_upstream_ddl_map.keys())}")
    print('=' * 100)
    try:
        result = migrate_dml_to_dbt(
            statement_file,
            target_dir,
            ddl_file=ddl_file,
            model_name=model_name,
            materialized=materialized,
            ref_overrides=ref_overrides,
            dbt_project_dir=dbt_project_dir,
            force=force,
            source_project_dir=source_project_dir,
            source_name=source_name,
            resolve_sources=not no_sources,
            upstream_ddl_map=auto_upstream_ddl_map,
            known_models=known_pipeline_models,
        )
    except (ValueError, FileNotFoundError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    existing_model = (
        result.model_path.read_text(encoding="utf-8")
        if result.model_path.exists()
        else ""
    )
    existing_schema = (
        result.schema_path.read_text(encoding="utf-8")
        if result.schema_path.exists()
        else ""
    )
    existing_sources = (
        result.sources_path.read_text(encoding="utf-8")
        if result.sources_path and result.sources_path.exists()
        else ""
    )

    would_change = (
        existing_model != result.model_sql
        or existing_schema != result.schema_yml
        or (result.sources_yml is not None and existing_sources != result.sources_yml)
    )

    if check and would_change:
        typer.echo(
            f"Output differs from {result.model_path} / {result.schema_path}; "
            "run with --write",
            err=True,
        )
        raise typer.Exit(1)
    typer.echo(f"result= {result}")
    if write:
        target_dir.mkdir(parents=True, exist_ok=True)
        if result.model_path.exists() and not force:
            typer.echo(
                f"Model already exists: {result.model_path} (use --force to overwrite)",
                err=True,
            )
            raise typer.Exit(1)
        result.model_path.write_text(result.model_sql, encoding="utf-8")
        result.schema_path.write_text(result.schema_yml, encoding="utf-8")
        typer.echo(f"Wrote {result.model_path}")
        typer.echo(f"Wrote {result.schema_path}")
        if result.sources_yml is not None and result.sources_path is not None:
            result.sources_path.parent.mkdir(parents=True, exist_ok=True)
            result.sources_path.write_text(result.sources_yml, encoding="utf-8")
            typer.echo(f"Wrote {result.sources_path}")
        typer.echo(f"DDL source: {result.ddl_path}", err=True)
        if result.upstream_tables:
            typer.echo(
                f"Upstream tables: {', '.join(result.upstream_tables)}",
                err=True,
            )

    print("# --- model ---")
    print(result.model_sql, end="")
    print("# --- schema.yml ---")
    print(result.schema_yml, end="")
    if result.sources_yml is not None:
        print("# --- sources.yaml ---")
        print(result.sources_yml, end="")
    print(f"# DDL source: {result.ddl_path}", file=sys.stderr)



@app.command()
def scaffold_missing_raws(
    pipelines_dir: Annotated[
        Path,
        typer.Argument(help="Root of the Flink pipelines tree to scan (e.g. ./pipelines)"),
    ],
    dbt_project_dir: Annotated[
        Path,
        typer.Argument(
            help="dbt project root — scaffolded files go under <dbt_project_dir>/models/raws/"
        ),
    ],
    profile_name: Annotated[
        str,
        typer.Option(
            "--profile",
            help="dbt source profile name used in sources.yaml (default: 'cc_flink')",
        ),
    ] = "cc_flink",
    write: Annotated[
        bool,
        typer.Option("--write", help="Write files to disk; without this flag only a dry-run report is printed"),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Overwrite existing files in models/raws/"),
    ] = False,
) -> None:
    """Scaffold Flink DDL + synthetic DML for tables referenced in pipelines but never declared.

    Scans every DML file under PIPELINES_DIR, finds table names referenced in FROM/JOIN
    clauses that have no matching CREATE TABLE DDL anywhere in the tree, then generates:

    \\b
      models/raws/<table_name>/ddl.<table_name>.sql  — CREATE TABLE (columns inferred from usage)
      models/raws/<table_name>/dml.<table_name>.sql  — INSERT synthetic rows for CI testing
      models/raws/sources.yaml                        — dbt source registration (merged idempotently)

    Run without --write to preview the report and generated SQL without touching the filesystem.

    \\b
    Example
    -------
      flink-sql-migrate-dbt scaffold-missing-raws ./pipelines ./my_dbt_project --write
    """
    log = _get_logger()
    pipelines_dir = pipelines_dir.resolve()
    dbt_project_dir = dbt_project_dir.resolve()

    typer.echo(f"Scanning {pipelines_dir} for undeclared tables …")
    undeclared = find_undeclared_tables(pipelines_dir)

    if not undeclared:
        typer.echo("✓  No undeclared tables found — all referenced tables have DDL files.")
        return

    typer.echo(f"\nFound {len(undeclared)} undeclared table(s):\n")

    # Build the DDL index once so column inference can reuse it.
    ddl_index = build_pipelines_ddl_index(pipelines_dir)

    raws_dir = dbt_project_dir / "models" / "raws"
    table_entries: list[tuple[str, list]] = []

    for table_name, dml_paths in sorted(undeclared.items()):
        typer.echo(f"  {table_name}")
        typer.echo(f"    referenced in: {', '.join(p.name for p in dml_paths)}")

        columns = infer_columns_for_table(table_name, dml_paths, ddl_index)
        typer.echo(f"    columns inferred: {len(columns)}")
        for col in columns:
            review_flag = "  ← TODO" if col.needs_review else ""
            typer.echo(f"      {col.name:<30} {col.flink_type}{review_flag}")

        ddl_sql = generate_ddl_sql(table_name, columns)
        dml_sql = generate_dml_sql(table_name, columns)

        table_dir = raws_dir / table_name
        ddl_path = table_dir / f"ddl.{table_name}.sql"
        dml_path_out = table_dir / f"dml.{table_name}.sql"

        if not write:
            typer.echo(f"\n    [dry-run] {ddl_path}")
            typer.echo(ddl_sql)
            typer.echo(f"    [dry-run] {dml_path_out}")
            typer.echo(dml_sql)
        else:
            table_dir.mkdir(parents=True, exist_ok=True)

            if ddl_path.exists() and not force:
                typer.echo(f"    skip (exists) {ddl_path}  — use --force to overwrite", err=True)
            else:
                ddl_path.write_text(ddl_sql, encoding="utf-8")
                typer.echo(f"    wrote {ddl_path}")
                log.info("scaffold_missing_raws | wrote %s", ddl_path)

            if dml_path_out.exists() and not force:
                typer.echo(f"    skip (exists) {dml_path_out}  — use --force to overwrite", err=True)
            else:
                dml_path_out.write_text(dml_sql, encoding="utf-8")
                typer.echo(f"    wrote {dml_path_out}")
                log.info("scaffold_missing_raws | wrote %s", dml_path_out)

        table_entries.append((table_name, columns))
        typer.echo("")

    # Always show / write sources.yaml
    sources_path = raws_dir / "sources.yaml"
    sources_yaml = generate_sources_yaml(profile_name, table_entries, existing_path=sources_path)

    if not write:
        typer.echo(f"[dry-run] {sources_path}")
        typer.echo(sources_yaml)
    else:
        raws_dir.mkdir(parents=True, exist_ok=True)
        sources_path.write_text(sources_yaml, encoding="utf-8")
        typer.echo(f"wrote {sources_path}")
        log.info("scaffold_missing_raws | wrote %s", sources_path)

    if not write:
        typer.echo("\nRun with --write to create the files above.")


if __name__ == "__main__":
    app()
