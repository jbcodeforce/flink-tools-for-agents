#!/usr/bin/env python3
"""
Run a snapshot query against a Flink table on Confluent Cloud (confluent-sql REST API).

Examples:
  uv run python -m cc_deploy.run_snapshot_query --table orders --limit 10
  uv run python -m cc_deploy.run_snapshot_query --table orders --columns "order_id, amount" --where "amount > 100"
  uv run python -m cc_deploy.run_snapshot_query --sql "SELECT COUNT(*) AS cnt FROM orders" --output json
"""

from __future__ import annotations

import sys
from enum import Enum
from typing import Optional

import typer

from tools.flink.cc_deploy.deploy_flink_statements import load_dotenv_file
from tools.flink.cc_deploy.flink_deploy import (
    STATEMENT_TIMEOUT_SEC,
    build_select_sql,
    default_snapshot_statement_name,
    print_snapshot_result,
    run_snapshot_query,
)


class OutputFormat(str, Enum):
    table = "table"
    json = "json"
    csv = "csv"


app = typer.Typer(
    add_completion=False,
    help="Run a snapshot query on a Confluent Cloud Flink table.",
)


@app.command()
def main(
    table: Optional[str] = typer.Option(
        None,
        "--table",
        help="Table to query (simple name or fully qualified identifier)",
    ),
    sql: Optional[str] = typer.Option(
        None,
        "--sql",
        help="Full SQL to run as a snapshot query (overrides --table builder options)",
    ),
    columns: str = typer.Option(
        "*",
        "--columns",
        help="Column list for generated SELECT when using --table (default: *)",
    ),
    where: Optional[str] = typer.Option(
        None,
        "--where",
        help="Optional WHERE clause (without the WHERE keyword) for --table",
    ),
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        help="Optional LIMIT for generated SELECT when using --table",
    ),
    statement_name: Optional[str] = typer.Option(
        None,
        "--statement-name",
        help="Optional Flink statement name (default: snapshot-<table>-<timestamp>)",
    ),
    output: OutputFormat = typer.Option(
        OutputFormat.table,
        "--output",
        help="Output format (default: table)",
    ),
    json: bool = typer.Option(
        False,
        "--json",
        help="Shorthand for --output json",
    ),
    as_dict: bool = typer.Option(
        False,
        "--as-dict",
        help="Fetch rows as dicts internally (json output always uses column names)",
    ),
    timeout: int = typer.Option(
        STATEMENT_TIMEOUT_SEC,
        "--timeout",
        help=f"Statement timeout in seconds (default: {STATEMENT_TIMEOUT_SEC})",
    ),
    keep_statement: bool = typer.Option(
        False,
        "--keep-statement",
        help="Do not delete the Flink statement after fetching results",
    ),
    quiet_meta: bool = typer.Option(
        False,
        "--quiet-meta",
        help="Suppress statement metadata on stderr",
    ),
) -> None:
    """Load environment variables and run the snapshot query."""
    load_dotenv_file()

    if not table and not sql:
        typer.echo("Error: one of --table or --sql is required.", err=True)
        raise typer.Exit(code=1)
    if table and sql:
        typer.echo("Error: --table and --sql are mutually exclusive.", err=True)
        raise typer.Exit(code=1)

    resolved_output = OutputFormat.json if json else output

    if sql:
        resolved_sql = sql.strip()
        resolved_name = statement_name or default_snapshot_statement_name("custom")
    else:
        assert table is not None
        try:
            resolved_sql = build_select_sql(
                table,
                columns=columns,
                limit=limit,
                where=where,
            )
        except ValueError as exc:
            print(exc, file=sys.stderr)
            raise typer.Exit(code=1) from exc
        resolved_name = statement_name or default_snapshot_statement_name(table)

    try:
        result = run_snapshot_query(
            resolved_sql,
            statement_name=resolved_name,
            as_dict=as_dict,
            timeout=timeout,
            delete_statement=not keep_statement,
        )
    except (RuntimeError, ValueError) as exc:
        print(exc, file=sys.stderr)
        raise typer.Exit(code=1) from exc

    print_snapshot_result(
        result,
        output=resolved_output.value,
        show_meta=not quiet_meta,
    )


if __name__ == "__main__":
    app()
