#!/usr/bin/env python3
"""
Run a streaming query against a Flink table on Confluent Cloud (confluent-sql REST API).

Examples:
  uv run flink-sql-stream --table orders
  uv run flink-sql-stream --sql "SELECT * FROM orders WHERE amount > 100"
  uv run flink-sql-stream --table orders --max-rows 20 --output json
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
    default_streaming_statement_name,
    run_streaming_query,
)


class OutputFormat(str, Enum):
    table = "table"
    json = "json"
    csv = "csv"


app = typer.Typer(
    add_completion=False,
    help="Run a streaming query on a Confluent Cloud Flink table.",
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
        help="Full SQL to run as a streaming query (overrides --table builder options)",
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
        help="Optional Flink statement name (default: stream-<table>-<timestamp>)",
    ),
    output: OutputFormat = typer.Option(
        OutputFormat.table,
        "--output",
        help="Per-row output format (default: table)",
    ),
    as_dict: bool = typer.Option(
        False,
        "--as-dict",
        help="Fetch rows as dicts internally",
    ),
    timeout: int = typer.Option(
        STATEMENT_TIMEOUT_SEC,
        "--timeout",
        help=f"Statement startup timeout in seconds (default: {STATEMENT_TIMEOUT_SEC})",
    ),
    max_rows: Optional[int] = typer.Option(
        None,
        "--max-rows",
        help="Stop after printing this many rows (default: run until Ctrl+C)",
    ),
    keep_statement: bool = typer.Option(
        False,
        "--keep-statement",
        help="Do not delete the Flink statement after stopping",
    ),
    quiet_meta: bool = typer.Option(
        False,
        "--quiet-meta",
        help="Suppress statement metadata on stderr",
    ),
) -> None:
    """Load environment variables and run the streaming query."""
    load_dotenv_file()

    if not table and not sql:
        typer.echo("Error: one of --table or --sql is required.", err=True)
        raise typer.Exit(code=1)
    if table and sql:
        typer.echo("Error: --table and --sql are mutually exclusive.", err=True)
        raise typer.Exit(code=1)

    if sql:
        resolved_sql = sql.strip()
        resolved_name = statement_name or default_streaming_statement_name("custom")
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
        resolved_name = statement_name or default_streaming_statement_name(table)

    try:
        run_streaming_query(
            resolved_sql,
            statement_name=resolved_name,
            as_dict=as_dict,
            timeout=timeout,
            output=output.value,
            max_rows=max_rows,
            show_meta=not quiet_meta,
            delete_statement=not keep_statement,
        )
    except (RuntimeError, ValueError) as exc:
        print(exc, file=sys.stderr)
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()
