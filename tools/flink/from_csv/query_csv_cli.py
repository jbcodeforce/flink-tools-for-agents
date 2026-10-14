#!/usr/bin/env python3
"""
from_csv CLI - Generate Avro schemas and Flink SQL from CSV files.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any


import typer

from .csv_parser import CSVParser, InferredType
from .avro_schema import build_avro_schema, write_schema, validate_avro_schema
from tools.kafka.register_schema import register_schema, SchemaType

app = typer.Typer(
    name="from_csv",
    help="Generate Avro schemas, from CSV files, populate the topic, and execute a snapshot query",
)


# ---------------------------------------------------------------------------
# Templates 
# ---------------------------------------------------------------------------

FLINK_QUERY_TEMPLATE = """-- Flink SQL snapshot query template
-- Generated from CSV file
-- This query requires the Flink table to be registered in the catalog first.

-- 1. CREATE the Flink table (once, in your FLINK SQL environment)
CREATE TABLE kafka_input_table (
{{flink_ddl}}
) WITH (

);
"""

SNAPSHOT_QUERY = """
SELECT * 
FROM kafka_input_table
-- WHERE col1 > {{value}}    -- add your filters here
-- AS OF SYSTEM TIME '2024-12-01 12:00:00'  -- uncomment for exact snapshot
;"""

def _register_schema(the_schema: dict[str, Any], table_name):
    current = Path(os.environ["PWD"])
    print(current)
    where: Path =  current / "schemas" / f"{table_name}.asvc"
    write_schema(schema = the_schema, output_path = where)
    typer.echo(message=f"Created file: {where}")
    resolved_subject, schema_id, version, resolved_type = register_schema(schema_path=where, 
                    schema_type="AVRO")
    typer.echo(f"Created schema: {resolved_subject} id: {schema_id}, version: {version}")

@app.command()
def main(
    csv_path : Path,
    sql: Path = typer.Option(
         None,
        "--sql",
        help="sql query to execute. if not provided a select * from table is used",
        ),
):
    print("="*40)
    parser = CSVParser(csv_path)
    table_name = csv_path.name.replace(".csv","")
    typer.echo(message=f"Topic and table name: {table_name}")
    if not sql:
        sql = f"SELECT * FROM {table_name};"
    typer.echo(sql)
    try:
        columns = parser.parse()
        avr_schema_value = build_avro_schema(columns=columns, 
                                                record_name=table_name, 
                                                namespace="example.com")
        _register_schema(avr_schema_value, table_name)
    except Exception as e:
        typer.echo(e)
        raise  typer.Exit(code=1)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app()
