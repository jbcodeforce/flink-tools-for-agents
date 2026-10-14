
from pathlib import Path
from tools.flink.from_csv.csv_parser import CSVParser, InferredType, validate_columns
import csv
from tools.flink.from_csv.avro_schema import build_avro_schema, write_schema, validate_avro_schema

_TEST_FOLDER = Path(__file__).parent.parent


def test_cvs_parser():
    test_csv = _TEST_FOLDER / "from_csv" / "test_users.csv"
    parser = CSVParser(test_csv)
    columns = parser.parse()
    print(columns)
    assert len(columns) == 6
    assert type(columns[0]) == InferredType
    assert 'user_id' == columns[0].name
    errors = validate_columns(columns)
    assert len(errors) == 0

def test_failing_file(tmp_path : Path):
    with open(tmp_path / "test.csv", 'w') as csvfile:
        _writer =  csv.writer(csvfile)
        _writer.writerow(["user-id", "@name", ""])
        _writer.writerow(["123", "bob", "the builder"])
    test_csv = tmp_path / "test.csv"
    parser = CSVParser(test_csv)
    columns = parser.parse()
    print(columns)
    errors = validate_columns(columns)
    assert len(errors) == 3
    assert "Invalid Avro field name 'user-id" in errors[0] 
    assert "must start with a letter or underscore" in errors[1]
    assert "Column name cannot be empty" in errors[2]

def test_avro_generation_and_validation()-> None:
    test_csv = _TEST_FOLDER / "from_csv" / "test_users.csv"
    parser = CSVParser(test_csv)
    columns = parser.parse()
    dict_of = build_avro_schema(columns=columns, record_name="users", namespace="example.com")
    assert dict_of
    print(dict_of)
    assert dict_of['type'] == 'record'
    assert dict_of['name'] == 'users'
    assert dict_of['namespace'] == 'example.com'
    assert len(dict_of['fields']) == 6
    assert dict_of['fields'][0]['name'] == 'user_id'
    assert dict_of['fields'][0]['type'] == 'int'
    errors = validate_avro_schema(dict_of)
    assert len(errors) == 0
        write_schema(dict_of, _TEST_FOLDER / "from_csv" / "schemas" / "users.avsc")


def test_pass_sql_as_parameter():
    from typer.testing import CliRunner
    from tools.flink.from_csv.query_csv_cli import app
    runner = CliRunner()
    test_csv = _TEST_FOLDER / "from_csv" / "test_users.csv"
    result = runner.invoke(app, str(test_csv))
    print(result.stderr)
    print(result.stdout)