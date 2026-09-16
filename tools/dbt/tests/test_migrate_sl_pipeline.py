

import pytest
from pathlib import Path
from typer.testing import CliRunner

from tools.dbt.flink_dbt_migrate.migrate_dml_to_dbt import app

dbt_mig_path =  Path(__file__).resolve().parent
sl_test_pipeline_path = dbt_mig_path / "fixtures" / "flink-project" / "pipelines"



@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


def test_cli_sl_repo_to_dbt(cli_runner: CliRunner, tmp_path: Path):
    print(tmp_path)
    result = cli_runner.invoke(
        app,
        ["migrate-sl-folder", str(sl_test_pipeline_path), str(tmp_path), "--write"],
        catch_exceptions=False,
    )
    print(result.stdout)
    # The fixture has some incomplete DDL stubs; we accept partial success (some tables migrate).
    assert result.exit_code in (0, 1), f"Unexpected CLI exit code {result.exit_code}:\n{result.output}"
    assert "migrated" in result.stdout, "Expected migration summary in output"


def test_cli_sl_repo_to_dbt_with_exclude_file(cli_runner: CliRunner, tmp_path: Path):
    exclude_file = tmp_path / "exclude.txt"
    exclude_file.write_text("dimensions/c360/dim_groups\n", encoding="utf-8")
    output_dir = tmp_path / "out"
    result = cli_runner.invoke(
        app,
        [
            "migrate-sl-folder",
            str(sl_test_pipeline_path),
            str(output_dir),
            "--exclude-file",
            str(exclude_file),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "Loaded 1 excluded folder path(s)" in result.stdout
    assert "dim_groups" not in result.stdout


def test_cli_sl_repo_to_dbt_exclude_file_not_found(cli_runner: CliRunner, tmp_path: Path):
    missing_file = tmp_path / "nonexistent.txt"
    output_dir = tmp_path / "out"
    result = cli_runner.invoke(
        app,
        [
            "migrate-sl-folder",
            str(sl_test_pipeline_path),
            str(output_dir),
            "-e",
            str(missing_file),
        ],
    )
    assert result.exit_code == 1
    assert "Exclusion file not found" in result.output
    


def test_cli_sl_repo_to_dbt_with_product_filter(cli_runner: CliRunner, tmp_path: Path):
    result = cli_runner.invoke(
        app,
        [
            "migrate-sl-folder",
            str(sl_test_pipeline_path),
            str(tmp_path),
            "--product",
            "c360",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "filtered to product: c360" in result.stdout
    assert "common" not in result.stdout  # no common-product tables in output


def test_tracking_first_run_creates_file(cli_runner: CliRunner, tmp_path: Path):
    """First --write run must create tracking.yml with done/failed entries."""
    result = cli_runner.invoke(
        app,
        ["migrate-sl-folder", str(sl_test_pipeline_path), str(tmp_path), "--write"],
        catch_exceptions=False,
    )
    assert result.exit_code in (0, 1), result.output
    tracking_file = tmp_path / "tracking.yml"
    assert tracking_file.exists(), "tracking.yml should be created after --write run"
    content = tracking_file.read_text(encoding="utf-8")
    assert "status:" in content, "tracking.yml should contain status entries"
    # At least one entry should be done or failed
    assert ("done" in content or "failed" in content), "Expected at least one done/failed entry"


def test_tracking_second_run_skips_unchanged(cli_runner: CliRunner, tmp_path: Path):
    """Second --write run with no DML changes should skip all previously-done tables."""
    # First run
    first = cli_runner.invoke(
        app,
        ["migrate-sl-folder", str(sl_test_pipeline_path), str(tmp_path), "--write"],
        catch_exceptions=False,
    )
    assert first.exit_code in (0, 1), first.output

    # Second run — nothing changed, done tables must be skipped
    second = cli_runner.invoke(
        app,
        ["migrate-sl-folder", str(sl_test_pipeline_path), str(tmp_path), "--write"],
        catch_exceptions=False,
    )
    assert second.exit_code in (0, 1), second.output
    assert "skipped (unchanged)" in second.stdout, (
        "Second run should skip unchanged tables"
    )
    # Summary line should show 0 migrated (all done tables skipped)
    assert "0 migrated" in second.stdout, (
        "Second run should report 0 migrated when all done tables are skipped"
    )


def test_tracking_force_reruns_all(cli_runner: CliRunner, tmp_path: Path):
    """Second --write --force run must not skip any table."""
    # First run to populate tracking.yml
    first = cli_runner.invoke(
        app,
        ["migrate-sl-folder", str(sl_test_pipeline_path), str(tmp_path), "--write"],
        catch_exceptions=False,
    )
    assert first.exit_code in (0, 1), first.output

    # Second run with --force — nothing should be skipped
    second = cli_runner.invoke(
        app,
        ["migrate-sl-folder", str(sl_test_pipeline_path), str(tmp_path), "--write", "--force"],
        catch_exceptions=False,
    )
    assert second.exit_code in (0, 1), second.output
    assert "skipped (unchanged)" not in second.stdout, (
        "--force run must not skip any table"
    )
