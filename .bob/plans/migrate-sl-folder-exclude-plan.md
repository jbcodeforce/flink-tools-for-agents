# Migration Folder Exclusion Plan

## Overview
Add an optional `--exclude-file` (`-e`) CLI option and function parameter to `migrate_sl_folder` (in `tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py` and supporting functions in `tools/dbt/flink_dbt_migrate/sl_discovery_mgr.py`). The option accepts a path to a plain text file listing folder paths to exclude from discovery/migration.

Lines in the exclusion file can be relative (resolved against the exclusion file parent directory or `pipeline_dir`) or absolute, with support for comments (`#`) and empty/blank lines.

## Sub-Tasks

### Sub-Task 1: Add helper to parse exclusion file and filter logic
- **Intent**: Create a helper function `load_excluded_folders(exclude_file: Path, base_dir: Path | None = None) -> set[Path]` to read and resolve folder paths from an exclusion text file, stripping whitespace and comments.
- **Expected Outcomes**:
  - Empty lines and lines starting with `#` are ignored.
  - Relative paths are resolved against `base_dir` or the file's parent.
  - Returns a `set[Path]` of resolved directory paths.
- **Relevant Context**: `tools/dbt/flink_dbt_migrate/sl_discovery_mgr.py`
- **Status**: [ ] pending

### Sub-Task 2: Support excluded folders in table discovery
- **Intent**: Update `crawl_pipeline_folder` (or filtering logic in `migrate_sl_folder`) to skip any table entries located within excluded folders or sub-trees.
- **Expected Outcomes**:
  - `crawl_pipeline_folder(folder: Path, excluded_folders: set[Path] | None = None)` ignores sql-scripts under any directory matching an excluded path or child of an excluded path.
  - Discovery output clearly reflects skipped folders / excluded count if appropriate.
- **Relevant Context**: `tools/dbt/flink_dbt_migrate/sl_discovery_mgr.py`, `tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py`
- **Status**: [ ] pending

### Sub-Task 3: Update `migrate_sl_folder` CLI command and signature
- **Intent**: Add the optional `exclude_file: Annotated[Path | None, typer.Option("--exclude-file", "-e", help="Path to text file listing folder paths to exclude from migration")] = None` parameter to `migrate_sl_folder`.
- **Expected Outcomes**:
  - CLI option `--exclude-file` / `-e` is exposed and documented in `--help`.
  - If the file is provided but does not exist, a friendly error is reported and the process exits with non-zero status.
  - Logging and stdout reflect the exclusions applied.
- **Relevant Context**: `tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py`
- **Status**: [ ] pending

### Sub-Task 4: Add Unit Tests
- **Intent**: Write automated tests covering `load_excluded_folders`, `crawl_pipeline_folder` with exclusions, and the `migrate_sl_folder` CLI command with `--exclude-file`.
- **Expected Outcomes**:
  - Tests verify that excluded folders are ignored during crawl and migration.
  - Tests verify comment handling, whitespace stripping, and missing file error handling.
  - All existing test suites pass.
- **Relevant Context**: `tools/dbt/tests/test_migrate_sl_pipeline.py`, `tools/dbt/tests/test_sl_discov_mgr.py`
- **Status**: [ ] pending

### Sub-Task 5: Update Documentation & Skills
- **Intent**: Update CLI docs and skill files to include `--exclude-file` / `-e`.
- **Expected Outcomes**:
  - `skills/dbt-project/SKILL.md` and `docs/dbt/index.md` document the new option.
- **Relevant Context**: `skills/dbt-project/SKILL.md`, `docs/dbt/index.md`
- **Status**: [ ] pending
