from dataclasses import asdict
import json
from pathlib import Path
from typing import List

from tools.dbt.flink_dbt_migrate.sl_discovery_mgr import (
    _upstream_ddl_map_from_pipeline_def,
    _find_pipelines_parent,
    TableEntry,
    crawl_pipeline_folder,
    load_excluded_folders,
)

dbt_mig_path =  Path(__file__).resolve().parent # flink_dbt_migrate
sl_test_pipeline_path = dbt_mig_path / "fixtures" / "flink-project" / "pipelines"


def  test__find_pipelines_parent() :
    print(sl_test_pipeline_path)
    pipeline_parent = _find_pipelines_parent(sl_test_pipeline_path)
    assert "pipelines" not in str(pipeline_parent)
   

def test__upstream_ddl_map_from_pipeline_def() :
    pipeline_parent = _find_pipelines_parent(sl_test_pipeline_path)
    table_dir  = sl_test_pipeline_path / "dimensions" / "c360" / "dim_groups"
    upstream_ddl_map = _upstream_ddl_map_from_pipeline_def(table_dir, pipeline_parent)
    assert len(upstream_ddl_map) == 2
    print(upstream_ddl_map)
    assert "src_tenant" in str(upstream_ddl_map['sl_cmn_src_tenants'])
    assert "src_c360_groups" in str(upstream_ddl_map['sl_c360_src_groups'])

def test_crawl_pipeline_folder() :
    table_entries: List[TableEntry] = crawl_pipeline_folder(sl_test_pipeline_path)
    assert table_entries
    assert len(table_entries) > 0
    print(json.dumps([asdict(e) for e in table_entries], indent=2, default=str))


def test_load_excluded_folders(tmp_path: Path):
    exclude_file = tmp_path / "excluded.txt"
    exclude_file.write_text(
        "# This is a comment\n"
        "\n"
        "dimensions/c360/dim_groups\n"
        "  # another comment\n"
        "facts/p1/fct_order  \n",
        encoding="utf-8",
    )
    base_dir = sl_test_pipeline_path
    excluded = load_excluded_folders(exclude_file, base_dir=base_dir)
    assert (base_dir / "dimensions" / "c360" / "dim_groups").resolve() in excluded
    assert (base_dir / "facts" / "p1" / "fct_order").resolve() in excluded
    assert len(excluded) == 2


def test_crawl_pipeline_folder_with_exclusions(tmp_path: Path):
    all_entries = crawl_pipeline_folder(sl_test_pipeline_path)
    excluded_dir = (sl_test_pipeline_path / "dimensions" / "c360" / "dim_groups").resolve()
    filtered_entries = crawl_pipeline_folder(
        sl_test_pipeline_path,
        excluded_folders={excluded_dir},
    )
    assert len(filtered_entries) < len(all_entries)
    table_names = [e.table_name for e in filtered_entries]
    assert "dim_groups" not in table_names
    

    


def test_product_filter_includes_matching():
    """Crawling with product='c360' returns only c360 tables."""
    entries = crawl_pipeline_folder(sl_test_pipeline_path, product="c360")
    assert len(entries) > 0
    for e in entries:
        assert "c360" in e.relative_path.parts, (
            f"Expected 'c360' in path parts of {e.relative_path}"
        )


def test_product_filter_excludes_nonmatching():
    """Crawling with product='nonexistent' returns an empty list."""
    entries = crawl_pipeline_folder(sl_test_pipeline_path, product="nonexistent")
    assert entries == []


def test_product_filter_none_returns_all():
    """No product filter returns all tables — same as existing behaviour."""
    all_entries = crawl_pipeline_folder(sl_test_pipeline_path)
    filtered_entries = crawl_pipeline_folder(sl_test_pipeline_path, product=None)
    assert len(filtered_entries) == len(all_entries)
