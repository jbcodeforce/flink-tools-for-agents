from __future__ import annotations

from pathlib import Path

import pytest

from tools.flink.manifest.manifest import create_manifest_from_folder, DeployManifest, DropTableRef


def test_create_manifest_from_folder_keeps_fully_qualified_table_name(tmp_path: Path) -> None:
    sql_dir = tmp_path / "demo"
    sql_dir.mkdir()
    (sql_dir / "ddl.route_role.sql").write_text(
        "CREATE TABLE `clone.dev.mc.dbo.route_role` (id INT);",
        encoding="utf-8",
    )

    manifest = create_manifest_from_folder(sql_dir)

    assert [ref.table for ref in manifest.drop_tables] == ["clone.dev.mc.dbo.route_role"]


def test_drop_table_groups_returns_empty_for_flat_list() -> None:
    manifest = DeployManifest(drop_tables=[DropTableRef(table="t1"), DropTableRef(table="t2")])
    assert manifest.drop_table_groups() == []


def test_drop_table_groups_returns_sorted_group_names() -> None:
    manifest = DeployManifest(
        drop_tables={
            "drop_crm": ["t1"],
            "drop_c360": ["t2"],
            "drop_fraud": ["t3"],
        }
    )
    assert manifest.drop_table_groups() == ["drop_c360", "drop_crm", "drop_fraud"]


def test_drop_tables_for_group_with_dict_based() -> None:
    manifest = DeployManifest(
        drop_tables={
            "drop_c360": ["fct_c360_profile", "dim_account_tx_agg"],
            "drop_crm": ["fct_crm_summary"],
        }
    )
    refs = manifest.drop_tables_for_group("drop_c360")
    assert len(refs) == 2
    assert refs[0].table == "fct_c360_profile"
    assert refs[1].table == "dim_account_tx_agg"


def test_drop_tables_dict_accepts_object_entries_with_materialized() -> None:
    manifest = DeployManifest.model_validate(
        {
            "drop_tables": {
                "drop_fraud": [
                    {"table": "fct_fraud_probe", "materialized": True},
                    {"table": "dim_x", "materialized": False},
                    "plain_table",
                ],
            }
        }
    )
    refs = manifest.drop_tables_for_group("drop_fraud")
    assert [(r.table, r.materialized) for r in refs] == [
        ("fct_fraud_probe", True),
        ("dim_x", False),
        ("plain_table", False),
    ]


def test_all_drop_tables_flattens_groups() -> None:
    manifest = DeployManifest.model_validate(
        {"drop_tables": {"a": [{"table": "t1", "materialized": True}], "b": ["t2"]}}
    )
    assert [(r.table, r.materialized) for r in manifest.all_drop_tables()] == [("t1", True), ("t2", False)]


def test_statement_ref_materialized_table_defaults_false_and_roundtrips() -> None:
    manifest = DeployManifest.model_validate(
        {"groups": {"ddl": [{"name": "a", "file": "a.sql", "materialized_table": True}, {"name": "b", "file": "b.sql"}]}}
    )
    assert [s.materialized_table for s in manifest.groups["ddl"]] == [True, False]


def test_create_manifest_from_folder_flags_materialized_table_statements(tmp_path: Path) -> None:
    sql_dir = tmp_path / "demo"
    sql_dir.mkdir()
    (sql_dir / "ddl.mt.sql").write_text("CREATE MATERIALIZED TABLE mt AS SELECT 1;", encoding="utf-8")
    (sql_dir / "ddl.t.sql").write_text("CREATE TABLE t (id INT);", encoding="utf-8")

    manifest = create_manifest_from_folder(sql_dir)

    flags = {s.file: s.materialized_table for s in manifest.groups["ddl"]}
    assert flags == {"ddl.mt.sql": True, "ddl.t.sql": False}


def test_drop_tables_for_group_returns_drop_table_refs() -> None:
    manifest = DeployManifest(
        drop_tables={
            "drop_c360": ["fct_c360_profile", "dim_account_tx_agg"],
        }
    )
    refs = manifest.drop_tables_for_group("drop_c360")
    assert all(isinstance(ref, DropTableRef) for ref in refs)


def test_drop_tables_for_group_with_all_on_flat_list() -> None:
    manifest = DeployManifest(
        drop_tables=[DropTableRef(table="t1"), DropTableRef(table="t2")]
    )
    refs = manifest.drop_tables_for_group("all")
    assert len(refs) == 2
    assert refs[0].table == "t1"


def test_drop_tables_for_group_raises_on_unknown_dict_group() -> None:
    manifest = DeployManifest(
        drop_tables={
            "drop_c360": ["fct_c360_profile"],
        }
    )
    with pytest.raises(KeyError, match="Unknown drop_tables group 'drop_unknown'"):
        manifest.drop_tables_for_group("drop_unknown")


def test_drop_tables_for_group_raises_on_group_with_flat_list() -> None:
    manifest = DeployManifest(
        drop_tables=[DropTableRef(table="t1")]
    )
    with pytest.raises(KeyError, match="drop_tables is not dict-based"):
        manifest.drop_tables_for_group("any_group")
