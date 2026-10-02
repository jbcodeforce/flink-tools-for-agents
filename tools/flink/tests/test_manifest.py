from __future__ import annotations

from pathlib import Path

from tools.flink.manifest.manifest import create_manifest_from_folder


def test_create_manifest_from_folder_keeps_fully_qualified_table_name(tmp_path: Path) -> None:
    sql_dir = tmp_path / "demo"
    sql_dir.mkdir()
    (sql_dir / "ddl.route_role.sql").write_text(
        "CREATE TABLE `clone.dev.mc.dbo.route_role` (id INT);",
        encoding="utf-8",
    )

    manifest = create_manifest_from_folder(sql_dir)

    assert [ref.table for ref in manifest.drop_tables] == ["clone.dev.mc.dbo.route_role"]
