"""
Unit tests for TrackingStore and TrackingRecord.

Covers:
1. Empty store: should_migrate returns True for any entry
2. After recording done with matching SHA: should_migrate returns False
3. After recording done but SHA changed: should_migrate returns True
4. After recording failed: should_migrate returns True
5. force=True always returns True even for done+matching SHA
6. save + load round-trip preserves all fields
7. summary() counts correctly
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tools.dbt.flink_dbt_migrate.sl_discovery_mgr import TableEntry
from tools.dbt.flink_dbt_migrate.tracking_store import TrackingRecord, TrackingStore


def _make_entry(table_name: str = "sl_fct_order", sha: str = "abc123") -> TableEntry:
    """Minimal TableEntry fixture for testing."""
    return TableEntry(
        table_name=table_name,
        dml_path=Path("facts/aqem/fct_order/sql-scripts/dml.sl_fct_order.sql"),
        ddl_path=Path("facts/aqem/fct_order/sql-scripts/ddl.sl_fct_order.sql"),
        dml_sha256=sha,
        relative_path=Path("facts/aqem/fct_order"),
    )


# ---------------------------------------------------------------------------
# 1. Empty store
# ---------------------------------------------------------------------------

def test_empty_store_should_migrate_any_entry():
    store = TrackingStore()
    entry = _make_entry()
    assert store.should_migrate(entry) is True


# ---------------------------------------------------------------------------
# 2. Done + matching SHA → skip
# ---------------------------------------------------------------------------

def test_done_matching_sha_should_not_migrate():
    store = TrackingStore()
    entry = _make_entry(sha="abc123")
    store.record("sl_fct_order", "done", "abc123", "facts/aqem/fct_order")
    assert store.should_migrate(entry) is False


# ---------------------------------------------------------------------------
# 3. Done + changed SHA → re-migrate
# ---------------------------------------------------------------------------

def test_done_changed_sha_should_migrate():
    store = TrackingStore()
    store.record("sl_fct_order", "done", "old_sha", "facts/aqem/fct_order")
    entry = _make_entry(sha="new_sha")  # SHA has changed
    assert store.should_migrate(entry) is True


# ---------------------------------------------------------------------------
# 4. Failed status → re-migrate
# ---------------------------------------------------------------------------

def test_failed_status_should_migrate():
    store = TrackingStore()
    store.record("sl_fct_order", "failed", "abc123", "facts/aqem/fct_order", error="DDL missing")
    entry = _make_entry(sha="abc123")  # same SHA, but previous run failed
    assert store.should_migrate(entry) is True


# ---------------------------------------------------------------------------
# 5. force=True always returns True
# ---------------------------------------------------------------------------

def test_force_always_migrates():
    store = TrackingStore()
    store.record("sl_fct_order", "done", "abc123", "facts/aqem/fct_order")
    entry = _make_entry(sha="abc123")
    assert store.should_migrate(entry, force=True) is True


# ---------------------------------------------------------------------------
# 6. save + load round-trip
# ---------------------------------------------------------------------------

def test_save_load_round_trip(tmp_path: Path):
    store = TrackingStore()
    store.record("sl_fct_order", "done", "abc123", "facts/aqem/fct_order")
    store.record("sl_int_payments", "failed", "def456", "intermediates/aqem/int_payments", error="DDL not found")

    tracking_file = tmp_path / "tracking.yml"
    store.save(tracking_file)

    assert tracking_file.is_file()
    loaded = TrackingStore.load(tracking_file)

    rec_order = loaded._records["sl_fct_order"]
    assert rec_order.table_name == "sl_fct_order"
    assert rec_order.relative_path == "facts/aqem/fct_order"
    assert rec_order.dml_sha256 == "abc123"
    assert rec_order.status == "done"
    assert rec_order.error is None
    assert rec_order.migrated_at  # non-empty ISO-8601 string

    rec_pay = loaded._records["sl_int_payments"]
    assert rec_pay.table_name == "sl_int_payments"
    assert rec_pay.status == "failed"
    assert rec_pay.error == "DDL not found"
    assert rec_pay.dml_sha256 == "def456"


def test_load_absent_file_returns_empty_store(tmp_path: Path):
    store = TrackingStore.load(tmp_path / "nonexistent.yml")
    assert store._records == {}
    assert store.summary() == {"done": 0, "failed": 0, "skipped": 0}


# ---------------------------------------------------------------------------
# 7. summary() counts
# ---------------------------------------------------------------------------

def test_summary_counts_correctly():
    store = TrackingStore()
    store.record("a", "done", "s1", "facts/a")
    store.record("b", "done", "s2", "facts/b")
    store.record("c", "failed", "s3", "facts/c", error="oops")
    store.record("d", "skipped", "s4", "facts/d")

    summary = store.summary()
    assert summary["done"] == 2
    assert summary["failed"] == 1
    assert summary["skipped"] == 1
