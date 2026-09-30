"""Unit tests for tools.flink.cc_deploy.deploy_state."""

from __future__ import annotations

import json
from pathlib import Path

from tools.flink.cc_deploy.deploy_state import (
    STATE_FILENAME,
    forget_deployed,
    is_already_deployed,
    load_state,
    mark_deployed,
    save_state,
    sql_hash,
    state_path,
    target_key,
)

CONFIG = {"ENVIRONMENT_ID": "env-1", "FLINK_COMPUTE_POOL_ID": "pool-1", "FLINK_DATABASE_NAME": "db"}
OTHER_ENV_CONFIG = {**CONFIG, "ENVIRONMENT_ID": "env-2"}


def test_state_path_uses_expected_filename(tmp_path: Path) -> None:
    assert state_path(tmp_path) == tmp_path / STATE_FILENAME


def test_sql_hash_is_stable_and_content_sensitive() -> None:
    assert sql_hash("SELECT 1") == sql_hash("SELECT 1")
    assert sql_hash("SELECT 1") != sql_hash("SELECT 2")


def test_target_key_combines_env_pool_database() -> None:
    assert target_key(CONFIG) == "env-1|pool-1|db"


def test_target_key_tolerates_missing_fields() -> None:
    assert target_key({}) == "||"


def test_load_state_missing_file_returns_empty_dict(tmp_path: Path) -> None:
    assert load_state(tmp_path) == {}


def test_load_state_ignores_corrupt_file(tmp_path: Path) -> None:
    state_path(tmp_path).write_text("not json", encoding="utf-8")
    assert load_state(tmp_path) == {}


def test_load_state_ignores_non_dict_payload(tmp_path: Path) -> None:
    state_path(tmp_path).write_text(json.dumps(["a", "b"]), encoding="utf-8")
    assert load_state(tmp_path) == {}


def test_save_and_load_state_round_trip(tmp_path: Path) -> None:
    state = {"orders-ddl": {"sql_hash": "abc", "target": "env-1|pool-1|db", "deployed_at": "now"}}
    save_state(tmp_path, state)
    assert load_state(tmp_path) == state


def test_is_already_deployed_false_when_never_recorded() -> None:
    assert is_already_deployed({}, "orders-ddl", "CREATE TABLE orders", CONFIG) is False


def test_mark_then_is_already_deployed_true_for_same_sql_and_target() -> None:
    state = {}
    mark_deployed(state, "orders-ddl", "CREATE TABLE orders", CONFIG)
    assert is_already_deployed(state, "orders-ddl", "CREATE TABLE orders", CONFIG) is True


def test_is_already_deployed_false_when_sql_changed() -> None:
    state = {}
    mark_deployed(state, "orders-ddl", "CREATE TABLE orders", CONFIG)
    assert is_already_deployed(state, "orders-ddl", "CREATE TABLE orders_v2", CONFIG) is False


def test_is_already_deployed_false_when_target_changed() -> None:
    state = {}
    mark_deployed(state, "orders-ddl", "CREATE TABLE orders", CONFIG)
    assert is_already_deployed(state, "orders-ddl", "CREATE TABLE orders", OTHER_ENV_CONFIG) is False


def test_forget_deployed_removes_entry() -> None:
    state = {}
    mark_deployed(state, "orders-ddl", "CREATE TABLE orders", CONFIG)
    forget_deployed(state, "orders-ddl")
    assert "orders-ddl" not in state


def test_forget_deployed_missing_entry_is_noop() -> None:
    state = {}
    forget_deployed(state, "does-not-exist")
    assert state == {}
