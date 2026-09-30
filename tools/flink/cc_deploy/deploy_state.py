"""
Local ledger of successfully-deployed Flink statements for a sql_dir.

Used by ``deploy_flink_statements.py`` to skip re-deploying a statement whose
SQL file content hasn't changed since it last deployed successfully to the
same Confluent Cloud target (environment / compute pool / database). Scoping
by target means the same sql_dir can be deployed to more than one environment
(e.g. dev vs prod) without one masking the other.

The ledger is a local, machine-specific cache (see ``STATE_FILENAME`` in the
repo's ``.gitignore``) — it is not a source of truth for what is actually
running on Confluent Cloud. ``undeploy_statements``/``full_undeploy`` clear
entries here on successful delete so a later deploy doesn't skip recreating
a statement that no longer exists in the cluster.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

STATE_FILENAME = ".deploy_state.json"

DeployState = dict[str, dict[str, Any]]


def state_path(sql_dir: Path) -> Path:
    """Path to the deploy ledger sidecar file for *sql_dir*."""
    return sql_dir / STATE_FILENAME


def sql_hash(sql_content: str) -> str:
    """Stable content hash used for change detection (not for security)."""
    return hashlib.sha256(sql_content.encode("utf-8")).hexdigest()


def target_key(config: dict[str, str]) -> str:
    """Identify the Confluent Cloud target (env/pool/database) a statement was deployed to."""
    return "|".join(
        config.get(key, "") for key in ("ENVIRONMENT_ID", "FLINK_COMPUTE_POOL_ID", "FLINK_DATABASE_NAME")
    )


def load_state(sql_dir: Path) -> DeployState:
    """Load the deploy ledger for *sql_dir*; returns {} if absent or unreadable."""
    path = state_path(sql_dir)
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def save_state(sql_dir: Path, state: DeployState) -> None:
    """Persist the deploy ledger for *sql_dir*."""
    state_path(sql_dir).write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def is_already_deployed(
    state: DeployState, name: str, sql_content: str, config: dict[str, str]
) -> bool:
    """True if *name* previously deployed successfully with this SQL to this target."""
    entry = state.get(name)
    if not entry:
        return False
    return entry.get("sql_hash") == sql_hash(sql_content) and entry.get("target") == target_key(config)


def mark_deployed(state: DeployState, name: str, sql_content: str, config: dict[str, str]) -> None:
    """Record *name* as successfully deployed with the current SQL content and target."""
    state[name] = {
        "sql_hash": sql_hash(sql_content),
        "target": target_key(config),
        "deployed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def forget_deployed(state: DeployState, name: str) -> None:
    """Remove *name* from the ledger, e.g. after it is undeployed."""
    state.pop(name, None)
