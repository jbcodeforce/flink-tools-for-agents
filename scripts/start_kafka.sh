#!/usr/bin/env bash
#
# Start a single-node KRaft Kafka broker locally for development/testing.
#
# Why this resets the log dir:
#   The broker is a combined broker+controller (process.roles=broker,controller)
#   using a dynamic quorum (--standalone). The controller's voter identity is
#   (node.id, directory.id). If the log.dirs directory survives from a previous
#   run but its meta.properties directory.id no longer matches the directory.id
#   baked into the __cluster_metadata quorum voter record, the node fails to
#   recognise itself as the voter. It then tries to reach the controller as a
#   remote node, never elects a leader, the metadata loader never learns the
#   high water mark, the broker cannot register, and Kafka auto-shuts-down.
#
#   Because the log dir lives under /tmp (ephemeral), we reset it by default to
#   guarantee a clean, consistent format on every start. Set KEEP_DATA=1 to
#   persist data across restarts instead.
#
# Env overrides:
#   KAFKA_HOME        Path to the Kafka install (default below).
#   KAFKA_CLUSTER_ID  Cluster id to format with (default below; fixed so topics
#                     are reproducible across clean starts).
#   KEEP_DATA=1       Reuse the existing formatted log dir instead of wiping it.

set -euo pipefail
KEEP_DATA=1
KAFKA_HOME="${KAFKA_HOME:-$HOME/Documents/Code/flink-studies/deployment/product-tar/kafka_2.13-4.3.1}"
KAFKA_CLUSTER_ID="${KAFKA_CLUSTER_ID:-g43MCl9gSmCBqJ_ela76IQ}"
CONFIG="config/server.properties"

cd "$KAFKA_HOME"

if [[ ! -f "$CONFIG" ]]; then
  echo "ERROR: $CONFIG not found under KAFKA_HOME=$KAFKA_HOME" >&2
  exit 1
fi

# Resolve log.dirs from the config so the reset targets the real path.
LOG_DIRS="$(grep -E '^[[:space:]]*log\.dirs[[:space:]]*=' "$CONFIG" | tail -1 | cut -d= -f2- | tr -d '[:space:]')"
if [[ -z "$LOG_DIRS" ]]; then
  echo "ERROR: could not read log.dirs from $CONFIG" >&2
  exit 1
fi

if [[ "${KEEP_DATA:-0}" == "1" ]]; then
  echo "KEEP_DATA=1: reusing existing log dir(s): $LOG_DIRS"
  # --ignore-formatted makes re-running a no-op on already-formatted dirs
  # instead of failing. This does NOT repair an inconsistent dir; unset
  # KEEP_DATA to reset if the broker fails to register.
  bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c "$CONFIG" --ignore-formatted
else
  echo "Resetting log dir(s) for a clean start: $LOG_DIRS"
  IFS=',' read -ra DIRS <<< "$LOG_DIRS"
  for d in "${DIRS[@]}"; do
    rm -rf "$d"
  done
  bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c "$CONFIG"
fi

exec bin/kafka-server-start.sh "$CONFIG"
