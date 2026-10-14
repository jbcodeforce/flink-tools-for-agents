"""
Kafka AdminClient helpers for listing cluster topics.

Env vars (typically from ~/.confluent/.env):
  KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET
  KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM (optional)
"""

from __future__ import annotations

import os
from typing import Optional
from confluent_kafka.admin import AdminClient, NewTopic 
from .config import get_kafka_client_config, resolve_kafka_security

INTERNAL_TOPIC_NAMES = frozenset({"__consumer_offsets"})
KAFKA_PARTITIONS: int = int(os.getenv("KAFKA_PARTITIONS", "1"))
_rf_env = os.getenv("KAFKA_REPLICATION_FACTOR")
KAFKA_REPLICATION_FACTOR: Optional[int] = int(_rf_env) if _rf_env is not None else None



def is_internal_topic(name: str) -> bool:
    """True for Kafka/Confluent internal topics that should not be dropped by default."""
    return name in INTERNAL_TOPIC_NAMES or name.startswith("_")


def list_topics(*, exclude_internal: bool = True, timeout: float = 15) -> list[str]:
    """List topic names in the Kafka cluster, sorted alphabetically."""
    config = get_kafka_client_config()
    admin = AdminClient(config)
    try:
        metadata = admin.list_topics(timeout=timeout)
    except Exception as exc:
        protocol = config.get("security.protocol", "PLAINTEXT")
        raise RuntimeError(
            f"Failed to connect to Kafka at {config.get('bootstrap.servers')!r} "
            f"(security.protocol={protocol}). For Confluent Cloud use "
            "KAFKA_SECURITY_PROTOCOL=SASL_SSL and KAFKA_SASL_MECHANISM=PLAIN."
        ) from exc

    names: list[str] = []
    for name, topic_meta in metadata.topics.items():
        if topic_meta.error is not None:
            continue
        if exclude_internal and is_internal_topic(name):
            continue
        names.append(name)
    return sorted(names)


def ensure_topic_exists(
    kafka_config: dict[str, str | int |float |bool],
    topic_name: str,
    partitions: int = KAFKA_PARTITIONS,
    replication_factor: int | None = None,
) -> bool:
    """Create the Kafka topic when it does not exist."""
    admin = AdminClient(get_kafka_client_config())
    try:
        metadata = admin.list_topics(timeout=15)
    except Exception as exc:
        protocol = kafka_config.get('security.protocol', 'PLAINTEXT')
        raise RuntimeError(
            f"Failed to connect to Kafka at {kafka_config.get('bootstrap.servers')!r} "
            f"(security.protocol={protocol}). For Confluent Cloud use "
            "KAFKA_SECURITY_PROTOCOL=SASL_SSL and KAFKA_SASL_MECHANISM=PLAIN."
        ) from exc
    if topic_name in metadata.topics and metadata.topics[topic_name].error is None:
        return True

    rf = replication_factor if replication_factor is not None else get_default_replication_factor()
    futures = admin.create_topics(
        [NewTopic(topic_name, num_partitions=partitions, replication_factor=rf)]
    )
    for name, future in futures.items():
        try:
            future.result(timeout=30)
            print(f"Created Kafka topic '{name}'.")
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                return True
            raise RuntimeError(f"Failed to create topic '{name}': {exc}") from exc
    return True

def get_default_replication_factor() -> int:
    """Return configured replication factor, or default: 1 for local PLAINTEXT, 3 for cloud."""
    if KAFKA_REPLICATION_FACTOR is not None:
        return KAFKA_REPLICATION_FACTOR
    protocol, _ = resolve_kafka_security()
    return 1 if protocol == "PLAINTEXT" else 3


def delete_topic(admin_client: AdminClient, topic: str) -> None:
    """Delete *topic* and wait for the broker to confirm.

    Silently ignores topics that do not exist.
    """
    futures = admin_client.delete_topics([topic], operation_timeout=15)
    for t, fut in futures.items():
        try:
            fut.result()
            print(f"Deleted topic: {t}")
        except Exception as exc:
            print(f"Topic delete skipped for '{t}': {exc}")

