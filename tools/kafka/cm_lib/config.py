"""

* Centralize all Kafka environment variables (KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET, KAFKA_SASL_MECHANISM, KAFKA_SECURITY_PROTOCOL, KAFKA_TOPIC, KAFKA_PARTITIONS, KAFKA_REPLICATION_FACTOR).
* Centralize all Schema Registry environment variables (SCHEMA_REGISTRY_ENDPOINT / SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_API_KEY / SCHEMA_REGISTRY_USER, SCHEMA_REGISTRY_API_SECRET / SCHEMA_REGISTRY_PASSWORD).
*  configuration helpers:
    resolve_kafka_security()
    get_kafka_client_config()
    get_default_replication_factor()
    get_schema_registry_config()

* Set structured logging setup with LOGGER and setup_logging().
"""

from __future__ import annotations
import logging
from typing import Dict, Optional
import os
from pathlib import Path


try:
    from dotenv import find_dotenv, load_dotenv

    _env_file = os.getenv("DEMO_ENV_FILE")
    if _env_file:
        load_dotenv(_env_file)
    else:
        # Search upward from current file location or cwd for .env
        _discovered_env = find_dotenv(usecwd=True)
        if not _discovered_env:
            _discovered_env = find_dotenv(filename=str(Path(__file__).resolve().parents[3] / ".env"))
        if _discovered_env:
            load_dotenv(_discovered_env)
        else:
            load_dotenv()
except ImportError:
    pass


# ── Logging Configuration ───────────────────────────────────────────────────

_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(filename)s:%(lineno)d %(message)s"


def setup_logging(logger_name: str = "flk", log_file: str | Path = "logs/flk_tools.log") -> logging.Logger:
    """Set up and return a file + stream logger."""
    _logger = logging.getLogger(logger_name)
    _logger.setLevel(logging.INFO)
    if not _logger.handlers:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_path)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT))
        _logger.addHandler(handler)
    return _logger


LOGGER = setup_logging()


# ── Kafka Configuration ──────────────────────────────────────────────────────

# Defaults used when environment variables are not set.
# All helpers read from os.getenv() at call-time so that tests/fixtures
# can override these by mutating os.environ before calling the helpers.
_KAFKA_BROKERS_DEFAULT = "localhost:9092"
_KAFKA_USER_DEFAULT = ""
_KAFKA_PASSWORD_DEFAULT = ""
_KAFKA_SASL_MECHANISM_DEFAULT = "PLAIN"
_KAFKA_SECURITY_PROTOCOL_DEFAULT = "PLAINTEXT"
DEFAULT_TOPIC = "raw-rides"


# ── Schema Registry Configuration ────────────────────────────────────────────

_SCHEMA_REGISTRY_URL_DEFAULT = "http://localhost:8081"
_SCHEMA_REGISTRY_USER_DEFAULT = ""
_SCHEMA_REGISTRY_PASSWORD_DEFAULT = ""


# ── Helper Functions ─────────────────────────────────────────────────────────

def resolve_kafka_security() -> tuple[str, str | None]:
    """Return (security.protocol, sasl.mechanisms) from env, with Confluent Cloud defaults."""
    user = os.getenv("KAFKA_API_KEY", _KAFKA_USER_DEFAULT)
    if not user:
        return os.getenv("KAFKA_SECURITY_PROTOCOL", _KAFKA_SECURITY_PROTOCOL_DEFAULT), None

    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", _KAFKA_SECURITY_PROTOCOL_DEFAULT)
    mechanism = os.getenv("KAFKA_SASL_MECHANISM", _KAFKA_SASL_MECHANISM_DEFAULT)

    # Common mistake: security protocol name placed in KAFKA_SASL_MECHANISM.
    if mechanism in ("SASL_SSL", "SASL_PLAINTEXT"):
        if not os.getenv("KAFKA_SECURITY_PROTOCOL"):
            protocol = mechanism
        mechanism = "PLAIN"
    elif mechanism in ("", "SASL"):
        mechanism = "PLAIN"

    # Confluent Cloud (and most hosted clusters) require SASL_SSL when API keys are set.
    if protocol in ("", "PLAINTEXT") and (
        user or "confluent.cloud" in os.getenv("KAFKA_BOOTSTRAP_SERVERS", _KAFKA_BROKERS_DEFAULT)
    ):
        protocol = "SASL_SSL"

    return protocol, mechanism


def get_kafka_client_config() -> Dict[str, str | int | float | bool]:
    """Shared broker/auth settings for producer and admin clients."""
    security_protocol, sasl_mechanism = resolve_kafka_security()
    options: dict[str, str | int | float | bool] = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", _KAFKA_BROKERS_DEFAULT),
    }
    if sasl_mechanism and sasl_mechanism:
        user = os.getenv("KAFKA_API_KEY", _KAFKA_USER_DEFAULT)
        if user:
            options.update(
                {
                    "security.protocol": security_protocol,
                    "sasl.mechanisms": sasl_mechanism,
                    "sasl.username": user,
                    "sasl.password": os.getenv("KAFKA_API_SECRET", _KAFKA_PASSWORD_DEFAULT),
                }
            )
    return options


def get_schema_registry_config(
    url: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> dict[str, str]:
    """Build configuration dict for confluent_kafka SchemaRegistryClient."""
    effective_url = url or os.getenv("SCHEMA_REGISTRY_ENDPOINT", os.getenv("SCHEMA_REGISTRY_URL", _SCHEMA_REGISTRY_URL_DEFAULT))
    effective_user = user if user is not None else os.getenv("SCHEMA_REGISTRY_API_KEY", os.getenv("SCHEMA_REGISTRY_USER", _SCHEMA_REGISTRY_USER_DEFAULT))
    effective_password = password if password is not None else os.getenv("SCHEMA_REGISTRY_API_SECRET", os.getenv("SCHEMA_REGISTRY_PASSWORD", _SCHEMA_REGISTRY_PASSWORD_DEFAULT))

    conf: dict[str, str] = {"url": effective_url}
    if effective_user:
        conf["basic.auth.user.info"] = f"{effective_user}:{effective_password}"
    return conf
