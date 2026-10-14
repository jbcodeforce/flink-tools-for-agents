from typing import Any


from confluent_kafka.schema_registry import avro
import pytest
import os
from tools.kafka.cm_lib.kafka_cluster_admin import get_kafka_client_config, list_topics, delete_topic, AdminClient
from confluent_kafka import Producer
from pydantic import BaseModel, Field
from typing import Optional


def _delivery_report(err, msg) -> None:
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()}")


@pytest.fixture
def set_env():
    """Set environment variables for local Kafka testing."""
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
    os.environ["KAFKA_API_SECRET"] = ""
    os.environ["KAFKA_API_KEY"] = ""
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "PLAINTEXT"
    os.environ["SCHEMA_REGISTRY_URL"] = "http://localhost:8081"
    # Clean up after each test
    yield
    # Cleanup: delete any test topics that might have been created
    try:
        kcfg = get_kafka_client_config()
        admin = AdminClient(kcfg)
        # Delete topic if it exists
        metadata = admin.list_topics(timeout=5)
        for topic_name in ["TEST_TOPIC", "raw_customers"]:
            if topic_name in metadata.topics and metadata.topics[topic_name].error is None:
                admin.delete_topics([topic_name])
    except Exception:
        pass  # Ignore cleanup errors (e.g., Kafka not running)


def test_connect_oss_kafka(set_env):
    kcfg = get_kafka_client_config()
    print(kcfg)
    topics = list_topics()
    print(topics)


def test_producer(set_env):
    options = {
            **get_kafka_client_config(),
            'delivery.timeout.ms': 15000,
            'request.timeout.ms': 15000,
            'client.id': f'producer_test',
        }
    kproducer = Producer(options)
    kproducer.produce( topic="TEST_TOPIC",
                        key="key_1",
                        value="value_1",
                        on_delivery=_delivery_report
            )
    kproducer.flush()


class Boat(BaseModel):
    boat_id: int
    name: str
    model: str
    year_launch: Optional[int] = Field(None, description="When we hit the water")


def test_avro_producer(set_env, tmp_path):
    import py_avro_schema as pas
    from tools.kafka.cm_lib.kafka_avro_producer import KafkaAvroProducer

    namespace = "com.example.schemas"
    key_schema_path = tmp_path / "raw_boats-key.avsc"
    key_schema_path.write_text('"string"', encoding="utf-8")
    value_schema_path = tmp_path / "raw_boats-value.avsc"
    value_schema_path.write_bytes(pas.generate(Boat, namespace=namespace))

    producer = KafkaAvroProducer("raw_boats", use_schema_registry=False)
    producer.specify_models_from_paths(
        key_schema_path, value_schema_path, namespace=namespace
    )

    aboat: Boat = Boat(boat_id=2, name="X", model="414", year_launch=2000)
    sent: bool = producer.send_record(current_key="C001", current_value=aboat)
    assert sent


def test_fastavro():
    import json
    import py_avro_schema as pas
    import io
    from fastavro import schemaless_writer, parse_schema, schemaless_reader
    avro_schema_str = pas.generate(Boat, namespace="com.example.schemas")
    print(avro_schema_str.decode())
    aboat: Boat = Boat(boat_id=2, name="X", model="414", year_launch=2000)
    value_dict = aboat.__dict__
    parsed_schema = parse_schema(json.loads(avro_schema_str))
    bytes_io = io.BytesIO()
    schemaless_writer(bytes_io, parsed_schema, value_dict)
    serialized_value = bytes_io.getvalue()
    print(serialized_value)
    # Read the raw bytes using schemaless_reader
    bytes_io = io.BytesIO(serialized_value)
    decoded_dict = schemaless_reader(bytes_io,parsed_schema)
    print(decoded_dict) 