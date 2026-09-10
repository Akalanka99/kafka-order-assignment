"""Simple standalone consumer that prints everything landing in the DLQ topic."""
import os

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
DLQ_TOPIC = os.getenv("ORDERS_DLQ_TOPIC", "orders-dlq")

with open(os.path.join(os.path.dirname(__file__), "order.avsc")) as f:
    ORDER_SCHEMA_STR = f.read()


def main():
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, ORDER_SCHEMA_STR, lambda d, ctx: d)

    consumer = DeserializingConsumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": avro_deserializer,
        "group.id": "orders-dlq-viewer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([DLQ_TOPIC])
    print(f"[dlq-viewer] watching '{DLQ_TOPIC}' (Ctrl+C to stop)")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            print(f"[dlq-viewer] DEAD LETTER -> {msg.value()}")
    except KeyboardInterrupt:
        print("\n[dlq-viewer] stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
