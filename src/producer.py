"""
Order producer: generates random order events and publishes them to Kafka
using Avro serialization backed by Schema Registry.
"""
import os
import random
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("ORDERS_TOPIC", "orders")

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]

with open(os.path.join(os.path.dirname(__file__), "order.avsc")) as f:
    ORDER_SCHEMA_STR = f.read()


def order_to_dict(order, ctx):
    return order


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed for {msg.key()}: {err}")
    else:
        print(f"[producer] delivered orderId={msg.key()} to {msg.topic()}[{msg.partition()}]")


def make_random_order(order_id: int) -> dict:
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        # occasionally emit a "bad" price (negative) to demonstrate DLQ handling downstream
        "price": round(random.uniform(-5.0, 500.0), 2),
    }


def build_producer() -> SerializingProducer:
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, ORDER_SCHEMA_STR, order_to_dict)

    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    }
    return SerializingProducer(producer_conf)


def main():
    producer = build_producer()
    order_id = 1000

    print(f"[producer] streaming orders to topic '{TOPIC}' (Ctrl+C to stop)")
    try:
        while True:
            order_id += 1
            order = make_random_order(order_id)

            producer.produce(
                topic=TOPIC,
                key=order["orderId"],
                value=order,
                on_delivery=delivery_report,
            )
            producer.poll(0)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[producer] stopping...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
