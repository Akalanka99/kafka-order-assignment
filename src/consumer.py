"""
Order consumer: reads Avro order events from Kafka, maintains a running
average price, retries transient failures, and routes permanently failed
messages to a Dead Letter Queue (DLQ) topic.
"""
import os
import random
import time

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.error import ConsumeError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("ORDERS_DLQ_TOPIC", "orders-dlq")
GROUP_ID = os.getenv("ORDERS_GROUP_ID", "orders-consumer-group")

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1
TRANSIENT_FAILURE_RATE = 0.15  # simulate ~15% chance of a transient error

with open(os.path.join(os.path.dirname(__file__), "order.avsc")) as f:
    ORDER_SCHEMA_STR = f.read()


class TransientProcessingError(Exception):
    """Simulates a temporary failure (e.g. downstream service hiccup)."""


class PermanentProcessingError(Exception):
    """Simulates a non-recoverable failure (e.g. invalid business data)."""


class RunningAverage:
    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, price: float) -> float:
        self.count += 1
        self.total += price
        return self.total / self.count


def order_from_dict(data, ctx):
    return data


def build_consumer() -> DeserializingConsumer:
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, ORDER_SCHEMA_STR, order_from_dict)

    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": avro_deserializer,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return DeserializingConsumer(consumer_conf)


def build_dlq_producer() -> SerializingProducer:
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, ORDER_SCHEMA_STR, lambda o, ctx: o)

    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    }
    return SerializingProducer(producer_conf)


def process_order(order: dict) -> None:
    """
    Business logic for handling one order. Raises TransientProcessingError
    for retryable failures and PermanentProcessingError for failures that
    should go straight to the DLQ.
    """
    if order["price"] < 0:
        # negative price is invalid data -> not worth retrying
        raise PermanentProcessingError(f"Invalid price {order['price']} for order {order['orderId']}")

    if random.random() < TRANSIENT_FAILURE_RATE:
        raise TransientProcessingError(f"Simulated transient failure for order {order['orderId']}")


def process_with_retry(order: dict) -> bool:
    """Returns True if processed successfully, False if it should go to the DLQ."""
    attempt = 0
    while attempt < MAX_RETRIES:
        attempt += 1
        try:
            process_order(order)
            return True
        except PermanentProcessingError as e:
            print(f"[consumer] permanent failure, sending to DLQ: {e}")
            return False
        except TransientProcessingError as e:
            print(f"[consumer] transient failure (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)  # simple linear backoff
    print(f"[consumer] exhausted retries for order {order['orderId']}, sending to DLQ")
    return False


def send_to_dlq(dlq_producer: SerializingProducer, order: dict) -> None:
    dlq_producer.produce(topic=DLQ_TOPIC, key=order["orderId"], value=order)
    dlq_producer.poll(0)


def main():
    consumer = build_consumer()
    dlq_producer = build_dlq_producer()
    running_avg = RunningAverage()

    consumer.subscribe([TOPIC])
    print(f"[consumer] subscribed to '{TOPIC}', DLQ='{DLQ_TOPIC}' (Ctrl+C to stop)")

    try:
        while True:
            try:
                msg = consumer.poll(1.0)
            except ConsumeError as e:
                # e.g. topic not yet created on the broker - transient, keep polling
                print(f"[consumer] poll error: {e}")
                continue
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer] error: {msg.error()}")
                continue

            order = msg.value()
            if order is None:
                continue

            success = process_with_retry(order)

            if success:
                avg = running_avg.update(order["price"])
                print(
                    f"[consumer] processed orderId={order['orderId']} product={order['product']} "
                    f"price={order['price']:.2f} | running_avg={avg:.2f} (n={running_avg.count})"
                )
            else:
                send_to_dlq(dlq_producer, order)

            consumer.commit(msg)
    except KeyboardInterrupt:
        print("\n[consumer] stopping...")
    finally:
        dlq_producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
