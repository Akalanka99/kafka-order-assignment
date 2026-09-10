# Kafka Order Processing Assignment

A Kafka-based producer/consumer system for order messages, using Avro
serialization (via Confluent Schema Registry), with real-time aggregation,
retry logic, and a Dead Letter Queue (DLQ).

## Components

- `src/order.avsc` — Avro schema for order messages (`orderId`, `product`, `price`).
- `src/producer.py` — generates random orders (occasionally with an invalid
  negative price) and publishes them to the `orders` topic.
- `src/consumer.py` — consumes from `orders`, maintains a running average of
  `price`, retries transient failures (simulated) up to 3 times with linear
  backoff, and routes permanently-failed or retry-exhausted messages to the
  `orders-dlq` topic.
- `src/dlq_viewer.py` — a standalone consumer that prints everything sent to
  the DLQ, used to demonstrate the DLQ path live.

## How failures are simulated

There's no real external dependency to fail against, so failure modes are
injected deliberately and documented:

- **Transient failure**: ~15% of messages randomly raise a
  `TransientProcessingError` in `process_order()`. These are retried up to
  `MAX_RETRIES` (3) times with increasing backoff before being sent to the DLQ.
- **Permanent failure**: any order with a negative `price` raises a
  `PermanentProcessingError` and is sent straight to the DLQ without retrying.

## Setup

1. Install Docker Desktop.
2. Start the Kafka broker + Zookeeper + Schema Registry:

   ```
   docker compose up -d
   ```

3. Create a Python virtual environment and install dependencies:

   ```
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```

   > `confluent-kafka` needs the `librdkafka` C library. The `pip install`
   > pulls prebuilt wheels for Windows/macOS/Linux, so no separate install
   > should be required.

## Running the demo

Open three terminals (all with the venv activated):

```
# Terminal 1: consumer (running average + retry + DLQ routing)
python src/consumer.py

# Terminal 2: producer (streams one order per second)
python src/producer.py

# Terminal 3: DLQ viewer (proves failed messages are routed correctly)
python src/dlq_viewer.py
```

You should see:
- The consumer printing each processed order with an updating running average.
- Occasional `[consumer] transient failure (attempt n/3)` retry logs.
- Messages with negative prices, or messages that exhaust retries, appearing
  in the DLQ viewer terminal.

Stop everything with Ctrl+C, then `docker compose down` to tear down Kafka.

## Design notes

- **Avro + Schema Registry**: both producer and consumer use
  `AvroSerializer`/`AvroDeserializer` bound to `order.avsc`, registered
  automatically in Schema Registry on first use — this gives schema
  validation and compatibility checking for free.
- **Running average**: kept in-process (`RunningAverage` class) in the
  consumer, updated only after successful processing of a message.
- **Retry logic**: transient errors are retried in-line before committing the
  offset, so a crash mid-retry replays the message rather than silently
  dropping it.
- **DLQ**: permanently-failed or retry-exhausted messages are re-published
  (Avro-encoded, same schema) to `orders-dlq` instead of being dropped,
  preserving them for later inspection/replay.
