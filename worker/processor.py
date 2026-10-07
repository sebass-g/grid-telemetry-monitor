import json
import logging
import os
import time
from typing import Any

import pika
import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("worker")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "password123")
QUEUE_NAME = "telemetry_events"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
STATE_TTL_SECONDS = int(os.getenv("STATE_TTL_SECONDS", "120"))
MAX_RETRIES = int(os.getenv("RABBITMQ_MAX_RETRIES", "10"))
RETRY_DELAY_SECONDS = float(os.getenv("RABBITMQ_RETRY_DELAY_SECONDS", "1.0"))


def get_redis_client() -> redis.Redis:
    """Return a configured Redis client bound to the telemetry state database."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def connect_rabbitmq() -> pika.BlockingConnection:
    """Open a blocking RabbitMQ connection with retry-based startup logic.

    Returns:
        A connected RabbitMQ session ready for queue consumption.

    Raises:
        RuntimeError: If the broker cannot be reached within the configured retry budget.
    """
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials,
                heartbeat=30,
            )
            return pika.BlockingConnection(parameters)
        except (pika.exceptions.AMQPConnectionError, OSError) as exc:
            last_error = exc
            logger.warning("RabbitMQ connection attempt %s/%s failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS * attempt)

    raise RuntimeError("Unable to connect to RabbitMQ") from last_error


def process_message(ch: pika.channel.Channel, method: pika.spec.Basic.Deliver, properties: Any, body: bytes) -> None:
    """Persist a telemetry payload in Redis and acknowledge it only after success.

    Args:
        ch: Active RabbitMQ channel handling the current delivery.
        method: Metadata describing the broker delivery tags.
        properties: Message properties supplied by AMQP.
        body: Raw JSON payload from the queue.
    """
    try:
        payload = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning("Discarding malformed message: %s", exc)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    device_id = str(payload.get("device_id", "unknown"))
    key = f"device_state:{device_id}"
    redis_client = get_redis_client()

    try:
        redis_client.set(key, json.dumps(payload, separators=(",", ":"), sort_keys=True), ex=STATE_TTL_SECONDS)
        logger.info("Updated Redis state for device %s with TTL %s", device_id, STATE_TTL_SECONDS)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except redis.RedisError as exc:
        logger.exception("Redis write failed for device %s; message will be retried", device_id)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main() -> None:
    """Run the telemetry worker loop, reconnecting to RabbitMQ when broker sessions drop."""
    connection: pika.BlockingConnection | None = None
    while True:
        try:
            if connection is None or not connection.is_open:
                connection = connect_rabbitmq()

            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_message)

            logger.info("Worker started; listening for messages on queue %s", QUEUE_NAME)
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Worker shutting down gracefully.")
            if connection is not None and connection.is_open:
                connection.close()
            break
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.ChannelClosedByBroker,
            pika.exceptions.StreamLostError,
        ) as exc:
            logger.warning("RabbitMQ connection lost: %s. Reconnecting in %s seconds.", exc, RETRY_DELAY_SECONDS)
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    pass
            connection = None
            time.sleep(RETRY_DELAY_SECONDS)
        except Exception as exc:
            logger.exception("Unexpected worker error: %s", exc)
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    pass
            connection = None
            time.sleep(RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    main()
