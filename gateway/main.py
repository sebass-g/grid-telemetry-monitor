import json
import logging
import os
import time
from typing import Any

import pika
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("gateway")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "password123")
QUEUE_NAME = "telemetry_events"
MAX_RETRIES = int(os.getenv("RABBITMQ_MAX_RETRIES", "10"))
RETRY_DELAY_SECONDS = float(os.getenv("RABBITMQ_RETRY_DELAY_SECONDS", "1.0"))

app = FastAPI(title="Utility Grid Telemetry Gateway")


class HeartbeatPayload(BaseModel):
    device_id: str = Field(..., min_length=1)
    lat: float
    lon: float
    timestamp: str


def publish_heartbeat(payload: dict[str, Any]) -> None:
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        connection: pika.BlockingConnection | None = None
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            connection_parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials,
                heartbeat=30,
            )
            connection = pika.BlockingConnection(connection_parameters)
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.confirm_delivery()

            message = json.dumps(payload, separators=(",", ":"), sort_keys=True)
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=message.encode("utf-8"),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            logger.info("Published telemetry for device %s to queue %s", payload["device_id"], QUEUE_NAME)
            return
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelError, OSError) as exc:
            last_error = exc
            logger.warning(
                "RabbitMQ publish attempt %s/%s failed for device %s: %s",
                attempt,
                MAX_RETRIES,
                payload.get("device_id", "unknown"),
                exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS * attempt)
        finally:
            if connection is not None and connection.is_open:
                connection.close()

    raise RuntimeError(f"Failed to publish telemetry for device {payload.get('device_id', 'unknown')}") from last_error


@app.post("/api/v1/heartbeat", status_code=status.HTTP_202_ACCEPTED)
async def ingest_heartbeat(payload: HeartbeatPayload) -> dict[str, str]:
    payload_dict = payload.model_dump(mode="json")
    try:
        publish_heartbeat(payload_dict)
    except RuntimeError as exc:
        logger.exception("Heartbeat ingestion failed for device %s", payload.device_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Telemetry broker unavailable; request not accepted.",
        ) from exc

    return {"status": "accepted"}
