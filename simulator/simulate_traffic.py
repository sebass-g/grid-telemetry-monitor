import logging
import random
import time
from typing import Any

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("simulator")

TARGET_URL = "http://localhost:8000/api/v1/heartbeat"
DEVICE_COUNT = 100
SEND_INTERVAL_SECONDS = 3
OUTAGE_AFTER_SECONDS = 15
OUTAGE_RATIO = 0.8


def generate_devices(device_count: int) -> list[dict[str, Any]]:
    devices: list[dict[str, Any]] = []
    for index in range(device_count):
        base_lat = 37.7749 + (index % 10) * 0.05
        base_lon = -122.4194 + (index % 7) * 0.05
        devices.append(
            {
                "device_id": f"device-{index:03d}",
                "lat": round(base_lat, 6),
                "lon": round(base_lon, 6),
            }
        )
    return devices


def send_heartbeat(device: dict[str, Any]) -> None:
    payload = {
        "device_id": device["device_id"],
        "lat": device["lat"],
        "lon": device["lon"],
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    try:
        response = requests.post(TARGET_URL, json=payload, timeout=5)
        if response.status_code != 202:
            logger.warning(
                "Device %s returned status %s: %s",
                device["device_id"],
                response.status_code,
                response.text,
            )
        else:
            logger.info("Heartbeat accepted for device %s", device["device_id"])
    except requests.RequestException as exc:
        logger.warning("Request failed for device %s: %s", device["device_id"], exc)


def main() -> None:
    devices = generate_devices(DEVICE_COUNT)
    outage_devices: set[str] = set()
    start_time = time.monotonic()
    logger.info("Starting simulation for %s devices. Posting to %s", DEVICE_COUNT, TARGET_URL)

    while True:
        elapsed_seconds = time.monotonic() - start_time

        if elapsed_seconds >= OUTAGE_AFTER_SECONDS and not outage_devices:
            outage_count = max(1, int(len(devices) * OUTAGE_RATIO))
            outage_devices = set(random.sample([device["device_id"] for device in devices], outage_count))
            logger.warning(
                "Localized grid outage detected. %s devices are offline.",
                len(outage_devices),
            )

        for device in devices:
            if elapsed_seconds >= OUTAGE_AFTER_SECONDS and device["device_id"] in outage_devices:
                continue
            send_heartbeat(device)

        time.sleep(SEND_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
