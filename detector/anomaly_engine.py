import logging
import os
import time

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("anomaly-engine")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "5"))
KEY_PREFIX = os.getenv("REDIS_DEVICE_KEY_PREFIX", "device_state:")
GRID_OUTAGE_THRESHOLD_RATIO = float(os.getenv("GRID_OUTAGE_THRESHOLD_RATIO", "0.75"))


def get_redis_client() -> redis.Redis:
    """Return a Redis client configured for device-state health monitoring."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def main() -> None:
    """Continuously evaluate fleet health and raise an outage alert when the device set exceeds the threshold."""
    redis_client = get_redis_client()
    known_fleet: set[str] = set()

    logger.info("Anomaly detector started. Monitoring %s* keys every %s seconds.", KEY_PREFIX, SCAN_INTERVAL_SECONDS)

    while True:
        active_devices: set[str] = set()

        for key in redis_client.scan_iter(match=f"{KEY_PREFIX}*"):
            device_id = key.removeprefix(KEY_PREFIX)
            active_devices.add(device_id)
            known_fleet.add(device_id)

        total_known = len(known_fleet)
        active_count = len(active_devices)
        offline_count = max(0, total_known - active_count)
        offline_ratio = (offline_count / total_known) if total_known else 0.0

        if total_known == 0:
            logger.info("✅ Grid stable. Active nodes: 0/0")
        elif offline_ratio > GRID_OUTAGE_THRESHOLD_RATIO:
            logger.warning(
                "🚨 CRITICAL: LOCALIZED GRID OUTAGE DETECTED 🚨 Active nodes: %s/%s | Offline: %s (%s%%)",
                active_count,
                total_known,
                offline_count,
                round(offline_ratio * 100, 2),
            )
        else:
            logger.info("✅ Grid stable. Active nodes: %s/%s", active_count, total_known)

        time.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
