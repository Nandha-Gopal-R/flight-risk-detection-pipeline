#!/usr/bin/env python3
"""
adsb_producer.py

Fetch aircraft data from airplanes.live–compatible endpoints
and publish normalized position updates to Kafka.
"""

import os
import json
import time
import signal
import logging
from typing import List

import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# ---------------- CONFIG ----------------

BASE_DIR = os.path.dirname(__file__)
# look for config/settings.env relative to package, fallback to project-level settings.env
ENV_PATH = os.path.join(BASE_DIR, "..", "config", "settings.env")
ENV_PATH = os.path.abspath(ENV_PATH)
# fallback to two levels up (project root) settings.env if the first path doesn't exist
if not os.path.exists(ENV_PATH):
    alt = os.path.abspath(os.path.join(BASE_DIR, "..", "..", "settings.env"))
    if os.path.exists(alt):
        ENV_PATH = alt

FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 5))
REQUEST_TIMEOUT = 10
REQUEST_RETRIES = 2

# --------------------------------------

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("adsb_producer")

# Load env
load_dotenv(ENV_PATH)

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "aircraft_positions")

ZONES = list(
    filter(
        None,
        [
            os.getenv("ZONE1"),
            os.getenv("ZONE2"),
            os.getenv("ZONE3"),
        ],
    )
)

if not ZONES:
    raise SystemExit("❌ No zones configured")

log.info("Kafka brokers : %s", BROKER)
log.info("Kafka topic   : %s", TOPIC)
log.info("Zones         : %d", len(ZONES))
log.info("Interval      : %ds", FETCH_INTERVAL)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BROKER.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    linger_ms=5,
)

shutdown = False


def stop(signum, frame):
    """Handle SIGINT/SIGTERM"""
    global shutdown
    shutdown = True
    log.info("Shutdown requested")


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

# Shared HTTP session
session = requests.Session()
session.headers["User-Agent"] = "adsb-producer/1.0"


def fetch_zone(url: str) -> List[dict]:
    """Fetch aircraft list for a zone"""
    for _ in range(REQUEST_RETRIES + 1):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                continue

            data = r.json()

            if isinstance(data, dict) and "ac" in data:
                return data["ac"]

            if isinstance(data, list):
                return data

        except Exception:
            pass

    return []


def normalize(ac: dict) -> dict:
    """Normalize airplanes.live aircraft record"""

    # airplanes.live uses 'hex' as ICAO
    icao = (
        ac.get("hex")
        or ac.get("icao24")
        or ac.get("icao")
        or ""
    ).strip().lower()

    if not icao:
        return {}

    return {
        "icao24": icao,
        "lat": ac.get("lat"),
        "lon": ac.get("lon"),
        "alt_baro": ac.get("alt_baro") or ac.get("alt"),
        "gs": ac.get("gs"),
        "track": ac.get("track"),
        "flight": (
            ac.get("flight")
            or ac.get("callsign")
            or ""
        ).strip() or None,
        "reg": ac.get("reg"),
        "src": ac.get("src"),
        "timestamp": int(time.time()),
    }


def main():
    log.info("ADS-B producer started (continuous)")

    while not shutdown:
        sent = 0
        total = 0

        for zone in ZONES:
            aircraft = fetch_zone(zone)
            total += len(aircraft)

            for ac in aircraft:
                msg = normalize(ac)
                if not msg:
                    continue

                producer.send(
                    TOPIC,
                    key=msg["icao24"],
                    value=msg,
                )
                sent += 1

        producer.flush()
        log.info("Fetched %d | Sent %d aircraft", total, sent)

        for _ in range(FETCH_INTERVAL):
            if shutdown:
                break
            time.sleep(1)

    log.info("Producer stopping...")
    producer.close()


if __name__ == "__main__":
    main()
