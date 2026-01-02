from kafka import KafkaConsumer
import json
import signal
import sys
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "aircraft_positions")

print(f" Flight consumer listening on {KAFKA_TOPIC}")

shutdown = False

def handler(sig, frame):
    global shutdown
    print("\n Consumer shutting down...")
    shutdown = True

signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: m.decode("utf-8", errors="ignore")
)

for msg in consumer:
    if shutdown:
        break

    raw = msg.value
    if not raw:
        continue

    try:
        data = json.loads(raw)
        print(f" Aircraft {data.get('icao24')} → ({data.get('lat')}, {data.get('lon')})")
    except json.JSONDecodeError:
        print(" Skipping non-JSON message")

consumer.close()
print(" Consumer stopped")
