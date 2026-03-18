"""
inventory_generator.py
Simulates warehouse inventory events and produces them to the
raw.inventory_updates Kafka topic.

Event type distribution (per event):
  stock_received      → 50%
  stock_returned      → 25%
  stock_damaged       → 15%
  manual_adjustment   → 10%

Rate: 1–3 events per second.
"""

import json
import random
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from catalog import PRODUCTS

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "raw.inventory_updates"
EVENTS_PER_SECOND_RANGE = (1, 3)
STATS_INTERVAL_SECONDS = 10

WAREHOUSES = ["WH-LAGOS", "WH-ABUJA", "WH-PH"]

# (event_type, weight, quantity_range)
#   positive quantity → stock increases (received / returned)
#   negative quantity → stock decreases (damaged)
EVENT_SPECS: list[tuple[str, float, tuple[int, int]]] = [
    ("stock_received",    0.50, (10, 200)),   # large positive batches
    ("stock_returned",    0.25, (1,  20)),    # small positive returns
    ("stock_damaged",     0.15, (1,  15)),    # small negative losses
    ("manual_adjustment", 0.10, (1,  50)),    # positive or negative
]

EVENT_TYPES   = [s[0] for s in EVENT_SPECS]
EVENT_WEIGHTS = [s[1] for s in EVENT_SPECS]
QTY_RANGES    = {s[0]: s[2] for s in EVENT_SPECS}


# ---------------------------------------------------------------------------
# Event builder
# ---------------------------------------------------------------------------
def build_event(product: dict, event_type: str, warehouse_id: str) -> dict:
    lo, hi = QTY_RANGES[event_type]
    qty = random.randint(lo, hi)

    if event_type == "stock_damaged":
        qty = -qty
    elif event_type == "manual_adjustment":
        # Equal chance of positive or negative correction
        qty = qty if random.random() < 0.5 else -qty

    return {
        "event_id":         str(uuid.uuid4()),
        "product_id":       product["product_id"],
        "product_name":     product["product_name"],
        "product_category": product["product_category"],
        "event_type":       event_type,
        "quantity_change":  qty,
        "warehouse_id":     warehouse_id,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------
def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
        batch_size=16384,
    )


def on_send_error(exc: Exception) -> None:
    print(f"[ERROR] Kafka send failed: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main() -> None:
    print(f"Connecting to Kafka at {KAFKA_BROKER} ...")
    try:
        producer = make_producer()
    except KafkaError as exc:
        print(f"[FATAL] Could not connect to Kafka: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Connected.  Warehouses: {WAREHOUSES} → topic '{KAFKA_TOPIC}'")
    print("Press Ctrl+C to stop.\n")

    shutdown = threading.Event()

    def handle_sigint(sig, frame):
        print("\n[SIGNAL] Shutting down gracefully ...")
        shutdown.set()

    signal.signal(signal.SIGINT, handle_sigint)

    total_events = 0
    event_counts: dict[str, int] = {t: 0 for t in EVENT_TYPES}
    last_report = time.monotonic()

    while not shutdown.is_set():
        tick_start = time.monotonic()

        n = random.randint(*EVENTS_PER_SECOND_RANGE)
        for _ in range(n):
            if shutdown.is_set():
                break

            product    = random.choice(PRODUCTS)
            warehouse  = random.choice(WAREHOUSES)
            event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
            event      = build_event(product, event_type, warehouse)

            producer.send(KAFKA_TOPIC, value=event).add_errback(on_send_error)
            total_events += 1
            event_counts[event_type] += 1

        now = time.monotonic()
        if now - last_report >= STATS_INTERVAL_SECONDS:
            ts = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{ts}] total={total_events:,} | "
                + " ".join(f"{k}={v:,}" for k, v in event_counts.items())
            )
            last_report = now

        elapsed = time.monotonic() - tick_start
        shutdown.wait(timeout=max(0.0, 1.0 - elapsed))

    producer.flush()
    producer.close()
    print(f"\nDone. Total events produced: {total_events:,}")
    for k, v in event_counts.items():
        print(f"  {k:<20} {v:,}")


if __name__ == "__main__":
    main()
