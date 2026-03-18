"""
competitor_price_generator.py
Simulates competitor price changes and produces them to the
raw.competitor_prices Kafka topic.

Each event represents one competitor re-pricing one product.
Competitor prices fluctuate within ±20% of our base (catalog) price.
Rate: 1 event every 2–5 seconds.
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
KAFKA_TOPIC = "raw.competitor_prices"
SLEEP_RANGE_SECONDS = (2.0, 5.0)       # one event per this interval
STATS_INTERVAL_SECONDS = 30            # slower cadence matches slower event rate
PRICE_FLUCTUATION = 0.20               # ±20% of our base price

COMPETITORS = ["TechMart", "GadgetZone", "ElectroHub"]


# ---------------------------------------------------------------------------
# Price simulation
# ---------------------------------------------------------------------------
def competitor_price(base_price: float) -> float:
    """Return a price within ±PRICE_FLUCTUATION of base_price, rounded to cents."""
    lo = base_price * (1 - PRICE_FLUCTUATION)
    hi = base_price * (1 + PRICE_FLUCTUATION)
    return round(random.uniform(lo, hi), 2)


def build_event(product: dict, competitor: str) -> dict:
    our_price  = product["product_price"]
    comp_price = competitor_price(our_price)
    diff       = round(comp_price - our_price, 2)
    diff_pct   = round((diff / our_price) * 100, 2)

    return {
        "event_id":             str(uuid.uuid4()),
        "product_id":           product["product_id"],
        "product_name":         product["product_name"],
        "product_category":     product["product_category"],
        "competitor_name":      competitor,
        "competitor_price":     comp_price,
        "our_price":            our_price,
        "price_difference":     diff,       # positive → competitor is more expensive
        "price_difference_pct": diff_pct,
        "timestamp":            datetime.now(timezone.utc).isoformat(),
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

    print(f"Connected.  Competitors: {COMPETITORS} → topic '{KAFKA_TOPIC}'")
    print(f"Rate: one event every {SLEEP_RANGE_SECONDS[0]}–{SLEEP_RANGE_SECONDS[1]}s")
    print("Press Ctrl+C to stop.\n")

    shutdown = threading.Event()

    def handle_sigint(sig, frame):
        print("\n[SIGNAL] Shutting down gracefully ...")
        shutdown.set()

    signal.signal(signal.SIGINT, handle_sigint)

    total_events = 0
    event_counts: dict[str, int] = {c: 0 for c in COMPETITORS}
    last_report = time.monotonic()

    while not shutdown.is_set():
        product    = random.choice(PRODUCTS)
        competitor = random.choice(COMPETITORS)
        event      = build_event(product, competitor)

        producer.send(KAFKA_TOPIC, value=event).add_errback(on_send_error)
        total_events += 1
        event_counts[competitor] += 1

        now = time.monotonic()
        if now - last_report >= STATS_INTERVAL_SECONDS:
            ts = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{ts}] total={total_events:,} | "
                + " ".join(f"{k}={v:,}" for k, v in event_counts.items())
            )
            last_report = now

        # Sleep between events; wake immediately on shutdown
        delay = random.uniform(*SLEEP_RANGE_SECONDS)
        shutdown.wait(timeout=delay)

    producer.flush()
    producer.close()
    print(f"\nDone. Total events produced: {total_events:,}")
    for k, v in event_counts.items():
        print(f"  {k:<25} {v:,}")


if __name__ == "__main__":
    main()
