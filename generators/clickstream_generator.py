"""
clickstream_generator.py
Simulates realistic e-commerce user sessions and produces events to Kafka.

Funnel outcome distribution (per session):
  page_view only  → 70%
  product_view    → 20%
  add_to_cart     →  8%
  purchase        →  2%

Derived per-step transition probabilities:
  P(page_view → product_view) = 0.30
  P(product_view → add_to_cart) = 0.333   (0.30 × 0.333 ≈ 10%)
  P(add_to_cart → purchase) = 0.20        (0.10 × 0.20  ≈  2%)
"""

import json
import random
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from enum import Enum, auto

from kafka import KafkaProducer
from kafka.errors import KafkaError

from catalog import PRODUCTS

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "raw.clickstream"
NUM_USERS = 50
EVENTS_PER_SECOND_RANGE = (5, 15)
STATS_INTERVAL_SECONDS = 10

# Funnel transition probabilities
P_PAGE_TO_PRODUCT = 0.30
P_PRODUCT_TO_CART = 0.333
P_CART_TO_PURCHASE = 0.20
P_CART_TO_REMOVE = 0.15

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.35, 0.10]


# ---------------------------------------------------------------------------
# Session state machine
# ---------------------------------------------------------------------------
class State(Enum):
    IDLE = auto()
    PAGE_VIEW = auto()
    PRODUCT_VIEW = auto()
    ADDING_TO_CART = auto()   # transient: emits add_to_cart, then → CARTED
    CARTED = auto()
    DONE = auto()             # transient: emits nothing, then → IDLE


class UserSession:
    """
    Per-user state machine.  Each call to next_event() advances the machine
    by one step and returns a fully-formed event dict (or None when idle).
    """

    def __init__(self, user_id: str, device_type: str):
        self.user_id = user_id
        self.device_type = device_type
        self.session_id: str = ""
        self.state: State = State.IDLE
        self.current_product: dict | None = None
        self.cart: list[dict] = []
        self.page_views_done: int = 0
        self.max_page_views: int = 1

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
    def next_event(self) -> dict | None:
        if self.state == State.IDLE:
            if random.random() < 0.25:          # ~25% chance to start a session each tick
                self._start_session()
            return None

        if self.state == State.PAGE_VIEW:
            return self._handle_page_view()

        if self.state == State.PRODUCT_VIEW:
            return self._handle_product_view()

        if self.state == State.ADDING_TO_CART:
            return self._handle_adding_to_cart()

        if self.state == State.CARTED:
            return self._handle_carted()

        if self.state == State.DONE:
            self.state = State.IDLE
            return None

        return None  # unreachable, satisfies type checker

    @property
    def is_active(self) -> bool:
        return self.state not in (State.IDLE, State.DONE)

    # ------------------------------------------------------------------
    # State handlers
    # ------------------------------------------------------------------
    def _handle_page_view(self) -> dict:
        event = self._build_event("page_view", product=None)
        self.page_views_done += 1

        if self.page_views_done >= self.max_page_views:
            if random.random() < P_PAGE_TO_PRODUCT:
                self.current_product = random.choice(PRODUCTS)
                self.state = State.PRODUCT_VIEW
            else:
                self.state = State.DONE

        return event

    def _handle_product_view(self) -> dict:
        event = self._build_event("product_view", product=self.current_product)
        roll = random.random()

        if roll < P_PRODUCT_TO_CART:
            self.state = State.ADDING_TO_CART          # next tick emits add_to_cart
        elif roll < P_PRODUCT_TO_CART + 0.40:
            self.current_product = random.choice(PRODUCTS)   # browse another product
        else:
            self.state = State.DONE                    # abandon

        return event

    def _handle_adding_to_cart(self) -> dict:
        event = self._build_event("add_to_cart", product=self.current_product)
        self.cart.append(self.current_product)
        self.state = State.CARTED
        return event

    def _handle_carted(self) -> dict:
        roll = random.random()

        if roll < P_CART_TO_PURCHASE:
            # Purchase one item from the cart
            product = random.choice(self.cart)
            event = self._build_event("purchase", product=product)
            self.state = State.DONE

        elif roll < P_CART_TO_PURCHASE + P_CART_TO_REMOVE:
            # Remove the most recently added item
            product = self.cart.pop()
            event = self._build_event("remove_from_cart", product=product)
            if self.cart:
                self.state = State.CARTED
            else:
                # Cart empty → browse again
                self.current_product = random.choice(PRODUCTS)
                self.state = State.PRODUCT_VIEW

        else:
            # Add another product to the cart
            new_product = random.choice(PRODUCTS)
            event = self._build_event("add_to_cart", product=new_product)
            self.cart.append(new_product)

        return event

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _start_session(self) -> None:
        self.session_id = str(uuid.uuid4())
        self.state = State.PAGE_VIEW
        self.current_product = None
        self.cart = []
        self.page_views_done = 0
        self.max_page_views = random.randint(1, 8)

    def _build_event(self, event_type: str, product: dict | None) -> dict:
        return {
            "event_id":         str(uuid.uuid4()),
            "user_id":          self.user_id,
            "session_id":       self.session_id,
            "event_type":       event_type,
            "product_id":       product["product_id"]       if product else None,
            "product_name":     product["product_name"]     if product else None,
            "product_category": product["product_category"] if product else None,
            "product_price":    product["product_price"]    if product else None,
            "timestamp":        datetime.now(timezone.utc).isoformat(),
            "device_type":      self.device_type,
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

    print(f"Connected.  Simulating {NUM_USERS} users → topic '{KAFKA_TOPIC}'")
    print("Press Ctrl+C to stop.\n")

    # Build user pool with weighted device assignment
    users = [
        UserSession(
            user_id=f"user_{i:04d}",
            device_type=random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0],
        )
        for i in range(1, NUM_USERS + 1)
    ]

    shutdown = threading.Event()

    def handle_sigint(sig, frame):
        print("\n[SIGNAL] Shutting down gracefully ...")
        shutdown.set()

    signal.signal(signal.SIGINT, handle_sigint)

    total_events = 0
    event_counts: dict[str, int] = {
        "page_view": 0, "product_view": 0,
        "add_to_cart": 0, "remove_from_cart": 0, "purchase": 0,
    }
    last_report = time.monotonic()

    while not shutdown.is_set():
        tick_start = time.monotonic()

        # Pick a random subset of users to tick this second
        target = random.randint(*EVENTS_PER_SECOND_RANGE)
        candidates = random.choices(users, k=min(target, NUM_USERS))

        for user in candidates:
            if shutdown.is_set():
                break
            event = user.next_event()
            if event is None:
                continue

            producer.send(KAFKA_TOPIC, value=event).add_errback(on_send_error)
            total_events += 1
            event_counts[event["event_type"]] = event_counts.get(event["event_type"], 0) + 1

        # Print stats every STATS_INTERVAL_SECONDS seconds
        now = time.monotonic()
        if now - last_report >= STATS_INTERVAL_SECONDS:
            active = sum(1 for u in users if u.is_active)
            ts = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{ts}] total={total_events:,} | active_sessions={active}/{NUM_USERS} | "
                + " ".join(f"{k}={v:,}" for k, v in event_counts.items())
            )
            last_report = now

        # Pace to ~1 tick per second
        elapsed = time.monotonic() - tick_start
        shutdown.wait(timeout=max(0.0, 1.0 - elapsed))

    producer.flush()
    producer.close()
    print(f"\nDone. Total events produced: {total_events:,}")
    for k, v in event_counts.items():
        print(f"  {k:<20} {v:,}")


if __name__ == "__main__":
    main()
