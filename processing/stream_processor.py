"""
stream_processor.py
Core stream processing engine for the e-commerce pricing intelligence platform.

Consumers (one thread each):
  raw.clickstream        → clickstream_events + product_metrics
  raw.inventory_updates  → inventory_events + inventory_state
  raw.competitor_prices  → competitor_prices

Pricing engine (one thread):
  Runs every 60 s, reads competitor_prices + clickstream_events,
  writes pricing_recommendations.
"""

import json
import logging
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Callable

import psycopg
from psycopg.rows import dict_row
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = "localhost:9092"

TOPIC_CLICKSTREAM = "raw.clickstream"
TOPIC_INVENTORY   = "raw.inventory_updates"
TOPIC_COMPETITOR  = "raw.competitor_prices"

DB_CONFIG = dict(
    host="localhost",
    port=5432,
    dbname="ecommerce_platform",
    user="postgres",
    password="postgres123",
    options="-c search_path=ecommerce",
)

BATCH_SIZE          = 100   # flush after this many buffered ops
BATCH_TIMEOUT_S     = 2.0   # … or after this many seconds
PRICING_INTERVAL_S  = 60    # run pricing engine every N seconds
STATS_INTERVAL_S    = 10    # print stats every N seconds

DEMAND_HIGH = 5             # purchases / hour → "high demand"
DEMAND_LOW  = 2             # purchases / hour → "low demand"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-22s] %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared shutdown + stats
# ---------------------------------------------------------------------------
shutdown    = threading.Event()
_stats_lock = threading.Lock()
_stats: dict[str, int] = defaultdict(int)


def inc(key: str, n: int = 1) -> None:
    with _stats_lock:
        _stats[key] += n


def snapshot_stats() -> dict[str, int]:
    with _stats_lock:
        return dict(_stats)


# ---------------------------------------------------------------------------
# SQL — clickstream
# ---------------------------------------------------------------------------
SQL_INSERT_CLICKSTREAM = """
INSERT INTO clickstream_events
    (event_id, user_id, session_id, event_type, product_id, product_price, device_type, timestamp)
VALUES (%s, %s, %s::uuid, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""

SQL_METRICS_VIEW = """
UPDATE product_metrics
SET total_views     = total_views + 1,
    conversion_rate = CASE WHEN total_views + 1 > 0
                          THEN total_purchases::numeric / (total_views + 1)
                          ELSE 0 END,
    last_updated    = NOW()
WHERE product_id = %s
"""

SQL_METRICS_CART = """
UPDATE product_metrics
SET total_cart_adds = total_cart_adds + 1,
    last_updated    = NOW()
WHERE product_id = %s
"""

SQL_METRICS_PURCHASE = """
UPDATE product_metrics
SET total_purchases = total_purchases + 1,
    revenue         = revenue + %s,
    conversion_rate = CASE WHEN total_views > 0
                          THEN (total_purchases + 1)::numeric / total_views
                          ELSE 0 END,
    last_updated    = NOW()
WHERE product_id = %s
"""

# ---------------------------------------------------------------------------
# SQL — inventory
# ---------------------------------------------------------------------------
SQL_INSERT_INVENTORY_EVENT = """
INSERT INTO inventory_events
    (event_id, product_id, warehouse_id, event_type, quantity_change, timestamp)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""

# Only applies the change when it wouldn't push stock below zero
SQL_UPDATE_INVENTORY_STATE = """
UPDATE inventory_state
SET current_stock = current_stock + %s,
    last_updated  = NOW()
WHERE product_id  = %s
  AND warehouse_id = %s
  AND current_stock + %s >= 0
"""

# ---------------------------------------------------------------------------
# SQL — competitor prices  (CTE-based upsert, no unique constraint needed)
# ---------------------------------------------------------------------------
SQL_UPSERT_COMPETITOR = """
WITH updated AS (
    UPDATE competitor_prices
    SET competitor_price     = %s,
        our_price            = %s,
        price_difference     = %s,
        price_difference_pct = %s,
        timestamp            = %s
    WHERE product_id = %s AND competitor_name = %s
    RETURNING id
)
INSERT INTO competitor_prices
    (product_id, competitor_name, competitor_price, our_price,
     price_difference, price_difference_pct, timestamp)
SELECT %s, %s, %s, %s, %s, %s, %s
WHERE NOT EXISTS (SELECT 1 FROM updated)
"""

# ---------------------------------------------------------------------------
# SQL — pricing recommendations  (CTE-based upsert keyed on product_id)
# ---------------------------------------------------------------------------
SQL_UPSERT_RECOMMENDATION = """
WITH updated AS (
    UPDATE pricing_recommendations
    SET product_name         = %s,
        current_price        = %s,
        avg_competitor_price = %s,
        demand_velocity      = %s,
        recommendation       = %s,
        confidence_score     = %s,
        recommended_price    = %s,
        timestamp            = NOW()
    WHERE product_id = %s
    RETURNING id
)
INSERT INTO pricing_recommendations
    (product_id, product_name, current_price, avg_competitor_price,
     demand_velocity, recommendation, confidence_score, recommended_price)
SELECT %s, %s, %s, %s, %s, %s, %s, %s
WHERE NOT EXISTS (SELECT 1 FROM updated)
"""

# ---------------------------------------------------------------------------
# Pricing engine queries
# ---------------------------------------------------------------------------
SQL_ALL_PRODUCTS = """
SELECT product_id, product_name, base_price FROM products
"""

SQL_DEMAND_LAST_HOUR = """
SELECT product_id, COUNT(*) AS purchases_last_hour
FROM   clickstream_events
WHERE  event_type = 'purchase'
  AND  timestamp  > NOW() - INTERVAL '1 hour'
GROUP  BY product_id
"""

SQL_LATEST_COMPETITOR_PRICES = """
SELECT DISTINCT ON (product_id, competitor_name)
       product_id,
       competitor_name,
       competitor_price
FROM   competitor_prices
ORDER  BY product_id, competitor_name, timestamp DESC
"""


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
def make_conn() -> psycopg.Connection:
    conn = psycopg.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Batcher — buffers callable DB ops and flushes as one transaction
# ---------------------------------------------------------------------------
Op = Callable[[psycopg.Cursor], None]


class Batcher:
    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn       = conn
        self._cur        = conn.cursor()
        self._ops: list[Op] = []
        self._last_flush = time.monotonic()

    def add(self, op: Op) -> None:
        self._ops.append(op)
        if len(self._ops) >= BATCH_SIZE or self._is_due():
            self.flush()

    def tick(self) -> None:
        """Honour the timeout even when event traffic is slow."""
        if self._is_due():
            self.flush()

    def flush(self) -> None:
        if not self._ops:
            self._last_flush = time.monotonic()
            return
        try:
            for op in self._ops:
                op(self._cur)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            log.exception("Batch commit failed — rolled back %d ops", len(self._ops))
        finally:
            self._ops.clear()
            self._last_flush = time.monotonic()

    def close(self) -> None:
        self.flush()
        try:
            self._cur.close()
            self._conn.close()
        except Exception:
            pass

    def _is_due(self) -> bool:
        return time.monotonic() - self._last_flush >= BATCH_TIMEOUT_S


# ---------------------------------------------------------------------------
# Event processors
# ---------------------------------------------------------------------------
def process_clickstream(batcher: Batcher, event: dict) -> None:
    event_id      = event.get("event_id")
    user_id       = event.get("user_id")
    session_id    = event.get("session_id")
    event_type    = event.get("event_type")
    product_id    = event.get("product_id")
    product_price = event.get("product_price")
    device_type   = event.get("device_type")
    timestamp     = event.get("timestamp")

    batcher.add(lambda cur: cur.execute(
        SQL_INSERT_CLICKSTREAM,
        (event_id, user_id, session_id, event_type, product_id, product_price, device_type, timestamp),
    ))

    if not product_id:
        return  # page_view events have no product; nothing to update in metrics

    if event_type == "product_view":
        batcher.add(lambda cur: cur.execute(SQL_METRICS_VIEW, (product_id,)))

    elif event_type == "add_to_cart":
        batcher.add(lambda cur: cur.execute(SQL_METRICS_CART, (product_id,)))

    elif event_type == "purchase":
        batcher.add(lambda cur: cur.execute(SQL_METRICS_PURCHASE, (product_price, product_id)))


def process_inventory(batcher: Batcher, event: dict) -> None:
    event_id        = event.get("event_id")
    product_id      = event.get("product_id")
    warehouse_id    = event.get("warehouse_id")
    event_type      = event.get("event_type")
    quantity_change = event.get("quantity_change")
    timestamp       = event.get("timestamp")

    batcher.add(lambda cur: cur.execute(
        SQL_INSERT_INVENTORY_EVENT,
        (event_id, product_id, warehouse_id, event_type, quantity_change, timestamp),
    ))

    def update_stock(cur: psycopg.Cursor) -> None:
        cur.execute(
            SQL_UPDATE_INVENTORY_STATE,
            (quantity_change, product_id, warehouse_id, quantity_change),
        )
        if cur.rowcount == 0:
            if quantity_change < 0:
                log.warning(
                    "Skipped stock update [%s @ %s]: change=%d would produce negative stock",
                    product_id, warehouse_id, quantity_change,
                )
            else:
                log.error(
                    "inventory_state row missing for product=%s warehouse=%s",
                    product_id, warehouse_id,
                )

    batcher.add(update_stock)


def process_competitor(batcher: Batcher, event: dict) -> None:
    product_id           = event.get("product_id")
    competitor_name      = event.get("competitor_name")
    competitor_price     = event.get("competitor_price")
    our_price            = event.get("our_price")
    price_difference     = event.get("price_difference")
    price_difference_pct = event.get("price_difference_pct")
    timestamp            = event.get("timestamp")

    batcher.add(lambda cur: cur.execute(
        SQL_UPSERT_COMPETITOR,
        (
            # UPDATE SET
            competitor_price, our_price, price_difference, price_difference_pct, timestamp,
            # UPDATE WHERE
            product_id, competitor_name,
            # INSERT SELECT (when UPDATE matched 0 rows)
            product_id, competitor_name, competitor_price, our_price,
            price_difference, price_difference_pct, timestamp,
        ),
    ))


# ---------------------------------------------------------------------------
# Pricing recommendation logic
# ---------------------------------------------------------------------------
def recommend(
    our_price: float,
    avg_comp_price: float,
    demand_velocity: float,
) -> tuple[str, float, float]:
    """
    Returns (recommendation, recommended_price, confidence_score).

    gap_pct > 0  → we are MORE expensive than competitors
    gap_pct < 0  → we are CHEAPER than competitors
    """
    gap_pct = (our_price - avg_comp_price) / avg_comp_price * 100

    if gap_pct > 10 and demand_velocity < DEMAND_LOW:
        # Overpriced + weak demand → lower toward competitor avg
        rec       = "lower"
        rec_price = round(avg_comp_price * 0.99, 2)
        confidence = round(min(gap_pct / 20.0, 1.0), 3)

    elif gap_pct < -10 and demand_velocity >= DEMAND_HIGH:
        # Underpriced + strong demand → raise toward competitor avg
        rec       = "raise"
        rec_price = round(avg_comp_price * 0.99, 2)
        confidence = round(min(abs(gap_pct) / 20.0, 1.0), 3)

    else:
        rec       = "hold"
        rec_price = our_price
        # More confident the closer we are to parity
        confidence = round(max(0.5, 1.0 - abs(gap_pct) / 20.0), 3)

    return rec, rec_price, confidence


# ---------------------------------------------------------------------------
# Pricing engine — runs in its own thread every PRICING_INTERVAL_S seconds
# ---------------------------------------------------------------------------
def pricing_engine() -> None:
    log.info("Pricing engine started (interval: %ds)", PRICING_INTERVAL_S)

    # First run is after one full interval so consumers can warm up
    while not shutdown.wait(timeout=PRICING_INTERVAL_S):
        try:
            _run_pricing_cycle()
        except Exception:
            log.exception("Pricing cycle failed")

    log.info("Pricing engine stopped")


def _run_pricing_cycle() -> None:
    conn = make_conn()
    try:
        cur = conn.cursor(row_factory=dict_row)

        cur.execute(SQL_ALL_PRODUCTS)
        products = cur.fetchall()

        cur.execute(SQL_DEMAND_LAST_HOUR)
        demand: dict[str, float] = {
            row["product_id"]: float(row["purchases_last_hour"])
            for row in cur.fetchall()
        }

        cur.execute(SQL_LATEST_COMPETITOR_PRICES)
        comp_prices: dict[str, list[float]] = defaultdict(list)
        for row in cur.fetchall():
            comp_prices[row["product_id"]].append(float(row["competitor_price"]))

        recs = 0
        for product in products:
            pid        = product["product_id"]
            pname      = product["product_name"]
            our_price  = float(product["base_price"])
            prices     = comp_prices.get(pid)

            if not prices:
                continue  # no competitor data yet — skip

            avg_comp   = round(sum(prices) / len(prices), 2)
            velocity   = demand.get(pid, 0.0)
            rec, rec_price, confidence = recommend(our_price, avg_comp, velocity)

            cur.execute(
                SQL_UPSERT_RECOMMENDATION,
                (
                    # UPDATE SET
                    pname, our_price, avg_comp, velocity, rec, confidence, rec_price,
                    # UPDATE WHERE
                    pid,
                    # INSERT SELECT
                    pid, pname, our_price, avg_comp, velocity, rec, confidence, rec_price,
                ),
            )
            recs += 1

        conn.commit()
        inc("recommendations", recs)
        log.info("Pricing cycle done: %d recommendations upserted", recs)

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Generic Kafka consumer thread
# ---------------------------------------------------------------------------
def consumer_thread(
    topic: str,
    group_id: str,
    process_fn: Callable[[Batcher, dict], None],
    stat_key: str,
) -> None:
    log.info("Connecting to topic '%s' (group: %s)", topic, group_id)
    try:
        conn     = make_conn()
        batcher  = Batcher(conn)
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKER,
            group_id=group_id,
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
    except Exception:
        log.exception("Failed to initialise consumer for '%s' — thread exiting", topic)
        return

    log.info("Consumer ready: '%s'", topic)
    try:
        while not shutdown.is_set():
            records = consumer.poll(timeout_ms=1000)
            for messages in records.values():
                for msg in messages:
                    if shutdown.is_set():
                        break
                    try:
                        process_fn(batcher, msg.value)
                        inc(stat_key)
                    except Exception:
                        log.exception("Error processing message from '%s': %s", topic, msg.value)
            batcher.tick()
    finally:
        batcher.close()
        consumer.close()
        log.info("Consumer for '%s' stopped", topic)


# ---------------------------------------------------------------------------
# Stats printer
# ---------------------------------------------------------------------------
def stats_printer() -> None:
    prev: dict[str, int] = defaultdict(int)

    while not shutdown.wait(timeout=STATS_INTERVAL_S):
        cur   = snapshot_stats()
        ts    = datetime.now().strftime("%H:%M:%S")
        rate  = lambda key: (cur.get(key, 0) - prev.get(key, 0)) / STATS_INTERVAL_S

        print(
            f"[{ts}] "
            f"clickstream={cur.get('clickstream', 0):,} ({rate('clickstream'):.1f}/s) | "
            f"inventory={cur.get('inventory', 0):,} ({rate('inventory'):.1f}/s) | "
            f"competitor={cur.get('competitor', 0):,} ({rate('competitor'):.1f}/s) | "
            f"recommendations={cur.get('recommendations', 0):,}",
            flush=True,
        )
        prev = cur


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    def handle_signal(sig, frame):
        log.info("Shutdown signal received (signal %d)", sig)
        shutdown.set()

    signal.signal(signal.SIGINT,  handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    threads = [
        threading.Thread(
            target=consumer_thread,
            args=(TOPIC_CLICKSTREAM, "pp-clickstream", process_clickstream, "clickstream"),
            name="consumer-clickstream",
            daemon=True,
        ),
        threading.Thread(
            target=consumer_thread,
            args=(TOPIC_INVENTORY, "pp-inventory", process_inventory, "inventory"),
            name="consumer-inventory",
            daemon=True,
        ),
        threading.Thread(
            target=consumer_thread,
            args=(TOPIC_COMPETITOR, "pp-competitor", process_competitor, "competitor"),
            name="consumer-competitor",
            daemon=True,
        ),
        threading.Thread(
            target=pricing_engine,
            name="pricing-engine",
            daemon=True,
        ),
        threading.Thread(
            target=stats_printer,
            name="stats-printer",
            daemon=True,
        ),
    ]

    log.info("Starting stream processor with %d threads", len(threads))
    for t in threads:
        t.start()

    # Block main thread until shutdown is signalled
    try:
        while not shutdown.is_set():
            shutdown.wait(timeout=1.0)
    except KeyboardInterrupt:
        shutdown.set()

    log.info("Waiting for threads to finish ...")
    for t in threads:
        t.join(timeout=10)

    final = snapshot_stats()
    log.info(
        "Final totals — clickstream=%d inventory=%d competitor=%d recommendations=%d",
        final.get("clickstream", 0),
        final.get("inventory", 0),
        final.get("competitor", 0),
        final.get("recommendations", 0),
    )
    log.info("Stream processor stopped")


if __name__ == "__main__":
    main()
