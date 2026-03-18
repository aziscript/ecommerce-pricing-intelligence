#!/usr/bin/env python3
"""
lake/parquet_writer.py

Exports new events from PostgreSQL to MinIO as date-partitioned Parquet files.

Data layout in MinIO:
    data-lake/
      clickstream/date=YYYY-MM-DD/events.parquet
      inventory/date=YYYY-MM-DD/events.parquet
      competitor_prices/date=YYYY-MM-DD/events.parquet

State is persisted in lake/.export_state.json so each run only exports
records newer than the last successful export per table.

Usage:
    python lake/parquet_writer.py              # export all new data
    python lake/parquet_writer.py --reset      # clear state, re-export everything

Environment variables (set in .env or export in shell):
    MINIO_ENDPOINT      MinIO base URL      (default: http://localhost:9000)
    MINIO_ACCESS_KEY    MinIO access key    (default: minioadmin)
    MINIO_SECRET_KEY    MinIO secret key    (default: minioadmin)
    MINIO_BUCKET        Target bucket       (default: data-lake)
"""

import argparse
import io
import json
import logging
import os
import pathlib
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Load .env from the project root (two levels up from this file) if present.
# Simple parser — no extra dependency needed.
# ---------------------------------------------------------------------------
_ENV_FILE = pathlib.Path(__file__).parent.parent / ".env"
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import psycopg

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_DSN = (
    "host=localhost port=5432 dbname=ecommerce_platform "
    "user=postgres password=postgres123 "
    "options='-c search_path=ecommerce'"
)

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",    "http://localhost:9000")
MINIO_ACCESS_KEY = (os.environ.get("MINIO_ACCESS_KEY") or
                    os.environ.get("MINIO_ROOT_USER",  "minioadmin"))
MINIO_SECRET_KEY = (os.environ.get("MINIO_SECRET_KEY") or
                    os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",      "data-lake")

STATE_FILE = pathlib.Path(__file__).parent / ".export_state.json"

EPOCH_START = "1970-01-01T00:00:00+00:00"

# ---------------------------------------------------------------------------
# Tables to export: (table_name, timestamp_column, prefix_in_lake)
# ---------------------------------------------------------------------------
EXPORT_TARGETS = [
    ("clickstream_events", "timestamp", "clickstream"),
    ("inventory_events",   "timestamp", "inventory"),
    ("competitor_prices",  "timestamp", "competitor_prices"),
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------
def load_state() -> dict[str, str]:
    """Return dict mapping table name → ISO timestamp of last exported row."""
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict[str, str]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def fetch_new_rows(
    conn: psycopg.Connection,
    table: str,
    ts_col: str,
    since: str,
) -> pd.DataFrame:
    """Fetch all rows from *table* where *ts_col* > *since*."""
    sql = f"""
        SELECT *
        FROM ecommerce.{table}
        WHERE {ts_col} > %s
        ORDER BY {ts_col}
    """
    with conn.cursor() as cur:
        cur.execute(sql, (since,))
        cols = [d.name for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# MinIO / S3 helpers
# ---------------------------------------------------------------------------
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
            log.info("Created bucket: %s", bucket)
        else:
            raise


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns with non-Parquet-serialisable types (UUID, Decimal) to str/float."""
    import uuid, decimal
    df = df.copy()
    for col in df.columns:
        sample = df[col].dropna()
        if sample.empty:
            continue
        first = sample.iloc[0]
        if isinstance(first, uuid.UUID):
            df[col] = df[col].astype(str)
        elif isinstance(first, decimal.Decimal):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialise a DataFrame to Parquet bytes in memory."""
    buf = io.BytesIO()
    _coerce_types(df).to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


def upload_partition(
    s3,
    bucket: str,
    prefix: str,
    date_str: str,
    data: bytes,
) -> str:
    """Upload *data* to bucket/prefix/date=YYYY-MM-DD/events.parquet."""
    key = f"{prefix}/date={date_str}/events.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    return key


# ---------------------------------------------------------------------------
# Core export logic
# ---------------------------------------------------------------------------
def export_table(
    conn: psycopg.Connection,
    s3,
    table: str,
    ts_col: str,
    lake_prefix: str,
    since: str,
) -> Optional[str]:
    """
    Fetch new rows for *table*, split by date, upload each partition to MinIO.
    Returns the ISO timestamp of the latest row exported, or None if no rows.
    """
    log.info("[%s] fetching rows newer than %s", table, since)
    df = fetch_new_rows(conn, table, ts_col, since)

    if df.empty:
        log.info("[%s] no new rows", table)
        return None

    log.info("[%s] %d new rows to export", table, len(df))

    # Normalise the timestamp column to Python datetime so we can extract dates
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
    df["_date"] = df[ts_col].dt.date.astype(str)

    latest_ts: Optional[str] = None
    total_uploaded = 0

    for date_str, partition_df in df.groupby("_date"):
        partition_df = partition_df.drop(columns=["_date"])
        parquet_bytes = df_to_parquet_bytes(partition_df)
        key = upload_partition(s3, MINIO_BUCKET, lake_prefix, date_str, parquet_bytes)
        log.info("[%s] uploaded %s (%d rows, %d bytes)", table, key, len(partition_df), len(parquet_bytes))
        total_uploaded += len(partition_df)

    latest_ts = df[ts_col].max().isoformat()
    log.info("[%s] done — %d rows in %d partition(s), latest: %s", table, total_uploaded, df["_date"].nunique(), latest_ts)
    return latest_ts


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export new PostgreSQL events to MinIO as Parquet files."
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear export state and re-export all historical data.",
    )
    args = parser.parse_args()

    if args.reset and STATE_FILE.exists():
        STATE_FILE.unlink()
        log.info("State reset — will re-export all data.")

    state = load_state()

    # Connect
    log.info("Connecting to PostgreSQL...")
    conn = psycopg.connect(DB_DSN)

    log.info("Connecting to MinIO at %s...", MINIO_ENDPOINT)
    s3 = get_s3_client()
    ensure_bucket(s3, MINIO_BUCKET)

    # Export each table
    updated = False
    for table, ts_col, prefix in EXPORT_TARGETS:
        since = state.get(table, EPOCH_START)
        latest = export_table(conn, s3, table, ts_col, prefix, since)
        if latest:
            state[table] = latest
            updated = True

    conn.close()

    if updated:
        save_state(state)
        log.info("State saved to %s", STATE_FILE)
    else:
        log.info("No new data — nothing to export.")


if __name__ == "__main__":
    main()
