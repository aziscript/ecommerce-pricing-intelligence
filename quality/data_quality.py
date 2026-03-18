#!/usr/bin/env python3
"""
data_quality.py — Automated data quality checks for the e-commerce pricing pipeline.

Usage:
    python data_quality.py              # standard report
    python data_quality.py --verbose    # also prints SQL and row counts

Exit codes:
    0 — all checks passed
    1 — one or more checks failed
"""

import argparse
import sys
import textwrap
from dataclasses import dataclass, field
from typing import Any

import psycopg

# ── Connection ─────────────────────────────────────────────────────────────────
DSN = (
    "host=localhost port=5432 dbname=ecommerce_platform "
    "user=postgres password=postgres123 "
    "options='-c search_path=ecommerce'"
)

# ── Terminal colours (disabled automatically when not a TTY) ───────────────────
_IS_TTY = sys.stdout.isatty()

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _IS_TTY else text

GREEN  = lambda t: _c("32", t)
RED    = lambda t: _c("31", t)
YELLOW = lambda t: _c("33", t)
BOLD   = lambda t: _c("1",  t)
DIM    = lambda t: _c("2",  t)


# ── Result dataclass ───────────────────────────────────────────────────────────
@dataclass
class CheckResult:
    name:    str
    passed:  bool
    detail:  str
    sql:     str  = ""
    rows:    Any  = None   # raw value returned by the query


# ── Core runner ────────────────────────────────────────────────────────────────
def run_check(
    conn: psycopg.Connection,
    name: str,
    sql: str,
    *,
    expect_zero: bool = False,
    expect_exact: int | None = None,
    expect_true: bool = False,
    max_value: int | None = None,
    detail_fn=None,
) -> CheckResult:
    """
    Execute *sql*, evaluate the scalar result against the expectation, and
    return a CheckResult.

    Expectations (mutually exclusive, first matched wins):
        expect_zero   — pass when result == 0
        expect_exact  — pass when result == expect_exact
        expect_true   — pass when result is truthy (bool query)
        max_value     — pass when result <= max_value
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    value = row[0] if row else None

    if expect_zero:
        passed = value == 0
        detail = f"count = {value} (expected 0)"
    elif expect_exact is not None:
        passed = value == expect_exact
        detail = f"count = {value} (expected {expect_exact})"
    elif expect_true:
        passed = bool(value)
        detail = f"result = {value}"
    elif max_value is not None:
        passed = value is not None and value <= max_value
        detail = f"max = {value} (limit {max_value:,})"
    else:
        raise ValueError("No expectation set for check: " + name)

    if detail_fn:
        detail = detail_fn(value, passed)

    return CheckResult(name=name, passed=passed, detail=detail, sql=sql, rows=value)


# ── Check definitions ──────────────────────────────────────────────────────────
def build_checks() -> list[dict]:
    """
    Return a list of check specs.  Each spec is a dict of kwargs forwarded
    directly to run_check(), plus a 'group' key for display grouping.
    """
    VALID_EVENT_TYPES = "('page_view', 'product_view', 'add_to_cart', 'remove_from_cart', 'purchase')"
    VALID_COMPETITORS = "('TechMart', 'GadgetZone', 'ElectroHub')"

    return [
        # ── Inventory ────────────────────────────────────────────────────────
        {
            "group": "Inventory",
            "name":  "No negative stock",
            "sql": textwrap.dedent("""\
                SELECT COUNT(*)
                FROM   ecommerce.inventory_state
                WHERE  current_stock < 0"""),
            "expect_zero": True,
        },
        {
            "group": "Inventory",
            "name":  "All products have inventory rows (90 expected)",
            "sql": textwrap.dedent("""\
                SELECT COUNT(*)
                FROM   ecommerce.inventory_state"""),
            "expect_exact": 90,
            "detail_fn": lambda v, ok: (
                f"rows = {v} (expected 90 = 30 products × 3 warehouses)"
            ),
        },
        {
            "group": "Inventory",
            "name":  "Stock values are reasonable (≤ 100,000 per warehouse slot)",
            "sql": textwrap.dedent("""\
                SELECT COALESCE(MAX(current_stock), 0)
                FROM   ecommerce.inventory_state"""),
            "max_value": 100_000,
        },

        # ── Clickstream ──────────────────────────────────────────────────────
        {
            "group": "Clickstream",
            "name":  "Valid event types only",
            "sql": textwrap.dedent(f"""\
                SELECT COUNT(*)
                FROM   ecommerce.clickstream_events
                WHERE  event_type NOT IN {VALID_EVENT_TYPES}"""),
            "expect_zero": True,
            "detail_fn": lambda v, ok: (
                f"{v} invalid event type(s) found"
                if not ok else "all event types valid"
            ),
        },
        {
            "group": "Clickstream",
            "name":  "Every event references a valid product_id",
            "sql": textwrap.dedent("""\
                SELECT COUNT(*)
                FROM   ecommerce.clickstream_events ce
                WHERE  ce.product_id IS NOT NULL
                  AND  ce.product_id NOT IN (
                           SELECT product_id FROM ecommerce.products
                       )"""),
            "expect_zero": True,
            "detail_fn": lambda v, ok: (
                f"{v} orphaned event(s) with unknown product_id"
                if not ok else "all product references valid"
            ),
        },
        {
            "group": "Clickstream",
            "name":  "No future timestamps (tolerance: 5 minutes)",
            "sql": textwrap.dedent("""\
                SELECT COUNT(*)
                FROM   ecommerce.clickstream_events
                WHERE  timestamp > NOW() + INTERVAL '5 minutes'"""),
            "expect_zero": True,
            "detail_fn": lambda v, ok: (
                f"{v} event(s) with timestamp more than 5 min in the future"
                if not ok else "no future timestamps"
            ),
        },
        {
            "group": "Clickstream",
            "name":  "Table is not empty",
            "sql": textwrap.dedent("""\
                SELECT COUNT(*)
                FROM   ecommerce.clickstream_events"""),
            "expect_exact": None,   # overridden below via detail_fn + custom logic
            # We need at least 1 row — handled by max_value trick with a wrapper
            "max_value": 2_000_000_000,   # effectively no upper bound
            "detail_fn": lambda v, ok: (
                "table is empty — no events ingested yet" if v == 0
                else f"{v:,} event(s) present"
            ),
            # Patch: re-evaluate pass condition as v >= 1
            "_pass_override": lambda v: v is not None and v >= 1,
        },

        # ── Competitor Prices ────────────────────────────────────────────────
        {
            "group": "Competitor Prices",
            "name":  "All competitor prices are positive",
            "sql": textwrap.dedent("""\
                SELECT COUNT(*)
                FROM   ecommerce.competitor_prices
                WHERE  competitor_price <= 0"""),
            "expect_zero": True,
            "detail_fn": lambda v, ok: (
                f"{v} row(s) with non-positive price" if not ok
                else "all prices positive"
            ),
        },
        {
            "group": "Competitor Prices",
            "name":  "Price difference percentage within ±80%",
            "sql": textwrap.dedent("""\
                SELECT COUNT(*)
                FROM   ecommerce.competitor_prices
                WHERE  price_difference_pct < -80
                   OR  price_difference_pct > 80"""),
            "expect_zero": True,
            "detail_fn": lambda v, ok: (
                f"{v} row(s) outside ±80% range" if not ok
                else "all price differences within ±80%"
            ),
        },
        {
            "group": "Competitor Prices",
            "name":  "All competitor names are in the known list",
            "sql": textwrap.dedent(f"""\
                SELECT COUNT(*)
                FROM   ecommerce.competitor_prices
                WHERE  competitor_name NOT IN {VALID_COMPETITORS}"""),
            "expect_zero": True,
            "detail_fn": lambda v, ok: (
                f"{v} row(s) with unrecognised competitor name" if not ok
                else "all competitor names valid"
            ),
        },

        # ── Pricing Recommendations ──────────────────────────────────────────
        {
            "group": "Pricing Recommendations",
            "name":  "All recommendation values are valid",
            "sql": textwrap.dedent("""\
                SELECT COUNT(*)
                FROM   ecommerce.pricing_recommendations
                WHERE  recommendation NOT IN ('raise', 'lower', 'hold')"""),
            "expect_zero": True,
            "detail_fn": lambda v, ok: (
                f"{v} row(s) with invalid recommendation value" if not ok
                else "all recommendation values valid"
            ),
        },
        {
            "group": "Pricing Recommendations",
            "name":  "Confidence scores are between 0 and 1",
            "sql": textwrap.dedent("""\
                SELECT COUNT(*)
                FROM   ecommerce.pricing_recommendations
                WHERE  confidence_score < 0
                   OR  confidence_score > 1"""),
            "expect_zero": True,
            "detail_fn": lambda v, ok: (
                f"{v} row(s) with out-of-range confidence score" if not ok
                else "all confidence scores in [0, 1]"
            ),
        },
        {
            "group": "Pricing Recommendations",
            "name":  "Every recommendation references a valid product_id",
            "sql": textwrap.dedent("""\
                SELECT COUNT(*)
                FROM   ecommerce.pricing_recommendations pr
                WHERE  pr.product_id NOT IN (
                           SELECT product_id FROM ecommerce.products
                       )"""),
            "expect_zero": True,
            "detail_fn": lambda v, ok: (
                f"{v} recommendation(s) with unknown product_id" if not ok
                else "all product references valid"
            ),
        },
    ]


# ── Execution ──────────────────────────────────────────────────────────────────
def execute_checks(conn: psycopg.Connection, specs: list[dict]) -> list[CheckResult]:
    results = []
    for spec in specs:
        spec = dict(spec)          # don't mutate the original
        spec.pop("group", None)
        pass_override = spec.pop("_pass_override", None)

        result = run_check(conn, **spec)

        if pass_override is not None:
            result.passed = pass_override(result.rows)
            # Re-run detail_fn with the corrected pass flag
            if spec.get("detail_fn"):
                result.detail = spec["detail_fn"](result.rows, result.passed)

        results.append(result)
    return results


# ── Reporting ──────────────────────────────────────────────────────────────────
_PASS_LABEL = "  PASS  "
_FAIL_LABEL = "  FAIL  "
_COL_WIDTH   = 54   # width of the check-name column

def print_report(specs: list[dict], results: list[CheckResult], verbose: bool) -> None:
    # Group results by their group label
    groups: dict[str, list[tuple[dict, CheckResult]]] = {}
    for spec, result in zip(specs, results):
        g = spec.get("group", "General")
        groups.setdefault(g, []).append((spec, result))

    print()
    print(BOLD("=" * 70))
    print(BOLD("  Data Quality Report — E-Commerce Pricing Intelligence Pipeline"))
    print(BOLD("=" * 70))

    for group_name, pairs in groups.items():
        print()
        print(BOLD(f"  {group_name}"))
        print(DIM("  " + "─" * 66))

        for spec, r in pairs:
            status = (
                GREEN(f"[{_PASS_LABEL}]") if r.passed
                else RED(f"[{_FAIL_LABEL}]")
            )
            name_col = r.name.ljust(_COL_WIDTH)
            print(f"  {status}  {name_col}  {DIM(r.detail)}")

            if verbose:
                # Indent and print the SQL
                indented_sql = textwrap.indent(r.sql.strip(), "              ")
                print(DIM(f"          SQL:\n{indented_sql}"))
                print(DIM(f"          Raw value: {r.rows!r}"))
                print()

    # ── Summary ───────────────────────────────────────────────────────────────
    total  = len(results)
    passed = sum(r.passed for r in results)
    failed = total - passed

    print()
    print(BOLD("=" * 70))
    if failed == 0:
        summary = GREEN(f"  ALL {total}/{total} CHECKS PASSED")
    else:
        summary = RED(f"  {passed}/{total} CHECKS PASSED  —  {failed} FAILED")
    print(summary)
    print(BOLD("=" * 70))
    print()


# ── Entry point ────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run data quality checks against the pricing intelligence database."
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print the SQL query and raw row count for each check.",
    )
    args = parser.parse_args()

    try:
        conn = psycopg.connect(DSN)
    except Exception as exc:
        print(RED(f"ERROR: could not connect to database.\n  {exc}"), file=sys.stderr)
        return 1

    specs   = build_checks()
    results = execute_checks(conn, specs)
    conn.close()

    print_report(specs, results, verbose=args.verbose)

    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
