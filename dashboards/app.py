"""
E-Commerce Pricing Intelligence — Streamlit Dashboard
Run: streamlit run dashboards/app.py
"""

import time

import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="E-Commerce Pricing Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Database ──────────────────────────────────────────────────────────────────
_DB_DSN = (
    "host=localhost port=5432 dbname=ecommerce_platform "
    "user=postgres password=postgres123 "
    "options='-c search_path=ecommerce'"
)


def run_query(sql: str, params=None) -> pd.DataFrame:
    with psycopg.connect(_DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            cols = [d.name for d in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# ── Sidebar nav ───────────────────────────────────────────────────────────────
st.sidebar.title("📊 Pricing Intelligence")
page = st.sidebar.radio(
    "Navigation",
    ["Overview", "Inventory", "Pricing Intelligence", "Live Activity"],
    label_visibility="collapsed",
)
st.sidebar.divider()
st.sidebar.caption(f"Last refreshed: {time.strftime('%H:%M:%S')}")


# ── Shared helpers ────────────────────────────────────────────────────────────
def metric_row(items: list) -> None:
    """Render a row of st.metric cards. Each item: (label, value) or (label, value, delta)."""
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        label, value, *rest = item
        col.metric(label, value, rest[0] if rest else None)


def _rec_color(val: str) -> str:
    return {
        "raise": "color: #00cc66; font-weight: bold",
        "lower": "color: #ff4c4c; font-weight: bold",
        "hold":  "color: #aaaaaa; font-weight: bold",
    }.get(str(val), "")


def style_recommendations(df: pd.DataFrame, price_cols: list, extra_fmt: dict | None = None) -> object:
    fmt = {
        "current_price":        "${:.2f}",
        "avg_competitor_price": "${:.2f}",
        "recommended_price":    "${:.2f}",
        "confidence_score":     "{:.1%}",
    }
    for col in price_cols:
        if col not in fmt:
            fmt[col] = "${:.2f}"
    if extra_fmt:
        fmt.update(extra_fmt)
    # Only format columns that actually exist in df
    fmt = {k: v for k, v in fmt.items() if k in df.columns}
    return df.style.map(_rec_color, subset=["recommendation"]).format(fmt)


# =============================================================================
# PAGE 1 — Overview
# =============================================================================
if page == "Overview":
    st.title("Overview")

    # ── KPI cards ─────────────────────────────────────────────────────────────
    kpis = run_query("""
        SELECT
            (SELECT COUNT(*) FROM products)                             AS total_products,
            COALESCE(SUM(revenue), 0)                                   AS total_revenue,
            COALESCE(SUM(total_purchases), 0)                           AS total_purchases,
            COALESCE(AVG(conversion_rate) FILTER (WHERE total_views > 0) * 100, 0)
                                                                        AS avg_conversion_rate
        FROM product_metrics
    """)
    r = kpis.iloc[0]
    metric_row([
        ("Total Products",      f"{int(r['total_products']):,}"),
        ("Total Revenue",       f"${float(r['total_revenue']):,.2f}"),
        ("Total Purchases",     f"{int(r['total_purchases']):,}"),
        ("Avg Conversion Rate", f"{float(r['avg_conversion_rate']):.2f}%"),
    ])

    st.divider()

    # ── Top 10 products by revenue ────────────────────────────────────────────
    top10 = run_query("""
        SELECT p.product_name, pm.revenue
        FROM   product_metrics pm
        JOIN   products p ON pm.product_id = p.product_id
        WHERE  pm.revenue > 0
        ORDER  BY pm.revenue DESC
        LIMIT  10
    """)
    if not top10.empty:
        fig = px.bar(
            top10,
            x="product_name", y="revenue",
            title="Top 10 Products by Revenue",
            labels={"product_name": "Product", "revenue": "Revenue ($)"},
            color="revenue",
            color_continuous_scale="Blues",
            template="plotly_dark",
        )
        fig.update_layout(
            xaxis_tickangle=-30,
            coloraxis_showscale=False,
            margin=dict(b=120),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No revenue data available yet.")

    st.divider()

    # ── Latest 10 pricing recommendations ────────────────────────────────────
    st.subheader("Latest Pricing Recommendations")
    latest_recs = run_query("""
        SELECT product_name, current_price, avg_competitor_price,
               recommendation, confidence_score, recommended_price
        FROM   pricing_recommendations
        ORDER  BY timestamp DESC
        LIMIT  10
    """)
    if not latest_recs.empty:
        st.dataframe(
            style_recommendations(latest_recs, []),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No pricing recommendations available yet.")


# =============================================================================
# PAGE 2 — Inventory
# =============================================================================
elif page == "Inventory":
    st.title("Inventory")

    # ── KPI cards ─────────────────────────────────────────────────────────────
    inv_kpis = run_query("""
        WITH per_product AS (
            SELECT product_id, SUM(current_stock) AS total
            FROM   inventory_state
            GROUP  BY product_id
        )
        SELECT
            (SELECT COALESCE(SUM(current_stock), 0) FROM inventory_state) AS total_stock,
            COUNT(*) FILTER (WHERE total < 20)                             AS low_stock_alerts,
            COUNT(*) FILTER (WHERE total = 0)                              AS out_of_stock
        FROM per_product
    """)
    r = inv_kpis.iloc[0]
    metric_row([
        ("Total Stock (all warehouses)", f"{int(r['total_stock']):,}"),
        ("Low Stock Alerts (< 20)",      f"{int(r['low_stock_alerts'])}"),
        ("Out of Stock",                 f"{int(r['out_of_stock'])}"),
    ])

    st.divider()

    # ── Pivoted stock table ───────────────────────────────────────────────────
    st.subheader("Stock by Product & Warehouse")
    inv = run_query("""
        SELECT
            p.product_name,
            p.category,
            COALESCE(MAX(CASE WHEN i.warehouse_id = 'WH-LAGOS' THEN i.current_stock END), 0) AS "WH-LAGOS",
            COALESCE(MAX(CASE WHEN i.warehouse_id = 'WH-ABUJA' THEN i.current_stock END), 0) AS "WH-ABUJA",
            COALESCE(MAX(CASE WHEN i.warehouse_id = 'WH-PH'    THEN i.current_stock END), 0) AS "WH-PH"
        FROM   products p
        LEFT JOIN inventory_state i ON p.product_id = i.product_id
        GROUP  BY p.product_id, p.product_name, p.category
        ORDER  BY p.product_name
    """)

    if not inv.empty:
        wh_cols = ["WH-LAGOS", "WH-ABUJA", "WH-PH"]
        inv["Total"] = inv[wh_cols].sum(axis=1)
        stock_cols = wh_cols + ["Total"]

        def _stock_color(val):
            if not isinstance(val, (int, float)):
                return ""
            if val < 10:
                return "background-color: #5c1010; color: #ff8080"
            if val < 30:
                return "background-color: #4a3800; color: #ffd700"
            return "background-color: #0d3320; color: #66ff99"

        st.dataframe(
            inv.style.map(_stock_color, subset=stock_cols),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No inventory data available yet.")

    st.divider()

    # ── Stock by category ─────────────────────────────────────────────────────
    st.subheader("Stock Levels by Category")
    cat_stock = run_query("""
        SELECT p.category, SUM(i.current_stock) AS total_stock
        FROM   products p
        JOIN   inventory_state i ON p.product_id = i.product_id
        GROUP  BY p.category
        ORDER  BY total_stock DESC
    """)
    if not cat_stock.empty:
        fig = px.bar(
            cat_stock,
            x="category", y="total_stock",
            title="Total Stock by Category",
            labels={"category": "Category", "total_stock": "Total Stock"},
            color="category",
            template="plotly_dark",
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)


# =============================================================================
# PAGE 3 — Pricing Intelligence
# =============================================================================
elif page == "Pricing Intelligence":
    st.title("Pricing Intelligence")

    # ── KPI cards ─────────────────────────────────────────────────────────────
    rec_kpis = run_query("""
        WITH latest AS (
            SELECT DISTINCT ON (product_id) recommendation
            FROM   pricing_recommendations
            ORDER  BY product_id, timestamp DESC
        )
        SELECT
            COUNT(*) FILTER (WHERE recommendation = 'raise') AS to_raise,
            COUNT(*) FILTER (WHERE recommendation = 'lower') AS to_lower,
            COUNT(*) FILTER (WHERE recommendation = 'hold')  AS on_hold
        FROM latest
    """)
    r = rec_kpis.iloc[0]
    metric_row([
        ("Products to Raise", f"{int(r['to_raise'])}"),
        ("Products to Lower", f"{int(r['to_lower'])}"),
        ("Products on Hold",  f"{int(r['on_hold'])}"),
    ])

    st.divider()

    # ── Full recommendations table ────────────────────────────────────────────
    st.subheader("All Pricing Recommendations")
    all_recs = run_query("""
        SELECT DISTINCT ON (product_id)
            product_name, current_price, avg_competitor_price,
            demand_velocity, recommendation, confidence_score,
            recommended_price, timestamp
        FROM   pricing_recommendations
        ORDER  BY product_id, timestamp DESC
    """)
    if not all_recs.empty:
        st.dataframe(
            style_recommendations(
                all_recs, [],
                extra_fmt={"demand_velocity": "{:.4f}"},
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No pricing recommendations available yet.")

    st.divider()

    # ── Scatter: price diff % vs demand velocity ──────────────────────────────
    st.subheader("Price Difference vs Demand Velocity")
    scatter = run_query("""
        SELECT DISTINCT ON (product_id)
            product_name,
            ROUND(
                (current_price - avg_competitor_price)
                / NULLIF(avg_competitor_price, 0) * 100,
                2
            ) AS price_difference_pct,
            demand_velocity,
            recommendation
        FROM   pricing_recommendations
        ORDER  BY product_id, timestamp DESC
    """)
    if not scatter.empty:
        fig = px.scatter(
            scatter,
            x="price_difference_pct",
            y="demand_velocity",
            color="recommendation",
            color_discrete_map={
                "raise": "#00cc66",
                "lower": "#ff4c4c",
                "hold":  "#aaaaaa",
            },
            hover_name="product_name",
            title="Our Price vs Competitor (%) — coloured by Recommendation",
            labels={
                "price_difference_pct": "Price vs Competitor (%)",
                "demand_velocity":      "Demand Velocity (events / min)",
            },
            template="plotly_dark",
            size_max=12,
        )
        fig.add_vline(x=0, line_dash="dash", line_color="#555555", opacity=0.7)
        fig.update_traces(marker=dict(size=10, opacity=0.85))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for scatter plot yet.")


# =============================================================================
# PAGE 4 — Live Activity
# =============================================================================
elif page == "Live Activity":
    st.title("Live Activity")

    # ── Latest 50 clickstream events ─────────────────────────────────────────
    st.subheader("Latest 50 Clickstream Events")
    events = run_query("""
        SELECT
            ce.event_type,
            p.product_name,
            ce.device_type,
            ce.timestamp
        FROM   clickstream_events ce
        LEFT JOIN products p ON ce.product_id = p.product_id
        ORDER  BY ce.timestamp DESC
        LIMIT  50
    """)
    if not events.empty:
        st.dataframe(events, use_container_width=True, hide_index=True)
    else:
        st.info("No clickstream events yet.")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        # ── Event type pie ────────────────────────────────────────────────────
        evt_dist = run_query("""
            SELECT event_type, COUNT(*) AS count
            FROM   clickstream_events
            GROUP  BY event_type
            ORDER  BY count DESC
        """)
        if not evt_dist.empty:
            fig_pie = px.pie(
                evt_dist,
                names="event_type",
                values="count",
                title="Event Type Distribution",
                template="plotly_dark",
                hole=0.35,
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No event data yet.")

    with col2:
        # ── Device type bar ───────────────────────────────────────────────────
        dev_dist = run_query("""
            SELECT device_type, COUNT(*) AS count
            FROM   clickstream_events
            GROUP  BY device_type
            ORDER  BY count DESC
        """)
        if not dev_dist.empty:
            fig_bar = px.bar(
                dev_dist,
                x="device_type", y="count",
                title="Activity by Device Type",
                labels={"device_type": "Device", "count": "Events"},
                color="device_type",
                template="plotly_dark",
            )
            fig_bar.update_layout(showlegend=False)
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No device data yet.")


# =============================================================================
# Auto-refresh every 30 seconds
# =============================================================================
st.sidebar.divider()
_refresh_slot = st.sidebar.empty()
for _i in range(30, 0, -1):
    _refresh_slot.caption(f"Refreshing in {_i}s...")
    time.sleep(1)
_refresh_slot.caption("Refreshing...")
st.rerun()
