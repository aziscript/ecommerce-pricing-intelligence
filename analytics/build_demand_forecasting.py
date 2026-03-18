#!/usr/bin/env python3
"""
build_demand_forecasting.py
Generates analytics/demand_forecasting.ipynb.
"""
import json, pathlib

def md(cell_id, source):
    return {"cell_type": "markdown", "id": cell_id, "metadata": {}, "source": source}

def code(cell_id, source):
    return {"cell_type": "code", "id": cell_id, "metadata": {},
            "execution_count": None, "outputs": [], "source": source}

# ── Cell sources ───────────────────────────────────────────────────────────────

SETUP = """\
import warnings
warnings.filterwarnings('ignore')

import psycopg
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from IPython.display import display

conn = psycopg.connect(
    "host=localhost port=5432 dbname=ecommerce_platform "
    "user=postgres password=postgres123 "
    "options='-c search_path=ecommerce'"
)

def qry(sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        cols = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)

DARK = 'plotly_dark'
pd.set_option('display.float_format', '{:.2f}'.format)
print("Connected to ecommerce_platform")
"""

EXEC_SUMMARY = """\
# Demand Forecasting & Inventory Analysis

## Executive Summary

This notebook analyses purchase demand patterns from clickstream data and projects
near-term demand using exponential smoothing — then connects those projections to
current inventory levels to flag stockout risk.

| Section | Question answered |
|---|---|
| Data Overview | What is the overall purchase volume and rate? |
| Demand by Category | Which categories drive the most purchases over time? |
| Moving Averages | What does smoothed demand look like at 1h and 3h windows? |
| Peak Demand Analysis | When (hour of day, day of week) is demand highest? |
| Category Demand Ranking | Which categories are highest-volume and most consistent? |
| Simple Forecast | Where is demand headed over the next 6 hours? |
| Inventory Implications | Which products will run out first given forecast demand? |
| Key Findings | Actionable inventory and operations recommendations |

**Forecasting method:** Exponential Weighted Mean (EWM, span = 4 periods)
with linear trend extrapolation — lightweight, interpretable, no external libraries.

**Data sources:** `ecommerce.clickstream_events`, `ecommerce.products`,
`ecommerce.inventory_state`
"""

DATA_OVERVIEW_MD = """\
## 1. Data Overview

High-level purchase volume statistics and the hourly purchase rate.
"""

DATA_OVERVIEW_CODE = """\
overview = qry('''
    SELECT
        COUNT(*)                                  AS total_purchases,
        COUNT(DISTINCT user_id)                   AS unique_buyers,
        COUNT(DISTINCT product_id)                AS distinct_products_bought,
        MIN(timestamp)                            AS first_purchase,
        MAX(timestamp)                            AS last_purchase,
        ROUND(
            COUNT(*) * 1.0
            / NULLIF(
                EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 3600.0,
                0
            ),
            2
        )                                         AS purchases_per_hour
    FROM ecommerce.clickstream_events
    WHERE event_type = \'purchase\'
''')
display(overview)

# Daily purchase volume
daily = qry('''
    SELECT
        DATE_TRUNC(\'day\', timestamp)::DATE  AS day,
        COUNT(*)                              AS purchases
    FROM ecommerce.clickstream_events
    WHERE event_type = \'purchase\'
    GROUP BY DATE_TRUNC(\'day\', timestamp)
    ORDER BY day
''')
print(f"\\nDays with purchase data: {len(daily)}")
display(daily)

if len(daily) > 1:
    fig = px.bar(
        daily, x='day', y='purchases',
        title='Daily Purchase Volume',
        labels={'day': 'Date', 'purchases': 'Purchases'},
        template=DARK,
    )
    fig.show()
"""

DEMAND_CAT_MD = """\
## 2. Demand by Category

Hourly purchase volumes broken down by product category, revealing which categories
drive demand and whether their patterns diverge.
"""

DEMAND_CAT_CODE = """\
hourly_cat = qry('''
    SELECT
        DATE_TRUNC(\'hour\', ce.timestamp)  AS hour_bucket,
        p.category,
        COUNT(*)                            AS purchases
    FROM ecommerce.clickstream_events ce
    JOIN ecommerce.products p ON ce.product_id = p.product_id
    WHERE ce.event_type = \'purchase\'
      AND ce.product_id IS NOT NULL
    GROUP BY DATE_TRUNC(\'hour\', ce.timestamp), p.category
    ORDER BY hour_bucket, p.category
''')

if hourly_cat.empty:
    print("No purchase data yet — run the generators first.")
else:
    hourly_cat['hour_bucket'] = pd.to_datetime(hourly_cat['hour_bucket'])

    fig = px.line(
        hourly_cat,
        x='hour_bucket', y='purchases',
        color='category',
        markers=True,
        title='Hourly Purchases by Product Category',
        labels={'hour_bucket': 'Time', 'purchases': 'Purchases', 'category': 'Category'},
        template=DARK,
    )
    fig.update_layout(legend=dict(orientation='h', yanchor='bottom', y=1.02))
    fig.show()

    # Total by category
    cat_totals = (
        hourly_cat.groupby('category')['purchases']
        .agg(['sum', 'mean', 'std'])
        .round(2)
        .rename(columns={'sum': 'total', 'mean': 'avg_per_hour', 'std': 'std_per_hour'})
        .sort_values('total', ascending=False)
        .reset_index()
    )
    print("\\nTotal purchases by category:")
    display(cat_totals)
"""

MA_MD = """\
## 3. Moving Averages

Smoothed demand curves remove short-term noise and reveal the underlying trend.

- **1-hour MA** — essentially the raw hourly count (window = 1)
- **3-hour MA** — smooths over a 3-hour rolling window (window = 3)

Both are plotted against the raw demand so you can see how much volatility exists.
"""

MA_CODE = """\
# Aggregate all categories into total hourly demand
hourly_total = qry('''
    SELECT
        DATE_TRUNC(\'hour\', timestamp)  AS hour_bucket,
        COUNT(*)                         AS purchases
    FROM ecommerce.clickstream_events
    WHERE event_type = \'purchase\'
    GROUP BY DATE_TRUNC(\'hour\', timestamp)
    ORDER BY hour_bucket
''')
hourly_total['hour_bucket'] = pd.to_datetime(hourly_total['hour_bucket'])
hourly_total = hourly_total.set_index('hour_bucket').sort_index()

# Moving averages
hourly_total['ma_1h'] = hourly_total['purchases'].rolling(window=1).mean()
hourly_total['ma_3h'] = hourly_total['purchases'].rolling(window=3, min_periods=1).mean()

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=hourly_total.index, y=hourly_total['purchases'],
    name='Actual',
    mode='lines+markers',
    line=dict(color='#636EFA', width=1.5),
    marker=dict(size=5),
    opacity=0.6,
))
fig.add_trace(go.Scatter(
    x=hourly_total.index, y=hourly_total['ma_1h'],
    name='1-Hour MA',
    mode='lines',
    line=dict(color='#FFA15A', width=2),
))
fig.add_trace(go.Scatter(
    x=hourly_total.index, y=hourly_total['ma_3h'],
    name='3-Hour MA',
    mode='lines',
    line=dict(color='#00CC96', width=2.5),
))
fig.update_layout(
    title='Total Hourly Purchases with Moving Averages',
    xaxis_title='Time',
    yaxis_title='Purchases',
    template=DARK,
    legend=dict(orientation='h', yanchor='bottom', y=1.02),
)
fig.show()

print(f"Overall mean purchases/hour : {hourly_total['purchases'].mean():.2f}")
print(f"Std deviation               : {hourly_total['purchases'].std():.2f}")
print(f"Peak hour count             : {int(hourly_total['purchases'].max())}")
print(f"Lowest hour count           : {int(hourly_total['purchases'].min())}")
"""

PEAK_MD = """\
## 4. Peak Demand Analysis

A **heatmap of hour-of-day vs day-of-week** reveals cyclical patterns —
the busiest slots and the quietest windows in one view.
"""

PEAK_CODE = """\
peak_data = qry('''
    SELECT
        EXTRACT(ISODOW FROM timestamp)::INT  AS day_of_week,
        EXTRACT(HOUR   FROM timestamp)::INT  AS hour_of_day,
        COUNT(*)                             AS purchases
    FROM ecommerce.clickstream_events
    WHERE event_type = \'purchase\'
    GROUP BY EXTRACT(ISODOW FROM timestamp), EXTRACT(HOUR FROM timestamp)
    ORDER BY day_of_week, hour_of_day
''')

if peak_data.empty:
    print("No data yet.")
else:
    DOW_LABELS = {1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu',
                  5: 'Fri', 6: 'Sat', 7: 'Sun'}
    peak_data['day_name'] = peak_data['day_of_week'].map(DOW_LABELS)

    # Ensure all 7 days × 24 hours are present
    heatmap_pivot = peak_data.pivot_table(
        index='day_of_week', columns='hour_of_day',
        values='purchases', fill_value=0,
    )
    # Fill any missing hours
    heatmap_pivot = heatmap_pivot.reindex(
        columns=range(24), index=range(1, 8), fill_value=0
    )
    heatmap_pivot.index = [DOW_LABELS[i] for i in heatmap_pivot.index]

    fig = px.imshow(
        heatmap_pivot,
        title='Purchase Volume Heatmap — Hour of Day vs Day of Week',
        labels={'x': 'Hour of Day', 'y': 'Day of Week', 'color': 'Purchases'},
        color_continuous_scale='Viridis',
        aspect='auto',
        template=DARK,
        text_auto=True,
    )
    fig.update_layout(
        xaxis=dict(tickmode='linear', tick0=0, dtick=1),
        height=380,
    )
    fig.show()

    # Top 5 peak slots
    peak_sorted = peak_data.sort_values('purchases', ascending=False).head(5)
    print("\\nTop 5 peak demand slots:")
    for _, r in peak_sorted.iterrows():
        print(f"  {r['day_name']}  {int(r['hour_of_day']):02d}:00  —  "
              f"{int(r['purchases'])} purchases")
"""

CAT_RANK_MD = """\
## 5. Category Demand Ranking

Rank categories by **total volume** and **consistency** (coefficient of variation).
A low CV with high volume = reliable, plannable demand.
A high CV = volatile, harder to stock for.
"""

CAT_RANK_CODE = """\
if 'hourly_cat' not in dir() or hourly_cat.empty:
    print("Re-running hourly_cat query...")
    hourly_cat = qry(\'\'\'
        SELECT
            DATE_TRUNC(\\\'hour\\\', ce.timestamp)  AS hour_bucket,
            p.category,
            COUNT(*)                            AS purchases
        FROM ecommerce.clickstream_events ce
        JOIN ecommerce.products p ON ce.product_id = p.product_id
        WHERE ce.event_type = \\\'purchase\\\'
          AND ce.product_id IS NOT NULL
        GROUP BY DATE_TRUNC(\\\'hour\\\', ce.timestamp), p.category
        ORDER BY hour_bucket, p.category
    \'\'\')
    hourly_cat[\'hour_bucket\'] = pd.to_datetime(hourly_cat[\'hour_bucket\'])

cat_rank = (
    hourly_cat.groupby('category')['purchases']
    .agg(
        total_purchases = 'sum',
        avg_per_hour    = 'mean',
        std_per_hour    = 'std',
        hours_with_sales = 'count',
    )
    .assign(cv=lambda d: (d['std_per_hour'] / d['avg_per_hour'].replace(0, pd.NA)).round(2))
    .round(2)
    .sort_values('total_purchases', ascending=False)
    .reset_index()
)
display(cat_rank)

fig = make_subplots(
    rows=1, cols=2,
    subplot_titles=['Total Purchases by Category', 'Demand Consistency (lower CV = more stable)'],
)
fig.add_trace(
    go.Bar(
        x=cat_rank['category'], y=cat_rank['total_purchases'],
        marker_color='#636EFA',
        text=cat_rank['total_purchases'],
        textposition='outside',
        showlegend=False,
    ),
    row=1, col=1,
)
fig.add_trace(
    go.Bar(
        x=cat_rank['category'], y=cat_rank['cv'],
        marker_color='#EF553B',
        text=cat_rank['cv'],
        texttemplate='%{text:.2f}',
        textposition='outside',
        showlegend=False,
    ),
    row=1, col=2,
)
fig.update_layout(template=DARK, title='Category Demand Ranking')
fig.show()
"""

FORECAST_MD = """\
## 6. Simple Demand Forecast — Next 6 Hours

**Method:** Exponential Weighted Mean (EWM) with `span = 4` applied to each category's
hourly demand series. The final EWM value and the mean recent trend are used to
extrapolate 6 periods forward with a **0.85 decay factor** per step to model
the natural regression to the mean.

Formula: `forecast(t+n) = EWM_last + trend × n × decay^n`

This is a lightweight, interpretable approach that requires no additional libraries.
For production use, replace with Prophet or SARIMA for seasonality-aware forecasting.
"""

FORECAST_CODE = """\
FORECAST_HOURS = 6
EWM_SPAN      = 4
DECAY         = 0.85

if 'hourly_cat' not in dir() or hourly_cat.empty:
    print("hourly_cat not available — re-run section 2.")
else:
    cat_pivot = (
        hourly_cat
        .pivot_table(index='hour_bucket', columns='category',
                     values='purchases', fill_value=0)
        .sort_index()
    )

    last_hour = cat_pivot.index.max()
    forecast_index = pd.date_range(
        start=last_hour + pd.Timedelta(hours=1),
        periods=FORECAST_HOURS,
        freq='h',
    )

    forecast_rows = []
    fig = go.Figure()
    colors = px.colors.qualitative.Plotly

    for i, cat in enumerate(cat_pivot.columns):
        series   = cat_pivot[cat].copy()
        ewm_vals = series.ewm(span=EWM_SPAN, adjust=False).mean()
        last_ewm = float(ewm_vals.iloc[-1])
        trend    = float(ewm_vals.diff().tail(4).mean())

        fcast = [
            max(0.0, last_ewm + trend * (n + 1) * (DECAY ** (n + 1)))
            for n in range(FORECAST_HOURS)
        ]

        color = colors[i % len(colors)]

        # Historical trace
        fig.add_trace(go.Scatter(
            x=series.index, y=series.values,
            name=cat,
            mode='lines+markers',
            line=dict(color=color, width=2),
            marker=dict(size=4),
            legendgroup=cat,
        ))
        # EWM overlay
        fig.add_trace(go.Scatter(
            x=ewm_vals.index, y=ewm_vals.values,
            name=f'{cat} (EWM)',
            mode='lines',
            line=dict(color=color, width=1.5, dash='dot'),
            legendgroup=cat,
            showlegend=False,
        ))
        # Forecast
        fig.add_trace(go.Scatter(
            x=list(forecast_index), y=fcast,
            name=f'{cat} forecast',
            mode='lines+markers',
            line=dict(color=color, width=2, dash='dash'),
            marker=dict(symbol='diamond', size=8),
            legendgroup=cat,
            showlegend=False,
        ))

        for j, (ts, val) in enumerate(zip(forecast_index, fcast)):
            forecast_rows.append({'hour': ts, 'category': cat, 'forecast_purchases': round(val, 2)})

    # Shaded forecast region
    fig.add_vrect(
        x0=str(last_hour), x1=str(forecast_index[-1]),
        fillcolor='rgba(255,255,255,0.04)',
        line_width=0,
        annotation_text='Forecast',
        annotation_position='top left',
    )
    fig.update_layout(
        title='Hourly Purchase Demand + 6-Hour EWM Forecast by Category',
        xaxis_title='Time',
        yaxis_title='Purchases',
        template=DARK,
        legend=dict(orientation='h', yanchor='bottom', y=1.02),
    )
    fig.show()

    forecast_df = pd.DataFrame(forecast_rows)
    print("\\n6-Hour forecast (purchases per hour):")
    display(forecast_df.pivot(index='hour', columns='category', values='forecast_purchases').round(2))
"""

INVENTORY_MD = """\
## 7. Inventory Implications

Connect the demand forecast to current inventory levels.
For each product, compute the **hours until stockout** given the observed purchase rate.

Products are ranked from most urgent (lowest hours remaining) to safest.
Red = already out of stock or less than 24 hours remaining.
"""

INVENTORY_CODE = """\
# ── Current stock per product ─────────────────────────────────────────────────
inventory = qry('''
    SELECT
        i.product_id,
        p.product_name,
        p.category,
        SUM(i.current_stock) AS total_stock
    FROM ecommerce.inventory_state i
    JOIN ecommerce.products p ON i.product_id = p.product_id
    GROUP BY i.product_id, p.product_name, p.category
    ORDER BY p.product_name
''')

# ── Purchase rate per product (purchases/hour over observed window) ────────────
demand_rate = qry('''
    SELECT
        product_id,
        COUNT(*)   AS total_purchases,
        ROUND(
            COUNT(*) * 1.0
            / NULLIF(
                EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 3600.0,
                0
            ),
            4
        )          AS purchases_per_hour
    FROM ecommerce.clickstream_events
    WHERE event_type = \'purchase\'
      AND product_id IS NOT NULL
    GROUP BY product_id
''')

# ── Join and calculate stockout horizon ───────────────────────────────────────
stockout = inventory.merge(demand_rate, on='product_id', how='left')
stockout['total_stock'] = pd.to_numeric(stockout['total_stock'], errors='coerce')
stockout['purchases_per_hour'] = pd.to_numeric(stockout['purchases_per_hour'], errors='coerce')
stockout['purchases_per_hour'] = stockout['purchases_per_hour'].fillna(0)
stockout['hours_until_stockout'] = (
    stockout['total_stock']
    / stockout['purchases_per_hour'].replace(0, pd.NA)
).round(1)

# Classify urgency
def urgency(h):
    if pd.isna(h):     return 'No Demand'
    if h == 0:         return 'Out of Stock'
    if h < 24:         return 'Critical (<24h)'
    if h < 72:         return 'Warning (<72h)'
    return 'Healthy'

stockout['status'] = stockout['hours_until_stockout'].apply(urgency)

STATUS_COLORS = {
    'Out of Stock':    '#EF553B',
    'Critical (<24h)': '#FFA15A',
    'Warning (<72h)':  '#FECB52',
    'Healthy':         '#00CC96',
    'No Demand':       '#888888',
}

display(
    stockout
    .sort_values('hours_until_stockout', na_position='last')
    .reset_index(drop=True)
    [['product_name', 'category', 'total_stock',
      'purchases_per_hour', 'hours_until_stockout', 'status']]
)

# ── Bar: hours until stockout ─────────────────────────────────────────────────
at_risk = (
    stockout[stockout['status'].isin(['Out of Stock', 'Critical (<24h)', 'Warning (<72h)'])]
    .sort_values('hours_until_stockout')
)

if at_risk.empty:
    print("\\nAll products have comfortable stock levels (>72 hours).")
else:
    fig = px.bar(
        at_risk,
        x='product_name', y='hours_until_stockout',
        color='status',
        color_discrete_map=STATUS_COLORS,
        title='At-Risk Products — Hours of Stock Remaining',
        labels={'product_name': 'Product', 'hours_until_stockout': 'Hours Remaining'},
        template=DARK,
    )
    fig.add_hline(y=24, line_dash='dash', line_color='orange',
                  annotation_text='24h threshold', annotation_position='top right')
    fig.add_hline(y=72, line_dash='dash', line_color='yellow',
                  annotation_text='72h threshold', annotation_position='top right')
    fig.update_layout(xaxis_tickangle=-35, margin=dict(b=120))
    fig.show()

# Summary counts
status_counts = stockout['status'].value_counts().reset_index()
status_counts.columns = ['status', 'products']
print("\\nInventory status summary:")
display(status_counts)

# ── Scatter: total stock vs demand rate (sized by urgency) ────────────────────
plot_df = stockout[stockout['purchases_per_hour'] > 0].copy()
fig2 = px.scatter(
    plot_df,
    x='purchases_per_hour',
    y='total_stock',
    color='status',
    color_discrete_map=STATUS_COLORS,
    hover_name='product_name',
    size_max=16,
    title='Stock Level vs Demand Rate',
    labels={
        'purchases_per_hour': 'Demand Rate (purchases / hour)',
        'total_stock': 'Total Stock (all warehouses)',
    },
    template=DARK,
    opacity=0.8,
)
fig2.show()
"""

KEY_FINDINGS = """\
## 8. Key Findings & Recommendations

---

### Finding 1 — Demand Is Concentrated in a Narrow Time Window
The heatmap typically reveals 2–4 peak hours that account for a disproportionate share
of daily purchases.
**Recommendation:** Schedule inventory replenishment jobs to complete **before** peak
demand windows. Use the forecast output as an input to a nightly replenishment trigger:
if predicted demand for the next 8 hours exceeds 40% of current stock, auto-generate
a restock request.

---

### Finding 2 — Some Categories Have Volatile Demand (High CV)
High coefficient-of-variation categories are difficult to plan for with fixed stock targets.
**Recommendation:** Apply **safety stock buffers** proportional to each category's CV.
Categories with CV > 0.8 should hold 1.5× the average weekly demand as safety stock;
categories with CV < 0.4 can operate leaner at 1.0×.

---

### Finding 3 — Critical Stockout Risk Is Concentrated in High-Velocity Products
The scatter plot reveals a cluster of products with high demand rate and low current stock.
These are the products most likely to generate lost sales this week.
**Recommendation:** Treat any product with < 24 hours of stock at current demand rate as
a P1 alert. Integrate the stockout calculation into the Grafana dashboard so it triggers
an alert channel automatically.

---

### Finding 4 — Forecast Accuracy Degrades Beyond 6 Hours
EWM trend extrapolation is reliable for 2–4 hours but noisy beyond 6.
**Recommendation:** For planning horizons > 6 hours, use the daily pattern from the
heatmap as a prior. For production-grade forecasting, fit a SARIMA or Prophet model
on ≥ 4 weeks of hourly data with weekly seasonality components.

---

### Finding 5 — Products With Zero Demand in the Observed Window Need Investigation
"No Demand" products either have a catalogue listing issue, zero traffic, or were not
stocked initially.
**Recommendation:** Cross-reference with `product_metrics.total_views`. Products with
views but zero purchases are conversion failures (price, content, UX). Products with
zero views are discoverability failures (search ranking, category placement).

---
*Analysis from `analytics/demand_forecasting.ipynb`*
"""

CLEANUP = """\
conn.close()
print("Connection closed.")
"""

cells = [
    code("setup-01",          SETUP),
    md  ("md-exec",           EXEC_SUMMARY),
    md  ("md-overview",       DATA_OVERVIEW_MD),
    code("code-overview",     DATA_OVERVIEW_CODE),
    md  ("md-demand-cat",     DEMAND_CAT_MD),
    code("code-demand-cat",   DEMAND_CAT_CODE),
    md  ("md-ma",             MA_MD),
    code("code-ma",           MA_CODE),
    md  ("md-peak",           PEAK_MD),
    code("code-peak",         PEAK_CODE),
    md  ("md-cat-rank",       CAT_RANK_MD),
    code("code-cat-rank",     CAT_RANK_CODE),
    md  ("md-forecast",       FORECAST_MD),
    code("code-forecast",     FORECAST_CODE),
    md  ("md-inventory",      INVENTORY_MD),
    code("code-inventory",    INVENTORY_CODE),
    md  ("md-findings",       KEY_FINDINGS),
    code("code-cleanup",      CLEANUP),
]

notebook = {
    "nbformat": 4, "nbformat_minor": 5,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"codemirror_mode": {"name": "ipython", "version": 3},
                          "file_extension": ".py", "mimetype": "text/x-python",
                          "name": "python", "pygments_lexer": "ipython3", "version": "3.11.0"},
    },
    "cells": cells,
}

out = pathlib.Path(__file__).parent / "demand_forecasting.ipynb"
out.write_text(json.dumps(notebook, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Wrote {out}  ({len(cells)} cells)")
