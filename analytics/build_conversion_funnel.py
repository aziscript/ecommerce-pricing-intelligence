#!/usr/bin/env python3
"""
build_conversion_funnel.py

Generates analytics/conversion_funnel.ipynb from scratch.
Run from any directory:
    python analytics/build_conversion_funnel.py
"""

import json
import pathlib

# ── Cell helpers ──────────────────────────────────────────────────────────────

def md(cell_id: str, source: str) -> dict:
    return {
        "cell_type": "markdown",
        "id": cell_id,
        "metadata": {},
        "source": source,
    }


def code(cell_id: str, source: str) -> dict:
    return {
        "cell_type": "code",
        "id": cell_id,
        "metadata": {},
        "execution_count": None,
        "outputs": [],
        "source": source,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Cell sources
# ═══════════════════════════════════════════════════════════════════════════════

SETUP = """\
import warnings
warnings.filterwarnings('ignore')

import psycopg
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from IPython.display import display

# ── Database connection ───────────────────────────────────────────────────────
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

# ── 0. Executive Summary ──────────────────────────────────────────────────────

EXEC_SUMMARY = """\
# Conversion Funnel Analysis
## Executive Summary

This notebook measures how effectively the platform moves users from their first page view through
to a completed purchase, and surfaces actionable insights about where that journey breaks down.

**What this analysis covers**

| Section | Question answered |
|---|---|
| Data Overview | How much data do we have and how is it distributed? |
| Funnel Analysis | Where do we lose users — and by how much? |
| Funnel by Device | Does mobile, desktop, or tablet convert best? |
| Funnel by Category | Which product categories close best? |
| Hourly Patterns | When are users most active and when do purchases peak? |
| Session Analysis | What separates a converting session from a non-converting one? |
| Key Findings | Actionable recommendations for the business |

**Data source:** `ecommerce.clickstream_events` joined with `ecommerce.products`

**Funnel stages tracked:** `page_view → product_view → add_to_cart → purchase`
"""

# ── 1. Data Overview ──────────────────────────────────────────────────────────

DATA_OVERVIEW_MD = """\
## 1. Data Overview

Baseline statistics and event-type distribution across the entire clickstream dataset.
"""

DATA_OVERVIEW_CODE = """\
# ── Dataset summary ───────────────────────────────────────────────────────────
overview = qry('''
    SELECT
        COUNT(*)                             AS total_events,
        COUNT(DISTINCT user_id)              AS unique_users,
        COUNT(DISTINCT session_id)           AS unique_sessions,
        MIN(timestamp)::DATE                 AS earliest_date,
        MAX(timestamp)::DATE                 AS latest_date,
        (MAX(timestamp) - MIN(timestamp))    AS data_span
    FROM ecommerce.clickstream_events
''')
display(overview)

# ── Event-type distribution ───────────────────────────────────────────────────
event_dist = qry('''
    SELECT
        event_type,
        COUNT(*)                                                    AS event_count,
        COUNT(DISTINCT session_id)                                  AS sessions,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)         AS pct_of_events
    FROM ecommerce.clickstream_events
    GROUP BY event_type
    ORDER BY event_count DESC
''')
display(event_dist)

fig = px.bar(
    event_dist,
    x='event_type', y='event_count',
    text='pct_of_events',
    color='event_type',
    title='Event Type Distribution',
    labels={'event_type': 'Event Type', 'event_count': 'Total Events'},
    template=DARK,
)
fig.update_traces(texttemplate='%{text}%', textposition='outside')
fig.update_layout(showlegend=False, uniformtext_minsize=10, uniformtext_mode='hide')
fig.show()
"""

# ── 2. Funnel Analysis ────────────────────────────────────────────────────────

FUNNEL_MD = """\
## 2. Funnel Analysis

Counts **unique sessions** that reached each stage.

- **Step CVR** — conversion rate from the immediately preceding stage
- **Overall CVR** — conversion rate from the top of the funnel (page view)
"""

FUNNEL_CODE = """\
# ── Unique sessions per funnel stage ─────────────────────────────────────────
funnel_raw = qry('''
    SELECT
        COUNT(DISTINCT CASE WHEN event_type = 'page_view'    THEN session_id END) AS page_view,
        COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN session_id END) AS product_view,
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart'  THEN session_id END) AS add_to_cart,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase'     THEN session_id END) AS purchase
    FROM ecommerce.clickstream_events
''')

STAGES  = ['page_view', 'product_view', 'add_to_cart', 'purchase']
LABELS  = ['Page View', 'Product View', 'Add to Cart', 'Purchase']
values  = [int(funnel_raw[s].iloc[0]) for s in STAGES]
top     = values[0] or 1

funnel_df = pd.DataFrame({
    'stage':           LABELS,
    'sessions':        values,
    'overall_cvr_pct': [round(v / top * 100, 1) for v in values],
    'step_cvr_pct': (
        [100.0] + [round(values[i] / (values[i - 1] or 1) * 100, 1)
                   for i in range(1, len(values))]
    ),
})
print("Funnel Summary")
display(funnel_df)

# ── Funnel chart ──────────────────────────────────────────────────────────────
fig = go.Figure(go.Funnel(
    y=LABELS,
    x=values,
    textinfo='value+percent initial+percent previous',
    marker=dict(color=['#636EFA', '#EF553B', '#FFA15A', '#00CC96']),
    connector=dict(line=dict(color='#555', width=1)),
))
fig.update_layout(
    title='Conversion Funnel — Unique Sessions',
    template=DARK,
    margin=dict(l=140),
)
fig.show()

# ── Summary stats ─────────────────────────────────────────────────────────────
pv_to_cart  = round(values[2] / (values[1] or 1) * 100, 1)
cart_to_buy = round(values[3] / (values[2] or 1) * 100, 1)
overall     = round(values[3] / (values[0] or 1) * 100, 2)

print(f"\\nEnd-to-end conversion rate (page view -> purchase) : {overall}%")
print(f"Product view  -> add to cart                       : {pv_to_cart}%")
print(f"Add to cart   -> purchase                          : {cart_to_buy}%")
"""

# ── 3. Funnel by Device Type ──────────────────────────────────────────────────

DEVICE_MD = """\
## 3. Funnel by Device Type

Breaks down the funnel by `device_type` (mobile / desktop / tablet).
All conversion rates are expressed relative to the page-view session count for that device.
"""

DEVICE_CODE = """\
device_funnel = qry('''
    SELECT
        device_type,
        COUNT(DISTINCT CASE WHEN event_type = 'page_view'    THEN session_id END) AS page_view,
        COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN session_id END) AS product_view,
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart'  THEN session_id END) AS add_to_cart,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase'     THEN session_id END) AS purchase
    FROM ecommerce.clickstream_events
    GROUP BY device_type
    ORDER BY device_type
''')

base = device_funnel['page_view'].replace(0, pd.NA)
device_funnel['product_view_cvr'] = (device_funnel['product_view'] / base * 100).round(1)
device_funnel['add_to_cart_cvr']  = (device_funnel['add_to_cart']  / base * 100).round(1)
device_funnel['purchase_cvr']     = (device_funnel['purchase']      / base * 100).round(1)
display(device_funnel)

# ── Grouped bar: conversion rates by device ───────────────────────────────────
cvr_cols   = ['product_view_cvr', 'add_to_cart_cvr', 'purchase_cvr']
cvr_labels = ['Product View Rate', 'Add to Cart Rate', 'Purchase Rate']

fig = go.Figure()
for col, label in zip(cvr_cols, cvr_labels):
    fig.add_trace(go.Bar(
        name=label,
        x=device_funnel['device_type'],
        y=device_funnel[col],
        text=device_funnel[col],
        texttemplate='%{text}%',
        textposition='outside',
    ))

fig.update_layout(
    title='Conversion Rates by Device Type (% of page-view sessions)',
    xaxis_title='Device Type',
    yaxis_title='Conversion Rate (%)',
    barmode='group',
    template=DARK,
    legend=dict(orientation='h', yanchor='bottom', y=1.02),
)
fig.show()

best_idx    = device_funnel['purchase_cvr'].idxmax()
best_device = device_funnel.loc[best_idx, 'device_type']
best_cvr    = device_funnel.loc[best_idx, 'purchase_cvr']
print(f"Best-converting device: {best_device}  ({best_cvr}% purchase CVR)")
"""

# ── 4. Funnel by Product Category ────────────────────────────────────────────

CATEGORY_MD = """\
## 4. Funnel by Product Category

Compares funnel performance across **phones, laptops, headphones, tablets,** and **accessories**.
Only events linked to a `product_id` are included so the category can be resolved from the products table.
"""

CATEGORY_CODE = """\
cat_funnel = qry('''
    SELECT
        p.category,
        COUNT(DISTINCT CASE WHEN ce.event_type = 'product_view' THEN ce.session_id END) AS product_view,
        COUNT(DISTINCT CASE WHEN ce.event_type = 'add_to_cart'  THEN ce.session_id END) AS add_to_cart,
        COUNT(DISTINCT CASE WHEN ce.event_type = 'purchase'     THEN ce.session_id END) AS purchase
    FROM ecommerce.clickstream_events ce
    JOIN ecommerce.products p ON ce.product_id = p.product_id
    WHERE ce.product_id IS NOT NULL
    GROUP BY p.category
    ORDER BY purchase DESC
''')

cat_funnel['purchase_cvr'] = (
    cat_funnel['purchase'] / cat_funnel['product_view'].replace(0, pd.NA) * 100
).round(1)
display(cat_funnel)

# ── Stacked bar: absolute session volumes by stage and category ───────────────
cat_melt = cat_funnel[['category', 'product_view', 'add_to_cart', 'purchase']].melt(
    id_vars='category', var_name='stage', value_name='sessions'
)
cat_melt['stage'] = cat_melt['stage'].str.replace('_', ' ').str.title()

fig = px.bar(
    cat_melt,
    x='category', y='sessions',
    color='stage',
    barmode='stack',
    title='Funnel Stage Volumes by Product Category',
    labels={'category': 'Category', 'sessions': 'Unique Sessions', 'stage': 'Stage'},
    color_discrete_sequence=['#636EFA', '#FFA15A', '#00CC96'],
    template=DARK,
)
fig.update_layout(legend=dict(orientation='h', yanchor='bottom', y=1.02))
fig.show()

# ── Bar: purchase conversion rate by category ─────────────────────────────────
fig2 = px.bar(
    cat_funnel.sort_values('purchase_cvr', ascending=False),
    x='category', y='purchase_cvr',
    text='purchase_cvr',
    title='Purchase Conversion Rate by Category  (product view → purchase)',
    labels={'category': 'Category', 'purchase_cvr': 'Purchase CVR (%)'},
    color='purchase_cvr',
    color_continuous_scale='Teal',
    template=DARK,
)
fig2.update_traces(texttemplate='%{text}%', textposition='outside')
fig2.update_layout(coloraxis_showscale=False)
fig2.show()
"""

# ── 5. Hourly Activity Patterns ───────────────────────────────────────────────

HOURLY_MD = """\
## 5. Hourly Activity Patterns

Examines when users are most active throughout the day and identifies peak purchase windows.
Knowing *when* purchases concentrate lets us time promotions, dynamic pricing, and notifications for maximum impact.
"""

HOURLY_CODE = """\
hourly = qry('''
    SELECT
        EXTRACT(HOUR FROM timestamp)::INT  AS hour,
        event_type,
        COUNT(*)                           AS event_count
    FROM ecommerce.clickstream_events
    GROUP BY EXTRACT(HOUR FROM timestamp), event_type
    ORDER BY hour, event_type
''')

# ── Line chart: all event types ───────────────────────────────────────────────
fig = px.line(
    hourly,
    x='hour', y='event_count',
    color='event_type',
    markers=True,
    title='Event Volume by Hour of Day',
    labels={'hour': 'Hour (0–23)', 'event_count': 'Event Count', 'event_type': 'Event Type'},
    template=DARK,
)
fig.update_layout(
    xaxis=dict(tickmode='linear', tick0=0, dtick=1),
    legend=dict(orientation='h', yanchor='bottom', y=1.02),
)
fig.show()

# ── Purchase-only heatmap bar ─────────────────────────────────────────────────
purchases_by_hour = (
    hourly[hourly['event_type'] == 'purchase']
    .copy()
    .reset_index(drop=True)
)

fig2 = px.bar(
    purchases_by_hour,
    x='hour', y='event_count',
    text='event_count',
    title='Purchase Volume by Hour of Day',
    labels={'hour': 'Hour (0–23)', 'event_count': 'Purchases'},
    color='event_count',
    color_continuous_scale='Reds',
    template=DARK,
)
fig2.update_traces(textposition='outside')
fig2.update_layout(
    xaxis=dict(tickmode='linear', tick0=0, dtick=1),
    coloraxis_showscale=False,
)
fig2.show()

# ── Peak purchase hours ───────────────────────────────────────────────────────
if not purchases_by_hour.empty:
    peak = purchases_by_hour.nlargest(3, 'event_count')[['hour', 'event_count']]
    print("Top 3 purchase hours:")
    for _, r in peak.iterrows():
        print(f"  {int(r['hour']):02d}:00  —  {int(r['event_count'])} purchases")
else:
    print("No purchase events recorded yet.")
"""

# ── 6. Session Analysis ───────────────────────────────────────────────────────

SESSION_MD = """\
## 6. Session Analysis

A **session** (`session_id`) groups all events from a single user visit.
This section compares sessions that ended in a purchase against those that did not, to reveal
what a high-intent visit looks like — and how to identify one in real time.
"""

SESSION_CODE = """\
sessions = qry('''
    SELECT
        session_id,
        device_type,
        COUNT(*)                                                          AS event_count,
        COUNT(DISTINCT event_type)                                        AS distinct_event_types,
        MIN(timestamp)                                                    AS session_start,
        MAX(timestamp)                                                    AS session_end,
        ROUND(
            EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 60.0,
            2
        )                                                                 AS duration_min,
        BOOL_OR(event_type = \'purchase\')                               AS converted
    FROM ecommerce.clickstream_events
    GROUP BY session_id, device_type
''')

total_sessions  = len(sessions)
converted_count = int(sessions['converted'].sum())
conv_rate       = converted_count / total_sessions * 100 if total_sessions else 0

print(f"Total sessions       : {total_sessions:,}")
print(f"Converted sessions   : {converted_count:,}  ({conv_rate:.2f}%)")
print(f"Avg events / session : {sessions['event_count'].mean():.1f}")
print(f"Avg duration         : {sessions['duration_min'].mean():.1f} min")

# ── Converted vs non-converted comparison table ───────────────────────────────
summary = (
    sessions
    .groupby('converted')
    .agg(
        sessions            = ('session_id',           'count'),
        avg_events          = ('event_count',          'mean'),
        avg_duration_min    = ('duration_min',          'mean'),
        avg_distinct_types  = ('distinct_event_types', 'mean'),
    )
    .round(2)
    .rename(index={False: 'Not Converted', True: 'Converted'})
)
print()
display(summary)

# ── Outcome split + avg events subplots ──────────────────────────────────────
conv_counts = (
    sessions.groupby('converted').size()
    .reset_index(name='count')
    .sort_values('converted')           # False first, True second
)
conv_counts['label'] = conv_counts['converted'].map(
    {True: 'Converted', False: 'Not Converted'}
)

avg_events = (
    sessions.groupby('converted')['event_count'].mean()
    .reset_index()
    .sort_values('converted')
)
avg_events['label'] = avg_events['converted'].map(
    {True: 'Converted', False: 'Not Converted'}
)

fig = make_subplots(
    rows=1, cols=2,
    specs=[[{'type': 'pie'}, {'type': 'bar'}]],
    subplot_titles=['Session Outcome Split', 'Avg Events per Session'],
)
fig.add_trace(
    go.Pie(
        labels=conv_counts['label'],
        values=conv_counts['count'],
        marker=dict(colors=['#EF553B', '#00CC96']),
        hole=0.45,
        showlegend=True,
    ),
    row=1, col=1,
)
fig.add_trace(
    go.Bar(
        x=avg_events['label'],
        y=avg_events['event_count'].round(1),
        text=avg_events['event_count'].round(1),
        textposition='outside',
        marker_color=['#EF553B', '#00CC96'],
        showlegend=False,
    ),
    row=1, col=2,
)
fig.update_layout(
    title='Session Analysis: Converted vs Non-Converted',
    template=DARK,
)
fig.show()

# ── Distribution of events per session ───────────────────────────────────────
sessions['outcome'] = sessions['converted'].map(
    {True: 'Converted', False: 'Not Converted'}
)
fig2 = px.histogram(
    sessions,
    x='event_count',
    color='outcome',
    nbins=25,
    barmode='overlay',
    opacity=0.72,
    title='Distribution of Events per Session',
    labels={'event_count': 'Events per Session', 'outcome': 'Outcome'},
    color_discrete_map={'Converted': '#00CC96', 'Not Converted': '#EF553B'},
    template=DARK,
)
fig2.update_layout(legend=dict(title=''))
fig2.show()

# ── Conversion rate by device (session-level) ─────────────────────────────────
device_conv = (
    sessions.groupby('device_type')
    .agg(total=('session_id', 'count'), converted=('converted', 'sum'))
    .assign(cvr=lambda d: (d['converted'] / d['total'] * 100).round(1))
    .reset_index()
    .sort_values('cvr', ascending=False)
)
fig3 = px.bar(
    device_conv,
    x='device_type', y='cvr',
    text='cvr',
    title='Session-Level Purchase Conversion Rate by Device',
    labels={'device_type': 'Device', 'cvr': 'Purchase CVR (%)'},
    color='cvr',
    color_continuous_scale='Teal',
    template=DARK,
)
fig3.update_traces(texttemplate='%{text}%', textposition='outside')
fig3.update_layout(coloraxis_showscale=False)
fig3.show()
"""

# ── 7. Key Findings & Recommendations ─────────────────────────────────────────

KEY_FINDINGS = """\
## 7. Key Findings & Recommendations

---

### Finding 1 — The Biggest Drop-off Is Between Product View and Add to Cart

The steepest funnel attrition almost always occurs at this step.
Users saw the product but didn't commit — typically due to price uncertainty, lack of reviews,
or a friction-heavy page.

**Recommendation:** A/B test enriched product pages for the top 10 products by traffic:
stronger social proof (review counts + star ratings), a visible price-match badge when our price
is within 5% of competitor average, and an above-the-fold CTA.
Target: lift add-to-cart rate by ≥ 5 pp.

---

### Finding 2 — Mobile Converts at a Lower Rate Than Desktop

If the device analysis confirms the typical mobile gap, the mobile checkout flow is a high-ROI target.

**Recommendation:** Reduce mobile checkout to ≤ 3 taps.
Implement Apple Pay / Google Pay, autofill on all form fields, and a sticky "Buy Now" button that follows the user down the product page.

---

### Finding 3 — Accessories Convert Best; High-Ticket Categories Need Nurturing

Accessories (low price point, impulse buy) close fast.
Phones and laptops require a longer consideration cycle — users will leave and return.

**Recommendation:** For laptops and phones, deploy a price-drop email trigger:
if a user views a product and the price drops within 72 hours, send a personalised alert.
This converts consideration-phase visitors who left before purchasing.

---

### Finding 4 — Purchase Volume Concentrates in Predictable Windows

Hourly patterns reveal clear peaks. Capitalise on them with time-sensitive tactics:

- Schedule flash discounts and push notifications **30 minutes before** a purchase peak.
- Use the pricing intelligence engine to **widen discounts during off-peak hours**
  to smooth demand and reduce server load during spikes.
- Suppress broad marketing emails during the lowest-activity hours to protect sender reputation.

---

### Finding 5 — Converted Sessions Have Significantly More Events

High event-count sessions are strong purchase signals.
A session that reaches ≥ N events (tune N from the distribution above) is statistically
far more likely to convert.

**Recommendation:** Instrument a real-time engagement score in the stream processor.
When a session crosses the threshold, trigger a personalised nudge:
an exit-intent overlay with a 5% single-use discount, or a live-chat prompt.
This converts high-intent visitors before they abandon.

---

*Analysis produced by `analytics/conversion_funnel.ipynb`
Data source: `ecommerce.clickstream_events` — refresh by re-running all cells.*
"""

CLEANUP = """\
# Close the database connection when done
conn.close()
print("Connection closed.")
"""

# ═══════════════════════════════════════════════════════════════════════════════
# Assemble the notebook
# ═══════════════════════════════════════════════════════════════════════════════

cells = [
    code("setup-01",          SETUP),
    md  ("md-exec-summary",   EXEC_SUMMARY),
    md  ("md-data-overview",  DATA_OVERVIEW_MD),
    code("code-overview-01",  DATA_OVERVIEW_CODE),
    md  ("md-funnel",         FUNNEL_MD),
    code("code-funnel-01",    FUNNEL_CODE),
    md  ("md-device",         DEVICE_MD),
    code("code-device-01",    DEVICE_CODE),
    md  ("md-category",       CATEGORY_MD),
    code("code-category-01",  CATEGORY_CODE),
    md  ("md-hourly",         HOURLY_MD),
    code("code-hourly-01",    HOURLY_CODE),
    md  ("md-session",        SESSION_MD),
    code("code-session-01",   SESSION_CODE),
    md  ("md-findings",       KEY_FINDINGS),
    code("code-cleanup",      CLEANUP),
]

notebook = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "codemirror_mode": {"name": "ipython", "version": 3},
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "pygments_lexer": "ipython3",
            "version": "3.11.0",
        },
    },
    "cells": cells,
}

out = pathlib.Path(__file__).parent / "conversion_funnel.ipynb"
out.write_text(json.dumps(notebook, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Wrote {out}  ({len(cells)} cells)")
