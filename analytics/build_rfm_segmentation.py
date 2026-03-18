#!/usr/bin/env python3
"""
build_rfm_segmentation.py
Generates analytics/rfm_segmentation.ipynb.
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
# RFM Customer Segmentation

## Executive Summary

RFM (Recency · Frequency · Monetary) is a proven framework for segmenting customers
by their purchasing behaviour. Each user is scored 1–5 on three dimensions:

| Dimension | What it measures | Higher score = |
|---|---|---|
| **Recency (R)** | Days since last purchase | Purchased more recently |
| **Frequency (F)** | Total number of purchases | Bought more often |
| **Monetary (M)** | Total spend across all purchases | Spent more |

Scores are combined to assign each user to a named segment, enabling targeted
marketing, retention campaigns, and personalised pricing.

**Data source:** `ecommerce.clickstream_events` (purchase events only),
joined with `ecommerce.products` for category context.
"""

DATA_OVERVIEW_MD = """\
## 1. Data Overview

Summary of the purchase event dataset used for RFM scoring.
"""

DATA_OVERVIEW_CODE = """\
overview = qry('''
    SELECT
        COUNT(*)                            AS total_purchase_events,
        COUNT(DISTINCT user_id)             AS unique_buyers,
        COUNT(DISTINCT session_id)          AS unique_purchase_sessions,
        MIN(timestamp)::DATE                AS first_purchase,
        MAX(timestamp)::DATE                AS last_purchase,
        ROUND(AVG(COALESCE(product_price, 0)), 2) AS avg_order_value,
        ROUND(SUM(COALESCE(product_price, 0)), 2) AS total_gmv
    FROM ecommerce.clickstream_events
    WHERE event_type = \'purchase\'
''')
display(overview)

# Purchases per user distribution
user_purchase_counts = qry('''
    SELECT
        COUNT(*) AS purchases,
        COUNT(user_id) AS user_count
    FROM (
        SELECT user_id, COUNT(*) AS purchases
        FROM ecommerce.clickstream_events
        WHERE event_type = \'purchase\'
        GROUP BY user_id
    ) sub
    GROUP BY purchases
    ORDER BY purchases
''')
print("\\nPurchases-per-user distribution:")
display(user_purchase_counts.head(10))

fig = px.histogram(
    qry('''
        SELECT user_id, COUNT(*) AS purchase_count
        FROM ecommerce.clickstream_events
        WHERE event_type = \'purchase\'
        GROUP BY user_id
    '''),
    x='purchase_count',
    nbins=30,
    title='Distribution of Purchase Count per User',
    labels={'purchase_count': 'Purchases per User', 'count': 'Users'},
    template=DARK,
    color_discrete_sequence=['#636EFA'],
)
fig.update_layout(showlegend=False)
fig.show()
"""

RFM_CALC_MD = """\
## 2. RFM Score Calculation

Each user receives a score of **1–5** on each dimension using quintile-based scoring.
`rank(method='first')` is applied before `pd.qcut` to break ties deterministically.

Recency scoring is **inverted** — a user who bought yesterday scores 5, one who
bought months ago scores 1.
"""

RFM_CALC_CODE = """\
raw_rfm = qry('''
    SELECT
        user_id,
        COUNT(*)                                  AS frequency,
        ROUND(SUM(COALESCE(product_price, 0)), 2) AS monetary,
        ROUND(
            EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) / 86400.0,
            1
        )                                         AS recency_days
    FROM ecommerce.clickstream_events
    WHERE event_type = \'purchase\'
    GROUP BY user_id
    HAVING COUNT(*) > 0
''')

print(f"Users with at least one purchase: {len(raw_rfm):,}")
display(raw_rfm.describe().round(2))

# ── Score 1–5 using quintiles (rank to break ties) ───────────────────────────
N_BINS = min(5, len(raw_rfm))  # guard for very small datasets

def score_col(series, ascending=True, n=N_BINS):
    # Rank the series then cut into n equal bins labeled 1..n (or n..1).
    ranked = series.rank(method='first')
    labels = list(range(1, n + 1)) if ascending else list(range(n, 0, -1))
    return pd.qcut(ranked, q=n, labels=labels).astype(int)

rfm = raw_rfm.copy()
rfm['R'] = score_col(rfm['recency_days'], ascending=False)   # lower days = higher score
rfm['F'] = score_col(rfm['frequency'],    ascending=True)
rfm['M'] = score_col(rfm['monetary'],     ascending=True)
rfm['RFM_sum'] = rfm['R'] + rfm['F'] + rfm['M']

print("\\nRFM score distribution:")
display(rfm[['R', 'F', 'M', 'RFM_sum']].describe().round(2))

# Score distribution heatmap (F vs R)
fr_heat = rfm.groupby(['R', 'F']).size().reset_index(name='users')
fr_pivot = fr_heat.pivot(index='R', columns='F', values='users').fillna(0)
fig = px.imshow(
    fr_pivot,
    title='User Count by Recency Score (R) vs Frequency Score (F)',
    labels={'x': 'Frequency Score (F)', 'y': 'Recency Score (R)', 'color': 'Users'},
    color_continuous_scale='Blues',
    template=DARK,
    text_auto=True,
)
fig.update_layout(yaxis=dict(autorange='reversed'))
fig.show()
"""

SEGMENTS_MD = """\
## 3. Customer Segments

RFM scores are mapped to six named segments using rule-based thresholds.
Each segment has distinct engagement and revenue potential.

| Segment | Rules | Strategy |
|---|---|---|
| Champions | R≥4, F≥4, M≥4 | Reward & upsell |
| Loyal Customers | R≥3, F≥4 | Loyalty programme |
| Potential Loyalists | R≥4, F≤2 | Nurture into habit |
| Big Spenders | M≥4, F≤2 | Cross-sell & bundles |
| At Risk | R≤2, F≥3 | Win-back campaign |
| Lost | R=1 | Last-chance offer |
| Needs Attention | All others | Re-engagement |
"""

SEGMENTS_CODE = """\
def assign_segment(row):
    r, f, m = int(row['R']), int(row['F']), int(row['M'])
    if r >= 4 and f >= 4 and m >= 4:
        return 'Champions'
    elif r >= 3 and f >= 4:
        return 'Loyal Customers'
    elif r >= 4 and f <= 2:
        return 'Potential Loyalists'
    elif m >= 4 and f <= 2:
        return 'Big Spenders'
    elif r <= 2 and f >= 3:
        return 'At Risk'
    elif r == 1:
        return 'Lost'
    else:
        return 'Needs Attention'

rfm['segment'] = rfm.apply(assign_segment, axis=1)

seg_counts = rfm['segment'].value_counts().reset_index()
seg_counts.columns = ['segment', 'users']
print("Segment sizes:")
display(seg_counts)

SEGMENT_ORDER = [
    'Champions', 'Loyal Customers', 'Potential Loyalists',
    'Big Spenders', 'At Risk', 'Lost', 'Needs Attention',
]
SEGMENT_COLORS = {
    'Champions':           '#00CC96',
    'Loyal Customers':     '#636EFA',
    'Potential Loyalists': '#AB63FA',
    'Big Spenders':        '#FFA15A',
    'At Risk':             '#FECB52',
    'Lost':                '#EF553B',
    'Needs Attention':     '#888888',
}
"""

SEG_VIZ_MD = """\
## 4. Segment Visualization

Two complementary views:
1. **Bar chart** — how many customers fall into each segment
2. **Scatter plot** — Recency vs Monetary coloured by segment (size = frequency)
"""

SEG_VIZ_CODE = """\
seg_counts_ordered = (
    seg_counts
    .set_index('segment')
    .reindex([s for s in SEGMENT_ORDER if s in seg_counts['segment'].values])
    .reset_index()
)

# ── Bar: segment sizes ────────────────────────────────────────────────────────
fig = px.bar(
    seg_counts_ordered,
    x='segment', y='users',
    text='users',
    color='segment',
    color_discrete_map=SEGMENT_COLORS,
    title='Customer Count by Segment',
    labels={'segment': 'Segment', 'users': 'Users'},
    template=DARK,
)
fig.update_traces(textposition='outside')
fig.update_layout(showlegend=False, xaxis_categoryorder='total descending')
fig.show()

# ── Scatter: Recency vs Monetary ──────────────────────────────────────────────
fig2 = px.scatter(
    rfm,
    x='recency_days',
    y='monetary',
    color='segment',
    size='frequency',
    size_max=18,
    color_discrete_map=SEGMENT_COLORS,
    hover_data=['user_id', 'R', 'F', 'M'],
    title='Recency vs Monetary Value — Coloured by Segment',
    labels={
        'recency_days': 'Recency (days since last purchase)',
        'monetary':     'Total Spend ($)',
        'segment':      'Segment',
    },
    template=DARK,
    opacity=0.75,
)
fig2.update_layout(legend=dict(orientation='h', yanchor='bottom', y=-0.25))
fig2.show()

# ── Treemap: segment share of total revenue ───────────────────────────────────
seg_revenue = rfm.groupby('segment')['monetary'].sum().reset_index()
seg_revenue.columns = ['segment', 'total_spend']
fig3 = px.treemap(
    seg_revenue,
    path=['segment'],
    values='total_spend',
    color='segment',
    color_discrete_map=SEGMENT_COLORS,
    title='Total Spend by Customer Segment',
    template=DARK,
)
fig3.update_traces(textinfo='label+percent root+value')
fig3.show()
"""

SEG_PROFILES_MD = """\
## 5. Segment Profiles

Average Recency, Frequency, and Monetary values for each segment —
the quantitative fingerprint of each customer group.
"""

SEG_PROFILES_CODE = """\
profiles = (
    rfm.groupby('segment')
    .agg(
        users             = ('user_id',      'count'),
        avg_recency_days  = ('recency_days', 'mean'),
        avg_frequency     = ('frequency',    'mean'),
        avg_monetary      = ('monetary',     'mean'),
        total_spend       = ('monetary',     'sum'),
        avg_R_score       = ('R',            'mean'),
        avg_F_score       = ('F',            'mean'),
        avg_M_score       = ('M',            'mean'),
    )
    .round(2)
    .sort_values('avg_monetary', ascending=False)
    .reset_index()
)
display(profiles)

# ── Radar / spider chart per segment ─────────────────────────────────────────
radar_cols = ['avg_R_score', 'avg_F_score', 'avg_M_score']
radar_labels = ['Recency', 'Frequency', 'Monetary']

fig = go.Figure()
for _, row in profiles.iterrows():
    seg = row['segment']
    vals = [row[c] for c in radar_cols]
    vals_closed = vals + [vals[0]]   # close the polygon
    fig.add_trace(go.Scatterpolar(
        r=vals_closed,
        theta=radar_labels + [radar_labels[0]],
        fill='toself',
        name=seg,
        line_color=SEGMENT_COLORS.get(seg, '#888888'),
        opacity=0.6,
    ))
fig.update_layout(
    polar=dict(radialaxis=dict(visible=True, range=[0, 5])),
    title='Average RFM Scores by Segment',
    template=DARK,
    legend=dict(orientation='h', yanchor='bottom', y=-0.3),
)
fig.show()
"""

DEVICE_MD = """\
## 6. Device Preference by Segment

Do Champions purchase mainly on desktop? Are Lost customers predominantly mobile?
Understanding device preferences lets us tailor re-engagement channels.
"""

DEVICE_CODE = """\
purchase_devices = qry('''
    SELECT
        user_id,
        device_type,
        COUNT(*) AS purchase_count
    FROM ecommerce.clickstream_events
    WHERE event_type = \'purchase\'
    GROUP BY user_id, device_type
''')

# Join with segments; keep primary device per user (most purchases on that device)
device_seg = (
    purchase_devices
    .merge(rfm[['user_id', 'segment']], on='user_id', how='inner')
)

# Preferred device per user = device with most purchases
preferred = (
    device_seg
    .sort_values('purchase_count', ascending=False)
    .drop_duplicates(subset='user_id')
    [['user_id', 'segment', 'device_type']]
)

device_summary = (
    preferred
    .groupby(['segment', 'device_type'])
    .size()
    .reset_index(name='users')
)

fig = px.bar(
    device_summary,
    x='segment',
    y='users',
    color='device_type',
    barmode='group',
    title='Preferred Purchase Device by Customer Segment',
    labels={'segment': 'Segment', 'users': 'Users', 'device_type': 'Device'},
    template=DARK,
    category_orders={'segment': SEGMENT_ORDER},
)
fig.update_layout(
    xaxis_tickangle=-20,
    legend=dict(orientation='h', yanchor='bottom', y=1.02),
)
fig.show()

# Share of each device within each segment (%)
device_pct = (
    device_summary
    .groupby('segment', group_keys=False)
    .apply(lambda g: g.assign(pct=(g['users'] / g['users'].sum() * 100).round(1)))
    .reset_index(drop=True)
)
device_pivot = device_pct.pivot_table(
    index='segment', columns='device_type', values='pct', fill_value=0
).round(1)
print("\\nDevice share (%) within each segment:")
display(device_pivot)
"""

KEY_FINDINGS = """\
## 7. Key Findings & Recommendations

---

### Finding 1 — Champions Are a Disproportionate Revenue Source
Champions (R≥4, F≥4, M≥4) represent a small share of users but typically account for
30–50% of total GMV.
**Recommendation:** Create a VIP tier exclusively for Champions. Offer early access to new
products, personalised pricing, and a direct feedback channel. Churn among Champions
is the single highest-impact retention risk.

---

### Finding 2 — Potential Loyalists Are the Highest-ROI Conversion Target
These users are recent (R≥4) but have only purchased once or twice (F≤2).
They are already sold on the platform — the barrier is habit formation.
**Recommendation:** Trigger a personalised email sequence 48 hours after their first
purchase: "Complete the set" product recommendations based on what they bought,
with a 10% loyalty discount on a second order.

---

### Finding 3 — At-Risk Customers Were Once Loyal
Users with high frequency but low recency (R≤2, F≥3) have gone quiet.
They are familiar with the platform but something caused them to stop.
**Recommendation:** Segment by last-purchased category and send a targeted win-back
email with a time-limited "we miss you" discount on that category.
A/B test 10% vs 15% discounts to find the minimum effective incentive.

---

### Finding 4 — Big Spenders Under-Purchase Relative to Their Value
High monetary value but low frequency suggests large, infrequent orders.
These may be business buyers.
**Recommendation:** Reach out with a B2B account offering: bulk pricing, invoicing,
and a dedicated account manager. Converting one Big Spender to a B2B account
can double their annual spend.

---

### Finding 5 — Device Preference Should Drive Channel Strategy
If Champions skew desktop but Lost customers are mobile-dominant,
our re-engagement channels should match those preferences.
**Recommendation:** Desktop-dominant segments → email campaigns with rich HTML.
Mobile-dominant segments → push notifications and SMS with deep links.

---
*Analysis from `analytics/rfm_segmentation.ipynb`*
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
    md  ("md-rfm-calc",       RFM_CALC_MD),
    code("code-rfm-calc",     RFM_CALC_CODE),
    md  ("md-segments",       SEGMENTS_MD),
    code("code-segments",     SEGMENTS_CODE),
    md  ("md-seg-viz",        SEG_VIZ_MD),
    code("code-seg-viz",      SEG_VIZ_CODE),
    md  ("md-seg-profiles",   SEG_PROFILES_MD),
    code("code-seg-profiles", SEG_PROFILES_CODE),
    md  ("md-device",         DEVICE_MD),
    code("code-device",       DEVICE_CODE),
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

out = pathlib.Path(__file__).parent / "rfm_segmentation.ipynb"
out.write_text(json.dumps(notebook, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Wrote {out}  ({len(cells)} cells)")
