"""
nosql_analytics.py
─────────────────────────────────────────────────────────────────────────────
NoSQL / JSON analytics layer for machine event logs and customer feedback.
Simulates MongoDB-style queries using Python (json + pandas).
In production: replace with pymongo queries against a MongoDB Atlas cluster.
─────────────────────────────────────────────────────────────────────────────
"""

import json
import pandas as pd
import numpy as np
import os
from collections import defaultdict, Counter

os.makedirs("output", exist_ok=True)

print("=" * 65)
print("  NoSQL ANALYTICS ENGINE")
print("=" * 65)

# ── LOAD JSON DOCUMENTS ──────────────────────────────────────────────────────
print("\n[1/4] Loading JSON documents...")
with open("data/machine_events.json") as f:
    events = json.load(f)
with open("data/customer_feedback.json") as f:
    feedback = json.load(f)
print(f"  machine_events      : {len(events):,} documents")
print(f"  customer_feedback   : {len(feedback):,} documents")

# ── MACHINE EVENTS ANALYTICS ─────────────────────────────────────────────────
print("\n[2/4] Analysing machine event logs...")

# Flatten nested JSON → DataFrame (mimics MongoDB $unwind + $project)
rows = []
for e in events:
    meta    = e.get("metadata", {})
    sensors = meta.get("sensor_readings", {})
    rows.append({
        "event_id":       e["event_id"],
        "timestamp":      pd.to_datetime(e["timestamp"]),
        "machine_id":     e["machine_id"],
        "plant":          e["plant"],
        "event_type":     e["event_type"],
        "operator_id":    e["operator_id"],
        "shift":          meta.get("shift"),
        "product_run":    meta.get("product_run"),
        "oee_score":      meta.get("oee_score"),
        "duration_min":   meta.get("duration_min"),
        "alert_code":     meta.get("alert_code"),
        "temperature_c":  sensors.get("temperature_c"),
        "vibration_hz":   sensors.get("vibration_hz"),
        "pressure_bar":   sensors.get("pressure_bar"),
    })

df_events = pd.DataFrame(rows)
df_events["date"]    = df_events["timestamp"].dt.date
df_events["month"]   = df_events["timestamp"].dt.to_period("M")

# Aggregation 1: OEE by machine
oee_by_machine = (
    df_events[df_events["event_type"] == "cycle_complete"]
    .groupby("machine_id")
    .agg(
        avg_oee    =("oee_score", "mean"),
        cycles     =("event_id", "count"),
        min_oee    =("oee_score", "min"),
        max_oee    =("oee_score", "max")
    )
    .round(3)
    .reset_index()
    .sort_values("avg_oee")
)
oee_by_machine["oee_rag"] = oee_by_machine["avg_oee"].apply(
    lambda x: "GREEN" if x >= 0.85 else "AMBER" if x >= 0.70 else "RED")
oee_by_machine.to_csv("output/nosql_oee_by_machine.csv", index=False)
print("\n  OEE by Machine (bottom 5):")
print(oee_by_machine.head(5).to_string(index=False))

# Aggregation 2: Downtime events by plant
downtime = df_events[df_events["event_type"] == "downtime_start"].copy()
downtime_by_plant = (
    downtime.groupby("plant")
    .agg(
        downtime_events=("event_id", "count"),
        total_duration =("duration_min", "sum"),
        avg_duration   =("duration_min", "mean")
    )
    .round(2)
    .reset_index()
    .sort_values("total_duration", ascending=False)
)
downtime_by_plant.to_csv("output/nosql_downtime_by_plant.csv", index=False)
print(f"\n  Downtime by Plant:")
print(downtime_by_plant.to_string(index=False))

# Aggregation 3: Alert frequency by machine
alerts = df_events[df_events["event_type"] == "alert"]
alert_counts = (
    alerts.groupby(["machine_id", "alert_code"])
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
    .head(10)
)
alert_counts.to_csv("output/nosql_top_alerts.csv", index=False)
print(f"\n  Top Alert Codes: {len(alert_counts)} unique machine-alert pairs")

# Aggregation 4: Sensor anomaly detection
# Flag readings > 2 std deviations from mean
sensor_df = df_events.dropna(subset=["temperature_c", "vibration_hz", "pressure_bar"])
for col in ["temperature_c", "vibration_hz", "pressure_bar"]:
    mean, std      = sensor_df[col].mean(), sensor_df[col].std()
    sensor_df[f"{col}_anomaly"] = (abs(sensor_df[col] - mean) > 2 * std).astype(int)

anomaly_summary = sensor_df[[
    "temperature_c_anomaly", "vibration_hz_anomaly", "pressure_bar_anomaly"
]].sum()
print(f"\n  Sensor Anomalies Detected:")
for k, v in anomaly_summary.items():
    print(f"    {k:<35}: {v:,}")

# ── CUSTOMER FEEDBACK ANALYTICS ──────────────────────────────────────────────
print("\n[3/4] Analysing customer feedback documents...")

fb_rows = []
for fb in feedback:
    res = fb.get("resolution") or {}
    fb_rows.append({
        "feedback_id":      fb["feedback_id"],
        "customer":         fb["customer"],
        "product":          fb["product"],
        "date":             fb["date"],
        "channel":          fb["channel"],
        "rating":           fb["rating"],
        "sentiment":        fb["sentiment"],
        "follow_up":        fb["follow_up_required"],
        "tags":             ", ".join(fb.get("tags", [])),
        "resolved":         res.get("resolved"),
        "resolution_days":  res.get("resolution_days"),
        "owner":            res.get("owner")
    })

df_fb = pd.DataFrame(fb_rows)

# Sentiment by customer
sentiment = (
    df_fb.groupby(["customer", "sentiment"])
    .size()
    .unstack(fill_value=0)
    .reset_index()
)
sentiment["total"]     = sentiment.iloc[:, 1:].sum(axis=1)
sentiment["neg_rate"]  = (sentiment.get("negative", 0) / sentiment["total"] * 100).round(1)
sentiment = sentiment.sort_values("neg_rate", ascending=False)
sentiment.to_csv("output/nosql_sentiment_by_customer.csv", index=False)
print(f"\n  Sentiment by Customer (top negatives):")
print(sentiment[["customer", "total", "neg_rate"]].head(5).to_string(index=False))

# Tag frequency
all_tags = []
for t in df_fb["tags"]:
    all_tags.extend([x.strip() for x in t.split(",")])
tag_counts = pd.Series(Counter(all_tags)).sort_values(ascending=False)
tag_counts.to_csv("output/nosql_tag_frequency.csv", header=["count"])
print(f"\n  Top Feedback Tags: {tag_counts.head(5).to_dict()}")

# Resolution SLA
resolved = df_fb[df_fb["resolved"] == True]
avg_res_days = resolved["resolution_days"].mean()
print(f"\n  Avg Resolution Days : {avg_res_days:.1f}")
print(f"  Unresolved Tickets  : {(df_fb['resolved'] == False).sum()}")

# ── EXPORT FLATTENED DATASETS ─────────────────────────────────────────────────
print("\n[4/4] Exporting flattened datasets for BI layer...")
df_events.to_csv("output/nosql_events_flattened.csv", index=False)
df_fb.to_csv("output/nosql_feedback_flattened.csv", index=False)
print("  nosql_events_flattened.csv  → saved")
print("  nosql_feedback_flattened.csv → saved")

print("\n" + "=" * 65)
print("  NoSQL Analytics complete.")
print("=" * 65)
