"""
generate_data.py
Generates all raw datasets for the Data Infrastructure & Analytics Engineering project.
Simulates a manufacturing/automotive supply chain analytics use case.
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

os.makedirs("data", exist_ok=True)

# ── CONFIG ───────────────────────────────────────────────────────────────────
SUPPLIERS     = [f"SUP_{i:03d}" for i in range(1, 21)]
PRODUCTS      = ["Drive Shaft", "Brake Assembly", "Gear Box", "Clutch Kit",
                 "Hydraulic Pump", "Sensor Array", "PCB Assembly", "Wiring Harness",
                 "Control Panel", "Suspension Unit"]
CUSTOMERS     = ["Sona Comstar", "Gestamp", "Denso", "Maruti Suzuki",
                 "JCB", "BMW", "Bosch", "Tata Motors", "Hero MotoCorp", "Mahindra"]
PLANTS        = ["Plant_Delhi", "Plant_Pune", "Plant_Chennai", "Plant_Ahmedabad"]
DEFECT_TYPES  = ["Dimensional", "Surface Finish", "Functional", "Assembly Error", "None"]
START         = datetime(2023, 1, 1)
N_ORDERS      = 2000
N_EVENTS      = 5000

print("Generating datasets...")

# ── 1. ORDERS TABLE (SQL) ────────────────────────────────────────────────────
orders = []
for i in range(1, N_ORDERS + 1):
    order_date    = START + timedelta(days=random.randint(0, 729))
    lead_days     = random.randint(3, 30)
    delivery_date = order_date + timedelta(days=lead_days)
    qty           = random.randint(50, 2000)
    unit_price    = round(random.uniform(200, 8000), 2)
    discount      = round(random.uniform(0, 0.15), 3)
    revenue       = round(qty * unit_price * (1 - discount), 2)
    orders.append({
        "order_id":      f"ORD_{i:05d}",
        "order_date":    order_date.strftime("%Y-%m-%d"),
        "delivery_date": delivery_date.strftime("%Y-%m-%d"),
        "customer":      random.choice(CUSTOMERS),
        "product":       random.choice(PRODUCTS),
        "plant":         random.choice(PLANTS),
        "supplier_id":   random.choice(SUPPLIERS),
        "quantity":      qty,
        "unit_price":    unit_price,
        "discount_pct":  discount,
        "revenue":       revenue,
        "status":        random.choice(["Delivered", "Delivered", "Delivered", "Pending", "Cancelled"])
    })
pd.DataFrame(orders).to_csv("data/orders.csv", index=False)
print(f"  orders.csv          → {N_ORDERS} rows")

# ── 2. SUPPLIERS TABLE (SQL) ─────────────────────────────────────────────────
suppliers = []
for s in SUPPLIERS:
    suppliers.append({
        "supplier_id":      s,
        "supplier_name":    f"Supplier_{s[-3:]}",
        "country":          random.choice(["India", "Germany", "Japan", "USA", "UK"]),
        "tier":             random.choice(["Tier 1", "Tier 1", "Tier 2", "Tier 3"]),
        "on_time_rate":     round(random.uniform(0.72, 0.99), 3),
        "quality_score":    round(random.uniform(3.2, 5.0), 2),
        "avg_lead_days":    random.randint(5, 25),
        "contract_value":   round(random.uniform(500000, 5000000), 2),
        "active":           random.choice([1, 1, 1, 0])
    })
pd.DataFrame(suppliers).to_csv("data/suppliers.csv", index=False)
print(f"  suppliers.csv       → {len(suppliers)} rows")

# ── 3. QUALITY INSPECTIONS TABLE (SQL) ───────────────────────────────────────
inspections = []
for i, order in enumerate(orders[:1500]):
    defect = random.choice(DEFECT_TYPES)
    inspections.append({
        "inspection_id":  f"INS_{i+1:05d}",
        "order_id":       order["order_id"],
        "product":        order["product"],
        "plant":          order["plant"],
        "inspection_date":order["delivery_date"],
        "batch_size":     order["quantity"],
        "defects_found":  0 if defect == "None" else random.randint(1, int(order["quantity"] * 0.08)),
        "defect_type":    defect,
        "inspector_id":   f"INS_EMP_{random.randint(1,10):02d}",
        "passed":         1 if defect == "None" else random.choice([0, 1])
    })
df_ins = pd.DataFrame(inspections)
df_ins["defect_rate"] = (df_ins["defects_found"] / df_ins["batch_size"]).round(4)
df_ins.to_csv("data/quality_inspections.csv", index=False)
print(f"  quality_inspections → {len(inspections)} rows")

# ── 4. MACHINE EVENTS LOG (NoSQL / JSON) ─────────────────────────────────────
events = []
machines = [f"MCH_{i:03d}" for i in range(1, 16)]
event_types = ["cycle_complete", "downtime_start", "downtime_end",
               "maintenance", "alert", "shift_start", "shift_end"]
for i in range(N_EVENTS):
    ts = START + timedelta(
        days=random.randint(0, 729),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    evt = random.choice(event_types)
    doc = {
        "event_id":    f"EVT_{i+1:06d}",
        "timestamp":   ts.isoformat(),
        "machine_id":  random.choice(machines),
        "plant":       random.choice(PLANTS),
        "event_type":  evt,
        "operator_id": f"OPR_{random.randint(1,30):02d}",
        "metadata": {
            "shift":        random.choice(["Morning", "Afternoon", "Night"]),
            "product_run":  random.choice(PRODUCTS),
            "oee_score":    round(random.uniform(0.55, 0.95), 3) if evt == "cycle_complete" else None,
            "duration_min": round(random.uniform(1, 480), 1) if "downtime" in evt else None,
            "alert_code":   f"ALT_{random.randint(100,999)}" if evt == "alert" else None,
            "sensor_readings": {
                "temperature_c": round(random.uniform(18, 95), 1),
                "vibration_hz":  round(random.uniform(0.1, 12.5), 2),
                "pressure_bar":  round(random.uniform(1.0, 8.5), 2)
            } if evt in ["cycle_complete", "alert"] else {}
        }
    }
    events.append(doc)

with open("data/machine_events.json", "w") as f:
    json.dump(events, f, indent=2)
print(f"  machine_events.json → {N_EVENTS} documents")

# ── 5. CUSTOMER FEEDBACK (NoSQL / JSON — semi-structured) ────────────────────
feedback = []
for i in range(500):
    fb = {
        "feedback_id":   f"FB_{i+1:04d}",
        "customer":      random.choice(CUSTOMERS),
        "product":       random.choice(PRODUCTS),
        "date":          (START + timedelta(days=random.randint(0, 729))).strftime("%Y-%m-%d"),
        "channel":       random.choice(["email", "portal", "call", "site_visit"]),
        "rating":        random.randint(1, 5),
        "tags":          random.sample(["quality", "delivery", "pricing",
                                        "communication", "packaging", "technical"], k=random.randint(1, 3)),
        "sentiment":     random.choice(["positive", "positive", "neutral", "negative"]),
        "follow_up_required": random.choice([True, False]),
        "resolution":    {
            "resolved":       random.choice([True, False]),
            "resolution_days": random.randint(1, 14) if random.random() > 0.3 else None,
            "owner":          f"CSM_{random.randint(1,5):02d}"
        } if random.random() > 0.2 else None
    }
    feedback.append(fb)

with open("data/customer_feedback.json", "w") as f:
    json.dump(feedback, f, indent=2)
print(f"  customer_feedback   → {len(feedback)} documents")

print("\nAll datasets generated successfully.")
