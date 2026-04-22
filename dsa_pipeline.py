"""
dsa_pipeline.py
─────────────────────────────────────────────────────────────────────────────
DSA-Optimised Data Processing Pipeline
Author: Tauseef Ahmad Khan

Demonstrates algorithmic thinking applied to analytical data engineering:
  1. Hash map O(1) supplier lookup vs O(n) scan
  2. Heap-based Top-K extraction (O(n log k))
  3. Sliding window revenue aggregation (O(n))
  4. Binary search on sorted delivery data (O(log n))
  5. Graph-based supplier dependency analysis
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import numpy as np
import heapq
import bisect
import time
import os
from collections import defaultdict, deque

os.makedirs("output", exist_ok=True)

print("=" * 65)
print("  DSA-OPTIMISED PIPELINE")
print("=" * 65)

orders    = pd.read_csv("data/orders.csv")
suppliers = pd.read_csv("data/suppliers.csv")

# ── DSA 1: HASH MAP — O(1) SUPPLIER LOOKUP ──────────────────────────────────
print("\n[1/5] Hash Map: O(1) supplier lookup vs O(n) linear scan")

# Build hash map: supplier_id → supplier record
supplier_map = {
    row["supplier_id"]: row.to_dict()
    for _, row in suppliers.iterrows()
}

# Benchmark: hash map vs linear scan
test_ids = orders["supplier_id"].dropna().sample(1000, random_state=42).tolist()

t0 = time.perf_counter()
for sid in test_ids:
    _ = supplier_map.get(sid)
hash_time = (time.perf_counter() - t0) * 1000

t0 = time.perf_counter()
for sid in test_ids:
    _ = suppliers[suppliers["supplier_id"] == sid]
scan_time = (time.perf_counter() - t0) * 1000

speedup = scan_time / hash_time
print(f"  Hash map lookup (1,000 lookups) : {hash_time:.2f} ms")
print(f"  Linear scan     (1,000 lookups) : {scan_time:.2f} ms")
print(f"  Speedup                         : {speedup:.1f}x faster")

# Enrich orders using hash map
delivered = orders[orders["status"] == "Delivered"].copy()
delivered["supplier_tier"]     = delivered["supplier_id"].map(
    lambda x: supplier_map.get(x, {}).get("tier", "Unknown"))
delivered["supplier_quality"]  = delivered["supplier_id"].map(
    lambda x: supplier_map.get(x, {}).get("quality_score", None))
delivered["supplier_ontime"]   = delivered["supplier_id"].map(
    lambda x: supplier_map.get(x, {}).get("on_time_rate", None))


# ── DSA 2: MIN-HEAP — TOP-K CUSTOMERS BY REVENUE ────────────────────────────
print("\n[2/5] Heap: Top-K customers by revenue  O(n log k)")

customer_revenue = delivered.groupby("customer")["revenue"].sum().to_dict()
K = 5

# Min-heap of size K → O(n log k)
heap = []
for customer, rev in customer_revenue.items():
    if len(heap) < K:
        heapq.heappush(heap, (rev, customer))
    elif rev > heap[0][0]:
        heapq.heapreplace(heap, (rev, customer))

top_k = sorted(heap, key=lambda x: -x[0])
print(f"  Top {K} Customers by Revenue:")
for rank, (rev, cust) in enumerate(top_k, 1):
    print(f"    {rank}. {cust:<20} ₹{rev/1e6:.2f} Mn")


# ── DSA 3: SLIDING WINDOW — 30-DAY ROLLING REVENUE ──────────────────────────
print("\n[3/5] Sliding Window: 30-day rolling revenue  O(n)")

daily = (
    delivered.assign(order_date=pd.to_datetime(delivered["order_date"]))
    .groupby("order_date")["revenue"]
    .sum()
    .sort_index()
    .reset_index()
)
daily.columns = ["date", "revenue"]
dates   = daily["date"].tolist()
revs    = daily["revenue"].tolist()
n       = len(revs)
WINDOW  = 30

# Manual sliding window
window_sums = []
window      = deque()
window_sum  = 0

for i, (date, rev) in enumerate(zip(dates, revs)):
    window.append((date, rev))
    window_sum += rev
    # Remove entries outside 30-day window
    while (date - window[0][0]).days > WINDOW:
        window_sum -= window.popleft()[1]
    window_sums.append(round(window_sum, 2))

daily["rolling_30d_revenue"] = window_sums
daily["rolling_30d_revenue_mn"] = (daily["rolling_30d_revenue"] / 1e6).round(3)
daily.to_csv("output/dsa_rolling_revenue.csv", index=False)
print(f"  Computed 30-day rolling revenue for {n} trading days")
print(f"  Peak 30-day window : ₹{max(window_sums)/1e6:.2f} Mn")
print(f"  Avg  30-day window : ₹{np.mean(window_sums)/1e6:.2f} Mn")


# ── DSA 4: BINARY SEARCH — SLA BREACH DETECTION ─────────────────────────────
print("\n[4/5] Binary Search: SLA breach detection  O(log n)")

delivered2 = delivered.copy()
delivered2["order_date"]    = pd.to_datetime(delivered2["order_date"])
delivered2["delivery_date"] = pd.to_datetime(delivered2["delivery_date"])
delivered2["lead_days"]     = (
    delivered2["delivery_date"] - delivered2["order_date"]
).dt.days

sorted_lead = sorted(delivered2["lead_days"].tolist())
SLA_THRESHOLD = 21  # days

# Binary search for first lead time > SLA threshold
breach_idx  = bisect.bisect_right(sorted_lead, SLA_THRESHOLD)
total       = len(sorted_lead)
breaches    = total - breach_idx
breach_pct  = breaches / total * 100

print(f"  SLA Threshold       : {SLA_THRESHOLD} days")
print(f"  Total Orders        : {total:,}")
print(f"  SLA Breaches        : {breaches:,} ({breach_pct:.1f}%)")
print(f"  Within SLA          : {breach_idx:,} ({100-breach_pct:.1f}%)")
print(f"  Avg Lead Time       : {np.mean(sorted_lead):.1f} days")
print(f"  95th Pctile         : {np.percentile(sorted_lead, 95):.0f} days")


# ── DSA 5: GRAPH — SUPPLIER DEPENDENCY MAP ──────────────────────────────────
print("\n[5/5] Graph: Supplier-Customer dependency map  O(V+E)")

# Build adjacency list: supplier → customers served
adj_list    = defaultdict(set)
edge_weights = defaultdict(float)  # supplier→customer: total revenue

for _, row in delivered.iterrows():
    sid, cust, rev = row["supplier_id"], row["customer"], row["revenue"]
    adj_list[sid].add(cust)
    edge_weights[(sid, cust)] += rev

# Graph stats
num_nodes  = len(adj_list) + len(set(delivered["customer"]))
num_edges  = sum(len(v) for v in adj_list.values())
avg_degree = num_edges / len(adj_list)

# Critical suppliers: serve 3+ customers (high dependency risk)
critical = {s: list(c) for s, c in adj_list.items() if len(c) >= 3}
print(f"  Graph nodes (suppliers + customers) : {num_nodes}")
print(f"  Graph edges (supply relationships)  : {num_edges}")
print(f"  Avg customer reach per supplier     : {avg_degree:.1f}")
print(f"  Critical suppliers (3+ customers)   : {len(critical)}")

# Export dependency table
dep_rows = []
for (sid, cust), rev in edge_weights.items():
    dep_rows.append({
        "supplier_id": sid,
        "customer":    cust,
        "total_revenue": round(rev, 2),
        "tier": supplier_map.get(sid, {}).get("tier", "Unknown")
    })
dep_df = pd.DataFrame(dep_rows).sort_values("total_revenue", ascending=False)
dep_df.to_csv("output/dsa_supplier_dependency.csv", index=False)
print(f"  Dependency map saved → output/dsa_supplier_dependency.csv")

# ── SUMMARY ──────────────────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("  DSA PIPELINE SUMMARY")
print("=" * 65)
print(f"  Hash Map speedup    : {speedup:.1f}x over linear scan")
print(f"  Top-K (k=5)         : Heap O(n log k) vs sort O(n log n)")
print(f"  Sliding Window      : O(n) vs O(n×w) naive approach")
print(f"  Binary Search       : O(log n) SLA breach detection")
print(f"  Graph traversal     : O(V+E) dependency mapping")
print("=" * 65)
