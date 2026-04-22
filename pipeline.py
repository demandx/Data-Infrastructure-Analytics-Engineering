"""
pipeline.py
─────────────────────────────────────────────────────────────────────────────
Master Pipeline Orchestrator
Data Infrastructure & Analytics Engineering
Author: Tauseef Ahmad Khan

Runs all pipeline stages in sequence:
  Stage 1 → generate_data.py       (data simulation)
  Stage 2 → sql_analytics.py       (relational SQL layer)
  Stage 3 → nosql_analytics.py     (NoSQL / JSON layer)
  Stage 4 → dsa/dsa_pipeline.py    (DSA-optimised processing)
  Stage 5 → visualisation.py       (MI dashboard + charts)
─────────────────────────────────────────────────────────────────────────────
"""

import subprocess
import sys
import time
import os

os.makedirs("output", exist_ok=True)
os.makedirs("charts", exist_ok=True)

STAGES = [
    ("Data Generation",      "generate_data.py"),
    ("SQL Analytics",        "sql_analytics.py"),
    ("NoSQL Analytics",      "nosql_analytics.py"),
    ("DSA Pipeline",         "dsa/dsa_pipeline.py"),
    ("Visualisation",        "visualisation.py"),
]

print("=" * 65)
print("  DATA INFRASTRUCTURE & ANALYTICS ENGINEERING")
print("  Master Pipeline — Tauseef Ahmad Khan")
print("=" * 65)

total_start = time.time()
results = []

for name, script in STAGES:
    print(f"\n{'─'*65}")
    print(f"  STAGE: {name}")
    print(f"{'─'*65}")
    t0  = time.time()
    ret = subprocess.run([sys.executable, script], capture_output=False, text=True)
    elapsed = time.time() - t0
    status  = "✓ PASSED" if ret.returncode == 0 else "✗ FAILED"
    results.append((name, status, elapsed))

total_elapsed = time.time() - total_start

print(f"\n{'='*65}")
print("  PIPELINE EXECUTION SUMMARY")
print(f"{'='*65}")
for name, status, elapsed in results:
    print(f"  {status}  {name:<35} {elapsed:.1f}s")
print(f"\n  Total runtime: {total_elapsed:.1f}s")
print(f"  Outputs saved to: output/  |  charts/")
print(f"{'='*65}")
