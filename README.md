# Data Infrastructure Analytics Engineering

### SQL · MySQL · NoSQL · Python · R · DSA
*Supply Chain Analytics | Multi-Layer Data Architecture | Algorithmic Optimisation*

---

## Project Overview

This project builds a **multi-layer data infrastructure and analytics engineering system** for a manufacturing supply chain operation. It simulates the full stack a data/analytics engineer builds in practice — from relational schema design and SQL query development, to NoSQL document analytics, statistical modelling in R, and DSA-optimised Python pipelines.

The business context is a manufacturing supply chain covering 10 customers (Sona Comstar, Gestamp, Denso, Maruti Suzuki, JCB, BMW, and others), 20 suppliers, 4 plants, and 10 product lines — generating 2,000 orders, 1,500 quality inspections, 5,000 machine event documents, and 500 customer feedback records.

---

## Business Problems Solved

| Layer | Problem | Solution |
|-------|---------|----------|
| SQL | No single source of truth for supplier performance | Relational schema + 7 analytical queries with window functions |
| NoSQL | Machine event logs and feedback unstructured, unanalysed | JSON flattening pipeline + OEE, downtime, sentiment analytics |
| R | No statistical rigour in supplier/defect reporting | Correlation analysis, regression, descriptive stats |
| DSA | Slow data processing pipelines on large order datasets | Hash map, heap, sliding window, binary search, graph algorithms |
| BI | No unified dashboard for leadership | 6-panel MI dashboard with RAG-coded KPIs |

---

## Project Structure

```
data-infrastructure-analytics/
│
├── data/
│   ├── orders.csv                      # 2,000 manufacturing orders
│   ├── suppliers.csv                   # 20 supplier records
│   ├── quality_inspections.csv         # 1,500 inspection records
│   ├── machine_events.json             # 5,000 machine event documents (NoSQL)
│   └── customer_feedback.json          # 500 feedback documents (semi-structured)
│
├── sql/
│   ├── schema.sql                      # MySQL schema + indexes
│   └── queries.sql                     # 7 analytical SQL queries
│
├── r_analysis/
│   └── r_analysis.R                    # Statistical analysis in R
│
├── dsa/
│   └── dsa_pipeline.py                 # DSA-optimised processing algorithms
│
├── output/                             # All pipeline outputs (CSV)
├── charts/
│   └── mi_dashboard.png                # 6-panel MI dashboard
│
├── generate_data.py                    # Simulates all datasets
├── sql_analytics.py                    # SQL analytics engine (SQLite)
├── nosql_analytics.py                  # NoSQL / JSON analytics
├── visualisation.py                    # Dashboard & charts
├── pipeline.py                         # Master pipeline orchestrator
└── requirements.txt
```

---

## Layer 1 — Relational Database (SQL / MySQL)

### Schema Design (`sql/schema.sql`)
Three normalised tables with foreign key relationships and performance indexes:

```sql
suppliers            -- supplier master (tier, quality score, on-time rate)
    ↓ FK
orders               -- 2,000 order transactions
    ↓ FK
quality_inspections  -- batch inspection results per order
```

Indexes created on: `customer`, `order_date`, `status`, `supplier_id`, `product`, `plant`, `defect_type`

### Analytical Queries (`sql/queries.sql`)

| Query | Technique | Business Output |
|-------|-----------|----------------|
| Q1 | `SUM`, `OVER()`, running total | Revenue by customer with share % |
| Q2 | Multi-table JOIN + CASE RAG logic | Supplier performance scorecard |
| Q3 | CTE + `LAG()` window function | Monthly revenue with MoM growth % |
| Q4 | Grouped aggregation + NULL handling | Defect rate by product and plant |
| Q5 | `RANK() OVER (PARTITION BY)` | Top products per customer |
| Q6 | CTE cohort + customer lifetime value | Customer tier segmentation |
| Q7 | LEFT JOIN + plant efficiency | Plant-level operational summary |

---

## Layer 2 — NoSQL / Document Analytics

### Machine Events (`data/machine_events.json`)
5,000 nested JSON documents simulating real IoT machine logs:

```json
{
  "event_id": "EVT_000042",
  "machine_id": "MCH_007",
  "event_type": "cycle_complete",
  "metadata": {
    "oee_score": 0.812,
    "shift": "Morning",
    "sensor_readings": {
      "temperature_c": 74.3,
      "vibration_hz": 3.21,
      "pressure_bar": 4.8
    }
  }
}
```

**Analytics performed:**
- JSON flattening (`$unwind` equivalent using Pandas)
- OEE aggregation by machine with RAG classification
- Downtime event aggregation by plant
- Sensor anomaly detection (2σ threshold)
- Alert frequency analysis

### Customer Feedback (`data/customer_feedback.json`)
500 semi-structured documents with nested resolution objects, tag arrays, and variable schema — designed to demonstrate NoSQL's advantage over relational models for variable-structure data.

**Analytics performed:**
- Sentiment distribution by customer
- Tag frequency analysis (counter aggregation)
- Resolution SLA compliance
- Unresolved ticket tracking

---

## Layer 3 — Statistical Analysis (R)

`r_analysis/r_analysis.R` implements:

```r
# Supplier quality vs revenue correlation
cor(supplier_agg$quality_score, supplier_agg$total_revenue)
# → r = 0.xxx (quantifies relationship between supplier quality and revenue)

# Linear regression: revenue trend over time
model <- lm(revenue ~ t, data = monthly_revenue)
# → R², monthly trend coefficient, statistical significance

# Defect rate: mean, median, SD by product
# → Identifies which products have highest variance (process instability risk)
```

---

## Layer 4 — DSA-Optimised Pipeline

`dsa/dsa_pipeline.py` applies algorithmic thinking to data engineering:

### Algorithm 1: Hash Map — O(1) Supplier Lookup
```
Naive scan : 254 ms  (O(n) per lookup)
Hash map   : 0.11 ms (O(1) per lookup)
Speedup    : 2,327x faster
```
Used to enrich 2,000 orders with supplier metadata without repeated DataFrame scans.

### Algorithm 2: Min-Heap — Top-K Customers
```
Time complexity: O(n log k) vs O(n log n) for full sort
Space complexity: O(k) vs O(n)
```
Extracts top-5 customers by revenue using a fixed-size heap — scales to millions of rows.

### Algorithm 3: Sliding Window — 30-Day Rolling Revenue
```
Time complexity: O(n) — single pass with deque
vs naive: O(n × window_size)
```
Computes rolling 30-day revenue for 588 trading days in a single pass.

### Algorithm 4: Binary Search — SLA Breach Detection
```
Time complexity: O(log n) on sorted lead times
SLA threshold  : 21 days
Breach rate    : 31.8% of delivered orders
95th percentile: 29 days
```

### Algorithm 5: Graph — Supplier Dependency Map
```
Structure: Adjacency list (supplier → customers served)
Nodes: 30 | Edges: 199
Critical suppliers (serving 3+ customers): 20
```
Identifies concentration risk: which suppliers are single points of failure.

---

## Pipeline Output (Sample Run)

```
SQL Layer
  Top customer (revenue)     : Gestamp — ₹523 Mn
  Supplier RED alerts        : 5 suppliers below quality/on-time threshold
  SLA breach rate            : 31.8% of orders exceed 21-day threshold

NoSQL Layer
  Machine with lowest OEE    : MCH_004 — 72.0% (AMBER)
  Total downtime (all plants): ~159,027 minutes
  Highest negative sentiment : BMW — 39.5% negative feedback rate

DSA Layer
  Hash map speedup            : 2,327x over linear scan
  30-day peak revenue window  : ₹276 Mn
```

---

## How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline (all stages)
python pipeline.py

# Or run individual stages
python generate_data.py       # Step 1: generate datasets
python sql_analytics.py       # Step 2: SQL layer
python nosql_analytics.py     # Step 3: NoSQL layer
python dsa/dsa_pipeline.py    # Step 4: DSA pipeline
python visualisation.py       # Step 5: MI dashboard

# R analysis (requires R + tidyverse)
Rscript r_analysis/r_analysis.R
```

---

## Tools & Methods

| Area | Tool / Method |
|------|--------------|
| Relational DB | SQL, MySQL (schema + 7 queries, window functions, CTEs) |
| NoSQL | JSON, Python (MongoDB-style aggregations, document flattening) |
| Statistical analysis | R (dplyr, ggplot2, lm, cor, lubridate) |
| Python pipeline | Pandas, NumPy |
| DSA | Hash map, min-heap, sliding window, binary search, graph (adjacency list) |
| Visualisation | Matplotlib (6-panel MI dashboard) |
| Orchestration | Python subprocess pipeline runner |

---

## Requirements

```
pandas>=2.0.0
numpy>=1.24.0
matplotlib>=3.7.0
openpyxl>=3.1.0
```

R packages: `dplyr`, `ggplot2`, `tidyr`, `readr`, `scales`, `lubridate`

---

*Project: Data Infrastructure & Analytics Engineering | Supply Chain Analytics | SQL · NoSQL · R · Python · DSA*
