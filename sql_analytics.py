"""
sql_analytics.py
─────────────────────────────────────────────────────────────────────────────
Loads CSV data into SQLite, runs all analytical SQL queries,
and exports results for BI reporting and Power BI ingestion.
(SQLite used for portability; same queries run on MySQL with schema.sql)
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import sqlite3
import os

os.makedirs("output", exist_ok=True)

print("=" * 65)
print("  SQL ANALYTICS ENGINE")
print("=" * 65)

# ── LOAD DATA INTO SQLITE ────────────────────────────────────────────────────
print("\n[1/3] Loading data into SQLite...")
conn = sqlite3.connect(":memory:")

orders      = pd.read_csv("data/orders.csv")
suppliers   = pd.read_csv("data/suppliers.csv")
inspections = pd.read_csv("data/quality_inspections.csv")

orders.to_sql("orders", conn, if_exists="replace", index=False)
suppliers.to_sql("suppliers", conn, if_exists="replace", index=False)
inspections.to_sql("quality_inspections", conn, if_exists="replace", index=False)
print(f"  orders              : {len(orders):,} rows")
print(f"  suppliers           : {len(suppliers):,} rows")
print(f"  quality_inspections : {len(inspections):,} rows")

# ── RUN QUERIES ──────────────────────────────────────────────────────────────
print("\n[2/3] Running analytical queries...")

# Q1: Revenue by customer
q1 = pd.read_sql("""
    SELECT
        customer,
        COUNT(order_id)                           AS total_orders,
        ROUND(SUM(revenue), 2)                    AS total_revenue,
        ROUND(AVG(revenue), 2)                    AS avg_order_value,
        ROUND(SUM(revenue) * 100.0 /
              (SELECT SUM(revenue) FROM orders WHERE status='Delivered'), 2) AS revenue_share_pct
    FROM orders
    WHERE status = 'Delivered'
    GROUP BY customer
    ORDER BY total_revenue DESC
""", conn)
q1.to_csv("output/q1_revenue_by_customer.csv", index=False)
print("\n  Q1 — Revenue by Customer:")
print(q1.to_string(index=False))

# Q2: Supplier scorecard
q2 = pd.read_sql("""
    SELECT
        s.supplier_id,
        s.tier,
        s.country,
        ROUND(s.on_time_rate * 100, 1)            AS on_time_pct,
        s.quality_score,
        COUNT(o.order_id)                         AS total_orders,
        ROUND(SUM(o.revenue), 2)                  AS total_revenue,
        ROUND(AVG(JULIANDAY(o.delivery_date) -
                  JULIANDAY(o.order_date)), 1)    AS avg_lead_days,
        CASE
            WHEN s.quality_score >= 4.5 AND s.on_time_rate >= 0.95 THEN 'GREEN'
            WHEN s.quality_score >= 3.8 AND s.on_time_rate >= 0.85 THEN 'AMBER'
            ELSE 'RED'
        END                                       AS supplier_rag
    FROM suppliers s
    LEFT JOIN orders o ON s.supplier_id = o.supplier_id
    WHERE s.active = 1
    GROUP BY s.supplier_id, s.tier, s.country, s.on_time_rate, s.quality_score
    ORDER BY total_revenue DESC
    LIMIT 10
""", conn)
q2.to_csv("output/q2_supplier_scorecard.csv", index=False)
print(f"\n  Q2 — Supplier Scorecard (top 10 shown):")
print(q2.to_string(index=False))

# Q3: Monthly revenue trend
q3 = pd.read_sql("""
    SELECT
        SUBSTR(order_date, 1, 7)                  AS month,
        ROUND(SUM(revenue), 2)                    AS revenue,
        COUNT(order_id)                           AS orders
    FROM orders
    WHERE status = 'Delivered'
    GROUP BY SUBSTR(order_date, 1, 7)
    ORDER BY month
""", conn)
q3["mom_growth_pct"] = q3["revenue"].pct_change().mul(100).round(2)
q3.to_csv("output/q3_monthly_trend.csv", index=False)
print(f"\n  Q3 — Monthly Revenue Trend: {len(q3)} months")

# Q4: Defect rate by product
q4 = pd.read_sql("""
    SELECT
        product,
        plant,
        COUNT(inspection_id)                      AS inspections,
        SUM(defects_found)                        AS total_defects,
        SUM(batch_size)                           AS total_units,
        ROUND(CAST(SUM(defects_found) AS FLOAT) /
              NULLIF(SUM(batch_size), 0) * 100, 3) AS defect_rate_pct,
        ROUND(SUM(CASE WHEN passed=0 THEN 1.0 ELSE 0 END) /
              COUNT(inspection_id) * 100, 2)      AS failure_rate_pct
    FROM quality_inspections
    GROUP BY product, plant
    ORDER BY defect_rate_pct DESC
""", conn)
q4["quality_rag"] = q4["defect_rate_pct"].apply(
    lambda x: "GREEN" if x < 1 else "AMBER" if x < 3 else "RED")
q4.to_csv("output/q4_defect_analysis.csv", index=False)
print(f"\n  Q4 — Defect Analysis: {len(q4)} product-plant combinations")

# Q5: Plant efficiency
q5 = pd.read_sql("""
    SELECT
        o.plant,
        COUNT(DISTINCT o.order_id)                AS total_orders,
        ROUND(SUM(o.revenue), 2)                  AS total_revenue,
        COUNT(DISTINCT o.customer)                AS unique_customers,
        ROUND(AVG(qi.defect_rate) * 100, 3)       AS avg_defect_rate_pct
    FROM orders o
    LEFT JOIN quality_inspections qi ON o.order_id = qi.order_id
    WHERE o.status = 'Delivered'
    GROUP BY o.plant
    ORDER BY total_revenue DESC
""", conn)
q5.to_csv("output/q5_plant_efficiency.csv", index=False)
print(f"\n  Q5 — Plant Efficiency:")
print(q5.to_string(index=False))

# ── EXPORT COMBINED ──────────────────────────────────────────────────────────
print("\n[3/3] Exporting results...")
print("  All query outputs saved to output/")

conn.close()
print("\n" + "=" * 65)
print("  SQL Analytics complete.")
print("=" * 65)
