-- =============================================================================
-- queries.sql
-- Analytical SQL queries for supply chain BI & reporting
-- Author: Tauseef Ahmad Khan
-- =============================================================================


-- ── Q1: REVENUE BY CUSTOMER — ranked with running total ───────────────────
SELECT
    customer,
    COUNT(order_id)                                  AS total_orders,
    SUM(revenue)                                     AS total_revenue,
    ROUND(AVG(revenue), 2)                           AS avg_order_value,
    ROUND(SUM(revenue) * 100.0 /
          SUM(SUM(revenue)) OVER (), 2)              AS revenue_share_pct,
    ROUND(SUM(SUM(revenue)) OVER (
          ORDER BY SUM(revenue) DESC
          ROWS UNBOUNDED PRECEDING), 2)              AS running_total
FROM orders
WHERE status = 'Delivered'
GROUP BY customer
ORDER BY total_revenue DESC;


-- ── Q2: SUPPLIER PERFORMANCE SCORECARD ────────────────────────────────────
SELECT
    s.supplier_id,
    s.supplier_name,
    s.tier,
    s.country,
    s.on_time_rate,
    s.quality_score,
    COUNT(o.order_id)                                AS total_orders,
    ROUND(SUM(o.revenue), 2)                         AS total_revenue,
    ROUND(AVG(DATEDIFF(o.delivery_date,
              o.order_date)), 1)                     AS avg_actual_lead_days,
    s.avg_lead_days                                  AS contracted_lead_days,
    ROUND(AVG(DATEDIFF(o.delivery_date,
              o.order_date)) - s.avg_lead_days, 1)   AS lead_day_variance,
    CASE
        WHEN s.quality_score >= 4.5
         AND s.on_time_rate  >= 0.95 THEN 'GREEN'
        WHEN s.quality_score >= 3.8
         AND s.on_time_rate  >= 0.85 THEN 'AMBER'
        ELSE 'RED'
    END                                              AS supplier_rag
FROM suppliers s
LEFT JOIN orders o ON s.supplier_id = o.supplier_id
WHERE s.active = 1
GROUP BY s.supplier_id, s.supplier_name, s.tier, s.country,
         s.on_time_rate, s.quality_score, s.avg_lead_days
ORDER BY total_revenue DESC;


-- ── Q3: MONTHLY REVENUE TREND WITH MoM GROWTH ─────────────────────────────
WITH monthly AS (
    SELECT
        DATE_FORMAT(order_date, '%Y-%m')             AS month,
        SUM(revenue)                                 AS revenue
    FROM orders
    WHERE status = 'Delivered'
    GROUP BY DATE_FORMAT(order_date, '%Y-%m')
)
SELECT
    month,
    ROUND(revenue, 2)                                AS revenue,
    LAG(revenue) OVER (ORDER BY month)               AS prev_month_revenue,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY month))
          / LAG(revenue) OVER (ORDER BY month) * 100, 2) AS mom_growth_pct
FROM monthly
ORDER BY month;


-- ── Q4: DEFECT RATE ANALYSIS BY PRODUCT & PLANT ───────────────────────────
SELECT
    qi.product,
    qi.plant,
    COUNT(qi.inspection_id)                          AS total_inspections,
    SUM(qi.defects_found)                            AS total_defects,
    SUM(qi.batch_size)                               AS total_units,
    ROUND(SUM(qi.defects_found) * 100.0 /
          NULLIF(SUM(qi.batch_size), 0), 3)          AS defect_rate_pct,
    SUM(CASE WHEN qi.passed = 0 THEN 1 ELSE 0 END)  AS failed_inspections,
    ROUND(SUM(CASE WHEN qi.passed = 0 THEN 1 ELSE 0 END) * 100.0
          / COUNT(qi.inspection_id), 2)              AS failure_rate_pct,
    CASE
        WHEN SUM(qi.defects_found) * 100.0 /
             NULLIF(SUM(qi.batch_size), 0) < 1 THEN 'GREEN'
        WHEN SUM(qi.defects_found) * 100.0 /
             NULLIF(SUM(qi.batch_size), 0) < 3 THEN 'AMBER'
        ELSE 'RED'
    END                                              AS quality_rag
FROM quality_inspections qi
GROUP BY qi.product, qi.plant
ORDER BY defect_rate_pct DESC;


-- ── Q5: TOP 5 CUSTOMERS BY REVENUE — WINDOW FUNCTION ─────────────────────
SELECT
    customer,
    product,
    SUM(revenue)                                     AS product_revenue,
    RANK() OVER (
        PARTITION BY customer
        ORDER BY SUM(revenue) DESC
    )                                                AS product_rank_within_customer
FROM orders
WHERE status = 'Delivered'
GROUP BY customer, product
QUALIFY product_rank_within_customer <= 3
ORDER BY customer, product_rank_within_customer;


-- ── Q6: COHORT ANALYSIS — CUSTOMER ORDER FREQUENCY ───────────────────────
WITH customer_months AS (
    SELECT
        customer,
        DATE_FORMAT(order_date, '%Y-%m')             AS order_month,
        COUNT(order_id)                              AS orders_in_month,
        SUM(revenue)                                 AS revenue_in_month
    FROM orders
    WHERE status = 'Delivered'
    GROUP BY customer, DATE_FORMAT(order_date, '%Y-%m')
),
customer_summary AS (
    SELECT
        customer,
        COUNT(DISTINCT order_month)                  AS active_months,
        SUM(orders_in_month)                         AS total_orders,
        ROUND(SUM(revenue_in_month), 2)              AS lifetime_value,
        ROUND(AVG(revenue_in_month), 2)              AS avg_monthly_revenue,
        MIN(order_month)                             AS first_order_month,
        MAX(order_month)                             AS last_order_month
    FROM customer_months
    GROUP BY customer
)
SELECT *,
    CASE
        WHEN active_months >= 18 THEN 'Champion'
        WHEN active_months >= 12 THEN 'Loyal'
        WHEN active_months >= 6  THEN 'Developing'
        ELSE 'New / At Risk'
    END                                              AS customer_tier
FROM customer_summary
ORDER BY lifetime_value DESC;


-- ── Q7: PLANT EFFICIENCY SUMMARY ──────────────────────────────────────────
SELECT
    o.plant,
    COUNT(DISTINCT o.order_id)                       AS total_orders,
    ROUND(SUM(o.revenue), 2)                         AS total_revenue,
    ROUND(AVG(o.revenue), 2)                         AS avg_order_value,
    COUNT(DISTINCT o.customer)                       AS unique_customers,
    ROUND(AVG(qi.defect_rate) * 100, 3)              AS avg_defect_rate_pct,
    SUM(CASE WHEN qi.passed = 0 THEN 1 ELSE 0 END)  AS failed_batches
FROM orders o
LEFT JOIN quality_inspections qi ON o.order_id = qi.order_id
WHERE o.status = 'Delivered'
GROUP BY o.plant
ORDER BY total_revenue DESC;
