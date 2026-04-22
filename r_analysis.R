# =============================================================================
# r_analysis.R
# Data Infrastructure & Analytics Engineering
# Author: Tauseef Ahmad Khan
# Statistical analysis, data wrangling, and visualisation in R
# Run: Rscript r_analysis.R
# =============================================================================

library(dplyr)
library(ggplot2)
library(tidyr)
library(readr)
library(scales)
library(lubridate)

cat("=================================================================\n")
cat("  R STATISTICAL ANALYSIS ENGINE\n")
cat("=================================================================\n\n")

# ── LOAD DATA ────────────────────────────────────────────────────────────────
cat("[1/5] Loading data...\n")
orders      <- read_csv("data/orders.csv",      show_col_types = FALSE)
suppliers   <- read_csv("data/suppliers.csv",   show_col_types = FALSE)
inspections <- read_csv("data/quality_inspections.csv", show_col_types = FALSE)

orders$order_date    <- as.Date(orders$order_date)
orders$delivery_date <- as.Date(orders$delivery_date)
orders$lead_days     <- as.numeric(orders$delivery_date - orders$order_date)

cat(sprintf("  Orders loaded      : %d rows\n", nrow(orders)))
cat(sprintf("  Suppliers loaded   : %d rows\n", nrow(suppliers)))
cat(sprintf("  Inspections loaded : %d rows\n", nrow(inspections)))

# ── STATISTICAL SUMMARY ──────────────────────────────────────────────────────
cat("\n[2/5] Descriptive statistics...\n")

delivered <- orders %>% filter(status == "Delivered")

cat("\n  Revenue Summary:\n")
print(summary(delivered$revenue))

cat("\n  Lead Time Summary (days):\n")
print(summary(delivered$lead_days))

# ── ANALYSIS 1: SUPPLIER QUALITY vs REVENUE CORRELATION ─────────────────────
cat("\n[3/5] Supplier quality correlation analysis...\n")

supplier_agg <- orders %>%
  filter(status == "Delivered") %>%
  group_by(supplier_id) %>%
  summarise(
    total_revenue  = sum(revenue),
    order_count    = n(),
    avg_lead_days  = mean(lead_days)
  ) %>%
  inner_join(suppliers, by = "supplier_id")

cor_quality_revenue <- cor(supplier_agg$quality_score,
                           supplier_agg$total_revenue,
                           use = "complete.obs")
cor_ontime_revenue  <- cor(supplier_agg$on_time_rate,
                           supplier_agg$total_revenue,
                           use = "complete.obs")

cat(sprintf("  Quality Score vs Revenue Correlation : r = %.3f\n", cor_quality_revenue))
cat(sprintf("  On-Time Rate  vs Revenue Correlation : r = %.3f\n", cor_ontime_revenue))

# ── ANALYSIS 2: DEFECT RATE DISTRIBUTION BY PRODUCT ─────────────────────────
cat("\n[4/5] Defect rate analysis...\n")

defect_summary <- inspections %>%
  group_by(product) %>%
  summarise(
    mean_defect_rate  = mean(defect_rate, na.rm = TRUE),
    median_defect_rate = median(defect_rate, na.rm = TRUE),
    sd_defect_rate    = sd(defect_rate, na.rm = TRUE),
    total_defects     = sum(defects_found),
    total_units       = sum(batch_size),
    overall_rate      = sum(defects_found) / sum(batch_size)
  ) %>%
  arrange(desc(mean_defect_rate))

cat("\n  Defect Rate by Product:\n")
print(defect_summary %>%
        mutate(across(where(is.numeric), ~round(.x, 4))) %>%
        as.data.frame())

# ── ANALYSIS 3: MONTHLY REVENUE TREND + LINEAR REGRESSION ───────────────────
cat("\n[5/5] Revenue trend regression...\n")

monthly_revenue <- delivered %>%
  mutate(month = floor_date(order_date, "month")) %>%
  group_by(month) %>%
  summarise(revenue = sum(revenue)) %>%
  mutate(t = row_number())

model <- lm(revenue ~ t, data = monthly_revenue)
cat("\n  Linear Trend Model: revenue ~ time\n")
print(summary(model)$coefficients)
cat(sprintf("\n  R-squared          : %.4f\n", summary(model)$r.squared))
cat(sprintf("  Monthly trend      : %+.0f per month\n",
            coef(model)["t"]))

# ── EXPORT RESULTS ───────────────────────────────────────────────────────────
dir.create("output", showWarnings = FALSE)
write_csv(defect_summary,  "output/r_defect_summary.csv")
write_csv(supplier_agg %>%
            select(supplier_id, tier, country, quality_score,
                   on_time_rate, total_revenue, order_count),
          "output/r_supplier_analysis.csv")
write_csv(monthly_revenue, "output/r_monthly_trend.csv")

cat("\n  R analysis outputs saved to output/\n")
cat("\n=================================================================\n")
cat("  R Analysis complete.\n")
cat("=================================================================\n")
