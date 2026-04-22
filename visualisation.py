"""
visualisation.py
─────────────────────────────────────────────────────────────────────────────
MI Dashboard & Analytics Charts
Data Infrastructure & Analytics Engineering
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import warnings
warnings.filterwarnings("ignore")
import os

os.makedirs("charts", exist_ok=True)

BLUE   = "#1F4E79"
LBLUE  = "#2E75B6"
GREEN  = "#1E8449"
AMBER  = "#D4AC0D"
RED    = "#C0392B"
LGRAY  = "#F2F2F2"
MGRAY  = "#CCCCCC"

# ── LOAD ALL OUTPUTS ─────────────────────────────────────────────────────────
orders      = pd.read_csv("data/orders.csv")
inspections = pd.read_csv("data/quality_inspections.csv")
oee         = pd.read_csv("output/nosql_oee_by_machine.csv")
monthly     = pd.read_csv("output/q3_monthly_trend.csv")
defects     = pd.read_csv("output/q4_defect_analysis.csv")
rolling     = pd.read_csv("output/dsa_rolling_revenue.csv", parse_dates=["date"])
dep         = pd.read_csv("output/dsa_supplier_dependency.csv")

delivered = orders[orders["status"] == "Delivered"]

fig = plt.figure(figsize=(22, 15))
fig.patch.set_facecolor("#F7F9FB")
fig.suptitle("Data Infrastructure & Analytics Engineering — MI Dashboard",
             fontsize=17, fontweight="bold", color=BLUE, y=0.98)

# ── PANEL 1: Monthly Revenue Trend ──────────────────────────────────────────
ax1 = fig.add_subplot(2, 3, 1)
monthly["month_label"] = pd.to_datetime(monthly["month"]).dt.strftime("%b-%y")
colors = [GREEN if v >= 0 else RED
          for v in monthly["mom_growth_pct"].fillna(0)]
ax1b = ax1.twinx()
ax1.bar(range(len(monthly)), monthly["revenue"] / 1e6,
        color=LBLUE, alpha=0.65, label="Revenue (₹Mn)", zorder=2)
ax1b.plot(range(len(monthly)), monthly["mom_growth_pct"].fillna(0),
          color=AMBER, linewidth=2, marker="o", markersize=4, label="MoM Growth %")
ax1b.axhline(0, color=MGRAY, linewidth=0.8, linestyle="--")
ax1.set_title("Monthly Revenue & MoM Growth", fontweight="bold", color=BLUE, fontsize=11)
ax1.set_xticks(range(len(monthly))[::3])
ax1.set_xticklabels(monthly["month_label"].iloc[::3], rotation=45, fontsize=8)
ax1.set_ylabel("Revenue (₹ Mn)", fontsize=9)
ax1b.set_ylabel("MoM Growth %", fontsize=9, color=AMBER)
ax1.set_facecolor(LGRAY)
ax1.grid(axis="y", alpha=0.3, zorder=1)

# ── PANEL 2: OEE by Machine ──────────────────────────────────────────────────
ax2 = fig.add_subplot(2, 3, 2)
oee_sorted = oee.sort_values("avg_oee")
bar_colors = [RED if r == "RED" else AMBER if r == "AMBER" else GREEN
              for r in oee_sorted["oee_rag"]]
bars = ax2.barh(oee_sorted["machine_id"], oee_sorted["avg_oee"] * 100,
                color=bar_colors, edgecolor="white", height=0.6)
ax2.axvline(85, color=GREEN, linewidth=1.5, linestyle="--", label="World Class (85%)")
ax2.axvline(70, color=RED,   linewidth=1.5, linestyle="--", label="Threshold (70%)")
ax2.set_title("OEE Score by Machine (RAG)", fontweight="bold", color=BLUE, fontsize=11)
ax2.set_xlabel("OEE %", fontsize=9)
ax2.set_xlim(50, 100)
ax2.legend(fontsize=8)
ax2.set_facecolor(LGRAY)
ax2.grid(axis="x", alpha=0.3)
ax2.tick_params(axis="y", labelsize=8)

# ── PANEL 3: Defect Rate by Product ──────────────────────────────────────────
ax3 = fig.add_subplot(2, 3, 3)
prod_defect = (inspections.groupby("product")
               .apply(lambda x: x["defects_found"].sum() / x["batch_size"].sum() * 100)
               .reset_index(name="defect_pct")
               .sort_values("defect_pct", ascending=True))
dc = [RED if v >= 3 else AMBER if v >= 1 else GREEN for v in prod_defect["defect_pct"]]
ax3.barh(prod_defect["product"], prod_defect["defect_pct"],
         color=dc, edgecolor="white", height=0.6)
ax3.axvline(3, color=RED,   linewidth=1.5, linestyle="--", label="Red (3%)")
ax3.axvline(1, color=AMBER, linewidth=1.5, linestyle="--", label="Amber (1%)")
ax3.set_title("Defect Rate % by Product", fontweight="bold", color=BLUE, fontsize=11)
ax3.set_xlabel("Defect Rate %", fontsize=9)
ax3.legend(fontsize=8)
ax3.set_facecolor(LGRAY)
ax3.grid(axis="x", alpha=0.3)
ax3.tick_params(axis="y", labelsize=8)

# ── PANEL 4: 30-Day Rolling Revenue ──────────────────────────────────────────
ax4 = fig.add_subplot(2, 3, 4)
ax4.fill_between(rolling["date"], rolling["rolling_30d_revenue_mn"],
                 alpha=0.25, color=BLUE)
ax4.plot(rolling["date"], rolling["rolling_30d_revenue_mn"],
         color=BLUE, linewidth=1.8, label="30-Day Rolling Revenue")
mean_val = rolling["rolling_30d_revenue_mn"].mean()
ax4.axhline(mean_val, color=AMBER, linewidth=1.5, linestyle="--",
            label=f"Mean: ₹{mean_val:.1f}Mn")
ax4.set_title("30-Day Rolling Revenue (Sliding Window)", fontweight="bold",
              color=BLUE, fontsize=11)
ax4.set_ylabel("₹ Mn", fontsize=9)
ax4.legend(fontsize=8)
ax4.set_facecolor(LGRAY)
ax4.grid(alpha=0.3)
ax4.tick_params(axis="x", rotation=30, labelsize=8)

# ── PANEL 5: Revenue by Customer ─────────────────────────────────────────────
ax5 = fig.add_subplot(2, 3, 5)
cust_rev = (delivered.groupby("customer")["revenue"]
            .sum().sort_values(ascending=True) / 1e6)
bar_c = [BLUE if i >= len(cust_rev) - 3 else LBLUE
         for i in range(len(cust_rev))]
ax5.barh(cust_rev.index, cust_rev.values, color=bar_c, edgecolor="white", height=0.6)
ax5.set_title("Revenue by Customer (₹ Mn)", fontweight="bold", color=BLUE, fontsize=11)
ax5.set_xlabel("Revenue (₹ Mn)", fontsize=9)
ax5.set_facecolor(LGRAY)
ax5.grid(axis="x", alpha=0.3)
ax5.tick_params(axis="y", labelsize=8)

# ── PANEL 6: KPI Summary ─────────────────────────────────────────────────────
ax6 = fig.add_subplot(2, 3, 6)
ax6.axis("off")

total_rev   = delivered["revenue"].sum() / 1e7
avg_oee_val = oee["avg_oee"].mean() * 100
avg_defect  = inspections["defect_rate"].mean() * 100
total_orders = len(delivered)
red_machines = (oee["oee_rag"] == "RED").sum()
avg_lead    = (pd.to_datetime(delivered["delivery_date"]) -
               pd.to_datetime(delivered["order_date"])).dt.days.mean()

kpis = [
    ("Total Revenue (Delivered)", f"₹{total_rev:.1f} Cr",   GREEN),
    ("Total Delivered Orders",    f"{total_orders:,}",       BLUE),
    ("Avg OEE Score",             f"{avg_oee_val:.1f}%",     GREEN if avg_oee_val >= 85 else AMBER),
    ("Avg Defect Rate",           f"{avg_defect:.2f}%",      GREEN if avg_defect < 1 else AMBER),
    ("Machines Below 70% OEE",   f"{red_machines}",          RED if red_machines > 2 else AMBER),
    ("Avg Lead Time",             f"{avg_lead:.1f} days",    GREEN if avg_lead <= 15 else AMBER),
]

ax6.set_title("Executive KPI Summary", fontweight="bold", color=BLUE, fontsize=11)
y = 0.90
for label, value, color in kpis:
    ax6.text(0.05, y, label, transform=ax6.transAxes,
             fontsize=10, color="#333333", va="top")
    ax6.text(0.95, y, value, transform=ax6.transAxes,
             fontsize=11, fontweight="bold", color=color, va="top", ha="right")
    ax6.plot([0.05, 0.95], [y - 0.04, y - 0.04],
             color=MGRAY, linewidth=0.5, transform=ax6.transAxes)
    y -= 0.14

rect = mpatches.FancyBboxPatch(
    (0.02, 0.02), 0.96, 0.96,
    boxstyle="round,pad=0.01",
    linewidth=2, edgecolor=BLUE, facecolor="white",
    transform=ax6.transAxes, zorder=0
)
ax6.add_patch(rect)

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig("charts/mi_dashboard.png", dpi=150,
            bbox_inches="tight", facecolor=fig.get_facecolor())
plt.close()
print("MI Dashboard saved → charts/mi_dashboard.png")
