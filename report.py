import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

GOLD_DIR   = Path("/data/pipeline/gold")
REPORT_DIR = Path("/data/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

images = []

def save_plot(df, x, y, title, fname):
    plt.figure()
    df.plot(x=x, y=y, kind="line" if df[x].is_monotonic_increasing else "bar")
    plt.title(title)
    plt.tight_layout()
    out = REPORT_DIR / fname
    plt.savefig(out)
    plt.close()
    images.append(out.name)

# 1) Daily revenue
try:
    daily = pd.read_parquet(GOLD_DIR / "daily_revenue")
    daily = daily.sort_values("date")
    save_plot(daily, "date", "gross_revenue", "Daily Revenue", "daily_revenue.png")
except Exception as e:
    print("Skip daily_revenue:", e)

# 2) Cancellation rate by source
try:
    canc = pd.read_parquet(GOLD_DIR / "cancellation_rate_by_source")
    canc = canc.sort_values("cancellation_rate_pct", ascending=False)
    save_plot(canc, "source", "cancellation_rate_pct", "Cancellation Rate by Source (%)", "cancel_rate.png")
except Exception as e:
    print("Skip cancellation_rate_by_source:", e)

# 3) Collection rate (table top)
coll_html = ""
try:
    coll = pd.read_parquet(GOLD_DIR / "collection_rate")
    coll = coll.sort_values("collection_rate", ascending=False).head(20)
    coll_html = coll.to_html(index=False)
except Exception as e:
    print("Skip collection_rate:", e)

# 4) Customer value enriched (avg sentiment)
cust_html = ""
try:
    cust = pd.read_parquet(GOLD_DIR / "customer_value_enriched")
    cust = cust.sort_values("revenue_sum", ascending=False).head(20)
    cust_html = cust.to_html(index=False)
except Exception as e:
    print("Skip customer_value_enriched:", e)

# Compose HTML
html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>GlobalStay – KPI Report</title>
<style>
 body {{ font-family: Arial, sans-serif; margin: 24px; }}
 h1, h2 {{ color: #333; }}
 img {{ max-width: 800px; display:block; margin: 16px 0; }}
 table {{ border-collapse: collapse; margin: 16px 0; }}
 th, td {{ border: 1px solid #ccc; padding: 6px 10px; }}
</style>
</head>
<body>
<h1>GlobalStay – KPI Report</h1>
<p>Questo report riassume i KPI della pipeline (Gold) e un ML semplice di sentiment sulle recensioni.</p>

<h2>Daily Revenue</h2>
{"<img src='daily_revenue.png' />" if "daily_revenue.png" in images else "<p>N/D</p>"}

<h2>Cancellation Rate by Source</h2>
{"<img src='cancel_rate.png' />" if "cancel_rate.png" in images else "<p>N/D</p>"}

<h2>Collection Rate (Top)</h2>
{coll_html or "<p>N/D</p>"}

<h2>Customer Value (Top, con avg_sentiment)</h2>
{cust_html or "<p>N/D</p>"}

<p style="margin-top:40px;font-size:12px;color:#666">Generato automaticamente.</p>
</body>
</html>
"""

out_html = REPORT_DIR / "report.html"
out_html.write_text(html, encoding="utf-8")
print("Report scritto in:", out_html)
