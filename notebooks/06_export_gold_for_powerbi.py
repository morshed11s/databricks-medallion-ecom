# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · (Fallback only) Export Gold marts for Power BI
# MAGIC Run this **only if** Power BI Desktop cannot use the native Databricks connector (authentication or network restriction).
# MAGIC
# MAGIC - It exports the **four Gold marts only**. No Bronze or Silver data, and not the order-level fact table, so the files stay small and contain only aggregated synthetic data.
# MAGIC - Files are written to the managed volume `exports`. Download them from Catalog Explorer into `powerbi/gold_exports/`, the only CSV path `.gitignore` allows.
# MAGIC - This notebook is not part of the Lakeflow Job.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ_SCHEMA}.`exports` "
          "COMMENT 'Small Gold-only CSV exports for the Power BI fallback'")
EXPORT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/exports"

MARTS = ["gold_daily_sales", "gold_revenue_breakdown", "gold_payment_success", "gold_top_customers"]
summary = []
for name in MARTS:
    pdf = spark.table(tn(name)).toPandas()
    path = f"{EXPORT_PATH}/{name}.csv"
    pdf.to_csv(path, index=False)
    revenue = float(pdf["revenue_usd"].sum()) if "revenue_usd" in pdf else None
    summary.append((name, len(pdf), path, revenue))

display(spark.createDataFrame(summary, "mart string, rows long, path string, revenue_usd_sum double"))

# COMMAND ----------

# MAGIC %md
# MAGIC `revenue_usd_sum` must be the same for `gold_daily_sales`, `gold_revenue_breakdown` and `gold_top_customers`. That proves the export kept every row.
