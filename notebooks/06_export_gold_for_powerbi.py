# Databricks notebook source
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
