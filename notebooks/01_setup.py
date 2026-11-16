# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Workspace setup: schema and landing volume
# MAGIC This creates the `ecom_poc` schema and the managed `raw_files` volume in the chosen catalog. It is idempotent, so running it again is safe.
# MAGIC The equivalent plain SQL is in `sql/00_setup.sql`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

statements = [
    f"CREATE SCHEMA IF NOT EXISTS {FQ_SCHEMA} "
    f"COMMENT 'E-commerce medallion PoC: bronze/silver/gold tables for synthetic orders and payments'",
    f"CREATE VOLUME IF NOT EXISTS {FQ_SCHEMA}.`{VOLUME}` "
    f"COMMENT 'Landing zone: raw, unchanged source CSV extracts (orders.csv, transactions.csv)'",
]
for s in statements:
    print(s + ";")
    spark.sql(s)

# COMMAND ----------

display(spark.sql(f"DESCRIBE SCHEMA EXTENDED {FQ_SCHEMA}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE VOLUME {FQ_SCHEMA}.`{VOLUME}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** upload `orders.csv` and `transactions.csv` with **New → Add or upload data → Upload files to a volume**.
# MAGIC Choose this catalog, then `ecom_poc`, then `raw_files`. After that, run `02_landing_checks`.

# COMMAND ----------

print("Upload target:", RAW_PATH + "/")
