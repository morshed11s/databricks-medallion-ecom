# Databricks notebook source
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

print("Upload target:", RAW_PATH + "/")
