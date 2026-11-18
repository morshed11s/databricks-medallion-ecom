# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}` "
              f"COMMENT 'E-commerce medallion PoC catalog'")
except Exception as e:
    raise RuntimeError(
        f"Could not create catalog {CATALOG}. Create it in Catalog Explorer "
        f"(Catalog > + > Create catalog, type Standard, default storage), then re-run this notebook."
    ) from e

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

dbutils.widgets.text("copy_from", "/Volumes/workspace/ecom_poc/raw_files", "Copy raw CSVs from (blank = skip)")
COPY_FROM = dbutils.widgets.get("copy_from").strip().rstrip("/")


def exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


for f in ["orders.csv", "transactions.csv"]:
    target = f"{RAW_PATH}/{f}"
    source = f"{COPY_FROM}/{f}" if COPY_FROM else ""
    if exists(target):
        print(f"{target} already present")
    elif source and source != target and exists(source):
        dbutils.fs.cp(source, target)
        print(f"copied {source} -> {target}")
    else:
        print(f"{target} missing: upload {f} to {RAW_PATH}/")

# COMMAND ----------

display(spark.sql(f"DESCRIBE CATALOG EXTENDED `{CATALOG}`"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE SCHEMA EXTENDED {FQ_SCHEMA}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE VOLUME {FQ_SCHEMA}.`{VOLUME}`"))

# COMMAND ----------

display(dbutils.fs.ls(RAW_PATH))
