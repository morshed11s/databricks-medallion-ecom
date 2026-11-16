# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Shared configuration
# MAGIC Other notebooks load this with `%run ./00_config`. Change the **catalog** widget, or pass a `catalog` job parameter, to point at a different Unity Catalog catalog.
# MAGIC
# MAGIC **Naming standard**
# MAGIC | Object | Pattern | Example |
# MAGIC |---|---|---|
# MAGIC | Catalog | existing writable trial catalog | `workspace` |
# MAGIC | Schema | `<domain>_<purpose>` | `ecom_poc` |
# MAGIC | Landing volume | `raw_files` (managed) | `/Volumes/workspace/ecom_poc/raw_files/` |
# MAGIC | Tables | `<layer>_<entity>` | `bronze_orders`, `silver_orders`, `gold_daily_sales` |
# MAGIC | Rejected / duplicate outcomes | `silver_<entity>_quarantine`, `silver_<entity>_duplicates` | `silver_orders_quarantine` |
# MAGIC | Operational / audit tables | `ops_<purpose>` | `ops_landing_file_log` |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = "ecom_poc"
VOLUME = "raw_files"
FQ_SCHEMA = f"`{CATALOG}`.`{SCHEMA}`"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SOURCE_FILES = {"orders": f"{RAW_PATH}/orders.csv", "transactions": f"{RAW_PATH}/transactions.csv"}


def t(name: str) -> str:
    """Fully-qualified, quoted table name for SQL text."""
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


def tn(name: str) -> str:
    """Fully-qualified table name for DataFrame APIs (spark.table / saveAsTable)."""
    return f"{CATALOG}.{SCHEMA}.{name}"


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "\\'")


def document_table(name: str, table_comment: str, column_comments: dict) -> None:
    """Write table and column descriptions to Unity Catalog (shown in Catalog Explorer)."""
    spark.sql(f"COMMENT ON TABLE {t(name)} IS '{_esc(table_comment)}'")
    for col, comment in column_comments.items():
        spark.sql(f"ALTER TABLE {t(name)} ALTER COLUMN `{col}` COMMENT '{_esc(comment)}'")


print(f"catalog={CATALOG}  schema={SCHEMA}  raw_path={RAW_PATH}")
