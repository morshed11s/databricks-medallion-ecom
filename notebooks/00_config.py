# Databricks notebook source
dbutils.widgets.text("catalog", "ecom_catalog", "Unity Catalog catalog")
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = "ecom_poc"
VOLUME = "raw_files"
FQ_SCHEMA = f"`{CATALOG}`.`{SCHEMA}`"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SOURCE_FILES = {"orders": f"{RAW_PATH}/orders.csv", "transactions": f"{RAW_PATH}/transactions.csv"}


def t(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


def tn(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "\\'")


def document_table(name: str, table_comment: str, column_comments: dict) -> None:
    spark.sql(f"COMMENT ON TABLE {t(name)} IS '{_esc(table_comment)}'")
    for col, comment in column_comments.items():
        spark.sql(f"ALTER TABLE {t(name)} ALTER COLUMN `{col}` COMMENT '{_esc(comment)}'")


print(f"catalog={CATALOG}  schema={SCHEMA}  raw_path={RAW_PATH}")
