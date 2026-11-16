# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Bronze: lossless ingestion
# MAGIC Loads each landed CSV into its own Delta table: **`bronze_orders`** and **`bronze_transactions`**.
# MAGIC
# MAGIC **Design**
# MAGIC - **All source columns are STRING.** The schema is explicit and nothing is inferred, so values like `"two"`, `"12,50"`, `"2026-02-30"` and `" completed "` arrive exactly as sent.
# MAGIC - **Why keep malformed data in Bronze?** Bronze is the evidence layer. Rejecting rows while loading would lose them for good: you couldn't count them, explain them, send them back to the source system, or reprocess them after a rule changes. Bronze answers "what did we receive?". Silver decides "what do we trust?", and records every rejection with a reason.
# MAGIC - **Idempotent loads.** `_batch_id` comes from the file's SHA-256 content hash. The write uses Delta `replaceWhere _batch_id = …`, so re-running with the same file **replaces** that batch instead of appending it again.
# MAGIC
# MAGIC **Audit columns**
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `_ingested_at` | UTC timestamp of the load run. Shows when the data entered the lakehouse. |
# MAGIC | `_source_file` | Full volume path of the CSV the row came from (lineage back to the landing zone). |
# MAGIC | `_source_line` | 1-based data-row number in that file (the header is not counted). Points to the exact raw line and gives a stable order. |
# MAGIC | `_batch_id` | `<file>:<first 16 hex chars of the file's sha256>`. The same content always gets the same ID, which is the key for idempotent reloads and reconciliation. |
# MAGIC | `_row_hash` | SHA-256 of all source columns, with a separator and a null marker. Equal hashes mean exact duplicate rows. Also used to spot changes. |
# MAGIC | `_bronze_row_id` | `<_batch_id>#<_source_line>`. A unique, stable row identifier kept through Silver (accepted, quarantined and duplicate outcomes). |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import hashlib
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType

DATASETS = {
    "orders": ["order_id", "customer_id", "order_date", "category", "quantity",
               "unit_price", "order_status", "country"],
    "transactions": ["txn_id", "order_id", "payment_method", "amount", "currency",
                     "txn_status", "txn_ts"],
}

COLUMN_COMMENTS = {
    "_ingested_at": "UTC timestamp of the load run that wrote this row",
    "_source_file": "Full Unity Catalog volume path of the source CSV",
    "_source_line": "1-based data-row number in the source file (header excluded)",
    "_batch_id": "<file>:<sha256 prefix> of the source file content; same content = same batch (idempotency key)",
    "_row_hash": "sha256 of all source columns; equal hash = exact duplicate row",
    "_bronze_row_id": "Unique stable row id <_batch_id>#<_source_line>, carried into Silver outcomes",
}


def file_sha256_and_rows(path):
    h, lines = hashlib.sha256(), 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 << 20), b""):
            h.update(chunk)
            lines += chunk.count(b"\n")
    return h.hexdigest(), lines - 1   # minus header

# COMMAND ----------

def ensure_bronze_table(name, cols):
    col_ddl = ",\n  ".join(f"`{c}` STRING COMMENT 'Raw source value of {c}, unparsed'" for c in cols)
    audit_ddl = ",\n  ".join([
        f"`_ingested_at` TIMESTAMP COMMENT '{COLUMN_COMMENTS['_ingested_at']}'",
        f"`_source_file` STRING COMMENT '{COLUMN_COMMENTS['_source_file']}'",
        f"`_source_line` BIGINT COMMENT '{COLUMN_COMMENTS['_source_line']}'",
        f"`_batch_id` STRING COMMENT '{COLUMN_COMMENTS['_batch_id']}'",
        f"`_row_hash` STRING COMMENT '{COLUMN_COMMENTS['_row_hash']}'",
        f"`_bronze_row_id` STRING COMMENT '{COLUMN_COMMENTS['_bronze_row_id']}'",
    ])
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {t(name)} (
          {col_ddl},
          {audit_ddl}
        ) USING DELTA
        COMMENT 'Bronze: lossless raw copy of {name.replace("bronze_", "")}.csv; all source fields as STRING plus audit metadata. Grain: one row per source data row.'
    """)


def load_bronze(dataset):
    cols = DATASETS[dataset]
    table = f"bronze_{dataset}"
    path = SOURCE_FILES[dataset]
    file_name = path.rsplit("/", 1)[-1]

    sha, source_rows = file_sha256_and_rows(path)
    batch_id = f"{file_name}:{sha[:16]}"

    schema = StructType([StructField(c, StringType(), True) for c in cols])
    raw = (spark.read.format("csv")
           .schema(schema)                      # explicit, all STRING
           .option("header", "true")
           .option("inferSchema", "false")
           .option("enforceSchema", "false")    # header must match the declared column names
           .option("mode", "FAILFAST")          # a structurally broken line fails the load (no silent drops)
           .option("ignoreLeadingWhiteSpace", "false")
           .option("ignoreTrailingWhiteSpace", "false")
           .option("quote", '"').option("escape", '"')
           .load(path)
           .withColumn("_source_file", F.col("_metadata.file_path"))
           .withColumn("_split_start", F.col("_metadata.file_block_start"))
           .withColumn("_pos_in_split", F.monotonically_increasing_id()))

    # Stable file order: order by the byte offset of the split, then by position inside the split.
    w = Window.orderBy("_split_start", "_pos_in_split")
    null_marker = F.lit("␀")   # distinguishes NULL from an empty string in the hash
    bronze = (raw
              .withColumn("_source_line", F.row_number().over(w).cast("bigint"))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_row_hash", F.sha2(F.concat_ws("␟", *[F.coalesce(F.col(c), null_marker) for c in cols]), 256))
              .withColumn("_bronze_row_id", F.concat_ws("#", F.col("_batch_id"), F.col("_source_line").cast("string")))
              .select(*cols, "_ingested_at", "_source_file", "_source_line", "_batch_id", "_row_hash", "_bronze_row_id"))

    ensure_bronze_table(table, cols)
    (bronze.write.format("delta")
     .mode("overwrite")
     .option("replaceWhere", f"_batch_id = '{batch_id}'")   # idempotent: replace only this batch
     .saveAsTable(f"{CATALOG}.{SCHEMA}.{table}"))

    bronze_rows = spark.table(f"{CATALOG}.{SCHEMA}.{table}").where(F.col("_batch_id") == batch_id).count()
    total_rows = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
    return dict(dataset=dataset, table=table, source_file=path, batch_id=batch_id, source_sha256=sha,
                source_data_rows=source_rows, bronze_rows_for_batch=bronze_rows, bronze_rows_total=total_rows,
                lossless=(source_rows == bronze_rows))

# COMMAND ----------

results = [load_bronze(d) for d in DATASETS]

log_schema = ("dataset string, table string, source_file string, batch_id string, source_sha256 string, "
              "source_data_rows long, bronze_rows_for_batch long, bronze_rows_total long, lossless boolean")
log_df = spark.createDataFrame([tuple(r[k] for k in [f.split()[0] for f in log_schema.split(", ")]) for r in results],
                               log_schema).withColumn("loaded_at", F.current_timestamp())
spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('ops_bronze_load_log')} ({log_schema}, loaded_at timestamp)
              COMMENT 'One row per Bronze load run and dataset: source vs Bronze row-count reconciliation'""")
log_df.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.ops_bronze_load_log")
display(log_df)

for r in results:
    assert r["lossless"], f"{r['table']}: source {r['source_data_rows']} != bronze {r['bronze_rows_for_batch']}"
print("✅ Lossless: source data-row count equals Bronze row count for every dataset.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidence: schema, sample rows and audit data

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {t('bronze_orders')}"))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_orders").orderBy("_source_line").limit(10))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_transactions").orderBy("_source_line").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidence: malformed values are kept as they arrived (examples)

# COMMAND ----------

display(spark.sql(f"""
SELECT _bronze_row_id, quantity, unit_price, order_date, order_status, country
FROM {t('bronze_orders')}
WHERE try_cast(quantity AS INT) IS NULL OR try_cast(unit_price AS DECIMAL(12,2)) IS NULL
   OR try_to_date(order_date, 'yyyy-MM-dd') IS NULL OR order_status <> upper(trim(order_status))
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidence: idempotency
# MAGIC Run this notebook a **second time**. The counts above stay at exactly 1,000,000 per table, and `DESCRIBE HISTORY` shows a second `WRITE` with `replaceWhere` (mode Overwrite), not an append.

# COMMAND ----------

display(spark.sql(f"""
SELECT 'bronze_orders' AS table_name, _batch_id, COUNT(*) AS rows, COUNT(DISTINCT _bronze_row_id) AS distinct_row_ids
FROM {t('bronze_orders')} GROUP BY _batch_id
UNION ALL
SELECT 'bronze_transactions', _batch_id, COUNT(*), COUNT(DISTINCT _bronze_row_id)
FROM {t('bronze_transactions')} GROUP BY _batch_id
"""))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {t('bronze_orders')}")
        .select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))
