# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver: cleaning, validation, deduplication and reconciliation
# MAGIC
# MAGIC Every Bronze row ends up in **exactly one** outcome:
# MAGIC
# MAGIC | Outcome | Orders | Transactions |
# MAGIC |---|---|---|
# MAGIC | Accepted (trusted, typed, one row per key) | `silver_orders` | `silver_transactions` |
# MAGIC | Quarantined (failed at least one rule, with `rejection_reasons`) | `silver_orders_quarantine` | `silver_transactions_quarantine` |
# MAGIC | Duplicate loser (valid, but another row won the key) | `silver_orders_duplicates` | `silver_transactions_duplicates` |
# MAGIC
# MAGIC **Processing precedence**
# MAGIC 1. **Standardise:** trim spaces, upper-case codes, and treat `''`, `NULL` and `None` as missing. These cosmetic problems are fixed, not rejected.
# MAGIC 2. **Safe parsing:** `try_cast` and `try_to_timestamp` with strict formats. A string that can't be parsed becomes `NULL` in the parsed column, the original string is kept, and the row fails a *type* rule. It is never cast to a wrong value and never fails the job.
# MAGIC 3. **Validate:** every rule is evaluated and **all** failures are recorded in `rejection_reasons`. Any failure means quarantine.
# MAGIC 4. **Deduplicate valid rows only:** one winner per business key, decided deterministically. All other rows are duplicate losers.
# MAGIC
# MAGIC Because validation runs before deduplication, a row that is both invalid and a duplicate is counted **once, in quarantine**. The three outcomes therefore split Bronze exactly:
# MAGIC **Bronze = accepted + quarantined + duplicate losers.** `_bronze_row_id` is kept in every outcome.
# MAGIC
# MAGIC **Keep-one rules**
# MAGIC - Orders (`order_id`): the **latest line in the extract** wins (highest `_source_line`). Later lines represent later versions of the order.
# MAGIC - Transactions (`txn_id`): the **latest `txn_ts`** wins, which is the latest status of the payment. Ties go to the highest `_source_line`.
# MAGIC - Each loser is labelled `exact_row_duplicate` (same `_row_hash` as the winner, so every raw field is identical) or `key_conflict` (same key, different content).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window

NULL_TOKENS = ["", "NULL", "NONE"]
DATE_RE = r"^\d{4}-\d{2}-\d{2}$"
TS_RE = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"

CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Beauty", "Sports", "Toys", "Grocery"]
ORDER_STATUSES = ["COMPLETED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED"]
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "CA", "AU", "BD", "NL", "ES"]
PAYMENT_METHODS = ["CARD", "PAYPAL", "WALLET", "BANK_TRANSFER", "COD"]
TXN_STATUSES = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]
FX_TO_USD = [("USD", "1.000000"), ("EUR", "1.086957"), ("GBP", "1.265823")]
QTY_MIN, QTY_MAX = 1, 100
PRICE_MAX = 100_000
AMOUNT_MAX = 1_000_000

ORDER_RAW = ["order_id", "customer_id", "order_date", "category", "quantity", "unit_price", "order_status", "country"]
TXN_RAW = ["txn_id", "order_id", "payment_method", "amount", "currency", "txn_status", "txn_ts"]
LINEAGE = ["_bronze_row_id", "_batch_id", "_source_file", "_source_line", "_row_hash", "_ingested_at"]


def clean(c):
    v = F.trim(F.col(c))
    return F.when(v.isNull() | F.upper(v).isin(NULL_TOKENS), F.lit(None).cast("string")).otherwise(v)


def reasons_col(rules):
    return F.filter(F.array(*[F.when(cond, F.lit(rid)) for rid, _, _, _, cond in rules]),
                    lambda x: x.isNotNull())


def missing_rules(fields):
    return [(f"missing_{f}", f, "mandatory", f"{f} must be present (not blank/NULL)", F.col(f"c_{f}").isNull())
            for f in fields]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data: supported currencies and FX rates

# COMMAND ----------

spark.createDataFrame(FX_TO_USD, "currency string, usd_per_unit string") \
    .select("currency", F.col("usd_per_unit").cast("decimal(12,6)").alias("usd_per_unit")) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tn("ref_fx_rates"))
document_table("ref_fx_rates",
               "Supported payment currencies and fixed PoC conversion rates to USD. Grain: one row per currency.",
               {"currency": "ISO 4217 code (supported set)", "usd_per_unit": "USD value of 1 unit of currency"})
display(spark.table(tn("ref_fx_rates")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders: standardise, parse, validate

# COMMAND ----------

bo = spark.table(tn("bronze_orders"))
o = (bo
     .withColumn("c_order_id", clean("order_id"))
     .withColumn("c_customer_id", clean("customer_id"))
     .withColumn("c_order_date", clean("order_date"))
     .withColumn("c_category", clean("category"))
     .withColumn("c_quantity", clean("quantity"))
     .withColumn("c_unit_price", clean("unit_price"))
     .withColumn("c_order_status", F.upper(clean("order_status")))
     .withColumn("c_country", F.upper(clean("country")))
     .withColumn("p_order_date", F.when(F.col("c_order_date").rlike(DATE_RE),
                                        F.expr("CAST(try_to_timestamp(c_order_date, 'yyyy-MM-dd') AS DATE)")))
     .withColumn("p_quantity", F.expr("try_cast(c_quantity AS INT)"))
     .withColumn("p_unit_price", F.expr("try_cast(c_unit_price AS DECIMAL(12,2))"))
     .withColumn("p_category", F.when(F.initcap("c_category").isin(CATEGORIES), F.initcap("c_category"))))

c = F.col
ORDER_RULES = missing_rules(ORDER_RAW) + [
    ("invalid_order_date", "order_date", "type", "real calendar date in yyyy-MM-dd format",
     c("c_order_date").isNotNull() & c("p_order_date").isNull()),
    ("future_order_date", "order_date", "range", "order_date not after the processing date",
     c("p_order_date") > F.current_date()),
    ("invalid_quantity", "quantity", "type", "whole number",
     c("c_quantity").isNotNull() & c("p_quantity").isNull()),
    ("quantity_out_of_range", "quantity", "range", f"{QTY_MIN} <= quantity <= {QTY_MAX}",
     c("p_quantity").isNotNull() & ~c("p_quantity").between(QTY_MIN, QTY_MAX)),
    ("invalid_unit_price", "unit_price", "type", "plain decimal number (no symbols / separators)",
     c("c_unit_price").isNotNull() & c("p_unit_price").isNull()),
    ("unit_price_out_of_range", "unit_price", "range", f"0 < unit_price <= {PRICE_MAX:,}",
     c("p_unit_price").isNotNull() & ((c("p_unit_price") <= 0) | (c("p_unit_price") > PRICE_MAX))),
    ("invalid_category", "category", "allowed values", ", ".join(CATEGORIES),
     c("c_category").isNotNull() & c("p_category").isNull()),
    ("invalid_order_status", "order_status", "allowed values", ", ".join(ORDER_STATUSES),
     c("c_order_status").isNotNull() & ~c("c_order_status").isin(ORDER_STATUSES)),
    ("invalid_country", "country", "allowed values", ", ".join(COUNTRIES),
     c("c_country").isNotNull() & ~c("c_country").isin(COUNTRIES)),
]
o = o.withColumn("rejection_reasons", reasons_col(ORDER_RULES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders: deterministic deduplication and outcome tables

# COMMAND ----------

def split_outcomes(df, key, order_cols):
    valid = df.where(F.size("rejection_reasons") == 0)
    w = Window.partitionBy(key).orderBy(*order_cols)
    ranked = (valid
              .withColumn("_rank", F.row_number().over(w))
              .withColumn("winner_bronze_row_id", F.first("_bronze_row_id").over(w))
              .withColumn("winner_row_hash", F.first("_row_hash").over(w)))
    losers = (ranked.where("_rank > 1")
              .withColumn("duplicate_type", F.when(c("_row_hash") == c("winner_row_hash"), "exact_row_duplicate")
                          .otherwise("key_conflict")))
    return ranked.where("_rank = 1"), losers


def write(df, name):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tn(name))


now = F.current_timestamp()
order_winners, order_losers = split_outcomes(o, "c_order_id", [c("_source_line").desc()])

silver_orders = order_winners.select(
    c("c_order_id").alias("order_id"),
    c("c_customer_id").alias("customer_id"),
    c("p_order_date").alias("order_date"),
    c("p_category").alias("category"),
    c("p_quantity").alias("quantity"),
    c("p_unit_price").alias("unit_price"),
    (c("p_quantity") * c("p_unit_price")).cast("decimal(14,2)").alias("order_amount_usd"),
    c("c_order_status").alias("order_status"),
    c("c_country").alias("country"),
    *LINEAGE, now.alias("_silver_processed_at"))
write(silver_orders, "silver_orders")

write(o.where(F.size("rejection_reasons") > 0)
       .select(*ORDER_RAW, "rejection_reasons", *LINEAGE, now.alias("_quarantined_at")),
      "silver_orders_quarantine")

write(order_losers.select(*ORDER_RAW, c("c_order_id").alias("dedup_key"), "duplicate_type",
                          "winner_bronze_row_id", F.lit("latest _source_line wins").alias("keep_rule"),
                          *LINEAGE, now.alias("_deduplicated_at")),
      "silver_orders_duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions: standardise, parse, validate (including the orphan check against accepted orders)

# COMMAND ----------

bt = spark.table(tn("bronze_transactions"))
known_orders = spark.table(tn("silver_orders")).select(c("order_id").alias("_known_order_id"))

x = (bt
     .withColumn("c_txn_id", clean("txn_id"))
     .withColumn("c_order_id", clean("order_id"))
     .withColumn("c_payment_method", F.upper(clean("payment_method")))
     .withColumn("c_amount", clean("amount"))
     .withColumn("c_currency", F.upper(clean("currency")))
     .withColumn("c_txn_status", F.upper(clean("txn_status")))
     .withColumn("c_txn_ts", clean("txn_ts"))
     .withColumn("p_amount", F.expr("try_cast(c_amount AS DECIMAL(14,2))"))
     .withColumn("p_txn_ts", F.when(c("c_txn_ts").rlike(TS_RE),
                                    F.expr("try_to_timestamp(c_txn_ts, 'yyyy-MM-dd HH:mm:ss')")))
     .join(F.broadcast(known_orders), c("c_order_id") == c("_known_order_id"), "left"))

SUPPORTED_CCY = [ccy for ccy, _ in FX_TO_USD]
TXN_RULES = missing_rules(TXN_RAW) + [
    ("invalid_amount", "amount", "type", "plain decimal number (no symbols / separators)",
     c("c_amount").isNotNull() & c("p_amount").isNull()),
    ("amount_out_of_range", "amount", "range", f"0 < amount <= {AMOUNT_MAX:,}",
     c("p_amount").isNotNull() & ((c("p_amount") <= 0) | (c("p_amount") > AMOUNT_MAX))),
    ("invalid_txn_ts", "txn_ts", "type", "real timestamp in yyyy-MM-dd HH:mm:ss format",
     c("c_txn_ts").isNotNull() & c("p_txn_ts").isNull()),
    ("future_txn_ts", "txn_ts", "range", "txn_ts not after the processing time",
     c("p_txn_ts") > F.current_timestamp()),
    ("unsupported_currency", "currency", "allowed values", ", ".join(SUPPORTED_CCY),
     c("c_currency").isNotNull() & ~c("c_currency").isin(SUPPORTED_CCY)),
    ("invalid_payment_method", "payment_method", "allowed values", ", ".join(PAYMENT_METHODS),
     c("c_payment_method").isNotNull() & ~c("c_payment_method").isin(PAYMENT_METHODS)),
    ("invalid_txn_status", "txn_status", "allowed values", ", ".join(TXN_STATUSES),
     c("c_txn_status").isNotNull() & ~c("c_txn_status").isin(TXN_STATUSES)),
    ("orphan_order_id", "order_id", "relationship", "order_id must exist in accepted silver_orders",
     c("c_order_id").isNotNull() & c("_known_order_id").isNull()),
]
x = x.withColumn("rejection_reasons", reasons_col(TXN_RULES))

txn_winners, txn_losers = split_outcomes(x, "c_txn_id", [c("p_txn_ts").desc(), c("_source_line").desc()])
fx = spark.table(tn("ref_fx_rates"))

silver_txns = (txn_winners
               .join(F.broadcast(fx), c("c_currency") == fx.currency, "left")
               .select(
                   c("c_txn_id").alias("txn_id"),
                   c("c_order_id").alias("order_id"),
                   c("c_payment_method").alias("payment_method"),
                   c("p_amount").alias("amount"),
                   c("c_currency").alias("currency"),
                   F.round(c("p_amount") * c("usd_per_unit"), 2).cast("decimal(14,2)").alias("amount_usd"),
                   c("c_txn_status").alias("txn_status"),
                   c("p_txn_ts").alias("txn_ts"),
                   F.to_date("p_txn_ts").alias("txn_date"),
                   *LINEAGE, now.alias("_silver_processed_at")))
write(silver_txns, "silver_transactions")

write(x.where(F.size("rejection_reasons") > 0)
       .select(*TXN_RAW, "rejection_reasons", *LINEAGE, now.alias("_quarantined_at")),
      "silver_transactions_quarantine")

write(txn_losers.select(*TXN_RAW, c("c_txn_id").alias("dedup_key"), "duplicate_type",
                        "winner_bronze_row_id", F.lit("latest txn_ts wins; tie -> latest _source_line").alias("keep_rule"),
                        *LINEAGE, now.alias("_deduplicated_at")),
      "silver_transactions_duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rules matrix (stored as `ref_dq_rules`)

# COMMAND ----------

rules_rows = [("orders", rid, fld, typ, desc) for rid, fld, typ, desc, _ in ORDER_RULES] + \
             [("transactions", rid, fld, typ, desc) for rid, fld, typ, desc, _ in TXN_RULES]
write(spark.createDataFrame(rules_rows, "dataset string, rejection_reason string, field string, rule_type string, rule string"),
      "ref_dq_rules")
display(spark.table(tn("ref_dq_rules")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table and column documentation

# COMMAND ----------

lineage_docs = {
    "_bronze_row_id": "Bronze row identifier (<batch>#<line>), traces this row to its raw source line",
    "_batch_id": "Source batch (<file>:<sha256 prefix>)",
    "_source_file": "Source CSV path in the landing volume",
    "_source_line": "1-based data-row number in the source file",
    "_row_hash": "sha256 of the raw source columns",
    "_ingested_at": "Bronze load timestamp",
}
document_table("silver_orders",
    "Silver orders: typed, validated, one row per order_id (latest extract line wins). Grain/key: order_id. Values in USD.",
    {"order_id": "Business key (unique)", "customer_id": "Customer identifier",
     "order_date": "Order date (validated, not in the future)", "category": "Product category (canonical case)",
     "quantity": f"Units ordered, {QTY_MIN}-{QTY_MAX}", "unit_price": "Unit price in USD (>0)",
     "order_amount_usd": "quantity * unit_price, USD (order value)",
     "order_status": "COMPLETED, SHIPPED, PENDING, CANCELLED or RETURNED", "country": "ISO-3166 alpha-2 country",
     "_silver_processed_at": "Silver run timestamp", **lineage_docs})
document_table("silver_transactions",
    "Silver payment transactions: typed, validated, referentially valid against silver_orders, one row per txn_id (latest txn_ts wins). Grain/key: txn_id.",
    {"txn_id": "Business key (unique)", "order_id": "FK to silver_orders.order_id (always resolvable)",
     "payment_method": "CARD, PAYPAL, WALLET, BANK_TRANSFER or COD", "amount": "Amount in the payment currency",
     "currency": "USD, EUR or GBP", "amount_usd": "amount converted with ref_fx_rates (rounded to cents)",
     "txn_status": "SUCCESS, FAILED, PENDING or REFUNDED (latest status)", "txn_ts": "Payment timestamp (UTC)",
     "txn_date": "Date part of txn_ts", "_silver_processed_at": "Silver run timestamp", **lineage_docs})
for ds in ("orders", "transactions"):
    document_table(f"silver_{ds}_quarantine",
        f"Rejected {ds} rows: raw Bronze values plus every failed rule in rejection_reasons. Grain: one row per rejected Bronze row (_bronze_row_id).",
        {"rejection_reasons": "Array of failed rule ids (see ref_dq_rules)", "_quarantined_at": "Silver run timestamp"})
    document_table(f"silver_{ds}_duplicates",
        f"Valid {ds} rows that lost deduplication on their business key. Grain: one row per losing Bronze row (_bronze_row_id).",
        {"dedup_key": "Business key shared with the winner", "duplicate_type": "exact_row_duplicate or key_conflict",
         "winner_bronze_row_id": "Bronze row id of the accepted winner", "keep_rule": "Deterministic keep-one rule applied"})
document_table("ref_dq_rules", "Business-rule matrix applied in Silver. Grain: dataset + rejection_reason.", {})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation: Bronze = accepted + quarantined + duplicate losers, with no overlap

# COMMAND ----------

def reconcile(ds):
    ids = lambda name: spark.table(tn(name)).select("_bronze_row_id")
    bronze = ids(f"bronze_{ds}")
    acc, qua, dup = ids(f"silver_{ds}"), ids(f"silver_{ds}_quarantine"), ids(f"silver_{ds}_duplicates")
    outcomes = acc.unionAll(qua).unionAll(dup)
    n_out = outcomes.count()
    return (ds, bronze.count(), acc.count(), qua.count(), dup.count(), n_out,
            n_out - outcomes.distinct().count(),
            bronze.join(outcomes, "_bronze_row_id", "left_anti").count(),
            outcomes.join(bronze, "_bronze_row_id", "left_anti").count())

recon = spark.createDataFrame(
    [reconcile(ds) for ds in ("orders", "transactions")],
    "dataset string, bronze_rows long, accepted long, quarantined long, duplicate_losers long, "
    "outcome_total long, overlapping_rows long, unassigned_bronze_rows long, unknown_outcome_rows long"
).withColumn("balanced", (c("bronze_rows") == c("accepted") + c("quarantined") + c("duplicate_losers"))
             & (c("overlapping_rows") == 0) & (c("unassigned_bronze_rows") == 0) & (c("unknown_outcome_rows") == 0)) \
 .withColumn("checked_at", F.current_timestamp())

spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('ops_silver_reconciliation')}
  (dataset string, bronze_rows long, accepted long, quarantined long, duplicate_losers long, outcome_total long,
   overlapping_rows long, unassigned_bronze_rows long, unknown_outcome_rows long, balanced boolean, checked_at timestamp)
  COMMENT 'One row per Silver run and dataset: proof that Bronze = accepted + quarantined + duplicate losers'""")
recon.write.mode("append").saveAsTable(tn("ops_silver_reconciliation"))
display(recon)
assert all(r["balanced"] for r in recon.collect()), "Silver reconciliation FAILED"
print("Reconciled: every Bronze row has exactly one Silver outcome, for both datasets.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross-check with the generator's dirty-data inventory
# MAGIC Expected values come from `generator/dirty_data_report.md`:
# MAGIC - Quarantine = all anomalies − fixable case/whitespace − duplicates.
# MAGIC - Orders: 75,729 − 29,834 − 9,863 = **36,032**. Transactions: 87,852 − 29,662 − 9,978 = **48,212**.
# MAGIC
# MAGIC A mismatch here is shown, not raised as an error. It would mean a rule is stricter or looser than the injected anomalies.

# COMMAND ----------

expected = {"orders": (954_105, 36_032, 9_863), "transactions": (941_810, 48_212, 9_978)}
rows = []
for r in recon.collect():
    e = expected[r["dataset"]]
    for metric, exp, act in zip(("accepted", "quarantined", "duplicate_losers"), e,
                                (r["accepted"], r["quarantined"], r["duplicate_losers"])):
        rows.append((r["dataset"], metric, exp, act, exp == act))
display(spark.createDataFrame(rows, "dataset string, metric string, expected_from_generator long, actual long, match boolean"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidence: row counts by rule

# COMMAND ----------

rule_counts = (spark.table(tn("silver_orders_quarantine")).select(F.lit("orders").alias("dataset"), F.explode("rejection_reasons").alias("rejection_reason"))
               .unionAll(spark.table(tn("silver_transactions_quarantine")).select(F.lit("transactions"), F.explode("rejection_reasons")))
               .groupBy("dataset", "rejection_reason").count().withColumnRenamed("count", "rows")
               .join(spark.table(tn("ref_dq_rules")), ["dataset", "rejection_reason"], "right")
               .fillna(0, ["rows"])
               .orderBy("dataset", F.desc("rows")))
write(rule_counts.withColumn("counted_at", F.current_timestamp()), "ops_silver_rule_counts")
display(rule_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidence: before/after conversion examples

# COMMAND ----------

display(o.where("order_status <> c_order_status OR category <> p_category OR country <> c_country "
                "OR (c_quantity IS NOT NULL AND p_quantity IS NULL) OR (c_order_date IS NOT NULL AND p_order_date IS NULL)")
         .select("_bronze_row_id", "order_status", "c_order_status", "category", "p_category", "country", "c_country",
                 "quantity", "p_quantity", "unit_price", "p_unit_price", "order_date", "p_order_date", "rejection_reasons")
         .limit(25))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidence: quarantine samples

# COMMAND ----------

display(spark.table(tn("silver_orders_quarantine")).select("_bronze_row_id", *ORDER_RAW, "rejection_reasons").limit(15))

# COMMAND ----------

display(spark.table(tn("silver_transactions_quarantine")).select("_bronze_row_id", *TXN_RAW, "rejection_reasons").limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidence: duplicate winners and losers side by side

# COMMAND ----------

display(spark.sql(f"""
SELECT d.dedup_key AS order_id, d.duplicate_type,
       d._bronze_row_id AS loser_row_id,  d._source_line AS loser_line,  d.order_status AS loser_status,  d.quantity AS loser_qty,
       w._bronze_row_id AS winner_row_id, w._source_line AS winner_line, w.order_status AS winner_status, w.quantity AS winner_qty
FROM {t('silver_orders_duplicates')} d JOIN {t('silver_orders')} w ON w._bronze_row_id = d.winner_bronze_row_id
ORDER BY d.duplicate_type, d.dedup_key LIMIT 20"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT d.dedup_key AS txn_id, d.duplicate_type,
       d._bronze_row_id AS loser_row_id,  d.txn_status AS loser_status,  d.txn_ts AS loser_ts,
       w._bronze_row_id AS winner_row_id, w.txn_status AS winner_status, w.txn_ts AS winner_ts
FROM {t('silver_transactions_duplicates')} d JOIN {t('silver_transactions')} w ON w._bronze_row_id = d.winner_bronze_row_id
ORDER BY d.duplicate_type, d.dedup_key LIMIT 20"""))
