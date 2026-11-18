# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

PAID_TOLERANCE_USD = "0.05"
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE gold_fact_orders AS
WITH pay AS (
  SELECT order_id,
         COUNT(*)                                                     AS payment_attempts,
         COUNT_IF(txn_status = 'SUCCESS')                             AS successful_payments,
         COALESCE(SUM(CASE WHEN txn_status = 'SUCCESS'  THEN amount_usd END), 0) AS paid_usd,
         COALESCE(SUM(CASE WHEN txn_status = 'REFUNDED' THEN amount_usd END), 0) AS refunded_usd
  FROM silver_transactions
  GROUP BY order_id
)
SELECT o.order_id, o.customer_id, o.order_date, o.category, o.country, o.order_status,
       o.quantity, o.unit_price, o.order_amount_usd,
       COALESCE(p.payment_attempts, 0)                         AS payment_attempts,
       COALESCE(p.successful_payments, 0)                      AS successful_payments,
       CAST(COALESCE(p.paid_usd, 0) AS DECIMAL(14,2))          AS paid_usd,
       CAST(COALESCE(p.refunded_usd, 0) AS DECIMAL(14,2))      AS refunded_usd,
       COALESCE(p.paid_usd, 0) >= o.order_amount_usd - {PAID_TOLERANCE_USD} AS is_fully_paid,
       o.order_status = 'CANCELLED'                            AS is_cancelled,
       (o.order_status IN ('COMPLETED','SHIPPED')
        AND COALESCE(p.paid_usd, 0) >= o.order_amount_usd - {PAID_TOLERANCE_USD}) AS is_revenue_eligible,
       CAST(CASE WHEN o.order_status IN ('COMPLETED','SHIPPED')
                  AND COALESCE(p.paid_usd, 0) >= o.order_amount_usd - {PAID_TOLERANCE_USD}
                 THEN o.order_amount_usd ELSE 0 END AS DECIMAL(14,2)) AS revenue_usd,
       CAST(GREATEST(COALESCE(p.paid_usd, 0) - o.order_amount_usd, 0) AS DECIMAL(14,2)) AS overpaid_usd
FROM silver_orders o
LEFT JOIN pay p ON p.order_id = o.order_id
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_daily_sales AS
SELECT order_date,
       COUNT(*)                                   AS total_orders,
       COUNT_IF(is_revenue_eligible)              AS revenue_orders,
       COUNT_IF(is_cancelled)                     AS cancelled_orders,
       ROUND(COUNT_IF(is_cancelled) / COUNT(*), 4) AS cancellation_rate,
       CAST(SUM(revenue_usd) AS DECIMAL(16,2))    AS revenue_usd,
       CAST(SUM(revenue_usd) / NULLIF(COUNT_IF(is_revenue_eligible), 0) AS DECIMAL(14,2)) AS avg_order_value_usd
FROM gold_fact_orders
GROUP BY order_date
""")

spark.sql("""
CREATE OR REPLACE TABLE gold_revenue_breakdown AS
SELECT order_date, category, country,
       COUNT(*)                                          AS total_orders,
       COUNT_IF(is_revenue_eligible)                     AS revenue_orders,
       SUM(CASE WHEN is_revenue_eligible THEN quantity ELSE 0 END) AS units_sold,
       CAST(SUM(revenue_usd) AS DECIMAL(16,2))           AS revenue_usd
FROM gold_fact_orders
GROUP BY order_date, category, country
""")

spark.sql("""
CREATE OR REPLACE TABLE gold_payment_success AS
SELECT txn_date, payment_method,
       COUNT(*)                              AS attempts,
       COUNT_IF(txn_status = 'SUCCESS')      AS successful,
       COUNT_IF(txn_status = 'FAILED')       AS failed,
       COUNT_IF(txn_status = 'PENDING')      AS pending,
       COUNT_IF(txn_status = 'REFUNDED')     AS refunded,
       ROUND(COUNT_IF(txn_status = 'SUCCESS')
             / NULLIF(COUNT_IF(txn_status IN ('SUCCESS','FAILED')), 0), 4) AS success_rate,
       CAST(SUM(CASE WHEN txn_status = 'SUCCESS' THEN amount_usd ELSE 0 END) AS DECIMAL(16,2)) AS successful_amount_usd
FROM silver_transactions
GROUP BY txn_date, payment_method
""")

spark.sql("""
CREATE OR REPLACE TABLE gold_top_customers AS
SELECT customer_id,
       COUNT(*)                                  AS total_orders,
       COUNT_IF(is_revenue_eligible)             AS revenue_orders,
       CAST(SUM(revenue_usd) AS DECIMAL(16,2))   AS revenue_usd,
       CAST(SUM(revenue_usd) / NULLIF(COUNT_IF(is_revenue_eligible), 0) AS DECIMAL(14,2)) AS avg_order_value_usd,
       MIN(order_date)                           AS first_order_date,
       MAX(order_date)                           AS last_order_date,
       ROW_NUMBER() OVER (ORDER BY SUM(revenue_usd) DESC, customer_id) AS spend_rank
FROM gold_fact_orders
GROUP BY customer_id
""")

# COMMAND ----------

document_table("gold_fact_orders",
    "Gold order fact. Grain: one row per order. Key: order_id. Source of truth for revenue: SUM(revenue_usd).",
    {"order_id": "Unique key", "order_amount_usd": "Order value = quantity * unit_price (USD)",
     "payment_attempts": "All payment rows for the order (any status)",
     "successful_payments": "Count of SUCCESS payments (split or repeat payments give >1)",
     "paid_usd": "Sum of SUCCESS payments converted to USD", "refunded_usd": "Sum of REFUNDED payments in USD",
     "is_fully_paid": f"paid_usd >= order_amount_usd - {PAID_TOLERANCE_USD} (FX rounding tolerance)",
     "is_cancelled": "order_status = CANCELLED",
     "is_revenue_eligible": "status COMPLETED/SHIPPED AND fully paid",
     "revenue_usd": "Recognised revenue: order_amount_usd if eligible else 0. Always SUM this column for revenue.",
     "overpaid_usd": "Successful payments above the order value (e.g. duplicate charges); not revenue"})
document_table("gold_daily_sales",
    "Daily sales. Grain: one row per order_date. Key: order_date. Revenue per gold_fact_orders definition.",
    {"order_date": "Unique key", "total_orders": "All accepted orders that day (any status)",
     "revenue_orders": "Orders counted in revenue", "cancelled_orders": "Orders with status CANCELLED",
     "cancellation_rate": "cancelled_orders / total_orders. Re-aggregate as SUM(cancelled)/SUM(total), never AVG",
     "revenue_usd": "SUM(gold_fact_orders.revenue_usd)", "avg_order_value_usd": "revenue_usd / revenue_orders"})
document_table("gold_revenue_breakdown",
    "Revenue by category and country. Grain: one row per order_date x category x country. Key: (order_date, category, country).",
    {"units_sold": "Units in revenue-eligible orders", "revenue_usd": "SUM(gold_fact_orders.revenue_usd)",
     "total_orders": "All accepted orders (any status)", "revenue_orders": "Orders counted in revenue"})
document_table("gold_payment_success",
    "Payment performance. Grain: one row per txn_date x payment_method. Key: (txn_date, payment_method). Based on deduplicated latest txn status.",
    {"attempts": "All payment rows", "success_rate": "successful / (successful + failed). PENDING (not final) and REFUNDED excluded. Re-aggregate as SUM/SUM",
     "successful_amount_usd": "Collected amount of SUCCESS payments in USD (cash view, not revenue)"})
document_table("gold_top_customers",
    "Customer ranking by spend over the full period. Grain: one row per customer. Key: customer_id. Not date-filterable (lifetime).",
    {"customer_id": "Unique key", "revenue_usd": "Lifetime recognised revenue (USD)",
     "spend_rank": "1 = highest revenue; ties broken by customer_id"})

# COMMAND ----------

for table, cols in [("gold_fact_orders", ["order_id"]), ("gold_daily_sales", ["order_date"]),
                    ("gold_revenue_breakdown", ["order_date", "category", "country"]),
                    ("gold_payment_success", ["txn_date", "payment_method"]), ("gold_top_customers", ["customer_id"])]:
    try:
        for col in cols:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{col}` SET NOT NULL")
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT pk_{table} PRIMARY KEY ({', '.join(cols)})")
    except Exception as e:
        print(f"PK on {table} skipped: {type(e).__name__}")

# COMMAND ----------

checks = spark.sql("""
SELECT 'gold_fact_orders' AS table_name, COUNT(*) AS rows, COUNT(DISTINCT order_id) AS distinct_keys,
       (SELECT COUNT(*) FROM silver_orders) AS expected_rows FROM gold_fact_orders
UNION ALL SELECT 'gold_daily_sales', COUNT(*), COUNT(DISTINCT order_date), NULL FROM gold_daily_sales
UNION ALL SELECT 'gold_revenue_breakdown', COUNT(*), COUNT(DISTINCT order_date, category, country), NULL FROM gold_revenue_breakdown
UNION ALL SELECT 'gold_payment_success', COUNT(*), COUNT(DISTINCT txn_date, payment_method), NULL FROM gold_payment_success
UNION ALL SELECT 'gold_top_customers', COUNT(*), COUNT(DISTINCT customer_id), NULL FROM gold_top_customers
""")
display(checks)
for r in checks.collect():
    assert r["rows"] == r["distinct_keys"], f"{r['table_name']}: grain is not unique"
    assert r["expected_rows"] is None or r["rows"] == r["expected_rows"], f"{r['table_name']}: fan-out detected"

rev = spark.sql("""
SELECT (SELECT SUM(revenue_usd) FROM gold_fact_orders)       AS fact_orders,
       (SELECT SUM(revenue_usd) FROM gold_daily_sales)       AS daily_sales,
       (SELECT SUM(revenue_usd) FROM gold_revenue_breakdown) AS revenue_breakdown,
       (SELECT SUM(revenue_usd) FROM gold_top_customers)     AS top_customers
""")
display(rev)
r = rev.first()
assert r["fact_orders"] == r["daily_sales"] == r["revenue_breakdown"] == r["top_customers"], "Revenue differs between marts"
print(f"Total revenue identical in all marts: {r['fact_orders']:,} USD")

# COMMAND ----------

display(spark.sql("""
SELECT 'gold_fact_orders' AS table_name, COUNT(*) AS rows FROM gold_fact_orders
UNION ALL SELECT 'gold_daily_sales', COUNT(*) FROM gold_daily_sales
UNION ALL SELECT 'gold_revenue_breakdown', COUNT(*) FROM gold_revenue_breakdown
UNION ALL SELECT 'gold_payment_success', COUNT(*) FROM gold_payment_success
UNION ALL SELECT 'gold_top_customers', COUNT(*) FROM gold_top_customers"""))

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_daily_sales ORDER BY order_date DESC LIMIT 10"))

# COMMAND ----------

display(spark.sql("""SELECT category, country, SUM(revenue_orders) AS revenue_orders, SUM(revenue_usd) AS revenue_usd
                     FROM gold_revenue_breakdown GROUP BY ALL ORDER BY revenue_usd DESC LIMIT 15"""))

# COMMAND ----------

display(spark.sql("""SELECT payment_method, SUM(attempts) AS attempts, SUM(successful) AS successful, SUM(failed) AS failed,
                            ROUND(SUM(successful) / NULLIF(SUM(successful) + SUM(failed), 0), 4) AS success_rate
                     FROM gold_payment_success GROUP BY ALL ORDER BY success_rate DESC"""))

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_top_customers ORDER BY spend_rank LIMIT 10"))

# COMMAND ----------

display(spark.sql("""
WITH tagged AS (
  SELECT *, CASE
      WHEN successful_payments >= 2 AND overpaid_usd > 1 THEN 'repeat charge'
      WHEN successful_payments >= 2                      THEN 'split payment'
      WHEN payment_attempts > successful_payments AND successful_payments = 1 AND order_status IN ('COMPLETED','SHIPPED') THEN 'failed then success'
      WHEN order_status = 'CANCELLED'                    THEN 'cancelled'
      WHEN order_status = 'RETURNED'                     THEN 'returned'
      WHEN order_status IN ('COMPLETED','SHIPPED') AND payment_attempts = 0 THEN 'no payment'
      WHEN successful_payments = 1                       THEN 'single payment' END AS case_name
  FROM gold_fact_orders)
SELECT case_name, order_id, order_status, order_amount_usd, payment_attempts, successful_payments,
       paid_usd, refunded_usd, overpaid_usd, revenue_usd
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY case_name ORDER BY order_id) AS rn FROM tagged WHERE case_name IS NOT NULL)
WHERE rn = 1 ORDER BY case_name"""))
