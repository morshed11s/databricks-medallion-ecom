USE CATALOG workspace;
USE SCHEMA ecom_poc;

WITH src AS (
  SELECT _metadata.file_name AS file_name, COUNT(*) - 1 AS source_rows
  FROM read_files('/Volumes/workspace/ecom_poc/raw_files/', format => 'text')
  GROUP BY ALL
), layers AS (
  SELECT 'orders' AS dataset, 'orders.csv' AS file_name,
         (SELECT COUNT(*) FROM bronze_orders)            AS bronze_rows,
         (SELECT COUNT(*) FROM silver_orders)            AS accepted,
         (SELECT COUNT(*) FROM silver_orders_quarantine) AS quarantined,
         (SELECT COUNT(*) FROM silver_orders_duplicates) AS duplicate_losers
  UNION ALL
  SELECT 'transactions', 'transactions.csv',
         (SELECT COUNT(*) FROM bronze_transactions),
         (SELECT COUNT(*) FROM silver_transactions),
         (SELECT COUNT(*) FROM silver_transactions_quarantine),
         (SELECT COUNT(*) FROM silver_transactions_duplicates)
)
SELECT l.dataset, s.source_rows, l.bronze_rows, l.accepted, l.quarantined, l.duplicate_losers,
       l.accepted + l.quarantined + l.duplicate_losers AS silver_outcomes,
       assert_true(s.source_rows = l.bronze_rows, concat(l.dataset, ': source rows != bronze rows')) AS a_source_eq_bronze,
       assert_true(l.bronze_rows = l.accepted + l.quarantined + l.duplicate_losers,
                   concat(l.dataset, ': bronze != accepted + quarantined + duplicates'))              AS a_partition
FROM layers l JOIN src s USING (file_name);

SELECT assert_true(COUNT(*) = 0, 'Bronze: a source column is not STRING') AS b1
FROM information_schema.columns
WHERE table_schema = 'ecom_poc' AND table_name IN ('bronze_orders', 'bronze_transactions')
  AND NOT startswith(column_name, '_') AND upper(data_type) <> 'STRING';

SELECT assert_true(COUNT(*) = COUNT(DISTINCT _bronze_row_id), 'bronze_orders: duplicate _bronze_row_id') AS b2a,
       assert_true(COUNT_IF(_ingested_at IS NULL OR _source_file IS NULL OR _batch_id IS NULL OR _row_hash IS NULL) = 0,
                   'bronze_orders: missing audit metadata') AS b2b
FROM bronze_orders;
SELECT assert_true(COUNT(*) = COUNT(DISTINCT _bronze_row_id), 'bronze_transactions: duplicate _bronze_row_id') AS b2c,
       assert_true(COUNT_IF(_ingested_at IS NULL OR _source_file IS NULL OR _batch_id IS NULL OR _row_hash IS NULL) = 0,
                   'bronze_transactions: missing audit metadata') AS b2d
FROM bronze_transactions;

SELECT assert_true(COUNT(*) = COUNT(DISTINCT _bronze_row_id), 'orders: a Bronze row is in more than one outcome') AS c1
FROM (SELECT _bronze_row_id FROM silver_orders UNION ALL SELECT _bronze_row_id FROM silver_orders_quarantine
      UNION ALL SELECT _bronze_row_id FROM silver_orders_duplicates);
SELECT assert_true(COUNT(*) = COUNT(DISTINCT _bronze_row_id), 'transactions: a Bronze row is in more than one outcome') AS c2
FROM (SELECT _bronze_row_id FROM silver_transactions UNION ALL SELECT _bronze_row_id FROM silver_transactions_quarantine
      UNION ALL SELECT _bronze_row_id FROM silver_transactions_duplicates);

SELECT assert_true(COUNT(*) = COUNT(DISTINCT order_id), 'silver_orders: order_id not unique') AS d1,
       assert_true(COUNT_IF(order_id IS NULL OR customer_id IS NULL OR order_date IS NULL OR category IS NULL
                            OR quantity IS NULL OR unit_price IS NULL OR order_status IS NULL OR country IS NULL) = 0,
                   'silver_orders: mandatory field is NULL') AS d2,
       assert_true(COUNT_IF(quantity NOT BETWEEN 1 AND 100) = 0, 'silver_orders: quantity out of range') AS d3,
       assert_true(COUNT_IF(unit_price <= 0 OR unit_price > 100000) = 0, 'silver_orders: unit_price out of range') AS d4,
       assert_true(COUNT_IF(order_status NOT IN ('COMPLETED','SHIPPED','PENDING','CANCELLED','RETURNED')) = 0,
                   'silver_orders: invalid status') AS d5,
       assert_true(COUNT_IF(country NOT IN ('US','GB','DE','FR','IN','CA','AU','BD','NL','ES')) = 0,
                   'silver_orders: invalid country') AS d6,
       assert_true(COUNT_IF(category NOT IN ('Electronics','Clothing','Home & Kitchen','Books','Beauty','Sports','Toys','Grocery')) = 0,
                   'silver_orders: invalid category') AS d7,
       assert_true(COUNT_IF(order_date > current_date()) = 0, 'silver_orders: future order_date') AS d8
FROM silver_orders;

SELECT assert_true(COUNT(*) = COUNT(DISTINCT txn_id), 'silver_transactions: txn_id not unique') AS d9,
       assert_true(COUNT_IF(txn_id IS NULL OR order_id IS NULL OR payment_method IS NULL OR amount IS NULL
                            OR currency IS NULL OR txn_status IS NULL OR txn_ts IS NULL) = 0,
                   'silver_transactions: mandatory field is NULL') AS d10,
       assert_true(COUNT_IF(amount <= 0 OR amount > 1000000) = 0, 'silver_transactions: amount out of range') AS d11,
       assert_true(COUNT_IF(txn_status NOT IN ('SUCCESS','FAILED','PENDING','REFUNDED')) = 0, 'silver_transactions: invalid status') AS d13,
       assert_true(COUNT_IF(payment_method NOT IN ('CARD','PAYPAL','WALLET','BANK_TRANSFER','COD')) = 0,
                   'silver_transactions: invalid payment method') AS d14,
       assert_true(COUNT_IF(txn_ts > current_timestamp()) = 0, 'silver_transactions: future txn_ts') AS d15,
       assert_true(COUNT_IF(amount_usd IS NULL) = 0, 'silver_transactions: amount_usd not converted') AS d16
FROM silver_transactions;

SELECT assert_true(COUNT(*) = 0, 'silver_transactions: unsupported currency') AS d12
FROM silver_transactions t LEFT ANTI JOIN ref_fx_rates f ON t.currency = f.currency;

SELECT assert_true(COUNT(*) = 0, 'silver_transactions: orphan order_id') AS d17
FROM silver_transactions t LEFT ANTI JOIN silver_orders o ON t.order_id = o.order_id;

SELECT assert_true(COUNT_IF(rejection_reasons IS NULL OR size(rejection_reasons) = 0) = 0,
                   'orders quarantine row without rejection reason') AS e1 FROM silver_orders_quarantine;
SELECT assert_true(COUNT_IF(rejection_reasons IS NULL OR size(rejection_reasons) = 0) = 0,
                   'transactions quarantine row without rejection reason') AS e2 FROM silver_transactions_quarantine;
SELECT assert_true(COUNT(*) = 0, 'orders duplicate loser without an accepted winner') AS e3
FROM silver_orders_duplicates d LEFT ANTI JOIN silver_orders w ON w._bronze_row_id = d.winner_bronze_row_id;
SELECT assert_true(COUNT(*) = 0, 'transactions duplicate loser without an accepted winner') AS e4
FROM silver_transactions_duplicates d LEFT ANTI JOIN silver_transactions w ON w._bronze_row_id = d.winner_bronze_row_id;

SELECT assert_true((SELECT COUNT(*) FROM gold_fact_orders) = (SELECT COUNT(*) FROM silver_orders),
                   'gold_fact_orders: fan-out or missing orders') AS f1,
       assert_true((SELECT COUNT(*) FROM gold_fact_orders) = (SELECT COUNT(DISTINCT order_id) FROM gold_fact_orders),
                   'gold_fact_orders: order_id not unique') AS f2,
       assert_true((SELECT COUNT_IF(revenue_usd <> 0 AND NOT is_revenue_eligible) FROM gold_fact_orders) = 0,
                   'gold_fact_orders: revenue on an ineligible order') AS f3,
       assert_true((SELECT COUNT_IF(revenue_usd < 0 OR revenue_usd > order_amount_usd) FROM gold_fact_orders) = 0,
                   'gold_fact_orders: revenue outside [0, order value]') AS f4;

SELECT assert_true(fact = daily AND fact = breakdown AND fact = customers, 'Gold marts disagree on total revenue') AS f5,
       fact AS total_revenue_usd
FROM (SELECT (SELECT SUM(revenue_usd) FROM gold_fact_orders)       AS fact,
             (SELECT SUM(revenue_usd) FROM gold_daily_sales)       AS daily,
             (SELECT SUM(revenue_usd) FROM gold_revenue_breakdown) AS breakdown,
             (SELECT SUM(revenue_usd) FROM gold_top_customers)     AS customers);

SELECT 'ALL QUALITY ASSERTIONS PASSED' AS result;
