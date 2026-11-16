-- Bronze reconciliation and idempotency checks (run in the SQL editor).
USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- 1) Source data rows (from the landing volume) vs Bronze rows: they must be equal.
WITH src AS (
  SELECT _metadata.file_name AS file_name, COUNT(*) - 1 AS source_data_rows
  FROM read_files('/Volumes/workspace/ecom_poc/raw_files/', format => 'text')
  GROUP BY ALL
),
brz AS (
  SELECT 'orders.csv' AS file_name, COUNT(*) AS bronze_rows FROM bronze_orders
  UNION ALL
  SELECT 'transactions.csv', COUNT(*) FROM bronze_transactions
)
SELECT s.file_name, s.source_data_rows, b.bronze_rows,
       s.source_data_rows = b.bronze_rows AS lossless
FROM src s JOIN brz b USING (file_name);

-- 2) Idempotency: each batch appears once and every row id is unique.
SELECT 'bronze_orders' AS t, _batch_id, COUNT(*) AS rows, COUNT(DISTINCT _bronze_row_id) AS distinct_ids
FROM bronze_orders GROUP BY _batch_id
UNION ALL
SELECT 'bronze_transactions', _batch_id, COUNT(*), COUNT(DISTINCT _bronze_row_id)
FROM bronze_transactions GROUP BY _batch_id;

-- 3) Load-run log written by notebooks/03_bronze.
SELECT * FROM ops_bronze_load_log ORDER BY loaded_at DESC;
