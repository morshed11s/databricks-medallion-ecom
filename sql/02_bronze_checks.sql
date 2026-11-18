USE CATALOG ecom_catalog;
USE SCHEMA ecom_poc;

WITH src AS (
  SELECT _metadata.file_name AS file_name, COUNT(*) - 1 AS source_data_rows
  FROM read_files('/Volumes/ecom_catalog/ecom_poc/raw_files/', format => 'text')
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

SELECT 'bronze_orders' AS t, _batch_id, COUNT(*) AS rows, COUNT(DISTINCT _bronze_row_id) AS distinct_ids
FROM bronze_orders GROUP BY _batch_id
UNION ALL
SELECT 'bronze_transactions', _batch_id, COUNT(*), COUNT(DISTINCT _bronze_row_id)
FROM bronze_transactions GROUP BY _batch_id;

SELECT * FROM ops_bronze_load_log ORDER BY loaded_at DESC;
