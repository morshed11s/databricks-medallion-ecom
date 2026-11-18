USE CATALOG workspace;
USE SCHEMA ecom_poc;

SELECT * FROM ref_dq_rules ORDER BY dataset, field, rule_type;

WITH o AS (
  SELECT 'orders' AS dataset,
         (SELECT COUNT(*) FROM bronze_orders)            AS bronze_rows,
         (SELECT COUNT(*) FROM silver_orders)            AS accepted,
         (SELECT COUNT(*) FROM silver_orders_quarantine) AS quarantined,
         (SELECT COUNT(*) FROM silver_orders_duplicates) AS duplicate_losers
  UNION ALL
  SELECT 'transactions',
         (SELECT COUNT(*) FROM bronze_transactions),
         (SELECT COUNT(*) FROM silver_transactions),
         (SELECT COUNT(*) FROM silver_transactions_quarantine),
         (SELECT COUNT(*) FROM silver_transactions_duplicates)
)
SELECT *, accepted + quarantined + duplicate_losers AS outcome_total,
       bronze_rows = accepted + quarantined + duplicate_losers AS balanced
FROM o;

SELECT _bronze_row_id, COUNT(*) AS outcomes
FROM (SELECT _bronze_row_id FROM silver_orders
      UNION ALL SELECT _bronze_row_id FROM silver_orders_quarantine
      UNION ALL SELECT _bronze_row_id FROM silver_orders_duplicates)
GROUP BY _bronze_row_id HAVING COUNT(*) > 1;

SELECT * FROM ops_silver_rule_counts ORDER BY dataset, rows DESC;

SELECT duplicate_type, COUNT(*) AS rows FROM silver_orders_duplicates GROUP BY ALL
UNION ALL
SELECT 'txn_' || duplicate_type, COUNT(*) FROM silver_transactions_duplicates GROUP BY ALL;

SELECT * FROM ops_silver_reconciliation ORDER BY checked_at DESC;
