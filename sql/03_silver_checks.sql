-- Silver: rules matrix, reconciliation and rule counts (run in the SQL editor).
USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- 1) Business-rule matrix
SELECT * FROM ref_dq_rules ORDER BY dataset, field, rule_type;

-- 2) Bronze = accepted + quarantined + duplicate losers (no overlap)
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

-- 3) Overlap check: a Bronze row id may appear in only one outcome (expect 0 rows)
SELECT _bronze_row_id, COUNT(*) AS outcomes
FROM (SELECT _bronze_row_id FROM silver_orders
      UNION ALL SELECT _bronze_row_id FROM silver_orders_quarantine
      UNION ALL SELECT _bronze_row_id FROM silver_orders_duplicates)
GROUP BY _bronze_row_id HAVING COUNT(*) > 1;

-- 4) Rows by rejection rule (a row can fail several rules)
SELECT * FROM ops_silver_rule_counts ORDER BY dataset, rows DESC;

-- 5) Deduplication rule (illustrative SQL equivalent of notebooks/04_silver)
-- Orders: latest extract line wins
--   ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _source_line DESC) = 1
-- Transactions: latest status wins
--   ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY txn_ts DESC, _source_line DESC) = 1
SELECT duplicate_type, COUNT(*) AS rows FROM silver_orders_duplicates GROUP BY ALL
UNION ALL
SELECT 'txn_' || duplicate_type, COUNT(*) FROM silver_transactions_duplicates GROUP BY ALL;

-- 6) Latest reconciliation runs
SELECT * FROM ops_silver_reconciliation ORDER BY checked_at DESC;
