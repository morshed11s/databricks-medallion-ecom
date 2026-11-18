-- Cross-tool revenue verification: Databricks SQL vs AI/BI dashboard vs Power BI.
-- Agreed comparison scope (use EXACTLY this in all three tools):
--   Date range : order_date 2026-01-01 .. 2026-06-30 (inclusive)
--   Currency   : USD (revenue_usd)
--   Filters    : none (all categories, all countries)
USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- A) Reference answer, computed three ways from Gold. All three columns must be equal.
SELECT
  (SELECT SUM(revenue_usd) FROM gold_fact_orders
    WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30')        AS from_fact_orders,
  (SELECT SUM(revenue_usd) FROM gold_daily_sales
    WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30')        AS from_daily_sales,
  (SELECT SUM(revenue_usd) FROM gold_revenue_breakdown
    WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30')        AS from_revenue_breakdown;

-- B) Whole-period total (useful if a tool's date filter is accidentally cleared)
SELECT SUM(revenue_usd) AS total_revenue_all_dates FROM gold_fact_orders;

-- C) Diagnostics if a tool disagrees
-- C1: monthly split. Find which month differs (date-boundary or timezone problem).
SELECT date_trunc('month', order_date) AS month, SUM(revenue_usd) AS revenue_usd
FROM gold_daily_sales
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30'
GROUP BY ALL ORDER BY month;

-- C2: what a WRONG definition would give. Shows why "sum of payments" or "all orders" differ.
SELECT
  SUM(revenue_usd)                                              AS correct_revenue,
  SUM(order_amount_usd)                                         AS wrong_all_orders_any_status,
  SUM(CASE WHEN order_status IN ('COMPLETED','SHIPPED') THEN order_amount_usd END) AS wrong_ignores_payment,
  SUM(paid_usd)                                                 AS wrong_sum_of_successful_payments
FROM gold_fact_orders
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30';
