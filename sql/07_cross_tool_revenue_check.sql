USE CATALOG ecom_catalog;
USE SCHEMA ecom_poc;

SELECT
  (SELECT SUM(revenue_usd) FROM gold_fact_orders
    WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30')        AS from_fact_orders,
  (SELECT SUM(revenue_usd) FROM gold_daily_sales
    WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30')        AS from_daily_sales,
  (SELECT SUM(revenue_usd) FROM gold_revenue_breakdown
    WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30')        AS from_revenue_breakdown;

SELECT SUM(revenue_usd) AS total_revenue_all_dates FROM gold_fact_orders;

SELECT date_trunc('month', order_date) AS month, SUM(revenue_usd) AS revenue_usd
FROM gold_daily_sales
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30'
GROUP BY ALL ORDER BY month;

SELECT
  SUM(revenue_usd)                                              AS correct_revenue,
  SUM(order_amount_usd)                                         AS wrong_all_orders_any_status,
  SUM(CASE WHEN order_status IN ('COMPLETED','SHIPPED') THEN order_amount_usd END) AS wrong_ignores_payment,
  SUM(paid_usd)                                                 AS wrong_sum_of_successful_payments
FROM gold_fact_orders
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30';
