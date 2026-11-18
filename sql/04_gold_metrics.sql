USE CATALOG workspace;
USE SCHEMA ecom_poc;

SELECT SUM(revenue_usd) AS total_revenue_usd, COUNT_IF(is_revenue_eligible) AS revenue_orders
FROM gold_fact_orders;

SELECT SUM(revenue_usd) AS revenue_usd
FROM gold_daily_sales
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30';

SELECT ROUND(SUM(cancelled_orders) / SUM(total_orders), 4) AS cancellation_rate
FROM gold_daily_sales;

SELECT category, SUM(revenue_usd) AS revenue_usd
FROM gold_revenue_breakdown GROUP BY category ORDER BY revenue_usd DESC;

SELECT payment_method,
       ROUND(SUM(successful) / NULLIF(SUM(successful) + SUM(failed), 0), 4) AS success_rate
FROM gold_payment_success GROUP BY payment_method ORDER BY success_rate DESC;

SELECT spend_rank, customer_id, revenue_usd, revenue_orders
FROM gold_top_customers WHERE spend_rank <= 10 ORDER BY spend_rank;
