USE CATALOG ecom_catalog;
USE SCHEMA ecom_poc;

SELECT SUM(revenue_usd) AS revenue_usd_q1_2026
FROM gold_daily_sales
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-03-31';

SELECT payment_method,
       SUM(successful) AS successful,
       SUM(failed)     AS failed,
       ROUND(SUM(successful) / NULLIF(SUM(successful) + SUM(failed), 0), 4) AS success_rate
FROM gold_payment_success
GROUP BY payment_method
ORDER BY success_rate DESC;

SELECT category, SUM(revenue_usd) AS revenue_usd
FROM gold_revenue_breakdown
WHERE country = 'DE'
GROUP BY category
ORDER BY revenue_usd DESC
LIMIT 3;

SELECT ROUND(SUM(cancelled_orders) / SUM(total_orders), 4) AS correct_cancellation_rate,
       ROUND(AVG(cancellation_rate), 4)                     AS naive_avg_of_daily_rates
FROM gold_daily_sales;
