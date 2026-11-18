-- Trusted SQL used to verify Genie answers (dashboards/genie.md).
-- Run on the SQL warehouse; compare each result with the Genie answer and its generated SQL.
USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- Q1  "What was our total revenue in the first quarter of 2026?"
SELECT SUM(revenue_usd) AS revenue_usd_q1_2026
FROM gold_daily_sales
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-03-31';

-- Q2  "Which payment method has the highest payment success rate?"
--     success rate = successful / (successful + failed); PENDING and REFUNDED excluded
SELECT payment_method,
       SUM(successful) AS successful,
       SUM(failed)     AS failed,
       ROUND(SUM(successful) / NULLIF(SUM(successful) + SUM(failed), 0), 4) AS success_rate
FROM gold_payment_success
GROUP BY payment_method
ORDER BY success_rate DESC;

-- Q3  "What are the top 3 product categories by revenue in Germany?"
SELECT category, SUM(revenue_usd) AS revenue_usd
FROM gold_revenue_breakdown
WHERE country = 'DE'
GROUP BY category
ORDER BY revenue_usd DESC
LIMIT 3;

-- Trap check (optional 4th question): "What is the overall cancellation rate?"
-- Correct = SUM/SUM. An AVG of the daily cancellation_rate gives a slightly different (wrong) number.
SELECT ROUND(SUM(cancelled_orders) / SUM(total_orders), 4) AS correct_cancellation_rate,
       ROUND(AVG(cancellation_rate), 4)                     AS naive_avg_of_daily_rates
FROM gold_daily_sales;
