-- Gold metric definitions and the TRUSTED reference queries used to cross-check
-- Databricks AI/BI, Genie and Power BI. All revenue = SUM(gold_fact_orders.revenue_usd).
USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- Revenue definition (also stored as the gold_fact_orders.revenue_usd column comment):
--   revenue_usd = order_amount_usd  IF order_status IN ('COMPLETED','SHIPPED')
--                                  AND SUM(SUCCESS payments in USD) >= order_amount_usd - 0.05
--                 ELSE 0
--   Currency: USD (EUR/GBP payments converted with ref_fx_rates). Revenue date: order_date.

-- R1) Total revenue, whole period
SELECT SUM(revenue_usd) AS total_revenue_usd, COUNT_IF(is_revenue_eligible) AS revenue_orders
FROM gold_fact_orders;

-- R2) Total revenue for a fixed date range (use the SAME range in AI/BI and Power BI)
SELECT SUM(revenue_usd) AS revenue_usd
FROM gold_daily_sales
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30';

-- R3) Cancellation rate (correct re-aggregation: sum / sum)
SELECT ROUND(SUM(cancelled_orders) / SUM(total_orders), 4) AS cancellation_rate
FROM gold_daily_sales;

-- R4) Revenue by category
SELECT category, SUM(revenue_usd) AS revenue_usd
FROM gold_revenue_breakdown GROUP BY category ORDER BY revenue_usd DESC;

-- R5) Payment success rate by method
SELECT payment_method,
       ROUND(SUM(successful) / NULLIF(SUM(successful) + SUM(failed), 0), 4) AS success_rate
FROM gold_payment_success GROUP BY payment_method ORDER BY success_rate DESC;

-- R6) Top 10 customers
SELECT spend_rank, customer_id, revenue_usd, revenue_orders
FROM gold_top_customers WHERE spend_rank <= 10 ORDER BY spend_rank;
