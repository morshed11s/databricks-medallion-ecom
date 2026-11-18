-- AI/BI dashboard datasets ("Data" tab of the dashboard). One query per dataset.
-- Every dataset reads GOLD only. Revenue is always SUM(revenue_usd), the same column used by SQL and Power BI.
-- Filters and aggregation happen in the dashboard, so the datasets keep their natural grain.

-- ds_daily_sales  (grain: order_date)
-- Used by: Cancellation rate KPI, revenue trend line
SELECT order_date, total_orders, revenue_orders, cancelled_orders, revenue_usd
FROM workspace.ecom_poc.gold_daily_sales;

-- ds_revenue_breakdown  (grain: order_date x category x country)
-- Used by: Total revenue / Revenue orders / AOV / Units KPIs, revenue by category, revenue by country
SELECT order_date, category, country, total_orders, revenue_orders, units_sold, revenue_usd
FROM workspace.ecom_poc.gold_revenue_breakdown;

-- ds_payment_success  (grain: txn_date x payment_method)
-- Used by: Payment success rate KPI, success rate by method
SELECT txn_date, payment_method, attempts, successful, failed, pending, refunded, successful_amount_usd
FROM workspace.ecom_poc.gold_payment_success;

-- ds_top_customers  (grain: customer_id, LIFETIME, not date-filterable)
SELECT spend_rank, customer_id, revenue_usd, revenue_orders, avg_order_value_usd, first_order_date, last_order_date
FROM workspace.ecom_poc.gold_top_customers
WHERE spend_rank <= 20;

-- Custom calculations to add in the dataset editor (ratios are always SUM/SUM, never AVG of a stored rate):
--   ds_revenue_breakdown : aov_usd           = SUM(revenue_usd) / SUM(revenue_orders)
--   ds_daily_sales       : cancellation_rate = SUM(cancelled_orders) / SUM(total_orders)
--   ds_payment_success   : success_rate      = SUM(successful) / (SUM(successful) + SUM(failed))
