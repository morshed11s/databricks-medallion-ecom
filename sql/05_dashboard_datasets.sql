SELECT order_date, total_orders, revenue_orders, cancelled_orders, revenue_usd
FROM workspace.ecom_poc.gold_daily_sales;

SELECT order_date, category, country, total_orders, revenue_orders, units_sold, revenue_usd
FROM workspace.ecom_poc.gold_revenue_breakdown;

SELECT txn_date, payment_method, attempts, successful, failed, pending, refunded, successful_amount_usd
FROM workspace.ecom_poc.gold_payment_success;

SELECT spend_rank, customer_id, revenue_usd, revenue_orders, avg_order_value_usd, first_order_date, last_order_date
FROM workspace.ecom_poc.gold_top_customers
WHERE spend_rank <= 20;
