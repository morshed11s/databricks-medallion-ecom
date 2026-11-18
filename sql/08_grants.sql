GRANT USE CATALOG ON CATALOG workspace         TO `ecom_reporting`;
GRANT USE SCHEMA  ON SCHEMA  workspace.ecom_poc TO `ecom_reporting`;

GRANT SELECT ON TABLE workspace.ecom_poc.gold_fact_orders       TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_daily_sales       TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_revenue_breakdown TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_payment_success   TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_top_customers     TO `ecom_reporting`;

SHOW GRANTS `ecom_reporting` ON CATALOG workspace;
SHOW GRANTS `ecom_reporting` ON SCHEMA  workspace.ecom_poc;
SHOW GRANTS `ecom_reporting` ON TABLE   workspace.ecom_poc.gold_daily_sales;
SHOW GRANTS `ecom_reporting` ON TABLE   workspace.ecom_poc.silver_orders;

SHOW GRANTS ON CATALOG workspace;
SHOW GRANTS ON SCHEMA  workspace.ecom_poc;
