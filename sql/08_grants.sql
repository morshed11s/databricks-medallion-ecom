GRANT USE CATALOG ON CATALOG ecom_catalog         TO `ecom_reporting`;
GRANT USE SCHEMA  ON SCHEMA  ecom_catalog.ecom_poc TO `ecom_reporting`;

GRANT SELECT ON TABLE ecom_catalog.ecom_poc.gold_fact_orders       TO `ecom_reporting`;
GRANT SELECT ON TABLE ecom_catalog.ecom_poc.gold_daily_sales       TO `ecom_reporting`;
GRANT SELECT ON TABLE ecom_catalog.ecom_poc.gold_revenue_breakdown TO `ecom_reporting`;
GRANT SELECT ON TABLE ecom_catalog.ecom_poc.gold_payment_success   TO `ecom_reporting`;
GRANT SELECT ON TABLE ecom_catalog.ecom_poc.gold_top_customers     TO `ecom_reporting`;

SHOW GRANTS `ecom_reporting` ON CATALOG ecom_catalog;
SHOW GRANTS `ecom_reporting` ON SCHEMA  ecom_catalog.ecom_poc;
SHOW GRANTS `ecom_reporting` ON TABLE   ecom_catalog.ecom_poc.gold_daily_sales;
SHOW GRANTS `ecom_reporting` ON TABLE   ecom_catalog.ecom_poc.silver_orders;

SHOW GRANTS ON CATALOG ecom_catalog;
SHOW GRANTS ON SCHEMA  ecom_catalog.ecom_poc;
