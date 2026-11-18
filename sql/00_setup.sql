CREATE CATALOG IF NOT EXISTS ecom_catalog
  COMMENT 'E-commerce medallion PoC catalog';

USE CATALOG ecom_catalog;

CREATE SCHEMA IF NOT EXISTS ecom_poc
  COMMENT 'E-commerce medallion PoC: bronze/silver/gold tables for synthetic orders and payments';

USE SCHEMA ecom_poc;

CREATE VOLUME IF NOT EXISTS raw_files
  COMMENT 'Landing zone: raw, unchanged source CSV extracts (orders.csv, transactions.csv)';
