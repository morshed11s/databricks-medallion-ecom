-- Workspace setup (equivalent to notebooks/01_setup.py).
-- Replace `workspace` with your Unity Catalog catalog if it is different.

USE CATALOG workspace;

CREATE SCHEMA IF NOT EXISTS ecom_poc
  COMMENT 'E-commerce medallion PoC: bronze/silver/gold tables for synthetic orders and payments';

USE SCHEMA ecom_poc;

CREATE VOLUME IF NOT EXISTS raw_files
  COMMENT 'Landing zone: raw, unchanged source CSV extracts (orders.csv, transactions.csv)';

-- Upload both files into:
--   /Volumes/workspace/ecom_poc/raw_files/orders.csv
--   /Volumes/workspace/ecom_poc/raw_files/transactions.csv
