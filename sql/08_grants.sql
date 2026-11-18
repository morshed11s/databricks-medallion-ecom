-- Least-privilege reporting access: Gold ONLY.
-- Principal: account group `ecom_reporting` (create it under Settings -> Identity and access -> Groups,
-- or in the account console; add the report consumers / Power BI service user to it).
-- Run as the schema owner or a workspace admin.

-- 1) Navigate (no data access by itself)
GRANT USE CATALOG ON CATALOG workspace         TO `ecom_reporting`;
GRANT USE SCHEMA  ON SCHEMA  workspace.ecom_poc TO `ecom_reporting`;

-- 2) Read Gold tables only (table-level grants, NOT schema-wide SELECT,
--    because ecom_poc also contains Bronze, Silver, quarantine, ops tables and the raw_files volume)
GRANT SELECT ON TABLE workspace.ecom_poc.gold_fact_orders       TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_daily_sales       TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_revenue_breakdown TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_payment_success   TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_top_customers     TO `ecom_reporting`;

-- Deliberately NOT granted: SELECT on bronze_* / silver_* / ops_* / ref_*, READ VOLUME on raw_files,
-- MODIFY, CREATE TABLE, or ALL PRIVILEGES. Compute access is separate: give the group
-- "Can use" on the SQL warehouse (SQL Warehouses -> Permissions), not "Can manage".

-- 3) Evidence: what the group can do
SHOW GRANTS `ecom_reporting` ON CATALOG workspace;
SHOW GRANTS `ecom_reporting` ON SCHEMA  workspace.ecom_poc;
SHOW GRANTS `ecom_reporting` ON TABLE   workspace.ecom_poc.gold_daily_sales;
SHOW GRANTS `ecom_reporting` ON TABLE   workspace.ecom_poc.silver_orders;   -- expect: no rows

-- 4) Check for broader inherited defaults on the trial's `workspace` catalog
--    (trial workspaces may give all users access by default; inherited grants would bypass least privilege)
SHOW GRANTS ON CATALOG workspace;
SHOW GRANTS ON SCHEMA  workspace.ecom_poc;

-- 5) Test plan (log in as a member of ecom_reporting, using a second identity you own):
--    SELECT COUNT(*) FROM workspace.ecom_poc.gold_daily_sales;       -- expect: succeeds
--    SELECT COUNT(*) FROM workspace.ecom_poc.silver_orders;          -- expect: PERMISSION_DENIED
--    SELECT COUNT(*) FROM workspace.ecom_poc.bronze_transactions;    -- expect: PERMISSION_DENIED
--    LIST '/Volumes/workspace/ecom_poc/raw_files/';                  -- expect: PERMISSION_DENIED
--    CREATE TABLE workspace.ecom_poc.x (a INT);                      -- expect: PERMISSION_DENIED
--    After a job re-run (CREATE OR REPLACE on Gold), repeat SHOW GRANTS to confirm grants persisted.

-- Rollback
-- REVOKE SELECT ON TABLE workspace.ecom_poc.gold_daily_sales FROM `ecom_reporting`;  (etc.)
