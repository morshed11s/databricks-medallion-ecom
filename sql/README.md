# SQL (run on the SQL warehouse or SQL editor)

| File | Purpose |
|---|---|
| `00_setup.sql` | Schema + managed volume |
| `01_landing_row_counts.sql` | Reproducible source row counts and byte sizes |
| `02_bronze_checks.sql` | Source vs Bronze parity, idempotency |
| `03_silver_checks.sql` | Rules matrix, reconciliation, overlap, dedup |
| `04_gold_metrics.sql` | Revenue definition and trusted reference queries |
| `05_dashboard_datasets.sql` | AI/BI dashboard datasets |
| `06_genie_verification.sql` | Trusted SQL used to check Genie answers |
| `07_cross_tool_revenue_check.sql` | SQL vs AI/BI vs Power BI revenue comparison + diagnostics |
| `08_grants.sql` | Least-privilege Gold-only reporting grants and test plan |
| `09_credit_usage.sql` | Trial DBU / cost tracking from system tables |
