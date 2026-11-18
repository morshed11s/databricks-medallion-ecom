# Evidence

Every screenshot here comes from my own Databricks trial workspace. Before committing:
- **Mask** emails, tokens, the workspace URL's personal parts and any connection secret.
- Keep the unmasked original as `<name>_unmasked.png`. That pattern is in `.gitignore`, so it is never committed.

## Checklist

| # | File | Topic | What it must show |
|---|---|---|---|
| 01 | `01_git_folder.png` | Git | Notebook opened from the Databricks Git folder on branch `main` ✅ |
| 02 | `02_catalog.png` | Unity Catalog | Catalog `workspace` → schema `ecom_poc` (owner email masked) ✅ |
| 03 | `03_workspace_home_trial.png` | Trial | Workspace home + *Manage Trial* panel (days and credits left) |
| 04 | `04_volume_upload.png` | Landing | `raw_files` volume with `orders.csv` and `transactions.csv` (sizes visible) |
| 05 | `05_compute_sql_warehouse.png` | Compute | SQL warehouse page: size, serverless, auto-stop setting |
| 06 | `06_landing_checks.png` | Landing | `02_landing_checks` output: rows, bytes and sha256 = manifest |
| 07 | `07_bronze_schema.png` | Bronze | `bronze_orders` schema (all STRING + audit columns) and sample rows |
| 08 | `08_bronze_lossless.png` | Bronze | `ops_bronze_load_log` with `lossless = true`; second run shows the same row count (idempotent) |
| 09 | `09_silver_reconciliation.png` | Silver | Reconciliation table, `balanced = true` for both datasets |
| 10 | `10_silver_rule_counts.png` | Silver | Rows by rejection rule + quarantine samples |
| 11 | `11_silver_duplicates.png` | Silver | Winners vs losers (`exact_row_duplicate` / `key_conflict`) |
| 12 | `12_gold_checks.png` | Gold | Unique grain + identical revenue across marts |
| 13 | `13_gold_catalog_comments.png` | Gold | Catalog Explorer showing table/column descriptions |
| 14 | `14_aibi_dashboard.png` | AI/BI | Dashboard with KPIs, trend, breakdowns and filters |
| 15 | `15_genie_q1.png` … `17_genie_q3.png` | Genie | Each answer with its generated SQL |
| 18 | `18_powerbi_connection.png` | Power BI | Connection dialog (hostname/HTTP path ok, **token masked**) |
| 19 | `19_powerbi_report.png` | Power BI | One-page report |
| 20 | `20_revenue_crosscheck.png` | Cross-check | SQL result for the agreed date range |
| 21 | `21_job_graph.png` | Jobs | Task graph `landing_checks → bronze → silver → gold` |
| 22 | `22_job_run_success.png` | Jobs | Successful run details with task durations |
| 23 | `23_job_retry_notifications.png` | Jobs | Retry and failure-notification settings |
| 24 | `24_grants.png` | Security | `SHOW GRANTS` output for the reporting group |
| 25 | `25_credits_cleanup.png` | Cleanup | Credits remaining + warehouse stopped + no schedule |
| 26 | `26_quality_assertions.png` | All layers | `sql/10_quality_assertions.sql` table A (source → Bronze → Silver) + final "ALL QUALITY ASSERTIONS PASSED" |

Text outputs can also be saved as `.md` or `.csv` snippets with the same number prefix.
