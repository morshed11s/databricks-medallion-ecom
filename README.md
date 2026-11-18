# Trustworthy E-commerce Lakehouse — Databricks Medallion Assignment

Bronze → Silver → Gold lakehouse on a Databricks free trial, using reproducible synthetic order and payment data.

```
generator (seed 42) ─▶ orders.csv / transactions.csv (1,000,000 rows each)
      │ upload
      ▼
/Volumes/workspace/ecom_poc/raw_files/          landing: unchanged files, checked against manifest sha256
      │ 03_bronze (STRING, audit columns, replaceWhere per batch)
      ▼
bronze_orders · bronze_transactions             lossless: source rows = Bronze rows
      │ 04_silver (safe parse → validate → dedup valid rows)
      ▼
silver_* (accepted) · silver_*_quarantine · silver_*_duplicates     Bronze = accepted + quarantined + duplicates
      │ 05_gold (aggregate payments per order first, then 1:1 join)
      ▼
gold_fact_orders → gold_daily_sales · gold_revenue_breakdown · gold_payment_success · gold_top_customers
      │ one SQL warehouse
      ▼
AI/BI dashboard · Genie Agent · Power BI (Import)          same revenue = SUM(revenue_usd)
```

## 1. Business context
An e-commerce company receives daily CSV extracts of orders and payment transactions. The extracts contain nulls, text in numeric fields, inconsistent labels, impossible or future timestamps, orphan payments and duplicates, so sales reports built on them disagree with each other. This project:
- keeps the raw evidence unchanged (landing + Bronze);
- cleans it with explicit, counted rules that never silently drop a row (Silver);
- publishes one documented revenue definition that every reporting tool reuses (Gold → AI/BI, Genie, Power BI).

## 2. Trial workspace & environment
| Item | Value |
|---|---|
| Account | Databricks **free trial**, registered and verified with my own email |
| Trial start | 2026-11-16 (14-day trial) |
| Credits | $400 at start; $394 remaining on 2026-11-16 (see `operations/README.md` §3) |
| ETL compute | Serverless notebook compute |
| BI compute | *Serverless Starter Warehouse* (SQL warehouse, smallest size, auto-stop 10 min) |

**Trial vs Free Edition.**
- **Free Edition** is a permanently free, quota-limited, serverless-only workspace for learning. It has no credits, no account console and no upgrade path, and several enterprise features are limited or unavailable.
- The **free trial** is the full commercial platform for 14 days with a credit allowance. It includes Unity Catalog administration, SQL warehouses, Lakeflow Jobs, AI/BI, Genie and partner connectors such as Power BI, and it can be upgraded to a paid account.
- This assignment needs governance, a SQL warehouse endpoint for Power BI and job orchestration, so it targets the trial.

**Compute choice and credit saving.**
- *ETL* runs on serverless notebooks. They start in seconds, need no sizing, and bill only while code runs. The PySpark notebooks (hashing, window functions, joins over 2 M rows) are batch work that suits this.
- *BI* runs on one small serverless SQL warehouse. It is the governed SQL endpoint that the dashboard, Genie and Power BI share. Its auto-stop is set to the minimum.
- *Saving credits:* no classic clusters, no parallel experiments, the job has no schedule, Power BI uses Import (the warehouse sleeps between refreshes), and the warehouse is stopped after each session.

**Observed restrictions:** see §10.

### Unity Catalog setup & naming standard
Setup SQL: [`sql/00_setup.sql`](sql/00_setup.sql) (also [`notebooks/01_setup.py`](notebooks/01_setup.py)).

| Object | Pattern | Example |
|---|---|---|
| Catalog | existing writable trial catalog (catalog creation not needed) | `workspace` |
| Schema | `<domain>_<purpose>` | `ecom_poc` |
| Landing volume | managed volume `raw_files` | `/Volumes/workspace/ecom_poc/raw_files/` |
| Tables | `<layer>_<entity>` | `bronze_orders`, `silver_transactions`, `gold_daily_sales` |
| Rejected / duplicate outcomes | `silver_<entity>_quarantine` / `_duplicates` | `silver_orders_quarantine` |
| Reference / ops tables | `ref_<what>`, `ops_<what>` | `ref_fx_rates`, `ops_bronze_load_log` |

Exact raw-file paths: `/Volumes/workspace/ecom_poc/raw_files/orders.csv` and `/Volumes/workspace/ecom_poc/raw_files/transactions.csv`.
Because the layer is the table prefix, anyone browsing the catalog can see a table's trust level without opening it. The catalog name is a single widget or job parameter (`catalog`), so the project can move to another catalog.

### Git integration
- Repository: <https://github.com/morshed11s/databricks-medallion-ecom>. Connected as a **Databricks Git folder**.
- There is one commit per project stage (scaffold/generator → setup → Bronze → Silver/Gold → AI/BI → Power BI → operations → README); see `git log`.
- Notebooks are stored in Databricks source format (`.py`), so diffs are reviewable.

### SQL & query strategy
- **Transformations in notebooks, on serverless.** Bronze and Silver are PySpark (file hashing, safe parsing, window dedup, fail-fast assertions). Gold is Spark SQL `CREATE OR REPLACE TABLE … AS SELECT`. Every notebook can be safely re-run and stops on a failed invariant.
- **Each rule is defined once.**
  - Validation rules exist only in `04_silver` and are materialised as `ref_dq_rules`.
  - The revenue rule exists only in `gold_fact_orders.revenue_usd`.
  - Marts, dashboards, Genie and Power BI only `SUM(revenue_usd)`. None of them re-derive revenue.
- **One governed query layer.** A single SQL warehouse serves the AI/BI dashboard, Genie and Power BI, and grants restrict consumers to Gold (`sql/08_grants.sql`).
- **Correct re-aggregation.**
  - Rates are stored with their numerator and denominator and re-aggregated as `SUM/SUM`, never `AVG(rate)`.
  - Money is `DECIMAL`, so totals don't drift.
  - Dates are `DATE`, so date-range filters match across tools.
- **Verification SQL is separate from the pipeline** (`sql/`). The landing, Bronze and Silver checks are reproducible queries. The trusted metric queries are the reference for BI cross-checks. [`sql/10_quality_assertions.sql`](sql/10_quality_assertions.sql) re-checks every invariant with `assert_true` and fails loudly.
- **Names are fully qualified** (`workspace.ecom_poc.<table>`), and the catalog is a single notebook/job parameter.

## 3. Synthetic source files
### Generator & dirty-data inventory
[`generator/generate_data.py`](generator/generate_data.py) uses only the Python standard library and seed **42**. It writes exactly **1,000,000 data rows + header** per file, and the same seed always produces the same bytes (sha256 in [`generator/manifest.json`](generator/manifest.json)). Each row gets at most one injected anomaly, so the inventory is exact: [`generator/dirty_data_report.md`](generator/dirty_data_report.md).

| Category (minimum required) | orders.csv | transactions.csv |
|---|---|---|
| Null / blank values | 10,119 (all 8 fields) | 9,888 (all 7 fields) |
| Text inside numeric fields | 8,051 (quantity, unit_price) | 8,104 (amount) |
| Negative / out-of-range numbers | 7,947 | 7,008 |
| Invalid / future timestamps | 3,973 invalid + 3,003 future | 4,021 invalid + 2,920 future |
| Inconsistent case / whitespace | 29,834 | 29,662 |
| Duplicate rows / keys | 5,001 exact + 4,862 key conflicts | 4,993 exact + 4,985 key conflicts |
| Invalid status labels | 2,939 | 2,028 |
| Orphan order IDs | n/a | 10,216 |
| Unsupported currency | n/a | 4,027 |
| **Total anomalous rows** | **75,729 (7.57 %)** | **87,852 (8.79 %)** |

**Why anomalies must be a small, measured fraction:**
1. *Realism.* Production extracts are mostly correct. If half the rows were broken, Gold metrics would describe the noise, not the business.
2. *Testability.* Every count is known in advance, so Silver's quarantine and duplicate numbers can be checked against the generator (notebook 04 does this) and a rule that is too strict or too loose shows up as a mismatch.
3. *Attribution.* At one anomaly per row and low rates, each rejection reason can be traced to a single injected cause.
4. *Analytics stay useful.* About 95 % of orders survive, so trends and breakdowns remain meaningful.

### Upload: row counts, bytes, volume paths
Uploaded via **New → Add or upload data → Upload files to a volume**. Reproducible counts: [`sql/01_landing_row_counts.sql`](sql/01_landing_row_counts.sql) and [`notebooks/02_landing_checks.py`](notebooks/02_landing_checks.py), which also compares sha256 with the manifest and logs to `ops_landing_file_log`.

| File | Volume path | Data rows (excl. header) | Bytes | sha256 |
|---|---|---|---|---|
| orders.csv | `/Volumes/workspace/ecom_poc/raw_files/orders.csv` | 1,000,000 | 66,468,913 | checked against `generator/manifest.json` by `02_landing_checks` |
| transactions.csv | `/Volumes/workspace/ecom_poc/raw_files/transactions.csv` | 1,000,000 | 71,233,922 | checked against `generator/manifest.json` by `02_landing_checks` |

### Why real data, credentials and raw extracts must not be in Git
Git history is permanent and gets copied to every clone, fork, CI cache and backup. Deleting a file later does not remove it from history.
- **Real customer data** in a repo is a personal-data breach (GDPR and similar laws). It escapes all access control, auditing and retention rules.
- **Credentials** pushed to a remote are scraped by bots within minutes and can be used to read data or run up compute bills.
- **Raw extracts** are large, are often sensitive, and belong in governed storage (a UC volume) with access control and audit logs, not in version control. Git is for code and small metadata.

Controls applied here: [`.gitignore`](.gitignore) excludes `*.csv`, `*.parquet`, `.env`, `*token*` and `.databrickscfg`, and only synthetic data is used.

**Minimum production controls:**
- secret scanning with push protection on the Git host, plus a pre-commit scanner (e.g. gitleaks);
- secrets kept in Databricks secret scopes, or OAuth/service principals instead of personal tokens;
- non-production environments use synthetic or masked data only;
- source data lives in Unity Catalog storage with least-privilege grants and audit logs;
- protected main branch with mandatory review;
- a documented process to rotate any credential that is ever exposed.

## 4. Bronze — lossless ingestion
Notebook: [`notebooks/03_bronze.py`](notebooks/03_bronze.py). Checks: [`sql/02_bronze_checks.sql`](sql/02_bronze_checks.sql).

- **All source fields are STRING with an explicit schema** (`inferSchema=false`). `"two"`, `"12,50"`, `"2026-02-30"` and `" completed "` arrive exactly as sent. A structurally broken line fails the load (`FAILFAST`) instead of being silently dropped.
- **Why malformed data belongs in Bronze:** Bronze answers *"what did we receive?"*. Rejecting rows on load would destroy the evidence. You could no longer count bad rows, explain them, return them to the source owner, or reprocess them after a rule changes. Deciding *"what do we trust?"* is Silver's job, and Silver records every rejection with a reason.

| Audit column | Meaning |
|---|---|
| `_ingested_at` | Timestamp of the load run: when the data entered the lakehouse |
| `_source_file` | Full volume path of the CSV: lineage back to the landing zone |
| `_source_line` | 1-based data-row number in the file: points to the exact raw line and gives a stable order (used by dedup) |
| `_batch_id` | `<file>:<sha256 prefix of file content>`: same content means same batch, the idempotency key |
| `_row_hash` | sha256 of all source columns (NULL-aware): equal hash means an exact duplicate row |
| `_bronze_row_id` | `<_batch_id>#<_source_line>`: unique, stable row id carried into **every** Silver outcome |

**Lossless proof:** the notebook counts the file's data lines, counts the Bronze rows for that batch, writes both to `ops_bronze_load_log`, and fails if they differ.

**Idempotent re-runs:** the write is a Delta overwrite with `replaceWhere _batch_id = '<this batch>'`. Re-running with the same file *replaces* that batch instead of appending it again, so a second run leaves the count at 1,000,000 (shown by the second row in `ops_bronze_load_log`). A file with *different* content gets a new `_batch_id` and is loaded as a new batch, as it should be.

## 5. Silver — cleaning, validation & reconciliation
Notebook: [`notebooks/04_silver.py`](notebooks/04_silver.py). Checks: [`sql/03_silver_checks.sql`](sql/03_silver_checks.sql).

### Processing precedence
1. **Standardise:** trim; upper-case codes; `''`, `NULL` and `None` → missing. Case and whitespace problems are *fixed*, not rejected.
2. **Safe parse:** `try_cast` / `try_to_timestamp` with strict formats (`yyyy-MM-dd`, `yyyy-MM-dd HH:mm:ss`).
3. **Validate:** every rule is evaluated and **all** failures go into `rejection_reasons`. Any failure means quarantine.
4. **Deduplicate valid rows only:** one deterministic winner per key. The other valid rows are duplicate losers.

Because validation comes first, a row that is both invalid and a duplicate counts once, in quarantine. That makes the three outcomes a true partition of Bronze.

### Safe type conversion
**What happens to unparseable strings?** `try_cast('two' AS INT)` returns **NULL** instead of an error, so the job never crashes on bad data and never stores a wrong value. Silver keeps the original string next to the parsed value. A "present but parsed to NULL" value fails the matching *type* rule (e.g. `invalid_quantity`), and the row goes to quarantine with the raw text intact. Before/after examples are displayed by `notebooks/04_silver`.

### Business-rule matrix
Stored as table `ref_dq_rules`. Row counts per rule are in `ops_silver_rule_counts`.

| Dataset | Rule id | Field | Type | Rule |
|---|---|---|---|---|
| orders | `missing_<field>` (×8) | every column | mandatory | present, not blank/NULL |
| orders | `invalid_order_date` | order_date | type | real date, `yyyy-MM-dd` |
| orders | `future_order_date` | order_date | range | not after processing date |
| orders | `invalid_quantity` / `quantity_out_of_range` | quantity | type / range | whole number, 1–100 |
| orders | `invalid_unit_price` / `unit_price_out_of_range` | unit_price | type / range | plain decimal, 0 < price ≤ 100,000 |
| orders | `invalid_category` | category | allowed values | 8 categories (case-insensitive) |
| orders | `invalid_order_status` | order_status | allowed values | COMPLETED, SHIPPED, PENDING, CANCELLED, RETURNED |
| orders | `invalid_country` | country | allowed values | US, GB, DE, FR, IN, CA, AU, BD, NL, ES |
| transactions | `missing_<field>` (×7) | every column | mandatory | present, not blank/NULL |
| transactions | `invalid_amount` / `amount_out_of_range` | amount | type / range | plain decimal, 0 < amount ≤ 1,000,000 |
| transactions | `invalid_txn_ts` / `future_txn_ts` | txn_ts | type / range | real timestamp, not in the future |
| transactions | `unsupported_currency` | currency | allowed values | USD, EUR, GBP (`ref_fx_rates`) |
| transactions | `invalid_payment_method` | payment_method | allowed values | CARD, PAYPAL, WALLET, BANK_TRANSFER, COD |
| transactions | `invalid_txn_status` | txn_status | allowed values | SUCCESS, FAILED, PENDING, REFUNDED |
| transactions | `orphan_order_id` | order_id | relationship | must exist in accepted `silver_orders` |

The orphan check is against *accepted* orders, so a payment for a quarantined order is also quarantined. Otherwise Gold could count payments for orders it cannot report.

### Deterministic deduplication
| Dataset | Key | Keep-one rule |
|---|---|---|
| orders | `order_id` | `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _source_line DESC) = 1`: the latest line in the extract is the latest version of the order |
| transactions | `txn_id` | `ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY txn_ts DESC, _source_line DESC) = 1`: the latest payment status wins, with ties broken by line |

Both orderings are total (`_source_line` is unique), so the same input always produces the same winners. Each loser stores `winner_bronze_row_id` and a `duplicate_type`:
- **`exact_row_duplicate`:** same `_row_hash` as the winner, so every raw field is identical (e.g. a file resent twice).
- **`key_conflict`:** same business key, different content (e.g. a status update). This is a real data conflict that the keep rule resolves.

### Quarantine & reconciliation
Invalid rows are written to `silver_<ds>_quarantine` with the raw values, `rejection_reasons` (an array) and all lineage columns. Losers go to `silver_<ds>_duplicates`. Nothing is deleted. The notebook proves the partition with `_bronze_row_id` and stores it in `ops_silver_reconciliation`:
- the counts add up;
- no id appears in more than one outcome;
- no Bronze id is unassigned;
- no outcome id is unknown.

| Dataset | Bronze | Accepted | Quarantined | Duplicate losers | Sum | Overlap | Balanced |
|---|---|---|---|---|---|---|---|
| orders (expected from generator) | 1,000,000 | 954,105 | 36,032 | 9,863 | 1,000,000 | 0 | Yes |
| transactions (expected from generator) | 1,000,000 | 941,810 | 48,212 | 9,978 | 1,000,000 | 0 | Yes |

## 6. Gold — business-ready models
Notebook: [`notebooks/05_gold.py`](notebooks/05_gold.py). Metric SQL: [`sql/04_gold_metrics.sql`](sql/04_gold_metrics.sql).

### Revenue definition
**Revenue (USD)** = the order value (`quantity × unit_price`) of every order where:
1. `order_status` is **COMPLETED** or **SHIPPED**. CANCELLED and RETURNED (refunded) orders earn nothing; PENDING is not yet earned.
2. The order is **fully paid**: the sum of its **SUCCESS** payments, converted to USD, is ≥ the order value (0.05 USD tolerance for FX rounding).

- **Payments:** FAILED/PENDING attempts never count. REFUNDED payments don't count as paid.
- **Partial (split) payments** are added together.
- **Repeat charges** can't inflate revenue, because revenue is the *order value*, not the payment sum. The excess is shown separately as `overpaid_usd`.
- **Currency:** EUR/GBP payments are converted with `ref_fx_rates`. Revenue is reported in **USD**.
- **Revenue date:** `order_date`.
- **Source of truth:** `gold_fact_orders.revenue_usd`. Every mart, the dashboard, Genie and Power BI **sum this one column**.

| Order | Status | Value | Successful payments | Revenue |
|---|---|---|---|---|
| A | COMPLETED | 100.00 | 100.00 | **100.00** |
| B | COMPLETED | 100.00 | 50.00 + 50.00 (split) | **100.00** |
| C | SHIPPED | 100.00 | 100.00 + 100.00 (repeat) | **100.00** (overpaid 100.00) |
| D | COMPLETED | 100.00 | FAILED, then 100.00 | **100.00** |
| E | CANCELLED | 100.00 | REFUNDED | **0** |
| F | COMPLETED | 100.00 | none | **0** (unpaid) |

### Gold datasets (grain, key, assumptions)
**Avoiding fan-out:** transactions are aggregated to one row per `order_id` **before** the join, so the join is 1:1. The notebook checks that `gold_fact_orders` has exactly as many rows as `silver_orders`, and that total revenue is identical across all marts.

| Table | Grain | Unique key | Main measures / assumptions |
|---|---|---|---|
| `gold_fact_orders` | order | `order_id` | `revenue_usd`, `paid_usd`, `is_revenue_eligible`, `overpaid_usd` |
| `gold_daily_sales` | day | `order_date` | revenue, orders, `cancellation_rate` (re-aggregate as Σcancelled/Σorders) |
| `gold_revenue_breakdown` | day × category × country | `(order_date, category, country)` | revenue, units, orders |
| `gold_payment_success` | payment day × method | `(txn_date, payment_method)` | `success_rate = SUCCESS/(SUCCESS+FAILED)`; PENDING (not final) and REFUNDED excluded |
| `gold_top_customers` | customer (lifetime) | `customer_id` | revenue, AOV, `spend_rank` (ties → customer_id); not date-filterable |

Row counts and sample output are displayed by `notebooks/05_gold`.

**Documentation:** table and column comments are written to Unity Catalog (`COMMENT ON TABLE` / `ALTER COLUMN … COMMENT`), and informational primary keys are declared. The comments show up in Catalog Explorer, Genie and BI tools, and they prevent the usual misreadings:
- averaging a stored rate instead of re-aggregating it;
- adding `paid_usd` instead of `revenue_usd`;
- filtering the lifetime customer ranking by date;
- double-counting by joining at the wrong grain.

## 7. Databricks AI/BI
### Dashboard
Spec and dataset SQL: [`dashboards/README.md`](dashboards/README.md), [`sql/05_dashboard_datasets.sql`](sql/05_dashboard_datasets.sql).
- **5 KPI counters:** revenue, revenue orders, AOV, cancellation rate, payment success rate.
- **1 trend:** weekly revenue.
- **3 breakdowns:** category, country, payment method.
- **Top-customer table.**
- **Shared filters:** a global **date range** (mapped to `order_date` / `txn_date`) plus category and country.


### Genie Agent
Setup, instructions and question checks: [`dashboards/genie.md`](dashboards/genie.md). Verification SQL: [`sql/06_genie_verification.sql`](sql/06_genie_verification.sql).

Questions checked against trusted SQL:
1. Total revenue in Q1 2026.
2. Payment method with the highest success rate.
3. Top 3 categories by revenue in Germany.

## 8. Power BI Desktop
Guide: [`powerbi/README.md`](powerbi/README.md). Measures: [`powerbi/measures.dax`](powerbi/measures.dax). Report: `powerbi/ecom_sales_report.pbix`.

- **Connection:** native Databricks connector to the SQL warehouse in **Import** mode.
- **Why Import over DirectQuery:** the Gold marts are small and change once per pipeline run. Import gives fast visuals and lets the warehouse auto-stop. DirectQuery would keep the warehouse running for every interaction and only pays off for live or very large data.
- **Measures:** Total Revenue, Revenue Orders, AOV, Units Sold, Cancellation Rate, Payment Success Rate. They use the same definitions as the dashboard.

### Revenue cross-check (SQL vs AI/BI vs Power BI)
Scope for all three: **order_date 2026-01-01 – 2026-06-30, USD, no category/country filter**. Query: [`sql/07_cross_tool_revenue_check.sql`](sql/07_cross_tool_revenue_check.sql).

| Tool | Where |
|---|---|
| Databricks SQL | first query in `07_cross_tool_revenue_check.sql` |
| AI/BI dashboard | KPI K1 with the date filter set |
| Power BI | Total Revenue card with the date slicer set |

**Diagnosing a mismatch.** All three tools sum the same column, so a difference can only come from scope or typing. Check these in order:
1. The date filter isn't the same, or isn't inclusive.
2. A category or country filter is still applied.
3. A Power BI date column was imported as datetime, or shifted by timezone (fix: type **Date**).
4. Floating-point type instead of fixed decimal.
5. A stale Import refresh after a job re-run (fix: refresh).
6. Someone used a different definition. The last query in `sql/07` shows how far "sum of payments" or "all orders" would drift.

The monthly-split query in `sql/07` narrows down which month disagrees.

## 9. Lakeflow Jobs, security & cleanup
Details: [`operations/README.md`](operations/README.md). Job definition: [`operations/lakeflow_job.json`](operations/lakeflow_job.json). Grants: [`sql/08_grants.sql`](sql/08_grants.sql). Credits: [`sql/09_credit_usage.sql`](sql/09_credit_usage.sql).

- **Job graph:** `landing_checks → bronze → silver → gold` on serverless.
  - Retry once after 2 min (every task can safely re-run).
  - No retry for landing checks.
  - Timeouts, max 1 concurrent run, failure email.
  - **No schedule.**
- **Why Gold stops after a failed dependency:** the default *Run if = All succeeded* marks Gold *Upstream failed*. Gold therefore never republishes marts from incomplete or unreconciled Silver tables, and the last good Gold stays in place for the dashboards.
- **Least privilege:** group `ecom_reporting` gets USE CATALOG, USE SCHEMA, SELECT on the five Gold tables only, and *Can use* on the warehouse. It gets nothing on Bronze, Silver, ops tables or the raw volume. Testing: as a group member, a Gold query succeeds and a Silver/Bronze/volume access returns `PERMISSION_DENIED`.
- **Cleanup:** stop the warehouse, confirm no schedules, revoke the Power BI token, export the dashboard and `.pbix`, record credits, and push all commits. Checklist in `operations/README.md` §3.

## 10. Trial limitations & fallbacks
| Area | Observed | Fallback applied |
|---|---|---|
| Catalog creation | Used the existing writable `workspace` catalog (as the brief allows) | Catalog is a parameter; nothing hard-codes it except the SQL helper files |

## 11. Requirement traceability
Status: **Repo** = implemented and committed. **Run** = needs execution in the trial workspace.

| Brief requirement | Where it is met | Status |
|---|---|---|
| Own-email trial; cloud, region, dates, compute, restrictions; trial vs Free Edition | §2, §10 | Repo done · Run |
| `ecom_poc` schema + managed `raw_files` volume; naming standard; exact path | §2 · `sql/00_setup.sql` · `notebooks/01_setup` | Done |
| Notebook compute + SQL warehouse; ETL vs BI reason; credit saving | §2 | Done |
| Git folder; repo URL; commit per stage | §2 · `git log` | Done |
| 1,000,000 rows per file; ≥5 dirty categories; inventory; why small fraction | §3 · `generator/` | Done |
| Upload; exact rows, bytes, path; reproducible count | §3 · `sql/01` · `notebooks/02` | Repo done · Run |
| Why no real data / credentials / extracts in Git; production controls; `.gitignore` | §3 · `.gitignore` | Done |
| Bronze: one Delta table per CSV, all STRING, no inference; why keep malformed rows | §4 · `notebooks/03` | Repo done · Run |
| Audit fields `_ingested_at`, `_source_file`, `_batch_id`, `_row_hash` explained | §4 | Done |
| Lossless source = Bronze; idempotent re-run | §4 · `ops_bronze_load_log` · `sql/02` | Repo done · Run (run Bronze twice) |
| Safe parsing (`try_cast`); what happens to unparseable strings; before/after | §5 · `notebooks/04` | Repo done · Run |
| Rule matrix (ranges, mandatory, statuses, currencies, non-future, orphans); counts by rule | §5 · `ref_dq_rules` · `ops_silver_rule_counts` | Repo done · Run |
| Deterministic dedup; keep-one rule; exact rows vs key conflicts; winners/losers | §5 · `notebooks/04` · `sql/03` | Repo done · Run |
| Quarantine with `rejection_reasons`; Bronze = accepted + quarantined + losers, no overlap | §5 · `ops_silver_reconciliation` · `sql/03`, `sql/10` | Repo done · Run |
| Revenue definition (statuses, currency, refunds, partial/repeat payments, source of truth) + example | §6 · `notebooks/05` | Done |
| ≥4 Gold datasets; no fan-out; table list, row counts, samples | §6 · `notebooks/05` | Repo done · Run |
| Table/column descriptions; grain, key, assumptions; why it prevents misreading | §6 · UC comments | Repo done · Run |
| AI/BI dashboard: ≥4 KPIs, 1 trend, 2 breakdowns, 1 shared filter; link or export | §7 · `dashboards/README.md` | Spec done · Run |
| Genie: 3 questions cross-checked with SQL, or documented fallback | §7 · `dashboards/genie.md` · `sql/06` | Spec done · Run |
| Power BI native connector, Import; Import vs DirectQuery; connection settings (secrets masked) | §8 · `powerbi/README.md` | Repo done · Run |
| ≥4 measures; one-page report (KPI cards, trend, category chart, slicers); `.pbix` | §8 · `powerbi/measures.dax` | Spec done · Run |
| Same revenue in SQL, AI/BI and Power BI under identical filters; mismatch diagnosis | §8 · `sql/07` | Repo done · Run |
| Lakeflow Job Bronze → Silver → Gold; dependencies, retries, failure notification; one successful run; schedule disabled; why Gold stops | §9 · `operations/` | Repo done · Run |
| Least-privilege Gold-only access; executable grants; test plan | §9 · `sql/08` | Repo done · Run |
| Cleanup: stop warehouse, disable schedules, check credits, export before expiry | `operations/README.md` §3 · `sql/09` | Repo done · Run |
| Every trial limitation and fallback disclosed | §10 | Run: fill table |
| No tokens, credentials, PII or real data in the repo | `.gitignore` | Done |

## How to reproduce
1. `python generator/generate_data.py`. The output's sha256 must match `generator/manifest.json`.
2. In Databricks, clone the repo as a Git folder.
3. Run `notebooks/01_setup`.
4. Upload both CSVs to `/Volumes/workspace/ecom_poc/raw_files/`.
5. Run `02_landing_checks` → `03_bronze` → `04_silver` → `05_gold`, or run the Lakeflow Job.
6. Build the dashboard, Genie and Power BI from `dashboards/` and `powerbi/`.

## Repository layout
| Folder | Contents |
|---|---|
| `generator/` | Seeded generation script, dirty-data report, manifest (CSVs excluded) |
| `notebooks/` | `00_config`, `01_setup`, `02_landing_checks`, `03_bronze`, `04_silver` (incl. reconciliation), `05_gold`, `06_export_gold_for_powerbi` (fallback) |
| `sql/` | Setup, landing counts, Bronze/Silver checks, Gold metrics, dashboard datasets, Genie verification, cross-tool check, grants, credit usage, quality assertions |
| `dashboards/` | AI/BI dashboard spec and export, Genie setup and question checks |
| `powerbi/` | `.pbix`, DAX measures, connection guide (and Gold-only exports if the fallback was used) |
| `operations/` | Lakeflow Job JSON, least-privilege plan, credit tracking, cleanup checklist |
