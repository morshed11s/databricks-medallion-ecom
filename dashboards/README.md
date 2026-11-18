# Databricks AI/BI

| Item | File |
|---|---|
| Dashboard build spec (this page) | `dashboards/README.md` |
| Dashboard dataset SQL | [`sql/05_dashboard_datasets.sql`](../sql/05_dashboard_datasets.sql) |
| Genie Agent setup and question checks | [`dashboards/genie.md`](genie.md) |
| Genie verification SQL | [`sql/06_genie_verification.sql`](../sql/06_genie_verification.sql) |
| Exported dashboard definition | `dashboards/ecom_sales_overview.lvdash.json` (export after building: ⋮ → *Export*) |

## Dashboard: "E-commerce Sales Overview"

**Compute:** the trial's serverless SQL warehouse (smallest size, auto-stop 10 min).
**Data:** Gold tables only. No Bronze/Silver, so every number has the documented Gold grain and the single revenue definition.

### 1. Datasets (Data tab)
Create one dataset per query in [`sql/05_dashboard_datasets.sql`](../sql/05_dashboard_datasets.sql), in file order:

| Dataset | Source | Grain | Custom calculation |
|---|---|---|---|
| `ds_daily_sales` | `gold_daily_sales` | order_date | `cancellation_rate = SUM(cancelled_orders) / SUM(total_orders)` |
| `ds_revenue_breakdown` | `gold_revenue_breakdown` | order_date × category × country | `aov_usd = SUM(revenue_usd) / SUM(revenue_orders)` |
| `ds_payment_success` | `gold_payment_success` | txn_date × payment_method | `success_rate = SUM(successful) / (SUM(successful) + SUM(failed))` |
| `ds_top_customers` | `gold_top_customers` (top 20) | customer, lifetime | none |

Ratios are computed as **SUM/SUM** so they stay correct after filtering. Averaging a stored daily rate would be wrong.

### 2. Widgets (Canvas)

| # | Widget | Type | Dataset | Value |
|---|---|---|---|---|
| K1 | Total revenue (USD) | Counter | ds_revenue_breakdown | `SUM(revenue_usd)` |
| K2 | Revenue orders | Counter | ds_revenue_breakdown | `SUM(revenue_orders)` |
| K3 | Average order value (USD) | Counter | ds_revenue_breakdown | `aov_usd` |
| K4 | Cancellation rate | Counter (%) | ds_daily_sales | `cancellation_rate` |
| K5 | Payment success rate | Counter (%) | ds_payment_success | `success_rate` |
| T1 | Revenue trend | Line | ds_daily_sales | X = `order_date` (weekly), Y = `SUM(revenue_usd)` |
| B1 | Revenue by category | Bar | ds_revenue_breakdown | X = `category`, Y = `SUM(revenue_usd)`, sorted desc |
| B2 | Revenue by country | Bar | ds_revenue_breakdown | X = `country`, Y = `SUM(revenue_usd)`, sorted desc |
| B3 | Success rate by payment method | Bar | ds_payment_success | X = `payment_method`, Y = `success_rate` |
| C1 | Top 20 customers (lifetime) | Table | ds_top_customers | rank, customer, revenue, orders |

### 3. Filters (shared across widgets)

| Filter | Type | Field per dataset |
|---|---|---|
| **Date range** (global) | Date range picker | `ds_daily_sales.order_date`, `ds_revenue_breakdown.order_date`, `ds_payment_success.txn_date` |
| Category | Multi-select | `ds_revenue_breakdown.category` |
| Country | Multi-select | `ds_revenue_breakdown.country` |

Notes shown as a text widget on the dashboard:
- *Revenue = paid COMPLETED/SHIPPED orders, in USD, dated by order date (see README §6).*
- *Category/Country filters apply to revenue widgets only. Cancellation rate and payment success have no category grain.*
- *Payment success is dated by payment date (`txn_date`).*
- *Top customers is lifetime and ignores the date filter.*

### 4. Revenue check
Set the date filter to **2026-01-01 → 2026-06-30**, clear Category/Country and read K1. It must equal the date-range query (second query) in [`sql/04_gold_metrics.sql`](../sql/04_gold_metrics.sql) and the Power BI card (README §8).

### 5. Publish & share
*Publish* with the warehouse's embedded credentials and share with the reporting group (`ecom_reporting`, see `operations/`). If sharing is restricted in the trial, export the `.lvdash.json` and a PDF instead, and record it in README §10.
