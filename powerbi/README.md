# Power BI Desktop

| Item | File |
|---|---|
| Report | `powerbi/ecom_sales_report.pbix` |
| Measures (DAX) | [`powerbi/measures.dax`](measures.dax) |
| Fallback exports (only if used) | `powerbi/gold_exports/*.csv` via [`notebooks/06_export_gold_for_powerbi.py`](../notebooks/06_export_gold_for_powerbi.py) |

## 1. Connect (native connector, Import)
1. In Databricks: **SQL Warehouses → (warehouse) → Connection details**. Copy *Server hostname* and *HTTP path*.
2. In Power BI Desktop: **Get data → Azure Databricks** (or **Databricks**). Paste the hostname and HTTP path. Data connectivity mode: **Import**.
3. Authentication: **OAuth / Microsoft Entra ID** if offered, otherwise a **personal access token** with a short lifetime (e.g. 7 days). The token is never saved in the repo. Revoke it when finished (`operations/README.md` cleanup).
4. Navigator: `workspace → ecom_poc` → select **only** `gold_daily_sales`, `gold_revenue_breakdown`, `gold_payment_success`, `gold_top_customers` → *Load*.

### Import vs DirectQuery

| | **Import** (chosen) | DirectQuery |
|---|---|---|
| Where queries run | In Power BI's in-memory engine after a refresh | On the Databricks SQL warehouse for every visual/slicer change |
| Warehouse cost | Warehouse runs only during refresh, then auto-stops | Warehouse must stay running while anyone uses the report |
| Speed | Very fast interactions | Depends on warehouse start-up and query latency |
| Freshness | As of last refresh | Live |
| Size limits | Fine here (Gold marts are small) | None in Power BI |
| DAX | Full DAX | Some functions restricted |

**Why Import:** the Gold marts are small aggregates that refresh once per pipeline run. Import gives fast visuals and lets the SQL warehouse auto-stop, which matters on a credit-limited trial. DirectQuery would only be worth it for near-real-time data or tables too large to import.

## 2. Model
- Calculated table `Date` from [`measures.dax`](measures.dax). Mark it as the date table.
- Relationships (single direction, one-to-many from `Date[Date]`): `gold_daily_sales[order_date]`, `gold_revenue_breakdown[order_date]`, `gold_payment_success[txn_date]`. `gold_top_customers` is lifetime and has no relationship.
- Set `order_date` / `txn_date` columns to type **Date** (no time part), so date-range filters match Databricks exactly.
- Set the money columns to **Fixed decimal number**, which avoids floating-point drift in totals.

## 3. Measures (≥ 4)
`Total Revenue (USD)`, `Revenue Orders`, `Avg Order Value (USD)`, `Units Sold`, `Cancellation Rate`, `Payment Success Rate`. These are the same definitions as the AI/BI dashboard: revenue is `SUM(revenue_usd)` and rates are ratios of sums.

## 4. One-page report "E-commerce Sales Overview"

| Area | Visual |
|---|---|
| Top row | KPI cards: Total Revenue, Revenue Orders, Avg Order Value, Cancellation Rate, Payment Success Rate |
| Middle left | Line chart: `Date[Week Start]` × Total Revenue (trend) |
| Middle right | Bar chart: `gold_revenue_breakdown[category]` × Total Revenue (category chart) |
| Bottom left | Bar chart: `gold_revenue_breakdown[country]` × Total Revenue |
| Bottom right | Bar chart: `gold_payment_success[payment_method]` × Payment Success Rate |
| Slicers | `Date[Date]` (between), `category`, `country` |

Add a text box with the revenue definition (same wording as the dashboard note).

## 5. Revenue agreement
Set the date slicer to **2026-01-01 – 2026-06-30** and clear the category and country slicers. The Total Revenue card must equal the first query in [`sql/07_cross_tool_revenue_check.sql`](../sql/07_cross_tool_revenue_check.sql) and dashboard KPI K1. Record the three values in README §8.

## Fallback: Gold-only CSV export
Use this only if the connector cannot authenticate or reach the warehouse:
1. Run `notebooks/06_export_gold_for_powerbi`.
2. Download the four CSVs from the `exports` volume into `powerbi/gold_exports/`.
3. In Power BI, use **Get data → Text/CSV** for each file. Set the column types as in §2.

Disclose this in README §10. Model, measures and report stay identical.
