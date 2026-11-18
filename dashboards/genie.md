# Genie Agent: "E-commerce Gold Analyst"

Availability in this trial: ⟨Available / Not available: describe exactly what the UI shows⟩
(The workspace navigation shows **Genie Agents**.)

## Setup
1. **Genie Agents → New**. Warehouse: the serverless starter warehouse.
2. **Tables** (Gold only): `gold_fact_orders`, `gold_daily_sales`, `gold_revenue_breakdown`, `gold_payment_success`, `gold_top_customers` in `workspace.ecom_poc`.
3. **General instructions:** paste the block below.
4. **SQL examples (trusted):** add the three queries from [`sql/06_genie_verification.sql`](../sql/06_genie_verification.sql) with their questions as titles.

```text
You answer questions about an e-commerce business using the Gold tables only.
- Revenue means SUM(revenue_usd). It is already in USD and already excludes cancelled, returned,
  pending and unpaid orders. Never recompute revenue from quantity * unit_price or from payments.
- Revenue is dated by order_date. Payment metrics are dated by txn_date.
- Rates must be re-aggregated as a ratio of sums, never AVG of a stored rate:
  cancellation rate = SUM(cancelled_orders) / SUM(total_orders);
  payment success rate = SUM(successful) / (SUM(successful) + SUM(failed)).
- country holds ISO-2 codes: US, GB (United Kingdom), DE (Germany), FR, IN, CA, AU, BD, NL, ES.
- gold_top_customers is lifetime (no dates); use gold_fact_orders for customer spend in a date range.
- Data covers order dates 2025-11-01 to 2026-10-31.
```

## Question checks

Run each question in a **new chat**. Note the answer **and** its generated SQL ("Show code"), then run the trusted SQL.

| # | Question asked to Genie | Trusted SQL | Genie answer | Trusted SQL answer | Match? | Notes |
|---|---|---|---|---|---|---|
| Q1 | What was our total revenue in the first quarter of 2026? | `06_genie_verification.sql` Q1 | ⟨ ⟩ | ⟨ ⟩ | ⟨ ⟩ | |
| Q2 | Which payment method has the highest payment success rate? | Q2 | ⟨ ⟩ | ⟨ ⟩ | ⟨ ⟩ | |
| Q3 | What are the top 3 product categories by revenue in Germany? | Q3 | ⟨ ⟩ | ⟨ ⟩ | ⟨ ⟩ | |
| (Q4) | What is the overall cancellation rate? | Trap check | ⟨ ⟩ | ⟨ ⟩ | ⟨ ⟩ | AVG-of-rates ≠ correct value |

**What to look for in Genie's SQL:**
- the right table;
- `SUM(revenue_usd)`, not `quantity * unit_price`;
- the right date column and an inclusive date range;
- `'DE'`, not `'Germany'`;
- a ratio of sums for rates.

If an answer is wrong, add or adjust an instruction or trusted example, ask again and record both attempts. That is the point of verifying.

## Fallback (only if Genie is unavailable)
Record the restriction (the missing menu or the exact error message) in README §10. Keep the three intended prompts above and provide the trusted SQL results from `sql/06_genie_verification.sql` as the assessment answers.
