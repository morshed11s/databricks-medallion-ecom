# Synthetic data generator

`generate_data.py` writes `orders.csv` and `transactions.csv`, each with exactly 1,000,000 data rows plus a header. It uses a fixed seed (`42`) and only the Python standard library.

```bash
python generator/generate_data.py              # full run: about 1-2 min
python generator/generate_data.py --rows 10000 # quick test
```

| Output | Location | Committed to Git? |
|---|---|---|
| `orders.csv`, `transactions.csv` | `generator/output/` | **No.** Excluded by `.gitignore` |
| `dirty_data_report.md` / `.json` | `generator/` | Yes. Lists every injected anomaly by type, field, count and % |
| `manifest.json` | `generator/` | Yes. Row counts, byte sizes and SHA-256 for each file |

## Data design
- **Orders:** 8 categories with realistic price ranges, 10 countries, and 50k customers. A few customers place many orders.
- **Status mix:** COMPLETED 70%, SHIPPED 10%, PENDING 6%, CANCELLED 9%, RETURNED 5%. Order dates run from 2025-11-01 to 2026-10-31.
- **Transactions:** payment attempts for clean orders, priced in local currency. GB pays in GBP; DE, FR, NL and ES pay in EUR; every other country pays in USD. Fixed rates are USD 1.00, EUR 0.92, GBP 0.79.
- **Realistic payment patterns:** a failed first attempt followed by a retry (the failure rate depends on the payment method), split payments (two partial SUCCESS payments), accidental repeat charges (two full SUCCESS payments), refunds for returns and cancellations, and PENDING payments.
- **Dirty data:** each row gets at most one injected problem, so the inventory is exact. See `dirty_data_report.md`.
- The last clean orders by ID may have no transaction, because the transaction file stops at exactly 1,000,000 rows.
