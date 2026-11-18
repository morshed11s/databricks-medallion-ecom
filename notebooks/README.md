# Notebooks (Databricks source format)

Run them in this order, from the Databricks Git folder. Use serverless compute, with widget/job parameter `catalog` = `ecom_catalog`.

| # | Notebook | Layer | Fails the run if… |
|---|---|---|---|
| 00 | `00_config` | shared config (`%run` by all others) | n/a |
| 01 | `01_setup` | creates catalog `ecom_catalog`, schema `ecom_poc`, volume `raw_files`; copies CSVs from the old `workspace` volume if found | catalog cannot be created |
| (manual) | upload `orders.csv`, `transactions.csv` to the volume | landing | |
| 02 | `02_landing_checks` | landing | a file is missing or rows/bytes/sha256 ≠ `generator/manifest.json` |
| 03 | `03_bronze` | Bronze | source data rows ≠ Bronze rows |
| 04 | `04_silver` | Silver + reconciliation | Bronze ≠ accepted + quarantined + duplicates, or overlap |
| 05 | `05_gold` | Gold | grain not unique, fan-out, or revenue differs between marts |
| 06 | `06_export_gold_for_powerbi` | fallback only | n/a (not in the job) |

Notebooks 02–05 are the Lakeflow Job tasks (`operations/lakeflow_job.json`). All of them can be safely re-run.
