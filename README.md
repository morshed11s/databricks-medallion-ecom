# Trustworthy E-commerce Lakehouse — Databricks Medallion Assignment

Bronze → Silver → Gold lakehouse on a Databricks free trial, using reproducible synthetic orders and payment data.

> All screenshots, row counts and links in this repository come from my own trial workspace.

## 1. Business context
_TODO_

## 2. Trial workspace & environment
- Cloud / provider: _TODO_
- Region: _TODO_
- Trial start / expiry: _TODO_
- Workspace URL (no tokens): _TODO_
- Compute choices (serverless notebooks for ETL, SQL warehouse for BI): _TODO_
- Observed account restrictions: _TODO_
- Trial vs Free Edition: _TODO_

### Unity Catalog setup & naming standard
_TODO_

### Git integration
_TODO_

## 3. Synthetic source files
### Generator & dirty-data inventory
_TODO_

### Upload: row counts, bytes, volume paths
_TODO_

### Why real data / credentials / raw extracts must not be in Git
_TODO_

## 4. Bronze — lossless ingestion
_TODO_

## 5. Silver — cleaning, validation & reconciliation
### Safe type conversion
_TODO_

### Business-rule matrix
_TODO_

### Deterministic deduplication
_TODO_

### Quarantine & reconciliation
_TODO_

## 6. Gold — business-ready models
### Revenue definition
_TODO_

### Gold datasets (grain, key, assumptions)
_TODO_

## 7. Databricks AI/BI
### Dashboard
_TODO_

### Genie Agent
_TODO_

## 8. Power BI Desktop
_TODO_

### Revenue cross-check (SQL vs AI/BI vs Power BI)
_TODO_

## 9. Lakeflow Jobs, security & cleanup
_TODO_

## 10. Trial limitations & fallbacks
_TODO_

## Repository layout
| Folder | Contents |
|---|---|
| `generator/` | Seeded generation script and dirty-data report (CSVs excluded) |
| `notebooks/` | Setup, Bronze, Silver, reconciliation, Gold notebooks |
| `sql/` | Rules, grants, quality assertions, Gold metrics, cross-tool validation |
| `dashboards/` | AI/BI references, screenshots, Genie checks |
| `powerbi/` | `.pbix` and screenshot |
| `evidence/` | Topic-labelled screenshots and output snippets |
| `operations/` | Job config, least-privilege evidence, credit check, cleanup notes |
