# Operations: Lakeflow Job, access control, credits & cleanup

## 1. Lakeflow Job `ecom_medallion_bronze_silver_gold`

Definition: [`lakeflow_job.json`](lakeflow_job.json) (Jobs API 2.2 format). Before using it, replace `<GIT_FOLDER_PATH>` with the Git folder path (e.g. `/Workspace/Users/<you>/databricks-medallion-ecom`) and `<YOUR_EMAIL>` with your own email. Don't commit the filled-in copy.

```
landing_checks ──▶ bronze ──▶ silver ──▶ gold
```

### Create it (UI)
1. **Jobs & Pipelines → Create → Job**. Name it `ecom_medallion_bronze_silver_gold`.
2. Add task `landing_checks`: type *Notebook*, source *Workspace*, path to `notebooks/02_landing_checks` in the Git folder, compute **Serverless**.
3. Add `bronze` (`03_bronze`), `silver` (`04_silver`) and `gold` (`05_gold`). Each one **depends on** the previous task, with *Run if: All succeeded*.
4. Job parameters: `catalog = workspace`. Job parameters are passed to the notebooks' `catalog` widget.
5. Per task: retries **1**, interval **2 min**, *retry on timeout* off, and the timeouts from the JSON. Max concurrent runs **1**.
6. Job notifications: **On failure → email** (your own address).
7. **No schedule/trigger.** Click *Run now* once. If you want to demonstrate a schedule, add one for a single run a few minutes ahead, then **pause or delete it** straight after.

Alternatively, paste the JSON into *Edit as JSON/YAML*, or run `databricks jobs create --json @operations/lakeflow_job.json` with the placeholders filled in.

### Why these settings
| Setting | Reason |
|---|---|
| Serverless compute | No cluster to size or forget. Billed only while tasks run. |
| `landing_checks` first | Stops the pipeline before Bronze if a file is missing or changed (sha256 ≠ manifest). |
| Retries = 1, after 2 min | Covers transient failures (serverless start-up, cloud storage hiccups). Every task can safely re-run: Bronze `replaceWhere` per batch, Silver overwrite, Gold `CREATE OR REPLACE`. Data-quality assertions fail the same way every time, so more retries would only burn credits. |
| No retry for `landing_checks` | Its failure always means the input is wrong. A retry cannot fix it. |
| `max_concurrent_runs = 1` + queue | Two overlapping runs would overwrite the same Silver/Gold tables. |
| Timeouts | A stuck run can't consume credits indefinitely. |
| Failure email | Someone learns a run failed without opening the UI. |

### Why Gold does not run after a failed dependency
Each task's default *Run if* condition is **All succeeded**. If Silver fails, for example because the reconciliation assertion `Bronze = accepted + quarantined + duplicates` is false, Gold is marked **Upstream failed** and never starts. That is the intended behaviour. Gold would otherwise rebuild the marts from Silver tables that are incomplete or from a previous run, and dashboards would publish wrong numbers that look trustworthy. The last good Gold tables stay in place until a run passes all checks. A *Repair run* re-runs only the failed task and the tasks after it.

## 2. Least-privilege reporting access

Grant plan: [`sql/08_grants.sql`](../sql/08_grants.sql).

| Principal | Catalog | Schema | Tables | Volume | Warehouse |
|---|---|---|---|---|---|
| `ecom_reporting` (report readers, Power BI user) | USE CATALOG | USE SCHEMA | SELECT on the **5 Gold tables only** | none | Can use |
| Pipeline owner (me / job run-as) | owner | owner | all | READ/WRITE | Can manage |

Reporting users can query Gold but can't see Bronze/Silver (raw and rejected records), the raw CSVs, or the ops tables, and can't create or modify anything.

**Testing:**
1. Add a second identity you own to the group.
2. As that identity, run:
   - `SELECT COUNT(*) FROM workspace.ecom_poc.gold_daily_sales;` succeeds
   - `SELECT COUNT(*) FROM workspace.ecom_poc.silver_orders;` returns `PERMISSION_DENIED`
   - `SELECT COUNT(*) FROM workspace.ecom_poc.bronze_transactions;` returns `PERMISSION_DENIED`
   - `LIST '/Volumes/workspace/ecom_poc/raw_files/';` returns `PERMISSION_DENIED`
   - `CREATE TABLE workspace.ecom_poc.x (a INT);` returns `PERMISSION_DENIED`
3. Record the `SHOW GRANTS` output, and repeat it after a job re-run to confirm the Gold grants persist.

If adding users or groups is restricted in the trial, submit `sql/08_grants.sql` as the executable plan with the `SHOW GRANTS` output, and record the restriction in README §10.

## 3. Credits & end-of-trial cleanup

Credit check: **Manage Trial** panel (top bar) and [`sql/09_credit_usage.sql`](../sql/09_credit_usage.sql) (system billing tables).

| Date | Credits remaining | Source |
|---|---|---|
| 2026-11-16 | $394 of $400 | Manage Trial panel |

### Cleanup checklist
- [ ] SQL warehouse **stopped**, auto-stop set to 10 min (the smallest allowed)
- [ ] Job has **no active schedule** (Jobs → job → *Schedules & Triggers* shows none, or paused)
- [ ] No running notebook sessions (serverless sessions detached)
- [ ] Power BI personal access token **revoked** (Settings → Developer → Access tokens)
- [ ] Dashboard exported (`.lvdash.json` + PDF) to `dashboards/`
- [ ] `.pbix` saved to `powerbi/`
- [ ] All commits **pushed** to GitHub
- [ ] Credits remaining recorded above
- [ ] No payment method added, no upgrade, no extra cloud resources
