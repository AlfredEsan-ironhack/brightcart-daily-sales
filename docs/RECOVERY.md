# Recovery plan

General rule: read the log of the **first** failed task (not the `upstream_failed` ones), find the first
ERROR line, then decide by failure type. Recovery ends with the quality checks passing, not just a green box.

Every scenario below was run for real on Airflow 3.3.2; run IDs refer to `evidence/airflow_dag_runs.txt`.

## Decision table

| Situation | Signal | Response |
|---|---|---|
| File not delivered yet | sensor still checking | Wait; nothing to do |
| File not delivered in 3 h | sensor fails (timeout) + alert | Contact source owner; when the file lands, clear the sensor |
| Airflow infrastructure down | tasks fail with empty logs | Fix the service; clear failed tasks + downstream |
| Transient storage error in ingest | `up_for_retry` on `ingest_raw` | Automatic: 2 retries with backoff |
| Required column missing | `SchemaError`, no retry | Get a corrected file; trigger a new run with `force_reingest` |
| Different file re-delivered | `RawConflictError`, no retry | Confirm with source; trigger a new run with `force_reingest` |
| Too many bad rows | `run_quality_checks` fails | Don't publish. Investigate the source; previous report stays published |
| Our code was wrong | wrong numbers found later | Fix, deploy, backfill the affected dates |
| Days missed | gap in `audit_log.csv` | Backfill the dates (below) |

## Scenario 1 - Schema break, partially completed run (tested)

Run `incident_2026-10-19`: the orders file arrived without `unit_price_gbp`.

```
wait_for_customers_file  success
wait_for_orders_file     success
ingest_raw               success      raw now holds the broken file
validate_customers       success
validate_orders          failed       SchemaError: missing ['unit_price_gbp']
build_daily_sales .. publish_report   upstream_failed
```

Nothing was published; `published/latest` still pointed at the previous day. The alert named the task, the
error, the log URL and the next step.

Recovery:

1. Ask the source to re-deliver a corrected file to `data/incoming/orders_2026-10-19.csv`.
2. Clearing `ingest_raw` alone is **not** enough: it fails with `RawConflictError` (raw already holds a
   different file for that date). This is deliberate: raw is never overwritten silently. (Tested: it
   failed at once, without retrying.)
3. Trigger a new run with the operator decision in its config:
   ```bash
   airflow dags trigger brightcart_daily_sales \
     --logical-date 2026-10-19T05:00:01+00:00 --run-id recovery_2026-10-19 \
     --conf '{"force_reingest": true}'
   ```
   In the UI: Trigger DAG, set the logical date, paste the conf.
4. Result (`recovery_2026-10-19`): every task succeeded. The broken file is kept in
   `raw/_superseded/business_date=2026-10-19/<timestamp>/orders.csv`. Customers were skipped (same
   checksum). All 10 checks passed; the 19th was published and became `latest`.
5. Validate: `audit_log.csv` row for 2026-10-19 shows `53 = 43 + 10`, `reconciled=True`.

Why a partial run is safe to redo: each step overwrites its own `business_date=` folder atomically, so the
rerun replaces whatever the failed run left behind instead of adding to it.

## Scenario 2 - Infrastructure failure (tested)

Runs `scheduled__2026-10-19T05:00:00+00:00` and `backfill__2026-10-17T05:00:00+00:00` failed while the
Airflow API server was down: sensors `failed` with **empty** logs (they never started). Empty log + every
task failing at once points at the platform, not the data.

Recovery: restart the service, then

```bash
airflow tasks clear brightcart_daily_sales --only-failed --downstream \
  --start-date 2026-10-16 --end-date 2026-10-19 --yes
```

Both runs reran (sensors on try 2) and succeeded. The 18th had already been processed by a manual run;
rerunning it changed nothing (ingest skipped on matching checksum, outputs overwritten with identical
content, still one row in `audit_log.csv`).

## Scenario 3 - Quality gate failure (tested)

Run `gate_test_2026-10-18`: Variable `brightcart_max_reject_rate` set to `0.05`.
`run_quality_checks` failed (23.8% / 18.2% rejected vs 5% limit); `publish_report` was `upstream_failed`.
The published report for the 18th was byte-for-byte unchanged (checksum compared).

Recovery: do not force a publish. Read `audit/business_date=<date>/quality_checks.json` and the rejected
files, contact the source owner, and rerun once the cause is fixed (corrected file -> `force_reingest`
run; threshold genuinely wrong -> change the Variable, clear `run_quality_checks` and `publish_report`).

## Scenario 4 - Backfill missed dates (tested)

1. Check the date range: which `business_date` values are missing from `audit_log.csv`?
2. Check rerun safety: outputs are overwritten per date and the audit log upserts per date, so a date
   processed twice is harmless. Source files for every date must be in `incoming/`.
3. Run oldest first, one at a time:
   ```bash
   airflow backfill create --dag-id brightcart_daily_sales \
     --from-date 2026-10-16 --to-date 2026-10-17T23:59:59 --max-active-runs 1
   ```
   Note the `--to-date`: with this timetable the run for the 17th has logical date 17 Oct 06:00 London,
   so a `--to-date` of plain `2026-10-17` (midnight) would leave the 17th out. Use `--dry-run` first.
4. Validate each date: `reconciled=True` in `audit_log.csv`, all checks passed, `_SUCCESS` present.
5. `published/latest` is unaffected by a backfill of older dates (it only moves forward).

## Scenario 5 - File late or missing (designed, not tested end to end)

Sensors check every 5 minutes for 3 hours (`mode="reschedule"`, so no worker is blocked), then fail and
alert. When the file arrives: clear `wait_for_*_file` with downstream. If the business confirms there were
no sales that day, supply an empty file with only the header row: the pipeline produces an empty report and
the `has_valid_orders` check records a warning instead of failing.
