# Recovery plan

General rule: open the log of the **first** failed task (not the `upstream_failed` ones), read the error from the
bottom of the traceback up, then decide by failure type. Recovery ends when the quality checks pass, not when a
box turns green.

All scenarios below were executed on Airflow 3.3.2 (Docker). Times are Amsterdam time (CET, UTC+1) on 2026-10-27.

## Decision table

| Situation | Signal | Response |
|---|---|---|
| File not delivered yet | sensor still checking | Wait |
| File not delivered within 3 h | sensor fails (timeout) + alert | Contact the source owner; when the file lands, clear the sensor |
| Airflow or a container was down | tasks fail with empty logs | Fix the service; clear the failed tasks + downstream |
| Transient storage error in ingest | `up_for_retry` on `ingest_raw` | Automatic: 2 retries with backoff |
| Required column missing | `SchemaError`, no retry | Get a corrected file; trigger a new run with `force_reingest` (Scenario 1) |
| Different file re-delivered for an ingested date | `RawConflictError`, no retry | Confirm with the source; new run with `force_reingest` |
| Too many bad rows | `run_quality_checks` fails | Don't publish. Investigate; if the limit was wrong, fix it and clear the gate (Scenario 2) |
| Days never processed | gap in `audit_log.csv` | Backfill, oldest first (Scenario 3) |
| Our code was wrong | wrong numbers found later | Fix, deploy, backfill the affected dates |

## Scenario 1: schema break, partially completed run

Business date **2026-10-19**. The order file arrived without the `unit_price_gbp` column.

| Time | Event |
|---|---|
| 19:33:18 | `incident_2026-10-19` triggered with the broken file |
| 19:33:23 | `validate_orders` failed: `SchemaError: orders file is missing required column(s) ['unit_price_gbp']`. Try 1, no retries. Alert written with a next step. `build_daily_sales` → `publish_report`: `upstream_failed`. Nothing published |
| 20:04:44 | Corrected file received; `recovery_2026-10-19` triggered with conf `{"force_reingest": true}` |
| 20:04:46 | Broken raw file archived to `data/raw/_superseded/business_date=2026-10-19/20261027T190446Z/orders.csv` |
| 20:04:55 | All 9 tasks succeeded; 2026-10-19 published; audit `43 + 10 = 53`, `reconciled=True` |

Why a new run and not just "Clear": the run had already stored the broken file in raw. Clearing `ingest_raw` fails
immediately with `RawConflictError`, because raw is never overwritten silently. Replacing it is an operator decision,
recorded in the run's conf.

```bash
python scripts/make_sample_data.py 2026-10-19          # corrected re-delivery
docker compose exec airflow airflow dags trigger brightcart_daily_sales \
  --logical-date 2026-10-19T12:00:01+00:00 --run-id recovery_2026-10-19 --conf '{"force_reingest": true}'
```

## Scenario 2: quality gate failure

Business date **2026-10-22**. Variable `brightcart_max_reject_rate` set to `0.05` (normal: 0.30).

| Time | Event |
|---|---|
| — | Published report fingerprint before: `43003cb7aff2…` |
| 20:17 | `gate_test_2026-10-22` triggered. `run_quality_checks` failed: `customers_reject_rate 23.8%`, `orders_reject_rate 18.5%` (limit 5%). `publish_report`: `upstream_failed` |
| — | Published report fingerprint: still `43003cb7aff2…` (untouched) |
| 20:38 | Variable deleted (back to 30%); `run_quality_checks` cleared with downstream in the same run; gate passed, report republished |
| — | Published report fingerprint after: still `43003cb7aff2…` (byte-identical republish) |

Why "Clear" here: the input was fine and only a setting changed, so only the affected tasks of that run needed to
run again.

## Scenario 3: backfill of missed dates

Business dates **2026-10-20** and **2026-10-21** had never run.

1. Dry run first: exactly 2 logical dates listed.
2. Backfill with `--max-active-runs 1` (oldest first). `--to-date` must be `2026-10-21T23:59:59`: a run's logical
   date is 06:00 London on its day, so a plain `2026-10-21` (midnight) would leave that day out.
3. Result: `backfill__2026-10-21T05:00:00+00:00` (20 Oct) and `backfill__2026-10-22T05:00:00+00:00` (21 Oct)
   succeeded; both audit rows `True`; `published/latest` stayed on 2026-10-26.

```bash
docker compose exec airflow airflow backfill create --dag-id brightcart_daily_sales \
  --from-date 2026-10-20 --to-date 2026-10-21T23:59:59 --max-active-runs 1 --dry-run
```

Safe to repeat because every step overwrites its own `business_date=` folder atomically and the audit log replaces a
date's row. Tested in `evidence/idempotency_test.txt`: run, rerun and rerun after a simulated crash produced the
same output fingerprint (`8aa918d1758b4680`) and one audit row.

## Scenario 4: pausing and resuming after several days

The DAG was paused on 23 Oct and WSL was shut down. On 27 Oct:

- Run dates for manual commands were taken from fixed values (never "yesterday"), because relative dates point at
  different days after a pause.
- Postgres restarted by itself (`restart: unless-stopped`); `docker compose up -d` started Airflow; history intact.
- Before unpausing, input files for 2026-10-26 were provided. Unpausing created exactly one run, for the latest
  business day (`catchup=False`), which succeeded. 23–25 Oct were not created automatically.

## Scenario 5: file late or missing (designed, not executed)

Sensors check every 5 minutes for 3 hours (`mode="reschedule"`, no worker blocked), then fail and alert. When the
file arrives, clear the sensor with downstream. For a confirmed no-sales day, deliver a header-only file: the
report is empty and `has_valid_orders` records a warning instead of failing.

## Findings fixed along the way

| Finding | Fix |
|---|---|
| Alert links pointed to `localhost:8080`, but the UI is on 8081 | `AIRFLOW__API__BASE_URL=http://localhost:8081`, with `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8080/execution/` so tasks keep reporting internally. Older alerts keep their original links |
| `.env` was committed locally: an append glued `.dates` onto the last `.gitignore` line (`.env.dates`) | Caught before any push; `.gitignore` rewritten, `.env` removed with `git rm --cached` + `git commit --amend`. Prevention: test rules with `git check-ignore -v`, a global ignore file and a pre-commit hook that blocks `.env` |
| Line endings of CSV evidence were converted by Git (CRLF → LF), so raw files in Git no longer matched their manifest SHA-256 | `.gitattributes`: `data/** -text`; verified that the committed raw file matches its manifest |
