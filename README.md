# BrightCart Daily Sales Pipeline

A daily batch pipeline that turns BrightCart's customer and order files into a trusted daily sales report.
It keeps the raw files unchanged, validates every row, keeps rejected rows with the reason, writes audit
evidence that the counts reconcile, and only publishes the report after a quality gate passes.
Apache Airflow orchestrates it; the data logic is plain Python that also runs without Airflow.

```
incoming/ ──► [wait for both files] ──► ingest_raw ──► validate_customers ──► validate_orders
                                                                                   │
published/ ◄── publish_report ◄── run_quality_checks ◄── write_audit ◄── build_daily_sales
```

## Layers

| Layer | Folder | Contents | Rule |
|---|---|---|---|
| Landing | `data/incoming/` | `customers_<date>.csv`, `orders_<date>.csv` as delivered | Source system writes here |
| Raw | `data/raw/business_date=<date>/` | Byte-for-byte copies + `_manifest.json` (SHA-256, rows, time) | Never edited, never silently overwritten |
| Valid | `data/valid/business_date=<date>/` | Cleaned, typed rows that passed every rule | Each row keeps `source_row_number` |
| Rejected | `data/rejected/business_date=<date>/` | Original rows that failed, with `rejection_reasons` | Nothing is dropped silently |
| Curated | `data/curated/business_date=<date>/daily_sales.csv` | Daily sales by country | Grain: one row per date per country |
| Audit | `data/audit/` | `audit.json`, `quality_checks.json` per date, `audit_log.csv` across dates | Recounted from files on disk |
| Published | `data/published/` | Report copy + `_SUCCESS` per date, and `latest/` | Only written after the gate passes |

Every output for a date lives in its own `business_date=` folder, so a rerun or backfill rewrites only that
date. All files are written atomically (temp file, then rename), so a crash never leaves half a file.

## Project structure

```
brightcart/
├── dags/brightcart_daily_sales.py   Airflow DAG (orchestration only)
├── pipeline/
│   ├── config.py      paths, entities, business rules, thresholds
│   ├── io_utils.py    atomic writes, checksums, text-only CSV reading
│   ├── ingest.py      step 1: landing -> raw
│   ├── validate.py    step 2: raw -> valid + rejected
│   ├── report.py      step 3: valid -> curated daily sales
│   ├── audit.py       step 4: audit evidence
│   ├── checks.py      step 5: quality gate
│   ├── publish.py     step 6: publish
│   └── alerts.py      on_failure_callback
├── scripts/make_sample_data.py      generates the sample source files
├── run_pipeline.py                  runs every step without Airflow
├── tests/test_pipeline.py           25 unit tests for the rules
├── docs/VALIDATION_RULES.md         every rule, and the rows rejected
├── docs/RECOVERY.md                 failure scenarios and recovery steps
├── evidence/                        Airflow run states, sample alerts
└── data/                            sample inputs and outputs (2026-10-16 .. 2026-10-19)
```

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run without Airflow (Day 1)

```bash
python scripts/make_sample_data.py 2026-10-16 2026-10-17 2026-10-18
python run_pipeline.py --date 2026-10-18
python -m pytest -q                       # 25 passed
```

Each step prints its counts; the run ends with the 10 quality checks and `Done: 2026-10-18 published.`
Try `--max-reject-rate 0.05` to watch the quality gate stop the publish.

## Run with Airflow (Day 2)

```bash
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db migrate
airflow connections add brightcart_fs --conn-type fs --conn-extra "{\"path\": \"$(pwd)/data\"}"
airflow standalone                        # UI at http://localhost:8080, login printed in the terminal
```

Then, in a second terminal (same exports):

```bash
airflow dags test brightcart_daily_sales 2026-10-18                        # one full run, in order
airflow backfill create --dag-id brightcart_daily_sales \
    --from-date 2026-10-16 --to-date 2026-10-17T23:59:59 --max-active-runs 1   # rebuild past dates
```

Optional Airflow Variables (change behaviour without changing code):

| Variable | Default | Meaning |
|---|---|---|
| `brightcart_max_reject_rate` | `0.30` | Max share of rejected rows per entity before the gate fails |
| `brightcart_freshness_hours` | `24` | Max age of the report when the gate runs |

**Schedule and dates.** The DAG runs daily at 06:00 London time. One run = one business day: the run that
starts on the 19th has `ds = 2026-10-18` and processes the 18th's files. This uses
`CronDataIntervalTimetable`, which behaves the same in Airflow 2 and 3 (Airflow 3's default cron timetable
would give `ds` = the run day instead). 06:00 London is the same calendar date in UTC all year, so `ds`
never shifts with summer time. `catchup=False` (history is rebuilt only by deliberate backfill) and
`max_active_runs=1` (dates run one at a time).

## Task design

| Task | Retries | Why |
|---|---|---|
| `wait_for_customers_file`, `wait_for_orders_file` | n/a (sensor) | Check every 5 min, `mode="reschedule"` (no worker held), time out after 3 h and alert |
| `ingest_raw` | 2, exponential backoff | Only step exposed to transient storage errors |
| `validate_*`, `build_daily_sales`, `write_audit`, `run_quality_checks`, `publish_report` | 0 | Deterministic: the same input gives the same failure, so fix, don't retry |

Deterministic failures (`SchemaError`, `RawConflictError`, missing file) raise `AirflowFailException`, which
fails the task at once and skips retries. Every failure after retries calls `on_failure_alert`, which writes
an actionable alert (DAG, task, run, business date, try, error, log URL, next step) to the log and to
`data/alerts/alerts.jsonl`.

## How to verify a run

1. `data/audit/audit_log.csv`: one row per date; `reconciled=True` means source = valid + rejected for both files.
2. `data/audit/business_date=<date>/quality_checks.json`: all 10 checks with their numbers.
3. `data/published/business_date=<date>/_SUCCESS`: present only if the gate passed (contains the report checksum).
4. `data/rejected/...`: every rejected row with its reasons and original values.

The quality gate:

| Check | Proves |
|---|---|
| `customers_reconcile`, `orders_reconcile` | No row was lost or invented |
| `no_duplicate_valid_orders` | Each order counted once |
| `report_orders_tie_out` | Report orders = valid orders |
| `report_revenue_tie_out` | Report revenue = revenue recomputed from valid orders |
| `report_date_matches` | Report is for the right day |
| `report_fresh` | Report was written within the freshness window |
| `customers_reject_rate`, `orders_reject_rate` | Source quality is within tolerance |
| `has_valid_orders` (warn) | Flags a possible no-sales day without blocking it |

## Results on the sample data

| business_date | customers source / valid / rejected | orders source / valid / rejected | net revenue GBP | reconciled |
|---|---|---|---|---|
| 2026-10-16 | 21 / 16 / 5 | 50 / 40 / 10 | 881.13 | True |
| 2026-10-17 | 21 / 16 / 5 | 51 / 41 / 10 | 1,133.39 | True |
| 2026-10-18 | 21 / 16 / 5 | 55 / 45 / 10 | 804.94 | True |
| 2026-10-19 | 21 / 16 / 5 | 53 / 43 / 10 | 933.48 | True |

The sample data is deliberately dirty (each file contains the same planted errors), which is why the
reject-rate limit defaults to 30%. A production limit would be closer to 1-2%.

Airflow runs captured in `evidence/airflow_dag_runs.txt`: a manual run, a scheduled run, a two-date backfill,
an infrastructure failure recovered with a clear, a schema-break incident recovered with a forced re-ingest,
and a quality-gate failure that correctly blocked publishing. Scenarios are written up in `docs/RECOVERY.md`.

## Design decisions

- **Raw is evidence.** Same file again = skip. Different file for an ingested date = fail loudly. Replacing
  raw needs a deliberate `force_reingest`, and the old file is archived in `raw/_superseded/`.
- **Two kinds of bad data.** A missing column fails the task (`SchemaError`); a bad value rejects the row.
- **Money in exact decimals.** Amounts are computed with `Decimal` and summed in whole pence, rounding half-up.
  An earlier float version was one penny off on UK revenue (25.50 x 15% = 3.825 rounded to 3.82); the
  independent revenue recompute caught it.
- **Write-audit-publish.** The report is built in `curated/`, checked, then copied to `published/`.
  `published/latest/` only moves forward, so a backfill of an old date never replaces the newest report.
- **Thin DAG.** Pipeline modules are imported inside task functions, so parsing the DAG doesn't load pandas.

## Known limitations

- Local filesystem only; in production the layers would sit in object storage (S3/ADLS) as Parquet or Delta.
- CSV outputs are for easy inspection; they don't preserve types as Parquet would.
- Duplicate IDs keep the first occurrence; with conflicting duplicates the "right" row isn't determined.
- Customers arrive as a full daily snapshot; there's no history of customer changes (no SCD Type 2).
- Alerts go to a log file; Slack/Teams/email delivery is a stub in `pipeline/alerts.py`.
- `sys.path` is extended in the DAG file to find `pipeline/`; a real deployment would install it as a package.
- Single process; volumes far beyond a few million rows would need Spark, DuckDB, or the warehouse.
