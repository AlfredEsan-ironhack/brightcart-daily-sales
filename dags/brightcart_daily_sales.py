"""BrightCart daily sales pipeline - Airflow DAG.

The DAG only ORCHESTRATES. All data logic lives in the ``pipeline`` package,
so it can be tested and run without Airflow (see run_pipeline.py).

Flow:
  [wait_for_customers_file, wait_for_orders_file]
      >> ingest_raw >> validate_customers >> validate_orders
      >> build_daily_sales >> write_audit >> run_quality_checks >> publish_report

Time: one run = one business day. The data interval is 06:00 -> 06:00 (London),
so the run that starts at 06:00 on the 19th has ds = the 18th and processes the
18th's files. CronDataIntervalTimetable makes this explicit and behaves the same
in Airflow 2 and 3 (Airflow 3's default cron timetable would give ds = the 19th).

Setup (once):
  airflow connections add brightcart_fs --conn-type fs --conn-extra '{"path": "<project>/data"}'
Optional Variables (change behaviour without code changes):
  brightcart_max_reject_rate (default 0.30), brightcart_freshness_hours (default 24)
Recovery after a corrected re-delivery of an already-ingested file:
  trigger with conf {"force_reingest": true}  (old raw is archived, not overwritten)
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

try:  # Airflow 3
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.sensors.filesystem import FileSensor
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # Airflow 2
    from airflow.exceptions import AirflowFailException
    from airflow.operators.python import PythonOperator
    from airflow.sensors.filesystem import FileSensor

# Make the project's ``pipeline`` package importable (dags/ is one level below the project root).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.alerts import on_failure_alert  # noqa: E402  (light: no pandas)


# --- Thin task functions --------------------------------------------------------
# Airflow re-parses this file every ~30 s. The pipeline modules import pandas,
# so we import them INSIDE the task functions: the cost is paid only when a task
# runs, never during parsing. Each function returns a small summary (sent to XCom).
#
# Deterministic errors (bad file, checksum conflict) are re-raised as
# AirflowFailException: the task fails at once and SKIPS its retries, because
# retrying the same bad input gives the same failure. Other errors (disk, network)
# still use the normal retries.

def ingest_task(business_date: str, force: str = "False") -> dict:
    from pipeline.ingest import RawConflictError, ingest_raw
    try:
        manifest = ingest_raw(business_date, force=force)
    except (RawConflictError, FileNotFoundError) as exc:
        raise AirflowFailException(str(exc)) from exc
    return {e: f["rows"] for e, f in manifest["files"].items()}


def validate_customers_task(business_date: str) -> dict:
    from pipeline.validate import SchemaError, validate_customers
    try:
        return validate_customers(business_date)
    except SchemaError as exc:
        raise AirflowFailException(str(exc)) from exc


def validate_orders_task(business_date: str) -> dict:
    from pipeline.validate import SchemaError, validate_orders
    try:
        return validate_orders(business_date)
    except SchemaError as exc:
        raise AirflowFailException(str(exc)) from exc


def build_report_task(business_date: str) -> dict:
    from pipeline.report import build_daily_sales
    return build_daily_sales(business_date)


def audit_task(business_date: str, run_id: str) -> dict:
    from pipeline.audit import build_audit
    audit = build_audit(business_date, run_id=run_id)
    return {e: {k: v[k] for k in ("source_rows", "valid_rows", "rejected_rows", "reconciled")}
            for e, v in audit["entities"].items()}


def quality_gate_task(business_date: str, max_reject_rate: str, freshness_hours: str) -> dict:
    from pipeline.checks import run_quality_checks
    results = run_quality_checks(business_date, max_reject_rate=max_reject_rate, freshness_hours=freshness_hours)
    return {"passed": sum(r["passed"] for r in results), "total": len(results)}


def publish_task(business_date: str) -> dict:
    from pipeline.publish import publish_report
    return publish_report(business_date)


BUSINESS_DATE = "{{ ds }}"  # Jinja: filled in per run, so backfills process their own date

default_args = {
    "owner": "data-eng",
    "retries": 0,  # default: deterministic steps are fixed, not retried
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_alert,
}

with DAG(
    dag_id="brightcart_daily_sales",
    description="Raw -> validated -> curated daily sales, with audit and quality gate",
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/London"),
    schedule=CronDataIntervalTimetable("0 6 * * *", timezone="Europe/London"),
    catchup=False,  # history is rebuilt deliberately with a backfill, never by accident
    max_active_runs=1,  # dates run one at a time, in order
    dagrun_timeout=timedelta(hours=6),
    default_args=default_args,
    tags=["brightcart", "mini-project"],
    doc_md=__doc__,
) as dag:

    sensor_settings = dict(
        fs_conn_id="brightcart_fs",  # base path lives in the Connection, not in code
        poke_interval=300,  # check every 5 minutes
        timeout=60 * 60 * 3,  # give up after 3 hours -> fails -> alert
        mode="reschedule",  # free the worker slot between checks
    )
    wait_for_customers_file = FileSensor(
        task_id="wait_for_customers_file", filepath=f"incoming/customers_{BUSINESS_DATE}.csv", **sensor_settings
    )
    wait_for_orders_file = FileSensor(
        task_id="wait_for_orders_file", filepath=f"incoming/orders_{BUSINESS_DATE}.csv", **sensor_settings
    )

    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=ingest_task,
        op_kwargs={
            "business_date": BUSINESS_DATE,
            # operator decision for a corrected re-delivery: trigger with conf {"force_reingest": true}
            "force": "{{ (dag_run.conf or {}).get('force_reingest', false) }}",
        },
        retries=2,  # the only step that reads the landing zone: transient storage errors possible
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
    )
    validate_customers = PythonOperator(
        task_id="validate_customers", python_callable=validate_customers_task,
        op_kwargs={"business_date": BUSINESS_DATE},
    )
    validate_orders = PythonOperator(
        task_id="validate_orders", python_callable=validate_orders_task,
        op_kwargs={"business_date": BUSINESS_DATE},
    )
    build_daily_sales = PythonOperator(
        task_id="build_daily_sales", python_callable=build_report_task,
        op_kwargs={"business_date": BUSINESS_DATE},
    )
    write_audit = PythonOperator(
        task_id="write_audit", python_callable=audit_task,
        op_kwargs={"business_date": BUSINESS_DATE, "run_id": "{{ run_id }}"},
    )
    run_quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=quality_gate_task,
        op_kwargs={
            "business_date": BUSINESS_DATE,
            # Variables are read via Jinja when the task runs, not when the file is parsed
            "max_reject_rate": "{{ var.value.get('brightcart_max_reject_rate', '0.30') }}",
            "freshness_hours": "{{ var.value.get('brightcart_freshness_hours', '24') }}",
        },
    )
    publish_report = PythonOperator(
        task_id="publish_report", python_callable=publish_task,
        op_kwargs={"business_date": BUSINESS_DATE},
    )

    (
        [wait_for_customers_file, wait_for_orders_file]
        >> ingest_raw
        >> validate_customers
        >> validate_orders
        >> build_daily_sales
        >> write_audit
        >> run_quality_checks
        >> publish_report
    )
