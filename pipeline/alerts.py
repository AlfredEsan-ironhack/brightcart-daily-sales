"""Failure alert used as Airflow's on_failure_callback.

It prints an actionable alert (shown in the task log) and appends it to
data/alerts/alerts.jsonl. To send it to Slack/Teams/email, replace or extend
``_deliver`` - the message content stays the same.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from .config import data_root

NEXT_STEPS = {
    "wait_for_customers_file": "Source file not delivered in time. Contact the CRM export owner; when the file lands, clear this task.",
    "wait_for_orders_file": "Source file not delivered in time. Contact the order system owner; when the file lands, clear this task.",
    "ingest_raw": "RawConflictError = a different file arrived for an already-ingested date. Confirm with the source; if the new file is the correct one, trigger a new run with conf {\"force_reingest\": true}.",
    "validate_customers": "SchemaError = the received file is unusable. If the source re-delivers a fixed file: trigger a new run with conf {\"force_reingest\": true}. If our code was wrong: fix, deploy, clear this task and downstream.",
    "validate_orders": "SchemaError = the received file is unusable. If the source re-delivers a fixed file: trigger a new run with conf {\"force_reingest\": true}. If our code was wrong: fix, deploy, clear this task and downstream.",
    "run_quality_checks": "Quality gate failed. Read audit/.../quality_checks.json. Do NOT force publish; investigate the failed checks.",
}


def _deliver(alert: dict) -> None:
    path = data_root() / "alerts" / "alerts.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(alert) + "\n")


def on_failure_alert(context) -> None:
    ti = context["task_instance"]
    alert = {
        "alert": "BrightCart pipeline task failed",
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id"),
        "business_date": context.get("ds"),
        "try_number": ti.try_number,
        "state": "failed",
        "error": str(context.get("exception"))[:500],
        "log_url": getattr(ti, "log_url", None),
        "next_step": NEXT_STEPS.get(ti.task_id, "Open the task log, find the first ERROR line, decide retry vs fix."),
        "alerted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    print("ALERT " + json.dumps(alert))
    _deliver(alert)
