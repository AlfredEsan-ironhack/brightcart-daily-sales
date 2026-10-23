"""Step 5 - the quality gate. Nothing is published unless every error check passes.

Checks (severity "error" fails the task, "warn" is recorded only):
  customers_reconcile        raw = valid + rejected (customers)
  orders_reconcile           raw = valid + rejected (orders)
  no_duplicate_valid_orders  order_id unique in valid orders
  report_orders_tie_out      report orders_total = valid order count
  report_revenue_tie_out     report net revenue = revenue recomputed from valid orders
  report_date_matches        every report row is for this business date
  report_fresh               report file written within the freshness window
  customers_reject_rate      rejected / source <= max_reject_rate
  orders_reject_rate         rejected / source <= max_reject_rate
  has_valid_orders           at least one valid order (warn: could be a genuine no-sales day)
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import DEFAULT_FRESHNESS_HOURS, DEFAULT_MAX_REJECT_RATE, Paths
from .io_utils import read_json, write_json


class DataQualityError(RuntimeError):
    """At least one error-level quality check failed."""


def run_quality_checks(
    business_date: str,
    root: Path | None = None,
    max_reject_rate: float = DEFAULT_MAX_REJECT_RATE,
    freshness_hours: float = DEFAULT_FRESHNESS_HOURS,
) -> list[dict]:
    p = Paths.for_date(business_date, root)
    max_reject_rate, freshness_hours = float(max_reject_rate), float(freshness_hours)
    audit = read_json(p.audit)
    c, o = audit["entities"]["customers"], audit["entities"]["orders"]
    report = pd.read_csv(p.report, dtype={"business_date": str})
    valid_orders = pd.read_csv(p.valid("orders"), dtype={"order_id": str})
    age_hours = (time.time() - p.report.stat().st_mtime) / 3600

    results: list[dict] = []

    def check(name: str, passed: bool, detail: str, severity: str = "error") -> None:
        results.append({"check": name, "passed": bool(passed), "severity": severity, "detail": detail})

    check("customers_reconcile", c["reconciled"],
          f"{c['source_rows']} source = {c['valid_rows']} valid + {c['rejected_rows']} rejected")
    check("orders_reconcile", o["reconciled"],
          f"{o['source_rows']} source = {o['valid_rows']} valid + {o['rejected_rows']} rejected")
    dupes = int(valid_orders["order_id"].duplicated().sum())
    check("no_duplicate_valid_orders", dupes == 0, f"{dupes} duplicate order_id values in valid orders")
    check("report_orders_tie_out", int(report["orders_total"].sum()) == len(valid_orders),
          f"report {int(report['orders_total'].sum())} vs valid orders {len(valid_orders)}")
    rep_net, exp_net = audit["report"]["net_revenue_gbp"], audit["report"]["expected_net_revenue_gbp_from_valid_orders"]
    check("report_revenue_tie_out", abs(rep_net - exp_net) < 0.01, f"report {rep_net:.2f} vs recomputed {exp_net:.2f} GBP")
    wrong_dates = sorted(set(report["business_date"]) - {business_date})
    check("report_date_matches", not wrong_dates, f"unexpected dates in report: {wrong_dates or 'none'}")
    check("report_fresh", age_hours <= freshness_hours, f"report written {age_hours:.2f} h ago (limit {freshness_hours} h)")
    check("customers_reject_rate", c["reject_rate"] <= max_reject_rate,
          f"{c['reject_rate']:.1%} rejected (limit {max_reject_rate:.0%})")
    check("orders_reject_rate", o["reject_rate"] <= max_reject_rate,
          f"{o['reject_rate']:.1%} rejected (limit {max_reject_rate:.0%})")
    check("has_valid_orders", len(valid_orders) > 0, f"{len(valid_orders)} valid orders", severity="warn")

    write_json(
        {"business_date": business_date, "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "max_reject_rate": max_reject_rate, "freshness_hours": freshness_hours, "results": results},
        p.quality_checks,
    )
    for r in results:
        mark = "PASS" if r["passed"] else ("FAIL" if r["severity"] == "error" else "WARN")
        print(f"[check] {mark:4} {r['check']:<26} {r['detail']}")

    failed = [r["check"] for r in results if not r["passed"] and r["severity"] == "error"]
    if failed:
        raise DataQualityError(f"Quality gate failed for {business_date}: {failed}. Report NOT published.")
    return results
