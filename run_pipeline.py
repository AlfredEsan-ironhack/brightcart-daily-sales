"""Run the whole BrightCart pipeline for one date WITHOUT Airflow.

Same functions, same order as the Airflow DAG. Use it for Day 1 evidence,
quick local testing, and debugging one step at a time.

  python run_pipeline.py --date 2026-10-18
  python run_pipeline.py --date 2026-10-18 --max-reject-rate 0.05   # see the quality gate fail
"""
from __future__ import annotations

import argparse
import sys

from pipeline.audit import build_audit
from pipeline.checks import DataQualityError, run_quality_checks
from pipeline.config import DEFAULT_FRESHNESS_HOURS, DEFAULT_MAX_REJECT_RATE
from pipeline.ingest import ingest_raw
from pipeline.publish import publish_report
from pipeline.report import build_daily_sales
from pipeline.validate import validate_customers, validate_orders


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="business date, YYYY-MM-DD")
    ap.add_argument("--max-reject-rate", type=float, default=DEFAULT_MAX_REJECT_RATE)
    ap.add_argument("--freshness-hours", type=float, default=DEFAULT_FRESHNESS_HOURS)
    ap.add_argument("--force-reingest", action="store_true", help="replace raw with a corrected re-delivery (old raw is archived)")
    args = ap.parse_args()
    d = args.date

    ingest_raw(d, force=args.force_reingest)
    validate_customers(d)
    validate_orders(d)
    build_daily_sales(d)
    build_audit(d, run_id="cli")
    try:
        run_quality_checks(d, max_reject_rate=args.max_reject_rate, freshness_hours=args.freshness_hours)
    except DataQualityError as exc:
        print(f"\nSTOPPED: {exc}")
        return 1
    publish_report(d)
    print(f"\nDone: {d} published.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
