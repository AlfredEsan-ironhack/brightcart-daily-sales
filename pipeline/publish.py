"""Step 6 - publish the report, only after the quality gate passed.

* Copies the curated report to published/business_date=.../ and writes _SUCCESS.
  Consumers should only read a date that has _SUCCESS.
* Updates published/latest/ only if this date is the newest one published.
  A backfill of an old date must never replace the latest report with older data.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .config import Paths
from .io_utils import copy_file, read_json, sha256, write_json


def publish_report(business_date: str, root: Path | None = None) -> dict:
    p = Paths.for_date(business_date, root)
    if not p.quality_checks.exists():
        raise RuntimeError(f"No quality-check record for {business_date}; refusing to publish.")
    checks = read_json(p.quality_checks)["results"]
    if any(not r["passed"] and r["severity"] == "error" for r in checks):
        raise RuntimeError(f"Quality checks for {business_date} did not pass; refusing to publish.")

    copy_file(p.report, p.published_report)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    write_json({"business_date": business_date, "published_at": now, "sha256": sha256(p.published_report)},
               p.published_marker)

    latest = read_json(p.published_latest_manifest) if p.published_latest_manifest.exists() else {}
    updated_latest = business_date >= latest.get("business_date", "")
    if updated_latest:
        copy_file(p.report, p.published_latest)
        write_json({"business_date": business_date, "published_at": now}, p.published_latest_manifest)

    result = {"published": str(p.published_report), "updated_latest": updated_latest}
    print(f"[publish] {result}")
    return result
