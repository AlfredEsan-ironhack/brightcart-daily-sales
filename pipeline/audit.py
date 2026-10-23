"""Step 4 - write audit evidence for one business date.

Everything is recounted *from the files on disk*, not taken from what earlier
steps said they did. The audit is proof, so it must not trust itself.

Outputs:
* audit/business_date=.../audit.json  - full record for that date
* audit/audit_log.csv                 - one row per date (rerun replaces the row)
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import ENTITIES, Paths
from .io_utils import count_data_rows, read_csv_as_text, read_json, write_csv, write_json
from .report import add_order_amounts


def _reason_counts(rejected_file: Path) -> dict[str, int]:
    rej = read_csv_as_text(rejected_file)
    if rej.empty:
        return {}
    exploded = rej["rejection_reasons"].str.split(";").explode()
    return {k: int(v) for k, v in exploded.value_counts().sort_index().items()}


def build_audit(business_date: str, run_id: str = "manual", root: Path | None = None) -> dict:
    p = Paths.for_date(business_date, root)
    manifest = read_json(p.raw_manifest)

    entities = {}
    for entity in ENTITIES:
        source = count_data_rows(p.raw(entity))
        valid = count_data_rows(p.valid(entity))
        rejected = count_data_rows(p.rejected(entity))
        entities[entity] = {
            "raw_file_sha256": manifest["files"][entity]["sha256"],
            "source_rows": source,
            "valid_rows": valid,
            "rejected_rows": rejected,
            "reconciled": source == valid + rejected,
            "reject_rate": round(rejected / source, 4) if source else 0.0,
            "rejection_reasons": _reason_counts(p.rejected(entity)),
        }

    report = pd.read_csv(p.report)
    orders = add_order_amounts(pd.read_csv(p.valid("orders"), dtype={"order_id": str}))
    expected_net = int(orders.loc[orders["is_revenue"], "net_pence"].sum()) / 100

    audit = {
        "business_date": business_date,
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "entities": entities,
        "report": {
            "file": str(p.report),
            "rows": len(report),
            "orders_total": int(report["orders_total"].sum()),
            "orders_completed": int(report["orders_completed"].sum()),
            "net_revenue_gbp": round(float(report["net_revenue_gbp"].sum()), 2),
            "expected_net_revenue_gbp_from_valid_orders": expected_net,
        },
    }
    write_json(audit, p.audit)
    _upsert_audit_log(audit, p)
    print(
        f"[audit] customers {entities['customers']['source_rows']} = "
        f"{entities['customers']['valid_rows']} + {entities['customers']['rejected_rows']} | "
        f"orders {entities['orders']['source_rows']} = "
        f"{entities['orders']['valid_rows']} + {entities['orders']['rejected_rows']} | "
        f"report rows {len(report)}"
    )
    return audit


def _upsert_audit_log(audit: dict, p: Paths) -> None:
    c, o = audit["entities"]["customers"], audit["entities"]["orders"]
    row = pd.DataFrame(
        [
            {
                "business_date": audit["business_date"],
                "run_id": audit["run_id"],
                "generated_at": audit["generated_at"],
                "customers_source": c["source_rows"],
                "customers_valid": c["valid_rows"],
                "customers_rejected": c["rejected_rows"],
                "orders_source": o["source_rows"],
                "orders_valid": o["valid_rows"],
                "orders_rejected": o["rejected_rows"],
                "report_rows": audit["report"]["rows"],
                "report_orders_total": audit["report"]["orders_total"],
                "net_revenue_gbp": audit["report"]["net_revenue_gbp"],
                "reconciled": c["reconciled"] and o["reconciled"],
            }
        ]
    )
    if p.audit_log.exists():
        log = pd.read_csv(p.audit_log, dtype={"business_date": str})
        log = log[log["business_date"] != audit["business_date"]]  # replace, never duplicate
        row = pd.concat([log, row], ignore_index=True)
    write_csv(row.sort_values("business_date"), p.audit_log)
