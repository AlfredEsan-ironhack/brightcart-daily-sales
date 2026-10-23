"""Paths, entity names and business rules shared by every pipeline step.

Every output for one business date lives in its own folder
(``business_date=YYYY-MM-DD``). Rerunning a date rewrites only that folder,
which is what makes reruns and backfills safe (idempotent).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

ENTITIES = ("customers", "orders")

# ---- Business rules (documented in docs/VALIDATION_RULES.md) ----------------
ALLOWED_COUNTRIES = {"UK", "IE", "NL", "DE", "FR"}
ALLOWED_ORDER_STATUSES = {"completed", "cancelled", "refunded", "pending"}
REVENUE_STATUS = "completed"  # only completed orders count as revenue

REQUIRED_COLUMNS = {
    "customers": ["customer_id", "first_name", "email", "country", "signup_date"],
    "orders": [
        "order_id",
        "customer_id",
        "order_date",
        "product_sku",
        "quantity",
        "unit_price_gbp",
        "discount_pct",
        "status",
    ],
}

# ---- Quality-gate defaults (overridable from Airflow Variables or the CLI) ---
DEFAULT_MAX_REJECT_RATE = 0.30  # sample data is deliberately dirty; production would be ~0.01-0.02
DEFAULT_FRESHNESS_HOURS = 24


def data_root() -> Path:
    """Data folder. Override with BRIGHTCART_DATA_DIR (e.g. a mounted volume)."""
    return Path(os.environ.get("BRIGHTCART_DATA_DIR", PROJECT_ROOT / "data"))


@dataclass(frozen=True)
class Paths:
    """Every file location for one business date, in one place."""

    business_date: str
    root: Path

    @classmethod
    def for_date(cls, business_date: str, root: Path | None = None) -> "Paths":
        return cls(business_date=business_date, root=Path(root) if root else data_root())

    @property
    def partition(self) -> str:
        return f"business_date={self.business_date}"

    # landing zone: where the source system drops files
    def incoming(self, entity: str) -> Path:
        return self.root / "incoming" / f"{entity}_{self.business_date}.csv"

    # raw layer: byte-for-byte copy of what was received
    def raw(self, entity: str) -> Path:
        return self.root / "raw" / self.partition / f"{entity}.csv"

    @property
    def raw_manifest(self) -> Path:
        return self.root / "raw" / self.partition / "_manifest.json"

    # validated layer
    def valid(self, entity: str) -> Path:
        return self.root / "valid" / self.partition / f"{entity}.csv"

    def rejected(self, entity: str) -> Path:
        return self.root / "rejected" / self.partition / f"{entity}_rejected.csv"

    # curated layer
    @property
    def report(self) -> Path:
        return self.root / "curated" / self.partition / "daily_sales.csv"

    # audit evidence
    @property
    def audit(self) -> Path:
        return self.root / "audit" / self.partition / "audit.json"

    @property
    def quality_checks(self) -> Path:
        return self.root / "audit" / self.partition / "quality_checks.json"

    @property
    def audit_log(self) -> Path:
        return self.root / "audit" / "audit_log.csv"

    # published layer: only reached after the quality gate passes
    @property
    def published_report(self) -> Path:
        return self.root / "published" / self.partition / "daily_sales.csv"

    @property
    def published_marker(self) -> Path:
        return self.root / "published" / self.partition / "_SUCCESS"

    @property
    def published_latest(self) -> Path:
        return self.root / "published" / "latest" / "daily_sales.csv"

    @property
    def published_latest_manifest(self) -> Path:
        return self.root / "published" / "latest" / "_latest.json"

    @property
    def alerts_log(self) -> Path:
        return self.root / "alerts" / "alerts.jsonl"
