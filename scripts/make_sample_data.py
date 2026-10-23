"""Generate BrightCart source files in data/incoming/.

Each date gets:
  customers_YYYY-MM-DD.csv  - full customer snapshot (some deliberately bad rows)
  orders_YYYY-MM-DD.csv     - that day's orders   (some deliberately bad rows)

Output is deterministic (same date -> same file), so reruns are reproducible.

Usage:
  python scripts/make_sample_data.py 2026-10-16 2026-10-17 2026-10-18
  python scripts/make_sample_data.py 2026-10-19 --break-schema   # drops unit_price_gbp (recovery demo)
"""
from __future__ import annotations

import argparse
import csv
import random
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.config import REQUIRED_COLUMNS, Paths  # noqa: E402

FIRST_NAMES = ["Amelia", "Oliver", "Isla", "George", "Ava", "Noah", "Mia", "Leo",
               "Freya", "Arthur", "Sophie", "Jack", "Emma", "Liam", "Daan"]
COUNTRIES = ["UK", "UK", "UK", "IE", "NL", "DE", "FR"]
SKUS = {"BC-MUG-01": 8.50, "BC-TEE-02": 15.00, "BC-BAG-03": 22.00, "BC-CAP-04": 12.75, "BC-BTL-05": 18.40}
STATUSES = ["completed"] * 7 + ["cancelled", "refunded", "pending"]


def customers_rows() -> list[dict]:
    rng = random.Random(42)
    rows = []
    for i, name in enumerate(FIRST_NAMES, start=1):
        cid = f"C{i:04d}"
        rows.append({
            "customer_id": cid,
            "first_name": name,
            "email": f"{name.lower()}.{i}@example.com",
            "country": COUNTRIES[i % len(COUNTRIES)],
            "signup_date": str(date(2025, 1, 1) + timedelta(days=rng.randint(0, 600))),
        })
    # deliberately bad or messy rows (see docs/VALIDATION_RULES.md)
    rows += [
        dict(rows[2]),                                                                    # exact duplicate of C0003
        {"customer_id": "C0016", "first_name": "Harry", "email": "harry.example.com",     # no @
         "country": "UK", "signup_date": "2025-06-01"},
        {"customer_id": "", "first_name": "Unknown", "email": "nobody@example.com",       # missing id
         "country": "UK", "signup_date": "2025-06-01"},
        {"customer_id": "C0017", "first_name": "Lena", "email": "lena@example.com",       # bad country
         "country": "XX", "signup_date": "2025-07-12"},
        {"customer_id": "C0018", "first_name": "Omar", "email": "omar@example.com",       # impossible date
         "country": "NL", "signup_date": "2026-13-01"},
        {"customer_id": "  C0019 ", "first_name": "Zoe", "email": "  ZOE@Example.com ",   # messy but VALID
         "country": " ie ", "signup_date": "2026-02-14"},
    ]
    return rows


def orders_rows(day: date) -> list[dict]:
    rng = random.Random(int(day.strftime("%Y%m%d")))
    good_customers = [f"C{i:04d}" for i in range(1, 16)] + ["C0019"]
    rows, n = [], 0

    def new_id() -> str:
        nonlocal n
        n += 1
        return f"O{day:%Y%m%d}-{n:04d}"

    for _ in range(rng.randint(38, 44)):
        sku = rng.choice(list(SKUS))
        rows.append({
            "order_id": new_id(), "customer_id": rng.choice(good_customers), "order_date": str(day),
            "product_sku": sku, "quantity": str(rng.randint(1, 4)), "unit_price_gbp": f"{SKUS[sku]:.2f}",
            "discount_pct": rng.choice(["", "0", "0", "10", "15"]), "status": rng.choice(STATUSES),
        })

    def bad(**overrides) -> dict:
        row = {"order_id": new_id(), "customer_id": "C0001", "order_date": str(day), "product_sku": "BC-MUG-01",
               "quantity": "1", "unit_price_gbp": "8.50", "discount_pct": "0", "status": "completed"}
        row.update(overrides)
        return row

    rows += [
        bad(customer_id="C0016"),                       # customer was rejected -> unknown_customer
        bad(customer_id="C9999"),                       # customer never existed
        bad(quantity="0", status="shipped"),            # two problems in one row
        bad(quantity="two"),
        bad(unit_price_gbp="-5.00"),
        bad(unit_price_gbp="abc"),
        bad(discount_pct="150"),
        bad(order_date=str(day - timedelta(days=1))),   # belongs to another day
        bad(order_id=""),                               # missing id
        dict(rows[0]),                                  # exact duplicate of the first order
        bad(status=" Completed "),                      # messy but VALID after standardising
    ]
    return rows


def write(path: Path, rows: list[dict], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows):3d} rows -> {path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("dates", nargs="+", help="business dates, YYYY-MM-DD")
    ap.add_argument("--break-schema", action="store_true", help="drop unit_price_gbp from orders (failure demo)")
    args = ap.parse_args()

    for d in args.dates:
        day = date.fromisoformat(d)
        p = Paths.for_date(d)
        write(p.incoming("customers"), customers_rows(), REQUIRED_COLUMNS["customers"])
        order_cols = [c for c in REQUIRED_COLUMNS["orders"] if not (args.break_schema and c == "unit_price_gbp")]
        write(p.incoming("orders"), orders_rows(day), order_cols)


if __name__ == "__main__":
    main()
