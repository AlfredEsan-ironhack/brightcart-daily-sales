"""Step 3 - build the curated daily sales report.

Grain: one row per business_date per customer country
(countries with no valid orders that day are not listed).

Revenue rules:
* gross_amount  = quantity * unit_price_gbp
* discount      = gross_amount * discount_pct / 100
* net_amount    = gross_amount - discount
* Only ``completed`` orders count towards units and revenue.
  Cancelled, refunded and pending orders are counted, but earn nothing.
"""
from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pandas as pd

from .config import REVENUE_STATUS, Paths
from .io_utils import write_csv

REPORT_COLUMNS = [
    "business_date",
    "country",
    "orders_total",
    "orders_completed",
    "orders_cancelled",
    "orders_refunded",
    "orders_pending",
    "units_sold",
    "unique_customers",
    "gross_revenue_gbp",
    "discount_gbp",
    "net_revenue_gbp",
    "avg_order_value_gbp",
]


CENT = Decimal("0.01")


def _to_pence(amount: Decimal) -> int:
    """Round half-up to the penny (25.50 * 15% = 3.825 -> 3.83) and return integer pence."""
    return int(amount.quantize(CENT, rounding=ROUND_HALF_UP) * 100)


def add_order_amounts(orders: pd.DataFrame) -> pd.DataFrame:
    """Per-order money, computed in exact decimals, never binary floats.

    Two traps with floats: 25.50 * 0.15 is stored as 3.8249999..., and pandas/numpy
    ``.round(2)`` rounds halves to even. Either way 3.825 becomes 3.82, while
    finance expects 3.83 (half-up). This bug was caught by the independent
    revenue recompute: one penny off on UK. So: Decimal for the maths, integer
    pence for sums, pounds only for display.
    """
    o = orders.copy()
    gross, disc = [], []
    for qty, price, pct in zip(o["quantity"], o["unit_price_gbp"], o["discount_pct"]):
        g = _to_pence(int(qty) * Decimal(str(price)))
        gross.append(g)
        disc.append(_to_pence(Decimal(g) / 100 * Decimal(str(pct)) / 100))
    o["gross_pence"] = gross
    o["discount_pence"] = disc
    o["net_pence"] = o["gross_pence"] - o["discount_pence"]
    o["gross_amount_gbp"] = o["gross_pence"] / 100
    o["discount_amount_gbp"] = o["discount_pence"] / 100
    o["net_amount_gbp"] = o["net_pence"] / 100
    o["is_revenue"] = o["status"] == REVENUE_STATUS
    return o


def build_report_df(orders: pd.DataFrame, customers: pd.DataFrame, business_date: str) -> pd.DataFrame:
    o = add_order_amounts(orders)
    # every valid order has a valid customer (validation guarantees it); many_to_one proves it
    o = o.merge(customers[["customer_id", "country"]], on="customer_id", how="left", validate="many_to_one")

    rev = o["is_revenue"]
    o["units_rev"] = o["quantity"].where(rev, 0)
    o["gross_rev"] = o["gross_pence"].where(rev, 0)  # sum whole pence: exact
    o["disc_rev"] = o["discount_pence"].where(rev, 0)
    o["net_rev"] = o["net_pence"].where(rev, 0)
    o["customer_rev"] = o["customer_id"].where(rev)
    for status in ("completed", "cancelled", "refunded", "pending"):
        o[f"is_{status}"] = (o["status"] == status).astype(int)

    report = (
        o.groupby("country", as_index=False)
        .agg(
            orders_total=("order_id", "size"),
            orders_completed=("is_completed", "sum"),
            orders_cancelled=("is_cancelled", "sum"),
            orders_refunded=("is_refunded", "sum"),
            orders_pending=("is_pending", "sum"),
            units_sold=("units_rev", "sum"),
            unique_customers=("customer_rev", "nunique"),
            gross_revenue_gbp=("gross_rev", "sum"),
            discount_gbp=("disc_rev", "sum"),
            net_revenue_gbp=("net_rev", "sum"),
        )
        .sort_values("country")
    )
    for col in ("gross_revenue_gbp", "discount_gbp", "net_revenue_gbp"):
        report[col] = report[col] / 100  # pence -> pounds, only at the very end
    # average per completed order; undefined (blank) when there were none
    report["avg_order_value_gbp"] = (
        report["net_revenue_gbp"] / report["orders_completed"].where(report["orders_completed"] > 0)
    ).round(2)
    report.insert(0, "business_date", business_date)
    return report[REPORT_COLUMNS].reset_index(drop=True)


def build_daily_sales(business_date: str, root: Path | None = None) -> dict:
    p = Paths.for_date(business_date, root)
    orders = pd.read_csv(p.valid("orders"), dtype={"customer_id": str, "order_id": str})
    customers = pd.read_csv(p.valid("customers"), dtype={"customer_id": str})
    report = build_report_df(orders, customers, business_date)
    write_csv(report, p.report)
    result = {
        "report_rows": len(report),
        "orders_in_report": int(report["orders_total"].sum()),
        "net_revenue_gbp": round(float(report["net_revenue_gbp"].sum()), 2),
    }
    print(f"[report] {result}")
    return result
