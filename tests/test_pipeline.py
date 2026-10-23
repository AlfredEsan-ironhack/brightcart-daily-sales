"""Unit tests for the BrightCart rules. Run: python -m pytest -q"""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.report import add_order_amounts, build_report_df
from pipeline.validate import SchemaError, validate_customers_df, validate_orders_df

DAY = "2026-10-18"


def customers(*rows):
    cols = ["customer_id", "first_name", "email", "country", "signup_date"]
    return pd.DataFrame([dict(zip(cols, r)) for r in rows], dtype=str)


def orders(*overrides):
    base = {"order_id": "O1", "customer_id": "C0001", "order_date": DAY, "product_sku": "SKU",
            "quantity": "1", "unit_price_gbp": "10.00", "discount_pct": "0", "status": "completed"}
    return pd.DataFrame([{**base, **o} for o in overrides], dtype=str)


def reasons(rejected):
    return list(rejected["rejection_reasons"])


# ------------------------------------------------------------- customers ---
def test_messy_but_valid_customer_is_cleaned_not_rejected():
    valid, rejected = validate_customers_df(customers(("  C0019 ", "Zoe", " ZOE@Example.com ", " ie ", "2026-02-14")), DAY)
    assert rejected.empty
    assert valid.loc[0, ["customer_id", "email", "country"]].tolist() == ["C0019", "zoe@example.com", "IE"]


@pytest.mark.parametrize("row, reason", [
    (("", "A", "a@x.com", "UK", "2025-01-01"), "missing_customer_id"),
    (("X12", "A", "a@x.com", "UK", "2025-01-01"), "invalid_customer_id_format"),
    (("C0001", "A", "a.x.com", "UK", "2025-01-01"), "invalid_email"),
    (("C0001", "A", "a@x.com", "XX", "2025-01-01"), "invalid_country"),
    (("C0001", "A", "a@x.com", "UK", "2026-13-01"), "invalid_signup_date"),
    (("C0001", "A", "a@x.com", "UK", "2027-01-01"), "signup_date_in_future"),
])
def test_customer_rules(row, reason):
    valid, rejected = validate_customers_df(customers(row), DAY)
    assert valid.empty and reason in reasons(rejected)[0]


def test_duplicate_customer_keeps_first_rejects_rest():
    row = ("C0001", "A", "a@x.com", "UK", "2025-01-01")
    valid, rejected = validate_customers_df(customers(row, row), DAY)
    assert len(valid) == 1 and reasons(rejected) == ["duplicate_customer_id"]


def test_rejected_rows_keep_original_values():
    valid, rejected = validate_customers_df(customers(("C0001", "A", " BAD EMAIL ", "UK", "2025-01-01")), DAY)
    assert rejected.loc[0, "email"] == " BAD EMAIL "  # as received, not standardised


def test_missing_column_is_a_schema_error_not_a_reject():
    with pytest.raises(SchemaError):
        validate_customers_df(customers(("C0001", "A", "a@x.com", "UK", "2025-01-01")).drop(columns="email"), DAY)


# ---------------------------------------------------------------- orders ---
@pytest.mark.parametrize("override, reason", [
    ({"order_id": ""}, "missing_order_id"),
    ({"customer_id": "C9999"}, "unknown_customer"),
    ({"order_date": "2026-10-17"}, "order_date_mismatch"),
    ({"order_date": "18/10/2026"}, "invalid_order_date"),
    ({"quantity": "0"}, "invalid_quantity"),
    ({"quantity": "1.5"}, "invalid_quantity"),
    ({"unit_price_gbp": "-1"}, "invalid_unit_price"),
    ({"unit_price_gbp": "abc"}, "invalid_unit_price"),
    ({"discount_pct": "101"}, "invalid_discount_pct"),
    ({"status": "shipped"}, "invalid_status"),
])
def test_order_rules(override, reason):
    valid, rejected = validate_orders_df(orders(override), {"C0001"}, DAY)
    assert valid.empty and reason in reasons(rejected)[0]


def test_every_failed_rule_is_listed():
    _, rejected = validate_orders_df(orders({"quantity": "0", "status": "shipped"}), {"C0001"}, DAY)
    assert reasons(rejected) == ["invalid_quantity;invalid_status"]


def test_blank_discount_means_zero_and_status_is_standardised():
    valid, rejected = validate_orders_df(orders({"discount_pct": "", "status": " Completed "}), {"C0001"}, DAY)
    assert rejected.empty and valid.loc[0, "discount_pct"] == 0 and valid.loc[0, "status"] == "completed"


def test_every_row_ends_up_valid_or_rejected():
    raw = orders({}, {"quantity": "x"}, {"order_id": "O2"}, {"order_id": "O2"})
    valid, rejected = validate_orders_df(raw, {"C0001"}, DAY)
    assert len(valid) + len(rejected) == len(raw)


# ---------------------------------------------------------------- report ---
def test_money_rounds_half_up_to_the_penny():
    o = pd.DataFrame({"quantity": [1], "unit_price_gbp": [25.50], "discount_pct": [15.0], "status": ["completed"]})
    assert add_order_amounts(o).loc[0, "discount_amount_gbp"] == 3.83  # pandas .round(2) would give 3.82


def test_only_completed_orders_earn_revenue():
    valid, _ = validate_orders_df(
        orders({"order_id": "O1"}, {"order_id": "O2", "status": "cancelled"}, {"order_id": "O3", "status": "refunded"}),
        {"C0001"}, DAY)
    cust = pd.DataFrame({"customer_id": ["C0001"], "country": ["UK"]})
    report = build_report_df(valid, cust, DAY)
    assert report.loc[0, ["orders_total", "orders_completed", "net_revenue_gbp"]].tolist() == [3, 1, 10.0]
