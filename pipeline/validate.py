"""Step 2 - validate customers and orders; split into valid and rejected.

Two kinds of problem are handled differently:

* **Whole-file problem** (a required column is missing): raise ``SchemaError``.
  The task fails. Retrying will not help; someone must fix the file or code.
* **Row problem** (bad email, negative price...): the row goes to the rejected
  file with every reason it failed. Nothing is dropped silently.

Every row lands in exactly one of valid or rejected, so
``raw rows = valid rows + rejected rows`` always holds (checked later).
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .config import ALLOWED_COUNTRIES, ALLOWED_ORDER_STATUSES, REQUIRED_COLUMNS, Paths
from .io_utils import read_csv_as_text, write_csv

CUSTOMER_ID_PATTERN = re.compile(r"^C\d{4}$")
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
INTEGER_PATTERN = re.compile(r"^\d+$")


class SchemaError(ValueError):
    """The file itself is unusable (e.g. a required column is missing)."""


# ---------------------------------------------------------------- helpers ---
def check_schema(df: pd.DataFrame, entity: str) -> None:
    missing = [c for c in REQUIRED_COLUMNS[entity] if c not in df.columns]
    if missing:
        raise SchemaError(
            f"{entity} file is missing required column(s) {missing}. Columns found: {list(df.columns)}"
        )


def _parse_date(s: pd.Series) -> pd.Series:
    """'YYYY-MM-DD' -> Timestamp; anything else -> NaT."""
    return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")


def _reasons(flags: pd.DataFrame) -> pd.Series:
    """Turn a table of True/False rule failures into 'rule_a;rule_b' per row."""
    names = flags.columns.to_numpy()
    return pd.Series(
        [";".join(names[row]) for row in flags.to_numpy()], index=flags.index, dtype="object"
    )


def _split(raw: pd.DataFrame, flags: pd.DataFrame, clean: pd.DataFrame, business_date: str):
    reasons = _reasons(flags)
    is_valid = reasons == ""

    valid = clean[is_valid].copy()

    rejected = raw[~is_valid].copy()  # the ORIGINAL values, as received
    rejected.insert(0, "source_row_number", clean.loc[~is_valid, "source_row_number"])
    rejected["rejection_reasons"] = reasons[~is_valid]
    rejected["business_date"] = business_date
    return valid.reset_index(drop=True), rejected.reset_index(drop=True)


# -------------------------------------------------------------- customers ---
def validate_customers_df(raw: pd.DataFrame, business_date: str):
    check_schema(raw, "customers")
    run_day = pd.Timestamp(business_date)

    c = raw.copy()
    c["source_row_number"] = range(2, len(c) + 2)  # line in the CSV (line 1 = header)

    # standardise (fix harmless formatting, never change meaning)
    for col in REQUIRED_COLUMNS["customers"]:
        c[col] = c[col].str.strip()
    c["email"] = c["email"].str.lower()
    c["country"] = c["country"].str.upper()
    signup = _parse_date(c["signup_date"])

    flags = pd.DataFrame(index=c.index)
    flags["missing_customer_id"] = c["customer_id"] == ""
    flags["invalid_customer_id_format"] = (c["customer_id"] != "") & ~c["customer_id"].str.match(CUSTOMER_ID_PATTERN)
    flags["invalid_email"] = ~c["email"].str.match(EMAIL_PATTERN)
    flags["invalid_country"] = ~c["country"].isin(ALLOWED_COUNTRIES)
    flags["invalid_signup_date"] = signup.isna()
    flags["signup_date_in_future"] = signup.notna() & (signup > run_day)

    # duplicates: among rows that passed every other rule, keep the first
    passed_so_far = ~flags.any(axis=1)
    dup = c["customer_id"].where(passed_so_far).duplicated(keep="first") & passed_so_far
    flags["duplicate_customer_id"] = dup

    clean = c[["source_row_number", *REQUIRED_COLUMNS["customers"]]].copy()
    clean["signup_date"] = signup.dt.strftime("%Y-%m-%d")
    clean["business_date"] = business_date
    return _split(raw, flags, clean, business_date)


# ----------------------------------------------------------------- orders ---
def validate_orders_df(raw: pd.DataFrame, valid_customer_ids: set[str], business_date: str):
    check_schema(raw, "orders")

    o = raw.copy()
    o["source_row_number"] = range(2, len(o) + 2)
    for col in REQUIRED_COLUMNS["orders"]:
        o[col] = o[col].str.strip()
    o["status"] = o["status"].str.lower()

    order_date = _parse_date(o["order_date"])
    quantity_ok = o["quantity"].str.match(INTEGER_PATTERN)
    quantity = pd.to_numeric(o["quantity"].where(quantity_ok), errors="coerce")
    price = pd.to_numeric(o["unit_price_gbp"], errors="coerce")
    discount = pd.to_numeric(o["discount_pct"].replace("", "0"), errors="coerce")  # blank = no discount

    flags = pd.DataFrame(index=o.index)
    flags["missing_order_id"] = o["order_id"] == ""
    flags["missing_product_sku"] = o["product_sku"] == ""
    flags["unknown_customer"] = ~o["customer_id"].isin(valid_customer_ids)
    flags["invalid_order_date"] = order_date.isna()
    flags["order_date_mismatch"] = order_date.notna() & (order_date != pd.Timestamp(business_date))
    flags["invalid_quantity"] = quantity.isna() | (quantity < 1)
    flags["invalid_unit_price"] = price.isna() | (price <= 0)
    flags["invalid_discount_pct"] = discount.isna() | (discount < 0) | (discount > 100)
    flags["invalid_status"] = ~o["status"].isin(ALLOWED_ORDER_STATUSES)

    passed_so_far = ~flags.any(axis=1)
    flags["duplicate_order_id"] = o["order_id"].where(passed_so_far).duplicated(keep="first") & passed_so_far

    clean = o[["source_row_number", *REQUIRED_COLUMNS["orders"]]].copy()
    clean["order_date"] = order_date.dt.strftime("%Y-%m-%d")
    clean["quantity"] = quantity.astype("Int64")
    clean["unit_price_gbp"] = price.round(2)
    clean["discount_pct"] = discount
    clean["business_date"] = business_date
    return _split(raw, flags, clean, business_date)


# ------------------------------------------------- file-level entry points ---
def validate_customers(business_date: str, root: Path | None = None) -> dict:
    p = Paths.for_date(business_date, root)
    raw = read_csv_as_text(p.raw("customers"))
    valid, rejected = validate_customers_df(raw, business_date)
    write_csv(valid, p.valid("customers"))
    write_csv(rejected, p.rejected("customers"))
    result = {"source": len(raw), "valid": len(valid), "rejected": len(rejected)}
    print(f"[validate] customers: {result}")
    return result


def validate_orders(business_date: str, root: Path | None = None) -> dict:
    p = Paths.for_date(business_date, root)
    raw = read_csv_as_text(p.raw("orders"))
    valid_ids = set(read_csv_as_text(p.valid("customers"))["customer_id"])
    valid, rejected = validate_orders_df(raw, valid_ids, business_date)
    write_csv(valid, p.valid("orders"))
    write_csv(rejected, p.rejected("orders"))
    result = {"source": len(raw), "valid": len(valid), "rejected": len(rejected)}
    print(f"[validate] orders: {result}")
    return result
