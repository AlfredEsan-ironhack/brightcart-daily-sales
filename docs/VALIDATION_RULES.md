# Validation rules

Every row is checked against every rule. A row that fails one or more rules goes to the rejected file with
**all** its failed rules in `rejection_reasons` (separated by `;`) and its **original** values, plus
`source_row_number` (line in the CSV, header = line 1). Every row ends up in exactly one of valid or rejected.

## Before the rules: file-level check

If a required column is missing, the whole file is unusable. The task fails with `SchemaError` (no rows are
rejected, nothing downstream runs, no retry).

- customers: `customer_id, first_name, email, country, signup_date`
- orders: `order_id, customer_id, order_date, product_sku, quantity, unit_price_gbp, discount_pct, status`

## Standardisation (fixes format, never meaning)

| Field | Change | Example |
|---|---|---|
| all fields | trim leading/trailing spaces | `"  C0019 "` -> `C0019` |
| `email` | lowercase | `ZOE@Example.com` -> `zoe@example.com` |
| `country` | uppercase | ` ie ` -> `IE` |
| `status` | lowercase | ` Completed ` -> `completed` |
| `discount_pct` | blank means no discount | `""` -> `0` |

## Customer rules

| Rule | Fails when |
|---|---|
| `missing_customer_id` | `customer_id` is empty |
| `invalid_customer_id_format` | not `C` followed by 4 digits |
| `invalid_email` | not `something@domain.tld` |
| `invalid_country` | not one of UK, IE, NL, DE, FR |
| `invalid_signup_date` | not a real `YYYY-MM-DD` date |
| `signup_date_in_future` | after the business date |
| `duplicate_customer_id` | same id as an earlier row that passed every other rule (first one kept) |

## Order rules

| Rule | Fails when |
|---|---|
| `missing_order_id` | `order_id` is empty |
| `missing_product_sku` | `product_sku` is empty |
| `unknown_customer` | `customer_id` is not in today's **valid** customers |
| `invalid_order_date` | not a real `YYYY-MM-DD` date |
| `order_date_mismatch` | a valid date, but not the business date |
| `invalid_quantity` | not a whole number of 1 or more |
| `invalid_unit_price` | not a number, or 0 or less |
| `invalid_discount_pct` | not a number between 0 and 100 |
| `invalid_status` | not completed, cancelled, refunded or pending |
| `duplicate_order_id` | same id as an earlier row that passed every other rule (first one kept) |

`unknown_customer` is a cascade on purpose: an order from a rejected customer is also rejected, because its
country (needed for the report) can't be trusted.

## Report rules

- gross = quantity x unit price; discount = gross x discount_pct / 100; net = gross - discount.
- Exact decimals, rounded half-up to the penny per order, summed in whole pence.
- Only `completed` orders count towards units, customers and revenue. All valid orders are counted by status.
- `avg_order_value_gbp` = net revenue / completed orders; blank when there are no completed orders.

## Rows rejected on 2026-10-18

Customers: 21 received, 16 valid, 5 rejected.

| Row | customer_id | Reason | What was wrong |
|---|---|---|---|
| 17 | C0003 | duplicate_customer_id | exact copy of row 4 |
| 18 | C0016 | invalid_email | `harry.example.com` has no @ |
| 19 | (empty) | missing_customer_id | no id at all |
| 20 | C0017 | invalid_country | `XX` |
| 21 | C0018 | invalid_signup_date | `2026-13-01` (month 13) |

Kept, not rejected: row 22 `  C0019 ` with ` ZOE@Example.com ` and ` ie ` is valid after standardisation.

Orders: 55 received, 45 valid, 10 rejected.

| Row | order_id | Reason(s) | What was wrong |
|---|---|---|---|
| 46 | O20261018-0045 | unknown_customer | C0016 was rejected (bad email) |
| 47 | O20261018-0046 | unknown_customer | C9999 doesn't exist |
| 48 | O20261018-0047 | invalid_quantity; invalid_status | quantity 0 and status `shipped` |
| 49 | O20261018-0048 | invalid_quantity | `two` |
| 50 | O20261018-0049 | invalid_unit_price | `-5.00` |
| 51 | O20261018-0050 | invalid_unit_price | `abc` |
| 52 | O20261018-0051 | invalid_discount_pct | `150` |
| 53 | O20261018-0052 | order_date_mismatch | dated 2026-10-17 in the 18th's file |
| 54 | (empty) | missing_order_id | no id |
| 55 | O20261018-0001 | duplicate_order_id | exact copy of row 2 |

Kept, not rejected: the order with status ` Completed ` is valid after standardisation.
The same pattern of planted errors appears on every sample date (see `data/rejected/`).
