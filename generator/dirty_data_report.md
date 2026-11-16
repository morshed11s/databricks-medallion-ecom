# Dirty-data inventory

Seed `42` · 1,000,000 data rows per file · data as-of `2026-11-01T00:00:00`  
Each row carries at most one injected anomaly.

## orders.csv

| Anomaly type | Field | Rows | % of file |
|---|---|---:|---:|
| case_whitespace | category | 9,856 | 0.986% |
| case_whitespace | country | 10,082 | 1.008% |
| case_whitespace | order_status | 9,896 | 0.99% |
| duplicate_exact_row | *all* | 5,001 | 0.5% |
| duplicate_key_conflict | order_status | 2,357 | 0.236% |
| duplicate_key_conflict | quantity | 2,505 | 0.251% |
| future_date | order_date | 3,003 | 0.3% |
| invalid_date | order_date | 3,973 | 0.397% |
| invalid_status_label | order_status | 2,939 | 0.294% |
| null_or_blank | category | 1,261 | 0.126% |
| null_or_blank | country | 1,306 | 0.131% |
| null_or_blank | customer_id | 1,286 | 0.129% |
| null_or_blank | order_date | 1,275 | 0.128% |
| null_or_blank | order_id | 1,266 | 0.127% |
| null_or_blank | order_status | 1,251 | 0.125% |
| null_or_blank | quantity | 1,232 | 0.123% |
| null_or_blank | unit_price | 1,242 | 0.124% |
| out_of_range_numeric | quantity | 4,000 | 0.4% |
| out_of_range_numeric | unit_price | 3,947 | 0.395% |
| text_in_numeric | quantity | 4,025 | 0.403% |
| text_in_numeric | unit_price | 4,026 | 0.403% |
| **Total** | | **75,729** | **7.573%** |

- unique_order_ids_created: 990,137

## transactions.csv

| Anomaly type | Field | Rows | % of file |
|---|---|---:|---:|
| case_whitespace | currency | 9,782 | 0.978% |
| case_whitespace | payment_method | 9,901 | 0.99% |
| case_whitespace | txn_status | 9,979 | 0.998% |
| duplicate_exact_row | *all* | 4,993 | 0.499% |
| duplicate_key_conflict | txn_status,txn_ts | 4,985 | 0.498% |
| future_timestamp | txn_ts | 2,920 | 0.292% |
| invalid_status_label | txn_status | 2,028 | 0.203% |
| invalid_timestamp | txn_ts | 4,021 | 0.402% |
| null_or_blank | amount | 1,392 | 0.139% |
| null_or_blank | currency | 1,462 | 0.146% |
| null_or_blank | order_id | 1,434 | 0.143% |
| null_or_blank | payment_method | 1,348 | 0.135% |
| null_or_blank | txn_id | 1,395 | 0.14% |
| null_or_blank | txn_status | 1,431 | 0.143% |
| null_or_blank | txn_ts | 1,426 | 0.143% |
| orphan_order_id | order_id | 10,216 | 1.022% |
| out_of_range_numeric | amount | 7,008 | 0.701% |
| text_in_numeric | amount | 8,104 | 0.81% |
| unsupported_currency | currency | 4,027 | 0.403% |
| **Total** | | **87,852** | **8.785%** |

- unique_txn_ids_created: 990,022
- clean_orders_with_a_clean_txn: 836,014
- payable_orders: 954,105
