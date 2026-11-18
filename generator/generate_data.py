#!/usr/bin/env python3
import argparse
import csv
import hashlib
import json
import os
import random
from collections import Counter
from datetime import date, datetime, timedelta

AS_OF = datetime(2026, 11, 1, 0, 0, 0)
START_DATE = date(2025, 11, 1)
N_DAYS = 365
N_CUSTOMERS = 50_000

CATEGORIES = {
    "Electronics":    (25.0, 1500.0, 18),
    "Clothing":       (8.0,  180.0,  20),
    "Home & Kitchen": (10.0, 400.0,  15),
    "Books":          (5.0,  60.0,   10),
    "Beauty":         (4.0,  120.0,  10),
    "Sports":         (10.0, 600.0,  10),
    "Toys":           (5.0,  150.0,   9),
    "Grocery":        (2.0,  80.0,    8),
}
COUNTRIES = [("US", 30), ("GB", 12), ("DE", 10), ("FR", 8), ("IN", 10),
             ("CA", 8), ("AU", 6), ("BD", 6), ("NL", 5), ("ES", 5)]
CURRENCY_BY_COUNTRY = {"GB": "GBP", "DE": "EUR", "FR": "EUR", "NL": "EUR", "ES": "EUR"}
FX_FROM_USD = {"USD": 1.00, "EUR": 0.92, "GBP": 0.79}

ORDER_STATUSES = [("COMPLETED", 70), ("SHIPPED", 10), ("PENDING", 6), ("CANCELLED", 9), ("RETURNED", 5)]
PAYMENT_METHODS = [("CARD", 50), ("PAYPAL", 20), ("WALLET", 15), ("BANK_TRANSFER", 10), ("COD", 5)]
P_FIRST_ATTEMPT_FAILS = {"CARD": 0.06, "PAYPAL": 0.04, "WALLET": 0.08, "BANK_TRANSFER": 0.12, "COD": 0.03}
P_SPLIT_PAYMENT = 0.06
P_REPEAT_PAYMENT = 0.04

ORDER_HEADER = ["order_id", "customer_id", "order_date", "category", "quantity",
                "unit_price", "order_status", "country"]
TXN_HEADER = ["txn_id", "order_id", "payment_method", "amount", "currency", "txn_status", "txn_ts"]

ORDER_ANOMALIES = [
    ("duplicate_exact_row",    0.005),
    ("duplicate_key_conflict", 0.005),
    ("null_or_blank",          0.010),
    ("text_in_numeric",        0.008),
    ("out_of_range_numeric",   0.008),
    ("invalid_date",           0.004),
    ("future_date",            0.003),
    ("invalid_status_label",   0.003),
    ("case_whitespace",        0.030),
]
TXN_ANOMALIES = [
    ("duplicate_exact_row",    0.005),
    ("duplicate_key_conflict", 0.005),
    ("null_or_blank",          0.010),
    ("text_in_numeric",        0.008),
    ("out_of_range_numeric",   0.007),
    ("invalid_timestamp",      0.004),
    ("future_timestamp",       0.003),
    ("unsupported_currency",   0.004),
    ("invalid_status_label",   0.002),
    ("orphan_order_id",        0.010),
    ("case_whitespace",        0.030),
]

NULL_TOKENS = ["", " ", "NULL", "null", "None"]
RING_SIZE = 5000


def _cum(items):
    values, cum, total = [], [], 0
    for v, w in items:
        total += w
        values.append(v)
        cum.append(total)
    return values, cum


CAT_VALUES, CAT_CUM = _cum([(k, v[2]) for k, v in CATEGORIES.items()])
COUNTRY_VALUES, COUNTRY_CUM = _cum(COUNTRIES)
STATUS_VALUES, STATUS_CUM = _cum(ORDER_STATUSES)
METHOD_VALUES, METHOD_CUM = _cum(PAYMENT_METHODS)


def pick(rng, values, cum):
    return rng.choices(values, cum_weights=cum)[0]


def pick_anomaly(rng, plan):
    r = rng.random()
    acc = 0.0
    for name, p in plan:
        acc += p
        if r < acc:
            return name
    return None


def messy_case(rng, s):
    variants = [s.lower(), s.upper() if s != s.upper() else s.lower(), " " + s, s + "  ",
                "  " + s.lower() + " ", s.title() if s != s.title() else s.swapcase()]
    v = rng.choice(variants)
    return v if v != s else " " + s


def fmt_ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def ring_add(rng, ring, row):
    if len(ring) < RING_SIZE:
        ring.append(row)
    else:
        ring[rng.randrange(RING_SIZE)] = row


def generate_orders(rng, n_rows, path, stats):
    payable = []
    ring = []
    next_id = 1
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(ORDER_HEADER)
        for _ in range(n_rows):
            anomaly = pick_anomaly(rng, ORDER_ANOMALIES)

            if anomaly in ("duplicate_exact_row", "duplicate_key_conflict") and ring:
                row = list(rng.choice(ring))
                if anomaly == "duplicate_exact_row":
                    field = "*all*"
                elif rng.random() < 0.5:
                    field = "order_status"
                    row[6] = rng.choice([s for s in STATUS_VALUES if s != row[6]])
                else:
                    field = "quantity"
                    row[4] = str(int(row[4]) % 10 + 1)
                stats[(anomaly, field)] += 1
                w.writerow(row)
                continue
            if anomaly in ("duplicate_exact_row", "duplicate_key_conflict"):
                anomaly = None

            order_id = f"ORD-{next_id:08d}"
            next_id += 1
            cust = int(N_CUSTOMERS * (rng.random() ** 2.5)) + 1
            customer_id = f"CUST-{cust:06d}"
            odate = START_DATE + timedelta(days=rng.randrange(N_DAYS))
            category = pick(rng, CAT_VALUES, CAT_CUM)
            lo, hi, _ = CATEGORIES[category]
            unit_price = round(rng.uniform(lo, hi), 2)
            quantity = min(10, int(rng.expovariate(0.6)) + 1)
            status = pick(rng, STATUS_VALUES, STATUS_CUM)
            country = pick(rng, COUNTRY_VALUES, COUNTRY_CUM)
            row = [order_id, customer_id, odate.isoformat(), category, str(quantity),
                   f"{unit_price:.2f}", status, country]
            clean_tuple = (order_id, round(quantity * unit_price, 2), country, status, odate)

            if anomaly is None:
                ring_add(rng, ring, list(row))
                payable.append(clean_tuple)
                w.writerow(row)
                continue

            if anomaly == "case_whitespace":
                field = rng.choice(["category", "order_status", "country"])
                idx = ORDER_HEADER.index(field)
                row[idx] = messy_case(rng, row[idx])
                payable.append(clean_tuple)
            elif anomaly == "null_or_blank":
                field = rng.choice(ORDER_HEADER)
                row[ORDER_HEADER.index(field)] = rng.choice(NULL_TOKENS)
            elif anomaly == "text_in_numeric":
                field = rng.choice(["quantity", "unit_price"])
                if field == "quantity":
                    row[4] = rng.choice(["two", "3 pcs", "five", "x", "1O"])
                else:
                    row[5] = rng.choice(["N/A", "12,50", "$19.99", "free", "abc", "9.99.1"])
            elif anomaly == "out_of_range_numeric":
                field = rng.choice(["quantity", "unit_price"])
                if field == "quantity":
                    row[4] = str(rng.choice([0, -1, -2, -5, rng.randint(1001, 5000)]))
                else:
                    row[5] = rng.choice([f"-{unit_price:.2f}", "0.00",
                                         f"{rng.uniform(100001, 999999):.2f}"])
            elif anomaly == "invalid_date":
                field = "order_date"
                row[2] = rng.choice(["2026-02-30", "2025-13-01", "31/12/2025", "not_a_date",
                                     "20251301", "2026-06-31", "0000-00-00"])
            elif anomaly == "future_date":
                field = "order_date"
                row[2] = (date(2027, 1, 1) + timedelta(days=rng.randrange(4 * 365))).isoformat()
            elif anomaly == "invalid_status_label":
                field = "order_status"
                row[6] = rng.choice(["DONE", "UNKNOWN", "DELIVERED?", "?", "IN_PROGRESS"])
            stats[(anomaly, field)] += 1
            w.writerow(row)

    stats[("_unique_order_ids_created", "order_id")] = next_id - 1
    return payable


def clean_txn_stream(rng, payable):
    max_ts = AS_OF - timedelta(seconds=1)
    for order_id, total_usd, country, status, odate in payable:
        cur = CURRENCY_BY_COUNTRY.get(country, "USD")
        amt = round(total_usd * FX_FROM_USD[cur], 2)
        ts = datetime(odate.year, odate.month, odate.day) + timedelta(seconds=rng.randrange(86400))
        ts = min(ts, max_ts)

        def row(method, amount, st, t):
            return [order_id, method, f"{amount:.2f}", cur, st, fmt_ts(min(t, max_ts))]

        method = pick(rng, METHOD_VALUES, METHOD_CUM)
        if status in ("COMPLETED", "SHIPPED"):
            if rng.random() < P_FIRST_ATTEMPT_FAILS[method]:
                yield row(method, amt, "FAILED", ts)
                ts += timedelta(minutes=rng.randint(1, 30))
                method = method if rng.random() < 0.5 else "CARD"
                yield row(method, amt, "SUCCESS", ts)
            else:
                r = rng.random()
                if r < P_SPLIT_PAYMENT:
                    first = round(amt / 2, 2)
                    yield row(method, first, "SUCCESS", ts)
                    yield row(method, round(amt - first, 2), "SUCCESS",
                              ts + timedelta(hours=rng.randint(1, 48)))
                elif r < P_SPLIT_PAYMENT + P_REPEAT_PAYMENT:
                    yield row(method, amt, "SUCCESS", ts)
                    yield row(method, amt, "SUCCESS", ts + timedelta(minutes=rng.randint(1, 10)))
                else:
                    yield row(method, amt, "SUCCESS", ts)
        elif status == "PENDING":
            if rng.random() < 0.5:
                yield row(method, amt, "PENDING", ts)
        elif status == "CANCELLED":
            r = rng.random()
            if r < 0.4:
                yield row(method, amt, "FAILED", ts)
            elif r < 0.7:
                yield row(method, amt, "REFUNDED", ts + timedelta(days=rng.randint(0, 3)))
        elif status == "RETURNED":
            yield row(method, amt, "REFUNDED", ts + timedelta(days=rng.randint(1, 14)))


KEY_CONFLICT_TRANSITION = {"SUCCESS": "REFUNDED", "FAILED": "SUCCESS",
                           "PENDING": "SUCCESS", "REFUNDED": "REFUNDED"}


def generate_transactions(rng, n_rows, path, payable, n_order_ids, stats):
    stream = clean_txn_stream(rng, payable)
    ring = []
    next_id = 1
    orders_with_txn = set()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(TXN_HEADER)
        for _ in range(n_rows):
            anomaly = pick_anomaly(rng, TXN_ANOMALIES)

            if anomaly in ("duplicate_exact_row", "duplicate_key_conflict") and ring:
                row = list(rng.choice(ring))
                if anomaly == "duplicate_exact_row":
                    field = "*all*"
                else:
                    field = "txn_status,txn_ts"
                    row[5] = KEY_CONFLICT_TRANSITION[row[5]]
                    t = datetime.strptime(row[6], "%Y-%m-%d %H:%M:%S") + \
                        timedelta(minutes=rng.randint(1, 120))
                    row[6] = fmt_ts(min(t, AS_OF - timedelta(seconds=1)))
                stats[(anomaly, field)] += 1
                w.writerow(row)
                continue
            if anomaly in ("duplicate_exact_row", "duplicate_key_conflict"):
                anomaly = None

            txn_id = f"TXN-{next_id:08d}"
            next_id += 1

            if anomaly == "orphan_order_id":
                fake_order = f"ORD-9{rng.randrange(10**7):07d}"
                cur = rng.choice(["USD", "EUR", "GBP"])
                ts = datetime(START_DATE.year, START_DATE.month, START_DATE.day) + \
                    timedelta(seconds=rng.randrange(N_DAYS * 86400))
                row = [txn_id, fake_order, pick(rng, METHOD_VALUES, METHOD_CUM),
                       f"{rng.uniform(5, 2000):.2f}", cur, "SUCCESS", fmt_ts(ts)]
                stats[(anomaly, "order_id")] += 1
                w.writerow(row)
                continue

            base = next(stream, None)
            if base is None:
                raise RuntimeError("Ran out of payable orders; increase payment probabilities.")
            row = [txn_id] + base

            if anomaly is None:
                ring_add(rng, ring, list(row))
                orders_with_txn.add(row[1])
                w.writerow(row)
                continue

            if anomaly == "case_whitespace":
                field = rng.choice(["payment_method", "currency", "txn_status"])
                idx = TXN_HEADER.index(field)
                row[idx] = messy_case(rng, row[idx])
                orders_with_txn.add(row[1])
            elif anomaly == "null_or_blank":
                field = rng.choice(TXN_HEADER)
                row[TXN_HEADER.index(field)] = rng.choice(NULL_TOKENS)
            elif anomaly == "text_in_numeric":
                field = "amount"
                row[3] = rng.choice(["abc", f"${row[3]}", "1,234.56", "ten", "12.34.56", "N/A"])
            elif anomaly == "out_of_range_numeric":
                field = "amount"
                row[3] = rng.choice(["0.00", f"-{row[3]}", f"{rng.uniform(1_000_001, 9_999_999):.2f}"])
            elif anomaly == "invalid_timestamp":
                field = "txn_ts"
                row[6] = rng.choice(["2026-02-30 10:15:00", "2026-06-15 25:61:00", "yesterday",
                                     "1700000000", "15/06/2026 10:00", "2026-13-01 00:00:00"])
            elif anomaly == "future_timestamp":
                field = "txn_ts"
                t = datetime(2027, 1, 1) + timedelta(seconds=rng.randrange(4 * 365 * 86400))
                row[6] = fmt_ts(t)
            elif anomaly == "unsupported_currency":
                field = "currency"
                row[4] = rng.choice(["JPY", "BTC", "XXX", "INR", "US$"])
            elif anomaly == "invalid_status_label":
                field = "txn_status"
                row[5] = rng.choice(["DECLINED?", "UNKNOWN", "OK", "ERR"])
            stats[(anomaly, field)] += 1
            w.writerow(row)

    stats[("_unique_txn_ids_created", "txn_id")] = next_id - 1
    stats[("_clean_orders_with_a_clean_txn", "order_id")] = len(orders_with_txn)
    stats[("_payable_orders", "order_id")] = len(payable)


def file_info(path):
    h = hashlib.sha256()
    lines = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
            lines += chunk.count(b"\n")
    return {"path": path, "bytes": os.path.getsize(path), "sha256": h.hexdigest(),
            "data_rows_excluding_header": lines - 1}


def write_report(out_dir, n_rows, seed, order_stats, txn_stats):
    def rows(stats):
        out = []
        for (a, field), c in sorted(stats.items()):
            if a.startswith("_"):
                continue
            out.append({"anomaly_type": a, "field": field, "rows": c,
                        "pct_of_file": round(100.0 * c / n_rows, 3)})
        return out

    def info(stats):
        return {a.lstrip("_"): c for (a, _), c in stats.items() if a.startswith("_")}

    report = {
        "seed": seed, "rows_per_file": n_rows, "as_of": AS_OF.isoformat(),
        "orders": {"anomalies": rows(order_stats), "info": info(order_stats)},
        "transactions": {"anomalies": rows(txn_stats), "info": info(txn_stats)},
    }
    for k in ("orders", "transactions"):
        report[k]["total_anomalous_rows"] = sum(r["rows"] for r in report[k]["anomalies"])
        report[k]["total_anomalous_pct"] = round(100.0 * report[k]["total_anomalous_rows"] / n_rows, 3)

    with open(os.path.join(out_dir, "dirty_data_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    md = [f"# Dirty-data inventory\n",
          f"Seed `{seed}` · {n_rows:,} data rows per file · data as-of `{AS_OF.isoformat()}`  ",
          "Each row carries at most one injected anomaly.\n"]
    for k in ("orders", "transactions"):
        md.append(f"## {k}.csv\n")
        md.append("| Anomaly type | Field | Rows | % of file |")
        md.append("|---|---|---:|---:|")
        for r in report[k]["anomalies"]:
            md.append(f"| {r['anomaly_type']} | {r['field']} | {r['rows']:,} | {r['pct_of_file']}% |")
        md.append(f"| **Total** | | **{report[k]['total_anomalous_rows']:,}** "
                  f"| **{report[k]['total_anomalous_pct']}%** |\n")
        for name, val in report[k]["info"].items():
            md.append(f"- {name}: {val:,}")
        md.append("")
    with open(os.path.join(out_dir, "dirty_data_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    return report


def main():
    ap = argparse.ArgumentParser(description="Seeded synthetic e-commerce data generator for orders.csv and transactions.csv.", formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "output"))
    ap.add_argument("--report-dir", default=os.path.dirname(os.path.abspath(__file__)),
                    help="where the dirty-data report + manifest are written (committed to Git)")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    os.makedirs(args.report_dir, exist_ok=True)
    orders_path = os.path.join(args.out, "orders.csv")
    txns_path = os.path.join(args.out, "transactions.csv")

    order_rng = random.Random(args.seed)
    txn_rng = random.Random(args.seed + 1)
    order_stats, txn_stats = Counter(), Counter()

    print(f"Generating {args.rows:,} orders ...")
    payable = generate_orders(order_rng, args.rows, orders_path, order_stats)
    print(f"Generating {args.rows:,} transactions ...")
    generate_transactions(txn_rng, args.rows, txns_path, payable,
                          order_stats[("_unique_order_ids_created", "order_id")], txn_stats)

    report = write_report(args.report_dir, args.rows, args.seed, order_stats, txn_stats)
    manifest = {"seed": args.seed, "files": [file_info(orders_path), file_info(txns_path)]}
    for fi in manifest["files"]:
        fi["path"] = os.path.basename(fi["path"])
        assert fi["data_rows_excluding_header"] == args.rows, fi
    with open(os.path.join(args.report_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    for fi in manifest["files"]:
        print(f"  {fi['path']}: {fi['data_rows_excluding_header']:,} rows, {fi['bytes']:,} bytes, "
              f"sha256={fi['sha256'][:16]}...")
    print(f"  orders anomalies: {report['orders']['total_anomalous_rows']:,} "
          f"({report['orders']['total_anomalous_pct']}%)")
    print(f"  transactions anomalies: {report['transactions']['total_anomalous_rows']:,} "
          f"({report['transactions']['total_anomalous_pct']}%)")
    print("Report: dirty_data_report.md / .json, manifest.json")


if __name__ == "__main__":
    main()
