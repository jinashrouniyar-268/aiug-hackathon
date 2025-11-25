#!/usr/bin/env python3
"""Convert transaction_logs.txt (TSV) into structured JSONL."""

from __future__ import annotations

import csv
import json
from pathlib import Path

LOG_PATH = Path("antm/data/logs/transaction_logs.txt").resolve()
OUTPUT_DIR = LOG_PATH.parent / "transaction_logs_structured"
OUTPUT_JSONL = OUTPUT_DIR / "transaction_logs.jsonl"


def parse_amount(value: str) -> float | None:
    value = value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_int(value: str) -> int | None:
    value = value.strip()
    if not value or not value.isdigit():
        return None
    return int(value)


def normalize_field(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if value.lower() == "null" or value == "":
        return None
    return value


def convert() -> int:
    if not LOG_PATH.exists():
        raise FileNotFoundError(f"Log file not found: {LOG_PATH}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    count = 0

    with LOG_PATH.open("r", encoding="utf-8") as source, OUTPUT_JSONL.open(
        "w", encoding="utf-8"
    ) as sink:
        reader = csv.DictReader(source, delimiter="\t")
        for row in reader:
            record = {
                "timestamp": row["timestamp"],
                "event_type": row["event_type"],
                "order_id": parse_int(row["order_id"]),
                "customer_sk": parse_int(row["customer_sk"]),
                "amount": parse_amount(row["amount"]),
                "status": row["status"],
                "error_code": normalize_field(row.get("error_code")),
            }
            json.dump(record, sink, ensure_ascii=False)
            sink.write("\n")
            count += 1

    return count


def main() -> None:
    count = convert()
    print(f"Wrote {count} records to {OUTPUT_JSONL}")


if __name__ == "__main__":
    main()
