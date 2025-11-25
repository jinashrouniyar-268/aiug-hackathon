#!/usr/bin/env python3
"""Convert log files into schema-specific JSONL chunks and sync DuckDB + MotherDuck."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import duckdb

BASE_DIR = Path(__file__).parent
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
DEFAULT_MOTHERDUCK_DB = os.getenv(
    "OPERATIONS_MOTHERDUCK_DB", os.getenv("DATABASE_NAME", "antm_hack")
)

BASE_COLUMNS = ("timestamp", "message_type", "service", "action")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse log files into schema-specific JSONL files and load databases."
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        required=True,
        help="Path to the log file to ingest (required).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to write schema JSONL files (defaults to <log-dir>/<log-stem>_structured).",
    )
    parser.add_argument(
        "--duckdb",
        type=Path,
        default=None,
        help="Path to the DuckDB database (omit to skip DuckDB loading).",
    )
    parser.add_argument(
        "--table-prefix",
        type=str,
        default=None,
        help="Prefix used for generated table and file names (defaults to log file stem).",
    )
    parser.add_argument(
        "--motherduck-db",
        type=str,
        default=DEFAULT_MOTHERDUCK_DB,
        help="MotherDuck database name (default: env OPERATIONS_MOTHERDUCK_DB or DATABASE_NAME).",
    )
    parser.add_argument(
        "--skip-duckdb",
        action="store_true",
        help="Skip loading data into DuckDB even if --duckdb is provided.",
    )
    parser.add_argument(
        "--skip-motherduck",
        action="store_true",
        help="Skip loading data into MotherDuck.",
    )
    return parser.parse_args()


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_value(raw: str):
    value = raw.strip()
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    if value.replace(".", "", 1).isdigit() and value.count(".") <= 1:
        try:
            return int(value) if "." not in value else float(value)
        except ValueError:
            return value
    return value


def parse_line(line: str) -> OrderedDict[str, object] | None:
    parts = line.strip().split()
    if len(parts) < 6:
        return None

    timestamp = f"{parts[0]} {parts[1]}"
    message_type = parts[2]
    service = parts[3]
    action = parts[4]

    payload_tokens = parts[5:]
    payload: OrderedDict[str, str] = OrderedDict()
    current_key: str | None = None

    for token in payload_tokens:
        if "=" in token:
            key, value = token.split("=", 1)
            payload[key] = value
            current_key = key
        elif current_key:
            payload[current_key] += f" {token}"

    record = OrderedDict(
        [
            ("timestamp", timestamp),
            ("message_type", message_type),
            ("service", service),
            ("action", action),
        ]
    )
    for key, raw_value in payload.items():
        record[key] = normalize_value(raw_value)

    issue_value = record.get("issue")
    if isinstance(issue_value, str):
        severity_match = re.search(r"\(severity:\s*([^)]+)\)", issue_value)
        if severity_match:
            record["severity"] = severity_match.group(1).strip()
            issue_cleaned = re.sub(r"\(severity:\s*([^)]+)\)", "", issue_value).strip()
            if issue_cleaned.endswith("."):
                issue_cleaned = issue_cleaned[:-1].rstrip()
            record["issue"] = issue_cleaned.strip()

    return record


def schema_key(record: OrderedDict[str, object]) -> Tuple[str, ...]:
    return tuple(record.keys())


def write_jsonl(
    records_by_schema: Dict[Tuple[str, ...], List[OrderedDict[str, object]]],
    output_dir: Path,
    table_prefix: str,
):
    ensure_output_dir(output_dir)
    manifest: List[Dict[str, object]] = []
    sorted_records = sorted(
        records_by_schema.items(), key=lambda item: (len(item[0]), item[0])
    )
    for idx, (columns, records) in enumerate(sorted_records, start=1):
        filename = output_dir / f"{table_prefix}_schema_{idx:02}.jsonl"
        table_name = f"{table_prefix}_schema_{idx:02}"
        with filename.open("w", encoding="utf-8") as sink:
            for record in records:
                json.dump(record, sink, ensure_ascii=False)
                sink.write("\n")
        manifest.append(
            {
                "schema_id": idx,
                "table_name": table_name,
                "columns": list(columns),
                "filename": str(
                    filename.relative_to(BASE_DIR)
                    if filename.is_relative_to(BASE_DIR)
                    else filename
                ),
                "row_count": len(records),
            }
        )
        print(f"Wrote {len(records)} rows to {filename}")

    manifest_path = output_dir / f"{table_prefix}_schema_manifest.json"
    with manifest_path.open("w", encoding="utf-8") as manifest_file:
        json.dump(manifest, manifest_file, indent=2)
    print(f"Schema manifest saved to {manifest_path}")

    return manifest


def rebuild_duckdb_tables(
    manifest: Iterable[Dict[str, object]],
    duckdb_path: Path | None,
    table_prefix: str,
    skip: bool,
) -> None:
    if skip:
        print("Skipping DuckDB load (flag set).")
        return

    if not duckdb_path:
        print("Skipping DuckDB load (no database path provided).")
        return

    if not duckdb_path.exists():
        raise FileNotFoundError(
            "DuckDB database not found. Please run load_data_to_duckdb.py first."
        )

    con = duckdb.connect(str(duckdb_path))
    try:
        con.execute(f"DROP TABLE IF EXISTS {table_prefix}")
        for entry in manifest:
            table_name = entry["table_name"]
            file_path = BASE_DIR / entry["filename"]
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_json_auto('{file_path}', format='newline_delimited')
                """
            )
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"{table_name}: {row_count} row(s) loaded.")
    finally:
        con.close()


def load_into_motherduck(
    manifest: Iterable[Dict[str, object]],
    table_prefix: str,
    motherduck_db: str,
    skip: bool,
) -> None:
    if skip:
        print("Skipping MotherDuck load (flag set).")
        return

    if not MOTHERDUCK_TOKEN:
        print("Skipping MotherDuck load (MOTHERDUCK_TOKEN not set).")
        return

    if not motherduck_db:
        print("Skipping MotherDuck load (database name not provided).")
        return

    print(f"Connecting to MotherDuck database: {motherduck_db}")
    con = duckdb.connect(
        f"md:{motherduck_db}", config={"motherduck_token": MOTHERDUCK_TOKEN}
    )
    try:
        con.execute(f"DROP TABLE IF EXISTS {table_prefix}")
        for entry in manifest:
            table_name = entry["table_name"]
            file_path = BASE_DIR / entry["filename"]
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_json_auto('{file_path}', format='newline_delimited')
                """
            )
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"[MotherDuck] {table_name}: {row_count} row(s) loaded.")
    finally:
        con.close()
        print("MotherDuck load complete.")


def main() -> None:
    args = parse_args()
    log_file = args.log_file.resolve()
    output_dir = (
        args.output_dir
        if args.output_dir is not None
        else log_file.parent / f"{log_file.stem}_structured"
    )
    output_dir = output_dir.resolve()
    table_prefix = args.table_prefix or log_file.stem
    duckdb_path = args.duckdb.resolve() if args.duckdb else None

    records_by_schema: Dict[Tuple[str, ...], List[OrderedDict[str, object]]] = {}
    with log_file.open("r", encoding="utf-8") as logfile:
        for line in logfile:
            if not line.strip():
                continue
            record = parse_line(line)
            if not record:
                continue
            key = schema_key(record)
            records_by_schema.setdefault(key, []).append(record)

    print(f"Discovered {len(records_by_schema)} unique schema(s).")
    manifest = write_jsonl(records_by_schema, output_dir, table_prefix)
    rebuild_duckdb_tables(manifest, duckdb_path, table_prefix, args.skip_duckdb)
    load_into_motherduck(
        manifest, table_prefix, args.motherduck_db, args.skip_motherduck
    )


if __name__ == "__main__":
    main()
