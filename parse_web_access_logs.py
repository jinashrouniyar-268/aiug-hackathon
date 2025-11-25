#!/usr/bin/env python3
"""Parse web access logs into structured JSONL rows."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterator, Optional

LOG_PATH = Path("antm/data/logs/web_access_logs.txt").resolve()
OUTPUT_DIR = LOG_PATH.parent / "web_access_logs_structured"
OUTPUT_JSONL = OUTPUT_DIR / "web_access_logs.jsonl"

LINE_RE = re.compile(
    r"^(?P<ip>\S+)\s+-\s+-\s+\[(?P<timestamp>[^\]]+)\]\s+\"(?P<request>[^\"]*)\"\s+"
    r"(?P<status>\d{3})\s+(?P<size>-|\d+)\s+\"(?P<referer>[^\"]*)\"\s+\"(?P<user_agent>[^\"]*)\"$"
)
REQUEST_RE = re.compile(r"^(?P<method>\S+)\s+(?P<path>\S+)\s+(?P<protocol>[^\s]+)$")


def parse_line(line: str) -> Optional[dict]:
    line = line.strip()
    if not line:
        return None

    match = LINE_RE.match(line)
    if not match:
        return None

    data = match.groupdict()

    request = data.get("request", "")
    method = path = protocol = None
    if request:
        req_match = REQUEST_RE.match(request)
        if req_match:
            method = req_match.group("method")
            path = req_match.group("path")
            protocol = req_match.group("protocol")

    size_str = data.get("size") or "-"
    size_value = int(size_str) if size_str.isdigit() else None

    return {
        "ip": data.get("ip"),
        "timestamp": data.get("timestamp"),
        "request_raw": request,
        "method": method,
        "path": path,
        "protocol": protocol,
        "status": int(data.get("status", "0")),
        "bytes": size_value,
        "referer": data.get("referer") or None,
        "user_agent": data.get("user_agent") or None,
    }


def iter_records(log_path: Path) -> Iterator[dict]:
    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            record = parse_line(line)
            if record:
                yield record


def main() -> None:
    if not LOG_PATH.exists():
        raise FileNotFoundError(f"Log file not found: {LOG_PATH}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    with OUTPUT_JSONL.open("w", encoding="utf-8") as sink:
        for record in iter_records(LOG_PATH):
            json.dump(record, sink, ensure_ascii=False)
            sink.write("\n")
            count += 1

    print(f"Wrote {count} records to {OUTPUT_JSONL}")


if __name__ == "__main__":
    main()
