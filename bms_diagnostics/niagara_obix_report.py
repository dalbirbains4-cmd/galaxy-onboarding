"""
Read-only Niagara N4 / Vykon Supervisor diagnostics via oBIX.

Discovers temperature & humidity points by name, reads current value/status/
timestamp via HTTP GET on /obix, and writes a CSV + Markdown report.

Only issues GET requests. Never writes to the station.

Usage:
    python niagara_obix_report.py \\
        --host https://supervisor.local \\
        --user readonly \\
        --password '...' \\
        --insecure                       # if Supervisor uses self-signed cert

    # Or set NIAGARA_USER / NIAGARA_PASSWORD in the env and omit the flags.

    # Restrict to an explicit point list (one oBIX path per line):
    python niagara_obix_report.py --host ... --points-file points.txt

Exit codes:
    0  report written, no out-of-spec or bad-status points
    2  report written, one or more deviations or alarms found
    1  fatal error (auth, connectivity, etc.)
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from requests.auth import HTTPBasicAuth

OBIX_NS = "http://obix.org/ns/schema/1.0"
DEFAULT_MATCH = r"(?i)(temp|humid|\brh\b)"
DEFAULT_ROOT = "/obix/config/"
STALE_SECONDS = 300


@dataclass
class Reading:
    path: str
    display_name: str
    value: str | None
    unit: str | None
    status: str
    timestamp: str | None
    kind: str
    out_of_spec: bool = False
    spec_note: str = ""
    error: str = ""


@dataclass
class Limits:
    temp_min: float | None = None
    temp_max: float | None = None
    rh_min: float | None = None
    rh_max: float | None = None


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def classify(name: str) -> str:
    low = name.lower()
    if "humid" in low or re.search(r"\brh\b", low):
        return "humidity"
    if "temp" in low:
        return "temperature"
    return "other"


def fetch(session: requests.Session, base: str, path: str, timeout: float) -> ET.Element:
    url = urljoin(base, path)
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return ET.fromstring(resp.content)


def discover(
    session: requests.Session,
    base: str,
    root: str,
    pattern: re.Pattern[str],
    max_points: int,
    max_depth: int,
    timeout: float,
) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    queue: deque[tuple[str, int]] = deque([(root, 0)])

    while queue and len(found) < max_points:
        path, depth = queue.popleft()
        if path in seen:
            continue
        seen.add(path)

        try:
            elem = fetch(session, base, path, timeout)
        except requests.RequestException:
            continue

        for child in elem:
            tag = strip_ns(child.tag)
            href = child.get("href")
            name = child.get("name") or child.get("displayName") or ""
            if not href:
                continue

            child_path = urljoin(path, href)
            child_url_path = urlparse(child_path).path

            if tag in {"real", "int", "bool", "str", "enum"}:
                if pattern.search(name) or pattern.search(child_url_path):
                    found.append((child_url_path, name or child_url_path))
                    if len(found) >= max_points:
                        break
            elif tag in {"obj", "ref", "list"}:
                if depth < max_depth:
                    queue.append((child_url_path, depth + 1))

    return found


def parse_point(elem: ET.Element, path: str, fallback_name: str) -> Reading:
    tag = strip_ns(elem.tag)
    name = elem.get("displayName") or elem.get("name") or fallback_name
    value = elem.get("val")
    unit_raw = elem.get("unit") or ""
    unit = unit_raw.rsplit("/", 1)[-1] if unit_raw else None
    status = elem.get("status") or "ok"
    timestamp = None

    for child in elem:
        ctag = strip_ns(child.tag)
        cname = child.get("name") or ""
        if ctag == "abstime" and cname in {"timestamp", "out.timestamp"}:
            timestamp = child.get("val")
        if ctag in {"real", "int", "bool", "str", "enum"} and cname in {"out", "value"}:
            if value is None:
                value = child.get("val")
            if not unit and child.get("unit"):
                u = child.get("unit") or ""
                unit = u.rsplit("/", 1)[-1] if u else None
            if status == "ok" and child.get("status"):
                status = child.get("status") or status

    kind = classify(name) if name else classify(path)
    if kind == "other":
        if unit and re.search(r"(?i)celsius|fahrenheit|kelvin|degree", unit):
            kind = "temperature"
        elif unit and re.search(r"(?i)percent", unit):
            kind = "humidity"

    return Reading(
        path=path,
        display_name=name,
        value=value,
        unit=unit,
        status=status,
        timestamp=timestamp,
        kind=kind,
    )


def apply_limits(reading: Reading, limits: Limits) -> None:
    if reading.value is None:
        return
    try:
        v = float(reading.value)
    except ValueError:
        return

    lo = hi = None
    if reading.kind == "temperature":
        lo, hi = limits.temp_min, limits.temp_max
    elif reading.kind == "humidity":
        lo, hi = limits.rh_min, limits.rh_max

    if lo is not None and v < lo:
        reading.out_of_spec = True
        reading.spec_note = f"below min {lo}"
    elif hi is not None and v > hi:
        reading.out_of_spec = True
        reading.spec_note = f"above max {hi}"


def is_stale(reading: Reading, now: datetime) -> bool:
    if not reading.timestamp:
        return False
    try:
        ts = datetime.fromisoformat(reading.timestamp.replace("Z", "+00:00"))
    except ValueError:
        return False
    return (now - ts).total_seconds() > STALE_SECONDS


def write_csv(readings: list[Reading], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["path", "name", "kind", "value", "unit", "status",
                    "timestamp", "out_of_spec", "spec_note", "error"])
        for r in readings:
            w.writerow([r.path, r.display_name, r.kind, r.value or "",
                        r.unit or "", r.status, r.timestamp or "",
                        r.out_of_spec, r.spec_note, r.error])


def write_markdown(
    readings: list[Reading], path: Path, host: str, generated: datetime, now: datetime
) -> dict[str, int]:
    by_kind: dict[str, list[Reading]] = {"temperature": [], "humidity": [], "other": []}
    for r in readings:
        by_kind.setdefault(r.kind, []).append(r)

    bad_status = [r for r in readings if r.status and r.status != "ok"]
    out_of_spec = [r for r in readings if r.out_of_spec]
    stale = [r for r in readings if is_stale(r, now)]
    errored = [r for r in readings if r.error]

    lines: list[str] = []
    lines.append("# Niagara N4 BMS Diagnostics Report")
    lines.append("")
    lines.append(f"- Generated: {generated.isoformat()}")
    lines.append(f"- Supervisor: {host}")
    lines.append(f"- Points sampled: {len(readings)}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Temperature points: {len(by_kind.get('temperature', []))}")
    lines.append(f"- Humidity points: {len(by_kind.get('humidity', []))}")
    lines.append(f"- Other tagged points: {len(by_kind.get('other', []))}")
    lines.append(f"- Out-of-spec: {len(out_of_spec)}")
    lines.append(f"- Bad status (non-ok): {len(bad_status)}")
    lines.append(f"- Stale (> {STALE_SECONDS}s): {len(stale)}")
    lines.append(f"- Read errors: {len(errored)}")
    lines.append("")

    def table(title: str, rows: Iterable[Reading]) -> None:
        rows = list(rows)
        if not rows:
            return
        lines.append(f"## {title}")
        lines.append("")
        lines.append("| Point | Kind | Value | Unit | Status | Timestamp | Note |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in rows:
            lines.append(
                f"| `{r.display_name or r.path}` | {r.kind} | {r.value or ''} | "
                f"{r.unit or ''} | {r.status} | {r.timestamp or ''} | "
                f"{r.spec_note or r.error} |"
            )
        lines.append("")

    table("Out-of-spec readings", out_of_spec)
    table("Bad status / alarms", bad_status)
    table("Stale readings", stale)
    table("Read errors", errored)

    lines.append("## All readings")
    lines.append("")
    lines.append("See accompanying CSV file for the full point list.")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    return {
        "out_of_spec": len(out_of_spec),
        "bad_status": len(bad_status),
        "stale": len(stale),
        "errors": len(errored),
    }


def load_points_file(path: Path) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append((s, s))
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", required=True, help="Supervisor base URL, e.g. https://10.0.0.5")
    p.add_argument("--user", default=os.environ.get("NIAGARA_USER"))
    p.add_argument("--password", default=os.environ.get("NIAGARA_PASSWORD"))
    p.add_argument("--root", default=DEFAULT_ROOT)
    p.add_argument("--points-file", type=Path, default=None)
    p.add_argument("--match", default=DEFAULT_MATCH)
    p.add_argument("--max-points", type=int, default=2000)
    p.add_argument("--max-depth", type=int, default=6)
    p.add_argument("--timeout", type=float, default=15.0)
    p.add_argument("--insecure", action="store_true", help="Skip TLS verification (self-signed certs)")
    p.add_argument("--out-dir", type=Path, default=Path("reports"))
    p.add_argument("--temp-min", type=float)
    p.add_argument("--temp-max", type=float)
    p.add_argument("--rh-min", type=float)
    p.add_argument("--rh-max", type=float)
    args = p.parse_args(argv)

    if not args.user or not args.password:
        print("error: --user/--password (or NIAGARA_USER/NIAGARA_PASSWORD) required", file=sys.stderr)
        return 1

    session = requests.Session()
    session.auth = HTTPBasicAuth(args.user, args.password)
    session.headers.update({"Accept": "text/xml,application/xml"})
    session.verify = not args.insecure
    if args.insecure:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base = args.host.rstrip("/") + "/"
    pattern = re.compile(args.match)

    try:
        if args.points_file:
            point_refs = load_points_file(args.points_file)
        else:
            print(f"discovering points under {args.root} (max={args.max_points}, depth={args.max_depth})...",
                  file=sys.stderr)
            point_refs = discover(session, base, args.root, pattern,
                                  args.max_points, args.max_depth, args.timeout)
    except requests.RequestException as e:
        print(f"error: discovery failed: {e}", file=sys.stderr)
        return 1

    print(f"reading {len(point_refs)} points...", file=sys.stderr)

    limits = Limits(args.temp_min, args.temp_max, args.rh_min, args.rh_max)
    readings: list[Reading] = []

    for path, fallback_name in point_refs:
        try:
            elem = fetch(session, base, path, args.timeout)
            r = parse_point(elem, path, fallback_name)
        except requests.RequestException as e:
            r = Reading(path=path, display_name=fallback_name, value=None,
                        unit=None, status="error", timestamp=None,
                        kind=classify(fallback_name), error=str(e))
        apply_limits(r, limits)
        readings.append(r)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    csv_path = args.out_dir / f"niagara_diagnostics_{stamp}.csv"
    md_path = args.out_dir / f"niagara_diagnostics_{stamp}.md"

    write_csv(readings, csv_path)
    summary = write_markdown(readings, md_path, args.host, now, now)

    print(f"wrote {csv_path}", file=sys.stderr)
    print(f"wrote {md_path}", file=sys.stderr)
    print(
        f"summary: out_of_spec={summary['out_of_spec']} bad_status={summary['bad_status']} "
        f"stale={summary['stale']} errors={summary['errors']}",
        file=sys.stderr,
    )

    if summary["out_of_spec"] or summary["bad_status"]:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
