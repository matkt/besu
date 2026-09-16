#!/usr/bin/env python3
"""RocksDB LSM level stats for all Besu column families (human-readable names).

Usage (single file — copy only this script):
  chmod +x rocksdb-besu-lsm-report.py
  sudo LDB=ldb ./rocksdb-besu-lsm-report.py /data/besu/database

If ldb is older than the DB (e.g. metadata_write_temperature), the script passes
--ignore_unknown_options (disable with LDB_IGNORE_UNKNOWN=0).

Besu must be stopped (or use a consistent snapshot copy of the database).
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

# Besu KeyValueSegmentIdentifier byte id -> enum name
BESU_SINGLE_BYTE: Dict[int, str] = dict(
    [
        (1, "BLOCKCHAIN"),
        (2, "WORLD_STATE"),
        (3, "PRIVATE_TRANSACTIONS"),
        (4, "PRIVATE_STATE"),
        (5, "PRUNING_STATE"),
        (6, "ACCOUNT_INFO_STATE"),
        (7, "CODE_STORAGE"),
        (8, "ACCOUNT_STORAGE_STORAGE"),
        (9, "TRIE_BRANCH_STORAGE"),
        (10, "TRIE_LOG_STORAGE"),
        (11, "VARIABLES"),
        (12, "GOQUORUM_PRIVATE_STORAGE"),
        (13, "BACKWARD_SYNC_HEADERS"),
        (14, "BACKWARD_SYNC_BLOCKS"),
        (15, "BACKWARD_SYNC_CHAIN"),
        (16, "SNAPSYNC_MISSING_ACCOUNT_RANGE"),
        (17, "SNAPSYNC_ACCOUNT_TO_FIX"),
        (18, "CHAIN_PRUNER_STATE"),
    ]
)

BESU_STRING_CF = frozenset(
    {
        "default",
        "ACCOUNT_INFO_STATE_ARCHIVE",
        "ACCOUNT_STORAGE_ARCHIVE",
        "TRIE_BRANCH_STORAGE_ARCHIVE",
    }
)


def cf_display_name(cf_name: str) -> str:
    if cf_name == "default":
        return "DEFAULT"
    if cf_name in BESU_STRING_CF and cf_name != "default":
        return cf_name
    if len(cf_name) == 1:
        b = ord(cf_name)
        return BESU_SINGLE_BYTE.get(b, f"UNKNOWN_BYTE_{b}")
    if cf_name.isprintable() and cf_name.strip():
        return cf_name
    return "CF_HEX_" + cf_name.encode("latin-1", "backslashreplace").hex()


def repr_cf_key(cf: str) -> str:
    if cf == "default":
        return "default"
    if len(cf) == 1:
        b = ord(cf)
        extra = {9: "TAB", 10: "LF", 11: "VT"}.get(b, "")
        suffix = f" {extra}" if extra else ""
        return f"byte=0x{b:02x}{suffix}"
    if cf.isprintable():
        return cf[:12]
    return "0x" + cf.encode("latin-1", "backslashreplace").hex()[:12]


def run_ldb(ldb: str, db_path: str, args: List[str]) -> subprocess.CompletedProcess:
    cmd = [ldb, f"--db={db_path}"]
    if os.environ.get("LDB_IGNORE_UNKNOWN", "1").lower() not in ("0", "false", "no"):
        cmd.append("--ignore_unknown_options")
    cmd.extend(args)
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="latin-1",
        errors="replace",
    )


def parse_list_column_families(raw: str) -> List[str]:
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if not lines:
        return []
    body = "".join(lines)
    if "{" in body:
        body = body.split("{", 1)[1]
    if "}" in body:
        body = body.rsplit("}", 1)[0]
    names: List[str] = []
    current: List[str] = []
    for ch in body:
        if ch == ",":
            name = "".join(current)
            if name:
                names.append(name)
            current = []
        else:
            current.append(ch)
    tail = "".join(current)
    if tail:
        names.append(tail)
    return names


def besu_known_cf_keys() -> List[str]:
    return ["default"] + [chr(b) for b in sorted(BESU_SINGLE_BYTE)] + [
        n for n in BESU_STRING_CF if n != "default"
    ]


def discover_column_families(ldb: str, db_path: str) -> List[str]:
    proc = run_ldb(ldb, db_path, ["list_column_families"])
    if proc.returncode != 0:
        print(proc.stderr or proc.stdout, file=sys.stderr)
        sys.exit(proc.returncode)
    parsed = parse_list_column_families(proc.stdout)
    # Besu uses single-byte CF names; 0x09 (TAB), 0x0a (LF), 0x0b (VT) are often dropped by .strip() or terminal display.
    merged: List[str] = []
    seen: set = set()
    for cf in parsed + besu_known_cf_keys():
        if cf not in seen:
            seen.add(cf)
            merged.append(cf)
    return merged


@dataclass
class LevelRow:
    level: int
    files: int
    size_mb: float


def parse_levelstats(stdout: str) -> List[LevelRow]:
    """Parse rocksdb.levelstats property and newer ldb 'levelstats' command output."""
    rows: List[LevelRow] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("-") or line.lower().startswith("level "):
            continue
        # ldb levelstats / cfstats compaction table: L0  12/0  1234.5 ...
        m = re.match(r"^L(\d+)\s+(\d+)(?:/\d+)?\s+([\d.]+)", line)
        if m:
            rows.append(LevelRow(int(m.group(1)), int(m.group(2)), float(m.group(3))))
            continue
        # rocksdb.levelstats property (all ldb versions): "  0        1      123"
        m = re.match(r"^(\d+)\s+(\d+)\s+([\d.]+)\s*$", line)
        if m:
            rows.append(LevelRow(int(m.group(1)), int(m.group(2)), float(m.group(3))))
    return rows


def fetch_levelstats(ldb: str, db_path: str, cf: str) -> subprocess.CompletedProcess:
    """levelstats is not an ldb subcommand on older builds; use DB property."""
    for prop in ("rocksdb.levelstats", "levelstats"):
        proc = run_ldb(ldb, db_path, ["--column_family=" + cf, "get_property", prop])
        if proc.returncode == 0 and proc.stdout.strip():
            return proc
    # Newer RocksDB ldb exposes a dedicated subcommand.
    return run_ldb(ldb, db_path, ["--column_family=" + cf, "levelstats"])


def lsm_state_vector(rows: List[LevelRow], max_level: int = 6) -> List[int]:
    by_level = {r.level: r.files for r in rows}
    return [by_level.get(i, 0) for i in range(max_level + 1)]


def get_property(ldb: str, db_path: str, cf_name: str, prop: str) -> Optional[str]:
    proc = run_ldb(ldb, db_path, ["--column_family=" + cf_name, "get_property", prop])
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def main() -> None:
    db_path = sys.argv[1] if len(sys.argv) > 1 else "/data/besu/database"
    ldb = os.environ.get("LDB", "ldb")

    if not os.path.isdir(db_path):
        print(f"error: database directory not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    cfs = discover_column_families(ldb, db_path)
    print(f"Database: {db_path}")
    print(f"ldb: {ldb}")
    print(f"Column families found: {len(cfs)}")
    print()

    header = (
        f"{'BESU_NAME':<32} {'CF_KEY':<14} {'LSM_STATE[L0..L6]':<28} "
        f"{'SST':>6} {'SIZE_MB':>10} {'TOTAL_SST':>12}"
    )
    print(header)
    print("-" * len(header))

    for cf in sorted(cfs, key=lambda c: (cf_display_name(c), c)):
        proc = fetch_levelstats(ldb, db_path, cf)
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout).strip().splitlines()
            msg = err[0] if err else "levelstats failed"
            print(
                f"{cf_display_name(cf):<32} {repr_cf_key(cf):<14} {'(missing)':<28} "
                f"{'-':>6} {'-':>10} {'-':>12}  # {msg}"
            )
            continue

        rows = parse_levelstats(proc.stdout)
        if not rows:
            print(
                f"{cf_display_name(cf):<32} {repr_cf_key(cf):<14} {'(empty)':<28} "
                f"{'0':>6} {'0':>10} {'-':>12}"
            )
            if proc.stdout.strip():
                print("    # raw levelstats:")
                for ln in proc.stdout.splitlines()[:15]:
                    print(f"    # {ln}")
            continue

        state = lsm_state_vector(rows)
        state_str = "[" + ", ".join(str(x) for x in state) + "]"
        total_files = sum(r.files for r in rows)
        total_mb = sum(r.size_mb for r in rows)
        total_sst = get_property(ldb, db_path, cf, "rocksdb.total-sst-files-size")
        total_sst_mb = "-"
        if total_sst and total_sst.isdigit():
            total_sst_mb = f"{int(total_sst) / (1024 * 1024):.1f}"

        print(
            f"{cf_display_name(cf):<32} {repr_cf_key(cf):<14} {state_str:<28} "
            f"{total_files:>6} {total_mb:>10.1f} {total_sst_mb:>12}"
        )
        for r in rows:
            if r.files > 0:
                print(f"    L{r.level}: files={r.files}, size_mb={r.size_mb:.2f}")

    print()
    print(
        "CF_KEY: byte=0xNN (0x09=TRIE_BRANCH TAB, 0x0a=TRIE_LOG LF — often hidden in ldb lists). "
        "Full flat DB: block execution reads 0x06/0x08, not trie nodes in 0x09."
    )


if __name__ == "__main__":
    main()
