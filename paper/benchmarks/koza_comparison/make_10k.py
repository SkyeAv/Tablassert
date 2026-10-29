#!/usr/bin/env python3
"""Deterministic 10,000-row generator shared by both koza-comparison runners.

Replicates tutorial-data.csv to exactly 10,000 data rows (plus header), preserving
row order, so Tablassert and Koza measure the identical input. Same output bytes
on every run (no RNG, no timestamps).
"""

from __future__ import annotations

import argparse
from pathlib import Path

TARGET_ROWS = 10_000


def generate(source: Path, out: Path) -> int:
    lines = source.read_text(encoding="utf-8").splitlines()
    header, rows = lines[0], [line for line in lines[1:] if line.strip()]
    if not rows:
        msg = f"{source} has no data rows"
        raise ValueError(msg)
    data = [header]
    while len(data) - 1 < TARGET_ROWS:
        data.extend(rows[: min(TARGET_ROWS - (len(data) - 1), len(rows))])
    out.write_text("\n".join(data) + "\n", encoding="utf-8")
    return len(data) - 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(__file__).with_name("tutorial-data.csv"))
    parser.add_argument("--out", type=Path, default=Path(__file__).with_name("tutorial-data-10k.csv"))
    args = parser.parse_args()
    rows = generate(args.source, args.out)
    print(f"wrote {args.out} with {rows} data rows")


if __name__ == "__main__":
    main()
