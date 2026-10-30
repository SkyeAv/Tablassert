"""Standalone local fullmap runner for the name-resolution benchmark.

Uses the installed tablassert API exactly as a build does: ``level_one`` /
``level_two`` normalization followed by ``tablassert.fullmap.resolve_batch``.
No code is injected as a string; this file is the measured code.

One input row is kept per original term via the ``input_term`` column, which
survives ``resolve_batch``'s join untouched, so every resolved row (including
ties, which resolve to multiple rows) still tracks its input term.

Usage:

Cold mode measures ONE pass in this process; the benchmark harness
(``bench_name_res.py``) launches one fresh ``local_runner`` process per cold
repeat, so cold repeats are genuinely separate processes. Warm mode runs
``--repeats`` passes back to back in a single process. Neither mode clears the
OS file cache; cold measures process-level first touch only.

    python local_runner.py --terms terms.txt --fullmap /path/to/fullmap \
        --mode cold --repeats 1 --out local_cold_run0.json
    python local_runner.py --terms terms.txt --fullmap /path/to/fullmap \
        --mode warm --repeats 3 --out local_warm.json

``--terms -`` reads the term list from stdin (one term per line).
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import time
from importlib.metadata import version
from pathlib import Path
from typing import cast


def read_terms(path: Path) -> list[str]:
    # "-" means stdin: the benchmark driver pipes the term list through it
    lines = sys.stdin.read().splitlines() if str(path) == "-" else path.read_text(encoding="utf-8").splitlines()
    terms: list[str] = []
    for line in lines:
        term = line.strip()
        if term and not term.startswith("#"):
            terms.append(term)
    return terms


def unique_terms_deterministic(terms: list[str]) -> list[str]:
    """Deduplicate terms deterministically: sorted unique, stable across runs."""
    return sorted(set(terms))


def run_once(terms: list[str], fullmap: Path) -> tuple[float, list[dict[str, object]]]:
    """One timed ``resolve_batch`` pass over ``terms``; returns (seconds, per-term records)."""
    import polars as pl

    from tablassert.fullmap import ResolveSpec, resolve_batch
    from tablassert.nlp import level_one, level_two

    # input_term preserves the raw string; term is consumed (normalized, then
    # overwritten with the resolved CURIE) by the resolve op chain.
    lf = pl.LazyFrame({"input_term": terms, "term": terms})
    lf = level_two(level_one(lf, "term"), "term")
    spec = ResolveSpec(col="term", taxon="9606")
    start = time.perf_counter()
    out = resolve_batch(lf, [spec], db=fullmap, log=False, column_context=False).collect()
    elapsed = time.perf_counter() - start
    records: list[dict[str, object]] = out.select(
        "input_term",
        pl.col("term").alias("canonical_id"),
        pl.col("term_name").alias("preferred_name"),
        pl.col("term_category").alias("category"),
        pl.col("term_taxon").alias("taxon"),
    ).to_dicts()
    return elapsed, records


def measure(terms: list[str], fullmap: Path, mode: str, repeats: int) -> dict[str, object]:
    """Cold mode: ``repeats`` passes here; the harness launches one process per pass.

    Each cold pass runs in its own ``local_runner`` invocation. Warm mode runs
    ``repeats`` passes back to back in one process (term cache and redb handles
    stay hot). The timer surrounds only ``resolve_batch(...).collect()``: process
    startup, imports, and input parsing are excluded. The OS file cache is not
    cleared by either mode. The modes are reported separately, never pooled.
    """
    runs: list[float] = []
    records_by_run: list[list[dict[str, object]]] = []
    for _ in range(repeats):
        elapsed, records = run_once(terms, fullmap)
        if not records:
            raise RuntimeError("local resolution returned no rows; refusing to report throughput")
        runs.append(elapsed)
        records_by_run.append(records)
    resolved = [len(records) for records in records_by_run]
    median_idx = runs.index(statistics.median(runs))
    resolved_rows = records_by_run[median_idx]
    resolved_terms = {record["input_term"] for record in resolved_rows}
    return {
        "mode": mode,
        "repeats": repeats,
        "n_terms": len(terms),
        "wall_s": runs,
        "wall_s_median": statistics.median(runs),
        "unique_terms_per_s_median": len(terms) / statistics.median(runs),
        "per_unique_term_ms_median": statistics.median(runs) / len(terms) * 1000,
        "resolved_rows_per_run": resolved,
        "resolved_terms_per_run": len(resolved_terms),
        # Rows from the median run: canonical id, preferred name, category, taxon per
        # input term (ties appear as multiple rows for one input term).
        "records": resolved_rows,
        "note": (
            "resolve_batch dedups terms before the redb fetch; throughput is over unique "
            "terms. For cold, the harness launches one local_runner process per repeat; "
            "warm repeats share one process. The OS file cache is not cleared. Ties "
            "resolve to multiple rows per input term."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--terms", type=Path, required=True)
    parser.add_argument("--fullmap", type=Path, required=True)
    parser.add_argument("--mode", choices=("cold", "warm"), required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    terms = unique_terms_deterministic(read_terms(args.terms))
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    result: dict[str, object] = {
        "date_utc": started,
        "tablassert_version": version("tablassert"),
        "polars_version": version("polars"),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "fullmap": str(args.fullmap),
        "mode": args.mode,
        "resolve_policy": {
            "taxon": "9606",
            "column_context": False,
            "prioritize": None,
            "avoid": None,
            "note": "per-term lookup: no column context, single taxon=9606 constraint",
        },
        **measure(terms, args.fullmap, args.mode, args.repeats),
    }
    args.out.write_text(json.dumps(result, indent=2))
    median = cast("float", result["wall_s_median"])
    print(f"{args.mode}: median {median:.3f}s over {len(terms)} unique terms; wrote {args.out}")


if __name__ == "__main__":
    main()
