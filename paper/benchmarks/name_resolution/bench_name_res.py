"""Benchmark: local fullmap resolution (tablassert quick-map) vs hosted SRI name resolution.

Compares Tablassert's embedded fullmap database against the Translator SRI hosted
name resolution service (https://name-resolution-sri.renci.org) on the same term
list. Terms are drawn from the DAKP production audit's most frequent free-text
subject/object pairs, so they are real ingest vocabulary, not synthetic.

Usage (on the build host):

    python bench_name_res.py --terms terms.txt --fullmap /path/to/fullmap \\
        --tablassert /path/to/tablassert --out results.json --repeats 3 \\
        --hosted-concurrency 6 --limit 5

Writes results.json. See README.md for the recorded run environment.
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

HOSTED_BASE = "https://name-resolution-sri.renci.org"
HOSTED_URL = f"{HOSTED_BASE}/lookup"


def read_terms(path: Path) -> list[str]:
    terms: list[str] = []
    for line in path.read_text().splitlines():
        term = line.strip()
        if term and not term.startswith("#"):
            terms.append(term)
    return terms


def bench_local(python: str, fullmap: str, terms: list[str], repeats: int) -> dict:
    """Time tablassert.fullmap.resolve_batch -- the exact resolution path a build runs.

    Runs level_one/level_two normalization then resolve_batch over a DataFrame
    holding every term, mirroring lib.build_graph_pipeline's op chain, and
    collects the result. One process per repeat.
    """
    runner = Path(__file__).with_name("_local_runner.py")
    terms_file = Path(__file__).with_name("_terms.json.tmp")
    terms_file.write_text(json.dumps(terms))
    code = """\nimport json, sys, time\nimport polars as pl\nfrom pathlib import Path\nfrom tablassert.fullmap import ResolveSpec, resolve_batch\nfrom tablassert.nlp import level_one, level_two\nterms = json.load(open(sys.argv[1]))\nrepeats = int(sys.argv[2])\ndb = Path(sys.argv[3])\nruns, resolved = [], []\nfor _ in range(repeats):\n    lf = pl.LazyFrame({"term": terms})\n    lf = level_two(level_one(lf, "term"), "term")\n    spec = ResolveSpec(col="term", taxon="9606")\n    start = time.perf_counter()\n    out = resolve_batch(lf, [spec], db=db, log=False, column_context=True).collect()\n    runs.append(time.perf_counter() - start)\n    resolved.append(out.height)\nprint(json.dumps({"runs": runs, "resolved": resolved, "n_terms": len(terms), "columns": out.columns}))\n"""
    runner.write_text(code)
    argv = [python, str(runner), str(terms_file), str(repeats), fullmap]
    start = time.perf_counter()
    proc = subprocess.run(argv, capture_output=True, text=True, env={"PYTHONIOENCODING": "utf-8"}, check=False)
    total = time.perf_counter() - start
    if proc.returncode != 0:
        msg = f"local runner exited {proc.returncode}: {proc.stderr[-800:]}"
        raise RuntimeError(msg)
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    runs = payload["runs"]
    n_unique = len(set(terms))
    return {
        "n_terms": payload["n_terms"],
        "n_unique_terms": n_unique,
        "repeats": repeats,
        "resolve_wall_s": runs,
        "resolve_wall_s_median": statistics.median(runs),
        "unique_terms_per_s_median": n_unique / statistics.median(runs),
        "per_unique_term_ms_median": statistics.median(runs) / n_unique * 1000,
        "resolved_rows": payload["resolved"],
        "process_total_s_incl_startup": total,
        "note": "resolve_batch dedups terms before the redb fetch; throughput is over unique terms",
    }


def _fetch(req: urllib.request.Request, timeout: float) -> object:
    """Open a request with two retries (exponential backoff) on transient errors."""
    last: Exception | None = None
    for attempt in range(3):
        try:
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(2.0 * (attempt + 1))
                continue
            raise
        except urllib.error.URLError as exc:
            last = exc
            if attempt < 2:
                time.sleep(2.0 * (attempt + 1))
                continue
            raise
    raise last if last else RuntimeError("unreachable")


def hosted_lookup(term: str, limit: int, timeout: float = 30.0, canonicalize: bool = False) -> dict:
    """One term through the hosted flow. With canonicalize, runs the full identifier-record
    workflow: /lookup for the string, then /synonyms on the top curie to fetch the
    canonical identifier's preferred name and categories (two HTTP calls).
    """
    url = f"{HOSTED_BASE}/lookup?string={urllib.parse.quote(term)}&limit={limit}"
    start = time.perf_counter()
    record: dict = {"term": term, "status": None, "latency_s": None, "curies": [], "canonical": None}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "tablassert-benchmark/1.0"})
        with _fetch(req, timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            record["status"] = resp.status
            record["curies"] = [hit.get("curie") for hit in body[:limit] if hit.get("curie")]
        if canonicalize and record["curies"]:
            payload = json.dumps({"preferred_curies": [record["curies"][0]]}).encode("utf-8")
            req = urllib.request.Request(
                f"{HOSTED_BASE}/synonyms", data=payload, headers={"User-Agent": "tablassert-benchmark/1.0", "Content-Type": "application/json"}
            )
            with _fetch(req, timeout) as resp:
                syn = json.loads(resp.read().decode("utf-8"))
                entry = syn.get(record["curies"][0], {})
                record["canonical"] = {
                    "curie": entry.get("curie"),
                    "preferred_name": entry.get("preferred_name"),
                    "types": entry.get("types", [])[:3],
                }
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        record["status"] = f"error: {type(exc).__name__}"
    finally:
        record["latency_s"] = time.perf_counter() - start
    return record


def bench_hosted(terms: list[str], concurrency: int, limit: int, canonicalize: bool = False) -> dict:
    results: list[dict] = []
    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(hosted_lookup, term, limit, 30.0, canonicalize): term for term in terms}
        for future in as_completed(futures):
            results.append(future.result())
    wall = time.perf_counter() - wall_start
    ok = [r for r in results if r["status"] == 200]
    lat = [r["latency_s"] for r in ok]
    got_canonical = sum(1 for r in ok if r.get("canonical"))
    return {
        "n_terms": len(terms),
        "concurrency": concurrency,
        "canonicalize": canonicalize,
        "wall_s": wall,
        "ok": len(ok),
        "errors": len(results) - len(ok),
        "coverage": len(ok) / len(results),
        "canonical_records": got_canonical,
        "latency_s": {
            "median": statistics.median(lat) if lat else None,
            "p95": sorted(lat)[int(len(lat) * 0.95) - 1] if lat else None,
            "mean": statistics.mean(lat) if lat else None,
        },
        "terms_per_s_effective": len(ok) / wall if wall else None,
        "records": results,
        "note": (
            "per-term /lookup + /synonyms, the full identifier-record workflow"
            if canonicalize
            else f"per-term HTTP GET, concurrency {concurrency}; a build would need one request per term"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--terms", type=Path, required=True)
    parser.add_argument("--fullmap", required=True)
    parser.add_argument("--python", required=True, help="python with tablassert importable")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--hosted-concurrency", type=int, default=6)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--skip-hosted", action="store_true")
    args = parser.parse_args()

    terms = read_terms(args.terms)
    version = subprocess.run(
        [args.python, "-c", 'from importlib.metadata import version; print(version("tablassert"))'], capture_output=True, text=True, check=True
    ).stdout.strip()

    result: dict = {
        "date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "tablassert_version": version,
        "fullmap": args.fullmap,
        "hosted_endpoint": HOSTED_URL,
        "local": bench_local(args.python, args.fullmap, terms, args.repeats),
    }
    if not args.skip_hosted:
        result["hosted"] = bench_hosted(terms, args.hosted_concurrency, args.limit)
        result["hosted_multihop"] = bench_hosted(terms, args.hosted_concurrency, args.limit, canonicalize=True)

    args.out.write_text(json.dumps(result, indent=2))
    local = result["local"]
    print(f"local: {local['unique_terms_per_s_median']:.0f} unique terms/s (median of {local['repeats']} runs)")
    if "hosted" in result:
        hosted = result["hosted"]
        med = hosted["latency_s"]["median"]
        med_s = f"{med:.3f}" if med is not None else "n/a"
        print(f"hosted: {hosted['ok']}/{hosted['n_terms']} ok, median latency {med_s}s/term")
        multi = result.get("hosted_multihop")
        if multi:
            mmed = multi["latency_s"]["median"]
            mmed_s = f"{mmed:.3f}" if mmed is not None else "n/a"
            print(f"hosted multihop: {multi['ok']}/{multi['n_terms']} ok, median {mmed_s}s/term, canonical {multi['canonical_records']}")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
