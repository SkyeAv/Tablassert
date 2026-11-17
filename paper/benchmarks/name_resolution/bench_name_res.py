"""Benchmark: local fullmap resolution vs hosted SRI NameResolution + NodeNormalization.

Local side runs ``local_runner.py`` (the installed tablassert API: level_one/level_two
normalization + ``resolve_batch``). Hosted side is the two-stage Translator flow:

1. ``GET https://name-resolution-sri.renci.org/lookup?string=<term>`` per term
   (concurrency <= 4, bounded call budget) for candidate CURIEs.
2. ``POST https://nodenormalization-sri.renci.org/get_normalized_nodes`` batched
   (bulk flow, one request per chunk of CURIEs) for canonical id, preferred name
   and category. The old ``/synonyms`` endpoint is NOT used: canonical records come
   from NodeNormalization, the same service a downstream consumer would call.

Coverage is reported honestly at three distinct levels, never collapsed into one:

- ``http_ok``: the request returned HTTP 200.
- ``nonempty_candidates``: HTTP 200 AND at least one candidate CURIE returned.
- ``complete_record``: the top-ranked candidate's NodeNormalization record has a
  canonical identifier, label, and at least one Biolink category.

Known, documented mismatches (neither side is secretly adjusted to fake parity):

- Taxon: /lookup DOES support server-side taxon filtering via the pipe-separated
  ``only_taxa`` query parameter; the matched run passes ``only_taxa=NCBITaxon:9606``
  on every lookup. This does not prove identical handling of candidates without
  taxon metadata; fullmap explicitly retains such rows. On top of that, the harness records a post-hoc human-taxon preference
  (``human_pref``) over the ranked candidate records, kept explicitly separate from
  ``canonical`` and clearly labeled as post-filtering.
- Category policy: the hosted API supports Biolink-type filters, but those are not
  equivalent to fullmap's per-column prioritize/avoid ranking and context rules.
  This run enables no category filter or priority on either side.
- No column context is used on the local side either (``column_context=False``),
  so both sides resolve terms out of table context.

Usage (on the build host):

    python bench_name_res.py --terms terms.txt --fullmap /path/to/fullmap \
        --python /path/to/python --out results.json --local-repeats 3 \
        --hosted-concurrency 4 --hosted-max-requests 300 --limit 5

Writes results.json with an ``api_spec`` block and separate local-run JSON files.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import statistics
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from importlib.metadata import version
from pathlib import Path
from threading import Lock
from typing import TypedDict, cast

LOOKUP_BASE = "https://name-resolution-sri.renci.org"
NODENORM_BASE = "https://nodenormalization-sri.renci.org"
RETRYABLE = frozenset({429, 500, 502, 503, 504})
USER_AGENT = "tablassert-benchmark/2.0"
HUMAN_TAXON = "NCBITaxon:9606"
HUMAN_TAXON_ID = "9606"


# ---------------------------------------------------------------- requests


class LookupRecord(TypedDict):
    """Outcome of one stage-1 /lookup call."""

    term: str
    http_status: int | None
    error: str | None
    curies: list[str]
    latency_s: float | None


class NormalizeResult(TypedDict):
    """Outcome of the batched NodeNormalization stage."""

    normalized: dict[str, dict[str, object]]
    http_calls: int
    error: str | None


def fetch_json(req: urllib.request.Request, timeout: float = 30.0, attempts: int = 3, budget: RequestBudget | None = None) -> tuple[int, object]:
    """One HTTP request, retried with linear backoff on transient errors.

    Returns (status, parsed body). Raises the last error when retries are exhausted;
    callers turn exceptions into failure records, never silent coverage.
    """
    last: Exception | None = None
    for attempt in range(attempts):
        if budget is not None:
            budget.spend()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code in RETRYABLE and attempt < attempts - 1:
                time.sleep(2.0 * (attempt + 1))
                continue
            raise
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last = exc
            if attempt < attempts - 1:
                time.sleep(2.0 * (attempt + 1))
                continue
            raise
    raise last if last else RuntimeError("unreachable")


class BudgetExhausted(RuntimeError):
    """The next HTTP attempt would exceed the configured budget."""


class RequestBudget:
    """Thread-safe hard cap on all HTTP attempts, including retries."""

    def __init__(self, max_requests: int) -> None:
        self.remaining = max_requests
        self._lock = Lock()

    def spend(self, n: int = 1) -> None:
        with self._lock:
            if n > self.remaining:
                msg = f"hosted request budget exhausted: needed {n}, remaining {self.remaining}"
                raise BudgetExhausted(msg)
            self.remaining -= n


# ---------------------------------------------------------------- hosted flow


def lookup_one(term: str, limit: int, budget: RequestBudget, only_taxa: str | None = HUMAN_TAXON) -> LookupRecord:
    """Stage 1: GET /lookup for one term. Never raises; failures become records.

    ``only_taxa`` uses the service's pipe-separated taxon filter format; the
    default NCBITaxon:9606 makes the hosted run taxon-matched to fullmap.
    """
    record: LookupRecord = {"term": term, "http_status": None, "error": None, "curies": [], "latency_s": None}
    params: dict[str, str] = {"string": term, "limit": str(limit)}
    if only_taxa:
        params["only_taxa"] = only_taxa
    url = f"{LOOKUP_BASE}/lookup?{urllib.parse.urlencode(params)}"
    start = time.perf_counter()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        status, body = fetch_json(req, budget=budget)
        record["http_status"] = status
        hits = body if isinstance(body, list) else []
        curies: list[str] = []
        for hit in hits:
            if isinstance(hit, dict):
                curie = hit.get("curie")
                if isinstance(curie, str):
                    curies.append(curie)
        record["curies"] = curies
    except BudgetExhausted:
        raise
    except Exception as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        record["latency_s"] = time.perf_counter() - start
    return record


def normalize_batch(curies: list[str], budget: RequestBudget, batch_size: int = 100) -> NormalizeResult:
    """Stage 2: POST /get_normalized_nodes over all candidate CURIEs in chunks (bulk flow).

    Returns {"normalized": {curie: {...}}, "http_calls": n, "error": str | None}.
    """
    normalized: dict[str, dict[str, object]] = {}
    calls = 0
    error: str | None = None
    for start in range(0, len(curies), batch_size):
        chunk = curies[start : start + batch_size]
        try:
            calls += 1
            payload = json.dumps({"curies": chunk}).encode("utf-8")
            req = urllib.request.Request(
                f"{NODENORM_BASE}/get_normalized_nodes", data=payload, headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"}
            )
            _, body = fetch_json(req, budget=budget)
            normalized.update(body if isinstance(body, dict) else {})
        except BudgetExhausted:
            raise
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
    result: NormalizeResult = {"normalized": normalized, "http_calls": calls, "error": error}
    return result


def canonical_record(entry: object) -> dict[str, object] | None:
    """Extract {id, preferred_name, categories} from one NodeNormalization entry.

    ``complete_record`` requires all three fields non-empty.
    """
    if not isinstance(entry, dict):
        return None
    ident = entry.get("id")
    if not isinstance(ident, dict):
        return None
    curie = ident.get("identifier")
    label = ident.get("label")
    raw_types = entry.get("type")
    types = [t for t in raw_types if isinstance(t, str)] if isinstance(raw_types, list) else []
    if not curie or not label or not types:
        return None
    return {"id": curie, "preferred_name": label, "categories": types, "taxa": entry.get("taxa")}


def prefers_human(entry: object) -> bool:
    """Post-hoc human-taxon preference: keep taxa-bearing entries only if human is present.

    Entries without taxa information stay eligible. This is a documented post-filter
    on top of /lookup output, NOT equivalent to fullmap's in-database taxon constraint.
    """
    if entry is None or not isinstance(entry, dict):
        return False
    taxa = entry.get("taxa")
    if not isinstance(taxa, list) or not taxa:
        return True
    return HUMAN_TAXON in taxa


def group_normalized_by_requested(requested: list[str], normalized: dict[str, object]) -> dict[str, dict[str, object] | None]:
    """Group NodeNormalization results by requested candidate CURIE.

    The service keys its response by the requested (input) CURIEs, not canonical
    ids, so records are resolved per requested candidate, preserving the /lookup
    ranking. As a robustness fallback for canonical-keyed responses, entries are
    additionally indexed by canonical identifier and equivalent identifiers. A
    record is never looked up by canonical id alone.
    """
    by_identity: dict[str, dict[str, object]] = {}
    for key, entry in normalized.items():
        if not isinstance(entry, dict):
            continue
        by_identity.setdefault(key, entry)
        record = canonical_record(entry)
        if record is None:
            continue
        ident = record["id"]
        if isinstance(ident, str):
            by_identity.setdefault(ident, entry)
        equivalents = entry.get("equivalent_identifiers")
        if isinstance(equivalents, list):
            for eq in equivalents:
                if isinstance(eq, dict) and isinstance(eq.get("identifier"), str):
                    by_identity.setdefault(eq["identifier"], entry)
    return {curie: by_identity.get(curie) for curie in requested}


def bench_hosted(terms: list[str], concurrency: int, limit: int, budget: RequestBudget) -> dict[str, object]:
    """Full hosted flow over ``terms``: per-term /lookup, then batched NodeNormalization."""
    expected_calls = len(terms) + math.ceil(len(terms) * limit / 100)
    if budget.remaining < expected_calls:
        raise BudgetExhausted(f"hosted request budget needs at least {expected_calls} calls before retries")
    wall_start = time.perf_counter()
    lookup_attempts_start = budget.remaining
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(lookup_one, term, limit, budget): term for term in terms}
        lookups: list[LookupRecord] = [future.result() for future in as_completed(futures)]
    lookups.sort(key=lambda result: result["term"])
    lookup_wall = time.perf_counter() - wall_start
    lookup_attempts = lookup_attempts_start - budget.remaining

    all_curies = sorted({curie for rec in lookups for curie in rec["curies"]})
    norm_start = time.perf_counter()
    norm_attempts_start = budget.remaining
    norm = normalize_batch(all_curies, budget)
    norm_wall = time.perf_counter() - norm_start
    grouped = group_normalized_by_requested(all_curies, cast("dict[str, object]", norm["normalized"]))

    records: list[dict[str, object]] = []
    for rec in lookups:
        curies = rec["curies"]
        # /lookup returns candidates ranked by relevance; keep that ranking and
        # resolve every candidate's record through the requested-CURIE grouping.
        ranked = [(curie, canonical_record(grouped.get(curie))) for curie in curies]
        top = ranked[0][1] if ranked else None
        # canonical result: the TOP-RANKED candidate's record, never the first
        # complete one found by arbitrary scan order.
        human = next((entry for _, entry in ranked if entry is not None and prefers_human(entry)), None)
        records.append(
            {
                "term": rec["term"],
                "http_status": rec["http_status"],
                "error": rec["error"],
                "latency_s": rec["latency_s"],
                "candidate_curies": curies,
                "candidate_records": [{"curie": curie, "canonical": entry} for curie, entry in ranked],
                # canonical id + preferred name + categories of the top-ranked candidate
                "canonical": top,
                # post-hoc human-taxon preference over the ranked records,
                # documented as post-filtering and kept separate from canonical
                "human_pref": human,
            }
        )

    http_ok = sum(1 for r in records if r["http_status"] == 200)
    nonempty = sum(1 for r in records if r["candidate_curies"])
    # complete_record: the TOP-RANKED candidate produced a complete canonical record
    complete = sum(1 for r in records if r["canonical"])
    human_ok = sum(1 for r in records if r["human_pref"])
    lat: list[float] = []
    for rec in records:
        value = rec.get("latency_s")
        if isinstance(value, float) and rec.get("error") is None:
            lat.append(value)
    return {
        "repeats": 1,
        "concurrency": concurrency,
        "lookup_limit": limit,
        "wall_s": lookup_wall + norm_wall,
        "lookup_wall_s": lookup_wall,
        "normalization_wall_s": norm_wall,
        "http_calls": {"lookup": len(terms), "get_normalized_nodes": norm["http_calls"]},
        "http_attempts_including_retries": {"lookup": lookup_attempts, "get_normalized_nodes": norm_attempts_start - budget.remaining},
        "coverage": {
            # three distinct levels; never collapsed into one number
            "http_ok": http_ok,
            "nonempty_candidates": nonempty,
            "complete_record": complete,
            "complete_record_human_pref": human_ok,
            "n_terms": len(records),
        },
        "latency_s": {"median": statistics.median(lat) if lat else None, "mean": statistics.mean(lat) if lat else None},
        "lookup_terms_per_s": len(records) / lookup_wall if lookup_wall else None,
        "terms_per_s_effective": len(records) / (lookup_wall + norm_wall) if lookup_wall + norm_wall else None,
        "normalization_error": norm["error"],
        "records": records,
    }


# ---------------------------------------------------------------- local + main


def bench_local(python: str, fullmap: Path, terms: list[str], repeats: int, out_dir: Path) -> dict[str, object]:
    """Cold: launch ONE fresh local_runner process per repeat; warm: one process, repeated.

    Each cold repeat is its own process (fresh interpreter, empty term cache, fresh
    redb handles), so cold is not a repeated in-process measurement. Warm runs
    ``repeats`` passes back to back inside a single process. The OS file cache is
    NOT cleared, so cold measures process-level first touch only; that limitation
    is recorded here, never claimed away.
    """
    runner = Path(__file__).with_name("local_runner.py")

    def spawn(mode: str, run_repeats: int, out: Path) -> dict[str, object]:
        argv = [python, str(runner), "--terms", "-", "--fullmap", str(fullmap), "--mode", mode, "--repeats", str(run_repeats), "--out", str(out)]
        proc = subprocess.run(argv, input="\n".join(terms) + "\n", capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            msg = f"local runner ({mode}) exited {proc.returncode}: {proc.stderr[-800:]}"
            raise RuntimeError(msg)
        return json.loads(out.read_text())

    cold_files = [out_dir / f"local_cold_run{i}.json" for i in range(repeats)]
    cold_runs = [spawn("cold", 1, out) for out in cold_files]
    cold_wall = [float(cast("float", run["wall_s_median"])) for run in cold_runs]
    median = statistics.median(cold_wall)
    median_idx = cold_wall.index(median)
    cold: dict[str, object] = {
        "repeats": repeats,
        "fresh_process_per_repeat": True,
        "os_cache_cleared": False,
        "wall_s": cold_wall,
        "wall_s_median": median,
        "unique_terms_per_s_median": len(terms) / median if median else None,
        "per_unique_term_ms_median": median / len(terms) * 1000,
        "resolved_rows_per_run": [cast("list[int]", run["resolved_rows_per_run"])[0] for run in cold_runs],
        "resolved_terms_per_run": [cast("int", run["resolved_terms_per_run"]) for run in cold_runs],
        # rows of the median cold process
        "records": cold_runs[median_idx]["records"],
        "run_files": [path.name for path in cold_files],
        "note": (
            "Each cold repeat is a separate local_runner process. The timer surrounds "
            "resolve_batch collection only and excludes interpreter startup, imports, "
            "and input parsing. The OS file cache is not cleared, so this is not a "
            "cold-page-cache measurement."
        ),
    }
    return {"cold": cold, "warm": spawn("warm", repeats, out_dir / "local_warm.json")}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--terms", type=Path, required=True)
    parser.add_argument("--fullmap", type=Path, required=True)
    parser.add_argument("--python", required=True, help="python with tablassert importable")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--local-repeats", type=int, default=3)
    parser.add_argument("--hosted-concurrency", type=int, default=4)
    parser.add_argument("--hosted-max-requests", type=int, default=300)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--skip-hosted", action="store_true")
    parser.add_argument("--skip-local", action="store_true")
    args = parser.parse_args()

    input_terms = [line.strip() for line in args.terms.read_text().splitlines() if line.strip() and not line.strip().startswith("#")]
    terms = sorted(set(input_terms))
    budget = RequestBudget(args.hosted_max_requests)

    result: dict[str, object] = {
        "date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "tablassert_version": version("tablassert") if not args.skip_local else None,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "fullmap": str(args.fullmap),
        "n_input_rows": len(input_terms),
        "n_unique_terms": len(terms),
        "sample_selection": "hand-selected ingest vocabulary; not a random or representative accuracy sample",
        "unique_terms_deterministic": True,
        "terms": terms,
        "hosted_request_budget": args.hosted_max_requests,
    }
    if not args.skip_local:
        result["local"] = bench_local(args.python, args.fullmap, terms, args.local_repeats, args.out.parent)
    if not args.skip_hosted:
        result["hosted"] = bench_hosted(terms, args.hosted_concurrency, args.limit, budget)
        result["hosted_requests_remaining"] = budget.remaining
        result["api_spec"] = api_spec(args.limit)

    args.out.write_text(json.dumps(result, indent=2))
    print(f"wrote {args.out}; hosted budget remaining: {budget.remaining}")


def api_spec(limit: int = 5) -> dict[str, object]:
    """Saved parameters of both hosted endpoints as called by this harness."""
    return {
        "lookup": {
            "base": LOOKUP_BASE,
            "method": "GET",
            "path": "/lookup",
            "params": {
                "string": "free-text term (URL-quoted)",
                "limit": "max candidates returned",
                "only_taxa": "pipe-separated taxon filter, e.g. NCBITaxon:9606",
                "biolink_type": "query param filtering candidates to biolink types",
                "only_prefixes": "pipe-separated prefix filter (not used here)",
                "exclude_prefixes": "pipe-separated prefix exclusion (not used here)",
            },
            "called_with": {"limit": limit, "only_taxa": HUMAN_TAXON, "retry": "3 attempts, linear backoff 2s/4s", "retries_on": sorted(RETRYABLE)},
            "taxon_filter": f"only_taxa={HUMAN_TAXON} passed on every lookup (matched run)",
            "unknown_taxa_handling": ("fullmap retains taxon-free rows; equivalence of hosted unknown-taxon handling is not established"),
            "bulk": "POST /bulk-lookup supports biolink_types in the request body; this harness uses GET per term",
        },
        "node_normalization": {
            "base": NODENORM_BASE,
            "method": "POST",
            "path": "/get_normalized_nodes",
            "params": {"curies": 'JSON body {"curies": [...]}, batched 100 per request'},
            "returns": "per-CURIE {id:{identifier,label}, type:[...], equivalent_identifiers, taxa, information_content}",
            "batch_size": 100,
            "response_keying": "response is keyed by the requested (input) CURIEs, not canonical ids; results are grouped by requested candidate via group_normalized_by_requested",
            "synonyms_endpoint": "not used; canonical records come from get_normalized_nodes",
        },
        "local_fullmap": {
            "api": "tablassert.fullmap.resolve_batch + tablassert.nlp.level_one/level_two",
            "column_context": False,
            "taxon": HUMAN_TAXON_ID,
        },
    }


if __name__ == "__main__":
    main()
