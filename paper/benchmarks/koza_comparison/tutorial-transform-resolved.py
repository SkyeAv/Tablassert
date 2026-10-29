"""Koza 2.7.0 transform with hosted label resolution and normalization.

Uses the same NameResolution then NodeNormalization client as the standalone
hosted benchmark. Raw labels require both endpoints: NodeNormalization accepts
CURIEs, not labels. Only the top-ranked candidate is emitted. Unresolved terms,
request failures, and incomplete canonical records fail the run.
"""

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import koza

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "name_resolution"))
from bench_name_res import RequestBudget, canonical_record, group_normalized_by_requested, lookup_one, normalize_batch

ASSOCIATION = "biolink:associated_with"


@dataclass
class NamedThing:
    id: str
    name: str
    category: list[str] = field(default_factory=list)


@dataclass
class Association:
    subject: str
    predicate: str
    object: str
    primary_knowledge_source: str
    publications: list[str] = field(default_factory=list)
    p_value: float | None = None
    sample_size: int | None = None


def resolve_labels(records: list[dict[str, str]]) -> tuple[dict[str, dict[str, object]], dict[str, object]]:
    terms = sorted({record[column] for record in records for column in ("gene_symbol", "disease_name")})
    started = time.perf_counter()
    budget = RequestBudget(len(terms) + 20)
    only_taxa = "NCBITaxon:9606"
    with ThreadPoolExecutor(max_workers=4) as pool:
        lookups = list(pool.map(lambda term: lookup_one(term, 5, budget, only_taxa=only_taxa), terms))
    lookup_attempts = len(terms) + 20 - budget.remaining
    normalization_attempts_start = budget.remaining
    curies = sorted({curie for result in lookups for curie in result["curies"]})
    normalization = normalize_batch(curies, budget)
    if normalization["error"]:
        raise RuntimeError(normalization["error"])
    grouped = group_normalized_by_requested(curies, normalization["normalized"])
    resolved: dict[str, dict[str, object]] = {}
    for result in lookups:
        if result["error"] or not result["curies"]:
            raise RuntimeError(f"hosted lookup failed for {result['term']!r}: {result['error'] or 'no candidates'}")
        record: dict[str, object] | None = canonical_record(grouped.get(result["curies"][0]))
        if record is None:
            raise RuntimeError(f"incomplete top-ranked canonical record for {result['term']!r}")
        resolved[result["term"]] = record
    report: dict[str, object] = {
        "input_rows": len(records),
        "unique_terms": len(terms),
        "complete_records": len(resolved),
        "candidate_curies": len(curies),
        "http_calls": {"lookup": len(lookups), "get_normalized_nodes": normalization["http_calls"]},
        "http_attempts_including_retries": {"lookup": lookup_attempts, "get_normalized_nodes": normalization_attempts_start - budget.remaining},
        "resolution_wall_s": time.perf_counter() - started,
        "lookup_limit": 5,
        "lookup_concurrency": 4,
        "only_taxa": only_taxa,
        "selection": "top-ranked NameResolution candidate; no ties emitted",
    }
    return resolved, report


def categories(record: dict[str, object]) -> list[str]:
    values = record["categories"]
    if not isinstance(values, list):
        raise TypeError("canonical categories must be a list")
    return [str(value) for value in values]


@koza.transform()
def transform_rows(koza, data):
    records = list(data)
    resolved, report = resolve_labels(records)
    Path("koza-hosted-resolution.json").write_text(json.dumps(report, indent=2))
    for record in records:
        subject = resolved[record["gene_symbol"]]
        obj = resolved[record["disease_name"]]
        koza.write(
            NamedThing(id=str(subject["id"]), name=str(subject["preferred_name"]), category=categories(subject)),
            NamedThing(id=str(obj["id"]), name=str(obj["preferred_name"]), category=categories(obj)),
            Association(
                subject=str(subject["id"]),
                predicate=ASSOCIATION,
                object=str(obj["id"]),
                primary_knowledge_source="infores:tutorial-kg",
                publications=["PMID:12345678"],
                p_value=float(record["p_value"]),
                sample_size=int(record["sample_size"]),
            ),
        )
