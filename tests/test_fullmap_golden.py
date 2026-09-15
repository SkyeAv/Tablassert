"""STRICT golden integration tests for the fullmap build, via the public ``rs`` API.

These pin the EXACT lookup output of a fixed embedded fixture so any future
optimization that changes the resolved rows (terms indexed, hydrated CURIEs,
preferred names, categories, taxa, sources) fails loudly.  They complement the
Rust-side ``rust/tests/build_golden.rs`` golden (which iterates the on-disk redb
shards directly); both fixtures are logically identical, so the two goldens agree.

The canonical comparison form is a sorted list of hydrated row dicts keyed by
``(term, CURIE)``.  ``SOURCE_VERSION`` is a compile-time constant baked into every
row, so it is asserted separately (``== rs.fullmap_source_version()``) rather than
repeated inline.  The pairs-format test compares ``term -> sorted(CURIE strings)``
after hydration rather than raw ``curie_id``s, which are scheduling-dependent.

All tests are OFFLINE and use ``tmp_path``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from tablassert import rs
from tablassert.fullmap import ResolveSpec, resolve, resolve_batch


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    return path


def synonym_row(curie: str, preferred_name: Any, names: list[str], category: str, taxon: str = "NCBITaxon:9606") -> dict[str, Any]:
    return {"curie": curie, "preferred_name": preferred_name, "names": names, "types": [category], "taxa": [taxon]}


def class_row(curie: str, equivalents: list[str]) -> dict[str, Any]:
    return {"id": curie, "equivalent_identifiers": [{"identifier": x} for x in equivalents]}


# Fixed embedded fixture — logically identical to rust/tests/build_golden.rs.
# Covers plain ASCII, unicode (café/naïve/été), escaped JSON (quotes/backslash/
# \uXXXX), alias fields (id/name/categories/taxon), dead terms (12345/none/nan),
# equivalent identifiers, multiple names per row, an empty names array, a null
# preferred_name, a row with no names array, and a class row with no equivalents.
SYNONYMS: list[dict[str, Any]] = [
    synonym_row("HGNC:1", "Alpha Gene", ["Alpha Gene", "alpha"], "Gene"),
    synonym_row("HGNC:2", "café", ["café", "naïve"], "Gene"),
    synonym_row("HGNC:3", "Esc", ['"Quoted Name"', "back\\slash", "été"], "Gene"),
    {"id": "MONDO:1", "name": "Alias Disease", "names": ["alias disease"], "categories": ["biolink:Disease"], "taxon": ["NCBITaxon:0"]},
    synonym_row("HGNC:4", "Dead", ["12345", "none", "nan", "realname"], "Gene"),
    synonym_row("HGNC:5", "Empty Names", [], "Gene"),
    synonym_row("HGNC:6", "Shared Hit", ["shared"], "Gene"),
    synonym_row("MONDO:2", "Shared Disease", ["shared"], "Disease", taxon="NCBITaxon:0"),
    synonym_row("HGNC:7", None, ["nullname"], "Gene"),
    {"curie": "HGNC:8", "preferred_name": "No Names", "types": ["Gene"], "taxa": ["NCBITaxon:9606"]},
    synonym_row("HGNC:9", "Equiv Free", ["equivfree"], "Gene"),
    synonym_row("HGNC:10", "Multi A", ["multi", "alpha"], "Gene"),
    synonym_row("MONDO:3", "Multi B", ["multi"], "Disease", taxon="NCBITaxon:0"),
]

CLASSES: list[dict[str, Any]] = [class_row("HGNC:1", ["NCBIGene:100", "NCBIGene:101"]), class_row("MONDO:1", ["DOID:999"]), {"id": "HGNC:9"}]

# Every normalized term the fixture indexes (the l1/l2 forms), in sorted order.
# Matches the terms enumerated by the Rust golden dump.
#
# REGENERATE ONLY DELIBERATELY, after a reviewed change to the build's key
# derivation or to the fixture -- never to make a red build green.  US-003
# (`tablassert.fullmap.v6`: level-one keys via `nlp::normalize_l1`) renamed
# exactly 8 of these keys -- "alias disease" -> "alia diseas" (+ its level-two
# twin), "quoted name" -> "name quot" (+ twin), "equivfree" -> "equivfre",
# "nullname" -> "nullnam", "realname" -> "realnam", "shared" -> "share" -- with
# every CURIE attribution and the row count unchanged.  Each new key was
# re-derived independently from the RAW fixture spellings through
# `rs.normalize_terms` (the same call the Python query side makes) and
# cross-checked against the regenerated Rust golden, not copied from a build.
PROBES: list[str] = [
    "alia diseas",
    "aliadiseas",
    "alpha",
    "alpha gene",
    "alphagene",
    "back\\slash",
    "backslash",
    "caf",
    "café",
    "doid999",
    "doid:999",
    "equivfre",
    "hgnc1",
    "hgnc10",
    "hgnc2",
    "hgnc3",
    "hgnc4",
    "hgnc5",
    "hgnc6",
    "hgnc7",
    "hgnc8",
    "hgnc9",
    "hgnc:1",
    "hgnc:10",
    "hgnc:2",
    "hgnc:3",
    "hgnc:4",
    "hgnc:5",
    "hgnc:6",
    "hgnc:7",
    "hgnc:8",
    "hgnc:9",
    "mondo1",
    "mondo2",
    "mondo3",
    "mondo:1",
    "mondo:2",
    "mondo:3",
    "multi",
    "name quot",
    "namequot",
    "nave",
    "naïve",
    "ncbigene100",
    "ncbigene101",
    "ncbigene:100",
    "ncbigene:101",
    "nullnam",
    "realnam",
    "share",
    "t",
    "été",
]

# The pinned hydrated lookup output (SOURCE_VERSION asserted separately).
GOLDEN_ROWS: list[dict[str, object]] = [
    {"term": "alia diseas", "CURIE": "MONDO:1", "PREFERRED_NAME": "Alias Disease", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "aliadiseas", "CURIE": "MONDO:1", "PREFERRED_NAME": "Alias Disease", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "alpha", "CURIE": "HGNC:1", "PREFERRED_NAME": "Alpha Gene", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "alpha", "CURIE": "HGNC:10", "PREFERRED_NAME": "Multi A", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "alpha gene", "CURIE": "HGNC:1", "PREFERRED_NAME": "Alpha Gene", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "alphagene", "CURIE": "HGNC:1", "PREFERRED_NAME": "Alpha Gene", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "back\\slash", "CURIE": "HGNC:3", "PREFERRED_NAME": "Esc", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "backslash", "CURIE": "HGNC:3", "PREFERRED_NAME": "Esc", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "caf", "CURIE": "HGNC:2", "PREFERRED_NAME": "café", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "café", "CURIE": "HGNC:2", "PREFERRED_NAME": "café", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "doid999", "CURIE": "MONDO:1", "PREFERRED_NAME": "Alias Disease", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "doid:999", "CURIE": "MONDO:1", "PREFERRED_NAME": "Alias Disease", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "equivfre", "CURIE": "HGNC:9", "PREFERRED_NAME": "Equiv Free", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc1", "CURIE": "HGNC:1", "PREFERRED_NAME": "Alpha Gene", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc10", "CURIE": "HGNC:10", "PREFERRED_NAME": "Multi A", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc2", "CURIE": "HGNC:2", "PREFERRED_NAME": "café", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc3", "CURIE": "HGNC:3", "PREFERRED_NAME": "Esc", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc4", "CURIE": "HGNC:4", "PREFERRED_NAME": "Dead", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc5", "CURIE": "HGNC:5", "PREFERRED_NAME": "Empty Names", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc6", "CURIE": "HGNC:6", "PREFERRED_NAME": "Shared Hit", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc7", "CURIE": "HGNC:7", "PREFERRED_NAME": "HGNC:7", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc8", "CURIE": "HGNC:8", "PREFERRED_NAME": "No Names", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc9", "CURIE": "HGNC:9", "PREFERRED_NAME": "Equiv Free", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc:1", "CURIE": "HGNC:1", "PREFERRED_NAME": "Alpha Gene", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc:10", "CURIE": "HGNC:10", "PREFERRED_NAME": "Multi A", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc:2", "CURIE": "HGNC:2", "PREFERRED_NAME": "café", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc:3", "CURIE": "HGNC:3", "PREFERRED_NAME": "Esc", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc:4", "CURIE": "HGNC:4", "PREFERRED_NAME": "Dead", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc:5", "CURIE": "HGNC:5", "PREFERRED_NAME": "Empty Names", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc:6", "CURIE": "HGNC:6", "PREFERRED_NAME": "Shared Hit", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc:7", "CURIE": "HGNC:7", "PREFERRED_NAME": "HGNC:7", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc:8", "CURIE": "HGNC:8", "PREFERRED_NAME": "No Names", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "hgnc:9", "CURIE": "HGNC:9", "PREFERRED_NAME": "Equiv Free", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "mondo1", "CURIE": "MONDO:1", "PREFERRED_NAME": "Alias Disease", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "mondo2", "CURIE": "MONDO:2", "PREFERRED_NAME": "Shared Disease", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "mondo3", "CURIE": "MONDO:3", "PREFERRED_NAME": "Multi B", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "mondo:1", "CURIE": "MONDO:1", "PREFERRED_NAME": "Alias Disease", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "mondo:2", "CURIE": "MONDO:2", "PREFERRED_NAME": "Shared Disease", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "mondo:3", "CURIE": "MONDO:3", "PREFERRED_NAME": "Multi B", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "multi", "CURIE": "HGNC:10", "PREFERRED_NAME": "Multi A", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "multi", "CURIE": "MONDO:3", "PREFERRED_NAME": "Multi B", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "name quot", "CURIE": "HGNC:3", "PREFERRED_NAME": "Esc", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "namequot", "CURIE": "HGNC:3", "PREFERRED_NAME": "Esc", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "nave", "CURIE": "HGNC:2", "PREFERRED_NAME": "café", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "naïve", "CURIE": "HGNC:2", "PREFERRED_NAME": "café", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "ncbigene100", "CURIE": "HGNC:1", "PREFERRED_NAME": "Alpha Gene", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "ncbigene101", "CURIE": "HGNC:1", "PREFERRED_NAME": "Alpha Gene", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "ncbigene:100", "CURIE": "HGNC:1", "PREFERRED_NAME": "Alpha Gene", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "ncbigene:101", "CURIE": "HGNC:1", "PREFERRED_NAME": "Alpha Gene", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "nullnam", "CURIE": "HGNC:7", "PREFERRED_NAME": "HGNC:7", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "realnam", "CURIE": "HGNC:4", "PREFERRED_NAME": "Dead", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "share", "CURIE": "HGNC:6", "PREFERRED_NAME": "Shared Hit", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "share", "CURIE": "MONDO:2", "PREFERRED_NAME": "Shared Disease", "CATEGORY_NAME": "Disease", "TAXON_ID": 0, "SOURCE_NAME": "SRC"},
    {"term": "t", "CURIE": "HGNC:3", "PREFERRED_NAME": "Esc", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
    {"term": "été", "CURIE": "HGNC:3", "PREFERRED_NAME": "Esc", "CATEGORY_NAME": "Gene", "TAXON_ID": 9606, "SOURCE_NAME": "SRC"},
]

EXPECTED_CURIES: list[str] = [
    "HGNC:1",
    "HGNC:10",
    "HGNC:2",
    "HGNC:3",
    "HGNC:4",
    "HGNC:5",
    "HGNC:6",
    "HGNC:7",
    "HGNC:8",
    "HGNC:9",
    "MONDO:1",
    "MONDO:2",
    "MONDO:3",
]


def _canonical(rows: list[dict[str, Any]]) -> list[dict[str, object]]:
    """Reduce hydrated lookup rows to the golden shape, sorted by (term, CURIE)."""
    return sorted(
        (
            {
                "term": r["term"],
                "CURIE": r["CURIE"],
                "PREFERRED_NAME": r["PREFERRED_NAME"],
                "CATEGORY_NAME": r["CATEGORY_NAME"],
                "TAXON_ID": r["TAXON_ID"],
                "SOURCE_NAME": r["SOURCE_NAME"],
            }
            for r in rows
        ),
        key=lambda r: (str(r["term"]), str(r["CURIE"])),
    )


def _expected_term_curies() -> dict[str, list[str]]:
    """Derive the golden term -> sorted(CURIE strings) map from GOLDEN_ROWS."""
    out: dict[str, set[str]] = {}
    for row in GOLDEN_ROWS:
        out.setdefault(str(row["term"]), set()).add(str(row["CURIE"]))
    return {term: sorted(curies) for term, curies in out.items()}


@pytest.fixture
def golden_db(tmp_path: Path) -> Path:
    classes: Path = write_jsonl(tmp_path / "classes.ndjson", CLASSES)
    synonyms: Path = write_jsonl(tmp_path / "SRC.ndjson", SYNONYMS)
    output: Path = tmp_path / "fullmap.redb"
    rs.build_fullmap_db(output, [classes], [synonyms])
    return output


def test_golden_lookup_is_pinned(golden_db: Path) -> None:
    """Looking up every indexed term yields exactly the pinned hydrated rows."""
    rows: list[dict[str, Any]] = rs.lookup_fullmap_terms(golden_db, PROBES, return_format="rows")

    # SOURCE_VERSION is a compile-time constant on every row (asserted once here,
    # not repeated inline in the golden).
    source_version: str = rs.fullmap_source_version()
    assert all(r["SOURCE_VERSION"] == source_version for r in rows)

    canon: list[dict[str, object]] = _canonical(rows)
    assert len(canon) == len(GOLDEN_ROWS) == 55
    assert len({r["term"] for r in canon}) == len(PROBES) == 52  # every probe resolved
    assert canon == GOLDEN_ROWS, "fullmap lookup output diverged from the pinned golden"


def test_pairs_format_matches_golden_curies(golden_db: Path) -> None:
    """return_format='pairs' yields (curie_id, source_id) records whose hydrated
    CURIE strings match the golden term -> CURIE mapping (raw curie_ids are
    scheduling-dependent and deliberately not pinned)."""
    pair_rows: list[dict[str, Any]] = rs.lookup_fullmap_terms(golden_db, PROBES, return_format="pairs")
    prefixes: list[str] = list(rs.hydrate_prefixes(golden_db))
    expected: dict[str, list[str]] = _expected_term_curies()

    assert len(pair_rows) == len(PROBES)
    for row in pair_rows:
        term: str = str(row["term"])
        records: list[tuple[int, int]] = [(int(a), int(b)) for a, b in row["records"]]
        assert records, f"term {term} has no records"
        # Single synonym source -> every record's source_id is 0.
        assert all(source_id == 0 for _curie_id, source_id in records)
        curie_ids: list[int] = [curie_id for curie_id, _source_id in records]
        hydrated: list[dict[str, Any]] = rs.hydrate_curies(golden_db, curie_ids)
        curie_strings: list[str] = sorted(f"{prefixes[int(h['prefix_id'])]}:{h['local_id']}" for h in hydrated)
        assert curie_strings == expected[term], f"term {term} CURIE set diverged"


def test_hydration_round_trip_is_consistent(golden_db: Path) -> None:
    """Every curie_id from the pairs hydrates to a complete, consistent CURIE row."""
    pair_rows: list[dict[str, Any]] = rs.lookup_fullmap_terms(golden_db, PROBES, return_format="pairs")
    curie_ids: list[int] = sorted({int(a) for row in pair_rows for a, _b in row["records"]})

    prefixes: list[str] = list(rs.hydrate_prefixes(golden_db))
    categories: list[str] = list(rs.hydrate_categories(golden_db))
    sources: list[str] = list(rs.hydrate_sources(golden_db))
    assert sources == ["SRC"]
    assert sorted(prefixes) == ["HGNC", "MONDO"]
    assert sorted(categories) == ["Disease", "Gene"]

    hydrated: list[dict[str, Any]] = rs.hydrate_curies(golden_db, curie_ids)
    assert len(hydrated) == len(curie_ids) == 13  # one row per unique CURIE
    reconstructed: list[str] = []
    for row in hydrated:
        # All five dimension fields are present.
        assert set(row) == {"prefix_id", "local_id", "preferred_name", "category_id", "taxon_id"}
        prefix_id: int = int(row["prefix_id"])
        category_id: int = int(row["category_id"])
        # prefix_id / category_id index valid dimension entries.
        assert 0 <= prefix_id < len(prefixes)
        assert 0 <= category_id < len(categories)
        assert isinstance(row["taxon_id"], int)
        assert row["preferred_name"]  # non-empty
        reconstructed.append(f"{prefixes[prefix_id]}:{row['local_id']}")

    assert sorted(reconstructed) == EXPECTED_CURIES


def test_resolve_batch_matches_sequential_resolve(golden_db: Path) -> None:
    """resolve_batch over multiple specs produces the same frame as sequential resolve."""
    lf: pl.LazyFrame = pl.DataFrame(
        {
            "subject": ["alpha gene", "not-a-real-term"],
            "subject_two": ["alpha gene", "not-a-real-term"],
            "object": ["equivfre", "equivfre"],
            "object_two": ["equivfre", "equivfre"],
        }
    ).lazy()

    sequential: pl.DataFrame = resolve(resolve(lf, "subject", golden_db, log=False), "object", golden_db, log=False).collect()
    batched: pl.DataFrame = resolve_batch(lf, [ResolveSpec("subject"), ResolveSpec("object")], golden_db, log=False).collect()

    cols: list[str] = sorted(sequential.columns)
    assert cols == sorted(batched.columns)
    assert sequential.select(cols).sort(cols).to_dicts() == batched.select(cols).sort(cols).to_dicts()
    # The resolved CURIEs are the expected entities.
    result: dict[str, Any] = batched.select(cols).sort(cols).to_dicts()[0]
    assert result["subject"] == "HGNC:1"
    assert result["object"] == "HGNC:9"


def test_rebuild_stability(tmp_path: Path) -> None:
    """Building twice at the same path yields identical lookups after rebuild."""
    classes: Path = write_jsonl(tmp_path / "classes.ndjson", CLASSES)
    synonyms: Path = write_jsonl(tmp_path / "SRC.ndjson", SYNONYMS)
    output: Path = tmp_path / "fullmap.redb"

    rs.build_fullmap_db(output, [classes], [synonyms])
    first: list[dict[str, object]] = _canonical(rs.lookup_fullmap_terms(output, PROBES, return_format="rows"))
    assert first == GOLDEN_ROWS

    # Rebuild identical content at the SAME path; bump mtime so the Python-side
    # term/dimension caches (keyed on path+mtime) invalidate deterministically.
    rs.build_fullmap_db(output, [classes], [synonyms])
    bumped: float = output.stat().st_mtime + 10.0
    os.utime(output, (bumped, bumped))

    second: list[dict[str, object]] = _canonical(rs.lookup_fullmap_terms(output, PROBES, return_format="rows"))
    assert second == GOLDEN_ROWS
    assert first == second
