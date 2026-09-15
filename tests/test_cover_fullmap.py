"""Targeted coverage tests for the uncovered branches in ``tablassert/fullmap.py``.

Each test executes a specific previously-uncovered line (or branch) and documents
which line(s) it covers and why. The redb-backed tests build a tiny REAL fullmap
database via ``rs.build_fullmap_db`` (the Rust extension), mirroring the fixtures
in ``tests/test_fullmap.py`` and ``tests/test_fullmap_golden.py``. All tests are
offline and use ``tmp_path``.

Covered targets:
- line 79:  ``_remember_term`` FIFO eviction (``_TERM_CACHE.popitem(last=False)``)
- line 119: ``lookup_rows`` empty-terms short-circuit (``return []``)
- lines 347-354: ``log_unmatched`` anti-join + per-term logging loop
- line 485: ``resolve_batch`` no-specs short-circuit (``return lf``)
- line 504: ``resolve_batch`` calling ``log_unmatched`` when ``log=True``
"""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

import polars as pl
import pytest

import tablassert.fullmap as fullmap
from tablassert import rs
from tablassert.fullmap import _remember_term, log_unmatched, lookup_rows, resolve, resolve_batch


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    return path


def synonym_row(curie: str, preferred_name: str, names: list[str], category: str, taxon: str = "NCBITaxon:9606") -> dict[str, Any]:
    return {"curie": curie, "preferred_name": preferred_name, "names": names, "types": [category], "taxa": [taxon]}


def class_row(curie: str, equivalents: list[str]) -> dict[str, Any]:
    return {"id": curie, "equivalent_identifiers": [{"identifier": x} for x in equivalents]}


class CapturingLogger:
    """Minimal stand-in for the module ``logger`` that records ``info`` calls."""

    def __init__(self) -> None:
        self.infos: list[dict[str, Any]] = []

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.infos.append(kwargs)


@pytest.fixture
def fullmap_db(tmp_path: Path) -> Path:
    """A tiny real fullmap redb where ``brca1`` resolves and unknown terms do not."""
    classes: Path = write_jsonl(tmp_path / "classes.ndjson", [class_row("HGNC:1100", ["NCBIGene:672"])])
    synonyms: Path = write_jsonl(tmp_path / "HGNC.ndjson", [synonym_row("HGNC:1100", "BRCA1", ["BRCA1", "breast cancer 1"], "Gene")])
    output: Path = tmp_path / "data" / "fullmap.redb"
    rs.build_fullmap_db(output, [classes], [synonyms])
    return output


def test_taxon_allowlist_and_inchikey_paths(tmp_path: Path) -> None:
    """The opt-in builder filters taxon-bearing rows but keeps organism/taxonless rows and InChIKeys."""
    synonyms = write_jsonl(
        tmp_path / "SRC.ndjson",
        [
            {"curie": "HGNC:1", "preferred_name": "human", "names": ["human gene"], "types": ["Gene"], "taxa": ["NCBITaxon:9606"]},
            {"curie": "HGNC:2", "preferred_name": "mouse", "names": ["mouse gene"], "types": ["Gene"], "taxa": ["NCBITaxon:10090"]},
            {"curie": "HGNC:3", "preferred_name": "other", "names": ["other gene"], "types": ["Gene"], "taxa": ["NCBITaxon:999999"]},
            {
                "curie": "NCBITaxon:999999",
                "preferred_name": "taxon",
                "names": ["other taxon"],
                "types": ["OrganismTaxon"],
                "taxa": ["NCBITaxon:999999"],
            },
            {
                "curie": "NCBITaxon:888888",
                "preferred_name": "multi-category taxon",
                "names": ["multi-category taxon"],
                "types": ["NamedThing", "biolink:OrganismTaxon"],
                "taxa": ["NCBITaxon:888888"],
            },
            {"curie": "CHEBI:1", "preferred_name": "untaxed", "names": ["untaxed chemical"], "types": ["ChemicalEntity"], "taxa": []},
            {"curie": "CHEBI:2", "preferred_name": "sentinel", "names": ["sentinel chemical"], "types": ["ChemicalEntity"], "taxa": ["NCBITaxon:0"]},
            {
                "curie": "CHEBI:3",
                "preferred_name": "malformed",
                "names": ["malformed chemical"],
                "types": ["ChemicalEntity"],
                "taxa": ["not-a-taxon"],
            },
            {
                "curie": "HGNC:4",
                "preferred_name": "multi",
                "names": ["multi gene"],
                "types": ["Gene"],
                "taxa": ["NCBITaxon:999999", "NCBITaxon:9606"],
            },
            {
                "curie": "HGNC:5",
                "preferred_name": "case-insensitive",
                "names": ["case-insensitive gene"],
                "types": ["Gene"],
                "taxa": ["ncbitaxon:9606"],
            },
            {"curie": "INCHIKEY:ABC-DEF", "preferred_name": "inchi", "names": ["ABC-DEF"], "types": ["SmallMolecule"], "taxa": ["NCBITaxon:9606"]},
        ],
    )
    output = tmp_path / "fullmap.redb"
    rs.build_fullmap_db(output, [], [synonyms], taxon_allowlist=[9606])
    # Probes are the KEYS the build stores, i.e. the `nlp::normalize_l1` forms of
    # the synonym `names` above (US-003): a lookup is an exact key match, so the
    # raw spellings no longer resolve.  Derived with the real normalizer
    # (`rs.normalize_terms`), not hand-stemmed: "human gene" -> "gene human",
    # "mouse gene" -> "gene mous", "other gene" -> "gene other",
    # "untaxed chemical" -> "chemic untax", "sentinel chemical" ->
    # "chemic sentinel", "malformed chemical" -> "chemic malform",
    # "multi gene" -> "gene multi"; "other taxon", "multi-category taxon",
    # "case-insensitive gene", "abc-def" and "inchikey:abc-def" are unchanged
    # (hyphenated / digit-bearing tokens are never stemmed).
    assert rs.lookup_fullmap_terms(output, ["gene human"])[0]["CURIE"] == "HGNC:1"
    assert rs.lookup_fullmap_terms(output, ["gene mous"]) == []
    assert rs.lookup_fullmap_terms(output, ["gene other"]) == []
    assert rs.lookup_fullmap_terms(output, ["other taxon"])[0]["CURIE"] == "NCBITaxon:999999"
    assert rs.lookup_fullmap_terms(output, ["multi-category taxon"])[0]["CURIE"] == "NCBITaxon:888888"
    assert rs.lookup_fullmap_terms(output, ["chemic untax"])[0]["CURIE"] == "CHEBI:1"
    assert rs.lookup_fullmap_terms(output, ["chemic sentinel"])[0]["CURIE"] == "CHEBI:2"
    assert rs.lookup_fullmap_terms(output, ["chemic malform"])[0]["CURIE"] == "CHEBI:3"
    assert rs.lookup_fullmap_terms(output, ["gene multi"])[0]["CURIE"] == "HGNC:4"
    assert rs.lookup_fullmap_terms(output, ["case-insensitive gene"])[0]["CURIE"] == "HGNC:5"
    assert rs.lookup_fullmap_terms(output, ["abc-def"])[0]["CURIE"] == "INCHIKEY:ABC-DEF"
    assert rs.lookup_fullmap_terms(output, ["inchikey:abc-def"])[0]["CURIE"] == "INCHIKEY:ABC-DEF"


def test_lookup_rows_empty_terms_returns_empty(tmp_path: Path) -> None:
    """Line 119: ``lookup_rows`` short-circuits to ``[]`` for an empty term list.

    The ``if not terms: return []`` guard fires before any database access, so a
    nonexistent path is fine and the result must be an empty list.
    """
    assert lookup_rows(tmp_path / "missing.redb", []) == []


def test_remember_term_evicts_oldest_when_over_max(monkeypatch: pytest.MonkeyPatch) -> None:
    """Line 79: ``_remember_term`` evicts the oldest FIFO entry past ``_TERM_CACHE_MAX``.

    The ``while len(_TERM_CACHE) > _TERM_CACHE_MAX: _TERM_CACHE.popitem(last=False)``
    loop only runs once the bounded cache overflows. Swap in a fresh cache and a tiny
    max (2), insert three distinct keys, and assert the oldest was evicted FIFO while
    the two newest survive.
    """
    cache: OrderedDict[tuple[Path, float, str], list[tuple[int, int]] | None] = OrderedDict()
    monkeypatch.setattr(fullmap, "_TERM_CACHE", cache)
    monkeypatch.setattr(fullmap, "_TERM_CACHE_MAX", 2)

    _remember_term((Path("/db"), 1.0, "t1"), [(1, 0)])
    _remember_term((Path("/db"), 1.0, "t2"), [(2, 0)])
    _remember_term((Path("/db"), 1.0, "t3"), [(3, 0)])  # size 3 > max 2 -> evict oldest

    assert len(cache) == 2
    assert (Path("/db"), 1.0, "t1") not in cache
    assert (Path("/db"), 1.0, "t2") in cache
    assert (Path("/db"), 1.0, "t3") in cache


def test_log_unmatched_logs_unresolved_level_one_terms(monkeypatch: pytest.MonkeyPatch) -> None:
    """Lines 347-354: ``log_unmatched`` anti-joins level-one terms and logs each miss.

    Builds the level-one subset (line 347), anti-joins against resolved terms (348),
    collects (351), enters the ``height > 0`` branch (352) and logs every unresolved
    term (353-354). A level-two-only unmatched term must NOT be logged (the level-one
    filter at 347 excludes it), and a resolved level-one term must NOT be logged.
    """
    cap = CapturingLogger()
    monkeypatch.setattr(fullmap, "logger", cap)

    terms: pl.LazyFrame = pl.DataFrame({"term": ["brca1", "zzz-not-real", "l2-only"], "nlp_level": [1, 1, 2]}).lazy()
    matches: pl.DataFrame = pl.DataFrame({"term": ["brca1"]})

    log_unmatched("subject", terms, matches, "hash123", "config.yaml")

    assert [info["term"] for info in cap.infos] == ["zzz-not-real"]
    assert cap.infos[0]["col"] == "subject"
    assert cap.infos[0]["config"] == "config.yaml"
    assert cap.infos[0]["hash"] == "hash123"


def test_log_unmatched_no_log_when_all_level_one_matched(monkeypatch: pytest.MonkeyPatch) -> None:
    """Line 352 false branch: no logging when every level-one term already resolved.

    The anti-join yields zero rows, so ``unnmatched.height > 0`` is false and the
    logging loop body (353-354) is skipped.
    """
    cap = CapturingLogger()
    monkeypatch.setattr(fullmap, "logger", cap)

    terms: pl.LazyFrame = pl.DataFrame({"term": ["brca1"], "nlp_level": [1]}).lazy()
    matches: pl.DataFrame = pl.DataFrame({"term": ["brca1"]})

    log_unmatched("subject", terms, matches, None, None)

    assert cap.infos == []


def test_resolve_batch_no_specs_returns_input(tmp_path: Path) -> None:
    """Line 485: ``resolve_batch`` returns the input LazyFrame unchanged for no specs.

    The ``if not specs: return lf`` guard returns the very same object before any
    term extraction or database access (so a nonexistent path is fine).
    """
    lf: pl.LazyFrame = pl.DataFrame({"subject": ["brca1"], "subject_two": ["brca1"]}).lazy()

    result: pl.LazyFrame = resolve_batch(lf, [], tmp_path / "missing.redb")

    assert result is lf


def test_resolve_log_true_logs_unmatched(fullmap_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Line 504: ``resolve_batch`` calls ``log_unmatched`` when ``log=True``.

    Every other fullmap test passes ``log=False``, so the ``if log: log_unmatched(...)``
    call site was never hit. Resolve a column with one resolvable term (``brca1``) and
    one unknown term (``zzz-not-real``) with ``log=True``: the unknown level-one term is
    dropped from the output yet logged through the real integration path (also exercising
    lines 347-354 end to end).
    """
    cap = CapturingLogger()
    monkeypatch.setattr(fullmap, "logger", cap)

    source: pl.DataFrame = pl.DataFrame({"subject": ["brca1", "zzz-not-real"], "subject_two": ["brca1", "zzz-not-real"]})

    result: pl.DataFrame = resolve(source.lazy(), "subject", fullmap_db, log=True, section_hash="abc", config_file="cfg.yaml").collect()

    assert result.height == 1
    assert result["subject"].to_list() == ["HGNC:1100"]
    assert [info["term"] for info in cap.infos] == ["zzz-not-real"]
    assert cap.infos[0]["config"] == "cfg.yaml"
    assert cap.infos[0]["hash"] == "abc"
