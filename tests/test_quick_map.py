"""Behavior tests for ``tablassert.fullmap.quick_map`` and the ``tablassert quick-map`` command.

The redb-backed tests build a tiny REAL fullmap database via ``rs.build_fullmap_db`` (the Rust
extension), mirroring the fixtures in ``tests/test_cover_fullmap.py`` and
``tests/test_fullmap.py``. All tests are offline and use ``tmp_path``.

Covered targets:
- exact-name hit ranks first (lowest ``PR``)
- level-two fallback hit (level-one key misses, stripped level-two key hits)
- taxon filtering, including a ``TAXON_ID`` 0 row surviving the filter
- ``prioritize`` boosting ``PR``; ``avoid`` dropping a whole category
- ``exclude_prefixes`` / ``exclude_regex`` dropping a CURIE
- a miss mapping to the canonical empty frame; junk numeric terms never probed
- multi-term input order, shared normalized keys, and exactly ONE ``lookup_rows`` round trip
- CLI rendering (tables, ``no matches`` lines), directory resolution, and every exit-2 path
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

import tablassert.fullmap as fullmap_module
from tablassert import rs
from tablassert.biolink import Categories
from tablassert.cli import quick_map_command
from tablassert.fullmap import empty_matches, quick_map


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    return path


def synonym_row(curie: str, preferred_name: str, names: list[str], category: str, taxa: list[str]) -> dict[str, Any]:
    return {"curie": curie, "preferred_name": preferred_name, "names": names, "types": [category], "taxa": taxa}


def class_row(curie: str) -> dict[str, Any]:
    return {"id": curie, "equivalent_identifiers": []}


@pytest.fixture
def fullmap_db(tmp_path: Path) -> Path:
    """A tiny real fullmap redb spanning two prefixes, two categories, and two taxa.

    ``breast cancer 1`` resolves to BOTH ``HGNC:1100`` and ``OMIM:600185`` (the exclusion
    tests' two-candidate case); ``MONDO:0005148`` carries ``TAXON_ID`` 0 (the taxon-filter
    survival case); ``HGNC:6666`` is mouse-only (the taxon-filter drop case).
    """
    rows: list[dict[str, Any]] = [
        synonym_row("HGNC:1100", "BRCA1", ["BRCA1", "breast cancer 1"], "Gene", ["NCBITaxon:9606"]),
        synonym_row("OMIM:600185", "breast cancer 1", ["breast cancer 1"], "Gene", ["NCBITaxon:9606"]),
        synonym_row("MONDO:0005148", "type 2 diabetes mellitus", ["type 2 diabetes mellitus", "T2DM"], "Disease", []),
        synonym_row("HGNC:6666", "mousegene", ["mousegene"], "Gene", ["NCBITaxon:10090"]),
    ]
    classes: Path = write_jsonl(tmp_path / "classes.ndjson", [class_row(row["curie"]) for row in rows])
    synonyms: Path = write_jsonl(tmp_path / "synonyms.ndjson", rows)
    output: Path = tmp_path / "data" / "fullmap.redb"
    rs.build_fullmap_db(output, [classes], [synonyms])
    return output


def test_exact_name_hit_ranks_first(fullmap_db: Path) -> None:
    """An exact preferred-name hit resolves at the lowest PR with NLP level one."""
    out: dict[str, pl.DataFrame] = quick_map(["BRCA1"], fullmap_db)
    assert list(out) == ["BRCA1"]
    frame: pl.DataFrame = out["BRCA1"]
    assert frame.height == 1
    row: dict[str, Any] = frame.row(0, named=True)
    assert row["CURIE"] == "HGNC:1100"
    assert row["NLP_LEVEL"] == 1
    assert row["PR"] == 250  # priority 50 x level-one normalized-name match 5


def test_level_two_fallback_hit(fullmap_db: Path) -> None:
    """A term whose level-one key misses still resolves through its stripped level-two key.

    ``Breast-Cancer 1`` normalizes to level-one ``1 breast-cancer`` (never indexed) and
    level-two ``1breastcancer`` (the indexed key for the ``breast cancer 1`` name).
    """
    frame: pl.DataFrame = quick_map(["Breast-Cancer 1"], fullmap_db)["Breast-Cancer 1"]
    assert frame.height > 0
    assert set(frame.get_column("NLP_LEVEL").to_list()) == {2}
    assert "HGNC:1100" in frame.get_column("CURIE").to_list()


def test_taxon_filter_drops_mismatches_and_keeps_taxon_zero(fullmap_db: Path) -> None:
    """A taxon filter drops foreign-taxon rows but retains TAXON_ID 0 rows, like a build."""
    unrestricted: pl.DataFrame = quick_map(["mousegene"], fullmap_db, taxon=None)["mousegene"]
    assert set(unrestricted.get_column("TAXON_ID").to_list()) == {10090}
    filtered: pl.DataFrame = quick_map(["mousegene"], fullmap_db, taxon="9606")["mousegene"]
    assert filtered.height == 0
    rematched: pl.DataFrame = quick_map(["mousegene"], fullmap_db, taxon="10090")["mousegene"]
    assert set(rematched.get_column("TAXON_ID").to_list()) == {10090}
    taxonless: pl.DataFrame = quick_map(["T2DM"], fullmap_db, taxon="9606")["T2DM"]
    assert set(taxonless.get_column("TAXON_ID").to_list()) == {0}


def test_prioritize_boosts_pr_and_avoid_drops_a_category(fullmap_db: Path) -> None:
    """``prioritize`` swaps the 50 priority multiplier for 1; ``avoid`` drops the whole category."""
    default: pl.DataFrame = quick_map(["T2DM"], fullmap_db)["T2DM"]
    assert default.get_column("PR").to_list() == [500]  # priority 50 x synonym-only pr_base 10
    prioritized: pl.DataFrame = quick_map(["T2DM"], fullmap_db, prioritize=[Categories("Disease")])["T2DM"]
    assert prioritized.get_column("PR").to_list() == [10]
    avoided: pl.DataFrame = quick_map(["T2DM"], fullmap_db, avoid=[Categories("Disease")])["T2DM"]
    assert avoided.height == 0


def test_exclude_prefixes_and_exclude_regex_drop_a_curie(fullmap_db: Path) -> None:
    """Both exclusion flags narrow a two-candidate term to the surviving CURIE.

    A term probing both its level-one and level-two keys keeps one row PER key that hit
    (the ranked frame is unique by ``(term, CURIE)``, and the two keys differ), so the
    assertions compare CURIE sets, not ordered lists.
    """
    both: pl.DataFrame = quick_map(["breast cancer 1"], fullmap_db)["breast cancer 1"]
    assert set(both.get_column("CURIE").to_list()) == {"HGNC:1100", "OMIM:600185"}
    by_prefix: pl.DataFrame = quick_map(["breast cancer 1"], fullmap_db, exclude_prefixes=["OMIM"])["breast cancer 1"]
    assert set(by_prefix.get_column("CURIE").to_list()) == {"HGNC:1100"}
    by_regex: pl.DataFrame = quick_map(["breast cancer 1"], fullmap_db, exclude_regex=["^OMIM:"])["breast cancer 1"]
    assert set(by_regex.get_column("CURIE").to_list()) == {"HGNC:1100"}


def test_miss_maps_to_canonical_empty_frame_and_empty_terms_short_circuit(fullmap_db: Path) -> None:
    """A miss returns ``empty_matches(False)`` (never a missing key); no terms returns ``{}``."""
    frame: pl.DataFrame = quick_map(["nonsense"], fullmap_db)["nonsense"]
    assert frame.height == 0
    assert frame.columns == empty_matches(False).columns
    assert quick_map([], fullmap_db) == {}


def test_junk_numeric_term_is_never_probed(fullmap_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A purely numeric term is dropped by ``distinct``'s junk filter, exactly as in a build."""
    probed: list[str] = []
    real = fullmap_module.lookup_rows

    def spy(db: Path, terms: list[str]) -> list[dict[str, object]]:
        probed.extend(terms)
        return real(db, terms)

    monkeypatch.setattr(fullmap_module, "lookup_rows", spy)
    frame: pl.DataFrame = quick_map(["2"], fullmap_db)["2"]
    assert frame.height == 0
    assert "2" not in probed


def test_multi_term_input_order_and_shared_normalized_keys(fullmap_db: Path) -> None:
    """Results follow input order; inputs sharing a normalized key both report it."""
    out: dict[str, pl.DataFrame] = quick_map(["T2DM", "BRCA1", "nonsense", "brca1"], fullmap_db)
    assert list(out) == ["T2DM", "BRCA1", "nonsense", "brca1"]
    assert out["T2DM"].height == 1
    assert out["nonsense"].height == 0
    # Different raw spellings, one shared level-one key: both entries carry the same pick.
    assert out["BRCA1"].get_column("CURIE").to_list() == out["brca1"].get_column("CURIE").to_list() == ["HGNC:1100"]


def test_multi_term_batch_issues_one_lookup_round_trip(fullmap_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The whole input is ONE ``lookup_rows`` batch, never a per-term redb round trip."""
    calls: list[int] = []
    real = fullmap_module.lookup_rows

    def spy(db: Path, terms: list[str]) -> list[dict[str, object]]:
        calls.append(len(terms))
        return real(db, terms)

    monkeypatch.setattr(fullmap_module, "lookup_rows", spy)
    quick_map(["BRCA1", "T2DM", "mousegene", "Breast-Cancer 1"], fullmap_db, taxon=None)
    # 5 distinct probe keys ("Breast-Cancer 1" contributes a fresh level-one key plus the
    # shared stripped level-two key), fetched in exactly one batched call.
    assert calls == [5]


def test_cli_renders_tables_and_no_matches_lines(fullmap_db: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The command renders one table per hit, a ``no matches`` line per miss, and exits clean."""
    quick_map_command(["BRCA1", "nonsense"], fullmap=tmp_path)  # directory form resolves to the redb
    out: str = capsys.readouterr().out
    assert "HGNC:1100" in out
    assert "no matches" in out
    assert out.index("HGNC:1100") < out.index("no matches")  # input order


def test_cli_taxon_zero_disables_the_filter(fullmap_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """``--taxon 0`` behaves like ``taxon: null``: the mouse-only row is shown."""
    quick_map_command(["mousegene"], fullmap=fullmap_db, taxon=0)
    assert "HGNC:6666" in capsys.readouterr().out


def test_cli_bad_category_exits_2(fullmap_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """An unknown category exits 2 naming the value and the nearest valid names."""
    with pytest.raises(SystemExit) as exc_info:
        quick_map_command(["BRCA1"], fullmap=fullmap_db, prioritize=["Gnee"])
    assert exc_info.value.code == 2
    err: str = capsys.readouterr().err
    assert "'Gnee'" in err
    assert "Gene" in err
    assert "not a known Biolink entity category" in err


def test_cli_bad_regex_exits_2(fullmap_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A non-polars regex and an empty pattern both exit 2 with the validator's wording."""
    with pytest.raises(SystemExit) as exc_info:
        quick_map_command(["BRCA1"], fullmap=fullmap_db, exclude_regex=["("])
    assert exc_info.value.code == 2
    assert "polars-compatible" in capsys.readouterr().err
    with pytest.raises(SystemExit) as exc_info:
        quick_map_command(["BRCA1"], fullmap=fullmap_db, exclude_regex=[""])
    assert exc_info.value.code == 2
    assert "non-empty patterns" in capsys.readouterr().err


def test_cli_missing_database_exits_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A fullmap path that resolves to no redb exits 2 pointing at build-fullmap."""
    with pytest.raises(SystemExit) as exc_info:
        quick_map_command(["BRCA1"], fullmap=tmp_path / "nope")
    assert exc_info.value.code == 2
    err: str = capsys.readouterr().err
    assert "no fullmap database" in err
    assert "build-fullmap" in err
