"""Targeted coverage for the UNCOVERED lines in ``tablassert.agent`` core helpers.

Each test names the exact source line(s) it exercises in its docstring. Everything
here runs OFFLINE with NO ``[agent]`` extra: the JATS parsers, ``is_open_access``,
``read_table`` and ``_count_ndjson_lines`` are plain base-env functions, and the
``build_and_audit`` branch tests drive the REAL validate/build pipelines against a
tiny REAL redb (the offline recipe from ``tests/test_agent_build.py``), faking ONLY
the documented seams (``tablassert.cli.validate_pipeline`` for the pydantic branch,
``tablassert.agent.map_coverage`` for the two non-fatal coverage-note branches).

Target lines (``src/tablassert/agent.py``): 233, 277, 309, 329, 501-503, 939, 1015,
1034, 1037-1038.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import yaml

from tablassert import rs
from tablassert.agent import (
    _count_ndjson_lines,
    _load_table,
    build_and_audit,
    is_open_access,
    parse_jats_summary,
    read_table,
    supplementary_materials_from_jats,
)

# --------------------------------------------------------------------------- #
# JATS parsers: _nearest_label/_nearest_caption (233) + supplementary (277)
# --------------------------------------------------------------------------- #

# Three supplementary materials exercising every label/caption/href branch:
#   #1 media WITH xlink:href but NO label/caption -> _nearest_label returns None (233)
#   #2 label + caption + media xlink:href        -> label/caption found + append
#   #3 media with NO href at all                 -> the `if not href: continue` (277)
SUPP_XML: str = (
    '<article xmlns:xlink="http://www.w3.org/1999/xlink">'
    "<back>"
    '<supplementary-material><media xlink:href="table_s1.xlsx"/></supplementary-material>'
    "<supplementary-material>"
    "<label>Table S2</label>"
    "<caption><p>Extra  data   caption</p></caption>"
    '<media xlink:href="table_s2.xlsx"/>'
    "</supplementary-material>"
    "<supplementary-material><media/></supplementary-material>"
    "</back>"
    "</article>"
)


def test_supplementary_materials_label_caption_and_no_href_branches() -> None:
    """Cover line 233 (``_nearest_label`` -> ``None``) and line 277 (media without href -> ``continue``).

    Material #1 has no ``<label>``, so ``_nearest_label`` falls through to ``return None``
    (233); material #3's ``<media>`` carries neither ``xlink:href`` nor ``href``, so the
    ``if not href: continue`` branch (277) skips it (only two entries are emitted). The
    label/caption present on material #2 also exercises the found-label/found-caption paths.
    """
    materials: list[dict[str, object]] = supplementary_materials_from_jats(SUPP_XML)

    assert len(materials) == 2  # material #3 (no href) is skipped via line 277
    first, second = materials
    assert first == {"href": "table_s1.xlsx", "label": None, "caption": None, "is_table": True}  # label None via 233
    assert second == {"href": "table_s2.xlsx", "label": "Table S2", "caption": "Extra data caption", "is_table": True}


def test_supplementary_materials_malformed_xml_returns_empty() -> None:
    """Cover the ``except ET.ParseError: return []`` guard of ``supplementary_materials_from_jats``.

    Malformed XML must yield ``[]`` (never raise); a natural edge companion to the branch test above.
    """
    assert supplementary_materials_from_jats("<<not xml") == []


# --------------------------------------------------------------------------- #
# parse_jats_summary: max_sections break (309) + journal (299)
# --------------------------------------------------------------------------- #

SUMMARY_XML: str = (
    "<article>"
    "<front><article-meta>"
    "<journal-title>Journal of Testing</journal-title>"
    "<title-group><article-title>My Title</article-title></title-group>"
    "<abstract>ABSTRACT This is the abstract.</abstract>"
    "</article-meta></front>"
    "<body>"
    "<sec><title>Intro</title></sec>"
    "<sec><title>Methods</title></sec>"
    "<sec><title>Results</title></sec>"
    "</body>"
    "</article>"
)


def test_parse_jats_summary_max_sections_break_and_journal() -> None:
    """Cover line 309 (the ``len(sections) >= max_sections`` ``break``) plus the journal/title/abstract paths.

    With ``max_sections=2`` and three body ``<title>`` headings, the loop appends ``Intro``
    and ``Methods`` then hits the ``break`` (309) before ``Results``; the ``<journal-title>``
    also exercises the journal assignment and the glued ``ABSTRACT`` heading is stripped.
    """
    info: dict[str, object] = parse_jats_summary(SUMMARY_XML, max_sections=2)

    assert info["title"] == "My Title"
    assert info["journal"] == "Journal of Testing"
    assert info["abstract"] == "This is the abstract."
    assert info["sections"] == ["Intro", "Methods"]  # Results dropped by the break (309)


def test_parse_jats_summary_malformed_xml_returns_empties() -> None:
    """Cover the ``except ET.ParseError`` empty-result guard of ``parse_jats_summary`` (never raises)."""
    assert parse_jats_summary("<<not xml") == {"title": "", "journal": "", "abstract": "", "sections": []}


# --------------------------------------------------------------------------- #
# is_open_access: non-dict, non-str input -> return False (329)
# --------------------------------------------------------------------------- #


def test_is_open_access_non_dict_input_returns_false() -> None:
    """Cover line 329: a non-str, non-dict input (a list) reaches ``data = metadata`` then ``return False``.

    A list is not a ``str`` (so the JSON branch is skipped and ``data = metadata`` runs) and not a
    ``dict``, so ``if not isinstance(data, dict): return False`` (328-329) fires instead of a raise.
    """
    assert is_open_access(["is_pmc_openaccess", True]) is False  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# read_table -> _load_table: parse-error (501-502) + unsupported suffix (503)
# --------------------------------------------------------------------------- #


def test_read_table_unparseable_csv_raises_valueerror(tmp_path: Path) -> None:
    """Cover lines 501-502: a 0-byte ``.csv`` makes ``pl.read_csv`` raise, collapsed into ``ValueError``.

    A zero-byte CSV has no data, so polars raises (``NoDataError``); ``_load_table`` catches it
    (501) and re-raises the actionable ``ValueError("Could not read table ...")`` (502) rather than
    leaking the raw engine error.
    """
    path: Path = tmp_path / "empty.csv"
    path.write_text("")
    with pytest.raises(ValueError, match="Could not read table"):
        read_table(path)


def test_read_table_unsupported_suffix_raises_valueerror(tmp_path: Path) -> None:
    """Cover line 503: a real file with an unknown suffix falls through ``_load_table`` to the ``ValueError``.

    A ``.dat`` file exists (passes the ``is_file`` guard) but matches no reader suffix, so the ``try``
    block completes without returning and the trailing ``raise ValueError(... unsupported extension ...)``
    (503) fires — never a silent mis-read.
    """
    path: Path = tmp_path / "blob.dat"
    path.write_text("a,b\n1,2\n")
    with pytest.raises(ValueError, match="unsupported extension"):
        read_table(path)


def test_read_table_reads_parquet_payload(tmp_path: Path) -> None:
    """``read_table`` renders a ``.parquet`` payload through ``_load_table`` -> ``read_parquet``.

    Parquet payloads downloaded from supplements must reach the agent context like csv/tsv do:
    the suffix dispatches to ``pl.read_parquet`` (typed columns as-is), the preview renders the
    cells, and the shape line reports the real height/width. No ``sheets:`` note appears --
    that annotation is Excel-only and parquet has no worksheets.
    """
    path: Path = tmp_path / "table.parquet"
    pl.DataFrame({"gene": ["brca1", "mapk1"], "score": [0.5, 0.9]}).write_parquet(path)

    rendered: str = read_table(path)
    assert "shape: 2x2" in rendered
    assert "brca1" in rendered
    assert "mapk1" in rendered
    assert "sheets:" not in rendered


def test_load_table_reads_parquet_directly(tmp_path: Path) -> None:
    """``_load_table`` returns the parquet frame as-is (real names, no positional remap).

    Unlike the pipeline reader (``lib.parquet``, which renames to ``column_1..`` for the
    positional config contract), the agent reads tables to SHOW them, so the frame keeps the
    file's own column names; only the rendered preview reaches the model.
    """
    path: Path = tmp_path / "table.parquet"
    pl.DataFrame({"gene": ["brca1"]}).write_parquet(path)
    frame: pl.DataFrame = _load_table(path)
    assert frame.columns == ["gene"]
    assert frame["gene"].to_list() == ["brca1"]


# --------------------------------------------------------------------------- #
# _count_ndjson_lines: absent file -> 0 (939)
# --------------------------------------------------------------------------- #


def test_count_ndjson_lines_absent_file_returns_zero(tmp_path: Path) -> None:
    """Cover line 939: a nonexistent artifact path short-circuits to ``0`` (the ``not path.is_file()`` branch)."""
    assert _count_ndjson_lines(tmp_path / "missing.ndjson") == 0


# --------------------------------------------------------------------------- #
# build_and_audit branches: pydantic (1015), unmeasurable (1034), unavailable (1037-1038)
#
# These drive the REAL validate/build pipelines against a tiny REAL redb; only the
# documented seams are faked (validate_pipeline for 1015, map_coverage for 1034/1037).
# --------------------------------------------------------------------------- #


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    return path


def _synonym_row(curie: str, preferred_name: str, names: list[str], category: str) -> dict[str, Any]:
    return {"curie": curie, "preferred_name": preferred_name, "names": names, "types": [category], "taxa": ["NCBITaxon:9606"]}


def _class_row(curie: str, equivalents: list[str]) -> dict[str, Any]:
    return {"id": curie, "equivalent_identifiers": [{"identifier": x} for x in equivalents]}


def _build_real_redb(root: Path) -> Path:
    """Build a tiny REAL fullmap redb: ``brca1`` -> HGNC:1100, ``mapk1`` -> HGNC:6871."""
    root.mkdir(parents=True, exist_ok=True)
    classes: Path = _write_jsonl(root / "classes.ndjson", [_class_row("HGNC:1100", ["NCBIGene:672"])])
    synonyms: Path = _write_jsonl(
        root / "synonyms.ndjson",
        [_synonym_row("HGNC:1100", "BRCA1", ["BRCA1", "brca1"], "Gene"), _synonym_row("HGNC:6871", "MAPK1", ["MAPK1", "mapk1"], "Gene")],
    )
    output: Path = root / "data" / "fullmap.redb"
    rs.build_fullmap_db(output, [classes], [synonyms])
    return output


@pytest.fixture
def redb(tmp_path: Path) -> Path:
    """Offline real redb under ``tmp_path`` (absolute path; no chdir needed to build it)."""
    return _build_real_redb(tmp_path / "fullmap")


def _write_table(tmp_path: Path, text: str) -> Path:
    data: Path = tmp_path / "data.tsv"
    data.write_text(text)
    return data


def _section_config(data: Path) -> dict[str, Any]:
    """A bare merged Section config: column A subject, column B object, PMC provenance."""
    return {
        "source": {"kind": "text", "local": str(data), "url": ["https://example.com/data.tsv"], "delimiter": "\t"},
        "statement": {
            "subject": {"method": "column", "encoding": "A"},
            "predicate": "associated_with",
            "object": {"method": "column", "encoding": "B"},
        },
        "provenance": {"repo": "PMC", "publication": "PMC1"},
    }


def _yaml(config: dict[str, Any]) -> str:
    return yaml.safe_dump(config, sort_keys=False)


def test_build_and_audit_pydantic_validation_error_branch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cover line 1015: a raw ``pydantic.ValidationError`` escaping the pipeline -> ``return _err(exc)``.

    The real pipelines wrap their own ``model_validate`` failures, so this fakes the deferred-imported
    ``tablassert.cli.validate_pipeline`` seam to raise a GENUINE ``pydantic.ValidationError`` (produced by
    ``Tcode.model_validate({})``). ``build_and_audit`` catches it at the ``except pydantic.ValidationError``
    clause (1014) and returns ``_err(exc)`` (1015) — ``ok=False``, flattened message, empty ``error_codes``
    (a ``pydantic.ValidationError`` has no ``.code``), no artifact, and never raises.
    """
    from tablassert.lib import Tcode

    def _raise_pydantic(*args: object, **kwargs: object) -> None:
        Tcode.model_validate({})  # raises a real pydantic.ValidationError

    monkeypatch.setattr("tablassert.cli.validate_pipeline", _raise_pydantic)

    data: Path = _write_table(tmp_path, "brca1\tmapk1\n")
    result = build_and_audit(_yaml(_section_config(data)), fullmap=tmp_path / "unused.redb", workdir=tmp_path)

    assert result["ok"] is False
    errors = result["errors"]
    assert isinstance(errors, list)
    assert errors
    assert isinstance(errors[0], str)
    assert result["error_codes"] == []  # pydantic.ValidationError carries no .code
    assert result["kgx_path"] is None
    assert result["node_count"] == 0


def test_build_and_audit_coverage_unmeasurable_note(tmp_path: Path, redb: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cover line 1034: a successful build whose coverage is ``measured=False`` keeps ``ok=True`` + a note.

    The REAL build runs to success; the ``tablassert.agent.map_coverage`` seam is faked to return
    ``measured=False``. ``build_and_audit`` then appends the ``coverage unmeasurable: ...`` note (1034),
    reports ``coverage_pct=0.0`` (never a false perfect score), and keeps ``ok=True`` with real artifacts.
    """

    def _unmeasurable(*args: object, **kwargs: object) -> dict[str, object]:
        return {"overall": 0.0, "measured": False, "per_column": {}, "unresolved": []}

    monkeypatch.setattr("tablassert.agent.map_coverage", _unmeasurable)

    data: Path = _write_table(tmp_path, "brca1\tmapk1\n")
    result = build_and_audit(_yaml(_section_config(data)), fullmap=redb, workdir=tmp_path)

    assert result["ok"] is True  # non-fatal: the KG still built
    assert result["coverage_pct"] == 0.0
    errors = result["errors"]
    assert isinstance(errors, list)
    assert any("coverage unmeasurable" in str(note) for note in errors)  # the note from line 1034
    assert isinstance(result["node_count"], int)
    assert result["node_count"] > 0


def test_build_and_audit_coverage_unavailable_note(tmp_path: Path, redb: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cover lines 1037-1038: a coverage measurement that RAISES is non-fatal (``ok=True`` + a note).

    The REAL build runs to success; the ``tablassert.agent.map_coverage`` seam is faked to raise.
    The ``except Exception`` clause (1037) catches it and appends ``coverage unavailable: ...`` (1038),
    keeping ``ok=True`` with ``coverage_pct=0.0`` and real artifacts — a coverage failure never masks a build.
    """

    def _explode(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError("coverage backend down")

    monkeypatch.setattr("tablassert.agent.map_coverage", _explode)

    data: Path = _write_table(tmp_path, "brca1\tmapk1\n")
    result = build_and_audit(_yaml(_section_config(data)), fullmap=redb, workdir=tmp_path)

    assert result["ok"] is True  # non-fatal: the KG still built
    assert result["coverage_pct"] == 0.0
    errors = result["errors"]
    assert isinstance(errors, list)
    assert any("coverage unavailable" in str(note) for note in errors)  # the note from line 1038
    assert any("coverage backend down" in str(note) for note in errors)  # the exception message surfaces
    assert isinstance(result["node_count"], int)
    assert result["node_count"] > 0
