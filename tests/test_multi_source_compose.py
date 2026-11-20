from __future__ import annotations

from pathlib import Path

import pydantic
import pytest

from tablassert.lib import Tcode, compose_sources, idxname, trim
from tablassert.models import Section


def test_section_source_and_compose_validation() -> None:
    base_stmt = {
        "subject": {"method": "column", "encoding": "A"},
        "predicate": "associated_with",
        "object": {"method": "column", "encoding": "lookup:B"},
    }
    base_prov = {"repo": "PMID", "publication": "12345678"}

    with pytest.raises(pydantic.ValidationError, match="section-source-conflict"):
        Section.model_validate(
            {
                "source": {"kind": "text", "local": "a.tsv", "url": ["https://example.org/a.tsv"]},
                "sources": {"facts": {"kind": "text", "local": "a.tsv", "url": ["https://example.org/a.tsv"]}},
                "statement": base_stmt,
                "provenance": base_prov,
            }
        )

    with pytest.raises(pydantic.ValidationError, match="section-source-required"):
        Section.model_validate({"statement": base_stmt, "provenance": base_prov})

    with pytest.raises(pydantic.ValidationError, match="compose-unknown-source"):
        Section.model_validate(
            {
                "sources": {"facts": {"kind": "text", "local": "a.tsv", "url": ["https://example.org/a.tsv"]}},
                "compose": [{"left": "facts", "right": "missing", "how": "left", "on": "A"}],
                "statement": {
                    "subject": {"method": "column", "encoding": "A"},
                    "predicate": "associated_with",
                    "object": {"method": "column", "encoding": "B"},
                },
                "provenance": base_prov,
            }
        )

    with pytest.raises(pydantic.ValidationError, match="encoding-unknown-source"):
        Section.model_validate(
            {
                "sources": {"facts": {"kind": "text", "local": "a.tsv", "url": ["https://example.org/a.tsv"]}},
                "statement": base_stmt,
                "provenance": base_prov,
            }
        )


def test_compose_sources_left_join_and_antijoin(tmp_path: Path) -> None:
    facts = tmp_path / "facts.tsv"
    facts.write_text("G1\tD1\nG2\tD2\nG3\tD3\n", encoding="utf-8")
    lookup = tmp_path / "lookup.tsv"
    lookup.write_text("G1\tBRCA1\nG2\tTP53\n", encoding="utf-8")
    blocklist = tmp_path / "block.tsv"
    blocklist.write_text("G2\n", encoding="utf-8")

    sources = {
        "facts": {"kind": "text", "local": str(facts), "delimiter": "\t", "url": ["https://example.org/facts.tsv"]},
        "lookup": {"kind": "text", "local": str(lookup), "delimiter": "\t", "url": ["https://example.org/lookup.tsv"]},
        "block": {"kind": "text", "local": str(blocklist), "delimiter": "\t", "url": ["https://example.org/block.tsv"]},
    }
    compose = [{"left": "facts", "right": "lookup", "how": "left", "on": "A"}, {"left": "composed", "right": "block", "how": "anti", "on": "A"}]

    lf = compose_sources(sources, compose)
    df = lf.collect()
    assert df["facts__column_1"].to_list() == ["G1", "G3"]
    assert df["lookup__column_2"].to_list() == ["BRCA1", None]
    assert df["extracted_from_row_number"].to_list() == [1, 3]

    trimmed = trim(lf).collect()
    assert trimmed.columns == ["extracted_from_row_number"]


def test_idxname_supports_namespaced_and_bare_columns() -> None:
    assert idxname("A") == "column_1"
    assert idxname("lookup:B") == "lookup__column_2"
    assert idxname("A", primary_prefix="facts__") == "facts__column_1"


def test_tcode_multi_source_builds_source_ops(tmp_path: Path) -> None:
    facts = tmp_path / "facts.tsv"
    facts.write_text("G1\tD1\n", encoding="utf-8")
    lookup = tmp_path / "lookup.tsv"
    lookup.write_text("G1\tBRCA1\n", encoding="utf-8")

    tc = Tcode.model_validate(
        {
            "config": tmp_path / "t.yaml",
            "store": tmp_path / "s.parquet",
            "sources": {
                "facts": {"kind": "text", "local": facts, "delimiter": "\t", "url": ["https://example.org/facts.tsv"]},
                "lookup": {"kind": "text", "local": lookup, "delimiter": "\t", "url": ["https://example.org/lookup.tsv"]},
            },
            "compose": [{"left": "facts", "right": "lookup", "how": "inner", "on": "A"}],
            "statement": {
                "subject": {"method": "column", "encoding": "lookup:B"},
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "B"},
            },
            "provenance": {"repo": "PMID", "publication": "12345678"},
        }
    )
    ops = tc._source_ops()
    assert ops[0][0] is compose_sources
