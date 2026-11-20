from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pydantic
import pytest

from tablassert.cli import APP
from tablassert.errors import GraphValidationError, TablassertError
from tablassert.lib import Tcode, passthrough_audit
from tablassert.models import Graph, NodeEncoding, Qualifier


def _rig_block(out_dir: Path) -> str:
    return f"""  source_info:
    infores_id: infores:passthrough-test
    terms_of_use_info:
      terms_of_use_description: CC0
    data_access_locations:
      - https://example.org/data.tsv
    source_status: unknown
  ingest_info:
    utility: Test passthrough build
    scope: Minimal fixture
  provenance_info:
    contributions:
      - Test Author - author
  artifact_base_url: https://example.org/out
  artifact_base_path: {out_dir}
"""


def test_identified_rejects_resolution_options_and_bad_literal_category() -> None:
    with pytest.raises(pydantic.ValidationError, match="identified-resolution-conflict"):
        NodeEncoding.model_validate(
            {
                "method": "column",
                "encoding": "A",
                "taxon": 9606,
                "identified": {"name": {"method": "column", "encoding": "B"}, "category": {"method": "value", "encoding": "Gene"}},
            }
        )

    with pytest.raises(pydantic.ValidationError, match="identified-bad-category"):
        NodeEncoding.model_validate(
            {
                "method": "column",
                "encoding": "A",
                "identified": {"name": {"method": "column", "encoding": "B"}, "category": {"method": "value", "encoding": "NotACategory"}},
            }
        )

    with pytest.raises(pydantic.ValidationError, match="qualifier-identified-unsupported"):
        Qualifier.model_validate(
            {
                "qualifier": "anatomical_context_qualifier",
                "method": "column",
                "encoding": "A",
                "identified": {"name": {"method": "column", "encoding": "B"}, "category": {"method": "value", "encoding": "AnatomicalEntity"}},
            }
        )


def test_passthrough_audit_validates_curie_and_category() -> None:
    bad_curie = pl.LazyFrame({"subject": ["not-a-curie"], "subject_name": ["TP53"], "subject_category": ["Gene"]})
    with pytest.raises(TablassertError, match="identified-bad-curie"):
        passthrough_audit(bad_curie, "subject").collect()

    bad_cat = pl.LazyFrame({"subject": ["HGNC:11998"], "subject_name": ["TP53"], "subject_category": ["FakeCategory"]})
    with pytest.raises(TablassertError, match="identified-bad-category"):
        passthrough_audit(bad_cat, "subject").collect()


def test_full_passthrough_builds_without_fullmap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tablassert" / "store").mkdir(parents=True, exist_ok=True)
    data = tmp_path / "data.tsv"
    data.write_text("HGNC:11998\tTP53\tGene\tMONDO:0007254\tbreast cancer\tDisease\n", encoding="utf-8")

    table = tmp_path / "table.yaml"
    table.write_text(
        f"""template:
  source:
    kind: text
    local: {data}
    delimiter: "\\t"
    url:
      - https://example.org/data.tsv
  statement:
    subject:
      method: column
      encoding: A
      identified:
        name:
          method: column
          encoding: B
        category:
          method: column
          encoding: C
    predicate: associated_with
    object:
      method: column
      encoding: D
      identified:
        name:
          method: column
          encoding: E
        category:
          method: value
          encoding: Disease
  provenance:
    repo: PMID
    publication: "12345678"
sections:
  - {{}}
""",
        encoding="utf-8",
    )

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    graph_file = tmp_path / "graph.yaml"
    graph_file.write_text(
        f"""name: passthrough-kg
version: 0.1.0
tables:
  - {table}
rig:
{_rig_block(out_dir)}""",
        encoding="utf-8",
    )

    Graph.model_validate(
        {
            "name": "passthrough-kg",
            "version": "0.1.0",
            "tables": [table],
            "rig": {
                "source_info": {
                    "infores_id": "infores:passthrough-test",
                    "terms_of_use_info": {"terms_of_use_description": "CC0"},
                    "data_access_locations": ["https://example.org/data.tsv"],
                    "source_status": "unknown",
                },
                "ingest_info": {"utility": "Test", "scope": "Test"},
                "provenance_info": {"contributions": ["Test - author"]},
                "artifact_base_url": "https://example.org/out",
                "artifact_base_path": out_dir,
            },
        }
    )

    APP(["validate", str(graph_file), "--schema", "graph"], exit_on_error=False, result_action="return_value")
    APP(["build-kg", str(graph_file)], exit_on_error=False, result_action="return_value")

    nodes_file = out_dir / "passthrough-kg_0.1.0.nodes.ndjson"
    edges_file = out_dir / "passthrough-kg_0.1.0.edges.ndjson"
    assert nodes_file.is_file()
    assert edges_file.is_file()

    nodes = {row["id"]: row for row in (json.loads(line) for line in nodes_file.read_text(encoding="utf-8").splitlines() if line.strip())}
    assert nodes["HGNC:11998"]["name"] == "TP53"
    assert nodes["HGNC:11998"]["category"] == ["biolink:Gene"]
    assert nodes["MONDO:0007254"]["name"] == "breast cancer"
    assert nodes["MONDO:0007254"]["category"] == ["biolink:Disease"]


def test_mixed_section_without_fullmap_is_rejected(tmp_path: Path) -> None:
    table = tmp_path / "table.yaml"
    table.write_text(
        """template:
  source:
    kind: text
    local: data.tsv
    delimiter: "\\t"
    url:
      - https://example.org/data.tsv
  statement:
    subject:
      method: column
      encoding: A
      identified:
        name:
          method: column
          encoding: B
        category:
          method: value
          encoding: Gene
    predicate: associated_with
    object:
      method: column
      encoding: C
  provenance:
    repo: PMID
    publication: "12345678"
sections:
  - {}
""",
        encoding="utf-8",
    )

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    graph_file = tmp_path / "graph.yaml"
    graph_file.write_text(
        f"""name: mixed-kg
version: 0.1.0
tables:
  - {table}
rig:
{_rig_block(out_dir)}""",
        encoding="utf-8",
    )

    with pytest.raises(GraphValidationError, match="fullmap is required"):
        APP(["validate", str(graph_file), "--schema", "graph"], exit_on_error=False, result_action="return_value")


def test_tcode_requires_fullmap_reflects_mixed_vs_full_passthrough(tmp_path: Path) -> None:
    mixed = Tcode.model_validate(
        {
            "config": tmp_path / "t.yaml",
            "store": tmp_path / "s.parquet",
            "source": {"kind": "text", "local": "d.tsv", "url": ["https://example.org/d.tsv"]},
            "statement": {
                "subject": {
                    "method": "column",
                    "encoding": "A",
                    "identified": {"name": {"method": "column", "encoding": "B"}, "category": {"method": "value", "encoding": "Gene"}},
                },
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "C"},
            },
            "provenance": {"repo": "PMID", "publication": "12345678"},
        }
    )
    assert mixed.requires_fullmap() is True
    with pytest.raises(TablassertError, match="fullmap-required"):
        mixed.collect(None)
