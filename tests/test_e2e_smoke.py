"""Fast offline end-to-end smokes wiring the REAL Rust redb to the REAL Python pipeline.

These two smokes catch Rust<->Python contract drift in the quick suite instead of a
multi-hour ``build-kg`` run:

* ``test_build_pipeline_against_real_redb`` drives the full six-stage ``build_pipeline``
  against a tiny REAL ``rs.build_fullmap_db`` redb (no monkeypatched lookup) and asserts
  the KGX NDJSON output contains CURIEs that only the real redb could resolve.
* ``test_validate_command_happy_path`` exercises the ``validate`` cyclopts
  command wrapper (``cli.py``) plus ``validate_pipeline`` on a valid table config.

Both are additive, offline, and fast (<~5s each); all artifacts land in ``tmp_path``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from tablassert import rs
from tablassert.cli import build_pipeline, validate, validate_pipeline
from tablassert.ingests import to_yaml
from tablassert.progress import PipelineProgress


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


def test_build_pipeline_against_real_redb(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """SMOKE (i): the six-stage build resolves terms through a REAL fullmap redb end-to-end.

    The fullmap lookup is NOT monkeypatched: the only way ``HGNC:1100``/``HGNC:6871`` can
    appear in the KGX output is if the Python pipeline really called the Rust redb. Output
    is redirected by ``chdir`` (``compile_graph`` writes to cwd, ``STORE`` is cwd-relative)
    so the repo working tree stays clean.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tablassert" / "store").mkdir(parents=True)

    fullmap: Path = _build_real_redb(tmp_path / "fullmap")

    # Two-row headerless text source: column A = subject term, column B = object term.
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tmapk1\nbrca1\tmapk1\n")

    table: Path = tmp_path / "table.yaml"
    table_config: dict[str, Any] = {
        "template": {
            "source": {"kind": "text", "local": str(data), "url": ["https://example.com/data.tsv"], "delimiter": "\t"},
            "statement": {
                "subject": {"method": "column", "encoding": "A"},
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "B"},
            },
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        }
    }
    to_yaml(table, table_config)

    graph: Path = tmp_path / "graph.yaml"
    graph_config: dict[str, Any] = {
        "name": "SMOKE_KG",
        "version": "1.0.0",
        "tables": [str(table)],
        "fullmap": str(fullmap),
        "rig": rig_factory(tmp_path, infores_id="infores:smoke-kg", source_info={"description": "e2e smoke graph"}),
    }
    to_yaml(graph, graph_config)

    build_pipeline(graph, PipelineProgress(total_stages=6))

    nodes: Path = tmp_path / "SMOKE_KG_1.0.0.nodes.ndjson"
    edges: Path = tmp_path / "SMOKE_KG_1.0.0.edges.ndjson"
    assert nodes.is_file()
    assert edges.is_file()

    node_text: str = nodes.read_text()
    edge_text: str = edges.read_text()
    assert node_text.strip()
    assert edge_text.strip()

    # These CURIEs exist only in the real redb; their presence proves the contract held.
    assert "HGNC:1100" in node_text
    assert "HGNC:6871" in node_text
    assert "HGNC:1100" in edge_text
    assert "HGNC:6871" in edge_text


def test_build_pipeline_over_parquet_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """SMOKE (iii): the same six-stage build reads a ``kind: parquet`` source end to end.

    The strongest possible proof of the new source kind: the declared ``kind`` must select the
    parquet reader, its positional ``column_1..`` convention must drive the column encodings
    (the same ``A``/``B`` letters smoke (i) uses on text), and the rows must reach the real
    redb and come back as resolved CURIEs in the KGX output. Mirrors smoke (i) exactly except
    for the source kind, so any parquet-specific break shows up as a diff against (i).
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tablassert" / "store").mkdir(parents=True)

    fullmap: Path = _build_real_redb(tmp_path / "fullmap")

    # Two-row parquet source: first column = subject term, second column = object term. Columns
    # are addressed positionally (encoding letters A/B), the same contract as text/excel sources.
    data: Path = tmp_path / "data.parquet"
    pl.DataFrame({"gene_a": ["brca1", "brca1"], "gene_b": ["mapk1", "mapk1"]}).write_parquet(data)

    table: Path = tmp_path / "table.yaml"
    table_config: dict[str, Any] = {
        "template": {
            "source": {"kind": "parquet", "local": str(data), "url": ["https://example.com/data.parquet"]},
            "statement": {
                "subject": {"method": "column", "encoding": "A"},
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "B"},
            },
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        }
    }
    to_yaml(table, table_config)

    graph: Path = tmp_path / "graph.yaml"
    graph_config: dict[str, Any] = {
        "name": "PARQUET_KG",
        "version": "1.0.0",
        "tables": [str(table)],
        "fullmap": str(fullmap),
        "rig": rig_factory(tmp_path, infores_id="infores:parquet-kg", source_info={"description": "parquet source smoke graph"}),
    }
    to_yaml(graph, graph_config)

    build_pipeline(graph, PipelineProgress(total_stages=6))

    nodes: Path = tmp_path / "PARQUET_KG_1.0.0.nodes.ndjson"
    edges: Path = tmp_path / "PARQUET_KG_1.0.0.edges.ndjson"
    assert nodes.is_file()
    assert edges.is_file()

    node_text: str = nodes.read_text()
    edge_text: str = edges.read_text()
    assert node_text.strip()
    assert edge_text.strip()

    # Only the real redb knows these CURIEs, so their presence proves the parquet rows flowed
    # through load, encode, and resolve under the positional column contract.
    assert "HGNC:1100" in node_text
    assert "HGNC:6871" in node_text
    assert "HGNC:1100" in edge_text
    assert "HGNC:6871" in edge_text


def test_validate_command_happy_path(tmp_path: Path) -> None:
    """SMOKE (ii): ``validate`` (cli.py wrapper) + ``validate_pipeline`` accept a valid table.

    Mirrors ``tests/fixtures/minimal_section.yaml`` (value-encoded BRCA1/TP53, PMC
    provenance) wrapped in the ``template`` shape ``to_sections`` requires. Validation never
    reads the source data file, so the ``local`` path need not exist.
    """
    config: Path = tmp_path / "table.yaml"
    table_config: dict[str, Any] = {
        "template": {
            "source": {"kind": "text", "local": "./test.tsv", "url": ["https://example.com/test.tsv"], "delimiter": "\t"},
            "statement": {"subject": {"method": "value", "encoding": "BRCA1"}, "object": {"method": "value", "encoding": "TP53"}},
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        }
    }
    to_yaml(config, table_config)

    # The three-stage validate pipeline alone does not raise on a valid section.
    assert validate_pipeline(config, PipelineProgress(total_stages=3)) is None
    # The cyclopts command wrapper (cli.py validate -> run(3, validate_pipeline, ...)).
    assert validate(config, schema="table") is None


def test_build_pipeline_splits_column_annotation_into_per_row_json_array(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """``split_by`` turns each cell's own delimited text into a real JSON array.

    ``split_by`` is the one multivalued encoding. Here the two rows carry different
    values AND different lengths — an array that differs per row, which only a
    per-row split can express.

    Without ``split_by`` the joined cell stays a scalar and ``prune_to_class``
    wraps it into a one-element list -- ``["EFO:0001|EFO:0002"]`` passes Biolink
    validation while giving consumers one unusable blob instead of two ids.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tablassert" / "store").mkdir(parents=True)

    fullmap: Path = _build_real_redb(tmp_path / "fullmap")

    # A=subject  B=object  C=pipe-joined evidence. Rows are swapped so the two edges stay
    # distinct (a shared subject/object would collapse and hide the per-row difference).
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tmapk1\tEFO:0001|EFO:0002\nmapk1\tbrca1\tEFO:0003\n")

    table: Path = tmp_path / "table.yaml"
    table_config: dict[str, Any] = {
        "template": {
            "source": {"kind": "text", "local": str(data), "url": ["https://example.com/data.tsv"], "delimiter": "\t"},
            "statement": {
                "subject": {"method": "column", "encoding": "A"},
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "B"},
            },
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
            "annotations": [{"annotation": "has_evidence", "method": "column", "encoding": "C", "split_by": "|"}],
        }
    }
    to_yaml(table, table_config)

    graph: Path = tmp_path / "graph.yaml"
    graph_config: dict[str, Any] = {
        "name": "SPLIT_KG",
        "version": "1.0.0",
        "tables": [str(table)],
        "fullmap": str(fullmap),
        "rig": rig_factory(tmp_path, infores_id="infores:split-kg", source_info={"description": "split_by annotation smoke graph"}),
    }
    to_yaml(graph, graph_config)

    build_pipeline(graph, PipelineProgress(total_stages=6))

    edges: list[dict[str, Any]] = [json.loads(line) for line in (tmp_path / "SPLIT_KG_1.0.0.edges.ndjson").read_text().splitlines() if line.strip()]
    assert len(edges) == 2
    evidence: dict[tuple[str, str], Any] = {(e["subject"], e["object"]): e["has_evidence"] for e in edges}
    # Each row splits into its OWN array -- different members and different lengths.
    assert evidence[("HGNC:1100", "HGNC:6871")] == ["EFO:0001", "EFO:0002"]
    assert evidence[("HGNC:6871", "HGNC:1100")] == ["EFO:0003"]


def test_build_pipeline_coerces_statistical_annotations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """The real pipeline normalizes raw statistical column names to canonical Biolink fields.

    Declares annotations with non-canonical source spellings (``p value``, ``sample size``,
    ``odds ratio``, ``effect type``) and asserts the emitted KGX edge carries the coerced
    canonical fields flat on the edge (``p_value`` in controlled scientific-notation
    string form, ``effect_size`` as a real float since biolink-model 4.4.4, ``effect_type``
    with the alias mapped to the ``EffectTypes`` enum, ``statistical_significance_qualifier``
    as a bare enum token) and routes the coerced ``study_size`` onto the inlined Study
    (biolink-model#1770 Study node properties). Proves the whole coercion pipeline
    (``coerce_pvalue_columns`` / ``coerce_study_size_columns`` / ``coerce_study_metadata_columns``
    / ``coerce_effect_size_columns`` / ``coerce_effect_type_columns`` / ``sig``) wires
    through ``build_pipeline`` end-to-end.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tablassert" / "store").mkdir(parents=True)

    fullmap: Path = _build_real_redb(tmp_path / "fullmap")

    # A=subject  B=object  C=p value  D=sample size  E=odds ratio(effect size)  F=effect type
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tmapk1\t0.01\t450\t0.85\tSpearman\n")

    table: Path = tmp_path / "table.yaml"
    table_config: dict[str, Any] = {
        "template": {
            "source": {"kind": "text", "local": str(data), "url": ["https://example.com/data.tsv"], "delimiter": "\t"},
            "statement": {
                "subject": {"method": "column", "encoding": "A"},
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "B"},
            },
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
            "annotations": [
                {"annotation": "p value", "method": "column", "encoding": "C"},
                {"annotation": "sample size", "method": "column", "encoding": "D"},
                {"annotation": "odds ratio", "method": "column", "encoding": "E"},
                {"annotation": "effect type", "method": "column", "encoding": "F"},
            ],
        }
    }
    to_yaml(table, table_config)

    graph: Path = tmp_path / "graph.yaml"
    graph_config: dict[str, Any] = {
        "name": "COERCE_KG",
        "version": "1.0.0",
        "tables": [str(table)],
        "fullmap": str(fullmap),
        "rig": rig_factory(tmp_path, infores_id="infores:coerce-kg", source_info={"description": "coercion smoke graph"}),
    }
    to_yaml(graph, graph_config)

    build_pipeline(graph, PipelineProgress(total_stages=6))

    edges_path: Path = tmp_path / "COERCE_KG_1.0.0.edges.ndjson"
    assert edges_path.is_file()
    edge_text: str = edges_path.read_text()
    edges: list[dict[str, Any]] = [json.loads(line) for line in edge_text.splitlines() if line.strip()]
    assert len(edges) == 1
    edge: dict[str, Any] = edges[0]

    # Raw annotation names normalized to canonical Biolink fields flat on the edge.
    # p_value keeps the controlled {:.4e} scientific-notation string contract (Biolink's
    # float typing is satisfied by lax coercion); effect_size is a real JSON number now
    # that biolink-model 4.4.4 types the slot float (PR #1774).
    assert isinstance(edge["p_value"], str)
    assert isinstance(edge["effect_size"], float)
    assert edge["p_value"] == "1.0000e-02"
    assert edge["effect_size"] == 0.85
    assert edge["effect_type"] == "spearmans_rho"  # "Spearman" alias mapped to the EffectTypes enum
    assert "sample size" not in edge
    assert "odds ratio" not in edge
    assert "effect type" not in edge

    # biolink-model 4.4.4 attaches ``statistical_significance_qualifier`` to every
    # association class: the derived band rides the edge as a bare enum token.
    assert edge["statistical_significance_qualifier"] == "strongly_significant"

    # The ``sample size`` alias is coerced to ``study_size`` and lands as a real int
    # property on the inlined Study (biolink-model#1770) -- never on the edge, never in
    # supporting_text. The StudyResult only anchors the source row.
    assert "study_size" not in edge
    assert "supporting_study_size" not in edge_text
    assert "sample_size" not in edge_text
    study: dict[str, Any] = edge["has_supporting_studies"]["PMCID:PMC0000000"]
    assert study["id"] == "PMCID:PMC0000000"
    assert study["name"] == "data.tsv"
    assert study["study_size"] == 450
    assert study["has_study_results"] == [{"id": "row:1"}]


def _build_context_redb(root: Path) -> Path:
    """Tiny real redb: SmallMolecule/Disease terms for a sparse disease_context_qualifier."""
    root.mkdir(parents=True, exist_ok=True)
    classes: Path = _write_jsonl(root / "classes.ndjson", [_class_row("CHEBI:1", ["DRUGBANK:1"]), _class_row("CHEBI:2", ["DRUGBANK:2"])])
    synonyms: Path = _write_jsonl(
        root / "synonyms.ndjson",
        [
            _synonym_row("CHEBI:1", "Aspirin", ["aspirin"], "SmallMolecule"),
            _synonym_row("CHEBI:2", "Ibuprofen", ["ibuprofen"], "SmallMolecule"),
            _synonym_row("MONDO:1", "Headache", ["headache"], "Disease"),
            _synonym_row("MONDO:3", "Migraine", ["migraine"], "Disease"),
            _synonym_row("MONDO:2", "Influenza", ["flu"], "Disease"),
        ],
    )
    output: Path = root / "data" / "fullmap.redb"
    rs.build_fullmap_db(output, [classes], [synonyms])
    return output


def test_nullable_qualifier_keeps_edge_without_key_while_strict_drops(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """A ``nullable`` qualifier keeps a blank-cell row (omitting the key); strict drops it.

    Two rows share a dense subject/object; row 1's disease_context_qualifier cell is
    populated (resolves), row 2's is blank. Strict resolution drops row 2's edge;
    ``nullable: true`` keeps it and the null-stripper omits the qualifier key for it.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tablassert" / "store").mkdir(parents=True)

    fullmap: Path = _build_context_redb(tmp_path / "fullmap")

    # Column C (disease_context) is blank on the second row.
    data: Path = tmp_path / "data.tsv"
    data.write_text("aspirin\theadache\tflu\nibuprofen\tmigraine\t\n")

    def _config(nullable: bool) -> dict[str, Any]:
        qualifier: dict[str, Any] = {"qualifier": "disease_context_qualifier", "method": "column", "encoding": "C", "taxon": None}
        if nullable:
            qualifier["nullable"] = True
        return {
            "template": {
                "source": {"kind": "text", "local": str(data), "url": ["https://example.com/data.tsv"], "delimiter": "\t"},
                "statement": {
                    "subject": {"method": "column", "encoding": "A", "taxon": None},
                    "predicate": "associated_with",
                    "object": {"method": "column", "encoding": "B", "taxon": None},
                    "qualifiers": [qualifier],
                },
                "provenance": {"repo": "PMC", "publication": "PMC0000000"},
            }
        }

    def _build(name: str, nullable: bool) -> list[dict[str, Any]]:
        table: Path = tmp_path / f"{name.lower()}_table.yaml"
        to_yaml(table, _config(nullable))
        graph: Path = tmp_path / f"{name.lower()}_graph.yaml"
        to_yaml(
            graph,
            {
                "name": name,
                "version": "1.0.0",
                "tables": [str(table)],
                "fullmap": str(fullmap),
                "rig": rig_factory(
                    tmp_path, infores_id=f"infores:{name.lower().replace('_', '-')}", source_info={"description": "nullable qualifier smoke"}
                ),
            },
        )
        build_pipeline(graph, PipelineProgress(total_stages=6))
        edges_path: Path = tmp_path / f"{name}_1.0.0.edges.ndjson"
        return [json.loads(line) for line in edges_path.read_text().splitlines() if line.strip()]

    strict_edges: list[dict[str, Any]] = _build("STRICT_KG", nullable=False)
    nullable_edges: list[dict[str, Any]] = _build("NULLABLE_KG", nullable=True)

    # Strict: the blank-qualifier row is dropped at resolution; only the resolved row survives.
    assert len(strict_edges) == 1
    assert strict_edges[0]["disease_context_qualifier"] == "MONDO:2"

    # Nullable: both rows survive. The resolved row carries the qualifier; the blank row omits it.
    assert len(nullable_edges) == 2
    by_subject: dict[str, dict[str, Any]] = {edge["subject"]: edge for edge in nullable_edges}
    assert by_subject["CHEBI:1"]["disease_context_qualifier"] == "MONDO:2"
    assert "disease_context_qualifier" not in by_subject["CHEBI:2"]
