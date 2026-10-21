"""Pipeline/e2e tests shaped after named production configs.

Each test here mirrors a config feature that a real production table config uses but
that the rest of the suite previously exercised only at the model/op level (unit tests
of ``fill``/``explode``/``reindex``/``retrieval_sources`` etc.). The configs that
shaped each test are named in its docstring:

* MultiomicsNext mokg-v12 (ELDJARN1, HUANG2, DENG8) -- explode+prefix subject
  encoding, qualifier-level regex direction mapping, ``${1}`` regex backrefs.
* TableConfigs MBKG (ALAM1, HOSKINSON3, MANOR2, RAVIK9) -- regex chains, default
  ``ne`` reindex, ``copysign`` transformations, partial node templates.
* TableConfigs FLAKASSIST (HU1, LIU1, BRUNDAGE2) -- ``fill: forward``/``zero``,
  ``suffix``.
* DAKP (contraindications, approved_treats) -- nullable+prioritize+avoid qualifiers,
  provenance ``override`` with per-upstream source-record-URL rehoming.

All tests are offline and deterministic: each builds a tiny REAL fullmap redb inline
via ``rs.build_fullmap_db`` and runs the real six-stage ``build_pipeline`` against
``tmp_path`` payloads, exactly like ``tests/test_e2e_smoke.py``.

Note on CURIE literals: the fullmap indexes each synonym row's own CURIE as a
resolvable term, so ``method: value`` nodes spelled as CURIEs (e.g. ``MONDO:0004979``)
resolve as long as the redb carries a synonym row for that CURIE -- which
``_build_rich_redb`` provides for every literal used below.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from tablassert import rs
from tablassert.cli import build_pipeline
from tablassert.ingests import from_yaml, to_sections, to_yaml
from tablassert.progress import PipelineProgress


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    return path


def _synonym_row(curie: str, preferred_name: str, names: list[str], category: str) -> dict[str, Any]:
    return {"curie": curie, "preferred_name": preferred_name, "names": names, "types": [category], "taxa": ["NCBITaxon:9606"]}


def _class_row(curie: str, equivalents: list[str]) -> dict[str, Any]:
    return {"id": curie, "equivalent_identifiers": [{"identifier": x} for x in equivalents]}


def _build_rich_redb(root: Path) -> Path:
    """Build a tiny REAL fullmap redb covering every term the tests below resolve.

    Beyond plain Gene synonyms this carries:

    * UniProtKB-shaped Protein entries (for the ELDJARN1 explode+prefix test -- the
      prefixed products ``UniProtKB:P12345``/``UniProtKB:Q99999`` must resolve).
    * A category-ambiguous term ``ambiterm`` on BOTH a Disease CURIE (MONDO:9999,
      preferred name exactly ``ambiterm`` so its ``pr_base`` rank is 1) and a
      PhenotypicFeature CURIE (HP:9999, preferred name differs, ``pr_base`` 10), so
      ``avoid``/``prioritize`` tiebreaks are deterministic: default ranking picks the
      Disease, ``prioritize: [PhenotypicFeature]`` flips to HP:9999, and
      ``avoid: [PhenotypicFeature]`` leaves only the Disease.
    * Synonym rows for the CURIE literals used as ``method: value`` nodes
      (``MONDO:0004979``) and for the suffix product ``asthma_incident``.
    """
    root.mkdir(parents=True, exist_ok=True)
    classes: Path = _write_jsonl(
        root / "classes.ndjson",
        [
            _class_row("HGNC:1100", ["NCBIGene:672"]),
            _class_row("HGNC:6871", ["NCBIGene:5594"]),
            _class_row("HGNC:11998", ["NCBIGene:7157"]),
            _class_row("HGNC:3236", ["NCBIGene:1956"]),
            _class_row("UniProtKB:P12345", []),
            _class_row("UniProtKB:Q99999", []),
            _class_row("MONDO:0004979", []),
            _class_row("MONDO:9999", []),
            _class_row("MONDO:5550", []),
            _class_row("HP:9999", []),
            _class_row("HP:0001250", []),
            _class_row("CHEBI:15365", []),
            _class_row("CHEBI:3672", []),
        ],
    )
    synonyms: Path = _write_jsonl(
        root / "synonyms.ndjson",
        [
            _synonym_row("HGNC:1100", "BRCA1", ["BRCA1", "brca1"], "Gene"),
            _synonym_row("HGNC:6871", "MAPK1", ["MAPK1", "mapk1"], "Gene"),
            _synonym_row("HGNC:11998", "TP53", ["TP53", "tp53"], "Gene"),
            _synonym_row("HGNC:3236", "EGFR", ["EGFR", "egfr"], "Gene"),
            _synonym_row("UniProtKB:P12345", "P12345", ["P12345"], "Protein"),
            _synonym_row("UniProtKB:Q99999", "Q99999", ["Q99999"], "Protein"),
            _synonym_row("MONDO:0004979", "asthma", ["asthma"], "Disease"),
            _synonym_row("MONDO:9999", "ambiterm", ["ambiterm"], "Disease"),
            _synonym_row("MONDO:5550", "asthma, incident", ["asthma_incident"], "Disease"),
            _synonym_row("HP:9999", "Ambiterm phenotype", ["ambiterm"], "PhenotypicFeature"),
            _synonym_row("HP:0001250", "seizure", ["seizure"], "PhenotypicFeature"),
            _synonym_row("CHEBI:15365", "aspirin", ["aspirin"], "SmallMolecule"),
            _synonym_row("CHEBI:3672", "ibuprofen", ["ibuprofen"], "SmallMolecule"),
        ],
    )
    output: Path = root / "data" / "fullmap.redb"
    rs.build_fullmap_db(output, [classes], [synonyms])
    return output


def _table_config(data: Path, statement: dict[str, Any], **extra: Any) -> dict[str, Any]:
    """Minimal one-template table config over a headerless TSV payload."""
    template: dict[str, Any] = {
        "source": {"kind": "text", "local": str(data), "url": ["https://example.com/data.tsv"], "delimiter": "\t"},
        "statement": statement,
        "provenance": {"repo": "PMC", "publication": "PMC0000000"},
    }
    template.update(extra)
    return {"template": template}


def _run_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any, table_config: dict[str, Any], fullmap: Path, name: str
) -> tuple[str, str]:
    """Write table+graph configs under ``tmp_path`` and run the real six-stage build.

    Returns:
        ``(nodes_text, edges_text)`` of the emitted ``<NAME>_1.0.0.*.ndjson`` files.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tablassert" / "store").mkdir(parents=True, exist_ok=True)

    table: Path = tmp_path / f"{name.lower()}_table.yaml"
    to_yaml(table, table_config)
    graph: Path = tmp_path / f"{name.lower()}_graph.yaml"
    graph_config: dict[str, Any] = {
        "name": name,
        "version": "1.0.0",
        "tables": [str(table)],
        "fullmap": str(fullmap),
        "rig": rig_factory(
            tmp_path, infores_id=f"infores:{name.lower().replace('_', '-')}", source_info={"description": f"{name} production-pattern graph"}
        ),
    }
    to_yaml(graph, graph_config)

    build_pipeline(graph, PipelineProgress(total_stages=6))

    nodes: Path = tmp_path / f"{name}_1.0.0.nodes.ndjson"
    edges: Path = tmp_path / f"{name}_1.0.0.edges.ndjson"
    assert nodes.is_file()
    assert edges.is_file()
    return nodes.read_text(), edges.read_text()


def _parse_edges(edge_text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in edge_text.splitlines() if line.strip()]


def test_enum_ranged_qualifier_literal_survives_unresolved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """An enum-ranged qualifier literal passes through verbatim, never near the fullmap.

    Shaped after the mokg-v12 ``object_direction_qualifier`` literals. The qualifier
    range is a closed Biolink vocabulary, so ``_node_ops`` routes it to the
    encode-only branch (lib.py): the emitted edge must carry the exact token
    ``increased`` -- resolving it would produce ``UMLS:C0205217``, which the slot
    rejects.
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tmapk1\n")

    config: dict[str, Any] = _table_config(
        data,
        {
            "subject": {"method": "column", "encoding": "A"},
            "predicate": "associated_with",
            "object": {"method": "value", "encoding": "MONDO:0004979"},
            "qualifiers": [{"qualifier": "object_direction_qualifier", "method": "value", "encoding": "increased"}],
        },
    )
    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "ENUMLIT_KG")

    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    assert len(edges) == 1
    # Verbatim literal: NOT resolved to a CURIE.
    assert edges[0]["object_direction_qualifier"] == "increased"
    assert ":" not in edges[0]["object_direction_qualifier"]


def test_explode_runs_before_prefix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """``explode_by`` splits the cell BEFORE ``prefix`` is applied, per exploded token.

    Shaped after MultiomicsNext mokg-v12 ELDJARN1 (``explode_by: _`` +
    ``prefix: 'UniProtKB:'`` + ``prioritize`` + ``taxon``). The op order in
    ``node_prep``/``encoding`` is fill -> explode -> regex -> remove -> prefix; if
    prefix ran first the single row would carry ``UniProtKB:P12345_Q99999`` and fail
    resolution instead of producing one row per UniProtKB CURIE.
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("P12345_Q99999\n")

    config: dict[str, Any] = _table_config(
        data,
        {
            "subject": {
                "method": "column",
                "encoding": "A",
                "explode_by": "_",
                "prefix": "UniProtKB:",
                "prioritize": ["Protein", "Gene"],
                "taxon": 9606,
            },
            "predicate": "associated_with",
            "object": {"method": "value", "encoding": "MONDO:0004979"},
        },
    )
    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "EXPLODE_KG")

    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    subjects: set[str] = {e["subject"] for e in edges}
    # Prefix applied per exploded token: both CURIEs resolve through the real redb.
    assert subjects == {"UniProtKB:P12345", "UniProtKB:Q99999"}
    # The wrong-order artifact (prefix over the un-split cell) never appears.
    assert "UniProtKB:P12345_Q99999" not in edge_text


def test_provenance_override_rehomes_urls_in_kgx(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """``provenance.override`` replaces KL/AT and re-homes record URLs onto upstreams.

    Shaped after DAKP approved_treats: two upstream infores (``infores:dailymed``,
    ``infores:faers``), a per-upstream ``upstream_source_record_urls`` mapping,
    ``knowledge_level: knowledge_assertion`` and
    ``agent_type: manual_validation_of_automated_agent``. The primary ``sources``
    entry (the graph RIG infores) lists the upstreams but emits NO
    ``source_record_urls``; each supporting entry carries its own URLs instead.
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tasthma\n")

    config: dict[str, Any] = _table_config(
        data,
        {"subject": {"method": "column", "encoding": "A"}, "predicate": "treats", "object": {"method": "column", "encoding": "B"}},
        provenance={
            "repo": "PMC",
            "override": {
                "upstream_resource_ids": ["infores:dailymed", "infores:faers"],
                "upstream_source_record_urls": {
                    "infores:dailymed": ["https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm"],
                    "infores:faers": ["https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"],
                },
                "knowledge_level": "knowledge_assertion",
                "agent_type": "manual_validation_of_automated_agent",
            },
        },
    )
    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "OVERRIDE_KG")

    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    assert len(edges) == 1
    edge: dict[str, Any] = edges[0]

    # Overridden KL/AT land flat on the edge.
    assert edge["knowledge_level"] == "knowledge_assertion"
    assert edge["agent_type"] == "manual_validation_of_automated_agent"

    sources: list[dict[str, Any]] = edge["sources"]
    by_id: dict[str, dict[str, Any]] = {s["resource_id"]: s for s in sources}

    primary: dict[str, Any] = by_id["infores:override-kg"]
    assert primary["resource_role"] == "primary_knowledge_source"
    assert primary["upstream_resource_ids"] == ["infores:dailymed", "infores:faers"]
    # Rehomed: the primary entry emits no record URLs (the null-stripper omits the key).
    assert "source_record_urls" not in primary

    dailymed: dict[str, Any] = by_id["infores:dailymed"]
    assert dailymed["resource_role"] == "supporting_data_source"
    assert dailymed["source_record_urls"] == ["https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm"]
    assert "upstream_resource_ids" not in dailymed

    faers: dict[str, Any] = by_id["infores:faers"]
    assert faers["resource_role"] == "supporting_data_source"
    assert faers["source_record_urls"] == ["https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"]
    assert "upstream_resource_ids" not in faers

    # The section's own source.url serves the RIG only: it is NOT on any edge entry.
    assert "https://example.com/data.tsv" not in edge_text


@pytest.mark.parametrize(
    ("comparison", "comparator", "driver_values", "expected_subjects"),
    [
        # eq/ne compare RAW strings (cast=False in _source_ops), so the driver column
        # must stay string-inferred: an "NA" sentinel keeps polars from typing it f64.
        pytest.param(None, "NA", ["NA", "ok1", "ok2", "ok3"], {"HGNC:6871", "HGNC:11998", "HGNC:3236"}, id="default-ne"),
        pytest.param("ne", "NA", ["NA", "ok1", "ok2", "ok3"], {"HGNC:6871", "HGNC:11998", "HGNC:3236"}, id="ne"),
        pytest.param("eq", "NA", ["NA", "ok1", "ok2", "ok3"], {"HGNC:1100"}, id="eq"),
        # The numeric operators cast the driver column to Float64 before comparing.
        pytest.param("lt", 2, ["0.5", "1.5", "2.5", "3.5"], {"HGNC:1100", "HGNC:6871"}, id="lt"),
        pytest.param("le", 1.5, ["0.5", "1.5", "2.5", "3.5"], {"HGNC:1100", "HGNC:6871"}, id="le"),
        pytest.param("gt", 2, ["0.5", "1.5", "2.5", "3.5"], {"HGNC:11998", "HGNC:3236"}, id="gt"),
        pytest.param("ge", 2.5, ["0.5", "1.5", "2.5", "3.5"], {"HGNC:11998", "HGNC:3236"}, id="ge"),
        # Membership comparisons take a list of identifiers matched against the raw cell value.
        pytest.param("join", ["ok1", "ok2"], ["NA", "ok1", "ok2", "ok3"], {"HGNC:6871", "HGNC:11998"}, id="join"),
        pytest.param("antijoin", ["ok1", "ok2"], ["NA", "ok1", "ok2", "ok3"], {"HGNC:1100", "HGNC:3236"}, id="antijoin"),
        pytest.param("antijoin", ["2.5"], ["0.5", "1.5", "2.5", "3.5"], {"HGNC:1100", "HGNC:6871", "HGNC:3236"}, id="antijoin-numeric-column"),
    ],
)
def test_reindex_filters_rows_all_comparisons(
    comparison: str | None,
    comparator: str | int | float | list[str],
    driver_values: list[str],
    expected_subjects: set[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rig_factory: Any,
) -> None:
    """``source.reindex`` filters rows at runtime for every comparison operator.

    The ``default-ne`` case omits ``comparison`` entirely, exactly like TableConfigs
    MBKG HOSKINSON3: ``Reindex.comparison`` defaults to ``ne``. ``eq``/``ne`` compare
    raw strings (``cast=False`` in the ``_source_ops`` call); the numeric operators
    cast the driver column to Float64 first. ``join``/``antijoin`` compare raw cell strings
    against an identifier list through :func:`tablassert.lib.reindex_membership`, including
    against a column the reader typed as numeric.
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    # Column A = subject term, column B = driver value that decides row survival.
    rows: list[str] = [f"{subject}\t{driver}" for subject, driver in zip(["brca1", "mapk1", "tp53", "egfr"], driver_values, strict=True)]
    data.write_text("\n".join(rows) + "\n")

    reindex: dict[str, Any] = {"column": "B", "comparator": comparator}
    if comparison is not None:
        reindex["comparison"] = comparison
    config: dict[str, Any] = _table_config(
        data,
        {
            "subject": {"method": "column", "encoding": "A"},
            "predicate": "associated_with",
            "object": {"method": "value", "encoding": "MONDO:0004979"},
        },
    )
    config["template"]["source"]["reindex"] = [reindex]

    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "REINDEX_KG")

    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    assert {e["subject"] for e in edges} == expected_subjects


def test_regex_backref_replacement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """``${1}`` capture-group backrefs and qualifier regex mapping survive the pipeline.

    Subject side is shaped after TableConfigs MBKG ALAM1 / mokg-v12 DENG8: an ordered
    regex chain ending in a ``${1}`` backref strips a wrapper prefix and a trailing
    comment, leaving the bare gene symbol that resolves. The qualifier side is shaped
    after mokg-v12 HUANG2: ``subject_direction_qualifier`` maps ``+`` -> upregulated
    and ``-`` -> downregulated on the encode-only (no-resolution) branch. The object
    is a PhenotypicFeature so the derived edge class
    (``GeneToPhenotypicFeatureAssociation``) actually declares the direction qualifier
    slot -- on a plain ``Association`` ``prune_to_class`` would null it.
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("prefix:brca1#row1\t+\nprefix:mapk1#row2\t-\n")

    config: dict[str, Any] = _table_config(
        data,
        {
            "subject": {
                "method": "column",
                "encoding": "A",
                "regex": [{"pattern": "^prefix:", "replacement": ""}, {"pattern": "^(.*)#.*$", "replacement": "${1}"}],
            },
            "predicate": "associated_with",
            "object": {"method": "value", "encoding": "HP:0001250"},
            "qualifiers": [
                {
                    "qualifier": "subject_direction_qualifier",
                    "method": "column",
                    "encoding": "B",
                    "regex": [{"pattern": "\\+", "replacement": "upregulated"}, {"pattern": "-", "replacement": "downregulated"}],
                }
            ],
        },
    )
    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "BACKREF_KG")

    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    by_subject: dict[str, dict[str, Any]] = {e["subject"]: e for e in edges}
    # The ${1} backref extracted the bare symbol, which then resolved.
    assert set(by_subject) == {"HGNC:1100", "HGNC:6871"}
    # The HUANG2 direction mapping landed verbatim (and not as raw +/-).
    assert by_subject["HGNC:1100"]["subject_direction_qualifier"] == "upregulated"
    assert by_subject["HGNC:6871"]["subject_direction_qualifier"] == "downregulated"


def test_avoid_and_prioritize_decide_ambiguous_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """``avoid``/``prioritize`` deterministically steer a category-ambiguous term.

    ``ambiterm`` sits on BOTH MONDO:9999 (Disease, preferred name matches the term,
    so it wins the default ranking) and HP:9999 (PhenotypicFeature). Shaped after the
    ELDJARN1/RAVIK9/contraindications ``prioritize``/``avoid`` usage: with
    ``avoid: [PhenotypicFeature]`` the edge carries the Disease CURIE; with
    ``prioritize: [PhenotypicFeature]`` the same cell flips to the phenotype CURIE
    (``filter_and_rank``: priority multiplier 1 beats the default 50).
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tambiterm\n")

    def _config(object_overrides: dict[str, Any]) -> dict[str, Any]:
        return _table_config(
            data,
            {
                "subject": {"method": "column", "encoding": "A"},
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "B", **object_overrides},
            },
        )

    _n1, avoid_text = _run_build(tmp_path, monkeypatch, rig_factory, _config({"avoid": ["PhenotypicFeature"]}), fullmap, "AVOID_KG")
    _n2, prio_text = _run_build(tmp_path, monkeypatch, rig_factory, _config({"prioritize": ["PhenotypicFeature"]}), fullmap, "PRIORITIZE_KG")

    avoid_edges: list[dict[str, Any]] = _parse_edges(avoid_text)
    assert len(avoid_edges) == 1
    assert avoid_edges[0]["object"] == "MONDO:9999"

    prio_edges: list[dict[str, Any]] = _parse_edges(prio_text)
    assert len(prio_edges) == 1
    assert prio_edges[0]["object"] == "HP:9999"


def test_fill_forward_and_zero_in_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """``fill: forward`` on a node and ``fill: zero`` on an annotation fill blank cells.

    Shaped after FLAKASSIST HU1 (gene symbols forward-filled across mask rows) and
    LIU1 (``fill: zero`` on statistical annotation columns). Row 2 has a blank subject
    and a blank p-value: forward fill carries ``brca1`` down so the row still resolves
    to HGNC:1100, and the p-value becomes 0 -> ``0.0000e-02``-style formatted output.
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tmapk1\t0.01\n\ttp53\t\n")

    config: dict[str, Any] = _table_config(
        data,
        {
            "subject": {"method": "column", "encoding": "A", "fill": "forward", "prioritize": ["Gene"]},
            "predicate": "associated_with",
            "object": {"method": "column", "encoding": "B"},
        },
        annotations=[{"annotation": "p value", "method": "column", "encoding": "C", "fill": "zero"}],
    )
    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "FILL_KG")

    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    assert len(edges) == 2
    by_object: dict[str, dict[str, Any]] = {e["object"]: e for e in edges}
    # The blank subject cell was forward-filled with brca1 and resolved to HGNC:1100.
    assert by_object["HGNC:11998"]["subject"] == "HGNC:1100"
    # fill: zero turned the blank p-value cell into 0, emitted in scientific notation.
    assert by_object["HGNC:11998"]["p_value"] == "0.0000e+00"
    assert by_object["HGNC:6871"]["p_value"] == "1.0000e-02"


def test_copysign_transformation_in_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """A ``copysign`` transformation flips the sign of a numeric annotation column.

    Shaped after TableConfigs MBKG MANOR2 (``transformations: [{function: copysign,
    arguments: [values, -1]}]`` on the ``relationship strength`` column, which the
    clean phase coerces to ``effect_size``). The math op runs on the raw column before
    the coercion rename, and the flipped value lands on the edge as ``effect_size``.
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tmapk1\t0.85\n")

    config: dict[str, Any] = _table_config(
        data,
        {"subject": {"method": "column", "encoding": "A"}, "predicate": "correlated_with", "object": {"method": "column", "encoding": "B"}},
        annotations=[
            {
                "annotation": "relationship strength",
                "method": "column",
                "encoding": "C",
                "transformations": [{"function": "copysign", "arguments": ["values", -1]}],
            },
            {"annotation": "effect type", "method": "value", "encoding": "Spearman"},
        ],
    )
    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "COPYSIGN_KG")

    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    assert len(edges) == 1
    # copysign(0.85, -1) == -0.85, emitted as a real JSON number now that biolink-model
    # 4.4.4 types `effect_size` as a float Association slot (PR #1774).
    assert edges[0]["effect_size"] == -0.85
    assert edges[0]["effect_type"] == "spearmans_rho"


def test_suffix_in_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """``suffix`` appends to the encoded value before resolution.

    Shaped after FLAKASSIST BRUNDAGE2 (``suffix: _incident`` on the object encoding):
    the raw cell ``asthma`` becomes ``asthma_incident``, which resolves to the
    incident-form MONDO:5550 -- proving the suffix ran pre-resolution (the bare term
    would have resolved to MONDO:0004979).
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tasthma\n")

    config: dict[str, Any] = _table_config(
        data,
        {
            "subject": {"method": "column", "encoding": "A"},
            "predicate": "associated_with",
            "object": {"method": "column", "encoding": "B", "suffix": "_incident", "prioritize": ["Disease"]},
        },
    )
    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "SUFFIX_KG")

    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    assert len(edges) == 1
    assert edges[0]["object"] == "MONDO:5550"


def test_nullable_qualifier_with_prioritize_and_avoid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """A ``nullable`` qualifier with prioritize/avoid keeps blank rows, resolves the rest.

    Shaped after DAKP contraindications (``disease_context_qualifier`` with
    ``nullable: true`` + ``prioritize: [Disease]`` + a long ``avoid`` list). Row 1's
    qualifier cell holds the category-ambiguous ``ambiterm`` -- avoid/prioritize steer
    it to the Disease CURIE; row 2's cell is blank, so its edge survives with the
    qualifier key omitted. The subjects are SmallMolecules (contraindications is a
    drug->disease table) so the derived edge class declares the
    ``disease_context_qualifier`` slot -- a plain ``biolink:Association`` does not, and
    ``prune_to_class`` would reroute the value onto the inlined study instead.
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("aspirin\tasthma\tambiterm\nibuprofen\tasthma\t\n")

    config: dict[str, Any] = _table_config(
        data,
        {
            "subject": {"method": "column", "encoding": "A"},
            "predicate": "associated_with",
            "object": {"method": "column", "encoding": "B"},
            "qualifiers": [
                {
                    "qualifier": "disease_context_qualifier",
                    "method": "column",
                    "encoding": "C",
                    "nullable": True,
                    "prioritize": ["Disease"],
                    "avoid": ["PhenotypicFeature"],
                }
            ],
        },
    )
    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "NULLQUAL_KG")

    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    assert len(edges) == 2
    by_subject: dict[str, dict[str, Any]] = {e["subject"]: e for e in edges}
    # Resolved row: avoid/prioritize picked the Disease CURIE for the ambiguous term.
    assert by_subject["CHEBI:15365"]["disease_context_qualifier"] == "MONDO:9999"
    # Blank row: edge kept, qualifier key omitted entirely.
    assert "disease_context_qualifier" not in by_subject["CHEBI:3672"]


def test_partial_node_template_merge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """A template node carrying ONLY ``prioritize`` deep-merges with the section's encoding.

    Shaped after TableConfigs MBKG RAVIK9, whose template declares
    ``object: {prioritize: [...]}`` with no ``method``/``encoding`` -- invalid alone,
    but ``to_sections`` deep-merges it over each section's
    ``object: {method: column, encoding: B}`` so the merged node has all three keys.
    Asserts the merge through the real ``to_sections`` expansion, then proves a full
    build over the merged config succeeds and honors the template's prioritize.
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tambiterm\n")

    config: dict[str, Any] = {
        "template": {
            "source": {"kind": "text", "local": str(data), "url": ["https://example.com/data.tsv"], "delimiter": "\t"},
            "statement": {"subject": {"method": "column", "encoding": "A"}, "predicate": "associated_with", "object": {"prioritize": ["Disease"]}},
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        },
        "sections": [{"statement": {"object": {"method": "column", "encoding": "B"}}}],
    }

    table: Path = tmp_path / "merge_table.yaml"
    to_yaml(table, config)
    sections: list[dict[str, Any]] = to_sections(cast(dict[str, Any], from_yaml(table)), table)  # pyright: ignore
    merged_object: dict[str, Any] = sections[0]["statement"]["object"]
    assert merged_object["prioritize"] == ["Disease"]
    assert merged_object["method"] == "column"
    assert merged_object["encoding"] == "B"

    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "MERGE_KG")
    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    assert len(edges) == 1
    # The template's prioritize steered the ambiguous term to the Disease CURIE.
    assert edges[0]["object"] == "MONDO:9999"


def test_row_slice_crops_specific_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """``row_slice: [2, auto]`` crops the leading rows before any encoding runs.

    The bound is a zero-based offset (``crop`` slices the frame from index 2), so on a
    four-row payload the first two rows are dropped and only rows 3-4 produce edges.
    Every production config above uses this shape to skip header rows
    (``row_slice: [1, auto]`` in the docs tutorial skips a one-line header).
    """
    fullmap: Path = _build_rich_redb(tmp_path / "fullmap")
    data: Path = tmp_path / "data.tsv"
    data.write_text("brca1\tasthma\nmapk1\tasthma\ntp53\tasthma\negfr\tasthma\n")

    config: dict[str, Any] = _table_config(
        data, {"subject": {"method": "column", "encoding": "A"}, "predicate": "associated_with", "object": {"method": "column", "encoding": "B"}}
    )
    config["template"]["source"]["row_slice"] = [2, "auto"]

    _nodes, edge_text = _run_build(tmp_path, monkeypatch, rig_factory, config, fullmap, "SLICE_KG")

    edges: list[dict[str, Any]] = _parse_edges(edge_text)
    subjects: set[str] = {e["subject"] for e in edges}
    # Rows 1-2 (brca1, mapk1) are cropped away; rows 3-4 (tp53, egfr) survive.
    assert subjects == {"HGNC:11998", "HGNC:3236"}
    assert "HGNC:1100" not in edge_text
    assert "HGNC:6871" not in edge_text
