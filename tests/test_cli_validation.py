from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from cyclopts.exceptions import MissingArgumentError  # pyright: ignore[reportMissingImports]

from tablassert import cli
from tablassert.cli import build_pipeline, validate, validate_pipeline
from tablassert.errors import GraphValidationError, SectionValidationError
from tablassert.ingests import to_yaml
from tablassert.progress import PipelineProgress


def test_validate_pipeline_rejects_section_missing_source(fixtures_path: Path) -> None:
    """Guard: `validate` fails fast on a table that cannot form a valid section.

    A table config with no usable source would otherwise surface only deep inside a
    multi-hour `build-kg` run; this pre-build check rejects it up front.
    """
    config: Path = fixtures_path / "invalid_section_missing_source.yaml"
    with pytest.raises(SectionValidationError) as exc_info:
        validate_pipeline(config, PipelineProgress(total_stages=3))
    assert exc_info.value.code == "section-validation-failed"


def test_build_pipeline_rejects_graph_referencing_invalid_section(tmp_path: Path, fixtures_path: Path, rig_factory: Any) -> None:
    """Guard: `build-kg` fails fast when a referenced table has an invalid section.

    A graph that points at a malformed table would otherwise fail deep inside a
    multi-hour build; this rejects it during section validation, before any work starts.
    """
    bad_table: Path = fixtures_path / "invalid_section_missing_source.yaml"
    graph_file: Path = tmp_path / "graph.yaml"
    graph: dict[str, Any] = {"name": "TEST", "version": "1.0.0", "tables": [str(bad_table)], "fullmap": ".fullmap", "rig": rig_factory(tmp_path)}
    to_yaml(graph_file, graph)
    with pytest.raises(SectionValidationError) as exc_info:
        build_pipeline(graph_file, PipelineProgress(total_stages=6))
    assert exc_info.value.code == "section-validation-failed"


def test_build_pipeline_rejects_malformed_graph(tmp_path: Path) -> None:
    """Guard: `build-kg` fails fast on a graph config missing required keys.

    A graph without `tables`/`fullmap`/`rig` cannot build; rejecting it at validation avoids
    a confusing failure deep inside a multi-hour build.
    """
    graph_file: Path = tmp_path / "graph.yaml"
    graph: dict[str, Any] = {"name": "TEST", "version": "1.0.0"}
    to_yaml(graph_file, graph)
    with pytest.raises(GraphValidationError) as exc_info:
        build_pipeline(graph_file, PipelineProgress(total_stages=6))
    assert exc_info.value.code == "graph-validation-failed"


def _valid_table_config() -> dict[str, Any]:
    """Minimal valid table config (value-encoded; validation never reads the source file)."""
    return {
        "template": {
            "source": {"kind": "text", "local": "./test.tsv", "url": ["https://example.com/test.tsv"], "delimiter": "\t"},
            "statement": {"subject": {"method": "value", "encoding": "BRCA1"}, "object": {"method": "value", "encoding": "TP53"}},
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        }
    }


def test_validate_pipeline_rejects_duplicate_qualifier_keys(tmp_path: Path) -> None:
    """Guard: `validate` rejects a statement that declares the same qualifier key twice.

    The duplicate used to slip through config validation and crash a `build-kg` run
    ~16 minutes in, when the second resolve pass looked for the already-dropped
    `<col>_two` column. The pre-build check now surfaces it as `qualifier-duplicated`
    before any work starts.
    """
    config: dict[str, Any] = _valid_table_config()
    config["template"]["statement"]["qualifiers"] = [
        {"qualifier": "anatomical_context_qualifier", "method": "value", "encoding": "UBERON:0000061"},
        {"qualifier": "anatomical_context_qualifier", "method": "column", "encoding": "A"},
    ]
    table: Path = tmp_path / "duplicate_qualifier.yaml"
    to_yaml(table, config)
    with pytest.raises(SectionValidationError) as exc_info:
        validate_pipeline(table, PipelineProgress(total_stages=3))
    assert exc_info.value.code == "section-validation-failed"
    assert "qualifier-duplicated" in str(exc_info.value)


def test_validate_command_selects_schema_explicitly(tmp_path: Path, rig_factory: Any) -> None:
    """Guard: `validate` checks a file against the schema the caller selects via `--schema`.

    `--schema table` validates section syntax only; `--schema graph` validates the Graph model
    AND each referenced table. The kind is chosen explicitly (never sniffed from the YAML), so a
    config is always checked against the schema the caller expected. Both return None when valid.
    """
    table: Path = tmp_path / "table.yaml"
    to_yaml(table, _valid_table_config())
    # Table schema: section syntax only.
    assert validate(table, schema="table") is None
    # Graph schema: validates the graph and its referenced tables.
    graph_file: Path = tmp_path / "graph.yaml"
    to_yaml(graph_file, {"name": "TEST", "version": "1.0.0", "tables": [str(table)], "fullmap": ".fullmap", "rig": rig_factory(tmp_path)})
    assert validate(graph_file, schema="graph") is None


def test_validate_schema_flag_parses_and_is_required(tmp_path: Path) -> None:
    """Guard: ``validate`` binds ``--schema``/``-s`` through the parser and REQUIRES it.

    The direct ``validate()`` calls above bypass Cyclopts; this pins the live CLI contract — the
    schema binds via ``--schema`` and ``-s``, and omitting it fails (it is a required option, no
    longer sniffed from the YAML). ``parse_args`` binds WITHOUT executing, so no validation runs.
    """
    config: Path = tmp_path / "config.yaml"

    def parse(argv: list[str]) -> dict[str, Any]:
        fn, bound, _ = cli.APP.parse_args(argv, exit_on_error=False)
        assert fn is validate
        return dict(bound.arguments)

    assert parse(["validate", str(config), "--schema", "table"])["schema"] == "table"
    assert parse(["validate", "-f", str(config), "-s", "graph"])["schema"] == "graph"
    with pytest.raises(MissingArgumentError):
        parse(["validate", str(config)])


def test_validate_command_graph_branch_rejects_invalid_table(tmp_path: Path, fixtures_path: Path, rig_factory: Any) -> None:
    """Guard: `validate --schema graph` fails fast when a referenced table is invalid."""
    bad_table: Path = fixtures_path / "invalid_section_missing_source.yaml"
    graph_file: Path = tmp_path / "graph.yaml"
    to_yaml(graph_file, {"name": "TEST", "version": "1.0.0", "tables": [str(bad_table)], "fullmap": ".fullmap", "rig": rig_factory(tmp_path)})
    with pytest.raises(SectionValidationError) as exc_info:
        validate(graph_file, schema="graph")
    assert exc_info.value.code == "section-validation-failed"


def _kgx_node_row() -> dict[str, Any]:
    """Minimal valid node record for ``validate-kgx`` fixtures."""
    return {"id": "HGNC:11998", "name": "TP53", "category": ["biolink:Gene"]}


def _kgx_edge_base() -> dict[str, Any]:
    """Minimal otherwise-valid edge record for ``validate-kgx`` fixtures."""
    return {
        "subject": "HGNC:11998",
        "predicate": "biolink:associated_with",
        "object": "MONDO:0008903",
        "category": ["biolink:GeneToDiseaseAssociation"],
        "knowledge_level": "statistical_association",
        "agent_type": "data_analysis_pipeline",
    }


def _write_kgx_pair(tmp_path: Path, node_rows: list[dict[str, Any]], edge_lines: list[Any]) -> tuple[Path, Path]:
    """Write a nodes/edges NDJSON pair (raw strings pass through verbatim, for defects)."""
    nodes: Path = tmp_path / "PRUNECLI_KG_1.0.0.nodes.ndjson"
    nodes.write_text("".join(json.dumps(row) + "\n" for row in node_rows), encoding="utf-8")
    edges: Path = tmp_path / "PRUNECLI_KG_1.0.0.edges.ndjson"
    edges.write_text("".join(line if isinstance(line, str) else json.dumps(line) + "\n" for line in edge_lines), encoding="utf-8")
    return nodes, edges


def test_validate_kgx_prune_flag_parses() -> None:
    """Guard: ``validate-kgx`` binds ``--prune``/``-p`` through the parser as a bool flag.

    The direct ``validate_kgx_command()`` calls below bypass Cyclopts; this pins the live
    CLI contract -- both spellings bind to the ``prune`` parameter and default to False,
    so a run without the flag can never rewrite a graph by accident.
    """

    def parse(argv: list[str]) -> dict[str, Any]:
        fn, bound, _ = cli.APP.parse_args(argv, exit_on_error=False)
        assert fn is cli.validate_kgx_command
        return dict(bound.arguments)

    dummy: str = "x.ndjson"
    assert parse(["validate-kgx", "-n", dummy, "-e", dummy, "--prune"])["prune"] is True
    assert parse(["validate-kgx", "-n", dummy, "-e", dummy, "-p"])["prune"] is True
    # cyclopts only binds flags the caller passed; the default stays False at call time
    # (pinned by the behavior tests below, which never rewrite a graph by accident).
    assert parse(["validate-kgx", "-n", dummy, "-e", dummy]).get("prune", False) is False


def test_validate_kgx_prune_removes_defective_edges_and_exits_zero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Guard: ``--prune`` drops the never-compliant edge from the FINAL graph and exits 0.

    Why: the verification requirement is that after a prune the shipped edges file no
    longer contains non-KGX-compliant records -- not merely that a report says so. The
    kept edge must survive byte-identically, and the report must name exactly what was
    removed and what remains.
    """
    nodes, edges = _write_kgx_pair(
        tmp_path, [_kgx_node_row()], [{**_kgx_edge_base(), "id": "keep-1"}, {**_kgx_edge_base(), "id": "drop-1", "p_value": "not-a-number"}]
    )
    original_kept_line: str = json.dumps({**_kgx_edge_base(), "id": "keep-1"}) + "\n"

    cli.validate_kgx_command(nodes=nodes, edges=edges, limit=20, prune=True)

    stderr: str = capsys.readouterr().err
    assert "edges: pruned 1/2 non-compliant records" in stderr
    assert "p_value: float_parsing" in stderr
    assert "KGX output is Biolink-compliant." in stderr
    assert edges.read_text(encoding="utf-8") == original_kept_line  # the defective record is GONE


def test_validate_kgx_prune_drops_pending_gaps_and_exits_zero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Guard: ``--prune`` removes pending carryovers too, and the run goes green.

    Why: the strict bar is the point of the flag -- after it runs, the final graph
    validates with zero failures, so a run over clean nodes exits 0. The report must
    still name the removed records, and a pruned file must be EMPTY here, not quietly
    holding the gaps.
    """
    nodes, edges = _write_kgx_pair(tmp_path, [_kgx_node_row()], [{**_kgx_edge_base(), "id": "pending-1", "provided_by": "infores:test"}])

    cli.validate_kgx_command(nodes=nodes, edges=edges, limit=20, prune=True)

    stderr: str = capsys.readouterr().err
    assert "edges: pruned 1/1 non-compliant records" in stderr
    assert "provided_by: extra_forbidden" in stderr
    assert "KGX output is Biolink-compliant." in stderr
    assert edges.read_bytes() == b""  # the pending gap did not survive the prune


def test_validate_kgx_prune_missing_edges_file_is_not_a_pass(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Guard: pruning a typo'd path reports it and exits non-zero without creating files.

    Why: the prune path has destructive power; a misspelled ``--edges`` must never read
    as a successful no-op prune (0/0 valid would otherwise exit 0 and hide the mistake).
    """
    nodes, edges = _write_kgx_pair(tmp_path, [_kgx_node_row()], [])
    edges.unlink()  # keep the nodes file; only the edges path is a typo

    with pytest.raises(SystemExit) as exc_info:
        cli.validate_kgx_command(nodes=nodes, edges=edges, limit=20, prune=True)

    assert exc_info.value.code == 1
    stderr: str = capsys.readouterr().err
    assert "edges: file not found" in stderr
    assert "KGX output is not Biolink-compliant." in stderr
    assert not edges.exists()  # the prune never created the file
