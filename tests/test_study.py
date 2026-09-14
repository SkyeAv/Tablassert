from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import tablassert.cli as cli
import tablassert.study as study


def _write_ndjson(path: Path, lines: list[str]) -> Path:
    """Write raw lines (already JSON-encoded or deliberately malformed) to ``path``."""
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _records(*records: dict[str, Any]) -> list[str]:
    return [json.dumps(record) for record in records]


def _clean(tmp_path: Path) -> tuple[Path, Path]:
    nodes: Path = _write_ndjson(tmp_path / "g_1.nodes.ndjson", _records({"id": "HGNC:5", "name": "insulin"}, {"id": "HGNC:6", "name": "IGF1"}))
    edges: Path = _write_ndjson(tmp_path / "g_1.edges.ndjson", _records({"subject": "HGNC:5", "object": "HGNC:6", "predicate": "biolink:related_to"}))
    return nodes, edges


def _checks(violations: list[study.StudyViolation]) -> dict[str, study.StudyViolation]:
    return {violation.check: violation for violation in violations}


def test_clean_files_pass(tmp_path: Path) -> None:
    """Well-formed nodes/edges files produce no violations."""
    nodes, edges = _clean(tmp_path)
    assert study.study_kgx(nodes, edges) == []


def test_duplicate_node_ids(tmp_path: Path) -> None:
    """A node id appearing on more than one line fails the duplicate assertion."""
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records({"id": "HGNC:5", "name": "a"}, {"id": "HGNC:5", "name": "different"}))
    _, edges = _clean(tmp_path)
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    assert checks["duplicate-node-ids"].count == 1
    assert checks["duplicate-node-ids"].examples == ["HGNC:5"]


def test_duplicate_examples_capped(tmp_path: Path) -> None:
    """Duplicate-id examples are capped while the count stays exact."""
    nodes: Path = _write_ndjson(
        tmp_path / "n.ndjson",
        _records(*({"id": f"X:{i}", "name": f"n{i}"} for i in range(30)), *({"id": f"X:{i}", "name": f"n{i}"} for i in range(30))),
    )
    _, edges = _clean(tmp_path)
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges, example_limit=10))
    assert checks["duplicate-node-ids"].count == 30
    assert len(checks["duplicate-node-ids"].examples) == 10


def test_unnamed_nodes(tmp_path: Path) -> None:
    """A missing, null, empty, or whitespace-only node name fails the unnamed assertion."""
    nodes: Path = _write_ndjson(
        tmp_path / "n.ndjson",
        _records(
            {"id": "A:1"}, {"id": "A:2", "name": None}, {"id": "A:3", "name": ""}, {"id": "A:4", "name": "   "}, {"id": "A:5", "name": "insulin"}
        ),
    )
    edges: Path = _write_ndjson(
        tmp_path / "e.ndjson",
        _records(
            {"subject": "A:1", "predicate": "biolink:related_to", "object": "A:5"},
            {"subject": "A:2", "predicate": "biolink:related_to", "object": "A:3"},
            {"subject": "A:4", "predicate": "biolink:related_to", "object": "A:5"},
        ),
    )
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    violation: study.StudyViolation = checks["unnamed-nodes"]
    assert violation.label == "nodes"
    assert violation.count == 4
    assert sorted(violation.examples) == ["A:1", "A:2", "A:3", "A:4"]


def test_unnamed_node_without_id(tmp_path: Path) -> None:
    """A nameless record with no string id trips both node assertions.

    It has neither id nor name, so it counts under `unnamed-nodes` (keyed
    `<no id>`) and under `unidentified-nodes` (keyed `<no name>`, since the
    name is exactly what is absent).
    """
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records({"name": ""}, {"id": "A:1", "name": "x"}))
    edges: Path = _write_ndjson(tmp_path / "e.ndjson", _records({"subject": "A:1", "predicate": "biolink:related_to", "object": "A:1"}))
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    assert checks["unnamed-nodes"].count == 1
    assert checks["unnamed-nodes"].examples == ["<no id>"]
    assert checks["unidentified-nodes"].count == 1
    assert checks["unidentified-nodes"].examples == ["<no name>"]


def test_unidentified_nodes(tmp_path: Path) -> None:
    """A missing, null, or empty node id fails the unidentified assertion, keyed by name."""
    nodes: Path = _write_ndjson(
        tmp_path / "n.ndjson",
        _records(
            {"name": "insulin"},
            {"id": None, "name": "IGF1"},
            {"id": "", "name": "EGFR"},
            {"id": "   ", "name": "GAPDH"},
            {"id": "A:1", "name": "valid"},
        ),
    )
    # The edge references only the well-formed node, so no other assertion fires.
    edges: Path = _write_ndjson(tmp_path / "e.ndjson", _records({"subject": "A:1", "predicate": "biolink:related_to", "object": "A:1"}))
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    violation: study.StudyViolation = checks["unidentified-nodes"]
    assert violation.label == "nodes"
    assert violation.count == 4
    assert sorted(violation.examples) == ["EGFR", "GAPDH", "IGF1", "insulin"]


def test_unidentified_examples_capped(tmp_path: Path) -> None:
    """Unidentified-node examples are capped while the count stays exact."""
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records(*({"name": f"n{i}"} for i in range(30))))
    _, edges = _clean(tmp_path)
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges, example_limit=10))
    assert checks["unidentified-nodes"].count == 30
    assert len(checks["unidentified-nodes"].examples) == 10


def test_non_string_id_is_not_unidentified(tmp_path: Path) -> None:
    """A non-string, non-null id passes the unidentified assertion, like non-string names.

    No writer emits a non-string id; the assertion targets absent ids, not their
    JSON types. Note such a node is invisible to every id-based check (duplicate,
    undeclared, isolated all collect string ids only) -- which is why a writer
    emitting one would be a separate bug to fix, not one to silently accept here.
    """
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records({"id": 5, "name": "a"}, {"id": "A:1", "name": "b"}))
    edges: Path = _write_ndjson(tmp_path / "e.ndjson", _records({"subject": "A:1", "predicate": "biolink:related_to", "object": "A:1"}))
    assert study.study_kgx(nodes, edges) == []


def test_incomplete_edges(tmp_path: Path) -> None:
    """An edge missing any of subject/predicate/object fails, counted per slot.

    A missing key, a null, and a strip-empty string all count; examples carry
    the slot name and per-slot totals, matching the whitespace example format.
    """
    nodes, _ = _clean(tmp_path)
    edges: Path = _write_ndjson(
        tmp_path / "e.ndjson",
        _records(
            {"predicate": "biolink:related_to", "object": "HGNC:5"},
            {"subject": "HGNC:5", "predicate": None, "object": "HGNC:6"},
            {"subject": "HGNC:5", "predicate": "biolink:related_to", "object": ""},
            {"subject": "HGNC:5", "predicate": "biolink:related_to", "object": "HGNC:6"},
        ),
    )
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    violation: study.StudyViolation = checks["incomplete-edges"]
    assert violation.label == "edges"
    assert violation.count == 3
    assert sorted(violation.examples) == ["object (1)", "predicate (1)", "subject (1)"]


def test_non_string_edge_slots_are_not_incomplete(tmp_path: Path) -> None:
    """Non-string, non-null edge slots pass the incomplete assertion.

    Mirrors the id/name convention: the assertion targets absent slots, not
    their JSON types; no writer emits a non-string slot.
    """
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records({"id": "A:1", "name": "a"}, {"id": "A:2", "name": "b"}))
    edges: Path = _write_ndjson(tmp_path / "e.ndjson", _records({"subject": "A:1", "predicate": 5, "object": "A:2"}))
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    assert "incomplete-edges" not in checks


def test_incomplete_edges_clean_pass(tmp_path: Path) -> None:
    """Edges carrying all three core slots raise no incomplete-edges violation."""
    nodes, _ = _clean(tmp_path)
    edges: Path = _write_ndjson(tmp_path / "e.ndjson", _records({"subject": "HGNC:5", "predicate": "biolink:related_to", "object": "HGNC:6"}))
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    assert "incomplete-edges" not in checks


def test_unnamed_examples_capped(tmp_path: Path) -> None:
    """Unnamed-node examples are capped while the count stays exact."""
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records(*({"id": f"X:{i}"} for i in range(30))))
    _, edges = _clean(tmp_path)
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges, example_limit=10))
    assert checks["unnamed-nodes"].count == 30
    assert len(checks["unnamed-nodes"].examples) == 10


def test_non_string_name_is_not_unnamed(tmp_path: Path) -> None:
    """A non-string, non-null name passes; the assertion targets absent/empty names only."""
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records({"id": "A:1", "name": 5}, {"id": "A:2", "name": "x"}))
    edges: Path = _write_ndjson(tmp_path / "e.ndjson", _records({"subject": "A:1", "predicate": "biolink:related_to", "object": "A:2"}))
    assert study.study_kgx(nodes, edges) == []


def test_empty_or_null_values(tmp_path: Path) -> None:
    """Nulls, strip-empty strings, and empty containers fail the empty-or-null assertion.

    The check asserts the stronger no-null-or-empty-anywhere contract (stricter than
    the writer's strip_nulls, which keeps array scalars verbatim), so these shapes
    fail loudly instead of shipping. Nested hits (an empty attribute value inside a
    list) are counted under the top-level field.
    """
    nodes: Path = _write_ndjson(
        tmp_path / "n.ndjson",
        _records(
            {"id": "A:1", "name": "a", "category": None, "provided_by": []},
            {"id": "A:2", "name": "b", "description": "   "},
            {"id": "A:3", "name": "c", "synonym": ["x", ""]},
        ),
    )
    edges: Path = _write_ndjson(
        tmp_path / "e.ndjson",
        _records(
            {"subject": "A:1", "object": "A:2", "predicate": "biolink:related_to", "p_value": None},
            {"subject": "A:1", "object": "A:3", "predicate": "biolink:related_to", "attributes": [{"value": None}]},
        ),
    )
    violations: list[study.StudyViolation] = [v for v in study.study_kgx(nodes, edges) if v.check == "empty-or-null-values"]
    by_label: dict[str, study.StudyViolation] = {v.label: v for v in violations}
    assert len(violations) == 2
    assert by_label["nodes"].count == 4
    assert sorted(by_label["nodes"].examples) == ["category (1)", "description (1)", "provided_by (1)", "synonym (1)"]
    assert by_label["edges"].count == 2
    assert sorted(by_label["edges"].examples) == ["attributes (1)", "p_value (1)"]


def test_empty_or_null_writer_pass_through_shapes(tmp_path: Path) -> None:
    """Shapes the writer passes verbatim are still flagged: array scalars, emptied dicts.

    `strip_nulls` scrubs dict entries but keeps array scalars verbatim and leaves a
    nested dict that empties as `{}`; the study asserts the stricter contract, so
    `["x", ""]`, `["x", null]`, `[{}]`, and `[[]]` all fail.
    """
    nodes: Path = _write_ndjson(
        tmp_path / "n.ndjson",
        _records(
            {"id": "A:1", "name": "a", "synonym": ["x", ""]},
            {"id": "A:2", "name": "b", "synonym": ["x", None]},
            {"id": "A:3", "name": "c", "attributes": [{}]},
            {"id": "A:4", "name": "d", "nested": [[]]},
        ),
    )
    edges: Path = _write_ndjson(
        tmp_path / "e.ndjson",
        _records(
            {"subject": "A:1", "predicate": "biolink:related_to", "object": "A:2"},
            {"subject": "A:3", "predicate": "biolink:related_to", "object": "A:4"},
        ),
    )
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    violation: study.StudyViolation = checks["empty-or-null-values"]
    assert violation.count == 4
    assert sorted(violation.examples) == ["attributes (1)", "nested (1)", "synonym (2)"]


def test_falsy_meaningful_values_pass(tmp_path: Path) -> None:
    """Zero and false are meaningful Biolink values, not absent ones.

    The writer deliberately keeps `0` and `false` (rust/src/json.rs is_present); the
    empty-or-null assertion must agree or every zero-effect-size edge would fail.
    """
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records({"id": "A:1", "name": "a"}, {"id": "A:2", "name": "b"}))
    edges: Path = _write_ndjson(
        tmp_path / "e.ndjson", _records({"subject": "A:1", "object": "A:2", "predicate": "biolink:related_to", "p_value": 0, "negated": False})
    )
    assert study.study_kgx(nodes, edges) == []


def test_original_fields_empty_still_flagged(tmp_path: Path) -> None:
    """The `original_*` exemption covers whitespace only, not emptiness.

    The writer drops null and strip-empty strings everywhere -- verbatim copies
    included -- so an empty `original_*` value is never legitimate output.
    """
    nodes: Path = _write_ndjson(
        tmp_path / "n.ndjson", _records({"id": "A:1", "name": "a", "original_subject": None}, {"id": "A:2", "name": "b", "original_object": ""})
    )
    edges: Path = _write_ndjson(tmp_path / "e.ndjson", _records({"subject": "A:1", "predicate": "biolink:related_to", "object": "A:2"}))
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    violation: study.StudyViolation = checks["empty-or-null-values"]
    assert violation.count == 2
    assert sorted(violation.examples) == ["original_object (1)", "original_subject (1)"]


def test_null_like_strings_are_not_empty_or_null(tmp_path: Path) -> None:
    """Null-like strings (NA/NaN/null/none) are dropped by the writer but are not null/empty.

    The assertion targets absent values, not their string spellings; the writer's
    bad-token sweep already guarantees the spellings never reach the final output.
    """
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records({"id": "A:1", "name": "NA"}, {"id": "A:2", "name": "b", "source": "none"}))
    edges: Path = _write_ndjson(tmp_path / "e.ndjson", _records({"subject": "A:1", "predicate": "biolink:related_to", "object": "A:2"}))
    assert study.study_kgx(nodes, edges) == []


def test_undeclared_nodes(tmp_path: Path) -> None:
    """Edge subject/object ids missing from the nodes file fail the undeclared assertion."""
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records({"id": "HGNC:5", "name": "a"}))
    edges: Path = _write_ndjson(
        tmp_path / "e.ndjson",
        _records(
            {"subject": "HGNC:5", "predicate": "biolink:related_to", "object": "HGNC:6"},
            {"subject": "HGNC:7", "predicate": "biolink:related_to", "object": "HGNC:5"},
        ),
    )
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    assert checks["undeclared-nodes"].count == 2
    assert checks["undeclared-nodes"].examples == ["HGNC:6", "HGNC:7"]


def test_isolated_nodes(tmp_path: Path) -> None:
    """Declared nodes participating in no edge fail the isolated assertion."""
    nodes: Path = _write_ndjson(
        tmp_path / "n.ndjson", _records({"id": "HGNC:5", "name": "a"}, {"id": "HGNC:6", "name": "b"}, {"id": "HGNC:7", "name": "c"})
    )
    edges: Path = _write_ndjson(tmp_path / "e.ndjson", _records({"subject": "HGNC:5", "predicate": "biolink:related_to", "object": "HGNC:6"}))
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    assert checks["isolated-nodes"].count == 1
    assert checks["isolated-nodes"].examples == ["HGNC:7"]


def test_malformed_lines(tmp_path: Path) -> None:
    """Empty lines, invalid JSON, and non-object lines all count as malformed."""
    nodes, _ = _clean(tmp_path)
    edges: Path = _write_ndjson(
        tmp_path / "e.ndjson",
        [json.dumps({"subject": "HGNC:5", "predicate": "biolink:related_to", "object": "HGNC:6"}), "", "{not json", '["a list"]'],
    )
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    assert checks["malformed-lines"].label == "edges"
    assert checks["malformed-lines"].count == 3


def test_whitespace_values(tmp_path: Path) -> None:
    """Leading/trailing whitespace in string values is counted per field."""
    nodes: Path = _write_ndjson(tmp_path / "n.ndjson", _records({"id": "HGNC:5", "name": " padded"}, {"id": "HGNC:6 ", "name": "x"}))
    edges: Path = _write_ndjson(tmp_path / "e.ndjson", _records({"subject": "HGNC:5", "predicate": "biolink:related_to", "object": "HGNC:6"}))
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    violation: study.StudyViolation = checks["whitespace-values"]
    assert violation.label == "nodes"
    assert violation.count == 2
    assert sorted(violation.examples) == ["id (1)", "name (1)"]


def test_whitespace_allowed_in_original_fields(tmp_path: Path) -> None:
    """`original_*` values keep their source whitespace; only other fields are flagged.

    `original_*` slots are verbatim copies of the source-table cell, so leading/trailing
    whitespace there is faithful to the input, not a defect. The study must not count it
    even as the same record carries genuinely padded values on other keys.
    """
    nodes: Path = _write_ndjson(
        tmp_path / "n.ndjson", _records({"id": "HGNC:5", "original_name": " padded source ", "name": " padded"}, {"id": "HGNC:6", "name": "b"})
    )
    edges: Path = _write_ndjson(
        tmp_path / "e.ndjson",
        _records({"subject": "HGNC:5", "predicate": "biolink:related_to", "object": "HGNC:6", "original_subject": " raw gene "}),
    )
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    violation: study.StudyViolation = checks["whitespace-values"]
    # Only the genuinely padded `name` field is flagged; the `original_*` slots are not.
    assert violation.count == 1
    assert violation.examples == ["name (1)"]


def test_missing_file_is_a_violation(tmp_path: Path) -> None:
    """A missing path must never read as a clean bill of health."""
    nodes, _ = _clean(tmp_path)
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, tmp_path / "nope.edges.ndjson"))
    assert checks["file-missing"].label == "edges"


def test_format_violations(tmp_path: Path) -> None:
    """The stderr summary renders one line per failed assertion with examples."""
    violations: list[study.StudyViolation] = [study.StudyViolation("duplicate-node-ids", "nodes", 3, ["HGNC:5", "HGNC:6"])]
    assert study.format_violations(violations) == "nodes: 3 duplicate node ids (e.g. HGNC:5, HGNC:6)"


def test_study_final_ndjson_exits_on_violations(monkeypatch: Any, tmp_path: Path, capsys: Any) -> None:
    """The --qc build stage fails the build (SystemExit 1) when assertions are violated."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(study, "study_kgx", lambda *args, **kwargs: [study.StudyViolation("isolated-nodes", "nodes", 1, ["HGNC:7"])])
    with pytest.raises(SystemExit) as excinfo:
        cli.study_final_ndjson("g", "1", tmp_path)
    assert excinfo.value.code == 1
    assert "no edge" in capsys.readouterr().err


def test_study_final_ndjson_passes_clean(monkeypatch: Any, tmp_path: Path) -> None:
    """The --qc build stage returns normally when every assertion passes."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(study, "study_kgx", lambda *args, **kwargs: [])
    cli.study_final_ndjson("g", "1", tmp_path)


def test_study_final_ndjson_forwards_category_override_flag(monkeypatch: Any, tmp_path: Path) -> None:
    """The --qc stage forwards the category_override declaration to study_kgx."""
    captured: dict[str, Any] = {}
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(study, "study_kgx", lambda *args, **kwargs: captured.update(kwargs) or [])
    cli.study_final_ndjson("g", "1", tmp_path, category_override_declared=True)
    assert captured == {"category_override_declared": True}


def test_duplicate_edge_ids(tmp_path: Path) -> None:
    """an edge id appearing on more than one line fails the duplicate assertion.

    The deduper keys edges on their derived id, so this can only fire when something
    bypassed it -- a hand-built file, or two builds concatenated. KGX requires edge ids
    to be unique, so the study asserts it independently of the writer.
    """
    nodes, _ = _clean(tmp_path)
    edges: Path = _write_ndjson(
        tmp_path / "e.ndjson",
        _records(
            {"id": "dup-id", "subject": "HGNC:5", "object": "HGNC:6", "predicate": "biolink:related_to"},
            {"id": "dup-id", "subject": "HGNC:5", "object": "HGNC:6", "predicate": "biolink:affects"},
        ),
    )
    checks: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    assert checks["duplicate-edge-ids"].count == 1
    assert checks["duplicate-edge-ids"].examples == ["dup-id"]


def test_distinct_edge_ids_pass(tmp_path: Path) -> None:
    """distinct edge ids raise no duplicate violation."""
    nodes, _ = _clean(tmp_path)
    edges: Path = _write_ndjson(
        tmp_path / "e.ndjson",
        _records(
            {"id": "a", "subject": "HGNC:5", "object": "HGNC:6", "predicate": "biolink:related_to"},
            {"id": "b", "subject": "HGNC:6", "object": "HGNC:5", "predicate": "biolink:related_to"},
        ),
    )
    assert "duplicate-edge-ids" not in _checks(study.study_kgx(nodes, edges))


def test_demoted_edges_violate_only_when_category_override_declared(tmp_path: Path) -> None:
    """Bare biolink:Association edges violate the study only under a declared pin.

    A section that pins the association class per object category promises every row
    lands on a pinned class; a demotion means a row escaped the pin (the DAKP
    GenomicEntity-object leak) and shipped without its class-specific evidence slots.
    Without a pin, bare Association is the author's accepted default, not a defect.
    """
    nodes: Path = _write_ndjson(tmp_path / "g_1.nodes.ndjson", _records({"id": "HGNC:5", "name": "insulin"}, {"id": "MONDO:1", "name": "cancer"}))
    edges: Path = _write_ndjson(
        tmp_path / "g_1.edges.ndjson",
        _records(
            {"id": "e1", "subject": "HGNC:5", "object": "MONDO:1", "predicate": "biolink:treats", "category": ["biolink:Association"]},
            {"id": "e2", "subject": "HGNC:5", "object": "MONDO:1", "predicate": "biolink:treats", "category": ["biolink:EntityToDiseaseAssociation"]},
        ),
    )

    flagged: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges, category_override_declared=True))
    assert flagged["demoted-edges"].label == "edges"
    assert flagged["demoted-edges"].count == 1
    assert flagged["demoted-edges"].examples == ["biolink:treats (1)"]

    unpinned: dict[str, study.StudyViolation] = _checks(study.study_kgx(nodes, edges))
    assert "demoted-edges" not in unpinned
