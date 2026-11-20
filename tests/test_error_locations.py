from __future__ import annotations

from pathlib import Path

import pytest

from tablassert import ingests
from tablassert.cli import APP
from tablassert.errors import SKILL_URL, GraphValidationError, SectionValidationError, YamlParseError


def test_malformed_yaml_reports_exact_location_and_skill_link(tmp_path: Path) -> None:
    bad = tmp_path / "broken.yaml"
    bad.write_text("name: test\nbad_indent: [\n  unclosed\n", encoding="utf-8")

    with pytest.raises(YamlParseError) as exc:
        ingests.from_yaml(bad)

    msg = str(exc.value)
    assert f"{bad}:" in msg
    assert f"{SKILL_URL}yaml-parse-error" in msg


def test_yaml_parse_error_without_mark_does_not_fabricate_location() -> None:
    err = YamlParseError(Path("graph.yaml"), None, None, "unexpected error")
    msg = str(err)
    assert "at graph.yaml:" in msg
    assert "graph.yaml:1:" not in msg
    assert f"{SKILL_URL}yaml-parse-error" in msg


def test_graph_validation_reports_exact_line_column_and_skill_link(tmp_path: Path) -> None:
    graph = tmp_path / "graph.yaml"
    graph.write_text(
        """name: demo
version: 0.1.0
tables: []
fullmap: .fullmap
description: legacy top-level key
""",
        encoding="utf-8",
    )

    with pytest.raises(GraphValidationError) as exc:
        APP(["validate", str(graph), "--schema", "graph"], exit_on_error=False, result_action="return_value")

    msg = str(exc.value)
    assert f"{SKILL_URL}graph-validation-failed" in msg
    assert "rig-legacy-keys" in msg


def test_template_inherited_error_preserves_template_source_location(tmp_path: Path) -> None:
    table = tmp_path / "table.yaml"
    table.write_text(
        """template:
  source:
    kind: text
    local: data.tsv
    url:
      - https://example.org/data.tsv
  statement:
    subject:
      method: column
      encoding: NOT_AN_EXCEL_COL
    predicate: associated_with
    object:
      method: column
      encoding: B
  provenance:
    repo: PMID
    publication: "12345678"
sections:
  - {}
""",
        encoding="utf-8",
    )

    with pytest.raises(SectionValidationError) as exc:
        APP(["validate", str(table), "--schema", "table"], exit_on_error=False, result_action="return_value")

    msg = str(exc.value)
    # Line 9, col 7 is the `statement.subject` mapping inside `template:`
    assert f"{table}:9:7" in msg
    assert f"{SKILL_URL}encoding-bad-excel-column" in msg
    assert f"{SKILL_URL}section-validation-failed" in msg
