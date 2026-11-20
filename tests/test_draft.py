from __future__ import annotations

from pathlib import Path

import pytest

from tablassert import ingests
from tablassert.cli import APP
from tablassert.models import Graph


def _write_minimal_table(path: Path) -> Path:
    path.write_text(
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
    predicate: related_to
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
    return path


def test_draft_generates_schema_valid_graph_config(tmp_path: Path) -> None:
    table = _write_minimal_table(tmp_path / "my-table.yaml")
    out = tmp_path / "graph.yaml"

    APP(["draft", str(table), "-o", str(out)], exit_on_error=False, result_action="return_value")

    assert out.is_file()
    raw = ingests.from_yaml(out)
    assert isinstance(raw, dict)
    graph = Graph.model_validate(raw)
    assert graph.name == "my-table"
    assert graph.version == "0.1.0"
    assert graph.tables == [table]
    assert graph.fullmap == Path(".fullmap")
    assert graph.rig is not None
    assert graph.rig.source_info.infores_id == "infores:tablassert-draft"
    APP(["validate", str(out), "--schema", "graph"], exit_on_error=False, result_action="return_value")


def test_draft_supports_multiple_tables(tmp_path: Path) -> None:
    t1 = _write_minimal_table(tmp_path / "first.yaml")
    t2 = _write_minimal_table(tmp_path / "second.yaml")
    out = tmp_path / "graph.yaml"

    APP(["draft", str(t1), str(t2), "--output", str(out)], exit_on_error=False, result_action="return_value")

    raw = ingests.from_yaml(out)
    assert isinstance(raw, dict)
    graph = Graph.model_validate(raw)
    assert graph.name == "first"
    assert graph.tables == [t1, t2]


def test_draft_refuses_overwrite_without_force(tmp_path: Path) -> None:
    table = _write_minimal_table(tmp_path / "table.yaml")
    out = tmp_path / "graph.yaml"
    out.write_text("existing: true\n", encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        APP(["draft", str(table), "-o", str(out)], exit_on_error=False, result_action="return_value")
    assert exc.value.code == 1
    assert out.read_text(encoding="utf-8") == "existing: true\n"

    APP(["draft", str(table), "-o", str(out), "--force"], exit_on_error=False, result_action="return_value")
    raw = ingests.from_yaml(out)
    assert isinstance(raw, dict)
    assert Graph.model_validate(raw).name == "table"


def test_draft_fails_on_missing_table(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.yaml"
    out = tmp_path / "graph.yaml"

    with pytest.raises(SystemExit) as exc:
        APP(["draft", str(missing), "-o", str(out)], exit_on_error=False, result_action="return_value")
    assert exc.value.code == 1
    assert not out.exists()


def test_draft_refuses_to_overwrite_input_table_or_directory(tmp_path: Path) -> None:
    table = _write_minimal_table(tmp_path / "table.yaml")
    original = table.read_text(encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        APP(["draft", str(table), "-o", str(table), "--force"], exit_on_error=False, result_action="return_value")
    assert exc.value.code == 1
    assert table.read_text(encoding="utf-8") == original

    out_dir = tmp_path / "out_dir"
    out_dir.mkdir()
    with pytest.raises(SystemExit) as exc_dir:
        APP(["draft", str(table), "-o", str(out_dir), "--force"], exit_on_error=False, result_action="return_value")
    assert exc_dir.value.code == 1
    assert out_dir.is_dir()
