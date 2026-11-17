"""Offline tests for the Koza configuration-size comparison."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import compare_outputs
from count_loc import count_python, count_yaml, measure


def test_count_python_excludes_docstrings_blanks_comments(tmp_path: Path) -> None:
    source = tmp_path / "m.py"
    source.write_text(
        '"""Module docstring\nspans lines.\n"""\n\nimport os  # noqa\n\n\ndef f():\n    """Function docstring."""\n    return 1\n\n\n# comment only line\nx = 2\n',
        encoding="utf-8",
    )
    assert count_python(source) == 4


def test_count_yaml_excludes_blanks_and_comments(tmp_path: Path) -> None:
    data = tmp_path / "c.yaml"
    data.write_text("name: x\n\n# a comment\nreader:\n  format: csv\n", encoding="utf-8")
    assert count_yaml(data) == 3


def test_measure_counts_non_whitespace_characters(tmp_path: Path) -> None:
    data = tmp_path / "c.yaml"
    data.write_text("name: x\n\n# ignored\nreader:\n  format: csv\n", encoding="utf-8")
    assert measure(data) == {"lines": 3, "non_whitespace_characters": 23}


@pytest.mark.parametrize("matched", [True, False])
def test_compare_resolution_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, matched: bool) -> None:
    tab, koza = tmp_path / "tab", tmp_path / "koza"
    tab.mkdir()
    koza.mkdir()
    (tab / "x.edges.ndjson").write_text(json.dumps({"subject": "X:1", "object": "X:2"}) + "\n")
    (tab / "x.nodes.ndjson").write_text(json.dumps({"id": "X:1", "name": "one", "category": ["biolink:Gene"]}) + "\n")
    (tab / "x.RIG.yaml").write_text("name: example\n")
    obj = "X:2" if matched else "X:3"
    (koza / "x_edges.tsv").write_text(f"subject\tobject\nX:1\t{obj}\n")
    (koza / "x_nodes.tsv").write_text("id\tname\tcategory\nX:1\tone\tbiolink:Gene\n")
    out = tmp_path / "result.json"
    monkeypatch.setattr(sys, "argv", ["compare", "--tablassert", str(tab), "--koza", str(koza), "--out", str(out)])
    if matched:
        compare_outputs.main()
        result = json.loads(out.read_text())
        assert result["edge_subject_object_pairs_equal"] is True
        assert result["unique_node_id_name_category_equal"] is True
        assert result["tablassert"]["rig"] is True
    else:
        with pytest.raises(SystemExit, match="resolution outputs differ"):
            compare_outputs.main()
        assert not out.exists()
