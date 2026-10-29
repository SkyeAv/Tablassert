"""Offline tests for the koza comparison harness (LOC counting + 10k generator)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from count_loc import count_python, count_yaml
from make_10k import generate


def test_count_python_excludes_docstrings_blanks_comments(tmp_path: Path) -> None:
    source = tmp_path / "m.py"
    source.write_text(
        '"""Module docstring\nspans lines.\n"""\n\nimport os  # noqa\n\n\ndef f():\n    """Function docstring."""\n    return 1\n\n\n# comment only line\nx = 2\n',
        encoding="utf-8",
    )
    # code lines: import os, def f():, return 1, x = 2 -> 4
    assert count_python(source) == 4


def test_count_yaml_excludes_blanks_and_comments(tmp_path: Path) -> None:
    data = tmp_path / "c.yaml"
    data.write_text("name: x\n\n# a comment\nreader:\n  format: csv\n", encoding="utf-8")
    assert count_yaml(data) == 3


def test_generate_deterministic_10k(tmp_path: Path) -> None:
    source = tmp_path / "base.csv"
    source.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    out1, out2 = tmp_path / "o1.csv", tmp_path / "o2.csv"
    assert generate(source, out1) == 10_000
    assert generate(source, out2) == 10_000
    assert out1.read_bytes() == out2.read_bytes()
    lines = out1.read_text().splitlines()
    assert lines[0] == "a,b"
    assert len(lines) == 10_001
    # order preserved: row 1 of output is row 1 of source
    assert lines[1] == "1,2"
    assert lines[-1] == "3,4"


def test_generate_rejects_empty_source(tmp_path: Path) -> None:
    source = tmp_path / "empty.csv"
    source.write_text("a,b\n", encoding="utf-8")
    try:
        generate(source, tmp_path / "o.csv")
        raised = False
    except ValueError:
        raised = True
    assert raised


def load_resolved_transform(monkeypatch: pytest.MonkeyPatch):
    decorators = SimpleNamespace(transform=lambda: lambda function: function)
    monkeypatch.setitem(sys.modules, "koza", decorators)
    path = Path(__file__).with_name("tutorial-transform-resolved.py")
    spec = importlib.util.spec_from_file_location("tutorial_transform_resolved", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_koza_hosted_resolution_uses_top_canonical_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_resolved_transform(monkeypatch)

    def fake_lookup(term: str, limit: int, budget: object, only_taxa: str) -> dict[str, object]:
        return {"term": term, "http_status": 200, "error": None, "curies": [f"X:{term}"], "latency_s": 0.1}

    def fake_normalize(curies: list[str], budget: object) -> dict[str, object]:
        normalized = {curie: {"id": {"identifier": curie, "label": curie}, "type": ["biolink:NamedThing"]} for curie in curies}
        return {"normalized": normalized, "http_calls": 1, "error": None}

    monkeypatch.setattr(module, "lookup_one", fake_lookup)
    monkeypatch.setattr(module, "normalize_batch", fake_normalize)
    records = [{"gene_symbol": "TP53", "disease_name": "lung cancer"}]
    resolved, report = module.resolve_labels(records)
    assert resolved["TP53"]["id"] == "X:TP53"
    assert report["complete_records"] == 2
    assert report["http_calls"] == {"lookup": 2, "get_normalized_nodes": 1}


def test_koza_hosted_resolution_rejects_empty_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_resolved_transform(monkeypatch)
    monkeypatch.setattr(
        module, "lookup_one", lambda term, limit, budget, only_taxa: {"term": term, "http_status": 200, "error": None, "curies": [], "latency_s": 0.1}
    )
    monkeypatch.setattr(module, "normalize_batch", lambda curies, budget: {"normalized": {}, "http_calls": 0, "error": None})
    with pytest.raises(RuntimeError, match="no candidates"):
        module.resolve_labels([{"gene_symbol": "unknown", "disease_name": "unknown"}])
