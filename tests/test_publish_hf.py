from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tablassert import biolink, extras
from tablassert.cli import APP
from tablassert.errors import MissingExtraError


def _write_graph(tmp_path: Path, out_dir: Path) -> Path:
    graph_path = tmp_path / "graph.yaml"
    graph_path.write_text(
        f"""name: demo-kg
version: 0.1.0
tables: []
fullmap: .fullmap
rig:
  source_info:
    infores_id: infores:demo-kg
    description: Demo biomedical knowledge graph
    terms_of_use_info:
      license_name: CC BY 4.0
    data_access_locations:
      - https://example.org/data
    source_status: unknown
  ingest_info:
    utility: Demo utility
    scope: Demo scope
  provenance_info:
    contributions:
      - Demo Author - modeling
  artifact_base_url: https://example.org/out
  artifact_base_path: {out_dir}
""",
        encoding="utf-8",
    )
    return graph_path


def test_publish_hf_requires_extra_when_absent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(extras, "find_spec", lambda name: None)
    graph = _write_graph(tmp_path, tmp_path)
    with pytest.raises(MissingExtraError, match="huggingface-hub"):
        APP(["publish-hf", str(graph), "-r", "org/demo"], exit_on_error=False, result_action="return_value")


def test_publish_hf_refuses_invalid_kgx(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(extras, "require", lambda *a, **_kw: None)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "demo-kg_0.1.0.nodes.ndjson").write_text('{"id": "HGNC:1"}\n', encoding="utf-8")
    (out_dir / "demo-kg_0.1.0.edges.ndjson").write_text('{"id": "e1"}\n', encoding="utf-8")
    monkeypatch.setattr(biolink, "validate_kgx", lambda *a, **_kw: {"nodes": {"failures": 1}, "edges": {"failures": 0}})

    graph = _write_graph(tmp_path, out_dir)
    with pytest.raises(SystemExit) as exc:
        APP(["publish-hf", str(graph), "-r", "org/demo"], exit_on_error=False, result_action="return_value")
    assert exc.value.code == 1


def test_publish_hf_dry_run_and_upload(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(extras, "require", lambda *a, **_kw: None)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "demo-kg_0.1.0.nodes.ndjson").write_text(
        json.dumps({"id": "HGNC:1", "name": "A1BG", "category": ["biolink:Gene"]}) + "\n", encoding="utf-8"
    )
    (out_dir / "demo-kg_0.1.0.edges.ndjson").write_text(
        json.dumps({"id": "e1", "subject": "HGNC:1", "predicate": "biolink:related_to", "object": "HGNC:2"}) + "\n", encoding="utf-8"
    )
    (out_dir / "demo-kg_0.1.0.RIG.yaml").write_text("name: demo\n", encoding="utf-8")
    monkeypatch.setattr(biolink, "validate_kgx", lambda *a, **_kw: {"nodes": {"failures": 0}, "edges": {"failures": 0}})

    graph = _write_graph(tmp_path, out_dir)
    APP(["publish-hf", str(graph), "-r", "org/demo", "--dry-run"], exit_on_error=False, result_action="return_value")
    captured = capsys.readouterr().out
    assert "org/demo" in captured
    assert "nodes/data.parquet" in captured
    assert "demo-kg_0.1.0.RIG.yaml" in captured

    uploaded: list[dict[str, Any]] = []

    class _FakeHfApi:
        def create_repo(self, **kwargs: Any) -> None:
            uploaded.append({"create": kwargs})

        def upload_folder(self, **kwargs: Any) -> None:
            readme = (Path(kwargs["folder_path"]) / "README.md").read_text(encoding="utf-8")
            uploaded.append({"upload": kwargs, "readme": readme})

    monkeypatch.setitem(sys.modules, "huggingface_hub", SimpleNamespace(HfApi=_FakeHfApi))
    APP(["publish-hf", str(graph), "-r", "org/demo", "--private"], exit_on_error=False, result_action="return_value")
    assert len(uploaded) == 2
    assert uploaded[0]["create"]["private"] is True
    assert "infores:demo-kg" in uploaded[1]["readme"]
    assert "CC BY 4.0" in uploaded[1]["readme"]


def test_publish_hf_refuses_missing_artifacts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(extras, "require", lambda *a, **_kw: None)
    monkeypatch.chdir(tmp_path)
    graph = _write_graph(tmp_path, tmp_path / "missing_out")
    with pytest.raises(SystemExit) as exc:
        APP(["publish-hf", str(graph), "-r", "org/demo"], exit_on_error=False, result_action="return_value")
    assert exc.value.code == 1
