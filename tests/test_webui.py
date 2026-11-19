from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tablassert import extras, webui
from tablassert.cli import APP
from tablassert.errors import MissingExtraError


def _sample_payload(data_path: Path) -> webui.WebUIBuildRequest:
    return webui.WebUIBuildRequest.model_validate(
        {
            "source": {"kind": "text", "local": str(data_path), "url": ["https://example.org/data.tsv"], "delimiter": "\t", "row_slice_start": 1},
            "statement": {
                "subject_column": "A",
                "subject_prioritize": "Gene",
                "predicate": "associated_with",
                "object_column": "B",
                "object_prioritize": "Disease",
            },
            "provenance": {"repo": "PMID", "publication": "12345678"},
            "graph": {
                "name": "ui-kg",
                "version": "0.1.0",
                "fullmap": ".fullmap",
                "infores_id": "infores:tablassert-draft",
                "description": "UI generated KG",
                "license_name": "CC0 1.0 Universal",
                "data_access_url": "https://example.org/data.tsv",
                "contribution": "Curator - modeling",
                "artifact_base_url": "https://example.org/kg",
                "artifact_base_path": "./output",
            },
        }
    )


def test_webui_requires_webui_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extras, "find_spec", lambda name: None)
    with pytest.raises(MissingExtraError, match="fastapi"):
        APP(["webui"], exit_on_error=False, result_action="return_value")


def test_webui_refuses_non_localhost_bind(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extras, "require", lambda *a, **_kw: None)
    with pytest.raises(SystemExit) as exc:
        APP(["webui", "--host", "0.0.0.0"], exit_on_error=False, result_action="return_value")
    assert exc.value.code == 1


def test_webui_payload_validates_and_round_trips_cli_validate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_path: Path = tmp_path / "data.tsv"
    data_path.write_text("a\tb\nBRCA1\tTP53\n", encoding="utf-8")

    payload = _sample_payload(data_path)
    result: webui.ValidationResponse = webui.validate_payload(payload)
    assert result.valid is True
    assert result.errors == []
    assert "kind: text" in result.table_yaml
    assert "name: ui-kg" in result.graph_yaml

    table_cfg, graph_cfg = webui.build_configs_from_payload(payload)
    _, graph_path = webui.write_configs(tmp_path, table_cfg, graph_cfg)
    monkeypatch.chdir(tmp_path)
    APP(["validate", str(graph_path), "--schema", "graph"], exit_on_error=False, result_action="return_value")


def test_webui_payload_reports_validation_errors(tmp_path: Path) -> None:
    payload = _sample_payload(tmp_path / "data.tsv")
    payload.graph.infores_id = "not-an-infores"
    result: webui.ValidationResponse = webui.validate_payload(payload)
    assert result.valid is False
    assert any("infores" in err.lower() for err in result.errors)


def test_webui_routes_when_fastapi_installed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient  # pyright: ignore[reportMissingImports]

    from tablassert import cli

    called: list[Path] = []
    monkeypatch.setattr(cli, "build_pipeline", lambda graph_path, _progress: called.append(graph_path))

    app = webui.create_app(output_dir=tmp_path)
    client = TestClient(app)

    health = client.get("/api/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    home = client.get("/")
    assert home.status_code == 200
    assert "Tablassert Web UI" in home.text

    payload = _sample_payload(tmp_path / "data.tsv").model_dump()
    val_resp = client.post("/api/validate", json=payload)
    assert val_resp.status_code == 200
    assert val_resp.json()["valid"] is True

    build_resp = client.post("/api/build", json=payload)
    assert build_resp.status_code == 200
    events: list[dict[str, Any]] = [json.loads(line) for line in build_resp.text.splitlines() if line.strip()]
    assert events[-1]["event"] == "done"
    assert called == [tmp_path / "graph.yaml"]
