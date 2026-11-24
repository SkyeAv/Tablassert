from __future__ import annotations

from pathlib import Path

import pytest

from tablassert import ingests
from tablassert.cli import APP
from tablassert.models import Graph


@pytest.mark.parametrize("kind", ["text", "excel", "parquet", "ipc", "ndjson"])
def test_init_scaffolds_valid_project(tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    project_dir = tmp_path / f"demo-{kind}"
    APP(["init", str(project_dir), "--kind", kind], exit_on_error=False, result_action="return_value")

    graph_path = project_dir / "graph.yaml"
    table_path = project_dir / "tables" / "table.yaml"
    assert graph_path.is_file()
    assert table_path.is_file()
    assert not (project_dir / "data").exists()

    raw_table = ingests.from_yaml(table_path)
    assert isinstance(raw_table, dict)
    assert raw_table["template"]["source"]["kind"] == kind
    assert str(raw_table["template"]["source"]["local"]).startswith("data/sample.")

    raw_graph = ingests.from_yaml(graph_path)
    assert isinstance(raw_graph, dict)
    graph = Graph.model_validate(raw_graph)
    assert graph.name == f"demo-{kind}"
    assert graph.rig is not None
    assert graph.rig.source_info.infores_id == "infores:tablassert-draft"

    monkeypatch.chdir(project_dir)
    APP(["validate", "graph.yaml", "--schema", "graph"], exit_on_error=False, result_action="return_value")


def test_init_refuses_overwrite_without_force(tmp_path: Path) -> None:
    project_dir = tmp_path / "demo"
    APP(["init", str(project_dir)], exit_on_error=False, result_action="return_value")

    with pytest.raises(SystemExit) as exc:
        APP(["init", str(project_dir)], exit_on_error=False, result_action="return_value")
    assert exc.value.code == 1

    APP(["init", str(project_dir), "--force"], exit_on_error=False, result_action="return_value")
    assert (project_dir / "graph.yaml").is_file()
