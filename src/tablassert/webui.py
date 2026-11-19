"""Localhost-only Web UI and typed FastAPI backend for YAML-free Tablassert config authoring."""

from __future__ import annotations

import json
import queue
import threading
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from io import StringIO
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError

from tablassert import extras, ingests
from tablassert.enums import IngestCategories, SourceStatuses
from tablassert.errors import flatten_pydantic_error
from tablassert.models import Graph, Section


def _package_version() -> str:
    try:
        return _pkg_version("tablassert")
    except PackageNotFoundError:
        return "0.0.0"


DEFAULT_STATIC_DIR: Path = Path(__file__).resolve().parent / "_webui_dist"

_FALLBACK_HTML: str = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Tablassert Web UI</title>
  <style>
    body { margin: 0; font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; padding: 24px; }
    main { max-width: 720px; margin: 0 auto; }
    code { background: #1e293b; padding: 2px 6px; border-radius: 4px; }
  </style>
</head>
<body>
  <main>
    <h1>Tablassert Web UI API</h1>
    <p>Typed FastAPI backend is running. Available endpoints: <code>GET /api/health</code>, <code>POST /api/validate</code>, <code>POST /api/build</code>.</p>
  </main>
</body>
</html>
"""


class SourcePayload(BaseModel):
    """Typed source input from the Web UI form."""

    kind: Literal["text", "excel", "parquet", "ipc", "ndjson"] = "text"
    local: str = ""
    url: list[str] = Field(default_factory=list)
    delimiter: str = ","
    sheet: str = "Sheet1"
    row_slice_start: int | None = None


class StatementPayload(BaseModel):
    """Typed statement mapping input from the Web UI form."""

    subject_column: str = "A"
    subject_prioritize: str | list[str] = ""
    predicate: str = "associated_with"
    object_column: str = "B"
    object_prioritize: str | list[str] = ""


class ProvenancePayload(BaseModel):
    """Typed provenance input from the Web UI form."""

    repo: str = "PMID"
    publication: str = ""


class GraphPayload(BaseModel):
    """Typed Graph and RIG metadata input from the Web UI form."""

    name: str = "my-kg"
    version: str = "0.1.0"
    fullmap: str = ".fullmap"
    infores_id: str = "infores:tablassert-draft"
    description: str = "Knowledge graph built with Tablassert"
    license_name: str = "CC0 1.0 Universal"
    data_access_url: str = "https://example.org/data"
    contribution: str = "Curator - modeling"
    artifact_base_url: str = "https://example.org/kg"
    artifact_base_path: str = "./output"


class WebUIBuildRequest(BaseModel):
    """Typed request payload shared by ``/api/validate`` and ``/api/build``."""

    source: SourcePayload = Field(default_factory=SourcePayload)
    statement: StatementPayload = Field(default_factory=StatementPayload)
    provenance: ProvenancePayload = Field(default_factory=ProvenancePayload)
    graph: GraphPayload = Field(default_factory=GraphPayload)


class ValidationResponse(BaseModel):
    """Typed response returned by ``POST /api/validate``."""

    valid: bool
    errors: list[str]
    table_yaml: str
    graph_yaml: str


class HealthResponse(BaseModel):
    """Typed response returned by ``GET /api/health``."""

    status: Literal["ok"]
    version: str


def _split_csv(raw: str | list[str]) -> list[str]:
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    return [part.strip() for part in raw.split(",") if part.strip()]


def _dump_yaml_str(data: object) -> str:
    buf = StringIO()
    yaml.safe_dump(data, buf, sort_keys=False, default_flow_style=False, allow_unicode=True, width=120)
    return buf.getvalue()


def build_configs_from_payload(payload: WebUIBuildRequest, *, table_rel_path: str = "tables/table.yaml") -> tuple[dict[str, Any], dict[str, Any]]:
    """Build plain ``table.yaml`` and ``graph.yaml`` dicts from a typed Web UI request."""
    src_in = payload.source
    stmt_in = payload.statement
    prov_in = payload.provenance
    graph_in = payload.graph

    urls: list[str] = [u.strip() for u in src_in.url if u.strip()]
    source_block: dict[str, Any] = {"kind": src_in.kind, "local": src_in.local, "url": urls}
    if src_in.kind == "text":
        source_block["delimiter"] = src_in.delimiter
    elif src_in.kind == "excel":
        source_block["sheet"] = src_in.sheet
    if src_in.row_slice_start is not None and src_in.kind in {"text", "excel"}:
        source_block["row_slice"] = [src_in.row_slice_start, "auto"]

    subj: dict[str, Any] = {"method": "column", "encoding": stmt_in.subject_column}
    subj_prio: list[str] = _split_csv(stmt_in.subject_prioritize)
    if subj_prio:
        subj["prioritize"] = subj_prio

    obj: dict[str, Any] = {"method": "column", "encoding": stmt_in.object_column}
    obj_prio: list[str] = _split_csv(stmt_in.object_prioritize)
    if obj_prio:
        obj["prioritize"] = obj_prio

    table_cfg: dict[str, Any] = {
        "template": {
            "source": source_block,
            "statement": {"subject": subj, "predicate": stmt_in.predicate, "object": obj},
            "provenance": {"repo": prov_in.repo, "publication": prov_in.publication},
        },
        "sections": [{}],
    }

    graph_cfg: dict[str, Any] = {
        "name": graph_in.name,
        "version": graph_in.version,
        "tables": [table_rel_path],
        "fullmap": graph_in.fullmap,
        "rig": {
            "source_info": {
                "infores_id": graph_in.infores_id,
                "name": graph_in.name,
                "description": graph_in.description,
                "terms_of_use_info": {"license_name": graph_in.license_name},
                "data_access_locations": [graph_in.data_access_url],
                "source_status": SourceStatuses.UNKNOWN.value,
            },
            "ingest_info": {
                "ingest_categories": [IngestCategories.TRANSLATOR_KNOWLEDGE_CREATOR.value],
                "utility": graph_in.description,
                "scope": "Generated via tablassert webui",
            },
            "provenance_info": {"contributions": [graph_in.contribution]},
            "artifact_base_url": graph_in.artifact_base_url,
            "artifact_base_path": graph_in.artifact_base_path,
        },
    }
    return table_cfg, graph_cfg


def validate_payload(payload: WebUIBuildRequest) -> ValidationResponse:
    """Generate YAML strings and validate them against ``Section`` and ``Graph``."""
    table_cfg, graph_cfg = build_configs_from_payload(payload)
    errors: list[str] = []
    try:
        Section.model_validate(table_cfg["template"])
    except ValidationError as exc:
        errors.append(flatten_pydantic_error(exc))
    try:
        Graph.model_validate(graph_cfg)
    except ValidationError as exc:
        errors.append(flatten_pydantic_error(exc))

    return ValidationResponse(valid=not errors, errors=errors, table_yaml=_dump_yaml_str(table_cfg), graph_yaml=_dump_yaml_str(graph_cfg))


def write_configs(output_dir: Path, table_cfg: dict[str, Any], graph_cfg: dict[str, Any]) -> tuple[Path, Path]:
    """Write validated plain YAML configs under ``output_dir``."""
    tables_dir: Path = output_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    table_path: Path = tables_dir / "table.yaml"
    graph_path: Path = output_dir / "graph.yaml"
    ingests.to_yaml(table_path, table_cfg)
    ingests.to_yaml(graph_path, graph_cfg)
    return table_path, graph_path


def create_app(output_dir: Path = Path("."), *, static_dir: Path | None = None) -> Any:
    """Create the typed FastAPI application for ``tablassert webui``."""
    extras.require("webui", required_by="tablassert webui")
    from fastapi import FastAPI  # pyright: ignore[reportMissingImports]
    from fastapi.responses import HTMLResponse, StreamingResponse  # pyright: ignore[reportMissingImports]
    from fastapi.staticfiles import StaticFiles  # pyright: ignore[reportMissingImports]

    app = FastAPI(title="Tablassert Web UI", version=_package_version())
    resolved_static: Path = static_dir or DEFAULT_STATIC_DIR
    assets_dir: Path = resolved_static / "assets"
    if assets_dir.is_dir():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    def index() -> HTMLResponse:
        index_file: Path = resolved_static / "index.html"
        if index_file.is_file():
            return HTMLResponse(index_file.read_text(encoding="utf-8"))
        return HTMLResponse(_FALLBACK_HTML)

    def api_health() -> HealthResponse:
        return HealthResponse(status="ok", version=_package_version())

    def api_validate(payload: WebUIBuildRequest) -> ValidationResponse:
        return validate_payload(payload)

    def api_build(payload: WebUIBuildRequest) -> Any:
        result: ValidationResponse = validate_payload(payload)
        if not result.valid:

            def _err_stream() -> Any:
                yield json.dumps({"event": "error", "errors": result.errors}) + "\n"

            return StreamingResponse(_err_stream(), media_type="application/x-ndjson", status_code=400)

        table_cfg, graph_cfg = build_configs_from_payload(payload)
        _, graph_path = write_configs(output_dir, table_cfg, graph_cfg)

        from tablassert.cli import build_pipeline
        from tablassert.progress import PipelineProgress

        q: queue.Queue[str | None] = queue.Queue()

        def _worker() -> None:
            try:
                q.put(json.dumps({"event": "started", "graph": str(graph_path)}) + "\n")
                build_pipeline(graph_path, PipelineProgress(total_stages=6))
                q.put(json.dumps({"event": "done", "graph": str(graph_path)}) + "\n")
            except Exception as exc:
                q.put(json.dumps({"event": "error", "detail": str(exc)}) + "\n")
            finally:
                q.put(None)

        threading.Thread(target=_worker, daemon=True).start()

        def _stream() -> Any:
            while True:
                item: str | None = q.get()
                if item is None:
                    break
                yield item

        return StreamingResponse(_stream(), media_type="application/x-ndjson")

    app.add_api_route("/", index, methods=["GET"], response_class=HTMLResponse)
    app.add_api_route("/api/health", api_health, methods=["GET"], response_model=HealthResponse)
    app.add_api_route("/api/validate", api_validate, methods=["POST"], response_model=ValidationResponse)
    app.add_api_route("/api/build", api_build, methods=["POST"])
    return app


def run_server(*, host: str = "127.0.0.1", port: int = 8765, output_dir: Path = Path(".")) -> None:
    """Start the localhost-only Uvicorn server for ``tablassert webui``."""
    extras.require("webui", required_by="tablassert webui")
    import uvicorn  # pyright: ignore[reportMissingImports]

    app = create_app(output_dir=output_dir)
    uvicorn.run(app, host=host, port=port, log_level="info")
