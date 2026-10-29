#!/usr/bin/env python3
"""Measure Tablassert and Koza tutorial workloads with verified outputs.

Three workloads are reported separately: Tablassert with local fullmap resolution,
Koza mapping without resolution, and Koza with hosted NameResolution followed by
NodeNormalization. Every timing includes a fresh CLI process. Cold runs have no
preceding repeat of that workload; warm runs immediately follow a prior repeat.
Outputs are removed before every run. OS, database, and remote-service caches are
never cleared, so cold does not mean a cold OS or service cache.

Usage on the build host:

    python make_10k.py
    python measure_koza.py --repeats 3 --koza ./koza-venv/bin/koza --out timing.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import shutil
import statistics
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import cast

HERE = Path(__file__).parent


def hardware() -> dict[str, object]:
    info: dict[str, object] = {"platform": platform.platform(), "python": platform.python_version(), "cpu_count": os.cpu_count()}
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal:"):
                info["mem_total_gib"] = round(int(line.split()[1]) / 1024 / 1024, 1)
                break
    return info


def clean_outputs(paths: list[Path]) -> None:
    for path in paths:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)


def output_rows(out_dir: Path, pattern: str) -> tuple[int, int]:
    files = list(out_dir.glob(pattern))
    return len(files), sum(sum(1 for _ in path.open()) for path in files)


def run_tablassert(graph_yaml: Path, out_dir: Path, tablassert_bin: str) -> tuple[float, dict[str, object]]:
    start = time.perf_counter()
    proc = subprocess.run([tablassert_bin, "build-kg", str(graph_yaml)], capture_output=True, text=True, check=False)
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        raise RuntimeError(f"tablassert build-kg exited {proc.returncode}: {proc.stderr[-800:]}")
    edge_files, edge_rows = output_rows(out_dir, "*.edges.ndjson")
    node_files, node_rows = output_rows(out_dir, "*.nodes.ndjson")
    if (edge_files, node_files, edge_rows, node_rows) != (1, 1, 12_500, 9):
        raise RuntimeError(f"unexpected Tablassert output: edges={edge_rows}, nodes={node_rows}")
    return elapsed, {"edge_rows": edge_rows, "node_rows": node_rows}


def run_koza(koza_yaml: Path, out_dir: Path, koza_bin: str, resolved: bool = False) -> tuple[float, dict[str, object]]:
    start = time.perf_counter()
    proc = subprocess.run([koza_bin, "transform", str(koza_yaml), "-o", str(out_dir)], capture_output=True, text=True, check=False)
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        raise RuntimeError(f"koza exited {proc.returncode}: {proc.stderr[-1200:]}")
    edge_files, edge_lines = output_rows(out_dir, "*_edges.tsv")
    node_files, node_lines = output_rows(out_dir, "*_nodes.tsv")
    edge_rows, node_rows = edge_lines - 1, node_lines - 1
    if (edge_files, node_files, edge_rows, node_rows) != (1, 1, 10_000, 20_000):
        raise RuntimeError(f"unexpected Koza output: edges={edge_rows}, nodes={node_rows}")
    check: dict[str, object] = {"edge_rows": edge_rows, "node_rows": node_rows}
    if resolved:
        report_path = HERE / "koza-hosted-resolution.json"
        report = json.loads(report_path.read_text())
        if report["complete_records"] != report["unique_terms"]:
            raise RuntimeError(f"incomplete hosted resolution: {report}")
        check["hosted_resolution"] = report
        for path in out_dir.glob("*_nodes.tsv"):
            with path.open() as stream:
                for node in csv.DictReader(stream, delimiter="\t"):
                    if not all(node.get(field) for field in ("id", "name", "category")) or ":" not in node["id"]:
                        raise RuntimeError(f"incomplete canonical Koza node: {node}")
    return elapsed, check


def measure(mode: str, runner: Callable[[], tuple[float, dict[str, object]]], repeats: int, outputs: list[Path]) -> dict[str, object]:
    walls: list[float] = []
    checks: list[dict[str, object]] = []
    for index in range(repeats):
        clean_outputs(outputs)
        elapsed, check = runner()
        walls.append(elapsed)
        checks.append({"run": index + 1, "wall_s": elapsed, **check})
    return {"mode": mode, "repeats": repeats, "runs": checks, "wall_s": walls, "median_s": statistics.median(walls)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--out", type=Path, default=HERE / "timing.json")
    parser.add_argument("--koza-yaml", type=Path, default=HERE / "tutorial-koza.yaml")
    parser.add_argument("--koza-resolved-yaml", type=Path, default=HERE / "tutorial-koza-resolved.yaml")
    parser.add_argument("--graph-yaml", type=Path, default=HERE / "tutorial-graph.yaml")
    parser.add_argument("--workdir", type=Path, default=HERE / "_runs")
    parser.add_argument("--tablassert", default="/users/sgoetz/Code/dakp/.venv/bin/tablassert")
    parser.add_argument("--koza", default="koza")
    parser.add_argument("--skip-hosted-reason", help="record an explicit blocked hosted arm without running it")
    args = parser.parse_args()

    os.chdir(HERE)
    tab_out = HERE / "tutorial-output"
    koza_out = args.workdir / "koza-out"
    resolved_out = args.workdir / "koza-resolved-out"
    report = HERE / "koza-hosted-resolution.json"
    args.workdir.mkdir(parents=True, exist_ok=True)

    result: dict[str, object] = {
        "date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "versions": {
            "tablassert": subprocess.check_output([args.tablassert, "--version"], text=True).strip(),
            "koza": subprocess.check_output([args.koza, "--version"], text=True).strip(),
            "python": platform.python_version(),
        },
        "hardware": hardware(),
        "input_rows": sum(1 for _ in (HERE / "tutorial-data-10k.csv").open()) - 1,
        "cold_definition": "fresh CLI process, first repeat of the workload, outputs removed; OS/database/service caches not cleared",
        "warm_definition": "fresh CLI process immediately following a repeat, outputs removed; OS/database/service caches not cleared",
        "output_equivalence": (
            "Tablassert uses local fullmap ranking and emits 12,500 edges because one input label has a two-CURIE tie. "
            "Resolved Koza emits the top hosted candidate and 10,000 edges. Mapping-only Koza emits raw labels. "
            "The workloads all perform tabular mapping, but the graphs and matching policies are not equivalent."
        ),
    }
    workloads = {
        "tablassert": (lambda: run_tablassert(args.graph_yaml, tab_out, args.tablassert), [tab_out]),
        "koza_mapping_only": (lambda: run_koza(args.koza_yaml, koza_out, args.koza), [koza_out]),
        "koza_hosted_resolved": (lambda: run_koza(args.koza_resolved_yaml, resolved_out, args.koza, resolved=True), [resolved_out, report]),
    }
    for label, (runner, outputs) in workloads.items():
        if label == "koza_hosted_resolved" and args.skip_hosted_reason:
            result[label] = {"status": "blocked", "reason": args.skip_hosted_reason}
            continue
        result[f"{label}_cold"] = measure("cold", runner, 1, outputs)
        result[f"{label}_warm"] = measure("warm", runner, args.repeats, outputs)

    args.out.write_text(json.dumps(result, indent=2))
    for section in result:
        if section.endswith(("_cold", "_warm")):
            data = cast("dict[str, object]", result[section])
            print(f"{section}: median {cast('float', data['median_s']):.3f}s")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
