#!/usr/bin/env python3
"""Compare Tablassert and Koza tutorial outputs for identical entity resolution.

Usage on the build host after running both tools:

    python compare_outputs.py --tablassert tutorial-output --koza koza-output --out output-comparison.json
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def tablassert_graph(out_dir: Path) -> tuple[list[tuple[str, str]], dict[str, tuple[str, tuple[str, ...]]], list[str]]:
    edges_path = next(out_dir.glob("*.edges.ndjson"))
    nodes_path = next(out_dir.glob("*.nodes.ndjson"))
    edges = [json.loads(line) for line in edges_path.open()]
    nodes = [json.loads(line) for line in nodes_path.open()]
    edge_pairs = sorted((edge["subject"], edge["object"]) for edge in edges)
    node_records = {node["id"]: (node["name"], tuple(sorted(node["category"]))) for node in nodes}
    edge_fields = sorted({key for edge in edges for key in edge})
    return edge_pairs, node_records, edge_fields


def koza_graph(out_dir: Path) -> tuple[list[tuple[str, str]], dict[str, tuple[str, tuple[str, ...]]], int, list[str]]:
    with next(out_dir.glob("*_edges.tsv")).open() as stream:
        edges = list(csv.DictReader(stream, delimiter="\t"))
    with next(out_dir.glob("*_nodes.tsv")).open() as stream:
        nodes = list(csv.DictReader(stream, delimiter="\t"))
    edge_pairs = sorted((edge["subject"], edge["object"]) for edge in edges)
    node_records = {node["id"]: (node["name"], tuple(sorted(node["category"].split("|")))) for node in nodes}
    return edge_pairs, node_records, len(nodes), sorted({key for edge in edges for key in edge})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tablassert", type=Path, required=True)
    parser.add_argument("--koza", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    tab_edges, tab_nodes, tab_fields = tablassert_graph(args.tablassert)
    koza_edges, koza_nodes, koza_node_rows, koza_fields = koza_graph(args.koza)
    result = {
        "edge_subject_object_pairs_equal": tab_edges == koza_edges,
        "unique_node_id_name_category_equal": tab_nodes == koza_nodes,
        "tablassert": {
            "edges": len(tab_edges),
            "unique_nodes": len(tab_nodes),
            "rig": any(args.tablassert.glob("*.RIG.yaml")),
            "edge_fields": tab_fields,
        },
        "koza": {"edges": len(koza_edges), "node_rows": koza_node_rows, "unique_nodes": len(koza_nodes), "edge_fields": koza_fields},
        "edge_pairs": tab_edges,
    }
    if not result["edge_subject_object_pairs_equal"] or not result["unique_node_id_name_category_equal"]:
        raise SystemExit(f"resolution outputs differ: {json.dumps(result, indent=2)}")
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
