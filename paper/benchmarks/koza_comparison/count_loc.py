#!/usr/bin/env python3
"""Line-of-code counter for the koza comparison: no blanks, no docstrings.

Python files: docstrings are excluded via the AST (module, class, and function
docstrings), blank lines are never counted, and comment-only lines are not code.
YAML files have no docstrings; blank lines are excluded.

The Tablassert graph wrapper (tutorial-graph.yaml) is always reported as its own
line item, separate from the recipe, so the two configuration layers are visible
instead of silently summed.
"""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path


def count_python(path: Path) -> int:
    """Count non-blank, non-docstring, non-comment lines of a Python file."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    docstring_spans: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                docstring_spans.append((first.lineno, first.end_lineno or first.lineno))
    count = 0
    for lineno, line in enumerate(source.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if any(start <= lineno <= end for start, end in docstring_spans):
            continue
        count += 1
    return count


def count_yaml(path: Path) -> int:
    """Count non-blank lines of a YAML file (comments excluded)."""
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.strip().startswith("#"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", type=Path, default=Path(__file__).parent)
    parser.add_argument("--out", type=Path, help="optional JSON output path")
    args = parser.parse_args()

    counts = {
        "tablassert_table_yaml": count_yaml(args.dir / "tutorial-table.yaml"),
        "tablassert_graph_wrapper_yaml": count_yaml(args.dir / "tutorial-graph.yaml"),
        "koza_yaml": count_yaml(args.dir / "tutorial-koza.yaml"),
        "koza_transform_py": count_python(args.dir / "tutorial-transform.py"),
        "koza_resolved_yaml": count_yaml(args.dir / "tutorial-koza-resolved.yaml"),
        "koza_resolved_transform_py": count_python(args.dir / "tutorial-transform-resolved.py"),
    }
    counts["tablassert_recipe_total"] = counts["tablassert_table_yaml"] + counts["tablassert_graph_wrapper_yaml"]
    counts["koza_total"] = counts["koza_yaml"] + counts["koza_transform_py"]
    counts["koza_resolved_total"] = counts["koza_resolved_yaml"] + counts["koza_resolved_transform_py"]
    print(json.dumps(counts, indent=2))
    if args.out:
        args.out.write_text(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
