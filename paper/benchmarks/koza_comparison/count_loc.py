#!/usr/bin/env python3
"""Count configuration and code size for the Tablassert and Koza tutorial mappings.

Python docstrings, comment-only lines, and blank lines are excluded. YAML comments
and blank lines are excluded. Verbosity is non-whitespace characters on the
counted lines. The Tablassert graph wrapper is reported separately from the table
recipe.
"""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path


def python_lines(path: Path) -> list[str]:
    """Return counted lines from a Python file."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    docstring_spans: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                docstring_spans.append((first.lineno, first.end_lineno or first.lineno))
    lines: list[str] = []
    for lineno, line in enumerate(source.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or any(start <= lineno <= end for start, end in docstring_spans):
            continue
        lines.append(line)
    return lines


def yaml_lines(path: Path) -> list[str]:
    """Return counted lines from a YAML file."""
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.strip().startswith("#")]


def count_python(path: Path) -> int:
    return len(python_lines(path))


def count_yaml(path: Path) -> int:
    return len(yaml_lines(path))


def characters(lines: list[str]) -> int:
    """Count non-whitespace characters, avoiding indentation-style differences."""
    return sum(1 for line in lines for char in line if not char.isspace())


def measure(path: Path) -> dict[str, int]:
    lines = python_lines(path) if path.suffix == ".py" else yaml_lines(path)
    return {"lines": len(lines), "non_whitespace_characters": characters(lines)}


def add(left: dict[str, int], right: dict[str, int]) -> dict[str, int]:
    return {key: left[key] + right[key] for key in left}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", type=Path, default=Path(__file__).parent)
    parser.add_argument("--out", type=Path, help="optional JSON output path")
    args = parser.parse_args()

    files = {
        "tablassert_table_yaml": args.dir / "tutorial-table.yaml",
        "tablassert_graph_wrapper_yaml": args.dir / "tutorial-graph.yaml",
        "koza_yaml": args.dir / "tutorial-koza.yaml",
        "koza_transform_py": args.dir / "tutorial-transform.py",
    }
    counts = {name: measure(path) for name, path in files.items()}
    counts["tablassert_total"] = add(counts["tablassert_table_yaml"], counts["tablassert_graph_wrapper_yaml"])
    counts["koza_total"] = add(counts["koza_yaml"], counts["koza_transform_py"])
    text = json.dumps(counts, indent=2) + "\n"
    print(text, end="")
    if args.out:
        args.out.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
