"""Offline tests for the Koza configuration-size comparison."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

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
