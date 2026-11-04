"""Tests for scripts/bump_version.py.

The bump script edits four release surfaces (pyproject.toml, uv.lock,
CITATION.cff, CHANGELOG.md) in one shot; a bug here either publishes a version
that disagrees across files (breaking pre-commit lock checks and the PyPI tag
workflow) or silently rewrites released changelog history. These tests pin the
semver math, the per-surface edit scope (especially the targeted uv.lock
package entry), the Unreleased roll behavior, the all-or-nothing write
guarantee, and the loud-failure paths.
"""

from __future__ import annotations

import difflib
import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "bump_version.py"

_spec = importlib.util.spec_from_file_location("bump_version", SCRIPT_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover - only if the script vanishes
    raise ImportError(f"cannot load bump script at {SCRIPT_PATH}")
bump_module = importlib.util.module_from_spec(_spec)
sys.modules["bump_version"] = bump_module
_spec.loader.exec_module(bump_module)

CURRENT = "22.2.2"

PYPROJECT = f"""[project]
name = "tablassert"
version = "{CURRENT}"
description = "Biomedical spreadsheets to KGX knowledge graphs."
authors = []

[tool.ruff]
line-length = 150
"""

# A decoy package carrying the same version value proves the lock edit is
# scoped to the tablassert entry alone.
UV_LOCK = f"""version = 1
requires-python = ">=3.13"

[[package]]
name = "polars"
version = "{CURRENT}"
source = {{ registry = "https://pypi.org/simple" }}

[[package]]
name = "tablassert"
version = "{CURRENT}"
source = {{ editable = "." }}
dependencies = [
    {{ name = "polars" }},
]
"""

CITATION = f"""cff-version: 1.2.0
message: "If you use Tablassert, please cite it as below."
title: Tablassert
version: {CURRENT}
"""

CHANGELOG_WITH_UNRELEASED = f"""# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Added

- A pending feature note.

## {CURRENT} - 2026-11-04

### Changed

- An older released entry.
"""

CHANGELOG_WITHOUT_UNRELEASED = f"""# Changelog

All notable changes to this project are documented in this file.

## {CURRENT} - 2026-11-04

### Changed

- An older released entry.
"""


def write_surfaces(
    root: Path, *, changelog: str = CHANGELOG_WITHOUT_UNRELEASED, pyproject: str = PYPROJECT, uv_lock: str = UV_LOCK, citation: str = CITATION
) -> None:
    """Materialize the four version surfaces in ``root``."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "pyproject.toml").write_text(pyproject, encoding="utf-8")
    (root / "uv.lock").write_text(uv_lock, encoding="utf-8")
    (root / "CITATION.cff").write_text(citation, encoding="utf-8")
    (root / "CHANGELOG.md").write_text(changelog, encoding="utf-8")


@pytest.mark.parametrize(
    ("current", "level", "expected"),
    [
        pytest.param("22.2.2", "patch", "22.2.3", id="patch increments the third part"),
        pytest.param("22.2.2", "minor", "22.3.0", id="minor increments and resets the patch"),
        pytest.param("22.2.2", "major", "23.0.0", id="major increments and resets minor and patch"),
        pytest.param("22.2.9", "patch", "22.2.10", id="patch carries on a 9 boundary"),
        pytest.param("22.2.9", "minor", "22.3.0", id="minor resets a nonzero patch"),
        pytest.param("9.9.9", "minor", "9.10.0", id="minor carries on 9 boundaries"),
        pytest.param("9.9.9", "major", "10.0.0", id="major carries on 9 boundaries"),
    ],
)
def test_bump_version_math(current: str, level: str, expected: str) -> None:
    """The three bump levels follow semver, including carry on 9 boundaries."""
    assert bump_module.bump_version(current, level) == expected


def test_patch_bumps_every_surface(tmp_path: Path) -> None:
    """A patch bump moves all four files and nothing else in them."""
    write_surfaces(tmp_path)
    new_version = bump_module.bump("patch", "2026-12-25", root=tmp_path)
    assert new_version == "22.2.3"
    assert 'version = "22.2.3"' in (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    lock = (tmp_path / "uv.lock").read_text(encoding="utf-8")
    assert 'name = "tablassert"\nversion = "22.2.3"' in lock
    assert 'name = "polars"\nversion = "22.2.2"' in lock, "the decoy package must keep its version"
    assert "version: 22.2.3" in (tmp_path / "CITATION.cff").read_text(encoding="utf-8")
    changelog = (tmp_path / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "## 22.2.3 - 2026-12-25" in changelog


def test_lock_edit_touches_only_the_tablassert_entry(tmp_path: Path) -> None:
    """The uv.lock rewrite is a one-line change scoped to the self-package entry."""
    write_surfaces(tmp_path)
    before = (tmp_path / "uv.lock").read_text(encoding="utf-8")
    bump_module.bump("major", "2026-12-25", root=tmp_path)
    after = (tmp_path / "uv.lock").read_text(encoding="utf-8")
    assert after.count("23.0.0") == 1
    assert 'name = "polars"\nversion = "22.2.2"' in after
    changed_lines = [(old, new) for old, new in zip(before.split("\n"), after.split("\n"), strict=False) if old != new]
    assert len(changed_lines) == 1


def test_changelog_rolls_unreleased_keeping_body(tmp_path: Path) -> None:
    """An Unreleased section becomes the dated release section, body intact."""
    write_surfaces(tmp_path, changelog=CHANGELOG_WITH_UNRELEASED)
    bump_module.bump("minor", "2027-01-02", root=tmp_path)
    changelog = (tmp_path / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "## Unreleased" not in changelog
    assert "## 22.3.0 - 2027-01-02\n\n### Added\n\n- A pending feature note." in changelog
    assert "## 22.2.2 - 2026-11-04" in changelog, "released sections must stay untouched"


def test_changelog_inserts_empty_section_without_unreleased(tmp_path: Path) -> None:
    """With no Unreleased section, a fresh dated section opens above the last release."""
    write_surfaces(tmp_path, changelog=CHANGELOG_WITHOUT_UNRELEASED)
    bump_module.bump("patch", "2026-12-25", root=tmp_path)
    changelog = (tmp_path / "CHANGELOG.md").read_text(encoding="utf-8")
    assert changelog.index("## 22.2.3 - 2026-12-25") < changelog.index("## 22.2.2 - 2026-11-04")
    assert "## 22.2.2 - 2026-11-04" in changelog


def test_bump_is_all_or_nothing_on_parse_failure(tmp_path: Path) -> None:
    """A broken surface aborts the bump with every file left byte-identical."""
    write_surfaces(tmp_path, changelog=CHANGELOG_WITH_UNRELEASED)
    (tmp_path / "CITATION.cff").write_text("cff-version: 1.2.0\nno version line here\n", encoding="utf-8")
    originals = {name: (tmp_path / name).read_bytes() for name in ("pyproject.toml", "uv.lock", "CITATION.cff", "CHANGELOG.md")}
    with pytest.raises(bump_module.BumpError, match=r"CITATION\.cff"):
        bump_module.bump("patch", "2026-12-25", root=tmp_path)
    for name, content in originals.items():
        assert (tmp_path / name).read_bytes() == content, f"{name} must not be written on failure"


@pytest.mark.parametrize("bad", ["22.2", "v22.2.2", "22.2.2-beta", ""])
def test_non_semver_current_version_fails(bad: str) -> None:
    """Versions the release workflow cannot tag are rejected, not guessed around."""
    with pytest.raises(bump_module.BumpError, match=r"not strict X\.Y\.Z"):
        bump_module.bump_version(bad, "patch")


def test_missing_file_fails_loudly(tmp_path: Path) -> None:
    """Running outside a Tablassert checkout names the missing file instead of crashing."""
    write_surfaces(tmp_path)
    (tmp_path / "uv.lock").unlink()
    with pytest.raises(bump_module.BumpError, match=r"uv\.lock"):
        bump_module.bump("patch", "2026-12-25", root=tmp_path)


def test_main_rejects_unknown_level() -> None:
    """argparse rejects a level outside patch/minor/major with exit code 2."""
    with pytest.raises(SystemExit) as excinfo:
        bump_module.main(["bogus"])
    assert excinfo.value.code == 2


def test_main_writes_and_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """The CLI end-to-end: writes the tree and prints the old -> new summary."""
    write_surfaces(tmp_path)
    monkeypatch.setattr(bump_module, "PROJECT_ROOT", tmp_path)
    exit_code = bump_module.main(["major", "--date", "2026-12-25"])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "version: 22.2.2 -> 23.0.0" in out
    assert 'version = "23.0.0"' in (tmp_path / "pyproject.toml").read_text(encoding="utf-8")


def changed_ops(real_lines: list[str], bumped_lines: list[str]) -> list[tuple[str, int, int, int, int]]:
    """Return difflib opcodes that are not 'equal', keeping only real edits."""
    matcher = difflib.SequenceMatcher(None, real_lines, bumped_lines, autojunk=False)
    return [op for op in matcher.get_opcodes() if op[0] != "equal"]


def test_real_repo_files_round_trip(tmp_path: Path) -> None:
    """Smoke the script against copies of the real checkout's four files.

    Proves the parsers match the true file shapes (not just fixtures) without
    ever mutating the working tree: each copy must differ from the real file by
    exactly one edit, and that edit must be the expected version change.
    """
    for name in ("pyproject.toml", "uv.lock", "CITATION.cff", "CHANGELOG.md"):
        (tmp_path / name).write_text((REPO_ROOT / name).read_text(encoding="utf-8"), encoding="utf-8")
    new_version = bump_module.bump("patch", "2000-01-01", root=tmp_path)
    assert new_version == "22.2.3"
    for name in ("pyproject.toml", "uv.lock", "CITATION.cff"):
        ops = changed_ops((REPO_ROOT / name).read_text(encoding="utf-8").split("\n"), (tmp_path / name).read_text(encoding="utf-8").split("\n"))
        assert len(ops) == 1, f"{name}: expected one edit, got {ops}"
        tag, i1, i2, j1, j2 = ops[0]
        assert tag == "replace"
        assert i2 - i1 == 1
        assert j2 - j1 == 1, f"{name}: unexpected edit {ops[0]}"
        bumped_lines = (tmp_path / name).read_text(encoding="utf-8").split("\n")
        assert new_version in bumped_lines[j1]
    changelog_ops = changed_ops(
        (REPO_ROOT / "CHANGELOG.md").read_text(encoding="utf-8").split("\n"), (tmp_path / "CHANGELOG.md").read_text(encoding="utf-8").split("\n")
    )
    assert len(changelog_ops) == 1
    assert changelog_ops[0][0] == "insert", f"CHANGELOG.md: expected a single insertion, got {changelog_ops}"
    real = (REPO_ROOT / "CHANGELOG.md").read_text(encoding="utf-8").split("\n")
    first_heading = next(i for i, line in enumerate(real) if line.startswith("## "))
    heading_block = [f"## {new_version} - 2000-01-01", ""]
    expected = [*real[:first_heading], *heading_block, *real[first_heading:]]
    assert (tmp_path / "CHANGELOG.md").read_text(encoding="utf-8").split("\n") == expected
