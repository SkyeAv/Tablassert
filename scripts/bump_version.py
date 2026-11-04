#!/usr/bin/env python3
"""Bump the Tablassert release version everywhere the release workflow cares.

Usage: bump_version.py {patch,minor,major} [--date YYYY-MM-DD]

Updates four files, all-or-nothing (every new content is computed before any
write, so a parse failure leaves the tree untouched):

- pyproject.toml  the ``version = "..."`` line inside ``[project]``. This is the
  canonical source: tag-version.yml watches it on main and creates vX.Y.Z,
  which then triggers the PyPI publish workflow.
- uv.lock         the ``version = "..."`` line of the ``[[package]]`` block whose
  name is ``tablassert`` (the self-package entry). A stale lock entry here makes
  every local pre-commit run reconcile the lock and abort the commit.
- CITATION.cff    the ``version:`` line.
- CHANGELOG.md    rolls a ``## Unreleased`` heading into ``## NEW - DATE`` keeping
  its body; when no Unreleased section exists, inserts an empty ``## NEW - DATE``
  section before the first ``##`` heading. Release note prose is never generated.

rust/Cargo.toml is deliberately untouched: the tablassert-rs crate is versioned
independently of the release and no chore(release) commit has ever bumped it.
"""

from __future__ import annotations

import argparse
import datetime
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")
PYPROJECT_VERSION_RE = re.compile(r'^version = "[^"]+"$', re.MULTILINE)
# The lock entry is uniquely identified by its [[package]] header + name line so
# an unrelated package that happens to carry the same version value never moves.
LOCK_ENTRY_RE = re.compile(r'\[\[package\]\]\nname = "tablassert"\nversion = "[^"]+"')
CITATION_VERSION_RE = re.compile(r"^version: \d+\.\d+\.\d+$", re.MULTILINE)


class BumpError(Exception):
    """Raised when a version surface cannot be parsed; the caller exits non-zero."""


def bump_version(current: str, level: str) -> str:
    """Apply a semver bump to a strict ``X.Y.Z`` string."""
    if not SEMVER_RE.fullmatch(current):
        raise BumpError(f"pyproject.toml version {current!r} is not strict X.Y.Z; release versions here are plain three-part numbers")
    major, minor, patch = (int(part) for part in current.split("."))
    if level == "patch":
        patch += 1
    elif level == "minor":
        minor += 1
        patch = 0
    else:
        major += 1
        minor = 0
        patch = 0
    return f"{major}.{minor}.{patch}"


def new_pyproject(text: str, new_version: str) -> str:
    """Rewrite the ``version`` key inside the ``[project]`` table only."""
    lines = text.split("\n")
    hits: list[int] = []
    in_project = False
    for index, line in enumerate(lines):
        if line == "[project]":
            in_project = True
            continue
        if in_project and line.startswith("["):
            break
        if in_project and PYPROJECT_VERSION_RE.fullmatch(line):
            hits.append(index)
    if len(hits) != 1:
        raise BumpError(f"expected exactly one version key in [project], found {len(hits)}")
    lines[hits[0]] = f'version = "{new_version}"'
    return "\n".join(lines)


def new_uv_lock(text: str, new_version: str) -> str:
    """Rewrite the self-package entry's version line, and nothing else."""
    updated, count = LOCK_ENTRY_RE.subn(f'[[package]]\nname = "tablassert"\nversion = "{new_version}"', text)
    if count != 1:
        raise BumpError(f"expected exactly one tablassert [[package]] entry in uv.lock, found {count}")
    return updated


def new_citation(text: str, new_version: str) -> str:
    """Rewrite the ``version:`` line of CITATION.cff."""
    updated, count = CITATION_VERSION_RE.subn(f"version: {new_version}", text)
    if count != 1:
        raise BumpError(f"expected exactly one version line in CITATION.cff, found {count}")
    return updated


def new_changelog(text: str, new_version: str, release_date: str) -> str:
    """Roll ``## Unreleased`` into a dated section, or open a fresh empty one."""
    heading = f"## {new_version} - {release_date}"
    lines = text.split("\n")
    for index, line in enumerate(lines):
        if line.strip() == "## Unreleased":
            lines[index] = heading
            return "\n".join(lines)
    for index, line in enumerate(lines):
        if line.startswith("## "):
            lines[index:index] = [heading, ""]
            return "\n".join(lines)
    return text.rstrip("\n") + "\n\n" + heading + "\n"


def bump(level: str, release_date: str, root: Path | None = None) -> str:
    """Bump every release surface under ``root``; return the new version."""
    root = PROJECT_ROOT if root is None else root
    surfaces = {"pyproject.toml": new_pyproject, "uv.lock": new_uv_lock, "CITATION.cff": new_citation}
    contents: dict[str, str] = {}
    for name in surfaces:
        path = root / name
        if not path.is_file():
            raise BumpError(f"missing required file: {path}")
        contents[name] = path.read_text(encoding="utf-8")

    current_match = PYPROJECT_VERSION_RE.search(contents["pyproject.toml"])
    if current_match is None:
        raise BumpError("pyproject.toml has no version key")
    current = current_match.group(0).split('"')[1]
    new_version = bump_version(current, level)

    changelog_path = root / "CHANGELOG.md"
    if not changelog_path.is_file():
        raise BumpError(f"missing required file: {changelog_path}")
    changelog = changelog_path.read_text(encoding="utf-8")

    # Compute every transformed content first: a parse error on any surface
    # must abort before the first byte is written.
    transformed = {name: transform(contents[name], new_version) for name, transform in surfaces.items()}
    transformed_changelog = new_changelog(changelog, new_version, release_date)
    for name in surfaces:
        (root / name).write_text(transformed[name], encoding="utf-8")
    changelog_path.write_text(transformed_changelog, encoding="utf-8")

    print(f"version: {current} -> {new_version}")
    for name in surfaces:
        print(f"  updated {name}")
    print("  rolled CHANGELOG.md")
    return new_version


def parse_date(value: str) -> str:
    """Validate an ISO date string passed via --date."""
    try:
        return datetime.date.fromisoformat(value).isoformat()
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}: {error}") from error


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("level", choices=("patch", "minor", "major"), help="which version part to increment")
    parser.add_argument("--date", type=parse_date, default=datetime.date.today().isoformat(), help="changelog date (ISO, default today)")
    args = parser.parse_args(argv)
    try:
        bump(args.level, args.date)
    except BumpError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
