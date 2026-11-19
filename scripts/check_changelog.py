#!/usr/bin/env python3
"""Require a new, substantive changelog entry on every pull request.

Compares CHANGELOG.md against the merge base of the current HEAD and the base
branch and fails when the pull request adds no bullet under the ``##
Unreleased`` section. Changelog entries are easiest to write and most accurate
when they accompany the changes they describe, so the check travels with every
PR (CI runs it on all pull requests, including forks) instead of relying on
review-time reminders.

What counts as a new entry:
- a diff-added line inside the head's ``## Unreleased`` section whose stripped
  form starts with ``-`` and carries text;
- whitespace, ``###`` subheadings, and bullets added to already released
  sections never satisfy the check.

Exemptions: release-bump and dependency-bump pull requests are skipped when the
PR title or the HEAD commit subject starts with ``chore(release):`` or
``chore(deps):`` (a release roll legitimately empties the Unreleased section;
override with ``--exempt-prefix``). Documentation-only pull requests -- every changed
path is markdown, under ``docs/``, or the docs index/config files -- are exempt too via
``--exempt-docs-only``, since their summary is the change itself.

Exit codes: 0 satisfied or explicitly skipped, 1 requirement violated, 2
plumbing error (git failure or unresolvable base ref) so unknown states fail
loudly instead of passing.

Usage:
    check_changelog.py [--base origin/main] [--repo PATH] [--file CHANGELOG.md]
                       [--pr-title TITLE] [--exempt-prefix PREFIX]...
                       [--exempt-docs-only] [--fetch-base]
"""

from __future__ import annotations

import argparse
import difflib
import re
import subprocess
import sys
from pathlib import Path

DEFAULT_BASE = "origin/main"
DEFAULT_EXEMPT_PREFIXES = ("chore(release):", "chore(deps):")
# Mirrors the documentation notion of the CI path filters: content files only
# (markdown anywhere, the docs tree, the docs nav and index). Lint configs such as
# .markdownlint-cli2.jsonc stay outside: they are tooling, not documentation.
DOCS_PATH_RE = re.compile(r"(?:^|/)[^/]+\.md$|^docs/|^mkdocs\.yml$|^llms\.txt$")


class ChangelogCheckError(Exception):
    """Plumbing failure (git or filesystem); the caller must exit non-zero."""


def added_lines(base_lines: list[str], head_lines: list[str]) -> list[tuple[int, str]]:
    """Return the diff's head-side additions as (zero-based index, text).

    Zero-based indices line up with positions in ``head_lines`` so callers can
    test membership in a section span without an off-by-one.
    """
    matcher = difflib.SequenceMatcher(None, base_lines, head_lines, autojunk=False)
    return [(j, head_lines[j]) for tag, _, _, j1, j2 in matcher.get_opcodes() if tag in ("insert", "replace") for j in range(j1, j2)]


def unreleased_span(head_lines: list[str]) -> tuple[int, int] | None:
    """Return the half-open line span of the Unreleased body, or None.

    The span starts just after the ``## Unreleased`` heading and ends at the
    next ``##``-level heading (subsection headings live inside the section).
    """
    for index, line in enumerate(head_lines):
        if line.strip() == "## Unreleased":
            end = next((i for i in range(index + 1, len(head_lines)) if head_lines[i].startswith("## ")), len(head_lines))
            return index + 1, end
    return None


def is_bullet(line: str) -> bool:
    """A changelog entry is a ``- `` bullet carrying text (not a bare dash)."""
    stripped = line.strip()
    return stripped.startswith("-") and len(stripped) > 1


def new_bullets(base_text: str, head_text: str) -> list[str]:
    """Bulleted lines the PR adds inside the head's Unreleased section."""
    head_lines = head_text.split("\n")
    span = unreleased_span(head_lines)
    if span is None:
        return []
    start, end = span
    return [text for index, text in added_lines(base_text.split("\n"), head_lines) if start <= index < end and is_bullet(text)]


def is_docs_path(path: str) -> bool:
    """Whether one changed path is documentation (markdown, docs tree, docs index/config)."""
    return DOCS_PATH_RE.search(path) is not None


def is_docs_only(changed: list[str]) -> bool:
    """A docs-only change touches at least one path, and every path is documentation."""
    return bool(changed) and all(is_docs_path(path) for path in changed)


def evaluate(
    base_text: str,
    head_text: str,
    pr_title: str | None,
    commit_subject: str,
    exempt_prefixes: tuple[str, ...],
    changed_files: list[str] | None = None,
    exempt_docs_only: bool = False,
) -> tuple[int, str]:
    """Pure check core: return (exit code, message) for the two changelog texts.

    ``changed_files`` (paths the pull request touches) enables the docs-only
    exemption when ``exempt_docs_only`` is set: a change whose every path is
    documentation is its own summary, so it does not need a changelog bullet.
    """
    for label, text in (("PR title", pr_title), ("HEAD commit subject", commit_subject)):
        if text:
            for prefix in exempt_prefixes:
                if text.startswith(prefix):
                    return 0, f"changelog check: skipped - {label} starts with {prefix!r}"
    if exempt_docs_only and is_docs_only(changed_files or []):
        return 0, f"changelog check: skipped - docs-only pull request ({len(changed_files or [])} documentation path(s))"
    if unreleased_span(head_text.split("\n")) is None:
        return (
            1,
            "changelog check: FAIL - CHANGELOG.md has no '## Unreleased' section.\n"
            "Every pull request must document its change under '## Unreleased' in CHANGELOG.md:\n"
            "add the section (above the most recent release heading) with a '- ...' bullet.\n"
            "See CONTRIBUTING.md.",
        )
    bullets = new_bullets(base_text, head_text)
    if not bullets:
        return (
            1,
            "changelog check: FAIL - no new substantive entry under '## Unreleased' in CHANGELOG.md.\n"
            "Add at least one bullet describing this pull request's change under '## Unreleased'\n"
            "(a line starting with '- '). Whitespace, '###' subheadings, and edits to already\n"
            "released sections do not satisfy the check. See CONTRIBUTING.md.",
        )
    shown = bullets[0].strip()
    return 0, f"changelog check: PASS - {len(bullets)} new Unreleased bullet(s), first: {shown}"


def git_out(repo: Path, args: list[str]) -> str:
    """Run a git command in ``repo`` and return stripped stdout."""
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)
    if result.returncode != 0:
        raise ChangelogCheckError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def file_at_commit(repo: Path, ref: str, path: str) -> str | None:
    """A file's content at ``ref``, or None when it does not exist there."""
    result = subprocess.run(["git", "-C", str(repo), "cat-file", "-e", f"{ref}:{path}"], capture_output=True)
    if result.returncode != 0:
        return None
    return git_out(repo, ["show", f"{ref}:{path}"])


def merge_base(repo: Path, base: str) -> str:
    """Resolve the merge base of ``base`` and HEAD, with actionable guidance."""
    result = subprocess.run(["git", "-C", str(repo), "merge-base", base, "HEAD"], capture_output=True, text=True)
    if result.returncode != 0:
        raise ChangelogCheckError(f"cannot resolve merge base of {base!r} and HEAD (run 'git fetch origin main' and retry): {result.stderr.strip()}")
    return result.stdout.strip()


def changed_files(repo: Path, base_ref: str) -> list[str]:
    """Paths the branch changes relative to the merge base (committed work only)."""
    return [line for line in git_out(repo, ["diff", "--name-only", base_ref, "HEAD"]).split("\n") if line]


def fetch_base(repo: Path, base: str) -> bool:
    """Fetch the branch behind a remote-tracking ``base`` ref; False when offline."""
    branch = base.removeprefix("origin/")
    result = subprocess.run(["git", "-C", str(repo), "fetch", "--no-tags", "origin", branch], capture_output=True, text=True)
    return result.returncode == 0


def build_parser() -> argparse.ArgumentParser:
    """CLI surface; split out so the hook and CI share one definition."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--base", default=DEFAULT_BASE, help="git ref to diff against (default: origin/main)")
    parser.add_argument("--repo", default=".", help="repository working tree (default: current directory)")
    parser.add_argument("--file", default="CHANGELOG.md", help="changelog path relative to the repo root")
    parser.add_argument("--pr-title", default=None, help="pull request title; also exempts on prefix match")
    parser.add_argument("--exempt-prefix", action="extend", default=[], help="commit/PR title prefixes to skip (repeatable)")
    parser.add_argument("--exempt-docs-only", action="store_true", help="skip documentation-only pull requests (every changed path is docs)")
    parser.add_argument("--fetch-base", action="store_true", help="fetch the base branch first; skip loudly when it fails")
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = build_parser().parse_args(argv)
    repo = Path(args.repo)
    prefixes = tuple(args.exempt_prefix) if args.exempt_prefix else DEFAULT_EXEMPT_PREFIXES
    if args.fetch_base and not fetch_base(repo, args.base):
        print(f"changelog check: skipped - could not fetch {args.base!r}; CI enforces this check on the pull request.", file=sys.stderr)
        return 0
    try:
        base_ref = merge_base(repo, args.base)
        base_text = file_at_commit(repo, base_ref, args.file) or ""
        changed = changed_files(repo, base_ref)
        head_path = repo / args.file
        head_text = head_path.read_text(encoding="utf-8") if head_path.is_file() else ""
        commit_subject = git_out(repo, ["log", "-1", "--pretty=%s"])
    except ChangelogCheckError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    code, message = evaluate(
        base_text, head_text, args.pr_title, commit_subject, prefixes, changed_files=changed, exempt_docs_only=args.exempt_docs_only
    )
    print(message, file=sys.stderr if code else sys.stdout)
    return code


if __name__ == "__main__":
    sys.exit(main())
