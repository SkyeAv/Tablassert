"""Typography style gate for user-facing documentation.

Guards the docs-overhaul fixes so they cannot regress: non-ASCII characters,
prose arrows, em-dash substitutes, and mid-prose double spaces are rejected in
user-facing prose. Inline code spans and fenced code blocks are exempt because
they carry real CLI flags, error text, and syntax where such characters are
legitimate.

To permit a new exception, add the exact character to ALLOWED_NON_ASCII with a
comment naming its source.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DOC_FILES = [
    REPO_ROOT / "README.md",
    REPO_ROOT / "CHANGELOG.md",
    REPO_ROOT / "CONTRIBUTING.md",
    REPO_ROOT / "CODE_OF_CONDUCT.md",
    REPO_ROOT / "llms.txt",
    *sorted((REPO_ROOT / "docs").rglob("*.md")),
]

# Exact non-ASCII characters permitted in docs prose, mapped to their source.
ALLOWED_NON_ASCII = {"ê": "Gwênlyn Glusman, author name in CITATION.cff"}

_CODE_SPAN = re.compile(r"`[^`]*`")
_ARROW = re.compile(r"->|=>|<-|→|←|⟶|⟵|↔|⇒")
_EMDASH_SUBSTITUTE = re.compile(r"\S -- \S|\S --$|^-- \S")
_DOUBLE_SPACE = re.compile(r"\S  +\S")


def _prose_lines(text: str) -> list[tuple[int, str]]:
    """Yield (line number, line) for non-fence lines, with inline code spans blanked."""
    prose: list[tuple[int, str]] = []
    in_fence = False
    for n, line in enumerate(text.splitlines(), 1):
        if line.strip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        prose.append((n, _CODE_SPAN.sub("\ue000", line)))
    return prose


def test_no_unlisted_non_ascii() -> None:
    for path in DOC_FILES:
        text = path.read_text(encoding="utf-8")
        offenders = sorted({c for c in text if ord(c) > 127 and c not in ALLOWED_NON_ASCII})
        assert not offenders, f"{path.name}: unlisted non-ASCII characters {offenders!r}"


def test_no_prose_arrows() -> None:
    for path in DOC_FILES:
        for n, line in _prose_lines(path.read_text(encoding="utf-8")):
            hit = _ARROW.search(line)
            assert hit is None, f"{path.name}:{n}: prose arrow {hit.group(0)!r}: {line.strip()[:120]}"


def test_no_em_dash_substitutes() -> None:
    for path in DOC_FILES:
        for n, line in _prose_lines(path.read_text(encoding="utf-8")):
            assert not _EMDASH_SUBSTITUTE.search(line), f"{path.name}:{n}: space-flanked -- in prose: {line.strip()[:120]}"


def test_no_mid_prose_double_spaces() -> None:
    for path in DOC_FILES:
        for n, line in _prose_lines(path.read_text(encoding="utf-8")):
            if line.startswith((" ", "|")):  # list continuations and table rows
                continue
            assert not _DOUBLE_SPACE.search(line), f"{path.name}:{n}: double space in prose: {line.strip()[:120]}"
