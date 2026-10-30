"""JOSS paper gate: structure, citations, and word budgets for paper/paper.md.

Mirrors tests/test_docs_style.py: plain functions over explicit file paths, no fixtures,
standard-library imports only (the pre-commit hook runs this file alone, so it must stay
import-light). Enforces the structural requirements Whedon compilation and JOSS review
impose so collaborators get fast local feedback: front matter carries title, authors
with ORCIDs, and tags; the body carries every JOSS-required section (JOSS paper-format
guidance, 2026); every [@key] citation resolves to a paper.bib entry and every entry is
cited; the abstract and body stay inside the official JOSS word budgets.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PAPER_MD = REPO_ROOT / "paper" / "paper.md"
PAPER_BIB = REPO_ROOT / "paper" / "paper.bib"

# Exact non-ASCII characters permitted in the paper, mapped to their source.
ALLOWED_NON_ASCII = {"ê": "Gwênlyn Glusman, author name in CITATION.cff"}

# JOSS guidance: "The paper should be between 750-1750 words" (body, excluding
# references); the leading unheaded abstract paragraph is capped separately.
BODY_WORD_LIMIT = 1750
ABSTRACT_WORD_LIMIT = 250

REQUIRED_SECTIONS = (
    "Summary",
    "Statement of need",
    "State of the field",
    "Software design",
    "Research impact statement",
    "AI usage disclosure",
    "Acknowledgements",
    "References",
)

_BIB_ENTRY = re.compile(r"^@\w+\s*\{([^,\s]+),", re.MULTILINE)
_CITED_GROUP = re.compile(r"\[@([^\]]*)\]")
_ORCID = re.compile(r"\b\d{4}-\d{4}-\d{4}-\d{3}[\dX]\b")


def _split_front_matter(text: str) -> tuple[str, str]:
    """Split the leading YAML front matter block (--- ... ---) from the body."""
    if not text.startswith("---\n"):
        raise ValueError("paper.md must start with a YAML front matter block")
    try:
        end = text.index("\n---\n", 3)
    except ValueError:
        raise ValueError("paper.md front matter is missing its closing --- line") from None
    return text[4:end], text[end + 5 :]


def _yaml_children(front_matter: str, key: str) -> list[str]:
    """Return the indented child lines under a top-level YAML key."""
    lines = front_matter.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.startswith(f"{key}:"))
    except StopIteration:
        raise ValueError(f"front matter is missing the {key} key") from None
    children: list[str] = []
    for line in lines[start + 1 :]:
        if line and not line.startswith((" ", "\t")):
            break
        children.append(line)
    return children


def _author_blocks(front_matter: str) -> list[list[str]]:
    """Group the authors section into one block per `- name:` entry."""
    blocks: list[list[str]] = []
    for line in _yaml_children(front_matter, "authors"):
        if line.lstrip().startswith("- "):
            blocks.append([line])
        elif blocks:
            blocks[-1].append(line)
    return blocks


def _strip_fences(body: str) -> str:
    """Drop fenced code blocks, which do not count toward prose word budgets."""
    lines: list[str] = []
    fence = False
    for line in body.splitlines():
        if line.strip().startswith("```"):
            fence = not fence
            continue
        if not fence:
            lines.append(line)
    return "\n".join(lines)


def _section(body: str, title: str) -> str:
    """Return the prose of one top-level section, excluding its heading."""
    match = re.search(rf"^#+\s+{re.escape(title)}\s*$", body, re.MULTILINE)
    if match is None:
        return ""
    rest = body[match.end() :]
    nxt = re.search(r"^#+\s", rest, re.MULTILINE)
    return rest[: nxt.start()] if nxt else rest


def _before_references(body: str) -> str:
    """Drop everything from the References heading onward (bibliography is rendered)."""
    match = re.search(r"^#+\s*References\s*$", body, re.MULTILINE)
    return body[: match.start()] if match else body


def _cited_keys(text: str) -> set[str]:
    """Collect every citation key inside [@key] / [@!key] / [@a; @b] brackets."""
    keys: set[str] = set()
    for group in _CITED_GROUP.findall(text):
        for part in group.split(";"):
            key = part.strip().lstrip("@").lstrip("!")
            if key:
                keys.add(key)
    return keys


def _bib_keys(text: str) -> set[str]:
    return set(_BIB_ENTRY.findall(text))


def test_front_matter_complete() -> None:
    front_matter, _ = _split_front_matter(PAPER_MD.read_text(encoding="utf-8"))
    assert re.search(r"^title:\s*\S", front_matter, re.MULTILINE), "front matter needs a title"
    assert re.search(r"^bibliography:\s*paper\.bib\s*$", front_matter, re.MULTILINE), (
        "front matter needs bibliography: paper.bib so pandoc citeproc loads the entries"
    )
    assert _yaml_children(front_matter, "tags"), "front matter needs at least one tag"
    blocks = _author_blocks(front_matter)
    assert blocks, "front matter needs at least one author"
    for block in blocks:
        assert any("name:" in line for line in block), "every author needs a name"
        assert any(_ORCID.search(line) for line in block), f"every author needs an ORCID: {block}"


def test_required_sections_present() -> None:
    _, body = _split_front_matter(PAPER_MD.read_text(encoding="utf-8"))
    headings = {match.group(1).strip() for match in re.finditer(r"^#+\s+(.+?)\s*$", body, re.MULTILINE)}
    missing = [section for section in REQUIRED_SECTIONS if section not in headings]
    assert not missing, f"paper.md is missing JOSS-required sections: {missing}"


def test_citations_resolve() -> None:
    text = PAPER_MD.read_text(encoding="utf-8")
    bib = _bib_keys(PAPER_BIB.read_text(encoding="utf-8"))
    unresolved = _cited_keys(text) - bib
    assert not unresolved, f"citations missing from paper.bib: {sorted(unresolved)}"


def test_no_uncited_entries() -> None:
    text = PAPER_MD.read_text(encoding="utf-8")
    bib = _bib_keys(PAPER_BIB.read_text(encoding="utf-8"))
    dead = bib - _cited_keys(text)
    assert not dead, f"paper.bib entries never cited in paper.md: {sorted(dead)}"


def test_abstract_within_budget() -> None:
    _, body = _split_front_matter(PAPER_MD.read_text(encoding="utf-8"))
    abstract = _section(_strip_fences(body), "Summary")
    words = len(abstract.split())
    assert words > 0, "paper.md needs a Summary section with abstract paragraphs"
    assert words <= ABSTRACT_WORD_LIMIT, f"abstract is {words} words (max {ABSTRACT_WORD_LIMIT})"


def test_body_within_budget() -> None:
    _, body = _split_front_matter(PAPER_MD.read_text(encoding="utf-8"))
    prose = _before_references(_strip_fences(body))
    words = len(prose.split())
    assert words >= 750, f"body is {words} words (JOSS minimum 750)"
    assert words <= BODY_WORD_LIMIT, f"body is {words} words (max {BODY_WORD_LIMIT})"


def test_ascii_only() -> None:
    text = PAPER_MD.read_text(encoding="utf-8")
    offenders = sorted({c for c in text if ord(c) > 127 and c not in ALLOWED_NON_ASCII})
    assert not offenders, f"paper.md: unlisted non-ASCII characters {offenders!r}"
