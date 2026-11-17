"""JOSS paper gate: structure, citations, and word budgets for paper/paper.md.

Mirrors tests/test_docs_style.py: plain functions over explicit file paths, no fixtures,
standard-library imports only (the pre-commit hook runs this file alone, so it must stay
import-light). Enforces the structural requirements Whedon compilation and JOSS review
impose so collaborators get fast local feedback: front matter carries title, authors
with ORCIDs, and bibliography metadata; the body carries every JOSS-required section (JOSS paper-format
guidance, 2026); every [@key] citation resolves to a paper.bib entry and every entry is
cited; the abstract and rendered body prose stay inside the official JOSS word budgets.
"""

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PAPER_MD = REPO_ROOT / "paper" / "paper.md"
PAPER_BIB = REPO_ROOT / "paper" / "paper.bib"

# Exact non-ASCII characters permitted in the paper, mapped to their source.
ALLOWED_NON_ASCII = {"ê": "Gwênlyn Glusman, author name in CITATION.cff"}

# The JOSS submission bot (openjournals/gh-action-repo-checks, checks.rb) is what
# actually flags a paper on the review issue. It runs `cat paper.md | wc -w` over the
# WHOLE file - YAML front matter, Markdown/LaTeX markup, and the References heading
# included - then, for 2026+ submissions, marks >1800 words as a warning and >1999 as
# an alarm. That raw count is what authors see, so the ceiling below mirrors it.
# JOSS prose guidance still says "750-1750 words"; the 750 floor is kept on visible
# prose, where it is meaningful, rather than on the raw token count.
BOT_WORD_LIMIT = 1800
BOT_ALARM_LIMIT = 1999
BODY_WORD_MINIMUM = 750
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

# Approximate the rendered manuscript rather than raw Markdown. The official
# submission check uses a crude whole-file `wc -w`, but this local gate is meant
# to count visible body prose: YAML, References, code blocks, image paths,
# labels, and Markdown syntax are excluded. Each citation group is counted as one
# rendered citation because expanding it would require parsing BibTeX styles.
_HTML_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
_TABLE_SEPARATOR = re.compile(r"^[ \t]*\|?[ \t:|-]+\|[ \t:|-]*$", re.MULTILINE)
_IMAGE = re.compile(r"!\[([^\]]*)\]\([^)]*\)(?:\{[^}]*\})?")
_LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)(?:\{[^}]*\})?")
_SPAN = re.compile(r"\[([^\]]*)\]\{[^}]*\}")
_LABEL = re.compile(r"\\label\{[^}]*\}")
_FLOAT_REF = re.compile(r"\\(auto)?ref\{[^}]*\}")
_CITATION_GROUP = re.compile(r"\[@[^\]]*\]")
_HEADING_MARKER = re.compile(r"^#{1,6}[ \t]+", re.MULTILINE)
_LIST_MARKER = re.compile(r"^[ \t]*(?:[-*+]|\d+\.)[ \t]+", re.MULTILINE)
_BLOCKQUOTE_MARKER = re.compile(r"^[ \t]*>[ \t]?", re.MULTILINE)
_WHITESPACE = re.compile(r"\s+")


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
    """Group the authors section into one block per `- name:` entry.

    Also accepts a compact inline flow list (`authors: [{"name":...},...]`), which
    JOSS parses identically but which counts fewer words in whole-file `wc -w`.
    """
    inline = re.search(r"^authors:[ \t]*(\[.*\])[ \t]*$", front_matter, re.MULTILINE)
    if inline:
        return [[f"name: {a.get('name', '')}", f"orcid: {a.get('orcid', '')}"] for a in json.loads(inline.group(1))]
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


def _strip_markup(text: str) -> str:
    """Reduce Markdown and JOSS-specific syntax to approximate rendered prose."""
    text = _HTML_COMMENT.sub(" ", text)
    text = _TABLE_SEPARATOR.sub(" ", text)
    text = _IMAGE.sub(r" \1 ", text)
    text = _LINK.sub(r" \1 ", text)
    text = _SPAN.sub(r" \1 ", text)
    text = _LABEL.sub(" ", text)
    text = _FLOAT_REF.sub(lambda match: "figure 1" if match.group(1) else "1", text)
    text = _CITATION_GROUP.sub(" citation ", text)
    text = _HEADING_MARKER.sub("", text)
    text = _LIST_MARKER.sub("", text)
    text = _BLOCKQUOTE_MARKER.sub("", text)
    text = text.replace("`", " ").replace("*", " ").replace("|", " ")
    return _WHITESPACE.sub(" ", text).strip()


def _visible_words(text: str) -> int:
    """Count words in approximately rendered visible prose."""
    return sum(1 for token in _strip_markup(text).split() if any(char.isalnum() for char in token))


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


def test_inline_author_and_citation_forms_parse() -> None:
    front_matter, body = _split_front_matter(
        '---\ntitle: t\nauthors: [{"name":"A B","affiliation":1,"orcid":"0000-0001-2345-6789"}]\n---\n\nCited [@a;@b].\n'
    )
    blocks = _author_blocks(front_matter)
    assert blocks == [["name: A B", "orcid: 0000-0001-2345-6789"]]
    assert _cited_keys(body) == {"a", "b"}


def test_front_matter_complete() -> None:
    front_matter, _ = _split_front_matter(PAPER_MD.read_text(encoding="utf-8"))
    assert re.search(r"^title:\s*\S", front_matter, re.MULTILINE), "front matter needs a title"
    assert re.search(r"^bibliography:\s*paper\.bib\s*$", front_matter, re.MULTILINE), (
        "front matter needs bibliography: paper.bib so pandoc citeproc loads the entries"
    )
    assert not re.search(r"^tags:", front_matter, re.MULTILINE), "front matter must omit tags"
    blocks = _author_blocks(front_matter)
    assert blocks, "front matter needs at least one author"
    for block in blocks:
        assert any("name:" in line for line in block), "every author needs a name"
        assert any(_ORCID.search(line) for line in block), f"every author needs an ORCID: {block}"


def test_required_sections_present() -> None:
    _, body = _split_front_matter(PAPER_MD.read_text(encoding="utf-8"))
    headings = {match.group(1).strip().casefold() for match in re.finditer(r"^#+\s+(.+?)\s*$", body, re.MULTILINE)}
    missing = [section for section in REQUIRED_SECTIONS if section.casefold() not in headings]
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


def test_visible_word_count_ignores_nonrendered_markup() -> None:
    sample = (
        "---\n"
        "title: Hidden Metadata\n"
        "---\n\n"
        "# Heading\n\n"
        'Visible ![**Caption.** text \\label{fig:sample}](image.png){height="9pt"} '
        "[@a; @b] \\ref{fig:sample} `code` [link text](https://example.com).\n\n"
        "# References\n\n"
        "ignored [@hidden]\n"
    )
    _, body = _split_front_matter(sample)
    prose = _before_references(_strip_fences(body))
    assert _strip_markup(prose) == "Heading Visible Caption. text citation 1 code link text ."
    assert _visible_words(prose) == 9


def test_abstract_within_budget() -> None:
    _, body = _split_front_matter(PAPER_MD.read_text(encoding="utf-8"))
    abstract = _section(_strip_fences(body), "Summary")
    words = _visible_words(abstract)
    assert words > 0, "paper.md needs a Summary section with abstract paragraphs"
    assert words <= ABSTRACT_WORD_LIMIT, f"abstract is {words} words (max {ABSTRACT_WORD_LIMIT})"


def test_body_above_minimum() -> None:
    _, body = _split_front_matter(PAPER_MD.read_text(encoding="utf-8"))
    prose = _before_references(_strip_fences(body))
    words = _visible_words(prose)
    assert words >= BODY_WORD_MINIMUM, f"body is {words} visible words (JOSS minimum {BODY_WORD_MINIMUM})"


def test_bot_word_count() -> None:
    """Fail when the JOSS submission bot would flag paper.md for length.

    Reproduces the bot's metric exactly: raw whitespace-split tokens over the whole
    file. The alarm threshold is asserted first so the more severe breach is reported.
    """
    words = len(PAPER_MD.read_text(encoding="utf-8").split())
    assert words <= BOT_ALARM_LIMIT, f"paper.md is {words} raw words; the JOSS bot alarms above {BOT_ALARM_LIMIT}"
    assert words <= BOT_WORD_LIMIT, f"paper.md is {words} raw words; the JOSS bot warns above {BOT_WORD_LIMIT}"


def test_ascii_only() -> None:
    text = PAPER_MD.read_text(encoding="utf-8")
    offenders = sorted({c for c in text if ord(c) > 127 and c not in ALLOWED_NON_ASCII})
    assert not offenders, f"paper.md: unlisted non-ASCII characters {offenders!r}"
