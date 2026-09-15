"""Source-of-truth guards for documentation YAML configs and the documentation index lists.

``tests/test_docs_examples.py`` guards the YAML files shipped under ``docs/examples/``, but the
configs readers copy first live in prose: the README quick start, the tutorial listings, and the
use-case gallery. Those blocks were never validated, so the README quick start drifted -- it
declared ``source.url`` as a bare string against ``models.BaseSource.url: list[HttpUrl]`` -- and
nothing failed. This module closes that gap and the matching navigation gap (a page added to
``mkdocs.yml`` nav without being linked from the README or the docs index).

Every expectation is derived from live source rather than copied out of the docs:

* YAML blocks are lifted from the Markdown itself and classified by shape, so a new complete
  config anywhere in ``README.md`` / ``docs/**/*.md`` is guarded automatically and an abbreviated
  or partial snippet is skipped by rule -- there is no hand-maintained include/skip list.
* Validation runs the exact paths the CLI runs: ``ingests.to_sections`` -> ``Tcode.model_validate``
  for a table (what ``tablassert validate --schema table`` does) and ``models.Graph.model_validate``
  for a graph (what ``--schema graph`` does first).
* The index lists are checked against the live ``mkdocs.yml`` nav and ``site_url``.
* The configuration references (``docs/configuration/table.md`` / ``graph.md``) are checked
  bidirectionally against the live Pydantic config models: every field row in a docs table must
  exist on its bound model (``extra="forbid"`` makes a phantom row a config-time error the docs
  would wrongly bless), and every live field must be documented. Field sets come from
  ``model.model_fields`` and from the parsed tables at test time -- never from copied lists.
* The API references (``docs/api/*.md``) have the first ```python block under every
  ``### Function Signature`` heading checked parameter-by-parameter against the live callable
  (``inspect.signature``), or against ``src/tablassert/rs.pyi`` for the Rust extension: parameter
  names, order, positional-only/keyword-only kind, and defaults (annotations are NOT compared;
  the docs spell optionality ``Optional[...]`` where the source uses ``X | None``).
* The optional-extra enumerations (the README extras table, the installation guide's Optional
  Extras table and preflight section, and both ``llms.txt`` lists) are checked against the live
  ``[project.optional-dependencies]`` table -- parsed with stdlib ``tomllib``, the sole extra
  authority -- and against the live ``extras.require`` / ``extras.is_installed`` call sites under
  ``src/tablassert`` (an AST walk, never a copied list).
* The contributor guide (``CONTRIBUTING.md``) is checked against the live ``Makefile``
  (``.PHONY`` targets), ``.github/workflows/ci.yml`` (the no-sharding decision and ``SYNC_ARGS``),
  ``.pre-commit-config.yaml`` (``default_install_hook_types``), ``pyproject.toml`` pytest addopts,
  and the filesystem for the paths listed in the Project layout block, with rotting hardcoded counts banned by rule.
"""

from __future__ import annotations

import ast
import inspect
import re
import tomllib
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from importlib.util import find_spec
from pathlib import Path
from typing import Annotated, Any, Literal, get_args, get_origin, get_type_hints

import cyclopts  # pyright: ignore[reportMissingImports]
import polars as pl
import pytest
import yaml
from pydantic import ValidationError
from pydantic.fields import FieldInfo
from yaml import CSafeLoader

import tablassert.biolink as biolink_api
import tablassert.cli as cli_module
import tablassert.enums as enums_api
import tablassert.fullmap as fullmap_api
import tablassert.lib as lib_api
import tablassert.log as log_module
import tablassert.models as config_models
import tablassert.qc as qc_api
import tablassert.utils as utils_module
from tablassert.cli import APP
from tablassert.errors import BiolinkRelocationWarning, UnpairedEffectAnnotationWarning
from tablassert.fullmap import filter_and_rank, join_matches
from tablassert.ingests import to_sections
from tablassert.lib import Tcode
from tablassert.models import (
    Annotation,
    Encoding,
    Excel,
    Graph,
    ManualProvenance,
    NodeEncoding,
    Provenance,
    Qualifier,
    Reindex,
    RIGConfig,
    RIGIngestInfo,
    RIGProvenanceInfo,
    RIGSourceInfo,
    SourceOverride,
    Statement,
    TablaBase,
    Text,
)

# Repository root (parent of tests/).
ROOT: Path = Path(__file__).resolve().parent.parent
DOCS: Path = ROOT / "docs"
README: Path = ROOT / "README.md"
MKDOCS: Path = ROOT / "mkdocs.yml"

# The docs index page whose "Documentation Sections" list must mirror the mkdocs nav. The README
# carries the same list in published-site-URL style; both are the site's Home surface, which is why
# the nav Home entry is not linked from either.
INDEX: Path = DOCS / "index.md"
# `Tcode` requires a `store` path but never touches it during validation, so one stable name keeps
# the parametrization deterministic and xdist-safe (no worker writes or reads it).
STORE: Path = ROOT / ".tablassert" / "store" / "docs-source-of-truth.parquet"

ConfigKind = Literal["graph", "table"]

# Keys whose presence makes a parsed block a COMPLETE config for one live schema. Mirrors the
# required fields of `models.Section` (source/statement/provenance) and `models.Graph`.
TABLE_KEYS: frozenset[str] = frozenset({"provenance", "source", "statement"})
GRAPH_KEYS: frozenset[str] = frozenset({"fullmap", "name", "rig", "tables", "version"})
# Elision markers. A block carrying one is an excerpt by construction and cannot validate.
PLACEHOLDER_TOKENS: tuple[str, ...] = ("{...}", "...")

FENCED_YAML: re.Pattern[str] = re.compile(r"^```(?:yaml|yml)[^\S\n]*\n(.*?)^```[^\S\n]*$", re.MULTILINE | re.DOTALL)
HEADING: re.Pattern[str] = re.compile(r"^(#+)[ \t]+(.*?)[ \t]*$", re.MULTILINE)
MARKDOWN_LINK: re.Pattern[str] = re.compile(r"\[([^\]]+)\]\(([^)\s]+)\)")


@dataclass(frozen=True)
class YamlSnippet:
    """One fenced YAML block lifted out of a documentation page, with its classified schema kind."""

    page: Path
    line: int
    block: str
    kind: ConfigKind | None

    @property
    def location(self) -> str:
        """Return a ``path:line`` pointer for test ids and failure messages.

        Returns:
            The block's opening-fence location relative to the repository root.
        """
        return f"{self.page.relative_to(ROOT)}:{self.line}"

    def parse(self) -> Any:
        """Parse the block fresh on every call.

        Returns:
            The parsed YAML object.

        Re-parsing instead of caching is deliberate: ``ingests.to_sections`` stamps ``config`` into
        the template dict it is handed, so a cached parse would leak mutation between tests (and
        between the two workers that collect the same parametrization under xdist).
        """
        return yaml.load(self.block, Loader=CSafeLoader)


def _mkdocs() -> dict[str, Any]:
    """Return the parsed live ``mkdocs.yml`` -- the nav and site_url source of truth.

    Returns:
        The mkdocs configuration as a mapping.
    """
    parsed: Any = yaml.load(MKDOCS.read_text(encoding="utf-8"), Loader=CSafeLoader)
    assert isinstance(parsed, dict), f"{MKDOCS.name} did not parse to a mapping"
    return parsed


def _excluded_docs_dirs() -> frozenset[str]:
    """Return ``docs/`` subdirectory names mkdocs excludes from the published site.

    ``mkdocs.yml:exclude_docs`` keeps gitignored local scratch notes out of the site; unpublished
    notes are not documentation a reader can copy from, so they are not schema-guarded here.

    Returns:
        Directory names (without the trailing slash mkdocs writes).
    """
    raw: str = str(_mkdocs().get("exclude_docs") or "")
    return frozenset(line.strip().removesuffix("/") for line in raw.splitlines() if line.strip().endswith("/"))


def _markdown_pages() -> list[Path]:
    """Return the README plus every published Markdown page under ``docs/``, in a stable order.

    Returns:
        Sorted documentation pages, README first.
    """
    excluded: frozenset[str] = _excluded_docs_dirs()
    published: list[Path] = [path for path in sorted(DOCS.rglob("*.md")) if path.relative_to(DOCS).parts[0] not in excluded]
    return [README, *published]


def _classify(block: str, parsed: Any) -> ConfigKind | None:
    """Return which live schema a block is a COMPLETE config for, or ``None`` when it must be skipped.

    Rule-based on purpose. The docs abbreviate with ``{...}`` / ``...`` and show partial fragments
    (a lone ``source:``, a template/section merge illustration); neither can validate, and neither
    may be excluded by a copied list -- so a block is guarded only when its parsed shape carries
    every key the corresponding live model requires.

    Args:
        block: Raw YAML text of the fenced block.
        parsed: The same block parsed with the safe loader.

    Returns:
        ``"table"``, ``"graph"``, or ``None`` to skip.
    """
    if any(token in block for token in PLACEHOLDER_TOKENS) or not isinstance(parsed, dict):
        return None
    template: Any = parsed.get("template")
    top: set[str] = set(parsed)
    declared: set[str] = top | (set(template) if isinstance(template, dict) else set())
    if TABLE_KEYS.issubset(declared):
        return "table"
    if GRAPH_KEYS.issubset(top):
        return "graph"
    return None


def _snippet(page: Path, line: int, block: str) -> YamlSnippet:
    """Build one classified documentation YAML snippet.

    Args:
        page: Markdown page the block came from.
        line: 1-based line of the block's opening fence.
        block: Raw YAML text of the block.

    Returns:
        The classified snippet. A block that does not parse classifies as ``None`` (skipped) so a
        docs typo cannot crash collection for the whole module; ``test_yaml_snippets_parse_as_yaml``
        reports it per block instead.
    """
    try:
        parsed: Any = yaml.load(block, Loader=CSafeLoader)
    except yaml.YAMLError:
        parsed = None
    return YamlSnippet(page=page, line=line, block=block, kind=_classify(block, parsed))


def _yaml_snippets(pages: list[Path] | None = None) -> list[YamlSnippet]:
    """Extract and classify every fenced YAML block in the documentation corpus.

    Args:
        pages: Pages to scan; defaults to the whole published corpus.

    Returns:
        Snippets in page order (README first), each carrying its classified kind.
    """
    snippets: list[YamlSnippet] = []
    for page in _markdown_pages() if pages is None else pages:
        text: str = page.read_text(encoding="utf-8")
        for match in FENCED_YAML.finditer(text):
            snippets.append(_snippet(page, text.count("\n", 0, match.start()) + 1, match.group(1)))
    return snippets


def _snippets_in(page: Path, complete_only: bool = False) -> list[YamlSnippet]:
    """Return one page's snippets, optionally narrowed to the ones the classifier guards.

    Args:
        page: Markdown page to scan.
        complete_only: When True, drop skipped fragments and placeholders.

    Returns:
        Snippets in document order.
    """
    found: list[YamlSnippet] = _yaml_snippets([page])
    return [snippet for snippet in found if snippet.kind is not None] if complete_only else found


def _complete_snippets() -> list[YamlSnippet]:
    """Return every documentation YAML block the classifier calls a complete table or graph config.

    Returns:
        Guarded snippets in corpus order.
    """
    return [snippet for snippet in _yaml_snippets() if snippet.kind is not None]


def _validate(snippet: YamlSnippet) -> None:
    """Validate one complete snippet through the same code path ``tablassert validate`` uses.

    Args:
        snippet: A snippet whose ``kind`` is ``"table"`` or ``"graph"``.

    Raises:
        AssertionError: If a table snippet expands to zero sections.
    """
    parsed: Any = snippet.parse()
    # Only the two known relocation/effect notices are silenced: the tutorial and the gallery
    # deliberately show `study_size` and non-slot annotations, and the live models WARN (never
    # fail) about where such a value lands. Everything else stays audible so new drift surfaces.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", BiolinkRelocationWarning)
        warnings.simplefilter("ignore", UnpairedEffectAnnotationWarning)
        if snippet.kind == "graph":
            Graph.model_validate(parsed)
            return
        sections: list[dict[str, Any]] = to_sections(parsed, Path(f"{snippet.location}.yaml"))  # pyright: ignore
        assert sections, f"{snippet.location} expanded to zero sections"
        for section in sections:
            Tcode.model_validate({**section, "store": STORE})


def _section_range(text: str, heading: str) -> tuple[int, int]:
    """Return the character span of the Markdown section opened by ``heading``.

    Args:
        text: Full Markdown source.
        heading: Heading text without its leading hashes.

    Returns:
        ``(start, end)`` offsets covering the section body, ending at the next heading of the same
        or higher level (or end of file).

    Raises:
        AssertionError: If no heading with that exact text exists -- an anchor that silently stops
            matching would make every pin below vacuous.
    """
    headings: list[re.Match[str]] = list(HEADING.finditer(text))
    for index, match in enumerate(headings):
        if match.group(2) != heading:
            continue
        level: int = len(match.group(1))
        end: int = len(text)
        for later in headings[index + 1 :]:
            if len(later.group(1)) <= level:
                end = later.start()
                break
        return match.end(), end
    raise AssertionError(f"heading {heading!r} not found; the section anchor is stale")


def _list_links(page: Path, heading: str) -> dict[str, str]:
    """Return ``{link text: target}`` for every Markdown link in one documentation index list.

    Args:
        page: Markdown page holding the list.
        heading: ``##`` heading the list lives under.

    Returns:
        Link text mapped to its target, in document order.
    """
    text: str = page.read_text(encoding="utf-8")
    start, end = _section_range(text, heading)
    return {match.group(1): match.group(2) for match in MARKDOWN_LINK.finditer(text, start, end)}


def _first_leaf(target: Any) -> str | None:
    """Return the first leaf page under a nav target (a page string or a nested section list).

    Args:
        target: A mkdocs nav value.

    Returns:
        The docs-relative page path, or ``None`` when the target holds no page.
    """
    if isinstance(target, str):
        return target
    if isinstance(target, list):
        for child in target:
            leaf: str | None = _first_leaf(next(iter(child.values()))) if isinstance(child, dict) else _first_leaf(child)
            if leaf is not None:
                return leaf
    return None


def _nav_pages() -> list[tuple[str, str]]:
    """Return ``(nav title, docs-relative page)`` for every top-level mkdocs nav entry.

    A section resolves to its first leaf page, which is what both index lists already link
    (``Configuration`` -> ``configuration/graph.md``). The Home entry is dropped: the README and
    ``docs/index.md`` ARE the Home surface and link the site root rather than themselves.

    Returns:
        Nav pages in ``mkdocs.yml`` order.
    """
    nav: Any = _mkdocs().get("nav")
    assert isinstance(nav, list), "mkdocs.yml nav is missing or not a list"
    assert nav, "mkdocs.yml nav is empty"
    pages: list[tuple[str, str]] = []
    for entry in nav:
        assert isinstance(entry, dict), f"unsupported mkdocs nav entry: {entry!r}"
        assert len(entry) == 1, f"mkdocs nav entry must carry exactly one title: {entry!r}"
        title, target = next(iter(entry.items()))
        leaf: str | None = _first_leaf(target)
        if leaf is not None and leaf != "index.md":
            pages.append((str(title), leaf))
    assert pages, "mkdocs.yml nav lists no page beyond Home"
    return pages


def _site_base() -> str:
    """Return the live published-site base URL, slash-terminated.

    Returns:
        ``mkdocs.yml``'s ``site_url``, so the guard never hardcodes the documentation host.
    """
    base: str = str(_mkdocs()["site_url"])
    return base if base.endswith("/") else f"{base}/"


def _published_url(page: str) -> str:
    """Return the published-site URL mkdocs serves ``page`` at -- the README's existing link style.

    Args:
        page: Docs-relative page path such as ``configuration/graph.md``.

    Returns:
        Absolute URL built from the live ``mkdocs.yml`` ``site_url``.
    """
    return f"{_site_base()}{page.removesuffix('.md')}/"


def _assert_nav_parity(page: Path, heading: str) -> None:
    """Assert one documentation index list links every top-level nav page at the right target.

    Args:
        page: Markdown page holding the index list.
        heading: ``##`` heading the list lives under.

    Raises:
        AssertionError: Naming the missing nav page, or the wrong target for a linked one.
    """
    links: dict[str, str] = _list_links(page, heading)
    for title, nav_page in _nav_pages():
        assert title in links, f"{page.relative_to(ROOT)} '## {heading}' is missing the nav page {title!r} ({nav_page})"
        expected: str = _published_url(nav_page) if page == README else nav_page
        assert links[title] == expected, f"{page.relative_to(ROOT)} links {title!r} to {links[title]!r}, expected {expected!r}"


@pytest.mark.parametrize("snippet", _yaml_snippets(), ids=lambda snippet: snippet.location)
def test_yaml_snippets_parse_as_yaml(snippet: YamlSnippet) -> None:
    """Every fenced YAML block in the documentation corpus is syntactically valid YAML.

    Runs over ALL blocks, including the fragments the schema guard skips, because an unparseable
    block is invisible to every other check here: it classifies as ``None`` and would otherwise
    drift unnoticed (and, if parsed at collection time, take the whole module down with it).

    Args:
        snippet: One documentation YAML block.
    """
    try:
        parsed: Any = snippet.parse()
    except yaml.YAMLError as error:
        pytest.fail(f"{snippet.location} is not valid YAML: {error}")
    assert snippet.block.strip(), f"{snippet.location} is an empty YAML block"
    assert parsed is not None, f"{snippet.location} parsed to nothing"


@pytest.mark.parametrize("snippet", _complete_snippets(), ids=lambda snippet: f"{snippet.location}-{snippet.kind}")
def test_yaml_snippets_validate_against_live_schema(snippet: YamlSnippet) -> None:
    """Every complete documentation config validates exactly as ``tablassert validate`` would.

    This is the guard that catches a quick start or gallery entry drifting away from the live
    Pydantic models: the block a reader copies is checked through the same ``to_sections`` ->
    ``Tcode`` path (table) or ``Graph.model_validate`` path (graph) the CLI uses.

    Args:
        snippet: A classified complete table or graph config lifted from the docs.
    """
    _validate(snippet)


def test_yaml_snippets_classifier_selects_complete_configs() -> None:
    """The classifier picks up the configs readers actually copy -- pinned against under-selection.

    A classifier that quietly skips the README quick start would make the guard above vacuous, so
    the selection is pinned from source: the README quick start and the tutorial each contribute a
    table/graph pair, every use-case gallery block is a complete table config, and none of those
    pages has a block the classifier drops.
    """
    assert _complete_snippets(), "no complete YAML config found in the corpus; the fenced-block regex broke"
    for page, why in ((README, "quick start"), (DOCS / "tutorial.md", "tutorial pair"), (DOCS / "examples.md", "use case gallery")):
        blocks: list[YamlSnippet] = _snippets_in(page)
        complete: list[YamlSnippet] = _snippets_in(page, complete_only=True)
        assert complete, f"{page.relative_to(ROOT)} {why}: classifier selected no complete config"
        assert len(complete) == len(blocks), f"{page.relative_to(ROOT)} {why}: classifier skipped {[s.location for s in blocks if s.kind is None]}"
    assert [snippet.kind for snippet in _snippets_in(README, complete_only=True)] == ["table", "graph"], (
        "README quick start must be one table then one graph config"
    )
    assert [snippet.kind for snippet in _snippets_in(DOCS / "tutorial.md", complete_only=True)] == ["table", "graph"], (
        "tutorial must be one table then one graph config"
    )
    assert {snippet.kind for snippet in _snippets_in(DOCS / "examples.md", complete_only=True)} == {"table"}, (
        "every gallery block is a complete table config"
    )


def test_yaml_snippets_classifier_skips_fragments() -> None:
    """Abbreviated and partial blocks are skipped by rule -- pinned against over-selection.

    Over-selection would fail CI on excerpts that are not configs at all, so two source-derived
    pins hold the line: every block in the corpus that elides with ``{...}`` / ``...`` is skipped
    (and such blocks exist, so the pin is not vacuous), and every block inside the table
    reference's merge-behavior section is skipped because those illustrate ``template``/``sections``
    merging rather than declaring a standalone config.
    """
    elided: list[YamlSnippet] = [s for s in _yaml_snippets() if any(token in s.block for token in PLACEHOLDER_TOKENS)]
    assert elided, "corpus no longer elides a YAML block with `{...}`/`...`; this pin went vacuous"
    assert all(s.kind is None for s in elided), f"classifier selected an elided block: {[s.location for s in elided if s.kind is not None]}"

    table_reference: Path = DOCS / "configuration" / "table.md"
    text: str = table_reference.read_text(encoding="utf-8")
    start, end = _section_range(text, "Merge Behavior (fastmerge)")
    first_line: int = text.count("\n", 0, start) + 1
    last_line: int = text.count("\n", 0, end) + 1
    merge_blocks: list[YamlSnippet] = [s for s in _snippets_in(table_reference) if first_line <= s.line < last_line]
    assert merge_blocks, "the merge-behavior section has no YAML block; this pin went vacuous"
    assert all(s.kind is None for s in merge_blocks), (
        f"classifier selected a merge fragment: {[s.location for s in merge_blocks if s.kind is not None]}"
    )


def test_readme_documentation_links_cover_every_nav_page() -> None:
    """The README documentation list links every top-level mkdocs nav page at its published URL.

    Both the nav and the expected target come from live source (``mkdocs.yml`` nav order plus
    ``site_url``), so a newly published page is orphaned in CI until the README links it -- the
    drift that left ``Fullmap`` unlinked here.
    """
    _assert_nav_parity(README, "Documentation")


def test_index_documentation_links_cover_every_nav_page() -> None:
    """``docs/index.md``'s Documentation Sections list reaches the same nav parity, repo-relative.

    The docs index renders inside the site, so its targets are docs-relative page paths rather than
    absolute URLs; the required set is still derived from the live nav.
    """
    _assert_nav_parity(INDEX, "Documentation Sections")


def test_readme_documentation_links_use_published_site_urls() -> None:
    """Every README documentation index link is a published-site URL, not a repo-relative path.

    The README renders on PyPI and GitHub, where ``installation.md`` is a dead link; pinning the
    host to the live ``mkdocs.yml`` ``site_url`` keeps the list usable off-repository and stops a
    contributor from "fixing" a link into a relative path.
    """
    base: str = _site_base()
    links: dict[str, str] = _list_links(README, "Documentation")
    assert links, "README '## Documentation' list is empty"
    broken: list[str] = [f"{text} -> {target}" for text, target in links.items() if not target.startswith(base)]
    assert not broken, f"README documentation links must be published-site URLs under {base}: {broken}"


# --- Configuration-reference field tables (US-002) ---------------------------- #
# The configuration references drifted once already: `NodeEncoding.exclude_prefixes` /
# `exclude_regex` shipped in `src/tablassert/models.py` with no row in
# `docs/configuration/table.md`, and nothing failed. These guards bind each live config model
# to the Markdown field table(s) that document it and check BOTH directions from live source:
# field sets are read from `model.model_fields` and parsed out of the docs tables on every
# run, never copied, so a new model field or a new table row immediately exercises the guard.

TABLE_CONFIGURATION: Path = DOCS / "configuration" / "table.md"
GRAPH_CONFIGURATION: Path = DOCS / "configuration" / "graph.md"


@dataclass(frozen=True)
class ModelTableBinding:
    """One live configuration model bound to the Markdown field table(s) that must document it.

    Attributes:
        model: Live Pydantic model from ``tablassert.models`` -- the field source of truth.
        page: Markdown page holding the model's field table(s).
        headings: Exact heading texts, each anchoring the FIRST Markdown table beneath it; several
            when the reference splits a model across tables (Graph's required/optional split).
        token_prefix: Dotted prefix the reference puts on this model's rows (``rig.`` for the RIG
            section table), stripped before comparing against live field names.
    """

    model: type[TablaBase]
    page: Path
    headings: tuple[str, ...]
    token_prefix: str = ""

    @property
    def location(self) -> str:
        """Return a ``path#heading`` pointer for test ids and failure messages.

        Returns:
            The binding's first table location relative to the repository root.
        """
        return f"{self.page.relative_to(ROOT)}#{self.headings[0]}"


CONFIG_MODEL_BINDINGS: tuple[ModelTableBinding, ...] = (
    ModelTableBinding(Excel, TABLE_CONFIGURATION, ("Excel Source",)),
    ModelTableBinding(Text, TABLE_CONFIGURATION, ("Text Source (CSV/TSV)",)),
    ModelTableBinding(Reindex, TABLE_CONFIGURATION, ("Reindexing (Conditional Filtering)",)),
    ModelTableBinding(Statement, TABLE_CONFIGURATION, ("Statement (Triple Definition)",)),
    ModelTableBinding(NodeEncoding, TABLE_CONFIGURATION, ("NodeEncoding",)),
    ModelTableBinding(Qualifier, TABLE_CONFIGURATION, ("Qualifiers",)),
    ModelTableBinding(Provenance, TABLE_CONFIGURATION, ("Provenance",)),
    ModelTableBinding(ManualProvenance, TABLE_CONFIGURATION, ("Manual provenance override",)),
    ModelTableBinding(SourceOverride, TABLE_CONFIGURATION, ("Explicit sources template",)),
    ModelTableBinding(Annotation, TABLE_CONFIGURATION, ("Annotations",)),
    ModelTableBinding(Graph, GRAPH_CONFIGURATION, ("Required Fields", "Optional Fields")),
    ModelTableBinding(RIGConfig, GRAPH_CONFIGURATION, ("The `rig:` section",), token_prefix="rig."),
    ModelTableBinding(RIGSourceInfo, GRAPH_CONFIGURATION, ("`rig.source_info`",)),
    ModelTableBinding(RIGIngestInfo, GRAPH_CONFIGURATION, ("`rig.ingest_info`",)),
    ModelTableBinding(RIGProvenanceInfo, GRAPH_CONFIGURATION, ("`rig.provenance_info`",)),
)
# Deliberately unbound live models: every `TablaBase` subclass the reference does NOT give its own
# field table. `TablaBase` / `BaseSource` / `Encoding` are shared bases whose fields are flattened
# into their concrete subclasses' tables; `Regex` / `Math` / `Section` are structural shells
# documented inline in prose; the RIG entry models are documented inside their parent table's row
# descriptions. `test_config_model_bindings_cover_live_models` asserts this set plus the bound
# models equals the live subclass set, so a new model forces an explicit decision: add a
# `ModelTableBinding` above, or add its name here.
UNBOUND_MODELS: frozenset[str] = frozenset(
    {
        "TablaBase",
        "BaseSource",
        "Encoding",
        "Regex",
        "Math",
        "Section",
        "RIGTermsOfUseInfo",
        "RIGRelevantFile",
        "RIGIncludedContent",
        "RIGFilteredContent",
        "RIGFutureContentConsideration",
        "RIGFutureModelingConsideration",
        "RIGSupportingDataSourceInfo",
        "RIGTargetInfoExtras",
    }
)

BINDINGS_BY_MODEL: dict[type[TablaBase], ModelTableBinding] = {binding.model: binding for binding in CONFIG_MODEL_BINDINGS}

# Ancestors the reference deliberately flattens into ONE descendant's table instead of giving them
# their own: `Encoding`'s fields are listed in the `NodeEncoding` table, which `Annotation`'s
# `(inherits Encoding)` row points readers at. Coverage for such an ancestor is looked up in that
# host table only -- never across every table -- so an inherited field can never be counted as
# documented because an unrelated model happens to declare a field of the same name.
FLATTENED_ANCESTOR_HOSTS: dict[type[TablaBase], type[TablaBase]] = {Encoding: NodeEncoding}

# Prose splitters used by the semantics pins below. Paragraphs are separated by a blank line that may
# carry stray whitespace, and each paragraph's own newlines are collapsed, so re-wrapping a sentence
# cannot fail a guard. Clauses split on sentence ends and the contrastive conjunctions the reference
# uses, which is what lets a claim be pinned to the SIDE of a contrast it belongs to (`nullable: true`
# keeps the edge / `nullable: false` drops the row) instead of to the paragraph as a whole. `:` is
# deliberately not a clause end -- it would split `nullable: false` itself.
PARAGRAPH_SPLIT: re.Pattern[str] = re.compile(r"\n[^\S\n]*\n")
CLAUSE_SPLIT: re.Pattern[str] = re.compile(r"(?<=[.;])\s+|\s+(?:whereas|while|but)\s+")

FIRST_TABLE_CELL_TOKEN: re.Pattern[str] = re.compile(r"^`(?P<token>[^`]+)`")
INHERITS_CELL: re.Pattern[str] = re.compile(r"^\(inherits\s+(?P<anchor>[A-Za-z]+)\)")
TABLE_DIVIDER: re.Pattern[str] = re.compile(r"^\|[\s:|-]+\|$")


@dataclass(frozen=True)
class FieldTable:
    """One parsed documentation field table.

    Attributes:
        header: The table's column names, in order (used to locate the ``Required`` column).
        rows: Documented field token -> the raw Markdown table row documenting it, in table order.
        inherits: Model names from ``(inherits X)`` rows (fields documented via the parent table).
    """

    header: tuple[str, ...]
    rows: dict[str, str]
    inherits: tuple[str, ...]

    def cell(self: FieldTable, field: str, column: str) -> str:
        """Return one documented field's cell under ``column``.

        Args:
            field: Documented field token.
            column: Header column name.

        Returns:
            The stripped cell text.

        Raises:
            AssertionError: If the table has no such column or the row is too short to hold it.
        """
        assert column in self.header, f"table header {list(self.header)} has no {column!r} column"
        index: int = self.header.index(column)
        cells: list[str] = _row_cells(self.rows[field])
        assert index < len(cells), f"row for `{field}` has no {column!r} cell: {self.rows[field]!r}"
        return cells[index]


def _first_table_lines(text: str, start: int, end: int) -> list[str]:
    """Return the raw lines of the FIRST Markdown table inside ``text[start:end]``.

    Args:
        text: Full Markdown source.
        start: Section body start offset.
        end: Section body end offset.

    Returns:
        The table's stripped lines (header, divider, body rows); empty when the span holds none.
    """
    lines: list[str] = []
    for line in text[start:end].splitlines():
        if line.lstrip().startswith("|"):
            lines.append(line.strip())
        elif lines:
            break
    return lines


def _row_cells(row: str) -> list[str]:
    """Split one Markdown table row into its stripped cells.

    Args:
        row: Raw table row, leading and trailing pipes included.

    Returns:
        The cell texts between the pipes.
    """
    return [cell.strip() for cell in row.strip().strip("|").split("|")]


def _field_table(binding: ModelTableBinding, heading: str) -> FieldTable:
    """Parse the first field table under ``heading`` into documented tokens and inherit anchors.

    Args:
        binding: Model/table binding supplying the page and any dotted token prefix.
        heading: Exact heading text anchoring the table.

    Returns:
        The parsed table.

    Raises:
        AssertionError: If the heading anchors no table, the table is malformed, or a row's first
            cell is neither a backticked field token nor an ``(inherits X)`` marker -- a table the
            guard cannot read would silently stop guarding.
    """
    text: str = binding.page.read_text(encoding="utf-8")
    start, end = _section_range(text, heading)
    lines: list[str] = _first_table_lines(text, start, end)
    where: str = f"{binding.page.relative_to(ROOT)} '{heading}'"
    assert len(lines) >= 2, f"{where} anchors no Markdown table"
    assert "Field" in lines[0], f"{where} table header does not name a Field column: {lines[0]!r}"
    assert TABLE_DIVIDER.match(lines[1]), f"{where} table divider is malformed: {lines[1]!r}"
    rows: dict[str, str] = {}
    inherits: list[str] = []
    for line in lines[2:]:
        cell: str = line.split("|")[1].strip()
        inherit: re.Match[str] | None = INHERITS_CELL.match(cell)
        if inherit:
            inherits.append(inherit.group("anchor"))
            continue
        token: re.Match[str] | None = FIRST_TABLE_CELL_TOKEN.match(cell)
        assert token, f"{where} table row has an unreadable first cell: {cell!r}"
        name: str = token.group("token")
        if binding.token_prefix:
            assert name.startswith(binding.token_prefix), f"{where} row {name!r} lacks the {binding.token_prefix!r} prefix the rest of the table uses"
            name = name.removeprefix(binding.token_prefix)
        rows[name] = line
    assert rows or inherits, f"{where} table declares no fields"
    return FieldTable(header=tuple(_row_cells(lines[0])), rows=rows, inherits=tuple(inherits))


def _own_fields(binding: ModelTableBinding) -> tuple[dict[str, str], tuple[str, ...]]:
    """Return the merged ``(rows, inherit anchors)`` across all of a binding's tables.

    Args:
        binding: Binding whose headings each anchor one field table.

    Returns:
        Field token -> documenting row, merged in heading order, plus the inherit anchors.
    """
    rows: dict[str, str] = {}
    inherits: list[str] = []
    for heading in binding.headings:
        table: FieldTable = _field_table(binding, heading)
        rows.update(table.rows)
        inherits.extend(table.inherits)
    return rows, tuple(inherits)


def _live_ancestor(anchor: str, binding: ModelTableBinding) -> type[TablaBase]:
    """Resolve an ``(inherits X)`` docs anchor to a live ancestor of the bound model.

    The anchor is checked against the live MRO, so a docs table cannot claim an inheritance the
    models do not have -- and a refactor that breaks the inheritance fails here, not silently.

    Args:
        anchor: Model name from the docs row.
        binding: Binding whose table carries the row.

    Returns:
        The live ancestor class.

    Raises:
        AssertionError: If the anchor names no live config model or is not a live ancestor.
    """
    candidate: Any = getattr(config_models, anchor, None)
    not_a_model: str = f"{binding.location} claims `(inherits {anchor})`, but {anchor!r} is not a live config model in tablassert.models"
    assert isinstance(candidate, type), not_a_model
    assert issubclass(candidate, TablaBase), not_a_model
    not_an_ancestor: str = f"{binding.location} claims {binding.model.__name__} inherits {anchor}, but the live MRO disagrees"
    assert candidate is not binding.model, not_an_ancestor
    assert issubclass(binding.model, candidate), not_an_ancestor
    return candidate


def _documented_fields(binding: ModelTableBinding) -> set[str]:
    """Return every field name the documentation covers for ``binding``'s model.

    Coverage is the binding's own table rows plus, for each ``(inherits X)`` row, the ancestor's
    documented fields: the bound ancestor's table recursively, or -- for an ancestor the reference
    deliberately flattens into ONE host table (``Encoding`` is documented inside the ``NodeEncoding``
    table, which ``Annotation``'s ``(inherits Encoding)`` row points readers at) -- the ancestor's
    live fields documented in THAT host table. Coverage is never drawn from the union of every
    table, so an inherited field cannot count as documented merely because an unrelated model's
    table carries a row of the same name. Both sides stay live-derived: dropping a row shrinks
    coverage and fails the inheriting model's check.

    Args:
        binding: Binding whose documented coverage is computed.

    Returns:
        Field names documented for the bound model.

    Raises:
        AssertionError: If an ``(inherits X)`` anchor resolves to an ancestor that is neither bound
            to its own table nor registered in ``FLATTENED_ANCESTOR_HOSTS`` -- its coverage would
            otherwise be unverifiable.
    """
    rows, anchors = _own_fields(binding)
    documented: set[str] = set(rows)
    for anchor in anchors:
        ancestor: type[TablaBase] = _live_ancestor(anchor, binding)
        host: type[TablaBase] = FLATTENED_ANCESTOR_HOSTS.get(ancestor, ancestor)
        parent: ModelTableBinding | None = BINDINGS_BY_MODEL.get(host)
        assert parent is not None, (
            f"{binding.location} points at `(inherits {anchor})`, but {anchor} has neither its own documentation table nor an entry in "
            "FLATTENED_ANCESTOR_HOSTS naming the table that documents its fields"
        )
        if parent.model is ancestor:
            documented |= _documented_fields(parent)
        else:
            # Flattened ancestor: only the host table's rows count, so a same-named field
            # elsewhere in the reference can never stand in for the missing row.
            documented |= set(ancestor.model_fields) & set(_own_fields(parent)[0])
    return documented


def _paragraphs(body: str) -> list[str]:
    """Return ``body``'s non-empty paragraphs, each with its internal whitespace collapsed.

    Args:
        body: Markdown section body.

    Returns:
        One whitespace-normalized string per paragraph, so the pins below are insensitive to how the
        prose happens to be line-wrapped.
    """
    return [" ".join(block.split()) for block in PARAGRAPH_SPLIT.split(body) if block.strip()]


def _clauses(paragraph: str) -> list[str]:
    """Split one normalized paragraph into the clauses a claim can be attributed to.

    Args:
        paragraph: A whitespace-normalized paragraph from :func:`_paragraphs`.

    Returns:
        Sentence/contrast clauses, empties dropped.
    """
    return [clause.strip() for clause in CLAUSE_SPLIT.split(paragraph) if clause.strip()]


def _nullable_join_curies(drop_unresolved: bool) -> list[str | None]:
    """Return the resolved column the LIVE join produces for one resolvable and one unresolvable cell.

    ``fullmap.join_matches``'s ``drop_unresolved`` is the single switch ``lib`` flips from
    ``Qualifier.nullable`` (``drop_unresolved=not spec.nullable``), so running it both ways reads the
    documented consequence -- drop the row vs keep the edge with a null qualifier column -- off the
    shipped code instead of restating it.

    Args:
        drop_unresolved: ``True`` for strict resolution (subject/object and ``nullable: false``
            qualifiers), ``False`` for a ``nullable: true`` qualifier.

    Returns:
        The ``subject`` column after the join, in row order.
    """
    terms: pl.DataFrame = pl.DataFrame({"term": ["resolvable", "unresolvable"], "nlp_level": [1, 1]})
    raw: pl.DataFrame = pl.DataFrame(
        {
            "term": ["resolvable"],
            "CURIE": ["HGNC:1100"],
            "PREFERRED_NAME": ["resolvable"],
            "CATEGORY_NAME": ["Gene"],
            "TAXON_ID": [9606],
            "SOURCE_NAME": ["SOURCE"],
            "SOURCE_VERSION": ["1"],
        }
    )
    matches: pl.DataFrame = filter_and_rank(raw, terms, None, None, None, False)
    frame: pl.DataFrame = pl.DataFrame(
        {"subject": ["resolvable", "unresolvable"], "subject_two": [None, None]}, schema={"subject": pl.String, "subject_two": pl.String}
    )
    joined: pl.DataFrame = join_matches(frame.lazy(), "subject", matches, drop_unresolved=drop_unresolved).collect()
    return joined["subject"].to_list()


def _prefix_filtered_curies(curies: list[str], exclude_prefixes: list[str]) -> list[str]:
    """Return the CURIEs that survive the LIVE ``exclude_prefixes`` filter, sorted.

    Runs ``fullmap.filter_and_rank`` -- the one place ``exclude_prefixes`` is applied during entity
    resolution -- over a synthetic single-term candidate set, so the documented matching semantics
    are read off the shipped filter instead of restated.

    Args:
        curies: Candidate CURIEs for one term, all equally ranked.
        exclude_prefixes: Prefixes to exclude, exactly as a config would declare them.

    Returns:
        Sorted surviving CURIEs.
    """
    terms: pl.DataFrame = pl.DataFrame({"term": ["term"], "nlp_level": [1]})
    raw: pl.DataFrame = pl.DataFrame(
        {
            "term": ["term"] * len(curies),
            "CURIE": curies,
            "PREFERRED_NAME": ["term"] * len(curies),
            "CATEGORY_NAME": ["Gene"] * len(curies),
            "TAXON_ID": [9606] * len(curies),
            "SOURCE_NAME": ["SOURCE"] * len(curies),
            "SOURCE_VERSION": ["1"] * len(curies),
        }
    )
    matches: pl.DataFrame = filter_and_rank(raw, terms, None, None, None, False, exclude_prefixes=exclude_prefixes)
    return sorted(matches["CURIE"].to_list())


def test_config_model_bindings_cover_live_models() -> None:
    """Every live ``TablaBase`` subclass is either bound to a docs table or explicitly unbound.

    ``CONFIG_MODEL_BINDINGS`` is hand-written, so without this guard a NEW config model could ship
    entirely undocumented while every parametrized case below still passed -- the same silent drift
    US-002 fixes, one level up. Live subclasses are read from ``vars(tablassert.models)`` at test
    time, so adding a model forces an explicit decision: bind it to the table that documents it, or
    list it in ``UNBOUND_MODELS`` with the reason it has none.
    """
    live: set[str] = {
        name
        for name, obj in vars(config_models).items()
        if isinstance(obj, type) and issubclass(obj, TablaBase) and obj.__module__ == config_models.__name__
    }
    assert live, "tablassert.models exposes no TablaBase subclasses; this guard went vacuous"
    bound: set[str] = {binding.model.__name__ for binding in CONFIG_MODEL_BINDINGS}
    assert len(bound) == len(CONFIG_MODEL_BINDINGS), f"CONFIG_MODEL_BINDINGS binds the same model twice: {sorted(bound)}"
    overlap: list[str] = sorted(bound & UNBOUND_MODELS)
    assert not overlap, f"models {overlap} are both bound to a documentation table and listed in UNBOUND_MODELS; keep exactly one claim"
    undecided: list[str] = sorted(live - bound - UNBOUND_MODELS)
    assert not undecided, (
        f"live config models {undecided} in src/tablassert/models.py are neither bound to a documentation field table nor listed in "
        "UNBOUND_MODELS; add a ModelTableBinding for the table documenting each, or record there why it has none"
    )
    stale: list[str] = sorted((bound | UNBOUND_MODELS) - live)
    assert not stale, f"CONFIG_MODEL_BINDINGS/UNBOUND_MODELS still name {stale}, which are no longer live in src/tablassert/models.py"


@pytest.mark.parametrize("binding", CONFIG_MODEL_BINDINGS, ids=lambda binding: binding.model.__name__)
def test_config_model_fields_match_live_models(binding: ModelTableBinding) -> None:
    """Every documented field of a config model exists on the live model, and vice versa.

    US-002: ``exclude_prefixes`` / ``exclude_regex`` shipped on ``NodeEncoding`` (and, by
    inheritance, ``Qualifier``) with no row in the table configuration reference, and nothing
    failed. Both directions matter: a documented-but-dead field is a config-time error
    (``extra="forbid"``) the docs would bless, and a live-but-undocumented field is a feature
    readers cannot discover. Graph-side models are bound too, so the graph reference is pinned
    against the same drift even though it is currently green.

    Args:
        binding: One live config model bound to its documentation field table(s).
    """
    rows, _ = _own_fields(binding)
    live: set[str] = set(binding.model.model_fields)
    phantom: list[str] = sorted(set(rows) - live)
    assert not phantom, (
        f"{binding.location} documents {phantom} for {binding.model.__name__}, but the live model has no such field "
        '(extra="forbid" rejects it at config time); remove the row(s) or restore the field(s)'
    )
    undocumented: list[str] = sorted(live - _documented_fields(binding))
    assert not undocumented, (
        f"{binding.model.__name__} fields {undocumented} are live in src/tablassert/models.py but undocumented in "
        f"{binding.location}; add a row (or an `(inherits ...)` marker for inherited fields)"
    )


def test_exclude_filters_docs_match_live_semantics() -> None:
    """The exclude-filter documentation carries the live models' semantics, not just the names.

    A row that names ``exclude_regex`` but misstates its behavior is the same drift class as the
    missing rows US-002 fixes, so the distinguishing claims are pinned against LIVE facts: the
    field descriptions on ``NodeEncoding.model_fields``, the error code and rationale the live
    validator actually raises for an empty or invalid pattern (reproduced here by triggering
    them, not copied), and the live ``Qualifier`` -> ``NodeEncoding`` inheritance.
    """
    binding: ModelTableBinding = BINDINGS_BY_MODEL[NodeEncoding]
    table: FieldTable = _field_table(binding, "NodeEncoding")
    rows: dict[str, str] = table.rows
    text: str = binding.page.read_text(encoding="utf-8")
    start, end = _section_range(text, "NodeEncoding")
    section: str = text[start:end]
    # The prose subsection, scoped WITHOUT the field table above it, so a claim required of the
    # narrative cannot be satisfied by the same words sitting in a table row (and vice versa).
    fstart, fend = _section_range(text, "Resolution Filters")
    filters_section: str = text[fstart:fend]
    assert "| `exclude_prefixes`" not in filters_section, (
        f"{binding.location} 'Resolution Filters' scope now contains the NodeEncoding field table; the prose pins below would go vacuous"
    )

    # The guarded field set itself is discovered from the live model, so a third `exclude_*`
    # field would extend this guard automatically (and an empty set fails loudly below).
    exclude_fields: list[str] = [name for name in NodeEncoding.model_fields if name.startswith("exclude_")]
    assert exclude_fields, "NodeEncoding no longer declares any `exclude_*` field; this guard went vacuous"

    # Every live exclude field has a row, and `avoid` plus the exclude filters appear in the same
    # relative order the model declares them -- the reference lists the resolution filters where
    # the models declare them, immediately after `avoid`.
    expected_order: list[str] = [name for name in NodeEncoding.model_fields if name == "avoid" or name in exclude_fields]
    documented_order: list[str] = [token for token in rows if token in expected_order]
    assert documented_order == expected_order, (
        f"{binding.location} must carry a row for each of {expected_order}, in that relative order (the live declaration order of "
        f"`avoid` and the exclude filters); found {documented_order}"
    )

    # Optionality is read off the live model, not the prose: every exclude filter is optional with
    # default None, so its `Required` cell must say No and its row must state the null default.
    for field in expected_order:
        live_field: FieldInfo = NodeEncoding.model_fields[field]
        assert not live_field.is_required(), f"live `{field}` is now required; the {binding.location} `Required` column must be re-derived"
        assert live_field.default is None, f"live `{field}` default is {live_field.default!r}, not None; re-derive this guard's default claim"
        required_cell: str = table.cell(field, "Required")
        assert required_cell == "No", f"{binding.location} `{field}` row marks Required={required_cell!r}, but the live field is optional"
    for field in exclude_fields:
        assert "defaults to null" in rows[field].lower(), (
            f"{binding.location} `{field}` row must state that it is optional and defaults to null (live default None); row: {rows[field]!r}"
        )

    # The rows carry the same semantics the live field descriptions carry; each anchor phrase is
    # checked against the live description FIRST, so a live semantics change fails here too.
    semantic_anchors: dict[str, str] = {"exclude_prefixes": "before the first", "exclude_regex": "case-sensitive"}
    assert set(semantic_anchors) <= set(exclude_fields), (
        f"live exclude fields {exclude_fields} no longer include {sorted(semantic_anchors)}; re-derive this guard's anchors"
    )
    for field, anchor in semantic_anchors.items():
        description: str = str(NodeEncoding.model_fields[field].description).lower()
        assert anchor in description, f"live `{field}` description no longer carries {anchor!r}; re-derive this guard's anchors"
        assert anchor in rows[field].lower(), f"{binding.location} `{field}` row must carry the live semantics ({anchor!r}); row: {rows[field]!r}"

    # Empty-pattern rejection: the docs must cite the same code and rationale the live validator
    # raises (reproduced live: a whitespace-only pattern would match EVERY CURIE and silently
    # drop all resolution candidates -- data loss discovered hours into a build).
    with pytest.raises(ValidationError) as exc_info:
        NodeEncoding(encoding="x", exclude_regex=["  "])  # pyright: ignore[reportCallIssue]
    live_message: str = str(exc_info.value)
    assert "regex-bad-pattern" in live_message, f"live whitespace-only `exclude_regex` no longer raises `regex-bad-pattern`: {live_message}"
    assert "regex-bad-pattern" in section, f"{binding.location} must name the `regex-bad-pattern` code the live validator raises"
    for phrase in ("empty pattern", "every CURIE"):
        assert phrase in live_message, f"live empty-pattern error no longer explains {phrase!r}; re-derive this guard"
        assert phrase in section, f"{binding.location} must explain the empty-pattern rejection ({phrase!r})"

    # The Polars/Rust regex dialect caveat that holds for `regex` holds for `exclude_regex` too:
    # the live validator probes patterns through polars, and the docs row must say so.
    with pytest.raises(ValidationError) as exc_info_bad:
        NodeEncoding(encoding="x", exclude_regex=["("])  # pyright: ignore[reportCallIssue]
    bad_pattern_message: str = str(exc_info_bad.value)
    assert "polars-compatible" in bad_pattern_message, (
        f"live `exclude_regex` no longer rejects an uncompilable pattern as polars-compatible-only: {bad_pattern_message}"
    )
    assert "polars" in rows["exclude_regex"].lower(), f"{binding.location} `exclude_regex` row must carry the Polars/Rust dialect caveat"

    # Prefix matching is a membership test against the listed strings (`fullmap.filter_and_rank`
    # uses `is_in`), so it is exact and case-sensitive -- neither a longer prefix nor a differently
    # cased one is dropped. Pinned by running the live filter, then requiring the prose to say so.
    surviving: list[str] = _prefix_filtered_curies(["OMIM:100100", "OMIMPS:100", "omim:100100", "HGNC:1100"], ["OMIM"])
    assert surviving == ["HGNC:1100", "OMIMPS:100", "omim:100100"], (
        f"live `exclude_prefixes` matching is no longer exact and case-sensitive (survivors: {surviving}); re-derive this guard"
    )
    # Required INDEPENDENTLY in both places a reader can land: the field-table row (what a reader
    # scanning the reference sees) and the Resolution Filters prose (scoped above to exclude that
    # table). Deleting the claim from either location fails.
    case_claim: str = "exact and case-sensitive"
    assert case_claim in rows["exclude_prefixes"].lower(), (
        f"{binding.location} `exclude_prefixes` row must state that matching is {case_claim} (live `is_in` on the prefix string); "
        f"row: {rows['exclude_prefixes']!r}"
    )
    assert case_claim in filters_section.lower(), (
        f"{binding.location} 'Resolution Filters' prose must state that `exclude_prefixes` matching is {case_claim} "
        "(live `is_in` on the prefix string); the field-table row alone does not satisfy this"
    )

    # The contrast with the pre-resolution rewrites is what makes the fields discoverable as
    # resolution filters rather than text transformations.
    for phrase in ("rewrite the cell text before resolution", "filter resolved curies after"):
        assert phrase in section.lower(), f"{binding.location} must state the contrast ({phrase!r})"

    # Qualifiers inherit NodeEncoding live, so they expose both filters; the Qualifiers section
    # must say so explicitly.
    assert issubclass(Qualifier, NodeEncoding), "Qualifier no longer inherits NodeEncoding; re-derive this guard"
    qstart, qend = _section_range(text, "Qualifiers")
    qualifier_section: str = text[qstart:qend]
    for field in exclude_fields:
        assert f"`{field}`" in qualifier_section, (
            f"{TABLE_CONFIGURATION.relative_to(ROOT)} 'Qualifiers' must state that qualifiers inherit NodeEncoding and expose `{field}`"
        )

    # A qualifier does NOT behave exactly like subject/object: `nullable` is a live Qualifier-only
    # field, and `lib._node_ops` forwards it into the ResolveSpec that keeps the edge instead of
    # dropping the row. Both sections must carry that exception with each half of the contrast
    # attached to the right side of it -- merely mentioning the word `nullable` in the paragraph
    # would bless a claim as wrong as "`nullable: true` drops the row".
    stale_nullable: str = "`nullable` is no longer the Qualifier-only field that makes filtered-away qualifiers keep the edge; re-derive this guard"
    assert "nullable" in Qualifier.model_fields, stale_nullable
    assert "nullable" not in NodeEncoding.model_fields, stale_nullable
    live_default: Any = Qualifier.model_fields["nullable"].default
    assert live_default is False, (
        f"live `Qualifier.nullable` default is {live_default!r}, not False; the docs' `nullable: false` default claim must be re-derived"
    )
    live_nullable_description: str = str(Qualifier.model_fields["nullable"].description).lower()
    for phrase in ("keeps the edge", "omits the qualifier"):
        assert phrase in live_nullable_description, f"live `Qualifier.nullable` description no longer says {phrase!r}; re-derive this guard's anchors"

    # The consequence itself is read off the live join rather than the description: `lib` passes
    # `drop_unresolved=not spec.nullable`, so strict resolution drops the unresolvable row while a
    # nullable one keeps it with a null column (which the null-stripper turns into an omitted key).
    strict_rows: list[str | None] = _nullable_join_curies(drop_unresolved=True)
    nullable_rows: list[str | None] = _nullable_join_curies(drop_unresolved=False)
    assert strict_rows == ["HGNC:1100"], f"live strict resolution no longer drops the unresolvable row (got {strict_rows}); re-derive this guard"
    assert nullable_rows == ["HGNC:1100", None], (
        f"live `nullable` resolution no longer keeps the unresolvable row with a null value (got {nullable_rows}); re-derive this guard"
    )

    strict_claim: str = "nullable: false"
    nullable_claim: str = "nullable: true"
    for scope, body in (("Resolution Filters", filters_section), ("Qualifiers", qualifier_section)):
        # Scoped to the paragraph that actually states the drop-the-row consequence AND names both
        # sides of the `nullable` contrast; an unrelated `nullable` mention must not satisfy this.
        consequence: list[str] = [
            para for para in _paragraphs(body) if "drops the row" in para.lower() and strict_claim in para.lower() and nullable_claim in para.lower()
        ]
        assert consequence, (
            f"{TABLE_CONFIGURATION.relative_to(ROOT)} {scope!r} must state what a fully filtered-away candidate set costs in one paragraph that "
            f"names both sides of the live contrast: a strict qualifier ({strict_claim!r}, the live default) 'drops the row', while a "
            f"{nullable_claim!r} qualifier 'keeps the edge' and 'omits the qualifier key'"
        )
        for para in consequence:
            clauses: list[str] = _clauses(para.lower())
            strict_clauses: list[str] = [clause for clause in clauses if strict_claim in clause]
            nullable_clauses: list[str] = [clause for clause in clauses if nullable_claim in clause]
            where: str = f"{TABLE_CONFIGURATION.relative_to(ROOT)} {scope!r}"
            assert any("drops the row" in clause for clause in strict_clauses), (
                f"{where} must say that a strict qualifier ({strict_claim!r}, the live default) 'drops the row', like an unresolved "
                f"subject/object; no such clause found in: {para!r}"
            )
            assert not any("keeps the edge" in clause for clause in strict_clauses), (
                f"{where} attaches 'keeps the edge' to {strict_claim!r}, but the live join drops the unresolved row when nullable is False; "
                f"paragraph: {para!r}"
            )
            for anchor in ("keeps the edge", "omits the qualifier key"):
                assert any(anchor in clause for clause in nullable_clauses), (
                    f"{where} must say that a {nullable_claim!r} qualifier {anchor!r} (live: the row survives with a null qualifier column); "
                    f"no such clause found in: {para!r}"
                )
            assert not any("drops the row" in clause for clause in nullable_clauses), (
                f"{where} attaches 'drops the row' to {nullable_claim!r}, but the live join KEEPS that row (the qualifier key is omitted "
                f"instead); paragraph: {para!r}"
            )


# --- API-reference signature blocks (US-003) ---------------------------- #
# The API pages drifted the same way the configuration reference did: `fullmap.resolve`
# grew `exclude_prefixes` / `exclude_regex`, `qc.fullmap_audit` grew `on_phase`, and
# `utils.md` presented the log FILE path as `log.LOGASSERT` (a DIRECTORY) -- and nothing
# failed. These guards lift the first ```python block under every `### Function Signature`
# heading in docs/api/*.md and compare parameter names, order, positional-only/
# keyword-only kind, and defaults (repr) against the live authority: `inspect.signature`
# for Python callables, `src/tablassert/rs.pyi` for the Rust extension (compiled Rust
# functions expose no defaults to `inspect`, so the stub is the declared-signature
# authority). Annotations are deliberately NOT compared: the docs spell optionality
# `Optional[...]` where the source uses `X | None`.

API_DOCS: Path = DOCS / "api"
RS_STUB: Path = ROOT / "src" / "tablassert" / "rs.pyi"

FENCED_PYTHON: re.Pattern[str] = re.compile(r"^```python[^\S\n]*\n(.*?)^```[^\S\n]*$", re.MULTILINE | re.DOTALL)


@dataclass(frozen=True)
class SignatureParam:
    """One normalized signature parameter from a docs block or a live authority.

    Attributes:
        name: Parameter name.
        kind: ``"positional-only"``, ``"positional-or-keyword"``, or ``"keyword-only"``.
        default_repr: ``repr`` of the default value, or ``None`` when the parameter has none
            (the docs' "no ``= ...``" and the live ``inspect.Parameter.empty`` both map here).
    """

    name: str
    kind: str
    default_repr: str | None


@dataclass(frozen=True)
class ApiSignatureBinding:
    """One documented ``### Function Signature`` block bound to the live authority it must match.

    Attributes:
        page: ``docs/api/`` page holding the block.
        function: Name the block's ``def`` must declare.
        live: Live Python callable whose ``inspect.signature`` is the authority, or ``None``
            for a Rust extension function.
        stub: Function name in ``src/tablassert/rs.pyi`` used as the authority when ``live``
            is None.
    """

    page: Path
    function: str
    live: Callable[..., Any] | None = None
    stub: str | None = None

    @property
    def location(self) -> str:
        """Return a ``page#function`` pointer for test ids and failure messages.

        Returns:
            The binding's page relative to the repository root, plus the function name.
        """
        return f"{self.page.relative_to(ROOT)}#{self.function}"


@dataclass(frozen=True)
class ApiSignatureBlock:
    """One parsed ``### Function Signature`` block from a ``docs/api`` page.

    Attributes:
        page: Page holding the block.
        line: 1-based line of the heading the block sits under.
        function: Name of the ``def`` the block declares.
        params: The block's parameters, normalized, in declaration order.
    """

    page: Path
    line: int
    function: str
    params: tuple[SignatureParam, ...]

    @property
    def location(self) -> str:
        """Return a ``path:line`` pointer for test ids and failure messages.

        Returns:
            The heading location relative to the repository root.
        """
        return f"{self.page.relative_to(ROOT)}:{self.line}"


API_SIGNATURE_BINDINGS: tuple[ApiSignatureBinding, ...] = (
    ApiSignatureBinding(API_DOCS / "fullmap.md", "resolve", live=fullmap_api.resolve),
    ApiSignatureBinding(API_DOCS / "lib.md", "resolve_many", live=lib_api.resolve_many),
    ApiSignatureBinding(API_DOCS / "qc.md", "fullmap_audit", live=qc_api.fullmap_audit),
    ApiSignatureBinding(API_DOCS / "utils.md", "namespace_uuid", stub="namespace_uuid"),
)

API_BINDINGS_BY_KEY: dict[tuple[str, str], ApiSignatureBinding] = {
    (binding.page.name, binding.function): binding for binding in API_SIGNATURE_BINDINGS
}

# Kind labels shared by the ast-parsed docs blocks and the `inspect` live signatures, so a
# positional-only (`/`) or keyword-only (`*`) marker drift fails with a readable message.
_LIVE_PARAMETER_KINDS: dict[Any, str] = {
    inspect.Parameter.POSITIONAL_ONLY: "positional-only",
    inspect.Parameter.POSITIONAL_OR_KEYWORD: "positional-or-keyword",
    inspect.Parameter.KEYWORD_ONLY: "keyword-only",
}


def _ast_default_repr(default: ast.expr | None, where: str) -> str | None:
    """Return the ``repr`` of a literal default expression, or ``None`` for no default.

    Args:
        default: The ``ast`` default node aligned to a parameter (``None`` when it has none).
        where: Location pointer for failure messages.

    Returns:
        ``repr`` of the literal default, or ``None``.
    """
    if default is None:
        return None
    try:
        value: Any = ast.literal_eval(default)
    except ValueError:
        pytest.fail(f"{where} default {ast.unparse(default)!r} is not a literal; this guard only compares literal defaults")
    return repr(value)


def _ast_signature_params(source: str, where: str, wanted: str | None = None) -> tuple[str, list[SignatureParam]]:
    """Parse a ``def`` signature out of Python source into normalized parameters.

    Args:
        source: Python source holding the function definition (a fenced docs block, or the
            ``rs.pyi`` stub).
        where: Location pointer for failure messages.
        wanted: Required function name, used when the source holds several definitions.

    Returns:
        The function name and its parameters in declaration order, with positional-only and
        keyword-only markers folded into each parameter's ``kind``.

    Raises:
        AssertionError: If the source declares no matching ``def``, or uses ``*args`` /
            ``**kwargs``, which the docs never show and this guard does not model.
    """
    # The docs present signatures without the trailing colon/body (`) -> pl.LazyFrame`),
    # which is not parseable Python; the rs.pyi stub ends each def with `: ...` already.
    source = source.rstrip()
    if source.endswith(":"):
        source += "\n    ..."
    elif not source.endswith("..."):
        source += ":\n    ..."
    all_defs: list[ast.FunctionDef | ast.AsyncFunctionDef] = [
        node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    ]
    definitions: list[ast.FunctionDef | ast.AsyncFunctionDef] = [node for node in all_defs if wanted is None or node.name == wanted]
    assert definitions, (
        f"{where} declares no def{f' named {wanted!r}' if wanted is not None else ''}; found {[node.name for node in all_defs] or 'none'}"
    )
    fn: ast.FunctionDef | ast.AsyncFunctionDef = definitions[0]
    args: ast.arguments = fn.args
    assert args.vararg is None, f"{where} `def {fn.name}` uses *args, which this guard does not model"
    assert args.kwarg is None, f"{where} `def {fn.name}` uses **kwargs, which this guard does not model"
    params: list[SignatureParam] = []
    positional: list[ast.arg] = [*args.posonlyargs, *args.args]
    kinds: list[str] = ["positional-only"] * len(args.posonlyargs) + ["positional-or-keyword"] * len(args.args)
    defaults: list[ast.expr | None] = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    for argument, kind, default in zip(positional, kinds, defaults, strict=True):
        params.append(SignatureParam(argument.arg, kind, _ast_default_repr(default, where)))
    for argument, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        params.append(SignatureParam(argument.arg, "keyword-only", _ast_default_repr(default, where)))
    return fn.name, params


def _api_signature_blocks() -> list[ApiSignatureBlock]:
    """Extract and parse the first ```python block under each ``### Function Signature`` heading.

    Returns:
        Parsed blocks in page order (``docs/api/*.md`` sorted), each bound to the function its
        ``def`` declares.

    Raises:
        AssertionError: If a ``### Function Signature`` heading anchors no ```python block --
            a heading without a block would silently stop guarding its function.
    """
    blocks: list[ApiSignatureBlock] = []
    for page in sorted(API_DOCS.glob("*.md")):
        text: str = page.read_text(encoding="utf-8")
        headings: list[re.Match[str]] = list(HEADING.finditer(text))
        for index, heading in enumerate(headings):
            if heading.group(2) != "Function Signature" or len(heading.group(1)) != 3:
                continue
            end: int = len(text)
            for later in headings[index + 1 :]:
                if len(later.group(1)) <= len(heading.group(1)):
                    end = later.start()
                    break
            line: int = text.count("\n", 0, heading.start()) + 1
            where: str = f"{page.relative_to(ROOT)}:{line}"
            fence: re.Match[str] | None = FENCED_PYTHON.search(text, heading.end(), end)
            assert fence is not None, f"{where} '### Function Signature' anchors no ```python block; the signature guard would go vacuous"
            function, params = _ast_signature_params(fence.group(1), where)
            blocks.append(ApiSignatureBlock(page=page, line=line, function=function, params=tuple(params)))
    return blocks


def _live_signature_params(binding: ApiSignatureBinding) -> list[SignatureParam]:
    """Return one binding's normalized live parameters, from ``inspect`` or the ``rs.pyi`` stub.

    Args:
        binding: Binding whose live authority is read.

    Returns:
        Live parameters in declaration order.

    Raises:
        AssertionError: If the live callable uses var-positional/var-keyword parameters, which
            this guard does not model.
    """
    if binding.live is None:
        _, params = _ast_signature_params(RS_STUB.read_text(encoding="utf-8"), str(RS_STUB.relative_to(ROOT)), wanted=binding.stub)
        return params
    live: list[SignatureParam] = []
    for parameter in inspect.signature(binding.live).parameters.values():
        kind: str | None = _LIVE_PARAMETER_KINDS.get(parameter.kind)
        assert kind is not None, (
            f"live `{binding.function}` parameter `{parameter.name}` is {parameter.kind}; this guard does not model var-positional/var-keyword"
        )
        default_repr: str | None = None if parameter.default is inspect.Parameter.empty else repr(parameter.default)
        live.append(SignatureParam(parameter.name, kind, default_repr))
    return live


def test_api_signatures_cover_all_documented_blocks() -> None:
    """Every documented ``### Function Signature`` block is bound to a live authority, and vice versa.

    ``API_SIGNATURE_BINDINGS`` is hand-written, so without this guard a NEW API signature block
    could ship unguarded while every parametrized case below still passed -- and a renamed page
    or function would leave a stale binding that silently stops guarding.
    """
    blocks: list[ApiSignatureBlock] = _api_signature_blocks()
    assert blocks, "no `### Function Signature` blocks found in docs/api; this guard went vacuous"
    discovered: set[tuple[str, str]] = {(block.page.name, block.function) for block in blocks}
    bound: set[tuple[str, str]] = set(API_BINDINGS_BY_KEY)
    assert len(bound) == len(API_SIGNATURE_BINDINGS), f"API_SIGNATURE_BINDINGS binds the same (page, function) twice: {sorted(bound)}"
    unbound: list[str] = sorted(f"{page}#{function}" for page, function in discovered - bound)
    assert not unbound, f"documented API signature blocks {unbound} have no ApiSignatureBinding; bind each to its live callable (or its rs.pyi stub)"
    stale: list[str] = sorted(f"{page}#{function}" for page, function in bound - discovered)
    assert not stale, f"API_SIGNATURE_BINDINGS name {stale}, but docs/api has no such `### Function Signature` block; fix or remove the binding(s)"


@pytest.mark.parametrize("binding", API_SIGNATURE_BINDINGS, ids=lambda binding: binding.location)
def test_api_signatures_match_live_authority(binding: ApiSignatureBinding) -> None:
    """A documented ``### Function Signature`` block matches its live authority parameter-for-parameter.

    US-003: ``fullmap.resolve`` shipped ``exclude_prefixes`` / ``exclude_regex`` and
    ``qc.fullmap_audit`` shipped ``on_phase`` with the API reference still showing the old
    parameter lists, and nothing failed. Names, order, positional-only/keyword-only kind, and
    defaults (``repr``) are compared; annotations are not, because the docs use ``Optional[...]``
    spelling where the source uses ``X | None``.

    Args:
        binding: One documented signature block bound to its live callable (or rs.pyi stub).
    """
    blocks: list[ApiSignatureBlock] = [
        block for block in _api_signature_blocks() if block.page == binding.page and block.function == binding.function
    ]
    assert blocks, f"{binding.location}: no `### Function Signature` block declares `def {binding.function}`; the docs dropped or renamed it"
    block: ApiSignatureBlock = blocks[0]
    live: list[SignatureParam] = _live_signature_params(binding)
    authority: str = "inspect.signature of the live callable" if binding.live is not None else f"the {RS_STUB.relative_to(ROOT)} stub"
    documented_shape: list[tuple[str, str]] = [(param.name, param.kind) for param in block.params]
    live_shape: list[tuple[str, str]] = [(param.name, param.kind) for param in live]
    assert documented_shape == live_shape, (
        f"{block.location} `def {binding.function}` parameter names/order/kinds drifted from {authority}\n"
        f"  documented: {documented_shape}\n"
        f"  live:       {live_shape}"
    )
    for documented_param, live_param in zip(block.params, live, strict=True):
        assert documented_param.default_repr == live_param.default_repr, (
            f"{block.location} `def {binding.function}` parameter `{documented_param.name}` documents default "
            f"{documented_param.default_repr}, but {authority} has {live_param.default_repr}"
        )


def test_api_utils_paths_distinguish_log_directory_from_log_file() -> None:
    """``docs/api/utils.md`` must not conflate ``log.LOGASSERT`` (the log DIRECTORY) with the log FILE inside it.

    Live: ``log.LOGASSERT`` is ``utils.BASE / "log"`` -- the ``.tablassert/log`` directory,
    created with ``mkdir`` at import when the ``log`` extra is installed -- while the loguru
    sink file is the private ``log._LOG_FILE`` at ``.tablassert/log/tablassert.log``. The
    reference once presented the file path AS ``log.LOGASSERT``, sending a reader who wanted
    the directory to a file.
    """
    # Live facts first, so a path change fails HERE rather than in the prose pins below.
    assert log_module.LOGASSERT == utils_module.BASE / "log", (
        f"live log.LOGASSERT is {log_module.LOGASSERT!r}, not utils.BASE / 'log'; re-derive this guard"
    )
    if find_spec("loguru") is not None:
        assert log_module.LOGASSERT.is_dir(), f"live log.LOGASSERT ({log_module.LOGASSERT}) is no longer a directory; re-derive this guard"
    assert log_module._LOG_FILE == log_module.LOGASSERT / "tablassert.log", (
        f"live log file is {log_module._LOG_FILE!r}, not LOGASSERT / 'tablassert.log'; re-derive this guard"
    )

    page: Path = API_DOCS / "utils.md"
    text: str = page.read_text(encoding="utf-8")
    start, end = _section_range(text, "Constants")
    paragraphs: list[str] = _paragraphs(text[start:end])

    # The directory: any paragraph naming LOGASSERT must call it a directory and give the
    # directory path, so a reader cannot mistake the constant for the file it holds.
    directory: str = str(log_module.LOGASSERT)
    logassert_paragraphs: list[str] = [paragraph for paragraph in paragraphs if "LOGASSERT" in paragraph]
    assert logassert_paragraphs, f"{page.relative_to(ROOT)} 'Constants' never names log.LOGASSERT"
    for paragraph in logassert_paragraphs:
        assert "directory" in paragraph.lower(), (
            f"{page.relative_to(ROOT)} must describe log.LOGASSERT as a directory (live: {log_module.LOGASSERT!r}); paragraph: {paragraph!r}"
        )
        assert f"`{directory}`" in paragraph or f"`{directory}/`" in paragraph, (
            f"{page.relative_to(ROOT)} must give log.LOGASSERT's directory path `{directory}`; paragraph: {paragraph!r}"
        )

    # The file: the page must name the log file path, and no paragraph may present that file
    # path AS log.LOGASSERT -- a paragraph naming both must keep the directory label on
    # LOGASSERT so the reader can tell them apart.
    log_file: str = str(log_module._LOG_FILE)
    file_paragraphs: list[str] = [paragraph for paragraph in paragraphs if "tablassert.log" in paragraph]
    assert any(f"`{log_file}`" in paragraph for paragraph in file_paragraphs), (
        f"{page.relative_to(ROOT)} 'Constants' must name the log file `{log_file}`"
    )
    for paragraph in file_paragraphs:
        if "LOGASSERT" in paragraph:
            assert "directory" in paragraph.lower(), (
                f"{page.relative_to(ROOT)} names the log file `{log_file}` and log.LOGASSERT together without calling LOGASSERT a "
                f"directory; that conflates the directory with the file inside it: {paragraph!r}"
            )


# --- Optional-extra enumerations (US-004) ---------------------------- #
# The `distill` extra shipped in pyproject.toml with the `tablassert distill-export` command
# while the README extras table, the installation guide, and both llms.txt enumerations still
# listed five extras -- and nothing failed. These guards derive the extra set from the live
# `[project.optional-dependencies]` table (parsed with stdlib tomllib; the sole extra authority)
# and the preflight set from live `extras.require(...)` / `extras.is_installed(...)` call sites
# under src/tablassert (an AST walk, never a copied list), so the next extra fails here the
# moment it is declared or preflighted without documentation.

PYPROJECT: Path = ROOT / "pyproject.toml"
LLMS_TXT: Path = ROOT / "llms.txt"
INSTALLATION: Path = DOCS / "installation.md"
SRC_TABLASSERT: Path = ROOT / "src" / "tablassert"

# Every surface that enumerates the optional extras for a reader deciding what to install --
# except llms.txt, which enumerates them TWICE (the Quickstart "Installation Guide" item and the
# Contributor Development "Project Metadata" item) and so gets its own scoped guard below:
# checking the whole file would let one enumeration silently drop an extra the other still named.
EXTRAS_SURFACES: tuple[Path, ...] = (README, INSTALLATION)

REQUIREMENT_NAME: re.Pattern[str] = re.compile(r"[<>=!~\[;\s]")
REQUIREMENT_SPECIFIER: re.Pattern[str] = re.compile(r"[<>=!~]")
EXTRA_TABLE_ROW: re.Pattern[str] = re.compile(r"^\|\s*`(?P<extra>[A-Za-z0-9_-]+)`\s*\|")
BACKTICKED_TOKEN: re.Pattern[str] = re.compile(r"`([^`]+)`")
LLMS_LIST_ITEM: re.Pattern[str] = re.compile(r"^-\s*\[(?P<label>[^\]]+)\]\([^)]*\):\s*(?P<body>.*)$")
LLMS_EXTRAS_ITEMS: tuple[str, str] = ("Installation Guide", "Project Metadata")
PREFLIGHT_FUNCTIONS: frozenset[str] = frozenset({"require", "is_installed"})


def _declared_extras() -> dict[str, list[str]]:
    """Return the live ``[project.optional-dependencies]`` table -- the sole extra authority.

    Returns:
        Extra name -> PEP 508 requirement strings, in pyproject declaration order.

    Raises:
        AssertionError: If pyproject declares no optional dependencies -- an empty set would
            make every parametrized guard below pass on zero cases.
    """
    parsed: dict[str, Any] = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    declared: Any = parsed["project"]["optional-dependencies"]
    assert isinstance(declared, dict), f"{PYPROJECT.name} [project.optional-dependencies] is not a table"
    assert declared, f"{PYPROJECT.name} declares no optional-dependencies; every extras guard below went vacuous"
    return {str(extra): [str(requirement) for requirement in requirements] for extra, requirements in declared.items()}


def _requirement_name(requirement: str) -> str:
    """Return the bare distribution name of one PEP 508 requirement string.

    Args:
        requirement: A requirement such as ``polars[rtcompat]>=1.40.1`` or ``datasets>=3.0.0``.

    Returns:
        The distribution name with any extras marker, specifier, and environment marker stripped
        (``polars``, ``datasets``).
    """
    return REQUIREMENT_NAME.split(requirement, maxsplit=1)[0]


def _preflight_call_sites() -> dict[str, list[str]]:
    """Return the extras every live ``extras.require`` / ``extras.is_installed`` call site guards.

    Walks the AST of every module under ``src/tablassert`` matching the ``extras.<fn>(...)``
    call shape (the definitions inside ``extras.py`` itself do not match it), so a command that
    gains a preflight is picked up the moment it lands -- never from a copied list.

    Returns:
        Extra name -> sorted ``path:line`` locations of its call sites.

    Raises:
        AssertionError: If a matched call passes a non-literal extra name, which this guard
            cannot derive.
    """
    sites: dict[str, list[str]] = {}
    for path in sorted(SRC_TABLASSERT.rglob("*.py")):
        tree: ast.Module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func: ast.expr = node.func
            if not (
                isinstance(func, ast.Attribute)
                and func.attr in {"require", "is_installed"}
                and isinstance(func.value, ast.Name)
                and func.value.id == "extras"
            ):
                continue
            assert node.args, f"{path.relative_to(ROOT)}:{node.lineno} calls extras.{func.attr} with no positional extra; this guard cannot derive it"
            first: ast.expr = node.args[0]
            assert isinstance(first, ast.Constant), (
                f"{path.relative_to(ROOT)}:{node.lineno} calls extras.{func.attr} without a literal extra; this guard cannot derive it"
            )
            assert isinstance(first.value, str), (
                f"{path.relative_to(ROOT)}:{node.lineno} calls extras.{func.attr} with a non-string extra; this guard cannot derive it"
            )
            sites.setdefault(first.value, []).append(f"{path.relative_to(ROOT)}:{node.lineno}")
    return {extra: sorted(locations) for extra, locations in sites.items()}


def _optional_extras_table() -> dict[str, str]:
    """Return ``{extra: Includes cell}`` parsed from the installation guide's Optional Extras table.

    Returns:
        Documented extra -> raw ``Includes`` cell text, in table order.

    Raises:
        AssertionError: If a row lacks the Extra | Description | Includes shape, or no backticked
            extra row is found at all -- a table the guard cannot read would silently stop
            guarding.
    """
    text: str = INSTALLATION.read_text(encoding="utf-8")
    start, end = _section_range(text, "Optional Extras")
    rows: dict[str, str] = {}
    for line in text[start:end].splitlines():
        if not EXTRA_TABLE_ROW.match(line.strip()):
            continue
        cells: list[str] = _row_cells(line.strip())
        assert len(cells) == 3, f"{INSTALLATION.relative_to(ROOT)} Optional Extras row must be Extra | Description | Includes: {line.strip()!r}"
        rows[cells[0].strip("`")] = cells[2]
    assert rows, f"{INSTALLATION.relative_to(ROOT)} 'Optional Extras' anchors no backticked extra rows; the table parser broke"
    return rows


def test_declared_extras_guard_is_not_vacuous() -> None:
    """The pyproject-derived extra set exists, so the parametrized extras guards cannot pass on zero cases."""
    assert _declared_extras(), f"{PYPROJECT.name} declares no optional extras; every extras guard in this module went vacuous"


@pytest.mark.parametrize("extra", sorted(_declared_extras()))
@pytest.mark.parametrize("page", EXTRAS_SURFACES, ids=lambda page: str(page.relative_to(ROOT)))
def test_declared_extras_documented_on_every_extras_surface(page: Path, extra: str) -> None:
    """Every extra declared in pyproject.toml is named (backticked) on each extras enumeration surface.

    US-004: ``distill`` shipped in ``[project.optional-dependencies]`` while the README extras
    table, the installation guide, and both ``llms.txt`` enumerations still listed five extras.
    The set under test is parsed from the live pyproject, so the NEXT extra fails here the moment
    it is declared.

    Args:
        page: One surface that enumerates the extras (README, installation guide, llms.txt).
        extra: One extra declared in the live ``[project.optional-dependencies]``.
    """
    text: str = page.read_text(encoding="utf-8")
    assert f"`{extra}`" in text, (
        f"{page.relative_to(ROOT)} never names the `{extra}` extra declared in {PYPROJECT.name} [project.optional-dependencies]; "
        "add it to every extras enumeration (table row, opening gloss, or list)"
    )


def _llms_extras_enumerations() -> dict[str, str]:
    """Return the bodies of both labeled optional-extra enumerations in ``llms.txt``.

    Returns:
        Mapping from the live list-item label to its extras-enumeration body.

    Raises:
        AssertionError: If either expected list item is missing or duplicated, which would make
            the per-extra checks below silently inspect the wrong surface.
    """
    text: str = LLMS_TXT.read_text(encoding="utf-8")
    matches: list[re.Match[str]] = [
        match for line in text.splitlines() if (match := LLMS_LIST_ITEM.match(line)) and match.group("label") in LLMS_EXTRAS_ITEMS
    ]
    enumerations: dict[str, str] = {match.group("label"): match.group("body") for match in matches}
    assert set(enumerations) == set(LLMS_EXTRAS_ITEMS), (
        f"{LLMS_TXT.name} is missing one of the expected extras enumerations {LLMS_EXTRAS_ITEMS}; found {tuple(enumerations)}"
    )
    assert len(matches) == len(enumerations), f"{LLMS_TXT.name} contains a duplicate extras enumeration label"
    return enumerations


@pytest.mark.parametrize("extra", sorted(_declared_extras()))
def test_declared_extras_documented_in_both_llms_enumerations(extra: str) -> None:
    """Every pyproject extra is named in both extras lists in ``llms.txt``."""
    for label, body in _llms_extras_enumerations().items():
        assert f"`{extra}`" in body, f"{LLMS_TXT.name} {label!r} enumeration is missing `{extra}`"


def test_installation_preflight_docs_cover_source_preflight_calls() -> None:
    """The installation guide's preflight section covers every extra the SOURCE preflights.

    The extras under test come from walking ``extras.require(...)`` / ``extras.is_installed(...)``
    call sites under ``src/tablassert`` (AST, not a copied list), so a new preflighted command
    fails here until the "When an extra is missing" section names its extra. The two documented
    exceptions are pinned from the same section: ``rt`` installs ``polars[rtcompat]``, which
    imports as plain ``polars`` and so cannot be detected by inspection, and ``log`` is never
    preflighted because a base install simply produces no logs.
    """
    sites: dict[str, list[str]] = _preflight_call_sites()
    assert sites, "no extras.require/extras.is_installed call sites found under src/tablassert; this guard went vacuous"
    unknown: list[str] = sorted(set(sites) - set(_declared_extras()))
    assert not unknown, f"source preflights {unknown}, which {PYPROJECT.name} does not declare; the install hint would be a dead end"
    text: str = INSTALLATION.read_text(encoding="utf-8")
    start, end = _section_range(text, "When an extra is missing")
    section: str = text[start:end]
    for extra, locations in sorted(sites.items()):
        assert f"`[{extra}]`" in section, (
            f"{INSTALLATION.relative_to(ROOT)} 'When an extra is missing' never names `[{extra}]`, but the source preflights it at "
            f"{locations}; document where the check fires"
        )
    assert "polars[rtcompat]" in section, (
        f"{INSTALLATION.relative_to(ROOT)} must keep the `rt` exception: polars[rtcompat] imports as plain polars and cannot be detected"
    )
    assert "no logs" in section, f"{INSTALLATION.relative_to(ROOT)} must keep the `log` exception: without loguru, Tablassert produces no logs at all"
    if "distill" in sites:
        # Live: the agent command's `--distill` flag records ChatML NDJSON with zero extra
        # dependencies (cli.py), so the section must not imply RECORDING needs the extra --
        # only the `distill-export` step does.
        assert "agent --distill" in section, (
            f"{INSTALLATION.relative_to(ROOT)} must state that recording via `agent --distill` is zero-dependency while exporting needs the extra"
        )
        assert "zero" in section.lower(), (
            f"{INSTALLATION.relative_to(ROOT)} must state that recording via `agent --distill` is zero-dependency while exporting needs the extra"
        )


def test_installation_extra_package_sets_match_pyproject_requirements() -> None:
    """The Optional Extras table's ``Includes`` cells match the live pyproject requirements.

    Both directions, both live-derived: the table must carry a row for every declared extra (and
    no row for a dropped one), each row must name every distribution its extra installs (and no
    distribution it does not), and a documented version specifier must be the one pyproject
    declares -- a stale ``aria2==...`` or ``datasets>=...`` is install advice that has drifted.
    Parenthesized asides (``torch`` / ``numpy`` arriving transitively, the ``aria2c`` import
    name) are not part of the Includes claim and are excluded by cutting the cell at the first
    ``(``.
    """
    declared: dict[str, list[str]] = _declared_extras()
    documented: dict[str, str] = _optional_extras_table()
    missing: list[str] = sorted(set(declared) - set(documented))
    assert not missing, (
        f"{INSTALLATION.relative_to(ROOT)} Optional Extras table has no row for {missing}, declared in {PYPROJECT.name}; add the row(s)"
    )
    stale: list[str] = sorted(set(documented) - set(declared))
    assert not stale, f"{INSTALLATION.relative_to(ROOT)} Optional Extras table documents {stale}, no longer declared in {PYPROJECT.name}"
    for extra, requirements in declared.items():
        includes: str = documented[extra].split("(", 1)[0]
        tokens: list[str] = BACKTICKED_TOKEN.findall(includes)
        live_names: list[str] = sorted(_requirement_name(requirement) for requirement in requirements)
        documented_names: list[str] = sorted(_requirement_name(token) for token in tokens)
        assert documented_names == live_names, (
            f"{INSTALLATION.relative_to(ROOT)} `[{extra}]` Includes {tokens or 'nothing'}, but {PYPROJECT.name} declares {requirements}; "
            "the row must name every distribution the extra installs, and no others"
        )
        for token in tokens:
            if REQUIREMENT_SPECIFIER.search(token):
                assert token in requirements, (
                    f"{INSTALLATION.relative_to(ROOT)} `[{extra}]` documents requirement `{token}`, but {PYPROJECT.name} declares "
                    f"{requirements}; re-sync the version specifier"
                )


# --- LLM index source-of-truth guards (US-005) ------------------------ #
# llms.txt is intentionally terse, but it is still an index of shipped surfaces. These guards
# derive the expected module, command, workflow, Graph-field, Biolink, and MkDocs-nav sets from
# live files at test time so a new surface cannot silently disappear from the index.


def _llms_text() -> str:
    """Return the generated-model index source."""
    return LLMS_TXT.read_text(encoding="utf-8")


def _llms_links() -> list[tuple[str, str]]:
    """Return every Markdown link in llms.txt as ``(label, target)`` pairs."""
    return [(match.group(1), match.group(2)) for match in MARKDOWN_LINK.finditer(_llms_text())]


def _llms_targets() -> set[str]:
    """Return link targets in llms.txt, preserving relative paths and dropping URL fragments."""
    return {target.split("#", 1)[0] for _, target in _llms_links()}


def test_llms_relative_link_targets_exist() -> None:
    """Every relative llms.txt link resolves to a checked-in file or directory."""
    missing: list[str] = []
    for _, target in _llms_links():
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        path = ROOT / target.split("#", 1)[0]
        if not path.exists():
            missing.append(target)
    assert not missing, f"llms.txt has missing relative link targets: {missing}"


def test_llms_indexes_every_live_python_module_and_rust_extension() -> None:
    """Every public Python module and the canonical compiled Rust stub are indexed by path."""
    targets: set[str] = _llms_targets()
    modules: list[Path] = sorted(path for path in SRC_TABLASSERT.rglob("*.py") if path.name == "__init__.py" or not path.stem.startswith("_"))
    assert modules, f"{SRC_TABLASSERT} contains no public Python modules; this guard went vacuous"
    missing: list[str] = [str(path.relative_to(ROOT)) for path in modules if str(path.relative_to(ROOT)) not in targets]
    assert not missing, f"llms.txt omits live Python modules: {missing}"
    rust_stub: str = str(RS_STUB.relative_to(ROOT))
    assert rust_stub in targets, f"llms.txt must reference the canonical Rust extension stub {rust_stub}"


def _llms_entry_body(label: str) -> str:
    """Return the body of the unique root-relative index entry with ``label``."""
    pattern = re.compile(rf"^-\s*\[{re.escape(label)}\]\([^)]*\):\s*(?P<body>.*)$", re.MULTILINE)
    found: list[str] = [match.group("body") for match in pattern.finditer(_llms_text())]
    assert len(found) == 1, f"llms.txt must contain exactly one [{label}] entry; found {len(found)}"
    return found[0]


def _llms_unique_entry_body(label: str, target: str) -> str:
    """Return the body of the unique index entry identified by label and target."""
    pattern = re.compile(rf"^-\s*\[{re.escape(label)}\]\({re.escape(target)}\):\s*(?P<body>.*)$", re.MULTILINE)
    found: list[str] = [match.group("body") for match in pattern.finditer(_llms_text())]
    assert len(found) == 1, f"llms.txt must contain exactly one [{label}] entry for {target}; found {len(found)}"
    return found[0]


def test_llms_enums_and_biolink_entries_follow_live_symbols() -> None:
    """The enum entries distinguish Tablassert-owned and Biolink-derived vocabularies."""
    enum_entry: str = _llms_entry_body("Enums Catalog")
    live_enums: set[str] = {
        name for name, obj in vars(enums_api).items() if inspect.isclass(obj) and issubclass(obj, Enum) and obj.__module__ == enums_api.__name__
    }
    assert live_enums, "tablassert.enums exposes no live Enum classes; this guard went vacuous"
    documented_enums: set[str] = set(re.findall(r"`([A-Za-z_][A-Za-z0-9_]*)`", enum_entry))
    assert documented_enums == live_enums, f"Enums Catalog symbols {sorted(documented_enums)} differ from live vocabularies {sorted(live_enums)}"
    live_biolink_enums: set[str] = {
        name for name, obj in vars(biolink_api).items() if inspect.isclass(obj) and issubclass(obj, Enum) and name in biolink_api.__all__
    }
    biolink_surface: set[str] = live_biolink_enums | {"ALLOWED_EDGE_FIELDS", "validate_kgx"}
    biolink_entry: str = _llms_entry_body("Biolink Vocabulary")
    documented_biolink: set[str] = set(re.findall(r"`([A-Za-z_][A-Za-z0-9_]*)`", biolink_entry))
    assert documented_biolink == biolink_surface, (
        f"Biolink Vocabulary symbols {sorted(documented_biolink)} differ from live surface {sorted(biolink_surface)}"
    )
    biolink_in_enums: set[str] = live_biolink_enums & documented_enums
    assert not biolink_in_enums, "Enums Catalog names Biolink-owned vocabularies"


def test_llms_graph_entry_matches_live_fields() -> None:
    """The Graph entry names only the live required/optional fields, never retired description."""
    entry: str = _llms_entry_body("Graph Configuration Reference")
    required: set[str] = {name for name, field in Graph.model_fields.items() if field.is_required()}
    optional: set[str] = set(Graph.model_fields) - required
    assert required
    assert optional
    documented: set[str] = set(re.findall(r"`([A-Za-z_][A-Za-z0-9_]*)`", entry))
    assert documented == required | optional, f"Graph entry fields {sorted(documented)} differ from live fields {sorted(required | optional)}"
    assert "description" not in documented, "Graph entry still presents retired description as a graph field"


def test_llms_cli_entry_matches_live_commands() -> None:
    """The CLI index lists exactly the live commands and app-level version flags."""
    entry: str = _llms_unique_entry_body("CLI Reference", "docs/cli.md")
    commands: set[str] = {name for name in APP.resolved_commands() if not name.startswith("-")}
    assert commands, "live CLI exposes no commands; this guard went vacuous"
    assert all(re.search(rf"(?<![\w-]){re.escape(command)}(?![\w-])", entry) for command in commands), (
        f"CLI entry omits live commands: {sorted(commands)}"
    )
    version_flags: tuple[str, ...] = tuple(APP.version_flags)
    assert version_flags, "live CLI exposes no app-level version flags; this guard went vacuous"
    assert all(re.search(rf"(?<![\w-]){re.escape(flag)}(?![\w-])", entry) for flag in version_flags), (
        f"CLI entry omits app-level version flags: {sorted(version_flags)}"
    )
    documented: list[str] = re.findall(r"`([^`]+)`", entry)
    assert len(documented) == len(commands) + len(version_flags), "CLI entry has stale or duplicate command/version tokens"
    assert set(documented) == commands | set(version_flags), f"CLI entry tokens {sorted(documented)} differ from live CLI surface"


def _workflow_purpose_terms(path: Path) -> set[str]:
    """Derive reader-facing purpose terms from one workflow's jobs, steps, and actions."""
    workflow: Any = yaml.load(path.read_text(encoding="utf-8"), Loader=CSafeLoader)
    assert isinstance(workflow, dict), f"{path.name} has no parseable workflow"
    jobs: Any = workflow.get("jobs")
    assert isinstance(jobs, dict), f"{path.name} has no parseable jobs"
    job_text: str = " ".join(
        str(value)
        for job in jobs.values()
        if isinstance(job, dict)
        for value in [
            job.get("name", ""),
            job.get("run", ""),
            job.get("uses", ""),
            *[
                field
                for step in job.get("steps", [])
                if isinstance(step, dict)
                for field in (step.get("name", ""), step.get("run", ""), step.get("uses", ""))
            ],
        ]
    ).lower()
    signals: dict[str, tuple[str, ...]] = {
        "lint": ("lint", "ruff"),
        "format": ("format",),
        "type": ("type", "pyright"),
        "test": ("test", "pytest"),
        "rust": ("rust", "cargo"),
        "mkdocs": ("mkdocs",),
        "deploy": ("deploy", "gh-pages"),
        "pypi": ("pypi",),
        "sdist": ("sdist",),
        "wheel": ("wheel",),
        "tag": ("tag", "git tag"),
        "version": ("version",),
    }
    return {term for term, markers in signals.items() if any(marker in job_text for marker in markers)}


def test_llms_workflow_entry_matches_live_workflows_and_purposes() -> None:
    """Each workflow filename is paired with purpose terms derived from its own jobs and steps."""
    entry: str = _llms_entry_body("GitHub Workflows")
    workflow_dir: Path = ROOT / ".github" / "workflows"
    workflows: list[Path] = sorted(path for path in workflow_dir.iterdir() if path.is_file() and path.suffix in {".yml", ".yaml"})
    assert workflows, "no GitHub workflows found; this guard went vacuous"
    assert "docker" not in entry.lower(), "GitHub Workflows entry mentions retired Docker automation"
    descriptions: list[tuple[str, str]] = [
        (match.group("name"), match.group("purpose")) for match in re.finditer(r"`(?P<name>[^`]+\.ya?ml)`\s+(?P<purpose>.*?)(?=;\s*`|$)", entry)
    ]
    live_names: set[str] = {path.name for path in workflows}
    for name in sorted(live_names):
        matches: list[str] = [purpose for description_name, purpose in descriptions if description_name == name]
        assert len(matches) == 1, f"GitHub Workflows entry must describe {name} exactly once; found {len(matches)}"
    extra_names: set[str] = {name for name, _ in descriptions} - live_names
    assert not extra_names, f"GitHub Workflows entry has non-live workflows: {sorted(extra_names)}"
    live_purpose_terms: dict[str, set[str]] = {path.name: _workflow_purpose_terms(path) for path in workflows}
    assert all(live_purpose_terms.values()), "a workflow yielded no workflow purpose terms; this guard went vacuous"
    all_purpose_terms: set[str] = set().union(*live_purpose_terms.values())
    for path in workflows:
        purpose_terms: set[str] = live_purpose_terms[path.name]
        description: str = next(purpose for name, purpose in descriptions if name == path.name)
        description_lower: str = description.lower()
        missing: set[str] = {
            term for term in purpose_terms if not re.search(rf"(?<![\w-]){re.escape(term)}(?:s|ed|ing)?(?![\w-])", description_lower)
        }
        assert not missing, f"GitHub Workflows entry gives {path.name} the wrong or incomplete purpose; missing {sorted(missing)}"
        claimed: set[str] = {
            term for term in all_purpose_terms if re.search(rf"(?<![\w-]){re.escape(term)}(?:s|ed|ing)?(?![\w-])", description_lower)
        }
        extra: set[str] = claimed - purpose_terms
        assert not extra, f"GitHub Workflows entry gives {path.name} unsupported purposes; extra {sorted(extra)}"


def _all_nav_leaf_pages(target: Any) -> list[str]:
    """Flatten a live MkDocs nav target into docs-relative leaf pages."""
    if isinstance(target, str):
        return [target]
    if isinstance(target, list):
        pages: list[str] = []
        for child in target:
            pages.extend(_all_nav_leaf_pages(next(iter(child.values())) if isinstance(child, dict) else child))
        return pages
    return []


def test_llms_indexes_every_mkdocs_page_and_changelog_pointer() -> None:
    """Every MkDocs leaf page is indexed, except docs/changelog.md via root CHANGELOG.md."""
    nav: Any = _mkdocs()["nav"]
    pages: list[str] = [_page for item in nav for target in item.values() for _page in _all_nav_leaf_pages(target)]
    targets: set[str] = _llms_targets()
    assert pages, "mkdocs.yml nav has no leaf pages; this guard went vacuous"
    for page in pages:
        if page == "changelog.md":
            assert "CHANGELOG.md" in targets, "llms.txt must represent docs/changelog.md with the root CHANGELOG.md pointer"
        else:
            expected: str = f"docs/{page}"
            assert expected in targets, f"llms.txt omits MkDocs nav page {expected}"
    assert "docs/agent.md" in targets, "llms.txt must index docs/agent.md"
    assert "examples/agent/README.md" in targets, "llms.txt must index examples/agent/README.md"


def test_llms_preserves_authority_and_migration_notes() -> None:
    """The authority, datassert migration, and redb co-location notes remain exact."""
    text: str = _llms_text()
    assert "When source code and prose docs disagree, treat `src/tablassert/models.py` and `src/tablassert/cli.py` as the current authority." in text
    assert (
        "If you encounter older configurations, migrate `datassert:` to `fullmap:` (the graph-config field pointing at the entity-resolution database)."
        in text
    )
    assert (
        "Current CLI behavior queries an embedded redb database whose primary file is `fullmap/data/fullmap.redb`; its sibling RECORDS shard files (`fullmap.s*.redb`, in the same directory) are required for lookup and must stay co-located with it."
        in text
    )


def test_llms_contains_no_retired_config_labels() -> None:
    """Tutorial indexing does not carry the retired TC<n>/GC<n> labels."""
    assert re.search(r"\b(?:TC|GC)\d+\b", _llms_text()) is None


# --- Retired CLI flags must stay retired in live docs ----------------- #
# `build-fullmap --aria2c` / `-a` was removed (the downloader is now selected automatically
# from the optional [aria2] extra), yet afterwards every live documentation surface still
# documented the flag. The live docs are the source of truth readers copy commands from, so a
# retired flag mentioned there silently misleads users into an unknown-option parse error.
# CHANGELOG.md is excluded on purpose: history legitimately names retired flags when it
# retires them.

# Entries are literal substrings no live doc may contain. `--aria2c` is the retired long flag
# itself; its short twin `-a` is far too generic to grep raw (prose and unrelated flags match),
# so it is guarded in the docs/cli.md option-table CELL shape instead -- `| `-a`` for a
# short-flag-only cell and `, `-a`` for the `| `--long`, `-a` |` pairing -- which is how a stale
# row for the removed flag would necessarily appear.
RETIRED_CLI_FLAGS: frozenset[str] = frozenset({"--aria2c", "| `-a`", ", `-a`"})


def _live_doc_surfaces() -> list[Path]:
    """Return every live documentation surface a reader may copy commands from.

    The published ``docs/`` corpus plus the README, ``CONTRIBUTING.md``, and the ``llms.txt``
    index. ``CHANGELOG.md`` is deliberately absent: history legitimately names retired flags
    when it retires them.

    Returns:
        The live documentation surfaces, README first.
    """
    return [*_markdown_pages(), ROOT / "CONTRIBUTING.md", LLMS_TXT]


@pytest.mark.parametrize("flag", sorted(RETIRED_CLI_FLAGS))
@pytest.mark.parametrize("page", _live_doc_surfaces(), ids=lambda page: str(page.relative_to(ROOT)))
def test_live_docs_mention_no_retired_cli_flags(page: Path, flag: str) -> None:
    """No live documentation surface mentions a retired CLI flag.

    ``build-fullmap --aria2c`` / ``-a`` was removed -- the downloader is now selected
    automatically from the optional ``[aria2]`` extra -- yet afterwards every live surface
    still documented the flag. Docs are the source of truth readers copy commands from, so a
    retired flag mentioned in live docs silently misleads users into an unknown-option parse
    error (passing the retired flag fails parsing before the command ever runs).

    Args:
        page: One live documentation surface.
        flag: One retired-flag pattern -- a flag, or the option-table cell shape of the
            too-generic short ``-a`` -- that must not appear on any live surface.
    """
    text: str = page.read_text(encoding="utf-8")
    assert flag not in text, (
        f"{page.relative_to(ROOT)} still mentions the retired flag pattern {flag}; live docs are the source of truth readers copy from, "
        "and a retired flag silently misleads them into an unknown-option parse error (only CHANGELOG.md history may name it)"
    )


# --- Agent page dependency tables and flag reminder (US-006) ------------ #
# docs/agent.md presented the [agent]/[optimize] requirements as `==` pins (and litellm as
# `(any)`) where pyproject declares lower bounds, and its compact flag reminder never listed
# `--task-model` or `--distill` -- and nothing failed. These guards parse the requirement
# strings from the live `[project.optional-dependencies]` table (tomllib) and the flag set
# from the live `tablassert.cli.agent` callback signature plus its cyclopts.Parameter
# metadata, so a pyproject bump or a new agent flag fails here until the page is re-synced.

AGENT_DOC: Path = DOCS / "agent.md"


def _agent_doc_text() -> str:
    """Return the live agent documentation page source."""
    return AGENT_DOC.read_text(encoding="utf-8")


def _agent_installation_section(text: str) -> str:
    """Return the agent page's Installation section body, delimited by the next ``## `` heading.

    ``_section_range`` cannot scope this section: its fenced bash block carries a ``#`` comment
    line that the shared heading regex reads as a level-1 heading, truncating the span before the
    dependency tables.

    Args:
        text: Full ``docs/agent.md`` source.

    Returns:
        The section body between ``## Installation`` and the next level-2 heading.
    """
    parts: list[str] = text.split("## Installation", 1)
    assert len(parts) == 2, f"{AGENT_DOC.relative_to(ROOT)} no longer holds exactly one '## Installation' heading; the section anchor is stale"
    return parts[1].split("\n## ", 1)[0]


def test_agent_doc_dependencies_quote_pyproject_requirements() -> None:
    """The [agent]/[optimize] dependency tables quote the live pyproject requirement strings verbatim.

    ``pyproject.toml`` declares lower bounds (``smolagents>=1.26.0``, ``litellm>=1.93.0``,
    ``dspy>=3.2.1``); the page once showed ``==`` pins and a litellm ``(any)`` instead. The
    verbatim strings and the absence of stale ``==`` pins are both checked against the live
    ``[project.optional-dependencies]`` table -- never a copied list -- so a version bump in
    pyproject fails here until the page is re-quoted.
    """
    declared: dict[str, list[str]] = _declared_extras()
    assert {"agent", "optimize"} <= set(declared), f"{PYPROJECT.name} no longer declares the [agent]/[optimize] extras; re-derive this guard"
    text: str = _agent_doc_text()
    section: str = _agent_installation_section(text)
    assert "pins" not in section, (
        f"{AGENT_DOC.relative_to(ROOT)} Installation still calls the requirements 'pins'; {PYPROJECT.name} declares lower bounds"
    )
    assert "lower bound" in section, (
        f"{AGENT_DOC.relative_to(ROOT)} Installation must say the extras declare lower bounds (minimum versions), not pins"
    )
    for extra in ("agent", "optimize"):
        for requirement in declared[extra]:
            assert f"`{requirement}`" in section, (
                f"{AGENT_DOC.relative_to(ROOT)} Installation must quote the live `[{extra}]` requirement `{requirement}` verbatim "
                f"({PYPROJECT.name} [project.optional-dependencies])"
            )
            name: str = _requirement_name(requirement)
            assert f"{name}==" not in text, (
                f"{AGENT_DOC.relative_to(ROOT)} carries a stale `==` pin for `{name}`; {PYPROJECT.name} declares `{requirement}` (a lower bound)"
            )
    assert not re.search(r"`==\d", section), (
        f"{AGENT_DOC.relative_to(ROOT)} Installation still shows backticked `==` pinned versions; quote the live requirement strings instead"
    )


def _agent_command_flags() -> dict[str, tuple[str, ...]]:
    """Return the live ``agent`` command's explicit flag names per parameter.

    Derived from ``inspect.signature(tablassert.cli.agent)`` plus the
    ``Annotated[..., cyclopts.Parameter(name=[...])]`` metadata -- the same authority
    ``tests/test_docs_cli_coverage.py`` uses -- never a copied list.

    Returns:
        Parameter name -> explicit flag/alias names (empty for the positional PMC ids).
    """
    signature: inspect.Signature = inspect.signature(cli_module.agent)
    type_hints: dict[str, Any] = get_type_hints(cli_module.agent, include_extras=True)
    flags: dict[str, tuple[str, ...]] = {}
    for python_name, parameter in signature.parameters.items():
        annotation: Any = type_hints.get(python_name, parameter.annotation)
        names: tuple[str, ...] = ()
        if get_origin(annotation) is Annotated:
            for metadata in get_args(annotation)[1:]:
                if isinstance(metadata, cyclopts.Parameter):
                    raw: Any = metadata.name
                    names = (raw,) if isinstance(raw, str) else tuple(raw or ())
        flags[python_name] = names
    return flags


def _agent_model_config_parameters() -> frozenset[str]:
    """Return the agent parameters the page documents in its own Model configuration table.

    Those parameters are exempt from the "Running it for real" reminder list because the page
    gives them a dedicated flag/env-var table. The set is derived from the live callback
    docstring: a parameter whose ``Args:`` entry names a ``TABLASSERT_AGENT_`` env var.

    Returns:
        Live agent parameter names with a documented env-var fallback.
    """
    docstring: str = inspect.getdoc(cli_module.agent) or ""
    entries: list[re.Match[str]] = list(re.finditer(r"^    (\w+):", docstring, re.MULTILINE))
    assert entries, "the live agent callback docstring lost its Args: entries; re-derive this guard"
    model_config: set[str] = set()
    for index, entry in enumerate(entries):
        stop: int = entries[index + 1].start() if index + 1 < len(entries) else len(docstring)
        if "TABLASSERT_AGENT_" in docstring[entry.start() : stop]:
            model_config.add(entry.group(1))
    assert model_config, "no live agent parameter documents a TABLASSERT_AGENT_ env-var fallback; re-derive this guard"
    return frozenset(model_config)


def test_agent_doc_flag_reminder_lists_every_live_agent_flag() -> None:
    """The compact flag reminder names every live agent flag and keeps cli.md#agent authoritative.

    ``--task-model`` and ``--distill`` shipped on the ``agent`` command while the page's compact
    reminder still ended at the older flag set -- and nothing failed. The expected set is derived
    from ``inspect.signature(tablassert.cli.agent)`` plus ``cyclopts.Parameter`` metadata: every
    parameter with explicit flags must appear in the reminder paragraph -- the optimize-path and
    gate flags included -- except the model-config triple the page documents in its own Model
    configuration table (exempted live via the callback docstring's ``TABLASSERT_AGENT_``
    env-var fallbacks). The reminder must keep pointing at ``docs/cli.md#agent`` as the
    authoritative flag table.
    """
    flags: dict[str, tuple[str, ...]] = _agent_command_flags()
    assert flags, "the live agent command exposes no parameters; this guard went vacuous"
    exempt: frozenset[str] = _agent_model_config_parameters()
    unknown_exemptions: list[str] = sorted(exempt - set(flags))
    assert not unknown_exemptions, f"docstring env-var fallbacks {unknown_exemptions} are not live agent parameters; re-derive this guard"
    text: str = _agent_doc_text()
    start, end = _section_range(text, "Running it for real")
    reminders: list[str] = [paragraph for paragraph in _paragraphs(text[start:end]) if "cli.md#agent" in paragraph]
    assert len(reminders) == 1, (
        f"{AGENT_DOC.relative_to(ROOT)} 'Running it for real' must hold exactly one compact-reminder paragraph pointing at cli.md#agent; "
        f"found {len(reminders)}"
    )
    reminder: str = reminders[0]
    assert "authoritative" in reminder, (
        f"{AGENT_DOC.relative_to(ROOT)} reminder must keep stating that docs/cli.md#agent is the authoritative flag table"
    )
    missing: list[str] = []
    for python_name, names in flags.items():
        if python_name in exempt or not names:
            continue
        long_flags: list[str] = [name for name in names if name.startswith("--")]
        assert long_flags, f"live agent parameter `{python_name}` has aliases but no long flag; this guard cannot check it"
        if not any(re.search(rf"(?<![\w-]){re.escape(flag)}(?![\w-])", reminder) for flag in long_flags):
            missing.append(f"{python_name} ({', '.join(names)})")
    assert not missing, (
        f"{AGENT_DOC.relative_to(ROOT)} compact flag reminder omits live agent flags: {missing}; "
        "add them to the reminder or remove the flag from tablassert.cli.agent"
    )


# --- Contributor-guide guards (US-007) ---------------------------- #
# CONTRIBUTING.md drifted from the automation it describes: it claimed CI shards the test suite
# across four runners while `.github/workflows/ci.yml` deliberately runs it as a single job, it
# hardcoded rotting numbers (test counts, coverage percentages), its pre-commit install rationale
# contradicted `.pre-commit-config.yaml`'s `default_install_hook_types`, and its Project layout
# block omitted live paths. These guards derive every expectation from the live files -- the
# Makefile's `.PHONY` line, the workflow YAML, the pre-commit config, pyproject's pytest addopts,
# and the filesystem itself -- never from copied lists. The layout guard enforces only the
# documented shape, real listed paths, and the three US-007 story additions; it is not exhaustive.

CONTRIBUTING: Path = ROOT / "CONTRIBUTING.md"
MAKEFILE: Path = ROOT / "Makefile"
CI_WORKFLOW: Path = ROOT / ".github" / "workflows" / "ci.yml"
PRE_COMMIT_CONFIG: Path = ROOT / ".pre-commit-config.yaml"

FENCED_BASH: re.Pattern[str] = re.compile(r"^```(?:bash|sh)[^\S\n]*\n(.*?)^```[^\S\n]*$", re.MULTILINE | re.DOTALL)
FENCED_TEXT: re.Pattern[str] = re.compile(r"^```text[^\S\n]*\n(.*?)^```[^\S\n]*$", re.MULTILINE | re.DOTALL)
MAKE_TARGET: re.Pattern[str] = re.compile(r"`make ([a-z][a-z0-9-]*)`")
SYNC_FLAG: re.Pattern[str] = re.compile(r"--no-default-groups\b|--(?:group|extra)\s+[A-Za-z0-9_.-]+")
NEGATED_SHARD: re.Pattern[str] = re.compile(
    r"\b(?:not|no)\s+(?:[\w'-]+\s+){0,4}shard\w*\b|\b(?:rather than|instead of|without)\s+(?:[\w'-]+\s+){0,4}shard\w*\b", re.IGNORECASE
)


def _sync_flags(text: str) -> set[str]:
    """Return normalized dependency flags from a CI sync-args string or Markdown line.

    Markdown punctuation and backticks are excluded from the argument token, so the same parser
    can compare the live workflow value with an inline command in ``CONTRIBUTING.md``.

    Args:
        text: Raw workflow ``SYNC_ARGS`` or a documentation line containing it.

    Returns:
        The set of complete sync flags, including each option's argument where applicable.
    """
    return {match.group(0).strip("`.,;:()[]{}") for match in SYNC_FLAG.finditer(text)}


def _ci_workflow() -> dict[str, Any]:
    """Return the parsed live ``ci.yml`` -- the CI source of truth.

    Returns:
        The workflow configuration as a mapping.
    """
    parsed: Any = yaml.load(CI_WORKFLOW.read_text(encoding="utf-8"), Loader=CSafeLoader)
    assert isinstance(parsed, dict), f"{CI_WORKFLOW.relative_to(ROOT)} did not parse to a mapping"
    return parsed


def _ci_sync_args() -> str:
    """Return the live ``SYNC_ARGS`` dependency set both syncing CI jobs install.

    Returns:
        The raw ``SYNC_ARGS`` value from ``ci.yml``'s ``env``.
    """
    env: Any = _ci_workflow().get("env")
    assert isinstance(env, dict), f"{CI_WORKFLOW.relative_to(ROOT)} no longer declares an env mapping; re-derive this guard"
    assert env.get("SYNC_ARGS"), f"{CI_WORKFLOW.relative_to(ROOT)} no longer declares env.SYNC_ARGS; re-derive this guard"
    return str(env["SYNC_ARGS"]).strip()


def _makefile_phony_targets() -> set[str]:
    """Return the live ``.PHONY`` target set declared by the ``Makefile``.

    Returns:
        Target names from the ``.PHONY:`` line.

    Raises:
        AssertionError: If the Makefile declares no ``.PHONY`` line -- the guard would go vacuous.
    """
    match: re.Match[str] | None = re.search(r"^\.PHONY:[^\S\n]*(?P<targets>.+)$", MAKEFILE.read_text(encoding="utf-8"), re.MULTILINE)
    assert match, f"{MAKEFILE.name} has no .PHONY line; this guard went vacuous"
    targets: set[str] = set(match.group("targets").split())
    assert targets, f"{MAKEFILE.name} .PHONY line is empty; this guard went vacuous"
    return targets


def _precommit_install_command(page: Path) -> str:
    """Return the unique ``uv run pre-commit install`` command shown on ``page``.

    Args:
        page: Markdown page to scan.

    Returns:
        The stripped command line.

    Raises:
        AssertionError: If the page shows zero or several such commands -- an ambiguous install
            instruction cannot be checked for cross-page parity.
    """
    text: str = page.read_text(encoding="utf-8")
    commands: list[str] = [
        line.strip() for block in FENCED_BASH.findall(text) for line in block.splitlines() if line.strip().startswith("uv run pre-commit install")
    ]
    assert len(commands) == 1, f"{page.relative_to(ROOT)} must show exactly one `uv run pre-commit install` command; found {commands or 'none'}"
    return commands[0]


def test_contributing_precommit_section_agrees_with_ci_on_no_sharding() -> None:
    """No CONTRIBUTING.md sentence may claim CI shards the test suite; the live workflow does not.

    The pre-commit section once justified keeping pytest out of the hooks with \"CI shards them
    across four runners\", while ``ci.yml`` carries a \"Deliberately NOT sharded across a matrix\"
    comment over a single ``python-test`` job. The live side is checked first -- no ``matrix``
    strategy on any job that runs pytest -- so a real move to sharding fails HERE with a
    re-derive instruction rather than letting the docs ban go stale. On the docs side, every
    occurrence of ``shard`` must sit in a negated clause (\"not sharded\", \"rather than
    sharding\"), so an affirmative sharding claim fails regardless of its exact wording.
    """
    jobs: Any = _ci_workflow().get("jobs")
    assert isinstance(jobs, dict), f"{CI_WORKFLOW.relative_to(ROOT)} has no jobs mapping; this guard went vacuous"
    assert jobs, f"{CI_WORKFLOW.relative_to(ROOT)} has no jobs; this guard went vacuous"
    pytest_jobs: list[str] = [name for name, job in jobs.items() if isinstance(job, dict) and "pytest" in str(job.get("steps"))]
    assert pytest_jobs, f"{CI_WORKFLOW.relative_to(ROOT)} has no job running pytest; this guard went vacuous"
    for name in pytest_jobs:
        strategy: Any = jobs[name].get("strategy") or {}
        assert "matrix" not in strategy, (
            f"{CI_WORKFLOW.relative_to(ROOT)} job {name!r} now shards across a matrix; CONTRIBUTING.md's no-sharding claim must be re-derived"
        )
    text: str = CONTRIBUTING.read_text(encoding="utf-8")
    sentences: list[str] = re.split(r"(?<=[.!?])\s+", text)
    for sentence in sentences:
        for match in re.finditer(r"\bshard\w*\b", sentence, re.IGNORECASE):
            negation: re.Match[str] | None = NEGATED_SHARD.search(sentence, 0, match.end())
            message: str = (
                f"{CONTRIBUTING.name} makes an affirmative CI-sharding claim near {match.group(0)!r}, contradicting "
                f"{CI_WORKFLOW.relative_to(ROOT)}'s deliberately unsharded single job"
            )
            assert negation is not None, message
            assert negation.end() == match.end(), message
    start, end = _section_range(text, "Pre-commit hooks")
    section: str = text[start:end]
    assert "rebuild the Rust extension" in section, (
        f"{CONTRIBUTING.name} 'Pre-commit hooks' must state the durable reason pytest/cargo-test sit in neither hook stage: "
        "they rebuild the Rust extension"
    )
    assert "single job" in section, f"{CONTRIBUTING.name} 'Pre-commit hooks' must state that CI runs the suite as a single job (not sharded)"


def test_contributing_quality_gates_carry_durable_suite_facts() -> None:
    """The Quality gates section carries durable suite facts and no rotting hardcoded numbers.

    \"over 600 tests\", \"46 Rust unit tests\", and \"around 90% coverage\" all rot on the next
    added test, so hardcoded counts/percentages are banned in the section BY RULE (a count of
    tests or a coverage percentage is never durable). The durable facts that replace them are
    pinned against live configuration: pytest-xdist parallelism (``-n auto``) and inline coverage
    (``--cov``) are read from ``pyproject.toml``'s addopts FIRST, so a config change fails here
    before the doc anchors are checked.
    """
    addopts: Any = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["tool"]["pytest"]["ini_options"]["addopts"]
    assert "-n auto" in addopts, f"{PYPROJECT.name} addopts dropped `-n auto`; re-derive this guard"
    assert "--cov" in addopts, f"{PYPROJECT.name} addopts dropped `--cov`; re-derive this guard"
    text: str = CONTRIBUTING.read_text(encoding="utf-8")
    start, end = _section_range(text, "Quality gates")
    section: str = text[start:end]
    for anchor in ("pytest-xdist", "-n auto", "--cov", "offline"):
        assert anchor in section, f"{CONTRIBUTING.name} 'Quality gates' must carry the durable fact {anchor!r} (live in {PYPROJECT.name} addopts)"
    banned: dict[str, str] = {
        r"\b\d[\d,]*\s+(?:\w+\s+){0,2}tests\b": "a hardcoded test count",
        r"\b\d+\s*%\s*coverage\b|\bcoverage\s+(?:of|around|about|~)?\s*~?\d+\s*%": "a hardcoded coverage percentage",
    }
    for pattern, what in banned.items():
        found: re.Match[str] | None = re.search(pattern, section, re.IGNORECASE)
        assert not found, (
            f"{CONTRIBUTING.name} 'Quality gates' carries {what} ({found.group(0)!r}); such numbers rot -- state the durable behavior instead"
        )


def test_contributing_layout_block_lists_only_existing_paths() -> None:
    """Validate the Project layout entries required by US-007.

    Every listed path must exist, and every line must keep the two-column shape (path, then a
    purpose separated by whitespace). The explicit required-path tuple captures the US-007 story
    additions (``examples/``, ``.github/``, and ``Makefile``); this guard intentionally does not
    require the block to enumerate every top-level repository path.
    """
    text: str = CONTRIBUTING.read_text(encoding="utf-8")
    start, end = _section_range(text, "Project layout")
    blocks: list[str] = FENCED_TEXT.findall(text[start:end])
    assert len(blocks) == 1, f"{CONTRIBUTING.name} 'Project layout' must hold exactly one ```text block; found {len(blocks)}"
    lines: list[str] = [line for line in blocks[0].splitlines() if line.strip()]
    assert lines, f"{CONTRIBUTING.name} 'Project layout' block is empty; this guard went vacuous"
    entries: list[str] = []
    for line in lines:
        assert re.match(r"^\S+\s{2,}\S", line), f"{CONTRIBUTING.name} layout line lacks the two-column 'path  purpose' shape: {line!r}"
        entries.append(line.split()[0])
    missing: list[str] = [entry for entry in entries if not (ROOT / entry).exists()]
    assert not missing, f"{CONTRIBUTING.name} 'Project layout' lists paths that do not exist: {missing}"
    required_paths: tuple[str, ...] = ("examples/", ".github/", "Makefile")
    for required in required_paths:
        assert required in entries, f"{CONTRIBUTING.name} 'Project layout' omits the required US-007 path {required!r}"


def test_contributing_and_installation_precommit_commands_match() -> None:
    """Both pages show the same hook-install command, and the rationale matches the live config.

    The guide once claimed ``--install-hooks`` was what wires up the pre-push stage; in fact the
    config's ``default_install_hook_types`` decides which stages a bare ``pre-commit install``
    wires (``--install-hooks`` only pre-builds hook environments). The stage set is read from
    ``.pre-commit-config.yaml`` at test time, and the command itself is lifted out of both pages'
    fenced blocks, so a drift on either page -- or a config change the rationale stops matching --
    fails here.
    """
    config: Any = yaml.load(PRE_COMMIT_CONFIG.read_text(encoding="utf-8"), Loader=CSafeLoader)
    hook_types: Any = config.get("default_install_hook_types") if isinstance(config, dict) else None
    assert hook_types, f"{PRE_COMMIT_CONFIG.name} no longer declares default_install_hook_types; re-derive this guard"
    assert set(hook_types) == {"pre-commit", "pre-push"}, (
        f"{PRE_COMMIT_CONFIG.name} default_install_hook_types is now {hook_types}; the docs' install rationale must be re-derived"
    )
    contributing_command: str = _precommit_install_command(CONTRIBUTING)
    installation_command: str = _precommit_install_command(INSTALLATION)
    assert contributing_command == installation_command, (
        f"hook-install command drifted: {CONTRIBUTING.name} shows {contributing_command!r}, "
        f"{INSTALLATION.relative_to(ROOT)} shows {installation_command!r}"
    )
    assert "--hook-type" not in contributing_command, (
        f"{CONTRIBUTING.name} passes explicit --hook-type flags, but a bare install already wires {sorted(hook_types)} via default_install_hook_types"
    )
    text: str = CONTRIBUTING.read_text(encoding="utf-8")
    start, end = _section_range(text, "Pre-commit hooks")
    assert "default_install_hook_types" in text[start:end], (
        f"{CONTRIBUTING.name} 'Pre-commit hooks' must explain the bare install via the config's default_install_hook_types"
    )


def test_contributing_ci_dependency_set_matches_workflow_sync_args() -> None:
    """Every CONTRIBUTING.md mention of the CI dependency set quotes the live ``SYNC_ARGS``.

    CI installs exactly ``--no-default-groups --group ci --extra qc --extra log`` (``ci.yml``
    ``env.SYNC_ARGS``, shared so both syncing jobs hit one warm uv cache); the guide once
    described the CI environment as just ``--extra qc``. Only lines that mention CI AND carry
    sync flags are checked, so the local ``uv sync --group dev ...`` setup command is out of
    scope; the expected string is lifted from the workflow on every run.
    """
    expected: set[str] = _sync_flags(_ci_sync_args())
    assert expected, f"{CI_WORKFLOW.relative_to(ROOT)} SYNC_ARGS contains no recognized dependency flags; this guard went vacuous"
    text: str = CONTRIBUTING.read_text(encoding="utf-8")
    mentions: list[str] = [line.strip() for line in text.splitlines() if re.search(r"\bCI\b", line) and SYNC_FLAG.search(line)]
    assert mentions, f"{CONTRIBUTING.name} never documents the CI dependency set; this guard went vacuous"
    for line in mentions:
        documented: set[str] = _sync_flags(line)
        assert documented == expected, (
            f"{CONTRIBUTING.name} documents CI dependency flags {sorted(documented)}, but "
            f"{CI_WORKFLOW.relative_to(ROOT)} SYNC_ARGS has {sorted(expected)}: {line!r}"
        )


def test_contributing_makefile_targets_are_real_phony_targets() -> None:
    """Every ``make <target>`` in the Task runner table is a live ``.PHONY`` target, and vice versa.

    The target set is parsed from the Makefile's ``.PHONY:`` line at test time -- never copied --
    so a renamed or added target fails here until the table is re-synced. Both directions are
    checked: a documented target that does not exist fails at a contributor's shell, and a live
    target the table omits is a workflow readers cannot discover.
    """
    phony: set[str] = _makefile_phony_targets()
    text: str = CONTRIBUTING.read_text(encoding="utf-8")
    start, end = _section_range(text, "Task runner")
    documented: set[str] = set(MAKE_TARGET.findall(text[start:end]))
    assert documented, f"{CONTRIBUTING.name} 'Task runner' documents no `make <target>` commands; this guard went vacuous"
    unknown: list[str] = sorted(documented - phony)
    assert not unknown, f"{CONTRIBUTING.name} 'Task runner' documents {unknown}, which are not .PHONY targets in {MAKEFILE.name}"
    undocumented: list[str] = sorted(phony - documented)
    assert not undocumented, f"{MAKEFILE.name} .PHONY targets {undocumented} are missing from {CONTRIBUTING.name} 'Task runner' table"
