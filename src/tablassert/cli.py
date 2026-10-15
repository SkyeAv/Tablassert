from __future__ import annotations

import hashlib
import re
import subprocess
import sys
import time
from collections.abc import Callable
from importlib import import_module
from importlib.metadata import version as get_version
from itertools import chain, pairwise
from multiprocessing import Pool
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, BinaryIO, Literal, NoReturn, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import cyclopts

from tablassert import extras, net
from tablassert._lazy import LazyModule
from tablassert.errors import BabelDownloadError, GraphValidationError, SectionValidationError, flatten_pydantic_error
from tablassert.log import cat

if TYPE_CHECKING:
    import polars as pl
    import pydantic

    from tablassert.biolink import Categories
    from tablassert.lib import Tcode
    from tablassert.models import Graph
    from tablassert.progress import PipelineProgress
    from tablassert.runcache import PlanEntry, RunCache, RunPlan
else:
    pl = LazyModule("polars")
    pydantic = LazyModule("pydantic")

# Pipeline completion events (BUILD, VALIDATE).
logger = cat("PIPELINE")
# BABEL downloader events (reuse, restart, done, retry).
download_logger = cat("DOWNLOAD")

APP: cyclopts.App = cyclopts.App(
    version=f"tablassert {get_version('tablassert')}", help="Extract Knowledge Assertions From Tabular Data Into KGX NDJSON"
)

BABEL_BASE: str = "https://stars.renci.org/var/babel_outputs"
BABEL_VERSION: str = "2026jul22"
BABEL_CLASS_ENDPOINTS: tuple[str, ...] = ("kgx/",)
BABEL_SYNONYM_ENDPOINTS: tuple[str, ...] = ("synonyms/", "synonyms-conflated/")
BABEL_EXCLUDE_PREFIXES: tuple[str, ...] = ("Publication", "GeneProteinConflated")
BABEL_CLASS_RE: re.Pattern[str] = re.compile(r'<a href="([^"]*_nodes[^"]*\.gz)"')
BABEL_SYNONYM_RE: re.Pattern[str] = re.compile(r'<a href="([^"]+\.gz)"')
TAXON_ALLOWLIST_PATH: Path = Path(__file__).parent / "data" / "experimental_taxa.yaml"


def _section_store_path(h: str, head: bool = False, release: bool = False, qc: bool = False, linkouts: bool = False) -> Path:
    """Name a section's cached subgraph parquet so build-mode flags never share a cache file.

    The cached parquet's content depends on the build mode: ``--head`` samples
    rows, ``--release`` applies the release-mode significance filters, ``--qc``
    drops fullmap-audit rejects, and ``--linkouts`` appends the EdgeLinkouts
    record URL to the primary ``sources`` entry. ``Tcode.collect`` quick-exits
    on any existing store file, skipping the whole op chain, so each mode
    combination must cache under its own suffix or one mode's build is silently
    reused as another's.

    Args:
        h: Section config hash (``mkhash`` of the section dict).
        head: ``--head`` preview build flag.
        release: ``--release`` build flag.
        qc: ``--qc`` build flag.
        linkouts: ``--linkouts`` build flag.

    Returns:
        Store path like ``<h>.parquet`` with ``.head`` / ``.release`` / ``.qc`` /
        ``.linkouts`` inserted before the extension for each set flag.
    """
    from tablassert.utils import STORE

    suffix: str = (".head" if head else "") + (".release" if release else "") + (".qc" if qc else "") + (".linkouts" if linkouts else "")
    return STORE / f"{h}{suffix}.parquet"


def _section_store_key_for_build(
    section: dict[str, Any], configuration_file: Path, content_hashes: dict[tuple[Path, int, int], str]
) -> tuple[str, str]:
    """Return a section's content-aware store key and config-only fallback label.

    The memo belongs to one ``build_graph_pipeline`` call and is invalidated by any
    ``(resolved path, mtime_ns, size)`` change. Progress and validation errors use the
    content-aware key; the returned config-only hash labels ``SourceFileError`` when
    hashing fails before a content-aware key can exist.
    """
    from tablassert.utils import file_content_hash, mkhash, section_store_key

    config_hash: str = mkhash(section)
    source: object = section.get("source")
    local: object = source.get("local") if isinstance(source, dict) else None
    if local is None:
        return section_store_key(section), config_hash

    resolved: Path = Path(str(local)).resolve()
    config_value: object = section.get("config", "section")
    section_label: str = f"{Path(str(config_value)).stem} · {config_hash[:8]}"
    digest: str | None = None
    try:
        stat = resolved.stat()
    except OSError:
        # Reuse the contextual helper so missing/unreadable paths never leak a raw OS error.
        digest = file_content_hash(resolved, config=configuration_file, section_label=section_label)
        return section_store_key(section, content_digest=digest), config_hash

    signature: tuple[Path, int, int] = (resolved, stat.st_mtime_ns, stat.st_size)
    if digest is None:
        digest = content_hashes.get(signature)
    if digest is None:
        digest = file_content_hash(resolved, config=configuration_file, section_label=section_label)
        content_hashes[signature] = digest
    return section_store_key(section, content_digest=digest), config_hash


def _load_table_indexed(args: tuple[int, Path]) -> tuple[int, object]:
    """Load one table, tagged with its input index (multiprocessing worker).

    Runs in a pool subprocess, so it re-imports ``from_yaml`` locally (the
    deferred import mirrors ``build_pipeline``). The carried index lets the
    caller reassemble results in input order even though ``imap_unordered``
    yields them in completion order.

    Args:
        args: ``(index, table_path)`` pair for one table.

    Returns:
        ``(index, parsed_yaml)`` so the caller can position the result by index.
    """
    from tablassert.ingests import from_yaml

    idx, table = args
    return idx, from_yaml(table)


def _extract_sections_indexed(args: tuple[int, object, Path]) -> tuple[int, list[dict[str, Any]]]:
    """Extract sections from one loaded table, tagged with its input index (multiprocessing worker).

    Runs in a pool subprocess, so it re-imports ``to_sections`` locally (the
    deferred import mirrors ``build_pipeline``). The carried index lets the
    caller reassemble per-table section lists in input order so the flattened
    ``sections`` order is byte-identical to the old ``starmap`` result.

    Args:
        args: ``(index, parsed_yaml, table_path)`` triple for one table.

    Returns:
        ``(index, section_list)`` so the caller can position the result by index.
    """
    from tablassert.ingests import to_sections

    idx, raw, table = args
    return idx, to_sections(raw, table)  # pyright: ignore


def _load_graph(configuration_file: Path) -> Graph:
    """Load and validate the Graph config that drives a build.

    Args:
        configuration_file: Graph YAML path.

    Returns:
        The validated Graph model.

    Raises:
        GraphValidationError: If the graph fails Pydantic validation.
    """
    from tablassert.ingests import from_yaml
    from tablassert.models import Graph

    raw: object = from_yaml(configuration_file)
    try:
        return Graph.model_validate(raw)
    except pydantic.ValidationError as e:
        raise GraphValidationError(configuration_file, flatten_pydantic_error(e)) from e


def _run_cache_resume(cache: RunCache, entry: PlanEntry) -> tuple[int, pl.LazyFrame] | None:
    """Build ``compile_subgraph``'s ``resume`` argument for one planned section.

    Args:
        cache: This build's open run cache.
        entry: The section's planned role (:class:`tablassert.runcache.PlanEntry`).

    Returns:
        ``(prefix_len, snapshot)`` for a consumer of a shared prefix, ``None`` for a producer and
        for an ungrouped section — both start from their own first op.

    Raises:
        RunCacheError: ``runcache-missing-snapshot`` when a planned consumer's snapshot is not in
            the cache at execution time. The plan runs every producer before its consumers, so a
            miss means the plan and the execution disagree; recomputing the prefix would hide that
            as a merely slower build, and the snapshot's absence would stay unexplained.
    """
    from tablassert.runcache import RunCacheError

    if entry.resume_digest is None or entry.prefix_len is None:
        return None
    snapshot: pl.LazyFrame | None = cache.load(entry.resume_digest)
    if snapshot is None:
        raise RunCacheError(
            f"run cache holds no snapshot for digest {entry.resume_digest}, which the build plan assigned to an "
            "earlier section as its producer; the plan and the execution order disagree, so this section cannot "
            "resume the shared prefix it was planned to read",
            code="runcache-missing-snapshot",
        )
    return (entry.prefix_len, snapshot)


def _run_cache_snapshot(cache: RunCache, entry: PlanEntry) -> Callable[[int, pl.LazyFrame], pl.LazyFrame | None] | None:
    """Build ``compile_subgraph``'s ``snapshot`` callback for one planned section.

    ``compile_subgraph`` offers EVERY checkpoint of the op list; only the one the plan assigned to
    this producer is stored. After storing, the producer switches its accumulator to the same
    materialized snapshot that consumers read, so its final write does not recompute the lazy
    prefix. The cache remains ephemeral for this build only.

    Args:
        cache: This build's open run cache.
        entry: The section's planned role (:class:`tablassert.runcache.PlanEntry`).

    Returns:
        The store callback for a producer, ``None`` for a consumer and for an ungrouped section:
        neither owns a digest, and a stray store would trip ``runcache-duplicate-store``.
    """
    if entry.produce_digest is None or entry.prefix_len is None:
        return None
    digest: str = entry.produce_digest
    prefix_len: int = entry.prefix_len

    def snapshot(position: int, lf: pl.LazyFrame) -> pl.LazyFrame | None:
        """Store this producer's assigned checkpoint and return its materialized lineage.

        Args:
            position: Prefix length ``compile_subgraph`` just finished (1-based op count).
            lf: Frame that ``ops[:position]`` produced.

        Returns:
            The stored snapshot scan at the selected checkpoint, making the producer's remaining
            tail consume the materialization; ``None`` for other offered checkpoints.

        Raises:
            RunCacheError: If the just-stored snapshot cannot be loaded, because continuing with
                the original lazy lineage would silently recompute the producer prefix.
        """
        if position != prefix_len:
            return None
        cache.store(digest, lf)
        materialized: pl.LazyFrame | None = cache.load(digest)
        if materialized is None:
            from tablassert.runcache import RunCacheError

            raise RunCacheError(
                f"run cache could not load the snapshot just stored for digest {digest}; refusing to continue "
                "with the original lazy producer lineage",
                code="runcache-missing-snapshot",
            )
        return materialized

    return snapshot


def build_pipeline(
    configuration_file: Path,
    progress: PipelineProgress,
    release: bool = False,
    qc: bool = False,
    log: bool = False,
    head: bool = False,
    linkouts: bool = False,
) -> None:
    """Load a graph YAML and build it through the shared in-process core."""
    graph: Graph = _load_graph(configuration_file)
    build_graph_pipeline(graph, configuration_file, progress, release=release, qc=qc, log=log, head=head, linkouts=linkouts)


def build_graph_pipeline(
    graph: Graph,
    configuration_file: Path,
    progress: PipelineProgress,
    release: bool = False,
    qc: bool = False,
    log: bool = False,
    head: bool = False,
    linkouts: bool = False,
    audit_sources: bool = True,
) -> None:
    """Build a validated :class:`Graph` without loading another graph YAML.

    The public ``build-kg`` command uses :func:`build_pipeline`. Keeping the core in-process
    lets library callers reuse the production stages and metadata without building every
    table in the caller's target graph.

    Args:
        graph: Validated graph model controlling tables, fullmap, identity, and RIG.
        configuration_file: Logical graph path used in validation error messages.
        progress: Pipeline progress reporter.
        release: When ``True``, emit release-mode artifacts.
        qc: When ``True``, run quality-control audits and final study assertions.
        log: When ``True``, enable per-section verbose logging.
        head: When ``True``, build a random sample of up to five rows per section.
        linkouts: When ``True``, append the EdgeLinkouts record URL to the graph infores entry in every edge's ``sources`` list.
    """
    from tablassert.fullmap import fullmap_db_path
    from tablassert.lib import Tcode, compile_graph, compile_subgraph
    from tablassert.progress import format_section_compact
    from tablassert.runcache import RunCache, plan_run

    # Stage 1/6: load tables.
    progress.stage("Loading Tables")
    g: Graph = graph
    # imap_unordered yields in completion order, so each worker carries its input
    # index and we reassemble by index to keep raw[i] aligned with g.tables[i].
    start, advance, _ = progress.section_loop(len(g.tables), "Load")
    raw: list[object] = [None for _ in g.tables]
    with Pool() as pool:
        for idx, parsed in pool.imap_unordered(_load_table_indexed, enumerate(g.tables)):
            raw[idx] = parsed
            start(str(g.tables[idx]))
            advance()

    # Stage 2/6: extract sections.
    progress.stage("Extracting Sections")
    # Same index-carrying reassembly keeps temp[i] aligned with g.tables[i], so the
    # flattened sections order is byte-identical to the old starmap result.
    start, advance, _ = progress.section_loop(len(g.tables), "Extract")
    temp: list[list[dict[str, Any]]] = [[] for _ in g.tables]
    with Pool() as pool:
        for idx, section_list in pool.imap_unordered(_extract_sections_indexed, zip(range(len(g.tables)), raw, g.tables, strict=True)):
            temp[idx] = section_list
            start(str(g.tables[idx]))
            advance()
    sections: list[dict[str, Any]] = list(chain.from_iterable(temp))
    n: int = len(sections)
    # Per-section source descriptors for the generated RIG's relevant-file cross-check:
    # each entry records the section's local file name and its validated source URLs.
    section_sources: list[dict[str, Any]] = []
    for s in sections:
        src: dict[str, Any] = s.get("source") or {}
        section_sources.append({"local": str(src.get("local") or ""), "urls": [str(u) for u in src.get("url") or []]})

    # Stage 3/6: build Tcode.
    progress.stage("Building TCode")
    start, advance, _ = progress.section_loop(n, "TCode")
    tcode: list[Tcode] = []
    # This memo is intentionally scoped to one build: a changed stat signature re-hashes,
    # while repeated sections pointing to the same unchanged file hash only once.
    content_hashes: dict[tuple[Path, int, int], str] = {}
    for s in sections:
        h, _ = _section_store_key_for_build(s, configuration_file, content_hashes)
        start(f"{Path(str(s['config'])).stem} · {h[:8]}")
        # Mode flags change the cached parquet's content, so each combination caches
        # to a distinct file and can never quick-exit another mode's build.
        store: Path = _section_store_path(h, head=head, release=release, qc=qc, linkouts=linkouts)
        try:
            tcode.append(
                Tcode.model_validate(
                    {
                        **s,
                        "store": store,
                        "log": log,
                        "qc": qc,
                        "release": release,
                        "head": head,
                        "linkouts": linkouts,
                        "name": g.name,
                        "infores": g.rig.source_info.infores_id,
                        "qc_similarity_threshold": g.qc_similarity_threshold,
                        "qc_fuzzy_ratio_threshold": g.qc_fuzzy_ratio_threshold,
                        "qc_fuzzy_partial_threshold": g.qc_fuzzy_partial_threshold,
                        "qc_aliases": g.qc_aliases,
                    }
                )
            )
        except pydantic.ValidationError as e:
            raise SectionValidationError(configuration_file, h, flatten_pydantic_error(e)) from e
        advance()

    db: Path = fullmap_db_path(g.fullmap)

    # Stage 4/6: collect instructions.
    progress.stage("Collecting Instructions")
    start, advance, sub_step = progress.section_loop(n, "Collect")
    instructions: list[Any] = []
    for x in tcode:
        start(format_section_compact(x))
        sub_step("planning")
        instructions.append(x.collect(db))
        advance()

    # Stage 5/6: build subgraphs.
    progress.stage("Building Subgraphs")
    start, advance, sub_step = progress.section_loop(n, "Subgraph")
    # `collect` quick-exits a section whose store parquet already exists to a Path: it has no op
    # list, so those sections are separated out BEFORE planning. `plan_run` indexes its entries by
    # position in the list it is handed (see its Args), so planning the unfiltered list would apply
    # every role to the wrong section.
    pending: list[tuple[int, Tcode, list[tuple[Callable, tuple[Any, ...]]]]] = [
        (index, x, op) for index, (x, op) in enumerate(zip(tcode, instructions, strict=True)) if not isinstance(op, Path)
    ]
    plan: RunPlan = plan_run([op for _, _, op in pending])
    subgraphs: list[Path]
    if plan.shares:
        # Sections sharing a checkpointed prefix run in the planned order — every producer before
        # its consumers — inside ONE ephemeral cache whose directory `__exit__` deletes when this
        # block ends, including when a section raises inside it.
        results: dict[int, Path] = {}
        for index, (x, op) in enumerate(zip(tcode, instructions, strict=True)):
            if isinstance(op, Path):
                # Already built: keep the quick-exit path and tick it in place, as before.
                results[index] = op
                start(format_section_compact(x))
                advance()
        with RunCache() as cache:
            for planned in plan.execution_order:
                section_index, x, op = pending[planned]
                entry: PlanEntry = plan.entries[planned]
                start(format_section_compact(x))
                # on_phase drives the per-op sub-step indicator (cache → resolve → write ...);
                # resume/snapshot are the producer/consumer seams the plan assigned this section.
                results[section_index] = compile_subgraph(
                    op, on_phase=sub_step, resume=_run_cache_resume(cache, entry), snapshot=_run_cache_snapshot(cache, entry)
                )
                advance()
        # Execution order changed only WHEN sections ran: reassemble by ORIGINAL section index so
        # `compile_graph` sees the same subgraph sequence as an unplanned build.
        subgraphs = [results[index] for index in range(n)]
    else:
        # Zero sharing: no snapshot could pay for itself, so no cache (and no temp directory) is
        # created and every section runs its full op list in the original order.
        subgraphs = []
        for x, op in zip(tcode, instructions, strict=True):
            start(format_section_compact(x))
            # on_phase drives the per-op sub-step indicator (load → filter → resolve → write ...).
            subgraphs.append(op if isinstance(op, Path) else compile_subgraph(op, on_phase=sub_step))
            advance()

    # Stage 6/6: compile graph.
    progress.stage("Compiling Graph")
    start, advance, sub_step = progress.section_loop(len(subgraphs), "Graph")
    start(f"{g.name} · v{g.version}")
    # on_phase drives the phase tag (scan → normalize → write-nodes → write-edges → dedup → rig);
    # on_subgraph ticks the bar once per subgraph, so the total is len(subgraphs).
    compile_graph(
        subgraphs,
        g.name,
        g.version,
        g.rig,
        section_sources if audit_sources else None,
        on_phase=sub_step,
        on_subgraph=advance,
        uuid_fields=g.uuid_fields,
        uuid_domain=g.uuid_namespace,
        uuid_on_collision=g.uuid_on_collision,
    )

    # Stage 7/7 (only with --qc): assert over the final NDJSON files.
    if qc:
        progress.stage("Studying Graph")
        # The demotion assertion is graph-wide: the study runs on the merged NDJSON with no
        # section attribution, so one pinned section makes every bare biolink:Association edge
        # in the graph a failure -- including edges from unpinned sections, for which bare
        # Association is the author's accepted default. Graphs where every section is pinned
        # are unaffected; a mixed graph should split or accept the stricter gate. An empty
        # ``category_override: {}`` pins nothing and counts as undeclared.
        study_final_ndjson(
            g.name, g.version, Path(g.rig.artifact_base_path), category_override_declared=any(x.statement.category_override for x in tcode)
        )

    logger.info("Built graph {name} v{version}: {n} sections", name=g.name, version=g.version, n=n)


def study_final_ndjson(name: str, version: str, out_dir: Path, *, category_override_declared: bool = False) -> None:
    """Run study assertions over a build's final NDJSON files (the ``--qc`` stage 7).

    Args:
        name: Graph name, used to locate ``<name>_<version>.nodes.ndjson``.
        version: Graph version, used to locate ``<name>_<version>.edges.ndjson``.
        out_dir: Artifact directory the build wrote into
            (``rig.artifact_base_path``).
        category_override_declared: Whether any built section declared a
            ``statement.category_override``; when set, edges demoted to bare
            ``biolink:Association`` violate the study (a row escaped its pin).

    Raises:
        SystemExit: With status 1 when any study assertion is violated.
    """
    from tablassert.study import format_violations, study_kgx

    violations = study_kgx(
        out_dir / f"{name}_{version}.nodes.ndjson", out_dir / f"{name}_{version}.edges.ndjson", category_override_declared=category_override_declared
    )
    if violations:
        summary: str = format_violations(violations)
        print(summary, file=sys.stderr)
        logger.warning("study assertions failed on final NDJSON:\n{summary}", summary=summary)
        raise SystemExit(1)
    logger.info("study assertions passed on final NDJSON")


def validate_pipeline(table_configuration_file: Path, progress: PipelineProgress) -> None:
    """Validate section syntax from a YAML configuration file.

    Runs the three-stage validate pipeline: load tables → extract sections →
    validate section syntax (no execution).

    Args:
        table_configuration_file: Path to the table YAML file.
        progress: Pipeline progress reporter.

    Raises:
        SectionValidationError: If any section fails Pydantic validation.
    """
    from tablassert.ingests import from_yaml, to_sections
    from tablassert.lib import Tcode
    from tablassert.utils import STORE, mkhash

    # Stage 1/3: load tables.
    progress.stage("Loading Tables")
    r: object = from_yaml(table_configuration_file)

    # Stage 2/3: extract sections.
    progress.stage("Extracting Sections")
    sections: list[dict[str, Any]] = to_sections(r, table_configuration_file)  # pyright: ignore
    n: int = len(sections)

    # Stage 3/3: validate section syntax.
    progress.stage("Validating Section Syntax")
    start, advance, _ = progress.section_loop(n, "Validate")
    for s in sections:
        h: str = mkhash(s)
        start(f"{Path(str(s['config'])).stem} · {h[:8]}")
        try:
            Tcode.model_validate({**s, "store": (STORE / f"{h}.parquet")})
        except pydantic.ValidationError as e:
            raise SectionValidationError(table_configuration_file, h, flatten_pydantic_error(e)) from e
        advance()

    logger.info("Validated {n} sections from {config}", n=n, config=table_configuration_file.name)


def validate_graph_pipeline(configuration_file: Path, progress: PipelineProgress) -> None:
    """Validate a graph config and every table it references (no execution).

    Runs a two-stage validate pipeline: validate the Graph model, then validate
    each referenced table's sections through the same Tcode path
    ``validate_pipeline`` uses.

    Args:
        configuration_file: Path to the graph YAML file.
        progress: Pipeline progress reporter.

    Raises:
        GraphValidationError: If the graph YAML fails Pydantic validation.
        SectionValidationError: If any referenced table section fails validation.
    """
    from tablassert.ingests import from_yaml, to_sections
    from tablassert.lib import Tcode
    from tablassert.models import Graph
    from tablassert.utils import STORE, mkhash

    # Stage 1/2: validate the graph config.
    progress.stage("Validating Graph")
    r: object = from_yaml(configuration_file)
    try:
        g: Graph = Graph.model_validate(r)
    except pydantic.ValidationError as e:
        raise GraphValidationError(configuration_file, flatten_pydantic_error(e)) from e

    # Stage 2/2: validate every referenced table's sections.
    progress.stage("Validating Tables")
    # Expand each referenced table into sections up front so the bar total is known.
    table_sections: list[tuple[Path, dict[str, Any]]] = []
    for table in g.tables:
        raw_table: object = from_yaml(table)
        sections: list[dict[str, Any]] = to_sections(raw_table, table)  # pyright: ignore
        for section in sections:
            table_sections.append((table, section))
    n: int = len(table_sections)
    start, advance, _ = progress.section_loop(n, "Validate")
    for table, s in table_sections:
        h: str = mkhash(s)
        start(f"{Path(str(s['config'])).stem} · {h[:8]}")
        try:
            Tcode.model_validate({**s, "store": (STORE / f"{h}.parquet")})
        except pydantic.ValidationError as e:
            raise SectionValidationError(table, h, flatten_pydantic_error(e)) from e
        advance()

    logger.info("Validated graph {name}: {n} sections across {tables} tables", name=g.name, n=n, tables=len(g.tables))


def run(stages: int, fn: Any, arg: Path, **kwargs: Any) -> None:
    from tablassert.log import LOG_FORMAT, logger
    from tablassert.progress import PipelineProgress

    with PipelineProgress(total_stages=stages) as progress:
        sink_id: int = logger.add(progress.log_sink, level="INFO", format=LOG_FORMAT)
        try:
            fn(arg, progress, **kwargs)
        finally:
            logger.remove(sink_id)


def babel_urls(version: str, endpoints: tuple[str, ...], pattern: re.Pattern[str]) -> list[tuple[str, str]]:
    """Discover BABEL files using the RENCI directory-listing convention.

    Mirrors the legacy Datassert tool this replaces: fetch each endpoint's
    HTML listing, apply ``pattern`` to extract compressed file URLs, and drop
    any file whose name starts with a banned prefix.

    Args:
        version: BABEL version label inserted into the URL template.
        endpoints: Subdirectory endpoints under ``{BABEL_BASE}/{version}/``.
        pattern: Regex with one capture group selecting ``*.gz`` file paths.

    Returns:
        List of ``(lowercased_filename, absolute_url)`` tuples.
    """
    out: list[tuple[str, str]] = []
    for endpoint in endpoints:
        listing_url: str = f"{BABEL_BASE}/{version}/{endpoint}"
        request: Request = Request(listing_url, headers={"User-Agent": "tablassert"})
        with urlopen(request, timeout=60) as response:
            body: str = response.read().decode("utf-8")
        matches: list[str] = pattern.findall(body)
        for match in matches:
            filename: str = Path(match).name
            if any(filename.startswith(prefix) for prefix in BABEL_EXCLUDE_PREFIXES):
                continue
            out.append((filename.lower(), f"{listing_url}{match}"))
    return out


def download_babel_file(filename: str, url: str, destination: Path, retries: int = 5, on_progress: Callable[[int, int], None] | None = None) -> Path:
    """Spool a BABEL download to disk so large responses are resumable and never held in memory.

    Downloads to ``{filename}.part`` with HTTP Range resume support, then
    atomically renames to ``{filename}`` on success. Cached final files are
    reused without re-fetching.

    Args:
        filename: Output basename under ``destination``.
        url: Source URL.
        destination: Directory to download into (created if missing).
        retries: Maximum number of attempts before giving up.
        on_progress: Optional ``(downloaded_bytes, total_bytes)`` callback fired
            after each chunk; ``total_bytes`` is 0 when the size is unknown.
            ``None`` (default) keeps the original download behavior exactly.

    Returns:
        Path to the downloaded file.

    Raises:
        BabelDownloadError: If every retry attempt fails.
    """
    destination.mkdir(parents=True, exist_ok=True)
    final_path: Path = destination / filename
    part_path: Path = destination / f"{filename}.part"
    if final_path.is_file():
        download_logger.info("Reusing cached BABEL file: {path}", path=final_path)
        return final_path

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        offset: int = part_path.stat().st_size if part_path.exists() else 0
        headers: dict[str, str] = {"User-Agent": "tablassert"}
        if offset > 0:
            headers["Range"] = f"bytes={offset}-"
        request: Request = Request(url, headers=headers)
        try:
            with urlopen(request, timeout=300) as response:
                status: int = response.getcode()
                mode: str = "ab" if offset > 0 and status == 206 else "wb"
                if offset > 0 and status != 206:
                    download_logger.warning("Server ignored Range header (HTTP {status}); restarting download: {url}", status=status, url=url)
                # Resume base: bytes already on disk count only when appending (HTTP 206).
                base: int = offset if mode == "ab" else 0
                content_length: str | None = response.headers.get("Content-Length")
                total: int = base + int(content_length) if content_length is not None else 0
                reporter: Callable[[int], None] | None = None if on_progress is None else _byte_reporter(on_progress, base, total)
                with part_path.open(mode) as handle:
                    stream_copy(response, handle, reporter)
            part_path.replace(final_path)
            download_logger.info("Downloaded {url} -> {path}", url=url, path=final_path)
            return final_path
        except (HTTPError, OSError, URLError) as e:
            # The transient/permanent table is single-sourced from tablassert.net: a permanent
            # failure (e.g. a 404 from a mistyped --version) fails fast instead of burning every
            # attempt, while 408/425/429, every 5xx, and all socket/DNS/TLS errors fall through to
            # backoff. HTTPError subclasses URLError, so this one clause matches all three.
            if not net.is_transient(e):
                raise BabelDownloadError(url, attempt, e) from e
            last_error = e
            # str(e), never the exception OBJECT: the loguru sink (log.py) runs with enqueue=True, which
            # pickles every bound kwarg to the writer process; HTTPError cannot unpickle there, so the
            # OBJECT form silently drops every retry warning line (observed as repeated
            # "TypeError: HTTPError.__init__() missing 5 required positional arguments" logging errors).
            download_logger.warning(
                "Download attempt {attempt}/{retries} failed for {url}: {error}", attempt=attempt, retries=retries, url=url, error=str(e)
            )
            # Exponential backoff (5, 10, 20, ... capped at 60s) only when another attempt
            # remains — no dead sleep after the final failed attempt before raising.
            if attempt < retries:
                time.sleep(min(60, 5 * 2 ** (attempt - 1)))
    raise BabelDownloadError(url, retries, last_error or RuntimeError("no attempts made")) from last_error


def _resolve_aria2_binary() -> str:
    """Return the bundled aria2c binary path from the optional ``[aria2]`` extra.

    Raises:
        ImportError: If the optional ``[aria2]`` extra is not installed.
    """
    aria2c = import_module("aria2c")
    try:
        binary = aria2c.ARIA2C
    except AttributeError as e:
        raise ImportError("aria2c.ARIA2C was not found; install the [aria2] extra") from e
    return str(binary)


def aria2_unavailable_detail() -> str:
    """Explain how to get a working ``aria2c``, accounting for the platform and install state.

    macOS gets DIFFERENT advice on purpose: the ``aria2`` distribution publishes no macOS
    wheels, so pointing a mac user at the extra sends them to a dead end. This helper is
    reached only from ``download_babel_file_aria2c``'s ImportError path, which
    :func:`_resolve_aria2_binary` raises for BOTH an absent module and a module that imports
    but exposes no ``ARIA2C`` (a shadowed ``aria2c``) — hence "cannot be resolved", never
    "unimportable". That path raises ``BabelDownloadError`` loudly and NOTHING rescues to the
    Python downloader: the prebuilt fallback re-enters this same helper with ``aria2c=True``
    and fails the same way whenever it gets that far, while a warm download cache returns
    before the resolver ever runs and ``--force`` skips the prebuilt attempt entirely — so
    those two never reach it at all. The honest fix is therefore to repair or remove the
    broken install, which is what actually restores the Python downloader.
    """
    if sys.platform == "darwin":
        return (
            "the [aria2] extra ships no macOS wheels and this install's aria2c cannot be resolved "
            "(missing, or lacking ARIA2C) — uninstall it to fall back to the Python downloader"
        )
    if extras.is_installed("aria2"):
        # The CLI reaches this helper only after its own extras.is_installed("aria2") probe said
        # yes, so "install the [aria2] extra" is a dead end there (pip reports
        # already-satisfied): name the broken install instead. Library callers that reach it
        # with a genuinely absent extra still get the install hint below.
        return (
            "the [aria2] extra is installed but its aria2c cannot be resolved (missing, or lacking ARIA2C) "
            f"— reinstall or uninstall it: {extras.install_command('aria2')}"
        )
    return f"install the [aria2] extra: {extras.install_command('aria2')}"


def download_babel_file_aria2c(filename: str, url: str, destination: Path, retries: int = 5) -> Path:
    """Download one BABEL file with the bundled aria2c binary from ``[aria2]``.

    The helper mirrors ``download_babel_file``'s final-file cache contract but
    delegates resume/retry behavior to aria2. The ``aria2`` PyPI package is an
    optional extra that bundles the aria2c binary and exposes it as
    ``aria2c.ARIA2C``, so no system ``aria2c`` executable is required. Incomplete
    aria2 downloads leave a ``<filename>.aria2`` control file next to the target;
    when that control file exists we do NOT treat the target as a cache hit, and
    failures never remove either file so a later run can continue.

    Args:
        filename: Output basename under ``destination``.
        url: Source URL.
        destination: Directory to download into (created if missing).
        retries: Maximum aria2 tries (forwarded to ``--max-tries``).

    Returns:
        Path to the downloaded file.

    Raises:
        BabelDownloadError: If the ``[aria2]`` extra is missing, aria2c fails,
            or aria2c does not leave a complete final file.
    """
    destination.mkdir(parents=True, exist_ok=True)
    final_path: Path = destination / filename
    control_path: Path = destination / f"{filename}.aria2"
    if final_path.is_file() and not control_path.exists():
        download_logger.info("Reusing cached BABEL file: {path}", path=final_path)
        return final_path
    if retries < 1:
        error = ValueError("aria2c retries must be a positive integer")
        raise BabelDownloadError(url, retries, error) from error

    try:
        binary: str = _resolve_aria2_binary()
    except ImportError as e:
        error = FileNotFoundError(aria2_unavailable_detail())
        raise BabelDownloadError(url, 0, error) from e

    command: list[str] = [
        binary,
        "--continue=true",
        "--max-tries",
        str(retries),
        "--retry-wait",
        "5",
        "--allow-overwrite=true",
        "--auto-file-renaming=false",
        "--max-connection-per-server=8",
        "--split=8",
        "--min-split-size=1M",
        "--summary-interval=0",
        "--console-log-level=warn",
        "--show-console-readout=false",
        "--dir",
        str(destination),
        "--out",
        filename,
        url,
    ]
    try:
        completed: subprocess.CompletedProcess[str] = subprocess.run(
            command, shell=False, check=False, capture_output=True, text=True, errors="replace"
        )
    except OSError as e:
        raise BabelDownloadError(url, retries, e) from e

    if completed.returncode != 0:
        output: str = (completed.stderr or completed.stdout or "").strip()
        detail: str = f"aria2c exited with status {completed.returncode}"
        if output:
            detail = f"{detail}: {output[-2000:]}"
        error = RuntimeError(detail)
        raise BabelDownloadError(url, retries, error) from error

    if not final_path.is_file() or control_path.exists():
        suffix: str = ""
        if control_path.exists():
            suffix = f"; resume control file still present: {control_path}"
        output = (completed.stderr or completed.stdout or "").strip()
        if output:
            suffix = f"{suffix}; aria2c output: {output[-2000:]}"
        error = FileNotFoundError(f"aria2c completed but did not create a complete file at {final_path}{suffix}")
        raise BabelDownloadError(url, retries, error) from error

    download_logger.info("Downloaded {url} -> {path} with aria2c", url=url, path=final_path)
    return final_path


def stream_copy(source: BinaryIO, destination: BinaryIO, on_bytes: Callable[[int], None] | None = None) -> None:
    """Copy ``source`` to ``destination`` in 1 MiB chunks.

    When ``on_bytes`` is given it is called after each write with the running
    total of bytes written by THIS call; ``None`` (default) keeps the original
    copy-only behavior exactly.
    """
    written: int = 0
    while True:
        chunk: bytes = source.read(1024 * 1024)
        if not chunk:
            return
        destination.write(chunk)
        written += len(chunk)
        if on_bytes is not None:
            on_bytes(written)


def _byte_reporter(on_progress: Callable[[int, int], None], base: int, total: int) -> Callable[[int], None]:
    """Adapt ``stream_copy``'s cumulative-bytes callback to ``on_progress(downloaded, total)``.

    ``base`` is the byte count already on disk (resume offset) so the reported
    ``downloaded`` value reflects the whole file, not just this call's chunks.
    """

    def report(bytes_this_call: int) -> None:
        on_progress(base + bytes_this_call, total)

    return report


def _download_detail(downloaded: int, total: int) -> str:
    """Render the live download detail line in megabytes (1 MB = 1_000_000 bytes).

    When ``total`` is unknown (``<= 0``) only the transferred amount is shown.
    """
    if total <= 0:
        return f"{downloaded / 1_000_000:.1f} MB"
    return f"{downloaded / 1_000_000:.1f}/{total / 1_000_000:.1f} MB"


@APP.command(name="build-kg")
def build_kg(
    graph_configuration_file: Annotated[Path, cyclopts.Parameter(name=["--configuration-file", "-f"])],
    release: Annotated[bool, cyclopts.Parameter(name=["--release", "-r"], negative="")] = False,
    qc: Annotated[bool, cyclopts.Parameter(name=["--qc", "-q"], negative="")] = False,
    log: Annotated[bool, cyclopts.Parameter(name=["--log", "-l"], negative="")] = False,
    head: Annotated[bool, cyclopts.Parameter(name=["--head", "-hd"], negative="")] = False,
    linkouts: Annotated[bool, cyclopts.Parameter(name=["--linkouts", "-lo"], negative="")] = False,
) -> None:
    """Build a knowledge graph from a YAML configuration file.

    The positional config is a Graph YAML that orchestrates one or more table
    configs into a single knowledge-graph build.

    ``--qc`` requires the ``[qc]`` extra (``pip install
    "tablassert[qc]"``); it is checked before the build starts, because the audit stage
    runs LAST and a missing extra would otherwise surface only after entity resolution
    has finished. It also runs a final study stage that asserts over the emitted NDJSON
    -- no duplicate node ids, every node has a non-empty id and name, every edge has a
    non-empty subject, predicate, and object, no undeclared or isolated nodes, no
    malformed lines, no null or empty values in any field, no stray whitespace, and (when
    any section declares a ``category_override``) no edge demoted to bare
    ``biolink:Association``. The demotion assertion is GRAPH-WIDE: the study reads the
    merged NDJSON with no section attribution, so a single pinned section also fails
    demoted edges from unpinned sections (a row escaped its pin and shipped without the
    class-specific slots ``prune_to_class`` nulled into ``has_supporting_studies``) --
    and fails the build (non-zero exit) when any assertion is violated.

    Args:
        graph_configuration_file: Path to the Graph YAML that orchestrates one or more table
            configs into a single knowledge-graph build (accepted positionally or via
            ``--configuration-file``/``-f``).
        release: Emit release-mode artifacts: a slim, significant-only graph that drops
            ``not_significant`` edges before resolution (cached under its own ``.release``
            store, so it never reuses a default build).
        qc: Run quality-control audits and the final study stage over the emitted NDJSON;
            requires the ``[qc]`` extra and exits non-zero when any assertion is violated.
        log: Enable verbose per-section logging (requires the ``[log]`` extra; without it
            Tablassert produces no logs).
        head: Fast output-shape preview: a random sample of up to five rows per section,
            cached under its own ``.head`` store, so it never reuses or replaces a full build.
        linkouts: Append the EdgeLinkouts per-edge record URL
            (``https://linkouts.skyelanegoetz.com/edges/{edge_id}``, resolved to the final
            edge id after dedup) to the graph infores entry (``rig.source_info.infores_id``)
            in every emitted edge's ``sources`` list, so linkouts.skyelanegoetz.com can
            deep-link each edge. Sections declaring ``provenance.override`` are untouched
            (their ``sources`` emission is author-controlled). Cached under its own
            ``.linkouts`` store, so it never reuses or replaces a full build.
    """
    if qc:
        extras.require("qc", required_by="--qc")
    run(7 if qc else 6, build_pipeline, graph_configuration_file, release=release, qc=qc, log=log, head=head, linkouts=linkouts)


@APP.command(name="validate")
def validate(
    configuration_file: Annotated[Path, cyclopts.Parameter(name=["--configuration-file", "-f"])],
    schema: Annotated[Literal["graph", "table"], cyclopts.Parameter(name=["--schema", "-s"])],
) -> None:
    """Validate a YAML configuration file against the graph or table config schema.

    ``--schema graph`` validates the Graph model AND every referenced table; ``--schema
    table`` validates section syntax only. The schema is selected explicitly rather than
    sniffed from the YAML, so a config is always checked against the schema you expected.

    Args:
        configuration_file: Path to the YAML configuration file to validate (accepted
            positionally or via ``--configuration-file``/``-f``).
        schema: Which schema to validate against: ``graph`` checks the Graph model and every
            referenced table; ``table`` checks section syntax of a single table YAML only.
    """
    if schema == "graph":
        run(2, validate_graph_pipeline, configuration_file)
    else:
        run(3, validate_pipeline, configuration_file)


def _print_kgx_section(label: str, section: dict[str, Any], path: Path) -> None:
    """Print one ``validate-kgx`` per-file section (shared by the plain and ``--prune`` paths)."""
    if section["missing"]:
        # Never let a typo'd path read as a pass: 0/0 valid would otherwise exit 0.
        print(f"{label}: file not found ({path})", file=sys.stderr)
        logger.info(f"validate-kgx {label}: file not found")
        return
    pending: int = section["valid_excluding_pending"] - section["valid"]
    suffix: str = f"; {pending} pending biolink-model support" if pending else ""
    print(f"{label}: {section['valid']}/{section['total']} valid ({section['failures']} failures{suffix})", file=sys.stderr)
    for problem, count in section["problems"].items():
        print(f"  {count:>9}  {problem}", file=sys.stderr)
    for example in section["examples"][:3]:
        print(f"  e.g. {example['id']}: {', '.join(example['errors'])}", file=sys.stderr)
    logger.info(f"validate-kgx {label}: {section['valid']}/{section['total']} valid")


def _print_infores_report(report: dict[str, Any], paths: dict[str, Path | None]) -> None:
    """Print one identity-half report: absent inputs, registry provenance, counts, examples."""
    for label, absent in report["missing"].items():
        if absent:
            print(f"{label}: file not found ({paths.get(label)})", file=sys.stderr)
    print(f"registry: {report['registry']['source']} ({report['registry']['entries']} entries)", file=sys.stderr)
    counts: str = (
        f"curies: {report['registered']} registered, {report['allowed']} allowed, "
        f"{report['unregistered']} unregistered, {report['malformed']} malformed"
    )
    print(counts, file=sys.stderr)
    logger.info(f"validate-kgx infores: {counts}")
    for example in report["examples"][:3]:
        print(f"  e.g. {example['curie']} ({example['where']}.{example['field']}): {example['problem']}", file=sys.stderr)


@APP.command(name="validate-kgx")
def validate_kgx_command(
    nodes: Annotated[Path, cyclopts.Parameter(name=["--nodes", "-n"])],
    edges: Annotated[Path, cyclopts.Parameter(name=["--edges", "-e"])],
    limit: Annotated[int, cyclopts.Parameter(name=["--limit"])] = 20,
    prune: Annotated[bool, cyclopts.Parameter(name=["--prune", "-p"])] = False,
    rig: Annotated[Path | None, cyclopts.Parameter(name=["--rig"])] = None,
    allow_infores: Annotated[list[str] | None, cyclopts.Parameter(name=["--allow-infores"], negative="", allow_leading_hyphen=False)] = None,
    registry: Annotated[Literal["snapshot", "refresh", "off"], cyclopts.Parameter(name=["--registry"])] = "snapshot",
    strict: Annotated[bool, cyclopts.Parameter(name=["--strict"])] = False,
) -> None:
    """Validate built KGX NDJSON against the Biolink Model and the Translator registry.

    The structural half constructs every node and edge as the Biolink Pydantic class
    named by its own ``category`` -- the same classes ``NCATSTranslator/translator-
    ingests`` builds -- and reports failures grouped by field and error type. The
    identity half classifies every emitted knowledge-source identifier (edge
    ``primary_knowledge_source``, ``sources[].resource_id`` and
    ``sources[].upstream_resource_ids``, node ``provided_by``, and, with ``--rig``, the
    RIG document's infores values) against ``biolink/information-resource-registry``'s
    ``infores_catalog.yaml`` (vendored as the bundled snapshot, so the default path is
    offline). Values without the ``infores:`` prefix are malformed, not unregistered.

    The structural half exits non-zero when any record fails, so a build can be gated
    in CI. The identity half is advisory by default -- unregistered CURIEs are reported
    and the command exits 0, because Tablassert legitimately mints graph-local CURIEs
    that will never sit in the registry -- and ``--strict`` (CI gates) exits 1 while any
    unregistered CURIE remains; allowlist legitimate local ids with repeatable
    ``--allow-infores``. Missing input files fail in every mode: a typo'd path must
    never read as a clean bill of health.

    ``--prune`` rewrites the EDGES file in place, keeping only edges that validate
    strictly. Real defects (non-coercible values, extras the model will never declare,
    rejected predicates), the deliberate pending carryovers (``synonym``/``xref``/
    ``relation``/``provided_by``, ...), and lines that are not JSON at all are all
    removed. On a real graph the carryovers are most of the edges, so ``--prune``
    trades them for a strictly-compliant graph. The rewrite is atomic and kept lines
    stay byte-identical; a clean file is not rewritten at all, and a missing edges file
    is reported without ever being pruned. Nodes are validated and reported, never
    pruned. The exit code stays strict over the post-prune state: after a prune, every
    kept edge is strictly valid, so the run is compliant exactly when the nodes are,
    and the identity half classifies the PRUNED file -- what ships is what is checked.

    Args:
        nodes: Path to the built nodes NDJSON file (``<name>_<version>.nodes.ndjson``).
        edges: Path to the built edges NDJSON file (``<name>_<version>.edges.ndjson``).
        limit: Maximum example failures retained per file for the ``e.g.`` report lines
            (structural and identity examples alike); every record is still validated
            regardless of this cap. A missing file is reported and fails the run rather
            than reading as a pass.
        prune: Rewrite the edges file in place, removing every edge record that is not
            strictly KGX/Biolink-compliant, including the deliberate pending carryovers.
            The nodes file is never modified; a clean edges file is left untouched.
        rig: Optional RIG yaml (``<name>_<version>.RIG.yaml``); its infores values are
            classified too.
        allow-infores: Registry-exempt CURIE, repeatable; use for the locally-minted
            graph identifier (``rig.infores()``) and other deliberate locals.
        registry: Registry source. ``snapshot`` (default) validates offline against the
            bundled copy; ``refresh`` fetches the live upstream catalog (network) and
            caches it under the user cache dir; ``off`` skips the identity half
            entirely (structure-only validation).
        strict: Exit non-zero when any unregistered CURIE remains. Without it the
            identity half is advisory and never fails a run once inputs exist.
    """
    from tablassert.biolink import _validate_file, prune_kgx_edges, validate_kgx
    from tablassert.infores_registry import REGISTRY_RAW_URL, refresh_registry, validate_infores

    ids: frozenset[str] | None = refresh_registry() if registry == "refresh" else None

    if not prune:
        report: dict[str, Any] = validate_kgx(nodes, edges, limit=limit)
        print(f"biolink-model {report['biolink_version']}", file=sys.stderr)
        _print_kgx_section("nodes", report["nodes"], nodes)
        _print_kgx_section("edges", report["edges"], edges)
        structural_ok: bool = report["ok"]
        structural_missing: bool = report["nodes"]["missing"] or report["edges"]["missing"]
    else:
        # The prune path validates nodes-only itself and drives prune_kgx_edges' own single
        # pass for both edges sections, so the edges file is never read and written twice.
        nodes_section: dict[str, Any] = _validate_file(nodes, edge=False, limit=limit)
        pruned_report: dict[str, Any] = prune_kgx_edges(edges, limit=limit)
        print(f"biolink-model {pruned_report['biolink_version']}", file=sys.stderr)
        _print_kgx_section("nodes", nodes_section, nodes)
        _print_kgx_section("edges", pruned_report["before"], edges)
        if pruned_report["before"]["missing"]:
            print("KGX output is not Biolink-compliant.", file=sys.stderr)
            raise SystemExit(1)
        malformed_note: str = f" ({pruned_report['malformed']} malformed lines)" if pruned_report["malformed"] else ""
        print(f"edges: pruned {pruned_report['pruned']}/{pruned_report['before']['total']} non-compliant records{malformed_note}", file=sys.stderr)
        logger.info(
            f"validate-kgx edges: pruned {pruned_report['pruned']}/{pruned_report['before']['total']} (rewritten={pruned_report['rewritten']})"
        )
        for problem, count in pruned_report["dropped"]["problems"].items():
            print(f"  {count:>9}  {problem}", file=sys.stderr)
        for example in pruned_report["dropped"]["examples"][:3]:
            print(f"  e.g. {example['id']}: {', '.join(example['errors'])}", file=sys.stderr)
        _print_kgx_section("edges", pruned_report["after"], edges)
        nodes_ok: bool = not nodes_section["missing"] and nodes_section["total"] == nodes_section["valid"]
        structural_ok = nodes_ok and pruned_report["ok"]
        structural_missing = nodes_section["missing"]

    # The identity half classifies what ships: in prune mode the edges file has already
    # been rewritten, so the scan reads the post-prune state. When the structural half
    # already failed on absent inputs and no RIG was given, a second scan would only
    # echo the same file-not-found lines.
    infores_ok: bool = True
    if registry != "off" and (rig is not None or not structural_missing):
        infores_report: dict[str, Any] = validate_infores(nodes, edges, rig_path=rig, allow=tuple(allow_infores or ()), registry=ids, limit=limit)
        if ids is not None:
            infores_report["registry"]["source"] = f"{REGISTRY_RAW_URL} (refreshed)"
        _print_infores_report(infores_report, {"nodes": nodes, "edges": edges, "rig": rig})
        if any(infores_report["missing"].values()):
            print("infores validation failed: missing input files.", file=sys.stderr)
            infores_ok = False
        else:
            clean: bool = not infores_report["unregistered"] and not infores_report["malformed"]
            if strict:
                if infores_report["ok_strict"]:
                    print("All emitted infores CURIEs are registered." if clean else "infores validation passed (strict).", file=sys.stderr)
                else:
                    print("infores validation failed (strict): unregistered CURIEs remain.", file=sys.stderr)
                    infores_ok = False
            elif not clean:
                print("infores warnings present; pass --strict to fail on them.", file=sys.stderr)
            else:
                print("All emitted infores CURIEs are registered.", file=sys.stderr)

    if not structural_ok:
        print("KGX output is not Biolink-compliant.", file=sys.stderr)
        raise SystemExit(1)
    if not infores_ok:
        raise SystemExit(1)
    print("KGX output is Biolink-compliant.", file=sys.stderr)


@APP.command(name="quick-map")
def quick_map_command(
    # negative="" suppresses cyclopts' auto-generated --empty-terms help row: the reset
    # negation is unused noise that rendered as a phantom flag on the help page.
    terms: Annotated[list[str], cyclopts.Parameter(allow_leading_hyphen=False, negative="")],
    *,
    fullmap: Annotated[Path, cyclopts.Parameter(name=["--fullmap", "-f"])],
    taxon: Annotated[int | None, cyclopts.Parameter(name=["--taxon", "-t"])] = 9606,
    prioritize: Annotated[list[str] | None, cyclopts.Parameter(name=["--prioritize", "-p"], negative="")] = None,
    avoid: Annotated[list[str] | None, cyclopts.Parameter(name=["--avoid", "-a"], negative="")] = None,
    exclude_prefixes: Annotated[list[str] | None, cyclopts.Parameter(name=["--exclude-prefixes", "-ep"], negative="")] = None,
    exclude_regex: Annotated[list[str] | None, cyclopts.Parameter(name=["--exclude-regex", "-er"], negative="")] = None,
) -> None:
    """Show what fullmap entity resolution does with one or more terms, as a build would see it.

    Each term runs through the exact op chain a build runs per node column -- level-one/level-two
    normalization, probe-key extraction, the redb fetch, then ``filter_and_rank``'s taxon filter,
    category prioritize/avoid, prefix/regex exclusion, ``PR`` scoring, and best-tier dedup -- so the
    printed rows are the rows ``build-kg`` would emit for a cell holding that term under a
    ``NodeEncoding`` carrying the same settings. Use it to debug a mapping ("why did this cell
    resolve to that CURIE?") or to smoke-test a freshly built fullmap. The flags mirror the
    ``NodeEncoding`` config fields one-to-one; ``--taxon`` defaults to 9606 like the config does,
    and ``--taxon 0`` disables the filter like ``taxon: null`` does.

    ``--prioritize`` / ``--avoid`` accept the live Biolink entity category names -- the same
    vocabulary those keys accept in a table config -- validated at run time rather than enumerated
    here (the enum is built dynamically from the installed biolink-model), so an invalid value
    exits 2 naming the nearest valid names. A term with no matches prints an explicit "no matches"
    line (with the normalized probe keys it became, so the miss is diagnosable) and still exits 0:
    a miss is a finding, not a failure. Only usage errors and an unreadable fullmap exit 2.

    Args:
        terms: One or more terms to resolve (positional); any casing or whitespace, and a CURIE
            string works too because the fullmap indexes CURIEs and their equivalent identifiers
            as terms.
        fullmap: Path to the fullmap redb file, or a directory holding one (resolved like the
            ``Graph.fullmap`` config field: ``<dir>/fullmap.redb`` then ``<dir>/data/fullmap.redb``).
        taxon: NCBI taxon id constraining taxon-bearing matches; ``0`` disables the filter.
        prioritize: Biolink categories ranked higher (see the vocabulary note above).
        avoid: Biolink categories dropped entirely (see the vocabulary note above).
        exclude_prefixes: CURIE namespace prefixes (text before the first ':') to drop.
        exclude_regex: Regex patterns; any resolved CURIE matching one is dropped.
    """
    from difflib import get_close_matches

    from rich.console import Console
    from rich.table import Table

    from tablassert import rs
    from tablassert.fullmap import _KNOWN_CATEGORIES, _probe_keys, fullmap_db_path, quick_map

    def fail(message: str) -> NoReturn:
        """Print one actionable user error and use the CLI's documented exit status."""
        print(f"tablassert quick-map: {message}", file=sys.stderr)
        raise SystemExit(2)

    # Flag validation precedes any fullmap access: a typo'd category or regex is the user's fastest
    # loop to close, and these checks are pure so they fire before the redb is even resolved.
    for label, categories in (("prioritize", prioritize), ("avoid", avoid)):
        for category in categories or []:
            if category not in _KNOWN_CATEGORIES:
                nearest: list[str] = get_close_matches(category, _KNOWN_CATEGORIES, n=3, cutoff=0.5)
                hint: str = f"; did you mean {', '.join(repr(x) for x in nearest)}" if nearest else ""
                fail(
                    f"--{label} value {category!r} is not a known Biolink entity category{hint}; accepted values are the same names the `prioritize`/`avoid` config keys accept"
                )
    for pattern in exclude_regex or []:
        # Mirrors the NodeEncoding.exclude_regex validator: an empty pattern matches EVERY CURIE
        # and would silently drop all candidates, and a non-polars regex would die mid-filter.
        if not str(pattern).strip():
            fail(f"--exclude-regex entries must be non-empty patterns (an empty pattern matches every CURIE), got {pattern!r}")
        try:
            pl.Series([""]).str.contains(str(pattern))
        except Exception as e:
            fail(f"--exclude-regex entries must be polars-compatible regular expressions, got {pattern!r}: {e}")

    db: Path = fullmap_db_path(fullmap)
    if not db.is_file():
        fail(f"no fullmap database at {db} (resolved from {fullmap}) — build one with tablassert build-fullmap")

    results: dict[str, pl.DataFrame] = quick_map(
        terms,
        db,
        taxon=str(taxon) if taxon else None,
        prioritize=cast("list[Categories] | None", prioritize),
        avoid=cast("list[Categories] | None", avoid),
        exclude_prefixes=exclude_prefixes,
        exclude_regex=exclude_regex,
    )

    # One run-level header: the resolved database, its source version, the term count, and every
    # active filter, printed once so per-term output stays pure result. Joined as ONE string so a
    # wrapped line never dangles a separator at the break.
    console: Console = Console()
    active: list[str] = []
    if taxon:
        active.append(f"taxon={taxon}")
    for label, values in (("prioritize", prioritize), ("avoid", avoid), ("exclude-prefixes", exclude_prefixes), ("exclude-regex", exclude_regex)):
        if values:
            active.append(f"{label}={','.join(values)}")
    header: str = " · ".join(["[bold]quick-map[/bold]", str(db), f"source {rs.fullmap_source_version()}", f"{len(results)} term(s)", *active])
    console.print(header)

    # Probe keys come from the SAME normalization quick_map probed with, so a title can never
    # diagnose a miss against a key the lookup never used.
    keys: pl.DataFrame = _probe_keys(terms)
    columns: tuple[str, ...] = ("CURIE", "PREFERRED_NAME", "CATEGORY_NAME", "TAXON_ID", "SOURCE_NAME", "NLP_LEVEL", "PR")
    for term, matches in results.items():
        position: int = terms.index(term)
        level_one_key: str = str(keys.get_column("term")[position])
        level_two_key: str = str(keys.get_column("term_two")[position])
        probe: str = level_one_key if level_one_key == level_two_key else f"{level_one_key} | {level_two_key}"
        if matches.height == 0:
            console.print(f"{term!r} → {probe} · no matches")
            continue
        table: Table = Table(title=f"{term!r} → {probe}", title_justify="left")
        for column in columns:
            table.add_column(column)
        for row in matches.iter_rows(named=True):
            table.add_row(*(str(row[column]) for column in columns))
        console.print(table)


class PrebuiltFullmapUnavailable(Exception):
    """A prebuilt fullmap could not be fetched or extracted.

    Raised by :func:`fetch_prebuilt_fullmap` whenever the prebuilt is absent for this
    version, the download/extract fails, or the checksum mismatches. The
    ``build-fullmap`` command catches it to fall back to a from-scratch BABEL build, so
    it is control flow, not a user-facing error (it never reaches the docs-coded
    ``TablassertError`` surface). Carries a short reason so the fallback warning is actionable.
    """


def _prebuilt_fullmap_urls(babel_version: str) -> tuple[str, str]:
    """Resolve the prebuilt archive + checksum URLs for THIS Tablassert version.

    RENCI publishes a prebuilt ``fullmap.tar.zst`` (and a ``sha256sum.txt``) under
    ``{BABEL_BASE}/{babel_version}/fullmap/{tablassert_version}/``, where the version
    directory is the INSTALLED Tablassert package version (e.g. ``10.1.0``) — resolved from
    installed-package metadata, never hardcoded, so a new release looks itself up.

    Args:
        babel_version: BABEL snapshot label (the ``--babel-version`` value), e.g. ``2026jul22``.

    Returns:
        ``(archive_url, checksum_url)`` for ``fullmap.tar.zst`` and ``sha256sum.txt``.
    """
    base: str = f"{BABEL_BASE}/{babel_version}/fullmap/{get_version('tablassert')}"
    return f"{base}/fullmap.tar.zst", f"{base}/sha256sum.txt"


def _fetch_prebuilt_sha256(url: str) -> str | None:
    """Fetch ``sha256sum.txt`` and return the hex digest listed for ``fullmap.tar.zst``.

    Best-effort: a missing or malformed checksum file returns ``None`` so the caller
    proceeds without verification (with a warning) instead of blocking a download.

    Args:
        url: URL of the ``sha256sum.txt`` file.

    Returns:
        The 64-char lowercase hex sha256, or ``None`` if it could not be fetched/parsed.
    """
    try:
        request: Request = Request(url, headers={"User-Agent": "tablassert"})
        with urlopen(request, timeout=60) as response:
            body: str = response.read().decode("utf-8", errors="replace")
    except (HTTPError, OSError, URLError):
        return None
    for line in body.splitlines():
        parts: list[str] = line.split()
        # sha256sum format: "<hex>  <filename>" (two spaces, optional leading "*").
        # sha256sum format: "<hex>  <filename>"; binary mode prefixes the filename with "*".
        if len(parts) >= 2 and Path(parts[1].removeprefix("*")).name == "fullmap.tar.zst":
            digest: str = parts[0].lower()
            if len(digest) == 64 and all(c in "0123456789abcdef" for c in digest):
                return digest
    return None


def _extract_prebuilt_fullmap(archive: Path, output: Path, on_phase: Callable[[str], None], taxon_allowlist: list[int] | None = None) -> None:
    """Extract + validate the prebuilt archive via the Rust extension (GIL-free).

    Rust streams the ``.tar.zst``, validates it (schema version, build id, exact shard
    set, and the required taxon-allowlist identity), and atomically installs the primary +
    shards beside ``output`` named after its stem. Any failure surfaces as
    ``PrebuiltFullmapUnavailable`` so ``build-fullmap`` can fall back to a from-scratch
    BABEL build.

    Args:
        archive: Path to the downloaded ``fullmap.tar.zst``.
        output: Target primary redb path; shards land beside it as ``<stem>.s<N>.redb``.
        on_phase: Progress callback fired with the active step label.
        taxon_allowlist: NCBI taxon IDs the archive MUST have been filtered by; an archive
            whose recorded ``META.taxon_allowlist`` identity differs (or is absent) fails
            validation instead of installing a database this build would not produce.
            ``None`` accepts any bundle.

    Raises:
        PrebuiltFullmapUnavailable: If decompression, validation, or extraction fails.
    """
    from tablassert import rs

    try:
        rs.extract_prebuilt_fullmap(archive, output, progress=on_phase, taxon_allowlist=taxon_allowlist)
    except Exception as exc:
        raise PrebuiltFullmapUnavailable(f"failed to extract prebuilt archive: {exc}") from exc


def fetch_prebuilt_fullmap(
    output: Path, progress: PipelineProgress, version: str = BABEL_VERSION, aria2c: bool = False, taxon_allowlist: list[int] | None = None
) -> None:
    """Download and extract a prebuilt fullmap database from RENCI (instead of building).

    Two stages: download ``fullmap.tar.zst`` for THIS Tablassert version (cached +
    resumable, optionally via ``aria2c``) beside ``output``, then extract it in Rust so
    the primary redb and its shards land beside ``output`` named after its stem. The
    checksum published alongside the archive is verified when present, and the extracted
    bundle must have been filtered by ``taxon_allowlist``.

    Args:
        output: Target primary redb path; the archive is downloaded + extracted beside it.
        progress: Pipeline progress reporter.
        version: BABEL snapshot label selecting the RENCI release directory (NOT the
            Tablassert package version, which the URL derives from installed-package metadata).
        aria2c: Download the archive through the bundled aria2c binary from the optional
            ``[aria2]`` extra when true (the auto-resolved choice).
        taxon_allowlist: NCBI taxon IDs the published archive must have been filtered by;
            a bundle built without that exact filter is rejected as unavailable so the
            caller falls back to a filtered source build.

    Raises:
        PrebuiltFullmapUnavailable: If the prebuilt is absent for this version, the download
            or extraction fails, or the checksum mismatches. ``build-fullmap`` catches this
            and falls back to a from-scratch BABEL build.
    """
    release: str = get_version("tablassert")
    archive_url, checksum_url = _prebuilt_fullmap_urls(version)
    download_dir: Path = output.parent
    archive: Path = download_dir / "fullmap.tar.zst"

    # Stage 1/2: download the prebuilt archive (cached + resumable, like a BABEL file).
    progress.stage("Downloading Prebuilt Fullmap")
    start, advance, sub_step = progress.section_loop(1, "Download")
    start(f"fullmap.tar.zst v{release}")

    def report_progress(downloaded: int, total: int) -> None:
        sub_step(_download_detail(downloaded, total))

    try:
        if aria2c:
            sub_step("aria2c downloading")
            download_babel_file_aria2c("fullmap.tar.zst", archive_url, download_dir)
        else:
            sub_step("downloading")
            download_babel_file("fullmap.tar.zst", archive_url, download_dir, on_progress=report_progress)
    except BabelDownloadError as exc:
        raise PrebuiltFullmapUnavailable(f"prebuilt archive download failed: {exc}") from exc
    advance()

    # Best-effort checksum: RENCI publishes sha256sum.txt; verify when present, warn otherwise.
    expected: str | None = _fetch_prebuilt_sha256(checksum_url)
    if expected is None:
        download_logger.warning("No sha256sum.txt at {url}; skipping integrity check", url=checksum_url)
    else:
        sub_step("verifying checksum")
        hasher = hashlib.sha256()
        with archive.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        actual: str = hasher.hexdigest()
        if actual != expected:
            archive.unlink(missing_ok=True)
            raise PrebuiltFullmapUnavailable(f"checksum mismatch for {archive.name}: expected {expected}, got {actual}")

    # Stage 2/2: extract + validate in Rust (streaming zstd+tar, GIL-free). Rust extracts
    # to a temp dir on the output's filesystem and atomically renames the primary + shards
    # beside ``output`` after its stem — a custom --output stem is honored, not assumed to
    # be fullmap.redb.
    progress.stage("Extracting Fullmap")
    start, advance, sub_step = progress.section_loop(1, "Extract")
    start("fullmap.tar.zst")
    sub_step("extracting")
    _extract_prebuilt_fullmap(archive, output, on_phase=sub_step, taxon_allowlist=taxon_allowlist)

    # The extracted redb files are the cache; drop the multi-GB archive to free the space.
    archive.unlink(missing_ok=True)
    download_logger.info("Installed prebuilt fullmap v{release} -> {output}", release=release, output=output)


def load_taxon_allowlist() -> list[int]:
    """Load the checked-in experimental-taxon YAML list every fullmap build filters by."""
    import yaml

    raw: object = yaml.safe_load(TAXON_ALLOWLIST_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"taxon allowlist must be a YAML list: {TAXON_ALLOWLIST_PATH}")
    ids: list[int] = []
    frequencies: list[int] = []
    for expected_rank, entry in enumerate(raw, start=1):
        if (
            not isinstance(entry, dict)
            or entry.get("rank") != expected_rank
            or not isinstance(entry.get("taxon_id"), int)
            or entry["taxon_id"] <= 0
            or not isinstance(entry.get("frequency"), int)
            or entry["frequency"] < 0
        ):
            raise ValueError(f"invalid taxon allowlist entry at rank {expected_rank}: {entry!r}")
        ids.append(entry["taxon_id"])
        frequencies.append(entry["frequency"])
    if len(ids) != 100 or len(set(ids)) != len(ids):
        raise ValueError(f"taxon allowlist must contain 100 unique positive IDs: {TAXON_ALLOWLIST_PATH}")
    if any(
        left < right or (left == right and left_id > right_id) for (left, left_id), (right, right_id) in pairwise(zip(frequencies, ids, strict=True))
    ):
        raise ValueError(f"taxon allowlist ranks are not deterministically ordered: {TAXON_ALLOWLIST_PATH}")
    return ids


def build_fullmap_pipeline(
    output: Path,
    progress: PipelineProgress,
    cache: Path = Path("./fullmap/downloads"),
    version: str = BABEL_VERSION,
    aria2c: bool = False,
    taxon_allowlist: list[int] | None = None,
) -> None:
    """Build an embedded fullmap redb database from BABEL outputs.

    Runs the three-stage build pipeline: discover BABEL files → download
    BABEL files → build fullmap redb database.

    Args:
        output: Path to the output redb file.
        progress: Pipeline progress reporter.
        cache: Directory for downloaded BABEL files.
        version: BABEL version label.
        aria2c: Use the bundled aria2c binary from the optional ``[aria2]`` extra for downloads when true.
        taxon_allowlist: NCBI taxon IDs passed to Rust before interning; ``None`` builds
            an unfiltered database (``build-fullmap`` always passes the built-in list).
    """
    from tablassert import rs

    # Stage 1/3: discover BABEL files.
    progress.stage("Discovering BABEL Files")
    start, advance, sub_step = progress.section_loop(2, "Discover")
    start("class endpoints")
    sub_step("fetching listings")
    class_urls: list[tuple[str, str]] = babel_urls(version, BABEL_CLASS_ENDPOINTS, BABEL_CLASS_RE)
    advance()
    start("synonym endpoints")
    sub_step("fetching listings")
    synonym_urls: list[tuple[str, str]] = babel_urls(version, BABEL_SYNONYM_ENDPOINTS, BABEL_SYNONYM_RE)
    advance()

    # Stage 2/3: download BABEL files.
    progress.stage("Downloading BABEL Files")
    total_files: int = len(class_urls) + len(synonym_urls)
    start, advance, sub_step = progress.section_loop(total_files, "Download")

    def report_progress(downloaded: int, total: int) -> None:
        sub_step(_download_detail(downloaded, total))

    def download_one(filename: str, url: str, destination: Path) -> Path:
        start(filename)
        if aria2c:
            sub_step("aria2c downloading")
            path: Path = download_babel_file_aria2c(filename, url, destination)
        else:
            sub_step("downloading")
            path = download_babel_file(filename, url, destination, on_progress=report_progress)
        advance()
        return path

    class_files: list[Path] = [download_one(filename, url, cache / "classes") for filename, url in class_urls]
    synonym_files: list[Path] = [download_one(filename, url, cache / "synonyms") for filename, url in synonym_urls]

    # Stage 3/3: build fullmap database.
    progress.stage("Building Fullmap Database")
    # Rust drives per-phase progress (equivalents -> synonyms -> writing) via the
    # callback; the GIL is released during the build so the bar repaints live.
    on_progress = progress.dynamic_loop("Build")
    rs.build_fullmap_db(output, class_files, synonym_files, progress=on_progress, taxon_allowlist=taxon_allowlist)
    progress.end_section_task()

    logger.info(
        "Built fullmap v{version}: {classes} classes, {synonyms} synonyms -> {output}",
        version=version,
        classes=len(class_files),
        synonyms=len(synonym_files),
        output=output,
    )


def fullmap_matches_allowlist(output: Path, taxon_allowlist: list[int]) -> bool:
    """Whether the fullmap already at ``output`` was built with exactly this allowlist.

    Compares the database's recorded ``META.taxon_allowlist`` identity against the
    identity ``taxon_allowlist`` would record. A database that is absent, unreadable, of
    an outdated schema, or built unfiltered reports ``False`` — it is not the database
    this build produces, so reusing it would silently resolve a different term set.

    Args:
        output: Primary redb path a previous build may have left behind.
        taxon_allowlist: NCBI taxon IDs the current build filters by.

    Returns:
        True only when the existing database carries the matching allowlist identity.
    """
    from tablassert import rs

    return rs.fullmap_taxon_allowlist_identity(output) == rs.taxon_allowlist_identity(taxon_allowlist)


@APP.command(name="build-fullmap")
def build_fullmap(
    output: Annotated[Path, cyclopts.Parameter(name=["--output", "-o"])] = Path("./fullmap/data/fullmap.redb"),
    cache: Annotated[Path, cyclopts.Parameter(name=["--cache", "-c"])] = Path("./fullmap/downloads"),
    # Named --babel-version because the app-level --version flag shadowed the old --version
    # long form: `build-fullmap --version <snapshot>` printed the app version and exited 0
    # without ever running the command. -v keeps the short form.
    version: Annotated[str, cyclopts.Parameter(name=["--babel-version", "-v"])] = BABEL_VERSION,
    force: Annotated[bool, cyclopts.Parameter(name=["--force", "-f"], negative="")] = False,
) -> None:
    """Build an embedded fullmap redb database, or download a prebuilt one from RENCI.

    Every database this command installs is filtered by the built-in top-100
    experimental-taxon allowlist (``src/tablassert/data/experimental_taxa.yaml``): there is
    no flag, and no unfiltered database is ever installed. The filter is recorded in the
    database as a ``META.taxon_allowlist`` identity, which gates both reuse paths below.

    By default, first try to download a prebuilt ``fullmap.tar.zst`` published for THIS
    Tablassert version under ``{BABEL_BASE}/{version}/fullmap/<tablassert-version>/`` and
    extract it — far faster than building from BABEL. An archive whose recorded identity
    does not match the built-in allowlist is rejected during extraction (nothing is
    installed). If no prebuilt exists for this version (or the download/extract/identity
    check fails), fall back to a from-scratch filtered build. ``--force`` / ``-f`` skips
    the prebuilt attempt and always builds from BABEL outputs.

    Downloads (the prebuilt archive and BABEL files alike) pick their downloader
    automatically: the bundled ``aria2c`` binary from the optional ``[aria2]`` extra when
    that extra is installed, Tablassert's Python downloader otherwise. There is no flag
    to choose, and a failing ``aria2c`` download fails loud rather than silently
    re-downloading with the Python downloader.

    Args:
        output: Path to write the redb file (prebuilt extraction or build output).
        cache: Directory for downloaded BABEL files when building from scratch.
        version: BABEL snapshot date to fetch via ``--babel-version`` (a RENCI stamp, NOT
            Tablassert's version).
        force: Skip the prebuilt download and always rebuild from BABEL outputs.
    """
    allowlist_ids: list[int] = load_taxon_allowlist()
    # Reuse the database already at --output ONLY when it carries the current allowlist
    # identity. An unfiltered (or differently filtered) leftover from an older Tablassert
    # resolves a different term set, so it is rebuilt rather than silently reused.
    if not force and output.is_file() and output.stat().st_size > 0:
        if fullmap_matches_allowlist(output, allowlist_ids):
            print(f"tablassert build-fullmap: fullmap already present at {output}; skipping (use --force to rebuild).", file=sys.stderr)
            return
        # The probe also returns False for an unreadable/foreign file, so this warns and
        # rebuilds rather than trusting whatever sits at the path.
        logger.warning("Fullmap at {output} was not built with the current taxon allowlist (or is unreadable); rebuilding it.", output=output)
    # Downloader selection is automatic: the bundled aria2c when the [aria2] extra is
    # installed, else the Python downloader. Resolved once HERE — after the reuse path
    # above (which downloads nothing, so a no-op run stays silent) and before the choice
    # branches, so it is logged and announced exactly once and threaded to both download
    # consumers.
    use_aria2c: bool = extras.is_installed("aria2")
    # Announced on stderr in ADDITION to the log record: loguru's console sink only exists
    # inside run() below, and file logging needs the optional [log] extra, so the info line
    # alone would never reach a normal terminal. stderr follows the reuse short-circuit's
    # precedent above — and, like it, stays silent on a no-op run. Both outputs derive from the
    # ONE string below, so the log record and the stderr line cannot drift apart.
    choice: str = (
        "the [aria2] extra is installed; using the bundled aria2c for downloads"
        if use_aria2c
        else "the [aria2] extra is not installed; using the Python downloader"
    )
    download_logger.info(choice)
    print(f"tablassert build-fullmap: {choice}", file=sys.stderr)
    if not force:
        try:
            run(2, fetch_prebuilt_fullmap, output, version=version, aria2c=use_aria2c, taxon_allowlist=allowlist_ids)
            return
        except PrebuiltFullmapUnavailable as exc:
            logger.warning("Prebuilt fullmap unavailable ({reason}); building from BABEL outputs.", reason=exc)
    run(3, build_fullmap_pipeline, output, cache=cache, version=version, aria2c=use_aria2c, taxon_allowlist=allowlist_ids)
