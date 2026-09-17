from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict
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


def _section_store_path(h: str, head: bool = False, release: bool = False, qc: bool = False) -> Path:
    """Name a section's cached subgraph parquet so build-mode flags never share a cache file.

    The cached parquet's content depends on the build mode: ``--head`` samples
    rows, ``--release`` applies the release-mode significance filters, and
    ``--qc`` drops fullmap-audit rejects. ``Tcode.collect`` quick-exits on any
    existing store file, skipping the whole op chain, so each mode combination
    must cache under its own suffix or one mode's build is silently reused as
    another's.

    Args:
        h: Section config hash (``mkhash`` of the section dict).
        head: ``--head`` preview build flag.
        release: ``--release`` build flag.
        qc: ``--qc`` build flag.

    Returns:
        Store path like ``<h>.parquet`` with ``.head`` / ``.release`` / ``.qc``
        inserted before the extension for each set flag.
    """
    from tablassert.utils import STORE

    suffix: str = (".head" if head else "") + (".release" if release else "") + (".qc" if qc else "")
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
    configuration_file: Path, progress: PipelineProgress, release: bool = False, qc: bool = False, log: bool = False, head: bool = False
) -> None:
    """Load a graph YAML and build it through the shared in-process core."""
    graph: Graph = _load_graph(configuration_file)
    build_graph_pipeline(graph, configuration_file, progress, release=release, qc=qc, log=log, head=head)


def build_graph_pipeline(
    graph: Graph,
    configuration_file: Path,
    progress: PipelineProgress,
    release: bool = False,
    qc: bool = False,
    log: bool = False,
    head: bool = False,
    audit_sources: bool = True,
) -> None:
    """Build a validated :class:`Graph` without loading another graph YAML.

    The public ``build-kg`` command uses :func:`build_pipeline`, while agent audits pass
    a one-table temporary ``Graph`` here.  Keeping the core in-process lets those audits
    reuse the production stages and metadata without building every table in the caller's
    target graph.

    Args:
        graph: Validated graph model controlling tables, fullmap, identity, and RIG.
        configuration_file: Logical graph path used in validation error messages.
        progress: Pipeline progress reporter.
        release: When ``True``, emit release-mode artifacts.
        qc: When ``True``, run quality-control audits and final study assertions.
        log: When ``True``, enable per-section verbose logging.
        head: When ``True``, build a random sample of up to five rows per section.
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
        store: Path = _section_store_path(h, head=head, release=release, qc=qc)
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
                        "name": g.name,
                        "infores": g.rig.source_info.infores_id,
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
    """
    if qc:
        extras.require("qc", required_by="--qc")
    run(7 if qc else 6, build_pipeline, graph_configuration_file, release=release, qc=qc, log=log, head=head)


@APP.command(name="validate")
def validate(
    configuration_file: Annotated[Path, cyclopts.Parameter(name=["--configuration-file", "-f"])],
    schema: Annotated[Literal["graph", "table"], cyclopts.Parameter(name=["--schema", "-s"])],
) -> None:
    """Validate a YAML configuration file against the graph or table config schema.

    ``--schema graph`` validates the Graph model AND every referenced table; ``--schema
    table`` validates section syntax only. The schema is selected explicitly rather than
    sniffed from the YAML, so a config is always checked against the schema you expected.
    """
    if schema == "graph":
        run(2, validate_graph_pipeline, configuration_file)
    else:
        run(3, validate_pipeline, configuration_file)


@APP.command(name="validate-kgx")
def validate_kgx_command(
    nodes: Annotated[Path, cyclopts.Parameter(name=["--nodes", "-n"])],
    edges: Annotated[Path, cyclopts.Parameter(name=["--edges", "-e"])],
    limit: Annotated[int, cyclopts.Parameter(name=["--limit"])] = 20,
) -> None:
    """Validate built KGX NDJSON against the Biolink Model.

    Constructs every node and edge as the Biolink Pydantic class named by its own
    ``category`` -- the same classes ``NCATSTranslator/translator-ingests`` builds --
    and reports failures grouped by field and error type. Exits non-zero when any
    record fails, so a build can be gated in CI.
    """
    from tablassert.biolink import validate_kgx

    report: dict[str, Any] = validate_kgx(nodes, edges, limit=limit)
    print(f"biolink-model {report['biolink_version']}", file=sys.stderr)
    for label in ("nodes", "edges"):
        section: dict[str, Any] = report[label]
        if section["missing"]:
            # Never let a typo'd path read as a pass: 0/0 valid would otherwise exit 0.
            print(f"{label}: file not found ({nodes if label == 'nodes' else edges})", file=sys.stderr)
            logger.info(f"validate-kgx {label}: file not found")
            continue
        pending: int = section["valid_excluding_pending"] - section["valid"]
        suffix: str = f"; {pending} pending biolink-model support" if pending else ""
        print(f"{label}: {section['valid']}/{section['total']} valid ({section['failures']} failures{suffix})", file=sys.stderr)
        for problem, count in section["problems"].items():
            print(f"  {count:>9}  {problem}", file=sys.stderr)
        for example in section["examples"][:3]:
            print(f"  e.g. {example['id']}: {', '.join(example['errors'])}", file=sys.stderr)
        logger.info(f"validate-kgx {label}: {section['valid']}/{section['total']} valid")
    if not report["ok"]:
        print("KGX output is not Biolink-compliant.", file=sys.stderr)
        raise SystemExit(1)
    print("KGX output is Biolink-compliant.", file=sys.stderr)


@APP.command(name="quick-map")
def quick_map_command(
    terms: Annotated[list[str], cyclopts.Parameter(allow_leading_hyphen=False)],
    *,
    fullmap: Annotated[Path, cyclopts.Parameter(name=["--fullmap", "-f"])],
    taxon: Annotated[int | None, cyclopts.Parameter(name=["--taxon", "-t"])] = 9606,
    prioritize: Annotated[list[str] | None, cyclopts.Parameter(name=["--prioritize", "-p"])] = None,
    avoid: Annotated[list[str] | None, cyclopts.Parameter(name=["--avoid", "-a"])] = None,
    exclude_prefixes: Annotated[list[str] | None, cyclopts.Parameter(name=["--exclude-prefixes", "-ep"])] = None,
    exclude_regex: Annotated[list[str] | None, cyclopts.Parameter(name=["--exclude-regex", "-er"])] = None,
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


@APP.command(name="agent")
def agent(
    pmc_ids: Annotated[list[str], cyclopts.Parameter(allow_leading_hyphen=False)],
    *,
    graph_configuration_file: Annotated[Path, cyclopts.Parameter(name=["--configuration-file", "-f"])],
    model_id: Annotated[str | None, cyclopts.Parameter(name=["--model-id", "-m"])] = None,
    api_base: Annotated[str | None, cyclopts.Parameter(name=["--api-base", "-ab"])] = None,
    api_key: Annotated[str | None, cyclopts.Parameter(name=["--api-key", "-ak"])] = None,
    max_steps: Annotated[int, cyclopts.Parameter(name=["--max-steps", "-ms"])] = 20,
    min_rows: Annotated[int, cyclopts.Parameter(name=["--min-rows", "-mr"])] = 50,
    map_threshold: Annotated[float, cyclopts.Parameter(name=["--map-threshold", "-mt"])] = 0.25,
    max_improve_iters: Annotated[int, cyclopts.Parameter(name=["--max-improve-iters", "-mi"])] = 3,
    state_dir: Annotated[Path, cyclopts.Parameter(name=["--state-dir", "-sd"])] = Path(".tablassert") / "agent",
    backend: Annotated[Literal["openai", "litellm"], cyclopts.Parameter(name=["--backend", "-b"])] = "openai",
    reflexion: Annotated[bool, cyclopts.Parameter(name=["--reflexion"], negative="")] = False,
    judge_model: Annotated[str | None, cyclopts.Parameter(name=["--judge-model"])] = None,
    judge_threshold: Annotated[float | None, cyclopts.Parameter(name=["--judge-threshold"])] = None,
    biolink_threshold: Annotated[float, cyclopts.Parameter(name=["--biolink-threshold"])] = 0.0,
    local: Annotated[list[str] | None, cyclopts.Parameter(name=["--local", "-l"])] = None,
    optimize: Annotated[bool, cyclopts.Parameter(name=["--optimize", "-o"], negative="")] = False,
    distill: Annotated[bool, cyclopts.Parameter(name=["--distill", "-d", "-dt"], negative="")] = False,
    instructions_file: Annotated[Path | None, cyclopts.Parameter(name=["--instructions-file"])] = None,
    instructions_out: Annotated[Path | None, cyclopts.Parameter(name=["--instructions-out"])] = None,
    max_metric_calls: Annotated[int, cyclopts.Parameter(name=["--max-metric-calls"])] = 8,
    dataset: Annotated[Path | None, cyclopts.Parameter(name=["--dataset"])] = None,
    task_model: Annotated[str | None, cyclopts.Parameter(name=["--task-model"])] = None,
) -> None:
    """Autonomously derive, build, audit, and improve KG configs from PMC articles.

    Takes one or more PMC ids POSITIONALLY (``tablassert agent PMC11708054 [PMC...]``) and runs the
    deterministic supervisor over them. For each article the loop is: fetch the open-access
    supplementary tables -> an inner LLM agent derives a schema-gated Section config -> build_and_audit
    scores it -> a deterministic improve loop proposes/accepts edits IFF strictly better -> the config is
    accepted when coverage reaches ``--map-threshold`` or SKIPPED when the improve budget is exhausted.
    State checkpoints to ``--state-dir`` for downloads, artifacts, and run history; requested articles
    are processed again on later invocations so a successful rerun can replace its target-graph entry.

    Model config comes from ``--model-id``/``--api-base``/``--api-key`` OR the ``TABLASSERT_AGENT_MODEL_ID``
    / ``TABLASSERT_AGENT_API_BASE`` / ``TABLASSERT_AGENT_API_KEY`` environment variables (explicit flags win).
    Secrets are NEVER hardcoded or defaulted: a missing value fails loud (exit 2) BEFORE any model is built.
    Requires the ``[agent]`` extra (``pip install "tablassert[agent]"``).

    Args:
        pmc_ids: One or more PMC article ids (positional).
        graph_configuration_file: Caller-owned Graph YAML to validate, use for metadata/fullmap, and
            update in place after successful article builds.
        model_id: Model id (falls back to ``TABLASSERT_AGENT_MODEL_ID``).
        api_base: API base URL (falls back to ``TABLASSERT_AGENT_API_BASE``).
        api_key: API key (falls back to ``TABLASSERT_AGENT_API_KEY``).
        max_steps: Max inner-agent steps per article.
        min_rows: Minimum non-empty data rows for a table or worksheet to reach the agent (default 50;
            0 disables the small-table guard).
        map_threshold: Coverage an article must reach to be MAPPED.
        max_improve_iters: Max deterministic improve iterations per article.
        state_dir: Checkpoint/resume directory.
        backend: Model backend (``openai`` or ``litellm``).
        reflexion: Enable the tier-2 LLM reflexion improver (uses the same model config) for edits that
            may change predicate/source when the deterministic proposer stalls.
        judge_model: Optional model id for the semantic judge gate (uses ``--api-base``/``--api-key``);
            when set, MAPPED additionally requires the judge score to clear ``--judge-threshold``.
        judge_threshold: Semantic judge normalized-score threshold for MAPPED (default 0.5 when unset).
        biolink_threshold: Minimum Biolink pass rate of the built KGX for MAPPED (0.0 = report only).
            Every record stores its ``biolink_valid_pct`` / ``demoted_edge_pct`` regardless; raising this
            turns that measurement into a terminal gate, so a config whose output no Biolink class
            accepts is SKIPPED rather than registered.
        local: Use a local payload instead of fetching from PMC-AWS: a single DIR (applied to every id) or
            one or more ``PMCid=DIR`` mappings (per-article). Fails loud (exit 2) if a DIR does not exist.
        optimize: Run GEPA prompt optimization over the model config and persist optimized instructions
            (instead of running the supervisor); use ``--instructions-out`` to choose the output file.
        distill: Record every LLM call of the run (inner agent, judge, reflexion) as ChatML NDJSON
            under ``<state-dir>/distill/records.ndjson`` for fine-tuning (Unsloth Studio / QLoRA).
            Zero extra dependencies; ``tablassert distill-export`` converts it to an on-disk HF dataset.
        instructions_file: Load GEPA-optimized instructions (from a prior ``--optimize`` run) for this run.
        instructions_out: Where ``--optimize`` writes optimized instructions (default
            ``<state-dir>/optimized_instructions.yaml``).
        max_metric_calls: GEPA metric-call budget for ``--optimize``.
        dataset: Optional YAML/JSON list of ``{table_summary, coverage_feedback}`` examples for ``--optimize``.
            An example may also carry ``fullmap`` (a fullmap path used to score each proposed config with
            REAL coverage) and ``head`` (default true: score a fast 5-row preview; set false for full builds).
        task_model: Optional FAST model id for GEPA's many program evaluations (GEPA best practice: a cheap
            task LM + a strong reflection LM); ``--model-id`` is the strong reflection LM. Defaults to the
            reflection LM when unset.
    """
    from tablassert import agent as agent_mod
    from tablassert.graph_target import prepare_graph

    if min_rows < 0:
        print("tablassert agent: --min-rows must be a non-negative integer.", file=sys.stderr)
        raise SystemExit(2)

    prepared_graph = prepare_graph(graph_configuration_file)

    resolved_id, resolved_base, resolved_key = agent_mod.resolve_model_config(model_id, api_base, api_key)
    # Fail loud on any missing secret BEFORE building a model (so this path never touches smolagents).
    checks: tuple[tuple[str | None, str, str, str], ...] = (
        (resolved_id, "model_id", "model-id", agent_mod.ENV_MODEL_ID),
        (resolved_base, "api_base", "api-base", agent_mod.ENV_API_BASE),
        (resolved_key, "api_key", "api-key", agent_mod.ENV_API_KEY),
    )
    for value, which, flag, env in checks:
        if not value:
            print(f"tablassert agent: missing {which}. Set --{flag} or the {env} environment variable. Never hardcode secrets.", file=sys.stderr)
            raise SystemExit(2)

    # A normalized-score threshold outside [0, 1] (or non-finite, e.g. nan/inf) silently changes the
    # semantic gate (-1 passes every score); fail loud BEFORE any model is built.
    if judge_threshold is not None and not 0 <= judge_threshold <= 1:
        print("tablassert agent: --judge-threshold must be a finite number between 0 and 1.", file=sys.stderr)
        raise SystemExit(2)

    # Same reasoning for the compliance gate: a threshold outside [0, 1] would silently disable it.
    if not 0 <= biolink_threshold <= 1:
        print("tablassert agent: --biolink-threshold must be a finite number between 0 and 1.", file=sys.stderr)
        raise SystemExit(2)

    # --distill records the SUPERVISOR's model calls; the --optimize path returns early below and
    # GEPA's dspy LM bypasses the recording seam, so the combination would silently record nothing.
    if distill and optimize:
        print("tablassert agent: --distill records supervisor LLM calls and is not supported with --optimize.", file=sys.stderr)
        raise SystemExit(2)

    # Preflight the extras once the flags are known to be valid and BEFORE any model is
    # built or any article fetched. smolagents is otherwise only required per-article
    # (inside build_agent) and dspy only once GEPA starts, so an absent extra would
    # surface after real work. --optimize needs BOTH, and reports whichever is missing.
    extras.require("agent", required_by="tablassert agent")
    if optimize:
        extras.require("optimize", required_by="tablassert agent --optimize")

    # Distillation capture (optional, zero-dep): every LLM call is appended as one ChatML NDJSON
    # record. The inner agent's model is wrapped per-article inside run_supervisor (which knows the
    # pmc_id); the judge and reflexion models are wrapped at their construction sites below.
    distill_recorder: object | None = None
    if distill:
        from tablassert import distill as distill_mod

        distill_path: Path = agent_mod.distill_dir(state_dir) / distill_mod.RECORDS_FILENAME
        distill_recorder = distill_mod.DistillRecorder(distill_path)
        print(f"tablassert agent: distilling LLM calls -> {distill_path}")

    def build_model_factory() -> object:
        base_model: object = agent_mod.build_model(resolved_id, resolved_base, resolved_key, backend=backend)
        return agent_mod.make_retrying_model(base_model, secrets=(resolved_key,) if resolved_key else ())

    # Tier-2 reflexion (optional): a prompt-callable over the same model config, built lazily per call.
    reflexion_factory: Callable[[], object] | None = None
    if reflexion:

        def _make_reflexion() -> object:
            reflexion_model: object = agent_mod.build_model(resolved_id, resolved_base, resolved_key, backend=backend)
            reflexion_model = agent_mod.make_retrying_model(reflexion_model, secrets=(resolved_key,) if resolved_key else ())
            if distill_recorder is not None:
                reflexion_model = agent_mod.make_distilling_model(reflexion_model, distill_recorder, purpose="reflexion")
            return agent_mod.make_prompt_callable(reflexion_model)

        reflexion_factory = _make_reflexion

    # Semantic judge (optional): a prompt-callable over the judge model (same api_base/api_key).
    judge: object | None = None
    if judge_model is not None:
        judge_base_model: object = agent_mod.build_model(judge_model, resolved_base, resolved_key, backend=backend)
        judge_base_model = agent_mod.make_retrying_model(judge_base_model, secrets=(resolved_key,) if resolved_key else ())
        if distill_recorder is not None:
            judge_base_model = agent_mod.make_distilling_model(judge_base_model, distill_recorder, purpose="judge")
        judge = agent_mod.make_prompt_callable(judge_base_model)

    # Local payload (optional, W4): a DIR for all ids, or PMCid=DIR mappings; fail loud on a missing dir.
    def parse_local(specs: list[str] | None) -> dict[str, Path] | Path | None:
        if not specs:
            return None
        if len(specs) == 1 and "=" not in specs[0]:
            single: Path = Path(specs[0])
            if not single.is_dir():
                print(f"tablassert agent: --local directory does not exist: {single}", file=sys.stderr)
                raise SystemExit(2)
            return single
        mapping: dict[str, Path] = {}
        for spec in specs:
            if "=" not in spec:
                print(f"tablassert agent: --local expects DIR or PMCid=DIR, got {spec!r}", file=sys.stderr)
                raise SystemExit(2)
            pid, _, dirstr = spec.partition("=")
            pid = pid.strip()
            dirstr = dirstr.strip()
            if not pid or not dirstr:
                print(f"tablassert agent: --local expects PMCid=DIR, got {spec!r}", file=sys.stderr)
                raise SystemExit(2)
            per_dir: Path = Path(dirstr)
            if not per_dir.is_dir():
                print(f"tablassert agent: --local directory does not exist: {per_dir}", file=sys.stderr)
                raise SystemExit(2)
            mapping[pid] = per_dir
        return mapping

    local_payload: dict[str, Path] | Path | None = parse_local(local)

    # W6 optimization path: run GEPA over the model config and persist optimized instructions; do NOT run
    # the supervisor. The reflection LM is a real dspy.LM (deferred live path); offline tests monkeypatch
    # ``run_gepa``/``make_dspy_lm`` so no model/network fires.
    if optimize:
        # Resolve the output path to ABSOLUTE up front: GEPA's parallel metric builds chdir the process cwd
        # (os.chdir is process-global), so a relative --instructions-out must be anchored to the invocation
        # cwd here, not the cwd GEPA happens to leave behind when it returns.
        out_path: Path = (instructions_out if instructions_out is not None else (state_dir / "optimized_instructions.yaml")).resolve()
        reflection_lm: object = agent_mod.make_dspy_lm(resolved_id, resolved_base, resolved_key, backend=backend)
        # GEPA best practice: a FAST task LM for the many program evaluations + the strong model for the few
        # reflection steps. --task-model selects the task LM; it defaults to the reflection LM when unset.
        task_lm: object | None = (
            agent_mod.make_dspy_lm(task_model, resolved_base, resolved_key, backend=backend, temperature=agent_mod.GEPA_TASK_TEMPERATURE)
            if task_model
            else None
        )
        gepa_dataset: list[dict[str, object]] | None = agent_mod.load_gepa_dataset(dataset) if dataset is not None else None
        gepa_result: dict[str, object] = agent_mod.run_gepa(
            seed_instructions=agent_mod.INSTRUCTIONS,
            reflection_lm=reflection_lm,
            task_lm=task_lm,
            dataset=gepa_dataset,
            max_metric_calls=max_metric_calls,
        )
        # A failed GEPA compile falls back to the SEED instructions with stats["error"]; do NOT persist that
        # unoptimized prompt or report success -- fail loud with a non-zero status.
        gepa_stats: object = gepa_result.get("stats")
        gepa_error: object = gepa_stats.get("error") if isinstance(gepa_stats, dict) else None
        if gepa_error:
            print(f"tablassert agent: GEPA optimization failed: {gepa_error}", file=sys.stderr)
            raise SystemExit(1)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        opt_instructions: object = gepa_result.get("optimized_instructions", agent_mod.INSTRUCTIONS)
        opt_descriptions: object = gepa_result.get("optimized_descriptions")
        agent_mod.save_optimized_instructions(out_path, str(opt_instructions), opt_descriptions if isinstance(opt_descriptions, dict) else None)
        print(f"tablassert agent: optimized instructions -> {out_path}")
        return

    # Normal run: optionally load GEPA-optimized instructions (--instructions-file).
    run_instructions: str | None = agent_mod.load_optimized_instructions(instructions_file) if instructions_file is not None else None

    result: dict[str, object] = agent_mod.run_supervisor(
        list(pmc_ids),
        graph=prepared_graph.graph,
        graph_path=prepared_graph.path,
        build_model_factory=build_model_factory,
        map_threshold=map_threshold,
        max_improve_iters=max_improve_iters,
        max_steps=max_steps,
        min_rows=min_rows,
        state_dir=state_dir,
        reflexion_model_factory=reflexion_factory,
        judge_model=judge,
        judge_threshold=judge_threshold,
        biolink_threshold=biolink_threshold,
        local=local_payload,
        instructions=run_instructions,
        distill_recorder=distill_recorder,
    )

    metrics_raw: object = result.get("metrics")
    metrics: dict[str, object] = metrics_raw if isinstance(metrics_raw, dict) else {}
    records_raw: object = result.get("records")
    records: dict[str, object] = records_raw if isinstance(records_raw, dict) else {}

    def metric(key: str, default: float) -> float:
        value: object = metrics.get(key, default)
        return float(value) if isinstance(value, (int, float)) else default

    mapped: int = int(metric("mapped", 0))
    skipped: int = int(metric("skipped", 0))
    mean_best: float = metric("mean_best_coverage", 0.0)
    total_tokens: int = int(metric("total_tokens", 0))
    total_steps: int = int(metric("total_steps", 0))
    print(
        f"tablassert agent: processed {len(records)} article(s) ({mapped} mapped, {skipped} skipped); "
        f"mean best coverage {mean_best:.3f}; {total_tokens} tokens over {total_steps} steps."
    )


@APP.command(name="distill-weigh")
def distill_weigh(
    *,
    distill_dir: Annotated[Path, cyclopts.Parameter(name=["--distill-dir", "-dd"])],
    out: Annotated[Path, cyclopts.Parameter(name=["--out", "-o"])],
    policy: Annotated[str, cyclopts.Parameter(name=["--policy", "-p"])] = "threshold",
    threshold: Annotated[float, cyclopts.Parameter(name=["--threshold", "-t"])] = 0.75,
    top_n: Annotated[int, cyclopts.Parameter(name=["--top-n", "-tn"])] = 2,
    replication_k: Annotated[int, cyclopts.Parameter(name=["--replication-k", "-rk"])] = 2,
    reward_config: Annotated[Path | None, cyclopts.Parameter(name=["--reward-config", "-rc"])] = None,
    edge_ref: Annotated[float | None, cyclopts.Parameter(name="--edge-ref")] = None,
    purpose: Annotated[str, cyclopts.Parameter(name="--purpose")] = "agent",
    final_call_only: Annotated[bool, cyclopts.Parameter(name="--final-call-only", negative="")] = False,
    manifest: Annotated[Path | None, cyclopts.Parameter(name="--manifest")] = None,
) -> None:
    """Join distillation records to outcomes and write deterministic training rows."""
    from tablassert.distill_reward import (
        OUTCOME_COLUMN_PREFIX,
        POLICIES,
        RECORD_TYPE_OUTCOME,
        SELECTION_KEYS,
        RewardConfig,
        RewardConfigError,
        is_outcome_file,
        iter_record_files,
        join_records_outcomes,
        load_reward_config,
        median_edge_ref,
        read_ndjson,
        reward,
        select,
        write_ndjson,
    )

    def fail(message: str) -> NoReturn:
        """Print one actionable user error and use the CLI's documented exit status."""
        print(f"tablassert distill-weigh: {message}", file=sys.stderr)
        raise SystemExit(2)

    if not distill_dir.exists() or not distill_dir.is_dir():
        fail(f"--distill-dir must be an existing directory: {distill_dir}")
    if not out.name or out.is_dir():
        fail(f"--out must name a file, not a directory: {out}")
    if manifest is not None and (not manifest.name or manifest.is_dir()):
        fail(f"--manifest must name a file, not a directory: {manifest}")
    input_dir: Path = distill_dir.resolve()
    output_path: Path = out.resolve()
    try:
        output_path.relative_to(input_dir)
    except ValueError:
        pass
    else:
        fail(
            f"--out {out} is inside --distill-dir {distill_dir}; distill-export loads every *.ndjson there, "
            "so this would duplicate raw records and mix schemas"
        )
    manifest_path: Path = manifest.resolve() if manifest is not None else output_path.with_name(f"{output_path.name}.manifest.json")
    if manifest_path == output_path:
        fail("--manifest and --out must be different paths")
    try:
        manifest_path.relative_to(input_dir)
    except ValueError:
        pass
    else:
        fail(f"--manifest {manifest} is inside --distill-dir {distill_dir}; keep derived artifacts outside the input corpus")

    files: list[Path] = iter_record_files(input_dir)
    if not files:
        fail(f"no .ndjson files under {distill_dir}; provide a distillation corpus with records and outcomes")

    try:
        rows_by_file: dict[Path, list[dict[str, Any]]] = {path: read_ndjson(path) for path in files}
    except (OSError, ValueError) as exc:
        fail(str(exc))

    # Content inspection BEFORE the join. Every derived training row carries the selection keys and
    # ``outcome_matched``; a raw capture line carries neither, so this separates a previous weigh
    # output (or any foreign derived corpus) from raw input without a filename heuristic.
    derived_markers: frozenset[str] = frozenset(SELECTION_KEYS) | {"outcome_matched"}
    for path, file_rows in rows_by_file.items():
        marker: str | None = next((key for row in file_rows for key in sorted(derived_markers) if key in row), None)
        if marker is not None:
            fail(
                f"derived training output detected in {path} (it carries {marker!r}); --distill-dir must contain "
                "the raw records.ndjson/outcomes.ndjson written by agent --distill, not derived training output"
            )

    outcome_files: list[Path] = [path for path in files if is_outcome_file(path)]
    record_files: list[Path] = [path for path in files if path not in outcome_files]
    # is_outcome_file reads only the FIRST non-blank line, so a concatenated sink is classified as
    # records and would otherwise be joined as if every line were a record.
    for path in record_files:
        if any(row.get("record_type") == RECORD_TYPE_OUTCOME for row in rows_by_file[path]):
            fail(
                f"mixed record/outcome content detected in {path}; --distill-dir must contain raw "
                "records.ndjson/outcomes.ndjson in separate files, not one concatenated file"
            )
    if not record_files:
        fail(f"no record files under {distill_dir}; this directory contains only outcome files")
    if not outcome_files:
        fail(f"no outcomes file under {distill_dir}; outcomes may never have been written because a run crashed")

    records: list[dict[str, Any]] = [record for path in record_files for record in rows_by_file[path]]
    outcomes: list[dict[str, Any]] = [outcome for path in outcome_files for outcome in rows_by_file[path]]
    # A record-classified file may be empty; an outcome-classified file cannot be, because
    # is_outcome_file only classifies a file after successfully parsing one non-blank outcome line.
    if not records:
        fail(f"record files under {distill_dir} contain no records")

    if policy not in POLICIES:
        fail(f"unknown --policy {policy!r}; expected one of {', '.join(POLICIES)}")
    if not 0.0 <= threshold <= 1.0:
        fail(f"--threshold must be in [0, 1], got {threshold!r}")
    if top_n < 1:
        fail(f"--top-n must be at least 1, got {top_n!r}")
    if not 0 <= replication_k <= 3:
        fail(f"--replication-k must be in [0, 3], got {replication_k!r}")

    live_purposes: list[str] = sorted({value for value in (record.get("purpose") for record in records) if isinstance(value, str)})
    if purpose != "all" and purpose not in live_purposes:
        present: str = ", ".join(live_purposes) if live_purposes else "none"
        fail(f"unrecognized --purpose {purpose!r}; live purposes are {present}; also accepted: all")

    config: RewardConfig
    try:
        config = RewardConfig() if reward_config is None else load_reward_config(reward_config)
    except (OSError, RewardConfigError, TypeError, ValueError) as exc:
        fail(str(exc))

    try:
        rows, join_stats = join_records_outcomes(records, outcomes)
    except (TypeError, ValueError) as exc:
        fail(str(exc))
    if join_stats["matched"] == 0 and join_stats["records"] > 0:
        fail("no records joined an outcome; likely a pre-v2 corpus with no run_id, or outcomes were never written because the run crashed")

    # Rebuild the same append-order index used by join_records_outcomes. The last line wins for
    # both reward and flattening, and non-string run ids are unjoinable by definition.
    outcome_by_run: dict[str, Mapping[str, Any]] = {}
    for outcome in outcomes:
        run_id: object = outcome.get("run_id")
        if isinstance(run_id, str):
            outcome_by_run[run_id] = outcome
    comparable_outcomes: list[Mapping[str, Any]] = list(outcome_by_run.values())
    if edge_ref is not None and (
        isinstance(edge_ref, bool) or not isinstance(edge_ref, (int, float)) or not math.isfinite(edge_ref) or edge_ref <= 0
    ):
        fail(f"--edge-ref must be a finite positive number, got {edge_ref!r}")
    resolved_edge_ref: float | None
    edge_ref_source: str
    if edge_ref is not None:
        resolved_edge_ref, edge_ref_source = float(edge_ref), "overridden"
    elif config.edge_ref is not None:
        resolved_edge_ref, edge_ref_source = config.edge_ref, "overridden"
    else:
        resolved_edge_ref = median_edge_ref(comparable_outcomes)
        edge_ref_source = "derived" if resolved_edge_ref is not None else "null"
    if resolved_edge_ref is None:
        print("tablassert distill-weigh: warning: edge_ref is null; breadth contributes 0.0 for every row", file=sys.stderr)

    def final_call_rankable(record: Mapping[str, Any]) -> bool:
        """True when a record can be ranked for --final-call-only: a string run id plus an int call index."""
        rank_run_id: object = record.get("run_id")
        rank_call_index: object = record.get("call_index")
        return isinstance(rank_run_id, str) and isinstance(rank_call_index, int) and not isinstance(rank_call_index, bool)

    unrankable_final_call: int = 0
    filtered_indices: list[int] = list(range(len(rows)))
    if purpose != "all":
        filtered_indices = [index for index in filtered_indices if records[index].get("purpose") == purpose]
    if final_call_only:
        final_by_run: dict[str, tuple[int, int]] = {}
        rankable: set[int] = set()
        for index in filtered_indices:
            record: Mapping[str, Any] = records[index]
            if not final_call_rankable(record):
                continue  # an unrankable record cannot be proven non-final, so it is retained, never dropped
            rankable.add(index)
            rank_run_id: str = record["run_id"]
            rank_call_index: int = record["call_index"]
            previous = final_by_run.get(rank_run_id)
            if previous is None or (rank_call_index, index) > previous:
                final_by_run[rank_run_id] = (rank_call_index, index)
        retained: set[int] = {value[1] for value in final_by_run.values()}
        unrankable_final_call = len(filtered_indices) - len(rankable)
        if unrankable_final_call:
            print(
                f"tablassert distill-weigh: warning: --final-call-only retained {unrankable_final_call} record(s) "
                "with no rankable run_id/call_index pair; they cannot be proven non-final",
                file=sys.stderr,
            )
        filtered_indices = [index for index in filtered_indices if index not in rankable or index in retained]
    filtered_rows: list[dict[str, Any]] = [rows[index] for index in filtered_indices]
    if not filtered_rows:
        # Defensive: --purpose is validated against the live purposes above and an unrankable record
        # is retained rather than dropped, so only a future narrowing filter can empty the selection.
        narrowing: str = "--final-call-only" if final_call_only else f"purpose filter {purpose!r}"
        fail(f"{narrowing} matched no records; live purposes are {', '.join(live_purposes) or 'none'}")
    try:
        for row in filtered_rows:
            run_id = row.get("run_id")
            nested = outcome_by_run.get(run_id) if isinstance(run_id, str) else None
            row["weight"] = 0.0 if nested is None else reward(nested, config, edge_ref=resolved_edge_ref)
    except (TypeError, ValueError) as exc:
        fail(str(exc))
    try:
        selected_rows = select(filtered_rows, policy=policy, threshold=threshold, top_n=top_n, replication_k=replication_k)
    except (TypeError, ValueError) as exc:
        fail(str(exc))

    try:
        rows_written: int = write_ndjson(output_path, selected_rows)
        # The record schema carries no config hash of its own: the config identity rides on the
        # flattened ``outcome_config_yaml_sha256`` column, so the diversity counter must read that.
        config_column: str = f"{OUTCOME_COLUMN_PREFIX}config_yaml_sha256"
        before_pmc = {row.get("pmc_id") for row in selected_rows if row.get("pmc_id") is not None}
        before_config = {row.get(config_column) for row in selected_rows if row.get(config_column) is not None}
        chosen = [row for row in selected_rows if row.get("selected") is True]
        after_pmc = {row.get("pmc_id") for row in chosen if row.get("pmc_id") is not None}
        after_config = {row.get(config_column) for row in chosen if row.get(config_column) is not None}
        resolved_config: dict[str, Any] = asdict(config)
        resolved_config["edge_ref"] = resolved_edge_ref
        manifest_data: dict[str, Any] = {
            "schema_version": 2,
            "tablassert_version": get_version("tablassert"),
            "reward_config": resolved_config,
            "edge_ref": resolved_edge_ref,
            "edge_ref_source": edge_ref_source,
            "policy": policy,
            "threshold": threshold,
            "top_n": top_n,
            "replication_k": replication_k,
            "purpose": purpose,
            "final_call_only": final_call_only,
            "unrankable_final_call": unrankable_final_call,
            "join_stats": join_stats,
            "rows_written": rows_written,
            "selected_count": len(chosen),
            "selected_zero_weight": sum(1 for row in chosen if row.get("weight") == 0.0),
            "unmatched_count": join_stats["unmatched"],
            "distinct": {
                "pmc_id": {"before_selection": len(before_pmc), "after_selection": len(after_pmc)},
                "config_yaml_sha256": {"before_selection": len(before_config), "after_selection": len(after_config)},
            },
            "source_files": [str(path) for path in files],
            "source_files_by_kind": {"records": [str(path) for path in record_files], "outcomes": [str(path) for path in outcome_files]},
        }
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = manifest_path.with_name(f".{manifest_path.name}.tmp")
        try:
            temporary.write_text(json.dumps(manifest_data, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            os.replace(temporary, manifest_path)
        finally:
            temporary.unlink(missing_ok=True)
    except (OSError, TypeError, ValueError) as exc:
        fail(f"could not write output or manifest: {exc}")
    print(f"tablassert distill-weigh: {rows_written} row(s), {len(chosen)} selected, policy {policy}, edge_ref {resolved_edge_ref}, output {out}")


@APP.command(name="distill-export")
def distill_export(
    *, distill_dir: Annotated[Path, cyclopts.Parameter(name=["--distill-dir", "-dd"])], out: Annotated[Path, cyclopts.Parameter(name=["--out", "-o"])]
) -> None:
    """Export a recorded distillation NDJSON dataset to an on-disk Hugging Face dataset.

    Loads every RECORD ``*.ndjson`` under ``--distill-dir`` (the ChatML records written by
    ``tablassert agent --distill``; outcome files are recognized by their content and skipped),
    normalizes the whole corpus to one uniform key set, and hands that single file to
    ``datasets.load_dataset("json", ...)`` whose result is written with ``save_to_disk`` to
    ``--out``. Requires the ``distill`` extra (``pip install "tablassert[distill]"``). The raw
    NDJSON also loads directly in Unsloth Studio — this export is only needed for
    ``datasets``-native workflows.

    Why not pass the corpus straight to ``load_dataset``: it infers ``features`` from the first
    block of the FIRST file only and raises ``CastError`` when a later file carries a column the
    inferred schema lacks — an append-only corpus spanning schema versions (a v1 line without
    ``run_id`` next to a v2 line) would be unexportable. Unioning the keys and re-emitting every
    row with an explicit ``null`` for an absent key makes the inferred schema correct by
    construction, so a v1-only corpus keeps exporting too. A column whose Python type varies
    across rows fails loud instead (exit 2), because ``datasets`` would otherwise silently
    JSON-encode that column into a string — a type demotion that corrupts the corpus without an
    error.

    Args:
        distill_dir: Directory holding the recorded ``*.ndjson`` files (default output of
            ``tablassert agent --distill`` is ``<state-dir>/distill``).
        out: Destination directory for the ``save_to_disk`` dataset.
    """
    from tablassert.distill_reward import detect_type_conflicts, is_outcome_file, iter_record_files, normalize_rows, read_ndjson, write_ndjson

    def fail(message: str) -> NoReturn:
        """Print one actionable user error and use the CLI's documented exit status."""
        print(f"tablassert distill-export: {message}", file=sys.stderr)
        raise SystemExit(2)

    # Input validation precedes the extras preflight: a missing directory is the user's typo, an
    # absent extra is their environment, and the typo is the faster loop to close first. Every
    # check below is pure stdlib (via distill_reward), so it all fires in the base environment
    # too — the partition, normalization and type-conflict rejection never depend on `datasets`.
    files: list[Path] = iter_record_files(distill_dir)
    if not files:
        print(f"tablassert distill-export: no .ndjson records under {distill_dir} — run tablassert agent --distill first.", file=sys.stderr)
        raise SystemExit(2)
    # Content-based partition (a renamed or relocated outcomes file is still recognized); only
    # the record half is loaded, so outcomes and records never mix into one table.
    outcome_files: list[Path] = [path for path in files if is_outcome_file(path)]
    record_files: list[Path] = [path for path in files if path not in outcome_files]
    if not record_files:
        fail(
            f"only outcome files under {distill_dir}; distill-export loads records only — "
            "record the corpus with tablassert agent --distill, or point --distill-dir at its records"
        )
    try:
        rows: list[dict[str, Any]] = [row for path in record_files for row in read_ndjson(path)]
    except (OSError, ValueError) as exc:
        fail(str(exc))
    if not rows:
        # An empty record corpus would otherwise surface as a bare StopIteration from inside
        # load_dataset's JSON reader (it cannot infer a schema from zero rows).
        fail(f"record files under {distill_dir} contain no records; nothing to export")
    # Normalize BEFORE loading: normalize_rows unions every row's keys (union_keys, first-appearance
    # order) and projects each row onto that union with an explicit None for absent keys, so the
    # single file handed to load_dataset has one uniform key set across every block — first-block
    # inference is exactly what must not be trusted on an append-only corpus (see the docstring).
    normalized: list[dict[str, Any]] = normalize_rows(rows)
    conflicts: dict[str, set[str]] = detect_type_conflicts(normalized)
    conflicting: list[tuple[str, str]] = [(key, ", ".join(sorted(types))) for key, types in conflicts.items() if len(types) > 1]
    if conflicting:
        listed: str = "; ".join(f"{key!r} is {types}" for key, types in conflicting)
        fail(
            f"type conflict: {listed}; datasets would silently JSON-encode a varying-type column "
            "into a string — keep each column to one non-null type and re-export"
        )
    extras.require("distill", required_by="tablassert distill-export")
    from datasets import load_dataset  # local import keeps the CLI import-light  # pyright: ignore[reportMissingImports]

    # The single uniform file passed to load_dataset lives OUTSIDE --distill-dir (nothing is ever
    # written into the append-only corpus) and is removed whether or not the export succeeds.
    workspace: Path = Path(tempfile.mkdtemp(prefix="tablassert-distill-export-"))
    try:
        uniform: Path = workspace / "normalized.ndjson"
        write_ndjson(uniform, normalized)
        dataset: object = load_dataset("json", data_files=[str(uniform)], split="train")
        out.parent.mkdir(parents=True, exist_ok=True)
        dataset.save_to_disk(str(out))  # pyright: ignore[reportAttributeAccessIssue]
        print(f"tablassert distill-export: {len(dataset)} record(s) from {len(record_files)} file(s) -> {out}")  # pyright: ignore[reportArgumentType]
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


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
        babel_version: BABEL snapshot label (the ``--version`` value), e.g. ``2026jul22``.

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
    version: Annotated[str, cyclopts.Parameter(name=["--version", "-v"])] = BABEL_VERSION,
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
        version: BABEL snapshot date to fetch (a RENCI stamp, NOT Tablassert's version).
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
