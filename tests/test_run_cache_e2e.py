"""End-to-end coverage for the run cache: a shared op prefix executes ONCE, and nothing else changes.

``tablassert.runcache`` digests, prices, plans, and snapshots op prefixes; ``lib.compile_subgraph``
grew the ``resume``/``snapshot`` seam; ``cli.build_graph_pipeline`` Stage 5 wires the three
together. These tests drive the REAL six-stage ``build_pipeline`` against a tiny real
``rs.build_fullmap_db`` redb (the ``test_store_invalidation_e2e.py`` pattern — no monkeypatched
lookup, no fixtures standing in for the pipeline) and pin the user-visible contract:

* two sections opening with the same source + encode + resolve instructions compute that prefix
  once, and both still write byte-identical stores to a build that shares nothing;
* a build that shares nothing never creates a cache — not even a temp directory;
* the graph's subgraph sequence follows the ORIGINAL section order even though sharing reordered
  execution, so the artifacts cannot change;
* the cache directory is gone when the build ends, including when a section raises mid-build.

Counting uses seams that cannot perturb a plan: ``lib.csv`` (an op callable, patched BEFORE
``Tcode.collect`` runs — op tuples bind callables at collect time, so patching afterwards counts
nothing) and ``fullmap.lookup_rows`` (called inside ``resolve_batch``, never an op itself, so no
digest, cost, or label-masking table ever sees the wrapper).
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import Any

import polars as pl
import pytest

from tablassert import fullmap, lib, rs, runcache
from tablassert.cli import build_pipeline
from tablassert.ingests import to_yaml
from tablassert.progress import PipelineProgress
from tablassert.runcache import CACHE_DIR_PREFIX, PlanEntry, RunCache, RunCacheStats, RunPlan

Ops = list[tuple[Callable, tuple[Any, ...]]]
"""One section's cleaned op list, the shape ``Tcode.collect`` returns and ``plan_run`` accepts."""


class _SectionFailure(RuntimeError):
    """Stand-in for any error a section raises mid-build."""


def _build_real_redb(root: Path) -> Path:
    """Build the tiny real fullmap these smokes resolve against (``brca1``/``mapk1``)."""
    root.mkdir(parents=True, exist_ok=True)
    classes: Path = root / "classes.ndjson"
    classes.write_text('{"id": "HGNC:1100", "equivalent_identifiers": [{"identifier": "NCBIGene:672"}]}\n')
    synonyms: Path = root / "synonyms.ndjson"
    synonyms.write_text(
        '{"curie": "HGNC:1100", "preferred_name": "BRCA1", "names": ["BRCA1", "brca1"], "types": ["Gene"], "taxa": ["NCBITaxon:9606"]}\n'
        '{"curie": "HGNC:6871", "preferred_name": "MAPK1", "names": ["MAPK1", "mapk1"], "types": ["Gene"], "taxa": ["NCBITaxon:9606"]}\n'
    )
    output: Path = root / "data" / "fullmap.redb"
    rs.build_fullmap_db(output, [classes], [synonyms])
    return output


def _write_inputs(root: Path, sections: list[tuple[str, str]]) -> Path:
    """Write the shared build inputs: one source per name, one table with ``len(sections)`` sections.

    Args:
        root: Directory to write into (created).
        sections: ``(source_name, publication)`` per section, in section order. Sections naming the
            SAME source repeat the load → encode → resolve head verbatim and differ only in their
            provenance tail, which is the sharing a real config produces; a different source name
            makes a section share nothing.

    Returns:
        Path to the table YAML (each run directory gets its own graph YAML).
    """
    root.mkdir(parents=True, exist_ok=True)
    for name in dict.fromkeys(source for source, _ in sections):
        (root / f"{name}.tsv").write_text("brca1\tmapk1\n")
    table: Path = root / "table.yaml"
    to_yaml(
        table,
        {
            "sections": [
                {
                    "source": {"kind": "text", "local": str(root / f"{name}.tsv"), "url": [f"https://example.com/{name}.tsv"], "delimiter": "\t"},
                    "statement": {
                        "subject": {"method": "column", "encoding": "A"},
                        "predicate": "associated_with",
                        "object": {"method": "column", "encoding": "B"},
                    },
                    "provenance": {"repo": "PMC", "publication": publication},
                }
                for name, publication in sections
            ]
        },
    )
    _build_real_redb(root / "fullmap")
    return table


def _write_graph(run: Path, table: Path, rig_factory: Any, name: str) -> Path:
    """Write one run's graph YAML, emitting its artifacts into that run's own directory."""
    graph: Path = run / "graph.yaml"
    to_yaml(
        graph,
        {
            "name": name,
            "version": "1.0.0",
            "tables": [str(table)],
            "fullmap": str(table.parent / "fullmap" / "data" / "fullmap.redb"),
            "rig": rig_factory(run, infores_id="infores:runcache-kg", source_info={"description": "run cache smoke graph"}),
        },
    )
    return graph


def _run_dir(root: Path, name: str) -> Path:
    """Create an isolated build directory with its own cwd-relative ``.tablassert/store``."""
    directory: Path = root / name
    (directory / ".tablassert" / "store").mkdir(parents=True)
    return directory


def _stores(run: Path) -> dict[str, bytes]:
    """The run's section-store parquets by file name, as bytes."""
    return {path.name: path.read_bytes() for path in sorted((run / ".tablassert" / "store").glob("*.parquet"))}


def _store_publications(run: Path) -> list[str]:
    """The publication each stored section carries, sorted (store file names are hashes)."""
    frames: list[pl.DataFrame] = [pl.read_parquet(path) for path in sorted((run / ".tablassert" / "store").glob("*.parquet"))]
    return sorted(frame["publications"].to_list()[0][0] for frame in frames)


def _count_prefix_ops(monkeypatch: pytest.MonkeyPatch) -> tuple[list[Path], list[tuple[Path, list[str]]]]:
    """Count the shared prefix's two observable calls: the source load and the fullmap round trip.

    Both wrappers go in BEFORE ``Tcode.collect`` runs (i.e. before ``build_pipeline``), because an
    op tuple binds its callable at collect time. ``lib.csv`` is patched directly — it is an op
    callable, and every section binds the same wrapper so their digests still match; the wrapper is
    absent from ``OP_COST`` and falls back to ``DEFAULT_OP_COST``, which shifts the cost floor by
    one unit without affecting a prefix whose ``resolve_batch`` (cost 60) dominates it.
    ``fullmap.lookup_rows`` is NOT an op (``resolve_batch`` calls it), so counting it leaves every
    digest, cost, and label-masking table untouched while still proving the resolve step ran.

    Returns:
        ``(loads, lookups)`` — source paths loaded, and ``(db, terms)`` per fullmap round trip.
    """
    loads: list[Path] = []
    real_csv: Callable = lib.csv

    def counting_csv(p: Path, sep: str) -> pl.LazyFrame:
        """Record one source load, then really load."""
        loads.append(p)
        return real_csv(p, sep)

    monkeypatch.setattr(lib, "csv", counting_csv)
    lookups: list[tuple[Path, list[str]]] = []
    real_lookup_rows: Callable = fullmap.lookup_rows

    def counting_lookup_rows(db: Path, terms: list[str]) -> list[dict[str, object]]:
        """Record one ``resolve_batch`` fullmap round trip, then really look the terms up."""
        lookups.append((db, list(terms)))
        return real_lookup_rows(db, terms)

    monkeypatch.setattr(fullmap, "lookup_rows", counting_lookup_rows)
    return loads, lookups


def _record_run_caches(monkeypatch: pytest.MonkeyPatch) -> tuple[list[RunCacheStats], list[Path]]:
    """Install a ``RunCache`` subclass that records each run's counters and directory.

    The least invasive stats seam available: Stage 5 resolves ``RunCache`` from
    ``tablassert.runcache`` when it runs, so production code needs no hook and no fail-loudly path
    is weakened — the subclass only observes, then delegates to the real ``__enter__``/``__exit__``.

    Returns:
        ``(summaries, directories)`` — one entry per cache the build opened, in open order.
    """
    summaries: list[RunCacheStats] = []
    directories: list[Path] = []

    class RecordingRunCache(RunCache):
        """Observing ``RunCache``: publishes the directory it opened and the counters it closed with."""

        def __enter__(self: RecordingRunCache) -> RecordingRunCache:
            """Open the real cache and record its directory."""
            super().__enter__()
            directories.append(self.directory)
            return self

        def __exit__(self: RecordingRunCache, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None) -> None:
            """Record the final counters, then delete the tree exactly as the real cache does."""
            summaries.append(self.stats())
            super().__exit__(exc_type, exc, tb)

    monkeypatch.setattr(runcache, "RunCache", RecordingRunCache)
    return summaries, directories


def _record_plans(monkeypatch: pytest.MonkeyPatch) -> list[RunPlan]:
    """Record the plan Stage 5 computed, without changing it."""
    plans: list[RunPlan] = []
    real_plan_run: Callable = runcache.plan_run

    def recording_plan_run(op_lists: list[Ops]) -> RunPlan:
        """Plan for real and keep the result."""
        plan: RunPlan = real_plan_run(op_lists)
        plans.append(plan)
        return plan

    monkeypatch.setattr(runcache, "plan_run", recording_plan_run)
    return plans


def _disable_sharing(monkeypatch: pytest.MonkeyPatch) -> list[list[Ops]]:
    """Force Stage 5 down its zero-sharing branch by planning an identity plan with no roles.

    There is deliberately no CLI flag to turn the run cache off, so the control build patches the
    planner — the one gate Stage 5 consults (``RunPlan.shares``). ``plan_run`` is resolved from
    ``tablassert.runcache`` when Stage 5 runs, so this reproduces the code path an unplanned build
    takes today: original section order, no cache, no temp directory.

    Returns:
        The op lists each planned run was handed (evidence the build really did plan).
    """
    planned: list[list[Ops]] = []

    def identity_plan(op_lists: list[Ops]) -> RunPlan:
        """Plan every section as ungrouped, in original order."""
        planned.append(op_lists)
        return RunPlan(execution_order=list(range(len(op_lists))), entries=[PlanEntry() for _ in op_lists])

    monkeypatch.setattr(runcache, "plan_run", identity_plan)
    return planned


def _record_subgraphs(monkeypatch: pytest.MonkeyPatch) -> list[list[Path]]:
    """Record the subgraph sequence handed to ``compile_graph``, then compile for real."""
    sequences: list[list[Path]] = []
    real_compile_graph: Callable = lib.compile_graph

    def recording_compile_graph(subgraphs: list[Path], *args: Any, **kwargs: Any) -> Any:
        """Keep a copy of the subgraph list Stage 6 was given."""
        sequences.append(list(subgraphs))
        return real_compile_graph(subgraphs, *args, **kwargs)

    monkeypatch.setattr(lib, "compile_graph", recording_compile_graph)
    return sequences


def test_two_sections_sharing_source_compute_prefix_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """Two sections repeating a source + resolve head run it ONCE and write identical stores.

    WHY: this is the feature. A control build with sharing planned away and a build with the real
    plan run the SAME inputs in isolated directories, so the cache is the only difference. Sharing
    must halve the prefix work (one source load, one fullmap round trip instead of one per
    section), must show up in the cache's own counters (a snapshot stored, a hit served), and — the
    part that makes it safe — must leave both sections' parquet byte-identical to the control's, so
    reuse can never change what a build produces.
    """
    table: Path = _write_inputs(tmp_path / "inputs", [("a", "PMC0000001"), ("a", "PMC0000002")])
    control: Path = _run_dir(tmp_path, "control")
    sharing: Path = _run_dir(tmp_path, "sharing")

    with monkeypatch.context() as control_patch:
        control_patch.chdir(control)
        control_loads, control_lookups = _count_prefix_ops(control_patch)
        control_summaries, control_dirs = _record_run_caches(control_patch)
        control_planned = _disable_sharing(control_patch)
        build_pipeline(_write_graph(control, table, rig_factory, "RUNCACHE_KG"), PipelineProgress(total_stages=6))

    with monkeypatch.context() as sharing_patch:
        sharing_patch.chdir(sharing)
        sharing_loads, sharing_lookups = _count_prefix_ops(sharing_patch)
        summaries, directories = _record_run_caches(sharing_patch)
        plans = _record_plans(sharing_patch)
        build_pipeline(_write_graph(sharing, table, rig_factory, "RUNCACHE_KG"), PipelineProgress(total_stages=6))

    # The control really did plan (and really did share nothing).
    assert len(control_planned) == 1
    assert len(control_planned[0]) == 2
    # The sharing build formed exactly one group: a producer and a consumer of one digest.
    assert len(plans) == 1
    plan: RunPlan = plans[0]
    assert plan.shares is True
    assert sum(1 for entry in plan.entries if entry.produce_digest is not None) == 1
    assert sum(1 for entry in plan.entries if entry.resume_digest is not None) == 1

    # Two sections built in both runs, and reuse changed nothing about their bytes.
    control_stores: dict[str, bytes] = _stores(control)
    sharing_stores: dict[str, bytes] = _stores(sharing)
    assert len(control_stores) == len(sharing_stores) == 2
    assert sharing_stores == control_stores
    assert _store_publications(sharing) == ["PMCID:PMC0000001", "PMCID:PMC0000002"]
    for path in sorted((sharing / ".tablassert" / "store").glob("*.parquet")):
        frame: pl.DataFrame = pl.read_parquet(path)
        assert frame["subject"].to_list() == ["HGNC:1100"]
        assert frame["object"].to_list() == ["HGNC:6871"]

    # The shared prefix ran once, not once per section.
    assert len(control_loads) == 2
    assert len(sharing_loads) == 1
    assert len(control_lookups) == 2
    assert len(sharing_lookups) == 1
    assert sharing_lookups[0][1] == control_lookups[0][1] == ["brca1", "mapk1"]

    # The cache itself reports a snapshot written and read; the control opened none at all.
    assert control_summaries == []
    assert control_dirs == []
    assert len(summaries) == 1
    assert summaries[0].stores >= 1
    assert summaries[0].hits >= 1
    assert summaries[0].bytes_written > 0
    assert directories
    assert not directories[0].exists()


def test_missing_planned_snapshot_fails_without_recomputing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """A planned consumer fails loudly when its assigned snapshot disappears.

    WHY: a cache miss for a planned consumer is a broken execution invariant, not permission to
    recompute the shared prefix. The real build must raise the coded error before invoking the
    consumer's compiler, and RunCache must still clean up the cache directory recorded for this build.
    """
    table: Path = _write_inputs(tmp_path / "inputs", [("a", "PMC0000001"), ("a", "PMC0000002")])
    run: Path = _run_dir(tmp_path, "missing-snapshot")
    monkeypatch.chdir(run)
    summaries, directories = _record_run_caches(monkeypatch)
    plans = _record_plans(monkeypatch)
    compiled: list[int] = []
    real_compile_subgraph: Callable = lib.compile_subgraph

    def recording_compile_subgraph(tcode: Ops, **kwargs: Any) -> Path:
        """Count real compiles so a missing consumer snapshot cannot fall back to recomputation."""
        compiled.append(len(compiled))
        return real_compile_subgraph(tcode, **kwargs)

    monkeypatch.setattr(lib, "compile_subgraph", recording_compile_subgraph)
    cache_type: type[RunCache] = runcache.RunCache

    def missing_load(self: RunCache, digest: str) -> pl.LazyFrame | None:
        """Simulate disappearance of the producer snapshot while preserving RunCache accounting."""
        self._misses += 1
        return None

    monkeypatch.setattr(cache_type, "load", missing_load)
    with pytest.raises(runcache.RunCacheError, match="runcache-missing-snapshot"):
        build_pipeline(_write_graph(run, table, rig_factory, "RUNCACHE_KG"), PipelineProgress(total_stages=6))

    assert len(plans) == 1
    assert plans[0].shares is True
    assert compiled == [0]  # producer ran; the consumer failed at _run_cache_resume before compile_subgraph
    assert len(summaries) == 1
    assert summaries[0].misses == 1
    assert directories
    assert not directories[0].exists()


def test_mixed_quick_exit_preserves_planned_mapping_and_result_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """An existing middle section does not shift plan roles or reorder final graph results.

    WHY: ``Tcode.collect`` returns a Path for an existing store, so planning operates on a filtered
    pending list while output assembly still uses original section indices. With planned sections
    on both sides of a quick-exit Path, both the role mapping and original publication order must
    survive the reorder.
    """
    table: Path = _write_inputs(tmp_path / "inputs", [("a", "PMC0000001"), ("b", "PMC0000002"), ("a", "PMC0000003")])
    run: Path = _run_dir(tmp_path, "mixed-quick-exit")
    monkeypatch.chdir(run)
    # First build creates all three real section stores. Remove only the planned pair's stores,
    # leaving the middle section as the actual Tcode.collect quick-exit Path on the second build.
    with monkeypatch.context() as first_patch:
        first_plan = _disable_sharing(first_patch)
        build_pipeline(_write_graph(run, table, rig_factory, "RUNCACHE_KG"), PipelineProgress(total_stages=6))
    for path in (run / ".tablassert" / "store").glob("*.parquet"):
        publication: str = pl.read_parquet(path)["publications"].to_list()[0][0]
        if publication != "PMCID:PMC0000002":
            path.unlink()
    assert len(first_plan) == 1

    plans = _record_plans(monkeypatch)
    sequences = _record_subgraphs(monkeypatch)
    summaries, directories = _record_run_caches(monkeypatch)
    build_pipeline(_write_graph(run, table, rig_factory, "RUNCACHE_KG"), PipelineProgress(total_stages=6))

    assert len(plans) == 1
    plan: RunPlan = plans[0]
    assert plan.shares is True
    assert len(plan.entries) == 2  # only the two pending sections are planner-indexed
    assert sum(entry.produce_digest is not None for entry in plan.entries) == 1
    assert sum(entry.resume_digest is not None for entry in plan.entries) == 1
    assert len(summaries) == 1
    assert summaries[0].hits >= 1
    assert directories
    assert not directories[0].exists()
    assert len(sequences) == 1
    assert [pl.read_parquet(path)["publications"].to_list()[0][0] for path in sequences[0]] == [
        "PMCID:PMC0000001",
        "PMCID:PMC0000002",
        "PMCID:PMC0000003",
    ]


def test_single_section_build_creates_no_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """A build with nothing to share never creates a run cache — not even a temp directory.

    WHY: zero-sharing is the common case (a one-section table, or sections whose sources differ),
    and a cache that materialized anyway would make every such build pay for a temp directory it
    cannot use. Both shapes are checked: one section, and two sections whose differing sources give
    them no common prefix. ``tempfile.TemporaryDirectory`` is wrapped, so a directory created and
    removed INSIDE the build still counts as created; cleanup is asserted from the recorded cache
    directories for this build, while the zero-sharing counters remain empty.
    """
    single: Path = _write_inputs(tmp_path / "single-inputs", [("a", "PMC0000001")])
    split: Path = _write_inputs(tmp_path / "split-inputs", [("a", "PMC0000001"), ("b", "PMC0000002")])
    created: list[str] = []
    real_temporary_directory: Callable = tempfile.TemporaryDirectory

    def recording_temporary_directory(*args: Any, **kwargs: Any) -> Any:
        """Record the prefix of every temp directory created, then create it for real."""
        created.append(str(kwargs.get("prefix", args[0] if args else "")))
        return real_temporary_directory(*args, **kwargs)

    monkeypatch.setattr(tempfile, "TemporaryDirectory", recording_temporary_directory)
    for label, table, count in (("single", single, 1), ("split", split, 2)):
        run: Path = _run_dir(tmp_path, label)
        monkeypatch.chdir(run)
        plans: list[RunPlan] = _record_plans(monkeypatch)
        summaries, directories = _record_run_caches(monkeypatch)
        build_pipeline(_write_graph(run, table, rig_factory, "RUNCACHE_KG"), PipelineProgress(total_stages=6))
        assert len(plans) == 1, label
        assert plans[0].shares is False, label
        assert plans[0].execution_order == list(range(count)), label
        assert summaries == [], label
        assert directories == [], label
        assert len(_stores(run)) == count, label
    assert not [prefix for prefix in created if prefix.startswith(CACHE_DIR_PREFIX)]


def test_subgraph_order_matches_section_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """Sharing reorders execution but never the graph: subgraphs stay in original section order.

    WHY: Stage 5 may run a producer before its consumers, and Stage 6 concatenates subgraphs in the
    order it is given — so a plan leaking into the artifact order would silently reorder a graph's
    rows. Three sections pin it: 0 and 2 share a source (so the plan runs them together) while 1
    reads a different one and is deferred to the end of the planned order. The recorded plan must
    differ from the original order and the list handed to ``compile_graph`` must not.
    """
    table: Path = _write_inputs(tmp_path / "inputs", [("a", "PMC0000000"), ("b", "PMC0000011"), ("a", "PMC0000022")])
    run: Path = _run_dir(tmp_path, "ordered")
    monkeypatch.chdir(run)
    plans: list[RunPlan] = _record_plans(monkeypatch)
    sequences: list[list[Path]] = _record_subgraphs(monkeypatch)
    summaries, directories = _record_run_caches(monkeypatch)

    build_pipeline(_write_graph(run, table, rig_factory, "RUNCACHE_KG"), PipelineProgress(total_stages=6))

    assert len(plans) == 1
    plan: RunPlan = plans[0]
    # Sharing is active AND the planned order is not the original one, so the assertions below are
    # about a real reorder being undone rather than about a no-op plan.
    assert plan.shares is True
    assert sorted(plan.execution_order) == [0, 1, 2]
    assert plan.execution_order != [0, 1, 2]
    # The grouped pair runs together (producer first, original index within the group), the
    # ungrouped section last.
    assert plan.execution_order[-1] == 1
    assert len(summaries) == 1
    assert summaries[0].stores >= 1
    assert directories
    assert not directories[0].exists()

    assert len(sequences) == 1
    subgraphs: list[Path] = sequences[0]
    assert len(subgraphs) == 3
    # Original section order, read back from what each subgraph actually contains.
    assert [pl.read_parquet(path)["publications"].to_list()[0][0] for path in subgraphs] == [
        "PMCID:PMC0000000",
        "PMCID:PMC0000011",
        "PMCID:PMC0000022",
    ]
    assert (run / "RUNCACHE_KG_1.0.0.edges.ndjson").is_file()


def test_cache_directory_deleted_when_a_section_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rig_factory: Any) -> None:
    """A section failing mid-build still leaves no cache directory behind, and the failure propagates.

    WHY: the cache is ephemeral by contract, and the exception path is where that contract is
    actually tested — a build that dies after a producer snapshotted a prefix is exactly the case
    that would otherwise leak a full intermediate table into the temp root forever. Cleanup is
    asserted against the cache directory recorded for this build. The failure is forced on the
    SECOND section so the first has already stored its snapshot, i.e. the deleted
    tree is non-empty; the original error must reach the caller unswallowed, since a cache that ate
    build errors would turn a loud failure into a mysteriously incomplete graph.
    """
    table: Path = _write_inputs(tmp_path / "inputs", [("a", "PMC0000001"), ("a", "PMC0000002")])
    run: Path = _run_dir(tmp_path, "failing")
    monkeypatch.chdir(run)
    summaries, directories = _record_run_caches(monkeypatch)
    real_compile_subgraph: Callable = lib.compile_subgraph
    compiled: list[int] = []

    def failing_compile_subgraph(tcode: Ops, **kwargs: Any) -> Path:
        """Compile the first section for real; blow up on the second, after its snapshot exists."""
        compiled.append(len(compiled) + 1)
        if len(compiled) == 2:
            raise _SectionFailure("section blew up mid-build")
        return real_compile_subgraph(tcode, **kwargs)

    monkeypatch.setattr(lib, "compile_subgraph", failing_compile_subgraph)
    with pytest.raises(_SectionFailure, match="section blew up mid-build"):
        build_pipeline(_write_graph(run, table, rig_factory, "RUNCACHE_KG"), PipelineProgress(total_stages=6))

    assert compiled == [1, 2]
    # A cache was opened, a snapshot was in it when the failure hit, and the tree is gone now.
    assert len(directories) == 1
    assert directories[0].name.startswith(CACHE_DIR_PREFIX)
    assert len(summaries) == 1
    assert summaries[0].stores >= 1
    assert not directories[0].exists()
