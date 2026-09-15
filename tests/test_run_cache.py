"""Tests for ``tablassert.runcache``: op digests, worth-caching guards, planner, and the run cache.

The first part pins the pure layers — content-addressed hashing (:func:`prefix_digest`) and the
cost guards (:func:`checkpoints`). The second part pins :class:`RunCache`, the one piece that
touches the filesystem: its snapshots must round-trip a frame exactly, its directory must be
deleted when the build ends (including when it ends in an exception), its counters must be
exact, and every way it could be misused must fail loudly instead of degrading into a silent
recompute or a false miss. The last part pins :func:`plan_run`, the pure planner that turns a
build's per-section op lists into a :class:`RunPlan`: which sections share a checkpointed prefix,
which one snapshots it, and what order the sections must run in for that snapshot to exist before
anything tries to load it.

The op-list shapes here mirror what ``Tcode.collect`` actually stores (see
``src/tablassert/lib.py``'s ``_source_ops``/``_node_ops`` and the ``(resolve_batch, (specs, db,
log, store.stem, config.name, column_context, tag))`` / ``(fullmap_audit, (col, store.stem,
config.name, out, log))`` tuples around lines 1301-1305): the leading LazyFrame is NOT part of
the stored args because ``compile_subgraph`` pipes it through ``reduce``.
"""

from __future__ import annotations

import operator
import os
import random
import tempfile
import time
from collections.abc import Callable
from itertools import pairwise
from pathlib import Path
from typing import Any

import polars as pl
import pytest

import tablassert.lib as lib
from tablassert import rs
from tablassert.enums import Comparisons, Tokens
from tablassert.fullmap import ResolveSpec, resolve_batch
from tablassert.models import Reindex
from tablassert.qc import fullmap_audit
from tablassert.runcache import (
    CACHE_DIR_PREFIX,
    DEFAULT_OP_COST,
    LABEL_FREE,
    LABEL_PLACEHOLDER,
    MIN_PREFIX_COST,
    MIN_PREFIX_OPS,
    OP_COST,
    PlanEntry,
    RunCache,
    RunCacheError,
    RunCacheStats,
    RunPlan,
    canonical,
    checkpoints,
    op_cost,
    op_repr,
    plan_run,
    prefix_digest,
)


def _source_ops() -> list[tuple[Callable, tuple[Any, ...]]]:
    """Build a realistic TCode-style op list from real repo callables (two separate calls give independent instances)."""
    return [
        (lib.csv, (Path("data/gwas.tsv"), "\t")),
        (lib.idx, ()),
        (lib.crop, ([0, 100],)),
        (lib.pick, ([1, 2, 3],)),
        (lib.column, ("subject", "gene")),
        (lib.reindex, ("A", operator.ne, "N/A", False)),
        (lib.edge_category, ("related_to", {"related_to": "biolink:Association"})),
    ]


def _resolve_ops(section_hash: str, config_file: str) -> list[tuple[Callable, tuple[Any, ...]]]:
    """A ``resolve_batch`` op with fresh ``ResolveSpec`` instances (label slots at positions 3 and 4)."""
    specs: list[ResolveSpec] = [ResolveSpec(col="subject"), ResolveSpec(col="object", taxon="NCBITaxon:9606")]
    return [(resolve_batch, (specs, Path("data/fullmap.redb"), True, section_hash, config_file, True, "_two"))]


def _audit_ops(section_hash: str, config_file: str) -> list[tuple[Callable, tuple[Any, ...]]]:
    """A ``fullmap_audit`` op (label slots at positions 1 and 2, per ``lib.py``'s collection)."""
    return [(fullmap_audit, ("subject", section_hash, config_file, "passed", True))]


def test_prefix_digest_stable_across_identical_op_lists() -> None:
    """Two separately-built identical op lists must digest equal — the foundation every cache story rests on.

    Builds the list twice (fresh ``Path``/``dict`` instances each time) because a real cache is
    consulted by a *later* build that re-collects ops from the same config; object identity
    must never matter. Also pins the full 16-hex XXH64 shape shared with section-store keys.
    """
    first: list[tuple[Callable, tuple[Any, ...]]] = _source_ops()
    second: list[tuple[Callable, tuple[Any, ...]]] = _source_ops()
    assert prefix_digest(first) == prefix_digest(second)
    digest: str = prefix_digest(first)
    assert len(digest) == 16
    assert all(char in "0123456789abcdef" for char in digest)
    payload: str = "\n".join(op_repr(fn, args) for fn, args in first)
    assert digest == rs.xxh64(payload)


def test_prefix_digest_differs_on_any_arg_change() -> None:
    """Any semantic argument change must change the digest — a stale-hit would serve wrong data.

    Mutates a column name, an int, a float-ish bool, a dict value, and the callable itself;
    each variant is a *different pipeline* and must never collide with the baseline digest.
    """
    baseline: str = prefix_digest(_source_ops())
    variants: list[tuple[Callable, tuple[Any, ...]]] = [
        (lib.column, ("object", "gene")),  # column name changed
        (lib.crop, ([0, 200],)),  # row-slice int changed
        (lib.pick, ([1, 2, 4],)),  # picked row changed
        (lib.edge_category, ("related_to", {"related_to": "biolink:GeneToGeneAssociation"})),  # dict value changed
        (lib.split_list, ("subject", "gene")),  # callable changed, args kept where shape allows
    ]
    for fn, args in variants:
        mutated: list[tuple[Callable, tuple[Any, ...]]] = _source_ops()
        mutated[-1] = (fn, args)
        assert prefix_digest(mutated) != baseline, f"{fn.__name__}{args} collided with the baseline digest"


def test_prefix_digest_excludes_label_args_for_resolve_batch_and_fullmap_audit() -> None:
    """Label-only slots (section_hash/config_file) must not split caches for the same transformation.

    The labels identify the *section being built* in log lines, not the pipeline: two configs
    (or the same config rebuilt after a rename) producing byte-identical resolution work must
    share one cache entry. Conversely every non-label slot — resolution inputs, db path,
    audit column, output name — must still change the digest, or the cache would serve the
    wrong results.
    """
    assert prefix_digest(_resolve_ops("aaaabbbb", "study-a.toml")) == prefix_digest(_resolve_ops("ccccdddd", "study-b.toml"))
    assert prefix_digest(_audit_ops("aaaabbbb", "study-a.toml")) == prefix_digest(_audit_ops("ccccdddd", "study-b.toml"))

    label_free_digest: str = prefix_digest(_resolve_ops("aaaabbbb", "study-a.toml"))
    # Changing the resolution inputs (a non-label slot) must change the digest.
    changed_specs: list[tuple[Callable, tuple[Any, ...]]] = [
        (
            resolve_batch,
            ([ResolveSpec(col="subject", taxon="NCBITaxon:9606")], Path("data/fullmap.redb"), True, "aaaabbbb", "study-a.toml", True, "_two"),
        )
    ]
    assert prefix_digest(changed_specs) != label_free_digest
    # Each remaining non-label slot must also matter.
    changed_db: list[tuple[Callable, tuple[Any, ...]]] = [
        (resolve_batch, ([ResolveSpec(col="subject")], Path("data/other.redb"), True, "aaaabbbb", "study-a.toml", True, "_two"))
    ]
    changed_log: list[tuple[Callable, tuple[Any, ...]]] = [
        (resolve_batch, ([ResolveSpec(col="subject")], Path("data/fullmap.redb"), False, "aaaabbbb", "study-a.toml", True, "_two"))
    ]
    changed_context: list[tuple[Callable, tuple[Any, ...]]] = [
        (resolve_batch, ([ResolveSpec(col="subject")], Path("data/fullmap.redb"), True, "aaaabbbb", "study-a.toml", False, "_two"))
    ]
    changed_tag: list[tuple[Callable, tuple[Any, ...]]] = [
        (resolve_batch, ([ResolveSpec(col="subject")], Path("data/fullmap.redb"), True, "aaaabbbb", "study-a.toml", True, "_one"))
    ]
    for mutated in (changed_db, changed_log, changed_context, changed_tag):
        assert prefix_digest(mutated) != label_free_digest, "a non-label resolve_batch slot failed to change the digest"

    audit_digest: str = prefix_digest(_audit_ops("aaaabbbb", "study-a.toml"))
    for mutated_audit in (
        [(fullmap_audit, ("object", "aaaabbbb", "study-a.toml", "passed", True))],
        [(fullmap_audit, ("subject", "aaaabbbb", "study-a.toml", "failed", True))],
        [(fullmap_audit, ("subject", "aaaabbbb", "study-a.toml", "passed", False))],
    ):
        assert prefix_digest(mutated_audit) != audit_digest, "a non-label fullmap_audit slot failed to change the digest"

    # The masked slots must literally render as the placeholder, proving masking happened.
    masked: str = op_repr(resolve_batch, _resolve_ops("aaaabbbb", "study-a.toml")[0][1])
    assert f"'{LABEL_PLACEHOLDER}', '{LABEL_PLACEHOLDER}'" in masked
    assert LABEL_FREE[resolve_batch] == (3, 4)
    assert LABEL_FREE[fullmap_audit] == (1, 2)


def test_canonical_raises_loudly_on_unknown_type() -> None:
    """Unsupported types must raise the coded error — never silently mis-hash.

    A silent fallback (e.g. hashing ``str(value)`` or ``id(value)``) would make the digest
    depend on repr noise or object identity, poisoning every downstream cache key. The error
    must carry the house ``code`` slug and surface through ``prefix_digest`` too.
    """
    for bad in ({1, 2, 3}, b"bytes", object()):
        with pytest.raises(RunCacheError, match="runcache-unserializable-arg"):
            canonical(bad)
        with pytest.raises(RunCacheError, match="runcache-unserializable-arg"):
            prefix_digest([(lib.column, ("subject", bad))])
    # Nested unknown types must raise too — nothing may be swallowed by the recursion.
    with pytest.raises(RunCacheError, match="runcache-unserializable-arg"):
        canonical([1, {"ok": 2}, {1, 2}])
    import functools

    # Callables without a stable qualified name (functools.partial) must fail loudly rather
    # than collide under a shared "functools.partial" name.
    with pytest.raises(RunCacheError, match="runcache-unserializable-arg"):
        canonical(functools.partial(op_repr, resolve_batch))


def test_canonical_handles_all_supported_types() -> None:
    """Every supported type serializes deterministically, and confusable types stay distinct.

    Pins the serialization contract future stories rely on: ``bool`` before ``int`` (``True``
    must not degrade to ``1``), ``Enum`` before ``str`` (repo enums are ``str`` subclasses),
    dict key ordering irrelevant, and ``list`` vs ``tuple`` vs ``str`` vs ``int`` never colliding.
    """
    assert canonical(None) == "None"
    assert canonical(True) == "True"
    assert canonical(False) == "False"
    assert canonical(3) == "3"
    assert canonical(-7) == "-7"
    assert canonical(3.5) == repr(3.5)
    assert canonical("x") == "'x'"
    assert canonical(Path("data/gwas.tsv")) == "Path('data/gwas.tsv')"
    assert canonical([1, "a"]) == "[1, 'a']"
    assert canonical((1, "a")) == "(1, 'a')"
    assert canonical([1, "a"]) != canonical((1, "a"))
    assert canonical("1") != canonical(1)
    assert canonical(Path("x")) != canonical("x")
    # str-Enum serializes by value, and dict keys sort (insertion order must not matter).
    assert canonical(Tokens.AUTO) == canonical("auto")
    assert canonical(Comparisons.NE) == canonical("ne")
    assert canonical({"b": 1, "a": 2}) == canonical({"a": 2, "b": 1}) == "{'a': 2, 'b': 1}"
    # A real pydantic model from tablassert.models serializes via model_dump_json().
    model: Reindex = Reindex(column="A", comparison=Comparisons.NE, comparator="N/A")
    assert canonical(model) == model.model_dump_json()
    # Callables serialize by qualified identity.
    assert canonical(lib.csv) == "tablassert.lib.csv"
    assert canonical(resolve_batch) == "tablassert.fullmap.resolve_batch"
    assert canonical(fullmap_audit) == "tablassert.qc.fullmap_audit"


def test_prefix_digest_is_deterministic_within_and_across_instances() -> None:
    """Repeated hashing of equal-but-independent op instances must agree every time.

    Within one process: the same op list hashed twice gives the same digest, and a freshly
    rebuilt list (new ``Path``/``ResolveSpec``/``dict`` objects) agrees too — proving no
    dependence on object identity or ``hash()`` randomization. Across "instances" (builds):
    two independent collections of the same pipeline produce the same key, which is what lets
    a later build hit an earlier build's cache entry.
    """
    ops: list[tuple[Callable, tuple[Any, ...]]] = [*_source_ops(), *_resolve_ops("aaaabbbb", "study.toml"), *_audit_ops("aaaabbbb", "study.toml")]
    for _ in range(3):
        assert prefix_digest(ops) == prefix_digest(ops)
    rebuilt: list[tuple[Callable, tuple[Any, ...]]] = [*_source_ops(), *_resolve_ops("aaaabbbb", "study.toml"), *_audit_ops("aaaabbbb", "study.toml")]
    assert prefix_digest(ops) == prefix_digest(rebuilt)
    # A semantically different pipeline must differ from both.
    assert prefix_digest([*reversed(rebuilt)]) != prefix_digest(rebuilt)


def _cost_ops(*fns: Callable) -> list[tuple[Callable, tuple[Any, ...]]]:
    """Build a checkpoints-test op list from callables.

    ``checkpoints`` prices an op by its callable identity alone (the cumulative :data:`OP_COST`
    of ``ops[:k]``); the stored args never affect cost, so they are left empty here. This keeps
    the guard tests focused on cost accounting rather than arg serialization (covered above).
    """
    return [(fn, ()) for fn in fns]


def test_checkpoints_skip_trivial_prefixes() -> None:
    """Trivial instruction runs must never be checkpointed — snapshotting them costs more than it saves.

    The whole point of the worth-caching guard: a prefix that is too short (``MIN_PREFIX_OPS``) or
    too cheap (cumulative cost below ``MIN_PREFIX_COST``) recomputes for less than the snapshot
    write-plus-rescan a checkpoint forces on every downstream section. Asserts the empty result
    for (a) a cheap-ops list whose total stays under the cost threshold, (b) a single expensive op
    blocked by ``MIN_PREFIX_OPS`` even though its cost alone clears ``MIN_PREFIX_COST``, (c) a
    two-op list at the length floor but below the cost floor, and (d) a long run of 1-unit ops
    that satisfies length yet never accumulates enough cost — proving length alone cannot qualify.
    """
    # (a) csv(2) + pick(1) + value(1) = 4 < MIN_PREFIX_COST(8): no checkpoint at any prefix.
    cheap: list[tuple[Callable, tuple[Any, ...]]] = _cost_ops(lib.csv, lib.pick, lib.value)
    assert sum(op_cost(fn) for fn, _ in cheap) < MIN_PREFIX_COST
    assert checkpoints(cheap) == []

    # (b) A single resolve_batch (cost 60) clears the cost floor but not MIN_PREFIX_OPS(2).
    assert op_cost(lib.resolve_batch) >= MIN_PREFIX_COST
    assert checkpoints(_cost_ops(lib.resolve_batch)) == []

    # (c) Two ops at the length floor but below the cost floor: csv(2) + pick(1) = 3 < 8.
    two_cheap: list[tuple[Callable, tuple[Any, ...]]] = _cost_ops(lib.csv, lib.pick)
    assert len(two_cheap) == MIN_PREFIX_OPS
    assert checkpoints(two_cheap) == []

    # (d) Seven 1-unit ops satisfy MIN_PREFIX_OPS at every k>=2 but total 7 < 8: still nothing.
    long_cheap: list[tuple[Callable, tuple[Any, ...]]] = _cost_ops(*([lib.value] * 7))
    assert sum(op_cost(fn) for fn, _ in long_cheap) < MIN_PREFIX_COST
    assert checkpoints(long_cheap) == []


def test_checkpoints_include_expensive_prefixes() -> None:
    """Once cumulative cost crosses the threshold, every longer prefix up to the write is a checkpoint.

    A realistic pipeline that reaches ``resolve_batch`` (cost 60) must be checkpointed from the
    crossing point onward: recomputing that resolution for each sibling section is exactly the
    cost the cache exists to avoid. Asserts the EXACT boundary set (not mere membership) for a
    short list and a full pipeline, and pins that the returned prefix lengths are strictly
    increasing ints within ``[1, len(ops)]`` so a later story can slice ``ops[:k]`` safely.
    """
    # csv(2), pick(1), resolve_batch(60): k=1 <MIN_PREFIX_OPS, k=2 total 3 <8, k=3 total 63 -> [3].
    short: list[tuple[Callable, tuple[Any, ...]]] = _cost_ops(lib.csv, lib.pick, lib.resolve_batch)
    assert checkpoints(short) == [3]

    # Full pipeline ending in a write: csv(2), idx(1), pick(1), resolve_batch(60), fullmap_audit(30),
    # prune_to_class(2), to_store(4). Cumulative crosses 8 at k=4 (64); k=5,6 also qualify; k=7 is
    # the final to_store and is excluded -> [4, 5, 6].
    full: list[tuple[Callable, tuple[Any, ...]]] = _cost_ops(
        lib.csv, lib.idx, lib.pick, lib.resolve_batch, lib.fullmap_audit, lib.prune_to_class, lib.to_store
    )
    result: list[int] = checkpoints(full)
    assert result == [4, 5, 6]

    # Prefix lengths are ints, strictly increasing, and within range.
    for k in result:
        assert isinstance(k, int)
        assert 1 <= k <= len(full)
    assert result == sorted(result)
    assert all(b > a for a, b in pairwise(result))


def test_checkpoint_never_placed_after_final_write_op() -> None:
    """A section always replays its own final write, so ``len(ops)`` is never a to_store checkpoint.

    The parquet a section writes IS its result; snapshotting the prefix that ends on that write
    would cache an output every consumer must produce anyway. Asserts that a write-final list
    whose cumulative cost fully qualifies still stops at ``len(ops) - 1`` (``len(ops)`` absent),
    and — as the contrast that proves the rule is write-specific, not "always drop the last" —
    that the SAME expensive list without the trailing ``to_store`` does include ``len(ops)``.
    """
    with_write: list[tuple[Callable, tuple[Any, ...]]] = _cost_ops(lib.csv, lib.pick, lib.resolve_batch, lib.to_store)
    # Cumulative cost at k=len is 2+1+60+4 = 67 >= 8, so the ONLY reason k=4 is absent is the write rule.
    assert sum(op_cost(fn) for fn, _ in with_write) >= MIN_PREFIX_COST
    result: list[int] = checkpoints(with_write)
    assert len(with_write) not in result
    assert max(result) <= len(with_write) - 1

    # Contrast: drop the trailing to_store and the identical expensive prefix now checkpoints at len.
    without_write: list[tuple[Callable, tuple[Any, ...]]] = _cost_ops(lib.csv, lib.pick, lib.resolve_batch)
    assert len(without_write) in checkpoints(without_write)


def test_op_cost_covers_every_phase_of_callable() -> None:
    """Every op TCode can emit is costed deliberately; unpriced ops fall back to the cheap default.

    Exhaustiveness pin: ``lib.PHASE_OF`` enumerates the op callables the pipeline dispatches, so
    each must appear in :data:`OP_COST` with a positive int — a future op added to ``PHASE_OF``
    without a cost would otherwise silently default and skew every checkpoint decision. Also
    verifies the :data:`DEFAULT_OP_COST` fallback for a callable absent from the table, and pins
    the dominant weights (``resolve_batch``/``resolve`` 60, ``fullmap_audit`` 30, ``to_store`` 4)
    so an accidental edit cannot quietly change cache behavior.
    """
    for fn in lib.PHASE_OF:
        assert fn in OP_COST, f"{getattr(fn, '__qualname__', fn)} is in lib.PHASE_OF but missing from OP_COST"
        cost: int = OP_COST[fn]
        assert isinstance(cost, int)
        assert cost > 0
        assert op_cost(fn) == cost

    # A callable absent from the table falls back to DEFAULT_OP_COST (never a KeyError, never 0).
    def _uncosted_op() -> None:
        """Dummy op deliberately absent from OP_COST to exercise the cost fallback."""

    assert _uncosted_op not in OP_COST
    assert op_cost(_uncosted_op) == DEFAULT_OP_COST
    assert DEFAULT_OP_COST > 0

    # Pin the dominant/structural weights the guard thresholds are calibrated against.
    assert op_cost(lib.resolve_batch) == 60
    assert op_cost(lib.resolve) == 60
    assert op_cost(lib.fullmap_audit) == 30
    assert op_cost(lib.to_store) == 4
    assert op_cost(lib.csv) == 2
    assert op_cost(lib.value) == 1


# --- Ephemeral RunCache ----------------------------------------------------------------------


class _BuildFailure(RuntimeError):
    """Stand-in for any error a section raises mid-build, carrying the directory it failed in.

    The directory rides along on the exception so the test can assert the tree was deleted AFTER
    the context exited (``cache.directory`` itself raises once the cache is closed), while keeping
    the ``pytest.raises`` body a single simple statement.
    """

    def __init__(self: _BuildFailure, directory: Path) -> None:
        super().__init__("section blew up")
        self.directory: Path = directory


def _mixed_frame() -> pl.LazyFrame:
    """A small frame spanning the dtype families a snapshot must survive: str, int, float, bool, null.

    ``Null`` is the interesting case — a snapshot layer that widened it (or dropped the column)
    would silently change a downstream section's schema, which is exactly what the schema-equality
    assertions below exist to catch. Two rows make row order meaningful as well.
    """
    return pl.DataFrame(
        {"subject": ["HGNC:11998", "MONDO:0005812"], "cases": [42, 7], "p_value": [1.5e-8, 0.25], "significant": [True, False], "note": [None, None]}
    ).lazy()


def _fail_after_storing(cache: RunCache, digest: str) -> None:
    """Store one snapshot, then raise the way a failing section would.

    Storing first is what makes the exception test meaningful: ``__exit__`` then has a NON-empty
    tree to delete, so an implementation that skipped cleanup on the error path cannot pass.
    """
    cache.store(digest, _mixed_frame())
    raise _BuildFailure(cache.directory)


def test_run_cache_directory_deleted_after_exit() -> None:
    """A normal exit must leave nothing behind — the run cache is ephemeral by contract.

    Captures the directory inside the context and stores a real snapshot into it, so deletion has
    actual work to do; a surviving directory would mean snapshots outlive the build, which is the
    exact staleness (a later build trusting an environment it never hashed) and disk growth the
    design forbids.
    """
    with RunCache() as cache:
        directory: Path = cache.directory
        assert directory.is_dir()
        stored: Path = cache.store(prefix_digest(_source_ops()), _mixed_frame())
        assert stored.is_file()
        assert list(directory.iterdir()) == [stored]
    assert not directory.exists()


def test_run_cache_directory_deleted_on_exception() -> None:
    """A failing build must still clean up, and the failure must reach the caller unswallowed.

    Pins BOTH halves of the exception-path contract in one go: ``pytest.raises`` proves the
    original error propagates (a cache that ate build errors would turn a loud failure into a
    mysteriously empty graph), and the post-block assertion proves the non-empty snapshot tree was
    deleted anyway, so a crashed build leaves no artifacts.
    """
    with pytest.raises(_BuildFailure, match="section blew up") as failure, RunCache() as cache:
        _fail_after_storing(cache, prefix_digest(_source_ops()))
    assert not failure.value.directory.exists()


def test_run_cache_temp_dir_uses_tablassert_prefix() -> None:
    """The run directory must be recognizable as Tablassert's, inside the system temp root.

    ``tempfile`` picks the location (respecting ``TMPDIR``), but the prefix is ours: after a hard
    crash (``SIGKILL``, power loss) ``TemporaryDirectory`` never runs, so the leftover's name is
    the ONLY thing telling an operator it is a Tablassert run cache that is safe to delete — and
    telling a tmp-reaping script not to mistake it for user data.
    """
    with RunCache() as cache:
        directory: Path = cache.directory
        assert directory.name.startswith(CACHE_DIR_PREFIX)
        assert directory.name != CACHE_DIR_PREFIX  # tempfile appends its random suffix
        assert directory.parent.resolve() == Path(tempfile.gettempdir()).resolve()
        assert directory.is_dir()


def test_run_cache_store_then_load_round_trips_schema_and_rows() -> None:
    """A snapshot must come back exactly: same column names, same dtypes, same rows, same order.

    Pinning dtype equality (not just names) is what keeps the cache from becoming a silent
    type-coercion trap — parquet can widen a column, and a downstream op that expects ``String``
    but receives ``Int64`` fails confusingly far from the cache that changed it. The collected
    frame is compared INSIDE the context because ``load`` returns a lazy scan of a file that
    ``__exit__`` deletes.
    """
    lf: pl.LazyFrame = _mixed_frame()
    expected: pl.DataFrame = lf.collect()
    digest: str = prefix_digest(_source_ops())
    with RunCache() as cache:
        stored: Path = cache.store(digest, lf)
        assert stored == cache.directory / f"{digest}.parquet"
        loaded: pl.LazyFrame | None = cache.load(digest)
        assert loaded is not None
        actual: pl.DataFrame = loaded.collect()
    assert actual.schema == expected.schema
    assert actual.columns == expected.columns
    assert actual.dtypes == expected.dtypes
    assert actual.schema["note"] == pl.Null
    assert actual.equals(expected)


def test_run_cache_missing_digest_is_a_miss() -> None:
    """An unstored digest is a MISS (``None``), never an exception.

    A miss is the normal branch of the cache protocol — it is what tells the caller to compute the
    prefix and then store it — so raising here would force every lookup into a ``try``/``except``
    and make the first section of every build look like a failure. Also pins that a miss counts as
    a miss and not as a hit, since the exit summary is the only signal an operator gets.
    """
    with RunCache() as cache:
        assert cache.load(prefix_digest(_source_ops())) is None
        assert cache.stats().hits == 0
    # stats() is a detached snapshot, so it stays readable after the directory is gone.
    summary: RunCacheStats = cache.stats()
    assert (summary.stores, summary.hits, summary.misses) == (0, 0, 1)


def test_run_cache_stats_counts_stores_and_hits() -> None:
    """Counters must be exact: they are the only evidence that a run cache paid for itself.

    Two distinct digests stored, one loaded twice (two hits from ONE snapshot) and one unknown
    digest loaded once (a miss). Exact equality rather than ``>=`` catches double counting (a hit
    counted on both the lookup and the later collect) and a store counted even though its write
    failed. ``bytes_written`` is pinned against the real parquet sizes on disk because it is what
    the exit log line reports as the run's disk cost.
    """
    digest_a: str = prefix_digest(_source_ops())
    digest_b: str = prefix_digest(_resolve_ops("aaaabbbb", "study.toml"))
    unstored: str = prefix_digest(_audit_ops("aaaabbbb", "study.toml"))
    assert len({digest_a, digest_b, unstored}) == 3
    with RunCache() as cache:
        path_a: Path = cache.store(digest_a, _mixed_frame())
        path_b: Path = cache.store(digest_b, _mixed_frame())
        on_disk: int = path_a.stat().st_size + path_b.stat().st_size
        assert cache.load(digest_a) is not None
        assert cache.load(digest_a) is not None
        assert cache.load(unstored) is None
        summary: RunCacheStats = cache.stats()
    assert (summary.stores, summary.hits, summary.misses) == (2, 2, 1)
    assert summary.bytes_written == on_disk > 0


def test_run_cache_duplicate_store_raises_and_keeps_the_first_snapshot() -> None:
    """A second frame under one digest raises the coded error; it does NOT overwrite.

    Chosen behavior for criterion "store of a present digest": a digest is a CONTENT address, so a
    second store under the same key means a hash collision or a caller bug — and either way every
    hit already served from that key is suspect. Overwriting would hide that and silently change
    what an earlier ``load`` returned, so the loud coded error is the repo's fail-loudly
    convention. The test also pins that the rejected store left the first snapshot's rows and the
    counters untouched.
    """
    digest: str = prefix_digest(_source_ops())
    first: pl.DataFrame = _mixed_frame().collect()
    with RunCache() as cache:
        cache.store(digest, _mixed_frame())
        before: RunCacheStats = cache.stats()
        with pytest.raises(RunCacheError, match="runcache-duplicate-store"):
            cache.store(digest, pl.DataFrame({"subject": ["a different frame"]}).lazy())
        after: RunCacheStats = cache.stats()
        reloaded: pl.LazyFrame | None = cache.load(digest)
        assert reloaded is not None
        assert reloaded.collect().equals(first)
    assert after.stores == before.stores == 1
    assert after.bytes_written == before.bytes_written


def test_run_cache_store_and_load_outside_the_context_raise() -> None:
    """``store``/``load`` only exist inside the ``with`` block — a closed cache never fakes a miss.

    After ``__exit__`` the directory is gone, so ``load`` returning ``None`` would report a MISS for
    a snapshot that did exist: the caller would silently recompute an expensive prefix, or treat a
    cached result as absent. Before ``__enter__`` there is nowhere to write. Both directions must
    raise the coded error, which is why the same three probes run before and after the context.
    """
    cache: RunCache = RunCache()
    digest: str = prefix_digest(_source_ops())

    def _store() -> None:
        """Store against a cache with no directory (not yet, or not any more)."""
        cache.store(digest, _mixed_frame())

    def _load() -> None:
        """Load against a cache with no directory (not yet, or not any more)."""
        cache.load(digest)

    def _directory() -> None:
        """Read the directory of a cache that has none (not yet, or not any more)."""
        _ = cache.directory  # the read itself must raise; a deleted path is nothing to inspect

    probes: tuple[Callable[[], None], ...] = (_store, _load, _directory)
    for probe in probes:
        with pytest.raises(RunCacheError, match="runcache-closed"):
            probe()

    with cache:
        cache.store(digest, _mixed_frame())

    for probe in probes:
        with pytest.raises(RunCacheError, match="runcache-closed"):
            probe()


def test_run_cache_rejects_a_digest_that_is_not_one_safe_filename() -> None:
    """A digest is used as a filename, so anything path-shaped must be refused.

    ``store`` writes ``<run dir>/<digest>.parquet``: a digest carrying a separator or ``..`` would
    land OUTSIDE the temp directory, where ``__exit__`` never deletes it — quietly breaking the
    "no artifacts survive a run" contract. ``prefix_digest`` only emits hex, so this guards against
    a caller handing the cache a non-digest key. Rejected digests must not be counted either.
    """
    unsafe: tuple[str, ...] = ("", ".", "..", "ab/cd", f"../{prefix_digest(_source_ops())}", "a\\b")
    with RunCache() as cache:
        for digest in unsafe:
            with pytest.raises(RunCacheError, match="runcache-bad-digest"):
                cache.store(digest, _mixed_frame())
            with pytest.raises(RunCacheError, match="runcache-bad-digest"):
                cache.load(digest)
        assert cache.stats() == RunCacheStats()
        assert list(cache.directory.iterdir()) == []


# --- Shared-prefix planner -------------------------------------------------------------------


def _load_ops(source: str) -> list[tuple[Callable, tuple[Any, ...]]]:
    """The ``csv`` + ``pick`` prologue (cost 2 + 1), which alone never reaches ``MIN_PREFIX_COST``.

    ``source`` is the only knob: two sections reading the SAME table share these ops verbatim,
    which is the realistic origin of a shared prefix in one graph's config.
    """
    return [(lib.csv, (Path(f"data/{source}.tsv"), "\t")), (lib.pick, ([1, 2, 3],))]


def _encode_ops(col: str) -> list[tuple[Callable, tuple[Any, ...]]]:
    """The ``coerce_columns`` + ``column`` encode step (cost 3 + 1); cumulative 7, still sub-floor."""
    return [(lib.coerce_columns, ()), (lib.column, (f"{col}_pre_resolution", col))]


def _shared_resolve_ops(col: str = "subject") -> list[tuple[Callable, tuple[Any, ...]]]:
    """One ``resolve_batch`` op (cost 60) — the step that carries a prefix over ``MIN_PREFIX_COST``.

    The label slots (positions 3 and 4) are held FIXED here so a planner test never has to reason
    about masking and content at once; ``test_prefix_digest_excludes_label_args_for_resolve_batch_and_fullmap_audit``
    already pins that differing labels digest equal.
    """
    return [(resolve_batch, ([ResolveSpec(col=col)], Path("data/fullmap.redb"), True, "aaaabbbb", "study-a.toml", True, "_two"))]


def _write_ops(name: str) -> list[tuple[Callable, tuple[Any, ...]]]:
    """A section-specific final ``to_store`` — the tail is where one config's sections differ.

    Ending on a write matters to the fixtures: ``checkpoints`` never selects ``len(ops)`` for a
    write-final list, so each section's deepest checkpoint is its shared head, not its own output.
    """
    return [(lib.to_store, (Path(f".tablassert/store/{name}.parquet"), name))]


def _deep_head() -> list[tuple[Callable, tuple[Any, ...]]]:
    """The six-op expensive head that sibling sections repeat verbatim (cumulative cost 97).

    csv(2) + pick(1) + coerce_columns(3) + column(1) + resolve_batch(60) + fullmap_audit(30).
    Cumulative cost crosses ``MIN_PREFIX_COST`` only at the ``resolve_batch`` (k=5), so a section
    built from this head plus a trailing write checkpoints at ``[5, 6]`` — a SHALLOW and a DEEP
    candidate, which is what lets the deepest-wins rule be asserted rather than assumed.
    """
    return [*_load_ops("gwas"), *_encode_ops("subject"), *_shared_resolve_ops(), *_audit_ops("aaaabbbb", "study-a.toml")]


def _shallow_head(source: str, col: str) -> list[tuple[Callable, tuple[Any, ...]]]:
    """A three-op head (csv + pick + resolve_batch, cost 63) that clears the cost floor at k=3.

    Distinct ``(source, col)`` pairs give distinct digests, so two of these make an independent
    group of their own rather than joining the deep one.
    """
    return [*_load_ops(source), *_shared_resolve_ops(col)]


def _two_group_sections() -> list[list[tuple[Callable, tuple[Any, ...]]]]:
    """Six sections: a deep three-member group, a shallow two-member group, and one loner.

    Indices are deliberately interleaved (deep at 0/2/5, shallow at 3/4, ungrouped at 1) so an
    implementation that planned by adjacency, or that left sections in config order, cannot pass
    the execution-order assertions below.
    """
    return [
        [*_deep_head(), *_write_ops("deep-0")],
        [*_shallow_head("loner", "object"), *_write_ops("loner-1")],
        [*_deep_head(), *_write_ops("deep-2")],
        [*_shallow_head("cohort", "subject"), *_write_ops("shallow-3")],
        [*_shallow_head("cohort", "subject"), *_write_ops("shallow-4")],
        [*_deep_head(), *_write_ops("deep-5")],
    ]


def _forbid(name: str) -> Callable[..., None]:
    """Build a stand-in that fails the test if the planner ever consults a clock or entropy source."""

    def _boom(*args: object, **kwargs: object) -> None:
        raise AssertionError(f"plan_run consulted {name}; the planner must be pure (no I/O, no clock, no randomness)")

    return _boom


def test_plan_groups_sections_sharing_deepest_prefix() -> None:
    """Sections sharing several checkpointed prefixes group on the DEEPEST one; a cheap sharer stays out.

    Two fixtures in one, because the rule has two halves. Sections 0 and 1 repeat the same
    six-op head, so both the k=5 and the k=6 prefix digests occur for two sections: the plan must
    pick k=6, since the longest shared run is the most recomputation avoided per snapshot and the
    shorter prefix is already inside it. Section 2 shares only the ``csv`` + ``pick`` prologue
    with them — genuinely shared instructions, but cost 3 sits below ``MIN_PREFIX_COST``, so it
    produces no checkpoint at all and must stay ungrouped rather than drag the group down to a
    prefix too cheap to be worth snapshotting.
    """
    deep: list[tuple[Callable, tuple[Any, ...]]] = _deep_head()
    deep_digest: str = prefix_digest(deep)
    shallow_digest: str = prefix_digest(deep[:5])
    assert deep_digest != shallow_digest
    cheap: list[tuple[Callable, tuple[Any, ...]]] = [*_load_ops("gwas"), (lib.value, ("predicate", "increases")), *_write_ops("cheap-2")]
    op_lists: list[list[tuple[Callable, tuple[Any, ...]]]] = [[*deep, *_write_ops("deep-0")], [*_deep_head(), *_write_ops("deep-1")], cheap]
    # The deep sections have BOTH prefixes to choose from; the cheap one has none to offer.
    assert checkpoints(op_lists[0]) == [5, 6] == checkpoints(op_lists[1])
    assert checkpoints(cheap) == []
    # Section 2 really does open with the same instructions as the group — the guard, not a
    # digest mismatch, is what keeps it out.
    assert prefix_digest(cheap[:2]) == prefix_digest(deep[:2])

    plan: RunPlan = plan_run(op_lists)
    assert plan.shares is True
    assert plan.execution_order == [0, 1, 2]
    assert plan.entries[0] == PlanEntry(resume_digest=None, prefix_len=len(deep), produce_digest=deep_digest)
    assert plan.entries[1] == PlanEntry(resume_digest=deep_digest, prefix_len=len(deep), produce_digest=None)
    assert plan.entries[2] == PlanEntry()
    # Deepest wins: no entry keys on the shallower shared prefix.
    assert all(entry.resume_digest != shallow_digest and entry.produce_digest != shallow_digest for entry in plan.entries)


def test_plan_execution_order_runs_producer_first() -> None:
    """Each group has exactly ONE producer, and it runs before every consumer of its digest.

    The reorder exists only to make this true: a consumer that ran first would miss, recompute the
    expensive prefix, and leave the snapshot it should have read unwritten — the cache would cost
    a write and save nothing. Two groups (deep 0/2/5, shallow 3/4) plus an ungrouped loner at 1
    pin the per-group invariants AND the block structure: grouped sections first, deeper group
    before shallower, original index within a group, ungrouped last in original relative order.
    """
    op_lists: list[list[tuple[Callable, tuple[Any, ...]]]] = _two_group_sections()
    deep_digest: str = prefix_digest(_deep_head())
    shallow_digest: str = prefix_digest(_shallow_head("cohort", "subject"))
    assert deep_digest != shallow_digest

    plan: RunPlan = plan_run(op_lists)
    assert plan.shares is True
    assert plan.execution_order == [0, 2, 5, 3, 4, 1]

    positions: dict[int, int] = {section: position for position, section in enumerate(plan.execution_order)}
    for digest, members, depth in ((deep_digest, (0, 2, 5), len(_deep_head())), (shallow_digest, (3, 4), len(_shallow_head("cohort", "subject")))):
        producers: list[int] = [index for index in members if plan.entries[index].produce_digest == digest]
        consumers: list[int] = [index for index in members if plan.entries[index].resume_digest == digest]
        # Exactly one writer per digest (RunCache.store rejects a second) and one reader per rest.
        assert len(producers) == 1
        producer: int = producers[0]
        assert sorted(consumers) == sorted(m for m in members if m != producer)
        assert len(consumers) == len(members) - 1
        assert positions[producer] < min(positions[consumer] for consumer in consumers)
        for index in members:
            entry: PlanEntry = plan.entries[index]
            assert entry.prefix_len == depth
            # A section is either the writer or a reader of its group's digest, never both.
            assert (entry.produce_digest is None) != (entry.resume_digest is None)
        assert plan.entries[producer].resume_digest is None

    # The loner shares nothing: no snapshot to write, nothing to resume, full op list replayed.
    assert plan.entries[1] == PlanEntry()
    assert len({entry.produce_digest for entry in plan.entries if entry.produce_digest is not None}) == 2


def test_plan_preserves_original_order_without_sharing() -> None:
    """A zero-sharing build plans EMPTY: identity order, all-``None`` entries, ``shares`` False.

    The downstream contract this pins: an empty plan must be recognizable so the executor skips
    both the reorder and creating a ``RunCache`` entirely. Reordering a build that cannot hit a
    snapshot would change section timing and progress reporting for nothing, and an open cache
    directory for a build with nothing to store is pure overhead. Each section here DOES have
    checkpoints — the emptiness comes from nothing being shared, not from trivial op lists — and
    the no-sections degenerate case plans empty too.
    """
    op_lists: list[list[tuple[Callable, tuple[Any, ...]]]] = [
        [*_shallow_head(f"table-{index}", col), *_write_ops(f"section-{index}")] for index, col in enumerate(("subject", "object", "gene", "disease"))
    ]
    # Every section is expensive enough to checkpoint on its own; no digest repeats across them.
    assert all(checkpoints(ops) for ops in op_lists)
    digests: list[str] = [prefix_digest(ops) for ops in op_lists]
    assert len(set(digests)) == len(digests)

    plan: RunPlan = plan_run(op_lists)
    assert plan.shares is False
    assert plan.execution_order == list(range(len(op_lists))) == [0, 1, 2, 3]
    assert len(plan.entries) == len(op_lists)
    assert all(entry == PlanEntry() for entry in plan.entries)
    assert all(entry.resume_digest is None and entry.produce_digest is None and entry.prefix_len is None for entry in plan.entries)

    empty: RunPlan = plan_run([])
    assert empty == RunPlan()
    assert empty.shares is False
    assert empty.execution_order == [] == list(range(0))
    assert empty.entries == []


def test_plan_is_deterministic_for_identical_input() -> None:
    """The same build always plans identically — no clock, no entropy, no identity dependence.

    A plan decides the order sections run in and which digest each one stores or loads, so any
    nondeterminism would show up as a build that is fast today and silently slow tomorrow, or as
    a consumer looking up a digest its producer never wrote. Three probes: replanning the SAME
    lists, replanning freshly rebuilt equal-but-independent instances (new ``Path``/``ResolveSpec``
    objects, so nothing may key on ``id()`` or ``hash()``), and replanning with two same-group
    sections swapped — equivalent input whose plan must be byte-identical, because roles are
    positional (the lowest index in a group always produces). A fourth probe replaces
    ``time``/``random``/``os.urandom`` with failures to prove purity by construction.
    """
    op_lists: list[list[tuple[Callable, tuple[Any, ...]]]] = _two_group_sections()
    deep_digest: str = prefix_digest(_deep_head())
    first: RunPlan = plan_run(op_lists)
    assert plan_run(op_lists) == first
    assert first.execution_order == [0, 2, 5, 3, 4, 1]
    assert first.entries[0].produce_digest == deep_digest

    rebuilt: list[list[tuple[Callable, tuple[Any, ...]]]] = _two_group_sections()
    assert all(rebuilt[index] == op_lists[index] for index in range(len(op_lists)))
    assert plan_run(rebuilt) == first

    # Swap two members of the deep group: the input list is reordered but equivalent, and the
    # resulting plan is IDENTICAL (not merely isomorphic) because entries are keyed by position.
    swapped: list[list[tuple[Callable, tuple[Any, ...]]]] = list(op_lists)
    swapped[0], swapped[2] = swapped[2], swapped[0]
    assert swapped != op_lists
    assert plan_run(swapped) == first

    # Digests are stable across every call above, so the group key cannot drift between builds.
    assert prefix_digest(_deep_head()) == deep_digest == first.entries[2].resume_digest

    # Scoped context, not the bare constructor: these patches must be undone the moment the
    # probe finishes, or a raising time.time()/os.urandom() leaks into every later test.
    with pytest.MonkeyPatch.context() as probe:
        for module, attribute in ((time, "time"), (time, "monotonic"), (random, "random"), (os, "urandom")):
            probe.setattr(module, attribute, _forbid(f"{module.__name__}.{attribute}"))
        assert plan_run(op_lists) == first


def test_plan_orders_deeper_group_before_shallower_group() -> None:
    """Two distinct groups run deeper-first, and equal depths fall back to the digest, not the index.

    Deeper-first is what maximizes reuse: the longest shared prefix is the most expensive
    recomputation avoided, so it is materialized while every one of its consumers is still
    pending. Depth must dominate the original index — a config whose cheap sections happen to be
    listed first still runs the expensive shared head first. When two groups are equally deep the
    digest breaks the tie, because the alternative (dict or set iteration order) varies with
    Python's per-process string hash seed and would make one build's execution order
    unreproducible.
    """
    deep: list[tuple[Callable, tuple[Any, ...]]] = _deep_head()
    shallow: list[tuple[Callable, tuple[Any, ...]]] = _shallow_head("cohort", "subject")
    assert len(deep) > len(shallow)
    # The SHALLOW group owns the lower original indices, so identity order would put it first.
    by_depth: list[list[tuple[Callable, tuple[Any, ...]]]] = [
        [*shallow, *_write_ops("shallow-0")],
        [*_shallow_head("cohort", "subject"), *_write_ops("shallow-1")],
        [*deep, *_write_ops("deep-2")],
        [*_deep_head(), *_write_ops("deep-3")],
    ]
    deeper_first: RunPlan = plan_run(by_depth)
    assert deeper_first.execution_order == [2, 3, 0, 1]
    assert deeper_first.entries[2].produce_digest == prefix_digest(deep)
    assert deeper_first.entries[0].produce_digest == prefix_digest(shallow)
    deep_len: int | None = deeper_first.entries[2].prefix_len
    shallow_len: int | None = deeper_first.entries[0].prefix_len
    assert deep_len == len(deep)
    assert shallow_len == len(shallow)
    assert deep_len is not None
    assert shallow_len is not None
    assert deep_len > shallow_len

    # Equal depth: the ascending digest decides, whichever way the indices happen to fall.
    head_x: list[tuple[Callable, tuple[Any, ...]]] = _shallow_head("x-table", "subject")
    head_y: list[tuple[Callable, tuple[Any, ...]]] = _shallow_head("y-table", "object")
    digest_x: str = prefix_digest(head_x)
    digest_y: str = prefix_digest(head_y)
    assert digest_x != digest_y
    same_depth: list[list[tuple[Callable, tuple[Any, ...]]]] = [
        [*head_x, *_write_ops("x-0")],
        [*_shallow_head("x-table", "subject"), *_write_ops("x-1")],
        [*head_y, *_write_ops("y-2")],
        [*_shallow_head("y-table", "object"), *_write_ops("y-3")],
    ]
    plan: RunPlan = plan_run(same_depth)
    leading: list[int] = [0, 1] if digest_x < digest_y else [2, 3]
    trailing: list[int] = [2, 3] if digest_x < digest_y else [0, 1]
    assert plan.execution_order == [*leading, *trailing]
    assert sorted(digest for digest in (digest_x, digest_y)) == [
        plan.entries[plan.execution_order[0]].produce_digest,
        plan.entries[plan.execution_order[2]].produce_digest,
    ]


def test_plan_does_not_group_a_shared_prefix_below_the_worth_caching_guard() -> None:
    """Guards beat sharing: two sections may share a prefix that is never checkpointed, and stay ungrouped.

    Sharing alone is not a reason to snapshot — writing and re-scanning an intermediate frame
    costs more than recomputing a prefix that never reached ``MIN_PREFIX_COST`` or
    ``MIN_PREFIX_OPS``. Both floors get a case: (a) two sections sharing an identical
    ``csv`` + ``pick`` prologue (cost 3) and (b) two sharing a single expensive ``resolve_batch``
    (cost 60, but one op, and the only longer prefix ends on their own write). In each the shared
    digest demonstrably exists — asserted, not assumed — yet ``checkpoints`` yields nothing, so
    the plan must stay empty. The contrast at the end proves the guard is the ONLY thing holding
    them apart: one more op over the cost floor and the same sections group immediately.
    """
    prologue: list[tuple[Callable, tuple[Any, ...]]] = _load_ops("gwas")
    cheap_a: list[tuple[Callable, tuple[Any, ...]]] = [*prologue, (lib.value, ("predicate", "increases")), *_write_ops("cheap-a")]
    cheap_b: list[tuple[Callable, tuple[Any, ...]]] = [*_load_ops("gwas"), (lib.value, ("predicate", "decreases")), *_write_ops("cheap-b")]
    assert prefix_digest(cheap_a[:2]) == prefix_digest(cheap_b[:2])  # genuinely shared instructions
    assert checkpoints(cheap_a) == [] == checkpoints(cheap_b)  # ... below the cost floor

    lone_resolve: list[tuple[Callable, tuple[Any, ...]]] = _shared_resolve_ops()
    assert op_cost(resolve_batch) >= MIN_PREFIX_COST
    short_c: list[tuple[Callable, tuple[Any, ...]]] = [*lone_resolve, *_write_ops("short-c")]
    short_d: list[tuple[Callable, tuple[Any, ...]]] = [*_shared_resolve_ops(), *_write_ops("short-d")]
    assert prefix_digest(short_c[:1]) == prefix_digest(short_d[:1])  # genuinely shared, and expensive
    assert checkpoints(short_c) == [] == checkpoints(short_d)  # ... below the length floor

    for op_lists in ([cheap_a, cheap_b], [short_c, short_d], [cheap_a, cheap_b, short_c, short_d]):
        plan: RunPlan = plan_run(op_lists)
        assert plan.shares is False
        assert plan.execution_order == list(range(len(op_lists)))
        assert all(entry == PlanEntry() for entry in plan.entries)

    # Contrast: cross the cost floor with the same shared prologue and the pair groups at once.
    over_floor_a: list[tuple[Callable, tuple[Any, ...]]] = [*prologue, *lone_resolve, *_write_ops("cheap-a")]
    over_floor_b: list[tuple[Callable, tuple[Any, ...]]] = [*_load_ops("gwas"), *_shared_resolve_ops(), *_write_ops("cheap-b")]
    grouped: RunPlan = plan_run([over_floor_a, over_floor_b])
    assert grouped.shares is True
    assert grouped.entries[0].produce_digest == prefix_digest(over_floor_a[:3]) == grouped.entries[1].resume_digest


def test_plan_prunes_a_lone_shallower_sharer_to_ungrouped() -> None:
    """A section whose peers all went deeper is PRUNED to ungrouped, never left a lone producer.

    WHY: a cache group must have a producer AND at least one consumer — a snapshot no consumer
    ever loads pays one parquet write plus rescan for zero reuse. Sections 0 and 1 checkpoint the
    k=5 and k=6 prefixes while section 2 checkpoints only k=5; the k=5 digest occurs for THREE
    sections so it passes the sharing count, but 0 and 1 are assigned the deeper k=6 and section 2
    ends up the sole member of the k=5 group. The prune must leave it fully ungrouped (nothing to
    produce, nothing to resume, no prefix length), drop it into the ungrouped tail of the execution
    order keeping its relative position, and disturb the surviving 0/1 group not at all.
    """
    deep: list[tuple[Callable, tuple[Any, ...]]] = _deep_head()
    deep_digest: str = prefix_digest(deep)
    shallow_digest: str = prefix_digest(deep[:5])
    op_lists: list[list[tuple[Callable, tuple[Any, ...]]]] = [
        [*deep, *_write_ops("deep-0")],
        [*_deep_head(), *_write_ops("deep-1")],
        [*deep[:5], *_write_ops("shallow-2")],
    ]
    assert checkpoints(op_lists[0]) == [5, 6] == checkpoints(op_lists[1])
    assert checkpoints(op_lists[2]) == [5]
    # The k=5 digest genuinely occurs for three sections: qualification passes, so it is the
    # post-assignment prune — not the sharing count — that removes section 2.
    assert sum(1 for ops in op_lists if 5 in checkpoints(ops) and prefix_digest(ops[:5]) == shallow_digest) == 3

    plan: RunPlan = plan_run(op_lists)
    assert plan.shares is True
    # The surviving group is unaffected: one producer, one consumer, same depth-6 digest.
    assert plan.entries[0] == PlanEntry(prefix_len=6, produce_digest=deep_digest)
    assert plan.entries[1] == PlanEntry(resume_digest=deep_digest, prefix_len=6)
    # The pruned section is fully ungrouped.
    assert plan.entries[2] == PlanEntry()
    assert plan.entries[2].resume_digest is None
    assert plan.entries[2].produce_digest is None
    assert plan.entries[2].prefix_len is None
    # Grouped block first, pruned section in the ungrouped tail keeping relative order.
    assert plan.execution_order == [0, 1, 2]
    # The pruned digest appears nowhere in the plan: nobody writes it, nobody reads it.
    assert all(shallow_digest not in (entry.produce_digest, entry.resume_digest) for entry in plan.entries)


def test_plan_keeps_a_two_member_group_when_peers_defect_deeper() -> None:
    """Negative regression: pruning must not over-remove — a group that keeps TWO members survives.

    The prune fires only below ``MIN_GROUP_SECTIONS`` after deepest assignment. Prefix digests
    nest — any section sharing another's depth-6 prefix necessarily shares its depth-5 prefix —
    so a deeper group always poaches at least two members from the shallower digest's pool, and
    the minimal fixture for "the shallow group loses members but stays valid" is four sections:
    0 and 1 defect to the shared k=6 digest while 2 and 3 stay on the k=5 digest all four
    produced. The k=5 group loses half its pool yet still has a producer and a consumer, so it
    must be planned exactly as before the prune existed. An implementation that pruned by
    digest-time counts, or cascaded the prune, would wrongly ungroup 2 and 3 and forfeit a real
    snapshot reuse.
    """
    deep: list[tuple[Callable, tuple[Any, ...]]] = _deep_head()
    deep_digest: str = prefix_digest(deep)
    shallow_digest: str = prefix_digest(deep[:5])
    op_lists: list[list[tuple[Callable, tuple[Any, ...]]]] = [
        [*deep, *_write_ops("deep-0")],
        [*_deep_head(), *_write_ops("deep-1")],
        [*deep[:5], *_write_ops("shallow-2")],
        [*_deep_head()[:5], *_write_ops("shallow-3")],
    ]
    assert checkpoints(op_lists[0]) == [5, 6] == checkpoints(op_lists[1])
    assert checkpoints(op_lists[2]) == [5] == checkpoints(op_lists[3])
    # All four sections produce the k=5 digest; only 0 and 1 produce (and are assigned) the k=6 one.
    assert sum(1 for ops in op_lists if 5 in checkpoints(ops) and prefix_digest(ops[:5]) == shallow_digest) == 4
    assert sum(1 for ops in op_lists if 6 in checkpoints(ops)) == 2

    plan: RunPlan = plan_run(op_lists)
    assert plan.shares is True
    # Deeper group first (original index within), then the surviving shallower group.
    assert plan.execution_order == [0, 1, 2, 3]
    assert plan.entries[0] == PlanEntry(prefix_len=6, produce_digest=deep_digest)
    assert plan.entries[1] == PlanEntry(resume_digest=deep_digest, prefix_len=6)
    # The k=5 group keeps exactly one producer and one consumer — nothing was over-removed.
    assert plan.entries[2] == PlanEntry(prefix_len=5, produce_digest=shallow_digest)
    assert plan.entries[3] == PlanEntry(resume_digest=shallow_digest, prefix_len=5)


# --- compile_subgraph resume/snapshot seam ----------------------------------------------------
#
# The seam ``build_pipeline`` Stage 5 drives: a producer offers every checkpoint to ``snapshot``
# and continues from the parquet it just wrote, a consumer hands ``resume`` the snapshot its
# producer stored and skips the ops it covers. Both parameters are keyword-only and default to
# ``None``, so the historical call sites — ``cli.py``'s unplanned branch, ``test_lib.py``'s e2e
# calls, and ``agent.py``'s ``_reduce_ops`` mirror (which never uses the run cache) — keep their
# exact behavior. The defaults test below pins that instead of assuming it.


def _seam_source(tmp_path: Path) -> Path:
    """Write the two-column headerless TSV the seam fixtures load (polars names them ``column_1``/``column_2``)."""
    source: Path = tmp_path / "seam.tsv"
    source.write_text("brca1\t1e-8\nmapk1\t0.5\n")
    return source


def _seam_ops(source: Path, store: Path, load: Callable = lib.csv) -> list[tuple[Callable, tuple[Any, ...]]]:
    """A real load → index → encode → clean → edge → write op list, shaped like ``Tcode.collect``'s.

    Args:
        source: TSV the load op reads.
        store: Parquet path the final ``to_store`` writes — the only slot that distinguishes two
            sections' lists here, so every prefix of two such lists is shared.
        load: Source-load callable, so a test can count loads.

    Returns:
        Cleaned ``(callable, args)`` ops. Note ``OP_COST`` prices the real ``lib.csv`` (2): a
        counting wrapper is unpriced and falls back to ``DEFAULT_OP_COST`` (1), which moves the
        cost floor one op later — so callers must take their checkpoint from :func:`checkpoints`
        rather than hardcoding a prefix length.
    """
    return [
        (load, (source, "\t")),
        (lib.idx, ()),
        (lib.column, ("subject", "column_1")),
        (lib.column, ("original_subject", "column_1")),
        (lib.coerce_columns, ()),
        (lib.clean_numeric, ()),
        (lib.value, ("predicate", "biolink:related_to")),
        (lib.to_store, (store, store.stem)),
    ]


def test_compile_subgraph_resume_skips_prefix_and_writes_identical_store(tmp_path: Path) -> None:
    """A resumed section skips the shared prefix and still writes a byte-identical store.

    WHY: this is the user-visible promise of the run cache — sections that open with the same
    instructions compute them ONCE. Three runs over one op list pin the whole handshake: a plain
    full run (the pre-seam baseline), a producer that snapshots its assigned checkpoint, and a
    consumer resumed from that snapshot. The consumer's parquet must be BYTE-identical to the
    baseline's, because a snapshot that widened a dtype, dropped a column, or reordered rows would
    silently change what a section writes; and the load op must not be invoked again, which is the
    evidence that the first ``prefix_len`` ops were skipped rather than merely re-run.
    """
    source: Path = _seam_source(tmp_path)
    loads: list[Path] = []

    def counting_load(p: Path, sep: str) -> pl.LazyFrame:
        """Counting stand-in for ``lib.csv``: record the load, then really load."""
        loads.append(p)
        return lib.csv(p, sep)

    baseline_ops: list[tuple[Callable, tuple[Any, ...]]] = _seam_ops(source, tmp_path / "baseline.parquet")
    producer_ops: list[tuple[Callable, tuple[Any, ...]]] = _seam_ops(source, tmp_path / "producer.parquet", load=counting_load)
    consumer_ops: list[tuple[Callable, tuple[Any, ...]]] = _seam_ops(source, tmp_path / "consumer.parquet", load=counting_load)
    # Only the final write differs, so every prefix is shared — and the digest computed here is the
    # key ``RunCache`` stores under, exactly as ``plan_run`` derives it.
    assert prefix_digest(producer_ops[:-1]) == prefix_digest(consumer_ops[:-1])
    offered: list[int] = checkpoints(producer_ops)
    assert offered, "the fixture must clear both worth-caching guards"
    prefix_len: int = offered[0]
    digest: str = prefix_digest(producer_ops[:prefix_len])

    baseline: Path = lib.compile_subgraph(baseline_ops)
    seen: list[int] = []
    with RunCache() as cache:

        def snapshot(position: int, lf: pl.LazyFrame) -> pl.LazyFrame | None:
            """Record every offered checkpoint and store the assigned one."""
            seen.append(position)
            if position == prefix_len:
                cache.store(digest, lf)
            return None

        producer: Path = lib.compile_subgraph(producer_ops, snapshot=snapshot)
        assert loads == [source]  # the producer built the shared prefix exactly once
        assert seen == offered  # every checkpoint offered, ascending, keyed by prefix length
        resumed: pl.LazyFrame | None = cache.load(digest)
        assert resumed is not None
        consumer: Path = lib.compile_subgraph(consumer_ops, resume=(prefix_len, resumed))
        summary: RunCacheStats = cache.stats()

    assert loads == [source]  # the consumer never invoked a skipped op
    assert (summary.stores, summary.hits, summary.misses) == (1, 1, 0)
    expected: bytes = baseline.read_bytes()
    assert producer.read_bytes() == expected
    assert consumer.read_bytes() == expected
    assert len({baseline, producer, consumer}) == 3  # three distinct stores, one identical result


def test_compile_subgraph_snapshot_replacement_avoids_producer_recompute(tmp_path: Path) -> None:
    """A producer's stored prefix is collected once when the callback returns its replacement scan."""
    evaluations: list[int] = []

    def source() -> pl.LazyFrame:
        """Create a tiny lazy source without doing any counted work."""
        return pl.DataFrame({"value": [1]}).lazy()

    def counted_prefix(lf: pl.LazyFrame) -> pl.LazyFrame:
        """Count execution of a lazy prefix expression, not construction of its plan."""
        return lf.with_columns(pl.col("value").map_elements(lambda value: evaluations.append(value) or value, return_dtype=pl.Int64))

    def identity(lf: pl.LazyFrame) -> pl.LazyFrame:
        """Keep enough cheap lazy ops to clear the checkpoint cost guard."""
        return lf

    output: Path = tmp_path / "producer.parquet"
    ops: list[tuple[Callable, tuple[Any, ...]]] = [
        (source, ()),
        (counted_prefix, ()),
        *[(identity, ()) for _ in range(6)],
        (lib.to_store, (output, output.stem)),
    ]
    offered: list[int] = checkpoints(ops)
    assert offered == [8]
    prefix_len: int = offered[0]
    digest: str = prefix_digest(ops[:prefix_len])

    with RunCache() as cache:

        def snapshot(position: int, lf: pl.LazyFrame) -> pl.LazyFrame | None:
            """Store the selected prefix and hand its scan back to compile_subgraph."""
            if position != prefix_len:
                return None
            cache.store(digest, lf)
            replacement: pl.LazyFrame | None = cache.load(digest)
            assert replacement is not None
            return replacement

        result: Path = lib.compile_subgraph(ops, snapshot=snapshot)

    assert evaluations == [1]
    assert pl.read_parquet(result)["value"].to_list() == [1]


@pytest.mark.parametrize("prefix_len", [-1, 8, 9])
def test_compile_subgraph_rejects_invalid_resume_boundaries(tmp_path: Path, prefix_len: int) -> None:
    """Reject negative and exhausted resume prefixes before executing any operation.

    WHY: a prefix at or beyond the instruction count skips the final ``to_store`` and returns a
    LazyFrame instead of the promised Path; a negative prefix silently changes the resume contract.
    The coded error must fire before the source or snapshot can be touched.
    """
    ops: list[tuple[Callable, tuple[Any, ...]]] = _seam_ops(_seam_source(tmp_path), tmp_path / "out.parquet")
    with pytest.raises(RunCacheError, match="runcache-invalid-resume"):
        lib.compile_subgraph(ops, resume=(prefix_len, pl.DataFrame().lazy()))


def test_compile_subgraph_rejects_empty_tcode_loudly() -> None:
    """An empty operation list is never treated as a successful subgraph compile.

    WHY: there is no final write operation to execute, so returning the seed or ``None`` would
    silently violate the compile_subgraph -> Path contract. Empty tcode is invalid with or without
    a resume tuple and must fail with the same coded run-cache validation error.
    """
    for resume in (None, (0, pl.DataFrame().lazy())):
        with pytest.raises(RunCacheError, match="runcache-invalid-resume"):
            lib.compile_subgraph([], resume=resume)


def test_compile_subgraph_defaults_are_no_op(tmp_path: Path) -> None:
    """Without ``resume``/``snapshot`` the seam changes nothing: same bytes, same phase sequence.

    WHY: every pre-existing call site relies on the historical contract, and the run cache must not
    leak into a build that never planned one. Two runs of one op list — bare, and passing the new
    keywords explicitly as ``None`` — must write byte-identical parquet and fire the identical
    phase sequence, hardcoded here (not recomputed from ``_phase_of``) so a change to the phase
    mapping cannot make the test agree with itself. The run-cache tag must never appear when
    nothing was resumed: a stray ``cache`` sub-step would misreport progress on every ordinary build.
    """
    source: Path = _seam_source(tmp_path)
    bare_ops: list[tuple[Callable, tuple[Any, ...]]] = _seam_ops(source, tmp_path / "bare.parquet")
    explicit_ops: list[tuple[Callable, tuple[Any, ...]]] = _seam_ops(source, tmp_path / "explicit.parquet")
    bare_phases: list[str] = []
    explicit_phases: list[str] = []

    bare: Path = lib.compile_subgraph(bare_ops, on_phase=bare_phases.append)
    explicit: Path = lib.compile_subgraph(explicit_ops, on_phase=explicit_phases.append, resume=None, snapshot=None)

    assert bare.read_bytes() == explicit.read_bytes()
    assert bare_phases == explicit_phases
    # One tag per phase transition over the fixture's ops: load(csv,idx) → encode(column x 2) →
    # clean(coerce_columns,clean_numeric) → edge(value "predicate") → write(to_store).
    assert bare_phases == ["load", "encode", "clean", "edge", "write"]
    assert lib.CACHE_PHASE not in bare_phases
