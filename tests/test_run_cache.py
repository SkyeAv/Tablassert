"""Tests for ``tablassert.runcache``: op digests, worth-caching guards, and the ephemeral run cache.

The first half pins the pure layers — content-addressed hashing (:func:`prefix_digest`) and the
cost guards (:func:`checkpoints`). The second half pins :class:`RunCache`, the one piece that
touches the filesystem: its snapshots must round-trip a frame exactly, its directory must be
deleted when the build ends (including when it ends in an exception), its counters must be
exact, and every way it could be misused must fail loudly instead of degrading into a silent
recompute or a false miss.

The op-list shapes here mirror what ``Tcode.collect`` actually stores (see
``src/tablassert/lib.py``'s ``_source_ops``/``_node_ops`` and the ``(resolve_batch, (specs, db,
log, store.stem, config.name, column_context, tag))`` / ``(fullmap_audit, (col, store.stem,
config.name, out, log))`` tuples around lines 1301-1305): the leading LazyFrame is NOT part of
the stored args because ``compile_subgraph`` pipes it through ``reduce``.
"""

from __future__ import annotations

import operator
import tempfile
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
    RunCache,
    RunCacheError,
    RunCacheStats,
    canonical,
    checkpoints,
    op_cost,
    op_repr,
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
