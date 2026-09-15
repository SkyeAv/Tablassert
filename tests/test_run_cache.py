"""Tests for the pure content-addressed hashing in ``tablassert.runcache``.

The op-list shapes here mirror what ``Tcode.collect`` actually stores (see
``src/tablassert/lib.py``'s ``_source_ops``/``_node_ops`` and the ``(resolve_batch, (specs, db,
log, store.stem, config.name, column_context, tag))`` / ``(fullmap_audit, (col, store.stem,
config.name, out, log))`` tuples around lines 1301-1305): the leading LazyFrame is NOT part of
the stored args because ``compile_subgraph`` pipes it through ``reduce``.
"""

from __future__ import annotations

import operator
from collections.abc import Callable
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest

import tablassert.lib as lib
from tablassert import rs
from tablassert.enums import Comparisons, Tokens
from tablassert.fullmap import ResolveSpec, resolve_batch
from tablassert.models import Reindex
from tablassert.qc import fullmap_audit
from tablassert.runcache import (
    DEFAULT_OP_COST,
    LABEL_FREE,
    LABEL_PLACEHOLDER,
    MIN_PREFIX_COST,
    MIN_PREFIX_OPS,
    OP_COST,
    RunCacheError,
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
