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
from pathlib import Path
from typing import Any

import pytest

import tablassert.lib as lib
from tablassert import rs
from tablassert.enums import Comparisons, Tokens
from tablassert.fullmap import ResolveSpec, resolve_batch
from tablassert.models import Reindex
from tablassert.qc import fullmap_audit
from tablassert.runcache import LABEL_FREE, LABEL_PLACEHOLDER, RunCacheError, canonical, op_repr, prefix_digest


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
