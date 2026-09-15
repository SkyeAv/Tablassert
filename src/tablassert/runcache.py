"""Pure content-addressed hashing of TCode op lists.

TCode compiles every config section to an ordered list of ``(callable, args)`` ops that
:class:`tablassert.lib.compile_subgraph` reduces over a LazyFrame. This module turns such an
op list into a stable digest (``prefix_digest``) so later stories can key a build cache on the
pipeline's *semantics* rather than file paths or timestamps. Everything here is pure: no I/O,
no global mutable state — identical op lists always digest identically, and any argument that
could change the output frame changes the digest. Arguments that only feed log lines (section
label / config name) are masked so two sections with identical transformations but different
log labels share one cache entry.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel

from tablassert import lib, rs
from tablassert.errors import DOCS_URL
from tablassert.fullmap import resolve_batch
from tablassert.qc import fullmap_audit

LABEL_PLACEHOLDER: str = "~"
"""Serialization stand-in for label-only arguments (see :data:`LABEL_FREE`)."""

LABEL_FREE: dict[Callable, tuple[int, ...]] = {
    # Positions index the STORED op args tuple. TCode drops each op's leading LazyFrame
    # (compile_subgraph pipes it through functools.reduce), so a position here is the
    # function-signature parameter index minus one. Masked slots feed log context only —
    # never the computation — so two sections whose ops differ ONLY in these slots are the
    # same transformation and must share a digest.
    resolve_batch: (
        3,  # section_hash: forwarded only to log_unmatched for log context (fullmap.py).
        4,  # config_file: forwarded only to log_unmatched for log context (fullmap.py).
    ),
    fullmap_audit: (
        1,  # section_hash: names the originating config in audit log lines only (qc.py).
        2,  # config_file: names the originating config in audit log lines only (qc.py).
    ),
}

RunCacheErrorCode = Literal["runcache-unserializable-arg"]
"""Stable kebab-case slug for canonicalization failures, appended to the docs URL on ``str()``."""


class RunCacheError(RuntimeError):
    """An argument cannot be canonically serialized into a content-addressed op digest.

    Follows the coded-error house style of :mod:`tablassert.errors` (human ``message`` plus
    kebab-case ``code`` plus docs URL on ``str()``), defined locally because widening the
    closed ``TablassertErrorCodes`` literal in ``errors.py`` is outside this module's scope.
    Raised instead of ever falling back to a lossy hash: an unsupported type silently hashed
    would mis-key the cache without anyone noticing, so unsupported types fail loudly.
    """

    code: RunCacheErrorCode = "runcache-unserializable-arg"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return f"{self.message}\n\nFor further information visit {DOCS_URL}{self.code}"


def canonical(value: object) -> str:
    """Recursively serialize ``value`` to a deterministic string.

    Serialization rules (checked in this order; order matters for subclasses — every
    ``str``-valued enum in ``tablassert.enums`` is a ``str`` subclass, and ``bool`` is an
    ``int`` subclass):

    - ``None`` → ``"None"``; ``bool``/``int``/``float`` → ``repr`` (``True`` never degrades to ``1``)
    - ``Enum`` → recursion on ``.value`` (the member itself carries no semantics beyond its value)
    - ``str`` → ``repr`` (quotes keep ``"1"`` distinct from ``1``)
    - ``pathlib.Path`` → ``Path(<str>)`` (path semantics use ``str``, not platform repr)
    - pydantic ``BaseModel`` → ``model_dump_json()`` (deterministic field order per class)
    - callables → ``<module>.<qualname>`` — ops are identified by their function identity
    - ``list``/``tuple`` → comma-joined items in ``[...]``/``(...)`` (brackets keep them distinct)
    - ``dict`` → ``{key: value, ...}`` sorted by canonical key (insertion order must not matter)

    Args:
        value: Any value appearing in an op's argument tuple.

    Returns:
        Deterministic string encoding; equal Python values yield equal strings.

    Raises:
        RunCacheError: For any type outside the supported set (e.g. ``set``, ``bytes``,
            ``object``, callables without a stable ``__qualname__`` such as
            ``functools.partial``) — loudly, because a fallback encoding would mis-hash.
    """
    if value is None:
        return "None"
    if isinstance(value, bool):
        return repr(value)
    if isinstance(value, int):
        return repr(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, Enum):
        return canonical(value.value)
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, Path):
        return f"Path({str(value)!r})"
    if isinstance(value, BaseModel):
        return value.model_dump_json()
    if callable(value):
        module: str | None = getattr(value, "__module__", None)
        qualname: str | None = getattr(value, "__qualname__", None)
        if module is not None and qualname is not None:
            return f"{module}.{qualname}"
    if isinstance(value, (list, tuple)):
        items: list[str] = [canonical(item) for item in value]
        return f"[{', '.join(items)}]" if isinstance(value, list) else f"({', '.join(items)})"
    if isinstance(value, dict):
        rendered: list[tuple[str, str]] = [(canonical(key), canonical(val)) for key, val in value.items()]
        rendered.sort(key=lambda pair: pair[0])
        return "{" + ", ".join(f"{key}: {val}" for key, val in rendered) + "}"
    raise RunCacheError(
        f"cannot canonically serialize {type(value).__module__}.{type(value).__qualname__} value {value!r} "
        "for an op digest; supported types are None, bool, int, float, str, Path, list, tuple, dict, "
        "Enum, pydantic BaseModel, and module-level callables"
    )


def op_repr(fn: Callable, args: tuple[Any, ...]) -> str:
    """Render one op as ``<module>.<qualname>(<canonical(args)>)`` with label slots masked.

    Args:
        fn: The op callable exactly as ``Tcode.collect`` stored it (its LazyFrame argument
            is never part of ``args`` — ``compile_subgraph`` pipes that through ``reduce``).
        args: The op's stored argument tuple.

    Returns:
        The canonical one-line op representation fed into :func:`prefix_digest`.

    Raises:
        RunCacheError: Propagated from :func:`canonical` for unsupported argument types.
    """
    label_positions: tuple[int, ...] = LABEL_FREE.get(fn, ())
    masked: tuple[Any, ...] = tuple(LABEL_PLACEHOLDER if position in label_positions else arg for position, arg in enumerate(args))
    return f"{canonical(fn)}({canonical(masked)})"


def prefix_digest(ops: list[tuple[Callable, tuple[Any, ...]]]) -> str:
    """Content-address an op list with the bundled Rust XXH64 implementation.

    The digest keys caching on pipeline semantics: identical op lists digest identically,
    any change to a callable or a semantic argument changes the digest, and label-only
    argument slots (see :data:`LABEL_FREE`) are masked so log context cannot split caches.
    Delegating to :func:`tablassert.rs.xxh64` keeps the run cache aligned with the existing
    section-store key primitive and avoids a second hashing implementation.

    Args:
        ops: Cleaned ``(callable, args)`` op list as produced by ``Tcode.collect``.

    Returns:
        16-character lowercase XXH64 hex string.

    Raises:
        RunCacheError: Propagated from :func:`op_repr` for unsupported argument types —
            never swallowed, because a mis-hashed op list would poison the cache key.
    """
    payload: str = "\n".join(op_repr(fn, args) for fn, args in ops)
    return rs.xxh64(payload)


# --- Worth-caching guards -------------------------------------------------------------------
#
# A later story snapshots shared op-prefix results so sections that begin with the same
# load/encode/resolve work reuse it instead of recomputing. Snapshotting is NOT free: writing
# the intermediate frame and re-scanning it on a hit costs real I/O, so checkpointing a trivial
# prefix would spend more than it saves. ``OP_COST`` prices every op in a static unit (one
# cheap lazy expression ≈ 1); ``checkpoints`` then only proposes a snapshot once a prefix is
# both long enough to amortize the write and expensive enough that recomputing it hurts.
# Everything below is pure: no I/O, no clock, no randomness, no mutable global state.

DEFAULT_OP_COST: int = 1
"""Cost charged to any op absent from :data:`OP_COST`.

A default of one unit keeps the guard conservative: an unpriced op is treated as the cheapest
meaningful lazy expression, so an unknown op alone can never push a prefix over the cost
threshold. New ops should still be costed deliberately (``test_op_cost_covers_every_phase_of_callable``
pins this for every callable in ``lib.PHASE_OF``) rather than relying on the fallback.
"""

MIN_PREFIX_OPS: int = 2
"""Smallest prefix length eligible to be checkpointed.

A one-op prefix has nothing to share beyond a single step, and snapshotting it can never beat
recomputing it, so the shortest checkpoint spans at least two ops.
"""

MIN_PREFIX_COST: int = 8
"""Cumulative :data:`OP_COST` a prefix must reach before it is worth snapshotting.

Set above the total cost of the cheap load/filter/encode prologue (csv 2 + a handful of 1-unit
ops) so a prefix that has not yet reached an expensive step — ``resolve``/``resolve_batch`` (60)
or ``fullmap_audit`` (30) — is never checkpointed. Recomputing a sub-8 prefix is cheaper than
the snapshot write plus rescan that a checkpoint forces on every downstream section.
"""

OP_COST: dict[Callable, int] = {
    # Load: opening/parsing a source table is heavier than one lazy expression but trivial next
    # to resolution.
    lib.csv: 2,
    lib.excel: 2,
    # Filter: single lazy row predicates/slices.
    lib.idx: 1,
    lib.crop: 1,
    lib.pick: 1,
    lib.reindex: 1,
    # head samples min(HEAD_ROWS, height) rows, so it needs a height pass before slicing.
    lib.head: 2,
    # Encode: each is one lazy expression over a single column.
    lib.value: 1,
    lib.column: 1,
    lib.fill: 1,
    lib.explode: 1,
    lib.regex: 1,
    lib.prefix: 1,
    lib.suffix: 1,
    lib.math_op: 1,
    lib.split_list: 1,
    # Clean: coerce_columns applies five coercion rules (schema resolve + renames + value
    # coercion) in one pass, so it outprices a single-expression clean op.
    lib.coerce_columns: 3,
    lib.clean_numeric: 2,
    # Significance: sig computes the qualifier; the drop_* ops are single lazy filters.
    lib.sig: 2,
    lib.drop_not_significant: 1,
    lib.drop_zero_effect_size: 1,
    lib.drop_low_number_of_cases: 1,
    # NLP normalization and trim are cheap lazy column ops.
    lib.level_one: 1,
    lib.level_two: 1,
    lib.trim: 1,
    # Resolve: the dominant cost — a redb round trip plus per-column filter/rank and eager joins
    # back into the frame. ``resolve`` is the single-column wrapper around ``resolve_batch`` and
    # pays the same round trip, so both are priced at 60. (``resolve`` is in ``lib.PHASE_OF`` but
    # was not itemized in the cost spec; it is costed here to keep the table exhaustive.)
    lib.resolve: 60,
    lib.resolve_batch: 60,
    # QC: fuzzy match plus a SapBERT (sentence-transformers) cascade when enabled.
    lib.fullmap_audit: 30,
    # Edge/provenance: single lazy struct/column emissions.
    lib.edge_category: 1,
    lib.publications: 1,
    lib.retrieval_sources: 1,
    lib.inline_supporting_study: 1,
    # Finalize.
    lib.prune_to_class: 2,
    lib.format_numeric: 2,
    # Write: collect the frame and write parquet.
    lib.to_store: 4,
}

WRITE_OPS: frozenset[Callable] = frozenset({lib.to_store})
"""Ops that materialize a section's own output and must never be checkpointed as a shared prefix.

A section always replays its final write itself: the parquet it produces IS the section's
result, so caching the prefix that ends on the write would snapshot an output each consumer
must write anyway. Excluding ``len(ops)`` for a write-final list keeps the last checkpoint at
``len(ops) - 1``.
"""


def op_cost(fn: Callable) -> int:
    """Return the static cost of one op callable.

    Args:
        fn: An op callable as stored by ``Tcode.collect``.

    Returns:
        Its :data:`OP_COST` weight, or :data:`DEFAULT_OP_COST` when ``fn`` is not priced.
    """
    return OP_COST.get(fn, DEFAULT_OP_COST)


def checkpoints(ops: list[tuple[Callable, tuple[Any, ...]]]) -> list[int]:
    """Select the prefix lengths of ``ops`` that are worth snapshotting.

    A prefix length ``k`` (``1 <= k``) is a checkpoint when BOTH guards pass:

    - ``k >= MIN_PREFIX_OPS`` — enough ops to amortize the snapshot write, and
    - the cumulative :func:`op_cost` of ``ops[:k]`` ``>= MIN_PREFIX_COST`` — enough
      recomputation saved to justify the write-plus-rescan a checkpoint forces downstream.

    Additionally ``k == len(ops)`` is never selected when the final op is a write op
    (:data:`WRITE_OPS`, i.e. ``to_store``): each section replays its own final write, so the
    last usable checkpoint for such a list is ``len(ops) - 1``. Trivial op lists (too short, or
    never crossing the cost threshold) yield an empty list — they are never checkpointed.

    Pure and deterministic: identical op lists always produce identical, strictly increasing
    checkpoint lengths.

    Args:
        ops: Cleaned ``(callable, args)`` op list as produced by ``Tcode.collect``.

    Returns:
        Strictly increasing prefix lengths ``k`` (``1 <= k <= len(ops)``) that clear both guards,
        excluding ``len(ops)`` for a write-final list. Empty when nothing is worth caching.
    """
    selected: list[int] = []
    cumulative: int = 0
    total: int = len(ops)
    for index, op in enumerate(ops):
        cumulative += op_cost(op[0])
        prefix_len: int = index + 1
        if prefix_len < MIN_PREFIX_OPS or cumulative < MIN_PREFIX_COST:
            continue
        if prefix_len == total and op[0] in WRITE_OPS:
            continue
        selected.append(prefix_len)
    return selected
