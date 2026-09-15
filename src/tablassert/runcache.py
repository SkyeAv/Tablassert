"""Content-addressed op digests, worth-caching guards, and the ephemeral run cache for TCode.

TCode compiles every config section to an ordered list of ``(callable, args)`` ops that
:class:`tablassert.lib.compile_subgraph` reduces over a LazyFrame. This module turns such an op
list into a stable digest (:func:`prefix_digest`) so caching keys on the pipeline's *semantics*
rather than file paths or timestamps, prices each op (:data:`OP_COST`) so only prefixes worth
snapshotting are checkpointed (:func:`checkpoints`), and materializes those checkpoints for the
duration of a single build (:class:`RunCache`).

Two layers, deliberately split:

- The digest and guard layers are pure: no I/O, no clock, no global mutable state. Identical op
  lists always digest identically, and any argument that could change the output frame changes
  the digest. Arguments that only feed log lines (section label / config name) are masked so two
  sections with identical transformations but different log labels share one cache entry.
- :class:`RunCache` is the only I/O here, and it is *ephemeral by design*: snapshots live in a
  temp directory deleted when the build ends — including when it ends in an exception. It is a
  separate layer from the persistent section store (``.tablassert/store``, xxh64-keyed, see
  :mod:`tablassert.utils`), which this module never reads or writes.
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel

from tablassert import lib, rs
from tablassert._lazy import LazyModule
from tablassert.errors import DOCS_URL
from tablassert.fullmap import resolve_batch
from tablassert.log import cat
from tablassert.qc import fullmap_audit

if TYPE_CHECKING:
    import polars as pl
else:
    pl = LazyModule("polars")

logger = cat("CACHE")

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

RunCacheErrorCode = Literal["runcache-unserializable-arg", "runcache-duplicate-store", "runcache-bad-digest", "runcache-closed"]
"""Stable kebab-case slugs for run-cache failures, appended to the docs URL on ``str()``."""


class RunCacheError(RuntimeError):
    """A run-cache operation cannot proceed without silently corrupting or mis-keying the cache.

    Follows the coded-error house style of :mod:`tablassert.errors` (human ``message`` plus
    kebab-case ``code`` plus docs URL on ``str()``), defined locally because widening the
    closed ``TablassertErrorCodes`` literal in ``errors.py`` is outside this module's scope.

    Raised instead of ever degrading quietly, because every case it covers would otherwise stay
    invisible until a build produced wrong data: an unsupported type hashed by a lossy fallback
    mis-keys every downstream digest; a second frame stored under one digest means two different
    prefixes claim the same content address; a store or load against a closed cache would either
    write into a deleted directory or report a false miss.
    """

    def __init__(self, message: str, *, code: RunCacheErrorCode) -> None:
        super().__init__(message)
        self.message = message
        self.code: RunCacheErrorCode = code

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
        "Enum, pydantic BaseModel, and module-level callables",
        code="runcache-unserializable-arg",
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


# --- Ephemeral run-scoped cache -------------------------------------------------------------
#
# ``checkpoints`` decides WHICH op prefixes are worth snapshotting; this is where a snapshot
# actually lives, for exactly one build. The cache is never persisted, on purpose:
#
# - Correctness: a digest covers an op prefix's *semantics*, not its environment — the polars
#   version, the bytes behind a source path argument, or an edited config that never reached the
#   op list. Nothing in the key would tell a later build that a surviving snapshot went stale,
#   so a stale hit would be indistinguishable from a fresh one.
# - Size: checkpoints sit just before the expensive ``resolve``/``resolve_batch`` steps, so each
#   snapshot is close to a full intermediate table. Accumulating those across runs fills the disk
#   with data that is worthless the moment the build that produced it finishes.
#
# Hence a ``tempfile.TemporaryDirectory`` torn down by ``__exit__`` — on the exception path too,
# so a crashed build leaves no artifacts either. I/O failures propagate rather than falling back
# to a recompute: a silently skipped store shows up only as a mysteriously slower build, and a
# silently failed load serves ``None`` where the caller expects a frame.

CACHE_DIR_PREFIX: str = "tablassert-runcache-"
"""``tempfile`` prefix naming a run's snapshot directory as Tablassert's.

``TemporaryDirectory`` removes the tree on ``__exit__`` (and at garbage collection), so the
prefix is normally visible only while a build runs. It earns its keep in the hard-crash case
(``SIGKILL``, power loss), where cleanup never runs and the directory name is the only thing
telling an operator that the leftover is a run cache and is safe to delete.
"""


@dataclass(frozen=True, slots=True)
class RunCacheStats:
    """Counters for one :class:`RunCache` lifetime — the values logged at context exit.

    Attributes:
        stores: Snapshots written. Equals the number of distinct digests held, because a
            duplicate store raises instead of overwriting.
        hits: :meth:`RunCache.load` calls that found a snapshot.
        misses: :meth:`RunCache.load` calls that did not. A miss is the normal signal to compute
            the prefix and then store it — never an error, which is why it gets its own counter
            instead of an exception.
        bytes_written: Sum of the on-disk parquet sizes written, i.e. what the ephemeral cache
            cost the disk during this build.
    """

    stores: int = 0
    hits: int = 0
    misses: int = 0
    bytes_written: int = 0


class RunCache:
    """Ephemeral, run-scoped snapshot store keyed by :func:`prefix_digest`.

    A context manager that materializes shared op-prefix results as parquet inside a
    ``tempfile.TemporaryDirectory`` for the duration of ONE build and deletes everything at exit
    — never persistent, never shared across runs. Sections that open with the same expensive
    prefix (load → encode → resolve) then :meth:`load` the snapshot a sibling already
    :meth:`store`d instead of recomputing it.

    Ephemerality is the design, not a limitation: see the section comment above this class for
    why a snapshot must not outlive its build. ``__exit__`` logs the run summary, delegates
    deletion to ``TemporaryDirectory.__exit__`` (which cleans up on the normal AND the exception
    path), and returns ``None`` — so an exception raised inside the ``with`` body always
    propagates. A cache is never a reason to swallow a build failure.

    Notes:
        Not thread-safe and not re-entrant: counters are plain ints mutated per call and one
        build owns one cache. ``store``/``load`` outside the ``with`` block raise rather than
        writing into a deleted directory or reporting a false miss.
    """

    def __init__(self: RunCache) -> None:
        self._tmp: tempfile.TemporaryDirectory[str] | None = None
        self._stores: int = 0
        self._hits: int = 0
        self._misses: int = 0
        self._bytes_written: int = 0

    @property
    def directory(self: RunCache) -> Path:
        """This run's snapshot directory — the tree :meth:`__exit__` deletes.

        Returns:
            The live ``TemporaryDirectory`` path.

        Raises:
            RunCacheError: ``runcache-closed`` outside the ``with`` block, so no caller can
                write snapshots somewhere that would outlive the run.
        """
        if self._tmp is None:
            raise RunCacheError(
                "run cache is not open: store()/load()/directory are only valid inside "
                "`with RunCache() as cache:`, because the snapshot directory exists for one build only",
                code="runcache-closed",
            )
        return Path(self._tmp.name)

    def __enter__(self: RunCache) -> RunCache:
        """Create this run's snapshot directory.

        Returns:
            This cache, open and empty.
        """
        # tempfile owns the location (TMPDIR-aware); the prefix makes a crash leftover ours.
        self._tmp = tempfile.TemporaryDirectory(prefix=CACHE_DIR_PREFIX)
        return self

    def __exit__(self: RunCache, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None) -> None:
        """Log the run summary, delete every snapshot, and let any in-flight exception propagate.

        Cleanup happens on both paths, so a failed build leaves no artifacts either. The return
        type is ``None`` rather than ``bool`` on purpose: a context manager that returned ``True``
        would suppress the exception, and this one never may.

        Args:
            exc_type: Type of the in-flight exception, or ``None`` on a normal exit.
            exc: The in-flight exception instance, or ``None``.
            tb: Its traceback, or ``None``.
        """
        summary: RunCacheStats = self.stats()
        logger.info(
            "Run cache: {stores} snapshots ({bytes} bytes) stored, {hits} hits, {misses} misses",
            stores=summary.stores,
            bytes=summary.bytes_written,
            hits=summary.hits,
            misses=summary.misses,
        )
        # Closed BEFORE delegating cleanup: if deletion itself raises, the cache is still not
        # open, so no later call can mistake the deleted tree for a usable directory.
        tmp: tempfile.TemporaryDirectory[str] | None = self._tmp
        self._tmp = None
        if tmp is not None:
            tmp.__exit__(exc_type, exc, tb)

    def stats(self: RunCache) -> RunCacheStats:
        """Report the counters accumulated so far.

        Returns:
            A frozen :class:`RunCacheStats` copy — safe to hold and compare after the cache
            is closed, since nothing in it references the deleted directory.
        """
        return RunCacheStats(stores=self._stores, hits=self._hits, misses=self._misses, bytes_written=self._bytes_written)

    def store(self: RunCache, digest: str, lf: pl.LazyFrame) -> Path:
        """Collect ``lf`` and write it as this run's snapshot for ``digest``.

        Args:
            digest: Content address of the op prefix that produced ``lf`` (:func:`prefix_digest`).
            lf: LazyFrame to materialize.

        Returns:
            The parquet path written, inside this run's temp directory (polars' default zstd
            compression: a good ratio for cheap CPU on data that lives minutes).

        Raises:
            RunCacheError: ``runcache-duplicate-store`` when this run already holds a snapshot
                for ``digest``. Overwriting was rejected deliberately: a digest is a CONTENT
                address, so a second frame under the same key means either a hash collision or a
                caller bug, and in both cases every hit already served from that key is
                suspect. Raising surfaces it at the store that caused it instead of letting a
                later section silently read a frame its own prefix never produced.
            RunCacheError: ``runcache-bad-digest`` or ``runcache-closed`` from :meth:`_snapshot`.
            OSError: Propagated from the collect/write/stat — a snapshot that cannot be written
                is a build failure, never a silent recompute.
        """
        path: Path = self._snapshot(digest)
        if path.is_file():
            raise RunCacheError(
                f"run cache already holds a snapshot for digest {digest} at {path}; a digest is a content "
                "address, so a second frame under it means two different op prefixes hashed alike (or the "
                "same prefix was stored twice) and the cache can no longer be trusted",
                code="runcache-duplicate-store",
            )
        df: pl.DataFrame = lf.collect()
        df.write_parquet(path)
        self._stores += 1
        self._bytes_written += path.stat().st_size
        return path

    def load(self: RunCache, digest: str) -> pl.LazyFrame | None:
        """Return a lazy scan of the snapshot for ``digest``, or ``None`` on a miss.

        Args:
            digest: Content address to look up.

        Returns:
            ``pl.scan_parquet`` of the snapshot — lazy, so a hit composes into the caller's
            pipeline exactly like a freshly computed prefix and is only read when it collects —
            or ``None`` when this run has not stored that digest. A miss is the signal to compute
            and then :meth:`store`, so it never raises.

        Raises:
            RunCacheError: ``runcache-bad-digest`` or ``runcache-closed`` from :meth:`_snapshot`.
                A closed cache raises instead of returning ``None``: its directory is gone, so a
                "miss" would be a lie about a snapshot that may well have existed.
            OSError: Propagated from ``scan_parquet`` for an unreadable or corrupt snapshot.
        """
        path: Path = self._snapshot(digest)
        if not path.is_file():
            self._misses += 1
            return None
        self._hits += 1
        return pl.scan_parquet(path)

    def _snapshot(self: RunCache, digest: str) -> Path:
        """Map ``digest`` to its parquet path inside this run's directory.

        Args:
            digest: Content address used as the filename stem.

        Returns:
            ``<run directory>/<digest>.parquet``.

        Raises:
            RunCacheError: ``runcache-bad-digest`` when ``digest`` is not one safe filename
                component. A digest carrying a separator would write OUTSIDE the temp directory,
                where ``__exit__``'s cleanup never reaches it — quietly breaking the "no
                artifacts survive a run" contract. :func:`prefix_digest` only ever emits hex, so
                this guards against a caller handing the cache something that is not a digest.
            RunCacheError: ``runcache-closed`` propagated from :attr:`directory`.
        """
        if not digest or digest in {".", ".."} or "\\" in digest or Path(digest).name != digest:
            raise RunCacheError(
                f"run cache digest {digest!r} is not a single safe filename component; digests come from "
                "prefix_digest (16 hex chars) and must not contain path separators",
                code="runcache-bad-digest",
            )
        return self.directory / f"{digest}.parquet"
