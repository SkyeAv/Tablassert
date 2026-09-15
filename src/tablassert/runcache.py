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

from tablassert import rs
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
