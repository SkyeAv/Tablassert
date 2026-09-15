"""Optional-extra registry: which extra ships which package, and how to install it.

Tablassert's base install builds knowledge graphs from CSV/TSV sources. QC, the
autonomous agent, and GEPA prompt optimization are OPTIONAL extras, so a user can
reach a code path whose dependencies were never installed. When that happens the
failure must name the extra and the exact install command — never a bare
``ModuleNotFoundError: No module named 'sklearn'`` raised hours into a build.

This module is the single source of truth for that mapping. Two entry points:

- :func:`require` is a PREFLIGHT check. It probes with ``importlib.util.find_spec``
  (which does not execute the module, so it costs nothing) and is called at the CLI
  boundary, before any expensive work starts.
- :func:`require_module` is the LATE check for a lazy import that has already been
  reached, used by :mod:`tablassert.agent` and :class:`tablassert._lazy.LazyModule`.
"""

from __future__ import annotations

from importlib import import_module
from importlib.util import find_spec
from typing import Final

from tablassert.errors import MissingExtraError, QcRuntimeMissingError, describe_missing

# Extra -> {import name: distribution name}. Both are needed: find_spec probes the import
# name, while the message must show the DISTRIBUTION name, which differs often enough
# (sklearn/scikit-learn) that printing the module would misdirect.
#
# ``rt`` is absent BY DESIGN. It installs polars[rtcompat], which imports as plain
# ``polars``, so no find_spec probe can tell it apart from the stock wheel; its hint is
# emitted from the polars import-failure path instead (see :func:`actionable_import_error`).
EXTRA_PACKAGES: Final[dict[str, dict[str, str]]] = {
    "aria2": {"aria2c": "aria2"},
    "qc": {"sklearn": "scikit-learn", "sentence_transformers": "sentence-transformers"},
    "agent": {"smolagents": "smolagents", "litellm": "litellm"},
    "optimize": {"dspy": "dspy"},
    "distill": {"datasets": "datasets"},
    "log": {"loguru": "loguru"},
}

# Import name -> owning extra, derived so the two can never drift apart.
EXTRA_FOR_MODULE: Final[dict[str, str]] = {module: extra for extra, packages in EXTRA_PACKAGES.items() for module in packages}

# What each extra unlocks, phrased to follow "required by ...".
FEATURES: Final[dict[str, str]] = {
    "rt": "the runtime-compatible polars build",
    "aria2": "the bundled aria2c downloader (used automatically by build-fullmap when installed)",
    "qc": "the QC audit",
    "agent": "the tablassert agent",
    "optimize": "GEPA prompt optimization (tablassert agent --optimize)",
    "distill": "the distillation dataset export (tablassert distill-export)",
    "log": "file and progress logging",
}

# An unregistered module reaching :func:`require_module` came from the agent's dynamic
# lazy-import path (the only place a module name is not known statically).
_FALLBACK_EXTRA: Final[str] = "agent"


def install_command(extra: str) -> str:
    """Return the pip command that installs ``extra``, quoted.

    The quotes are load-bearing, not cosmetic: ``pip install tablassert[agent]`` is a glob
    pattern in zsh and fails with ``no matches found`` before pip is ever reached.
    """
    return f'pip install "tablassert[{extra}]"'


def missing(extra: str) -> tuple[str, ...]:
    """Return the distribution names in ``extra`` that are not importable.

    Probes with ``find_spec``, so a present dependency is NOT imported and an absent
    one costs nothing: safe to call on every invocation of a command.

    Args:
        extra: A detectable extra (``qc``, ``agent``, ``optimize`` or ``log``).

    Returns:
        Distribution names that could not be found, in declaration order; empty when
        the extra is fully installed.

    Raises:
        KeyError: If ``extra`` is not detectable (notably ``rt``; see the module docstring).
    """
    return tuple(dist for module, dist in EXTRA_PACKAGES[extra].items() if find_spec(module) is None)


def is_installed(extra: str) -> bool:
    """Return whether every package in ``extra`` is importable."""
    return not missing(extra)


def require(extra: str, *, required_by: str | None = None) -> None:
    """Fail loudly unless ``extra`` is fully installed.

    Preflight guard: call it at the point the user's intent is known (a ``--qc`` flag, the
    ``agent`` command) rather than where the import happens, so the error arrives before
    the work instead of after it.

    Args:
        extra: A detectable extra (``qc``, ``agent``, ``optimize`` or ``log``).
        required_by: Feature name for the message, phrased to follow "required by".
            Defaults to the extra's entry in :data:`FEATURES`.

    Raises:
        QcRuntimeMissingError: If ``extra`` is ``qc`` and any of its packages is absent
            (kept distinct so the documented ``qc-runtime-missing`` code still fires).
        MissingExtraError: If any other extra is incomplete.
    """
    absent: tuple[str, ...] = missing(extra)
    if not absent:
        return
    if extra == "qc":
        raise QcRuntimeMissingError(absent)
    raise MissingExtraError(extra, describe_missing(absent, required_by or FEATURES[extra]), missing=absent)


def require_module(module: str, *, required_by: str | None = None) -> None:
    """Import one optional module or raise an error naming the extra that ships it.

    Unlike :func:`require` this actually imports, so a package that is present but broken
    still fails here (with its own error) rather than being reported as missing.

    Args:
        module: Import name (``smolagents``, ``dspy``, ...).
        required_by: Feature name for the message; defaults to the owning extra's entry
            in :data:`FEATURES`.

    Raises:
        MissingExtraError: If ``module`` cannot be imported.
    """
    extra: str = EXTRA_FOR_MODULE.get(module, _FALLBACK_EXTRA)
    try:
        import_module(module)
    except ImportError as exc:
        dist: str = EXTRA_PACKAGES.get(extra, {}).get(module, module)
        raise MissingExtraError(extra, describe_missing((dist,), required_by or FEATURES[extra]), missing=(dist,)) from exc


def actionable_import_error(module: str) -> MissingExtraError | None:
    """Return the extra-aware error to raise for a failed ``import module``, or ``None``.

    Used by :class:`tablassert._lazy.LazyModule`, which imports optional and core modules
    alike and only knows the module name. ``None`` means "nothing useful to add" — the
    caller re-raises the original ``ImportError`` untouched.

    ``polars`` is special-cased: it is a CORE dependency, so a failure to import it is
    almost never absence but an incompatible wheel — the exact problem the ``[rt]`` extra
    (``polars[rtcompat]``) exists to solve.
    """
    if module == "polars":
        return MissingExtraError(
            "rt",
            "polars failed to import — if this machine's CPU lacks the instructions the default polars wheel needs, "
            "the runtime-compatible build is the fix.",
            missing=("polars",),
        )
    extra: str | None = EXTRA_FOR_MODULE.get(module)
    if extra is None:
        return None
    dist: str = EXTRA_PACKAGES[extra][module]
    return MissingExtraError(extra, describe_missing((dist,), FEATURES[extra]), missing=(dist,))
