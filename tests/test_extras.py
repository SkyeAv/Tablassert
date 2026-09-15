"""The optional-extra registry: detection, messages, and the guarantee that it matches pyproject.

Every test here runs in the BASE environment. The registry is the single place that answers
"which extra ships this package, and what do I type to install it", so the tests pin BOTH halves:
the mapping stays in sync with ``pyproject.toml`` (it silently misdirects users otherwise), and
every failure carries a copy-pasteable install command.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

import pytest

from tablassert import extras
from tablassert._lazy import LazyModule
from tablassert.agent import AGENT_EXTRA
from tablassert.errors import MissingExtraError, QcRuntimeMissingError, format_missing_extra

PYPROJECT: Path = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _declared_extras() -> dict[str, list[str]]:
    """Read ``[project.optional-dependencies]`` straight from pyproject."""
    data: dict[str, Any] = tomllib.loads(PYPROJECT.read_text())
    return data["project"]["optional-dependencies"]


def test_registry_covers_every_declared_extra() -> None:
    """Every extra in ``pyproject.toml`` is described by the registry.

    Why: a new extra added without a registry entry gets no actionable error at all — its
    absence surfaces as a raw ``ModuleNotFoundError`` from wherever it is imported. ``rt`` is
    exempt from package DETECTION (polars[rtcompat] imports as plain ``polars``, so find_spec
    cannot see it) but must still carry a feature description for its message.
    """
    declared: set[str] = set(_declared_extras())
    assert declared == set(extras.FEATURES), "an extra was added to pyproject without a FEATURES entry"
    assert set(extras.EXTRA_PACKAGES) == declared - {"rt"}


@pytest.mark.parametrize("extra", ["cli", "qc", "agent", "optimize"])
def test_registered_packages_are_actually_shipped_by_their_extra(extra: str) -> None:
    """Each import name maps to a distribution that its extra really installs.

    Why: the message tells users to install ``tablassert[qc]`` to get ``scikit-learn``. If the
    dependency were dropped from that extra, the advice would be a dead end — this catches the
    drift at test time rather than in a user's terminal.
    """
    requirements: str = " ".join(_declared_extras()[extra]).lower()
    for dist in extras.EXTRA_PACKAGES[extra].values():
        assert dist.lower() in requirements, f"{dist} is not declared in [{extra}]"


def test_module_lookup_is_derived_and_unambiguous() -> None:
    """``EXTRA_FOR_MODULE`` is derived from ``EXTRA_PACKAGES``, and dspy belongs to [optimize].

    Why: the two tables must never disagree about which extra owns a package — pointing a user
    at ``[agent]`` for dspy sends someone who already installed it in a circle.
    """
    assert extras.EXTRA_FOR_MODULE["cyclopts"] == "cli"
    assert extras.EXTRA_FOR_MODULE["rich"] == "cli"
    assert extras.EXTRA_FOR_MODULE["dspy"] == "optimize"
    assert extras.EXTRA_FOR_MODULE["smolagents"] == "agent"
    assert extras.EXTRA_FOR_MODULE["sklearn"] == "qc"
    assert len(extras.EXTRA_FOR_MODULE) == sum(len(packages) for packages in extras.EXTRA_PACKAGES.values())


def test_cli_entrypoint_preflights_the_extra_before_importing_the_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    """The installed console script explains how to enable the otherwise-optional CLI runtime."""
    import sys

    from tablassert import cli_entry

    calls: list[str] = []

    def reject(extra: str, *, required_by: str | None = None) -> None:
        calls.append(extra)
        raise MissingExtraError(extra, "CLI runtime is missing.")

    monkeypatch.delitem(sys.modules, "tablassert.cli", raising=False)
    monkeypatch.setattr(cli_entry.extras, "require", reject)
    with pytest.raises(MissingExtraError, match="tablassert\\[cli\\]"):
        cli_entry.main()
    assert calls == ["cli"]
    assert "tablassert.cli" not in sys.modules


def test_cli_entrypoint_runs_the_app_once_the_extra_is_present(monkeypatch: pytest.MonkeyPatch) -> None:
    """A satisfied preflight hands control to the live Cyclopts application."""
    import sys

    from tablassert import cli_entry

    calls: list[str] = []

    def fake_app() -> None:
        calls.append("app")

    cli_module = type(sys)("tablassert.cli")
    cli_module.APP = fake_app
    monkeypatch.delitem(sys.modules, "tablassert.cli", raising=False)
    monkeypatch.setitem(sys.modules, "tablassert.cli", cli_module)
    monkeypatch.setattr(cli_entry.extras, "require", lambda extra, *, required_by=None: None)
    cli_entry.main()
    assert calls == ["app"]


def test_missing_reports_distribution_names_not_import_names(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing package is reported as ``scikit-learn``, the name users type into pip.

    Why: the import name and the distribution name differ often enough (sklearn/scikit-learn)
    that echoing the module would send users to a package that does not
    exist on PyPI.
    """
    monkeypatch.setattr(extras, "find_spec", lambda module: None if module == "sklearn" else object())
    assert extras.missing("qc") == ("scikit-learn",)


def test_missing_is_empty_when_every_package_resolves(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fully installed extra reports nothing missing and passes ``require`` silently."""
    monkeypatch.setattr(extras, "find_spec", lambda module: object())
    assert extras.missing("agent") == ()
    assert extras.is_installed("agent")
    extras.require("agent")  # must not raise


def test_require_reports_every_missing_package_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """A half-installed extra names ALL of its gaps in one error.

    Why: reporting one package at a time turns a single fix into a retry loop — install
    scikit-learn, rerun the build, discover sentence-transformers is also absent.
    """
    monkeypatch.setattr(extras, "find_spec", lambda module: None)
    with pytest.raises(QcRuntimeMissingError) as excinfo:
        extras.require("qc", required_by="the QC audit")

    message: str = str(excinfo.value)
    assert "scikit-learn" in message
    assert "sentence-transformers" in message
    assert 'pip install "tablassert[qc]"' in message


def test_require_qc_keeps_its_documented_error_code(monkeypatch: pytest.MonkeyPatch) -> None:
    """The QC gap still raises ``qc-runtime-missing``, not the generic code.

    Why: ``build_and_audit`` catches ``QcRuntimeMissingError`` by name and the slug is a
    documented, linkable identifier. Unifying the MESSAGE must not renumber the error.
    """
    monkeypatch.setattr(extras, "find_spec", lambda module: None)
    with pytest.raises(QcRuntimeMissingError) as excinfo:
        extras.require("qc")
    assert excinfo.value.code == "qc-runtime-missing"


def test_require_is_an_import_error_for_the_other_extras(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing non-QC extra raises ``MissingExtraError``, which IS an ``ImportError``.

    Why: existing ``except ImportError`` guards around lazy optional imports must keep catching
    it — a missing package is an import failure, however nicely it is worded.
    """
    monkeypatch.setattr(extras, "find_spec", lambda module: None)
    with pytest.raises(ImportError) as excinfo:
        extras.require("optimize")
    assert isinstance(excinfo.value, MissingExtraError)
    assert excinfo.value.extra == "optimize"
    assert excinfo.value.missing == ("dspy",)


def test_require_uses_find_spec_and_never_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    """The preflight probes without importing, so it is free to run on every invocation.

    Why: it guards the CLI entry points. Importing torch (via sentence-transformers) just to
    check the extra is present would add seconds to every ``build-kg --qc`` run.
    """
    probed: list[str] = []

    def _spy(module: str) -> object:
        probed.append(module)
        return object()

    monkeypatch.setattr(extras, "find_spec", _spy)
    monkeypatch.setattr(extras, "import_module", lambda module: pytest.fail(f"preflight imported {module}"))
    extras.require("qc")
    assert probed == ["sklearn", "sentence_transformers"]


def test_require_module_names_the_extra_for_an_unregistered_package() -> None:
    """An unknown module falls back to ``[agent]``, the only dynamic lazy-import path.

    Why: ``tablassert.agent`` calls ``_require`` with names the registry may not list. The
    fallback must still hand back a real install command instead of a bare import error.
    """
    with pytest.raises(MissingExtraError) as excinfo:
        extras.require_module("definitely_not_a_real_package_xyz_123")
    assert 'pip install "tablassert[agent]"' in str(excinfo.value)


def test_lazy_module_failure_names_the_extra() -> None:
    """A proxied optional import that is absent raises the actionable error, not ``No module named``.

    Why: :class:`LazyModule` is the LAST place that knows which module was wanted — by the time
    the caller sees the failure, the module name is all that is left of the context.
    """
    if extras.is_installed("optimize"):
        pytest.skip("dspy installed: the failure path is unreachable")

    with pytest.raises(MissingExtraError) as excinfo:
        LazyModule("dspy").LM  # noqa: B018 - attribute access is what triggers the import
    assert 'pip install "tablassert[optimize]"' in str(excinfo.value)


def test_lazy_module_reraises_untouched_for_unregistered_modules() -> None:
    """A non-optional module keeps its original ``ModuleNotFoundError``.

    Why: the registry only speaks for packages an extra actually ships. Dressing up an unrelated
    import failure as a missing extra would send users to install something irrelevant.
    """
    with pytest.raises(ModuleNotFoundError) as excinfo:
        LazyModule("definitely_not_a_real_package_xyz_123").anything  # noqa: B018
    assert not isinstance(excinfo.value, MissingExtraError)


def test_polars_import_failure_suggests_the_rt_extra() -> None:
    """A polars import failure points at ``[rt]``, the CPU-compatibility build.

    Why: polars is a CORE dependency, so it is never merely absent — the realistic failure is a
    wheel whose instruction set this CPU does not support, which is exactly what
    ``polars[rtcompat]`` fixes. It cannot be detected by ``find_spec`` (it imports as plain
    ``polars``), so this hint is the only place a user learns the extra exists.
    """
    error: MissingExtraError | None = extras.actionable_import_error("polars")
    assert error is not None
    assert error.extra == "rt"
    assert 'pip install "tablassert[rt]"' in str(error)
    assert "CPU" in str(error)


def test_install_commands_quote_the_extra() -> None:
    """Every suggested command quotes ``"tablassert[x]"``.

    Why: unquoted brackets are a glob pattern in zsh (a default shell on macOS), so
    ``pip install tablassert[qc]`` fails with ``no matches found`` before pip is ever reached.
    A suggestion the user's own shell rejects is worse than no suggestion.
    """
    for extra in extras.FEATURES:
        assert extras.install_command(extra) == f'pip install "tablassert[{extra}]"'
    assert extras.install_command("agent") == AGENT_EXTRA


def test_every_extra_message_carries_both_installers() -> None:
    """Each extra's message offers the pip AND uv commands.

    Why: Tablassert documents ``uv tool install`` as a first-class install path, and ``pip
    install`` inside a uv-managed tool environment silently does nothing useful.
    """
    for extra in extras.FEATURES:
        message: str = format_missing_extra(extra, "Something is missing.")
        assert f'pip install "tablassert[{extra}]"' in message
        assert f'uv tool install "tablassert[{extra}]"' in message
