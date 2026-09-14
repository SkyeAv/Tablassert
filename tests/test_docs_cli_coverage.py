"""Executable guardrail that keeps the rendered CLI documentation aligned with the live Cyclopts app.

The expected command/option surface is derived from ``tablassert.cli.APP`` and the registered command
callbacks rather than copied from the Markdown pages. These tests intentionally fail when a live flag,
alias, default, or agent environment variable is no longer mentioned in its documentation page.
"""

from __future__ import annotations

import inspect
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, get_args, get_origin, get_type_hints

import cyclopts  # pyright: ignore[reportMissingImports]

from tablassert import agent as agent_module
from tablassert.cli import APP

ROOT: Path = Path(__file__).resolve().parent.parent
DOCS: Path = ROOT / "docs"

COMMAND_NAME_DOCS: dict[str, tuple[str, ...]] = {
    "agent": ("agent.md",),
    "build-fullmap": ("cli.md", "fullmap.md"),
    "build-kg": ("cli.md",),
    "distill-export": ("cli.md",),
    "distill-weigh": ("cli.md",),
    "validate": ("cli.md",),
    "validate-kgx": ("cli.md",),
}
COMMAND_FLAG_DOCS: dict[str, tuple[str, ...]] = {
    "agent": ("agent.md", "cli.md"),
    "build-fullmap": ("cli.md", "fullmap.md"),
    "build-kg": ("cli.md",),
    "distill-export": ("cli.md",),
    "distill-weigh": ("cli.md",),
    "validate": ("cli.md",),
    "validate-kgx": ("cli.md",),
}


@dataclass(frozen=True)
class CliParameter:
    """One command callback parameter extracted from the live Cyclopts registration."""

    command: str
    python_name: str
    names: tuple[str, ...]
    default: object

    @property
    def flags(self) -> tuple[str, ...]:
        """Return explicit Cyclopts flags/aliases; generated positional aliases are documented positionally."""
        return tuple(name for name in self.names if name.startswith("-"))


def _doc_text(filename: str) -> str:
    """Read one documentation page relative to the repository root, matching existing docs tests."""
    return (DOCS / filename).read_text(encoding="utf-8")


def _live_command_callbacks() -> dict[str, Any]:
    """Enumerate user-facing commands from the live Cyclopts app, excluding meta flags like ``--help``.

    Cyclopts stores each registered command as a sub-``App``. Reading ``resolved_commands`` keeps this test
    tied to the command table users actually invoke, so adding/renaming a command changes the expected set.
    """
    callbacks: dict[str, Any] = {}
    for name, command_app in APP.resolved_commands().items():
        if name.startswith("-"):
            continue
        callback = command_app.default_command
        assert callback is not None, f"live command {name!r} has no callback"
        callbacks[name] = callback
    return callbacks


def _cyclopts_parameter(annotation: object) -> cyclopts.Parameter | None:
    """Return the ``cyclopts.Parameter`` metadata from an ``Annotated`` signature annotation, if present."""
    if get_origin(annotation) is not Annotated:
        return None
    for metadata in get_args(annotation)[1:]:
        if isinstance(metadata, cyclopts.Parameter):
            return metadata
    return None


def _live_cli_parameters() -> list[CliParameter]:
    """Derive command parameters, explicit aliases, and defaults from live callback signatures.

    This is the core guardrail: Markdown never defines the expected options. The registered Cyclopts
    callbacks do, through ``Annotated[..., cyclopts.Parameter(name=[...])]`` metadata and Python defaults.
    """
    parameters: list[CliParameter] = []
    for command, callback in sorted(_live_command_callbacks().items()):
        signature = inspect.signature(callback)
        type_hints = get_type_hints(callback, include_extras=True)
        for python_name, signature_parameter in signature.parameters.items():
            annotation = type_hints.get(python_name, signature_parameter.annotation)
            cyclopts_parameter = _cyclopts_parameter(annotation)
            raw_names = () if cyclopts_parameter is None else cyclopts_parameter.name
            names = (raw_names,) if isinstance(raw_names, str) else tuple(raw_names or ())
            parameters.append(CliParameter(command, python_name, names, signature_parameter.default))
    return parameters


def _token_pattern(token: str) -> re.Pattern[str]:
    """Match CLI/default tokens without accepting substrings like ``-m`` inside ``--model-id``."""
    if token.startswith("-"):
        return re.compile(rf"(?<![\w-]){re.escape(token)}(?![\w-])")
    return re.compile(rf"(?<![\w./-]){re.escape(token)}(?![\w./-])")


def _contains_token(text: str, token: str) -> bool:
    """Return whether a command, flag, alias, or default token appears as standalone documentation text."""
    return _token_pattern(token).search(text) is not None


def _pages_with_token(page_names: tuple[str, ...], token: str) -> list[str]:
    """Return documentation pages from ``page_names`` that contain ``token``."""
    return [page_name for page_name in page_names if _contains_token(_doc_text(page_name), token)]


def _flag_default_locations(page_names: tuple[str, ...], flags: tuple[str, ...], default_token: str) -> list[str]:
    """Return ``page:line`` locations where a live flag and its default are documented together."""
    locations: list[str] = []
    for page_name in page_names:
        for line_number, line in enumerate(_doc_text(page_name).splitlines(), start=1):
            if _contains_token(line, default_token) and any(_contains_token(line, flag) for flag in flags):
                locations.append(f"{page_name}:{line_number}")
    return locations


def _positional_documented(parameter: CliParameter) -> bool:
    """Return whether a positional argument is documented by metavar, snake_case, or prose spelling.

    Cyclopts also accepts generated option spellings for some positional parameters, but current docs present
    those arguments primarily as positionals. This still gates drift because a renamed callback parameter
    changes these derived tokens.
    """
    tokens = {
        parameter.python_name,
        parameter.python_name.replace("_", "-"),
        parameter.python_name.replace("_", " "),
        parameter.python_name.replace("_", "-").upper(),
    }
    if parameter.python_name.endswith("_ids"):
        tokens.add(parameter.python_name[:-1].replace("_", " "))
    pages = COMMAND_FLAG_DOCS[parameter.command]
    return any(token.lower() in _doc_text(page).lower() for token in tokens for page in pages)


def _documented_default_token(default: object) -> str | None:
    """Normalize user-visible defaults whose exact token must appear beside the option docs."""
    if default is inspect.Signature.empty or default is None or isinstance(default, bool):
        return None
    if isinstance(default, Path):
        value = default.as_posix()
        if value.startswith(".") or default.is_absolute():
            return value
        return f"./{value}"
    return str(default)


def _agent_env_vars() -> tuple[str, ...]:
    """Read the live agent model env-var constants without requiring the optional ``[agent]`` extra."""
    return tuple(
        sorted(
            value
            for name in dir(agent_module)
            if name.startswith("ENV_") and isinstance((value := getattr(agent_module, name)), str) and value.startswith("TABLASSERT_AGENT_")
        )
    )


def test_live_app_command_names_are_documented() -> None:
    """Every registered live command has a reachable documentation page mentioning the command name.

    If a command is added to ``tablassert.cli.APP`` without docs, this test fails because the command list is
    enumerated from the live Cyclopts app rather than from a hand-maintained expected list.
    """
    callbacks = _live_command_callbacks()
    assert set(callbacks) == set(COMMAND_NAME_DOCS), "update docs command-name coverage mapping for the live command set"
    assert set(callbacks) == set(COMMAND_FLAG_DOCS), "update docs flag coverage mapping for the live command set"
    for command, page_names in COMMAND_NAME_DOCS.items():
        assert _pages_with_token(page_names, command), f"docs missing live command {command!r} in {page_names}"


def test_app_version_flag_is_documented_in_cli_reference() -> None:
    """The top-level app ``--version`` flag is documented as a flag, not mistaken for a subcommand."""
    version_flags = tuple(APP.version_flags)
    assert "--version" in version_flags, "live Cyclopts app no longer exposes --version"
    cli_text = _doc_text("cli.md")
    for flag in version_flags:
        assert _contains_token(cli_text, flag), f"docs/cli.md missing app version flag {flag}"


def test_live_command_flags_aliases_and_positionals_are_documented() -> None:
    """Every explicit live Cyclopts flag/alias, plus every positional argument, is documented.

    Removing a real flag mention such as ``--map-threshold`` from ``docs/agent.md`` trips this assertion;
    adding a new ``cyclopts.Parameter(name=[...])`` in ``cli.py`` does the same until docs are updated.
    """
    for parameter in _live_cli_parameters():
        if not parameter.flags:
            assert _positional_documented(parameter), f"docs missing positional {parameter.command} {parameter.python_name!r}"
            continue
        pages = COMMAND_FLAG_DOCS[parameter.command]
        for flag in parameter.flags:
            assert _pages_with_token(pages, flag), f"docs missing live flag/alias {parameter.command} {flag} in {pages}"


def test_live_command_defaults_are_documented_on_the_flag_page() -> None:
    """Every user-visible non-boolean default appears on the same page as its live flag.

    Defaults are taken from the command callback signature, so changes like ``--max-steps`` moving from ``20``
    to another value fail until the corresponding docs page is updated.
    """
    for parameter in _live_cli_parameters():
        default_token = _documented_default_token(parameter.default)
        if default_token is None or not parameter.flags:
            continue
        pages = COMMAND_FLAG_DOCS[parameter.command]
        locations = _flag_default_locations(pages, parameter.flags, default_token)
        assert locations, f"docs missing default {default_token!r} beside {parameter.command} {parameter.flags}; checked {pages}"


def test_agent_model_environment_variables_are_documented() -> None:
    """The live agent model environment variable constants are documented for secret-free configuration."""
    env_vars = _agent_env_vars()
    assert env_vars, "no live TABLASSERT_AGENT_* environment variables found"
    pages = COMMAND_FLAG_DOCS["agent"]
    for env_var in env_vars:
        assert _pages_with_token(pages, env_var), f"docs missing agent environment variable {env_var} in {pages}"


def _positional_in_cli_md(parameter: CliParameter) -> bool:
    """Return whether a positional argument is documented in ``docs/cli.md`` specifically (SSOT check).

    Mirrors ``_positional_documented`` but scoped to ``cli.md`` alone, so the single-source-of-truth
    guarantee does not lean on ``agent.md`` or ``fullmap.md`` for the positional spelling.
    """
    tokens = {
        parameter.python_name,
        parameter.python_name.replace("_", "-"),
        parameter.python_name.replace("_", " "),
        parameter.python_name.replace("_", "-").upper(),
    }
    if parameter.python_name.endswith("_ids"):
        tokens.add(parameter.python_name[:-1].replace("_", " "))
    text = _doc_text("cli.md").lower()
    return any(token.lower() in text for token in tokens)


def test_cli_md_is_self_complete_for_every_command() -> None:
    """US-003: ``docs/cli.md`` is the single source of truth for the complete CLI flag tables.

    The per-command coverage mapping above lets agent flags live in ``agent.md`` and build-fullmap flags
    live in ``fullmap.md``. This stricter guard requires ``cli.md`` ITSELF to document every live command
    name, the app ``--version`` flag, and every flag/alias/positional/default — so a reader never has to
    leave the page for the full option surface. Removing any flag mention from ``cli.md`` trips this even
    when ``agent.md``/``fullmap.md`` still carry it.
    """
    cli_text = _doc_text("cli.md")
    # Every live command name and the app-level --version flag.
    for command in _live_command_callbacks():
        assert _contains_token(cli_text, command), f"docs/cli.md missing live command {command!r}"
    for flag in APP.version_flags:
        assert _contains_token(cli_text, flag), f"docs/cli.md missing app version flag {flag}"
    # Every flag/alias, positional, and user-visible default — all on cli.md itself.
    for parameter in _live_cli_parameters():
        if not parameter.flags:
            assert _positional_in_cli_md(parameter), f"docs/cli.md missing positional {parameter.command} {parameter.python_name!r}"
            continue
        for flag in parameter.flags:
            assert _contains_token(cli_text, flag), f"docs/cli.md missing live flag/alias {parameter.command} {flag}"
        default_token = _documented_default_token(parameter.default)
        if default_token is not None:
            locations = _flag_default_locations(("cli.md",), parameter.flags, default_token)
            assert locations, f"docs/cli.md missing default {default_token!r} beside {parameter.command} {parameter.flags}"
