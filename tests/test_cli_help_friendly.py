"""Agent-friendliness guarantees over the rendered CLI help surface.

An LLM agent invoking ``tablassert <command> --help`` must be able to pick flags and
values from help alone: every parameter carries a description, closed vocabularies render
as choices and are rejected at parse time, no phantom flags (like cyclopts' ``--empty-*``
list negations) appear, and every long flag the help advertises actually binds (no
app-level shadow). Each test below pins one of those guarantees; when one fails the CLI
has drifted back toward "read the source to use it".
"""

from typing import get_args, get_type_hints

import pytest
from cyclopts.exceptions import CoercionError

from tablassert import cli
from tablassert.distill_reward import POLICIES


def render_help(tokens: list[str]) -> str:
    """Render a command's help page as wrap-insensitive plain text.

    Rich wraps the panel at the console width, which would otherwise split asserted
    phrases across lines; collapsing all whitespace makes the substring checks stable.
    """
    import io

    from rich.console import Console

    buffer = io.StringIO()
    cli.APP.help_print(tokens, console=Console(file=buffer, width=120, legacy_windows=False))
    # Drop the help panel's box-drawing borders, then collapse whitespace (rich wraps at
    # the console width), so substring assertions are stable against rewrapping.
    return " ".join(buffer.getvalue().replace("│", " ").split())


def test_distill_weigh_policy_literal_matches_policies() -> None:
    """``--policy``'s Literal must mirror ``distill_reward.POLICIES`` exactly.

    Why: the Literal is written inline so the CLI stays import-light (no module-level
    distill_reward import), which makes help/choices drift possible when POLICIES changes.
    This parity check is the lockstep guard; a mismatch means help lies about the accepted
    values or parse rejects a value the reward layer supports.
    """
    hints = get_type_hints(cli.distill_weigh)
    literal_values = set(get_args(hints["policy"]))
    assert literal_values == set(POLICIES)
    assert get_args(hints["policy"])  # the annotation really is a Literal, not Any


def test_distill_weigh_help_documents_every_flag() -> None:
    """Every distill-weigh flag renders with a description in ``--help``.

    Why: this command previously rendered bare flag rows with a one-line docstring, so an
    agent had to read the source to learn what --policy/--edge-ref/--final-call-only do.
    """
    text = render_help(["distill-weigh"])
    for flag in (
        "--distill-dir",
        "--out",
        "--policy",
        "--threshold",
        "--top-n",
        "--replication-k",
        "--reward-config",
        "--edge-ref",
        "--purpose",
        "--final-call-only",
        "--manifest",
    ):
        assert flag in text, f"help page lost the {flag} row"
    # The load-bearing semantics an agent cannot guess: the three policies, the dynamic
    # purpose vocabulary and its failure mode, and the final-call retention guarantee.
    assert "threshold" in text
    assert "best-of-n" in text
    assert "replication" in text
    assert "exit 2" in text
    assert "call_index" in text
    assert "manifest.json" in text


def test_distill_weigh_policy_rejects_unknown_value_at_parse_time() -> None:
    """A bad ``--policy`` fails during parsing with a choices error, not at reward time.

    Why: the pre-Literal behavior surfaced the error only after records were loaded and
    joined; parse-time rejection is the fastest possible feedback loop and names the valid
    values in the error itself.
    """
    with pytest.raises(CoercionError, match="best-of-n"):
        cli.APP.parse_args(["distill-weigh", "--distill-dir", "x", "--out", "y", "--policy", "bogus"], exit_on_error=False)


def test_build_kg_help_documents_the_build_mode_flags() -> None:
    """build-kg renders a description for every build-mode flag.

    Why: this command previously rendered bare ``RELEASE --release -r`` rows with no
    description, so an agent could not tell --release (slim, significant-only graph) from
    --head (5-row preview) without reading the source or the docs site.
    """
    text = render_help(["build-kg"])
    assert "Graph YAML" in text
    assert "not_significant" in text  # --release's concrete effect
    assert "five rows" in text  # --head's sample size
    assert "verbose per-section logging" in text
    assert "[qc]" in text


def test_validate_help_documents_the_schema_choice() -> None:
    """validate renders what each --schema value checks.

    Why: the two schema values run different pipelines (graph validates referenced tables
    too); without per-flag help an agent cannot predict what it is about to run.
    """
    text = render_help(["validate"])
    assert "every referenced table" in text
    assert "section syntax" in text


def test_validate_kgx_help_documents_the_failure_example_cap() -> None:
    """validate-kgx renders what --limit caps (examples, not validation coverage).

    Why: a caller could reasonably read ``--limit`` as 'validate only the first N records';
    the help must state that every record is validated and only the examples are capped.
    """
    text = render_help(["validate-kgx"])
    assert "example failures" in text
    assert "every record is still validated" in text
    assert ".nodes.ndjson" in text
    assert ".edges.ndjson" in text


def test_validate_kgx_help_documents_the_prune_flag() -> None:
    """validate-kgx renders what --prune rewrites (edges in place; strict bar).

    Why: --prune has destructive power (it rewrites a build artifact in place), so an
    agent must be able to learn from --help alone that it drops every non-strictly-
    valid edge INCLUDING the deliberate pending carryovers, never touches nodes, and
    leaves a clean file untouched -- without reading the source or docs.
    """
    text = render_help(["validate-kgx"])
    assert "--prune" in text
    assert "pending carryovers" in text
    assert "nodes file is never modified" in text
    assert "clean edges file is left untouched" in text


ALL_COMMANDS = ["agent", "build-fullmap", "build-kg", "distill-export", "distill-weigh", "quick-map", "validate", "validate-kgx"]


def test_no_help_page_renders_a_phantom_empty_flag() -> None:
    """No rendered help contains cyclopts' auto --empty-* list-negation rows.

    Why: cyclopts auto-generates an ``--empty-<name>`` reset flag for every list
    parameter and renders it as its own help row; on ``agent`` the row even carried
    ``[required]``, reading as a real flag an agent must pass. List params now opt out
    via ``Parameter(negative="")``, and this sweeps every command so a newly added list
    parameter cannot reintroduce the noise.
    """
    for command in ALL_COMMANDS:
        text = render_help([command])
        assert "--empty-" not in text, f"{command} --help renders a phantom --empty-* row"


def test_build_fullmap_babel_version_flag_binds_and_the_old_shadow_is_documented() -> None:
    """--babel-version selects the snapshot; the stale --version form stays app-owned.

    Why: the app-level --version flag shadowed build-fullmap's --version, so
    `build-fullmap --version <snapshot>` printed the app version and exited 0 without
    running anything. The rename gives the command a long flag that actually binds; the
    remaining --version behavior (print the package version - by cyclopts design the app
    version flag answers anywhere in the command chain) is pinned here and documented in
    docs/cli.md's warning block so the old form's result is never mistaken for a build.
    """
    fn, bound, _ = cli.APP.parse_args(["build-fullmap", "--babel-version", "2026aug22"], exit_on_error=False)
    assert fn is cli.build_fullmap
    assert dict(bound.arguments) == {"version": "2026aug22"}

    fn, bound, _ = cli.APP.parse_args(["build-fullmap", "-v", "2026aug22"], exit_on_error=False)
    assert fn is cli.build_fullmap
    assert dict(bound.arguments) == {"version": "2026aug22"}

    # The stale form resolves to the app's version printer (a per-call App view), never
    # to build_fullmap; compare by function name since the view's binding differs.
    fn, bound, _ = cli.APP.parse_args(["build-fullmap", "--version"], exit_on_error=False)
    assert getattr(fn, "__name__", "") == "version_print"

    text = render_help(["build-fullmap"])
    assert "--babel-version" in text
