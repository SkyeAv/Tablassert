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
    """Render a command's help page into a string (rich markup resolved to plain text)."""
    import io

    from rich.console import Console

    buffer = io.StringIO()
    cli.APP.help_print(tokens, console=Console(file=buffer, width=120, legacy_windows=False))
    return buffer.getvalue()


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
