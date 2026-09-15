"""Tests for US-008 model builders + INSTRUCTIONS + step_callback + build_agent + FakeModel.

The no-secret / env / INSTRUCTIONS / step-callback tests are PURE and run in the base
environment (no ``[agent]`` extra): ``build_model`` performs its secret check BEFORE the
lazy ``smolagents`` import, and ``make_step_callback`` is pure Python over duck-typed
objects. The smolagents-dependent tests (model construction, agent wiring, offline run)
call ``pytest.importorskip("smolagents")`` so they skip cleanly when the extra is absent.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tablassert.agent import (
    ENV_API_BASE,
    ENV_API_KEY,
    ENV_MODEL_ID,
    INSTRUCTIONS,
    build_agent,
    build_model,
    make_fake_model,
    make_step_callback,
    make_tools,
    resolve_model_config,
    validate_section,
    validate_table_config,
)
from tablassert.errors import LlmTransientError


@pytest.fixture(autouse=True)
def _offline_no_telemetry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep ``agent.run`` hermetically offline by disabling HuggingFace telemetry.

    Why: a real ``CodeAgent.run`` fires huggingface_hub telemetry that BLOCKS on a network
    call (observed: the run hangs indefinitely at zero CPU when telemetry is enabled). The
    standard opt-out vars make every ``agent.run`` in this module deterministic and
    network-free. Scoped to agent tests (NOT global conftest) so the QC suite's own
    HuggingFace model loading is unaffected.
    """
    monkeypatch.setenv("HF_HUB_DISABLE_TELEMETRY", "1")
    monkeypatch.setenv("DO_NOT_TRACK", "1")


# --------------------------------------------------------------------------- #
# PURE tests (base env; no importorskip)
# --------------------------------------------------------------------------- #


def test_build_model_fails_loud_without_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing model_id/api_base/api_key raises RuntimeError naming its env var (no secret).

    The check precedes the lazy smolagents import, so this passes even without the extra.
    """
    monkeypatch.delenv(ENV_MODEL_ID, raising=False)
    monkeypatch.delenv(ENV_API_BASE, raising=False)
    monkeypatch.delenv(ENV_API_KEY, raising=False)

    with pytest.raises(RuntimeError, match=ENV_MODEL_ID) as excinfo:
        build_model(None, None, None)
    message: str = str(excinfo.value).lower()
    assert "hardcode" in message or "secret" in message

    with pytest.raises(RuntimeError, match=ENV_API_BASE):
        build_model("m", None, None)
    with pytest.raises(RuntimeError, match=ENV_API_KEY):
        build_model("m", "http://x/v1", None)


def test_resolve_model_config_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """None args are filled from the TABLASSERT_AGENT_* env vars; explicit args win."""
    monkeypatch.setenv(ENV_MODEL_ID, "env-model")
    monkeypatch.setenv(ENV_API_BASE, "env-base")
    monkeypatch.setenv(ENV_API_KEY, "env-key")

    assert resolve_model_config(None, None, None) == ("env-model", "env-base", "env-key")
    assert resolve_model_config("a", "b", "c") == ("a", "b", "c")
    assert resolve_model_config("a", None, None) == ("a", "env-base", "env-key")


def test_instructions_content() -> None:
    """INSTRUCTIONS carries the data-fence markers, both exemplars, ReAct/plan, and final_answer."""
    assert "<<<PMC_DATA_BEGIN>>>" in INSTRUCTIONS
    assert "<<<PMC_DATA_END>>>" in INSTRUCTIONS
    assert "tutorial-table" in INSTRUCTIONS or "tutorial" in INSTRUCTIONS
    assert "ALAMV6" in INSTRUCTIONS
    assert "ReAct" in INSTRUCTIONS or "plan" in INSTRUCTIONS
    assert "final_answer" in INSTRUCTIONS


def test_instructions_carry_us006_derivation_guidance() -> None:
    """US-006 regression guard: every derivation-guidance block is present in the prompt.

    Covers (a) header-row detection -> row_slice + exact sheet name, (b) explode_by for delimited
    multi-entity cells, (c) prioritize breadth, (d) p_value capture + the effect_size/effect_type
    PAIR (invalid values become null; unpaired halves dropped with a warning per US-001), and
    (e) one section per mappable sheet.
    """
    # (a) header-row detection -> row_slice [start, auto] + exact sheet name
    assert "HEADERS + row_slice" in INSTRUCTIONS
    assert "row_slice: [<first data row>, auto]" in INSTRUCTIONS
    assert "EXACT sheet name" in INSTRUCTIONS
    # (b) explode_by for delimited multi-entity subject/object cells (common separators named)
    assert "explode_by" in INSTRUCTIONS
    for sep in ("`;`", "`|`", "`,`", "`/`"):
        assert sep in INSTRUCTIONS, f"explode_by guidance missing separator {sep}"
    # (c) prioritize lists name ALL plausible categories, not a single guess
    assert "EVERY plausible biolink Category" in INSTRUCTIONS
    # (d) statistics capture: p_value columns + the effect_size/effect_type PAIR
    assert "STATISTICS: capture p-value columns" in INSTRUCTIONS
    assert "adjusted_p_value" in INSTRUCTIONS
    assert "method: value, encoding: <statistic>" in INSTRUCTIONS
    assert "invalid values become null" in INSTRUCTIONS
    assert "dropped with a warning" in INSTRUCTIONS  # US-001 drop-with-warning pairing semantics
    # (e) one section per mappable sheet reinforced
    assert "ONE SECTION PER MAPPABLE SHEET" in INSTRUCTIONS.upper()
    # No stale pre-US-001 pairing claims survive anywhere in the prompt.
    assert "mandatory pair" not in INSTRUCTIONS.lower()
    assert "hard validation error" not in INSTRUCTIONS.lower()


def test_instructions_annotations_bullet_excludes_relationship_strength() -> None:
    """The ANNOTATIONS bullet must NOT list `relationship_strength` among StudyResult-rerouted names.

    Why: `relationship_strength` is a legacy ALIAS that `coerce.coerced_target` routes to
    `effect_size` (pinned by tests/test_lib.py), a real, satisfiable edge slot: the relocation
    validator keeps effect_size ON the edge, and pairing treats it as the effect_size half.
    Listing it among annotation names whose values are rerouted into an inlined StudyResult
    contradicts that routing and teaches the agent its value
    is lost when it actually reaches the edge. This guard stops a future prompt edit from
    re-adding the alias to the bullet. `supporting_study_size`, `sample_size` and the other
    `supporting_study_*` names genuinely have NO satisfiable target, so they must stay named.
    """
    region = INSTRUCTIONS.split("ANNOTATIONS must name", 1)[1].split("MULTIVALUED", 1)[0]
    assert "relationship_strength" not in region
    assert "supporting_study_size" in region
    assert "sample_size" in region


def test_step_callback_tallies() -> None:
    """make_step_callback tallies steps/tokens/tool-call quality over duck-typed steps (pure)."""
    metrics: dict[str, object] = {}
    cb = make_step_callback(metrics)
    agent = SimpleNamespace(memory=None)

    step = SimpleNamespace(
        error=None,
        tool_calls=[SimpleNamespace(name="build_and_audit", arguments={"config_yaml": "x"})],
        observations="ok",
        token_usage=SimpleNamespace(input_tokens=10, output_tokens=5, total_tokens=15),
        is_final_answer=False,
    )
    cb(step, agent)
    assert metrics["steps"] == 1
    assert metrics["total_tool_calls"] == 1
    assert metrics["input_tokens"] == 10
    assert metrics["output_tokens"] == 5
    assert metrics["total_tokens"] == 15

    # Same tool-call signature again -> redundant.
    cb(step, agent)
    assert metrics["redundant_tool_calls"] == 1

    # Observations that look like an error/traceback -> wrong.
    bad = SimpleNamespace(error=None, tool_calls=[], observations="Traceback: Error", token_usage=None, is_final_answer=False)
    cb(bad, agent)
    assert metrics["wrong_tool_calls"] >= 1  # type: ignore[operator]

    # step.error set -> failed.
    failed = SimpleNamespace(error="boom", tool_calls=[], observations=None, token_usage=None, is_final_answer=False)
    cb(failed, agent)
    assert metrics["failed_tool_calls"] == 1


# --------------------------------------------------------------------------- #
# smolagents-dependent tests (require the [agent] extra; skip cleanly when absent)
# --------------------------------------------------------------------------- #


def test_build_model_constructs_offline() -> None:
    """build_model constructs OpenAI/LiteLLM models offline (no network at construction)."""
    pytest.importorskip("smolagents")
    model = build_model("test-model", "http://localhost:9/v1", "sk-test")
    assert "OpenAI" in type(model).__name__
    lite = build_model("test-model", "http://localhost:9/v1", "sk-test", backend="litellm")
    assert "LiteLLM" in type(lite).__name__


def _retrying_model_fixture(
    monkeypatch: pytest.MonkeyPatch, failures: list[BaseException], *, model_id: str = "test-model"
) -> tuple[Any, list[float], Any]:
    """Build a scripted smolagents model and retry wrapper with no real sleeps."""
    pytest.importorskip("smolagents")
    import contextlib

    from smolagents.models import ChatMessage, MessageRole, Model  # pyright: ignore[reportMissingImports]

    class ScriptedModel(Model):  # pyright: ignore[reportMissingImports]
        def __init__(self) -> None:
            with contextlib.suppress(Exception):
                super().__init__(model_id=model_id)
            self.calls: int = 0

        def generate(self, messages: list[Any], **kwargs: Any) -> Any:  # type: ignore[override]
            self.calls += 1
            if failures:
                raise failures.pop(0)
            return ChatMessage(role=MessageRole.ASSISTANT, content="ok")

    from tablassert.agent import make_retrying_model

    sleeps: list[float] = []
    raw: Any = ScriptedModel()
    wrapped: Any = make_retrying_model(raw, attempts=4, base_delay=1.0, max_delay=20.0, max_total_backoff=45.0, sleep=sleeps.append, rng=lambda: 0.0)
    return raw, sleeps, wrapped


def test_retrying_model_returns_first_success_without_sleeping(monkeypatch: pytest.MonkeyPatch) -> None:
    """A first-attempt success delegates directly: one call, zero sleeps, no message copying."""
    pytest.importorskip("smolagents")
    from smolagents.models import ChatMessage, MessageRole  # pyright: ignore[reportMissingImports]

    raw, sleeps, wrapped = _retrying_model_fixture(monkeypatch, [])
    messages: list[Any] = [ChatMessage(role=MessageRole.USER, content="hello")]
    response: Any = wrapped.generate(
        messages, stop_sequences=["END"], response_format={"type": "text"}, tools_to_call_from=["tool"], custom_flag=True
    )
    assert response.content == "ok"
    assert response is not None
    assert raw.calls == 1
    assert sleeps == []
    assert wrapped.model_id == "test-model"


def test_retrying_model_retries_transient_503_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A transient 503 retries through the shared seam, with injected sleeps and no wall-clock delay."""
    from email.message import Message
    from urllib.error import HTTPError

    raw, sleeps, wrapped = _retrying_model_fixture(monkeypatch, [HTTPError("https://gateway/x", 503, "Unavailable", Message(), None)])
    response: Any = wrapped.generate([])
    assert response.content == "ok"
    assert raw.calls == 2
    assert sleeps == [0.5], "equal jitter with rng=0 uses half of the one-second schedule"


def test_retrying_model_raises_llm_transient_after_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exhaustion raises the coded LLM error, naming only model_id (never an API key)."""
    from email.message import Message
    from urllib.error import HTTPError

    failures: list[BaseException] = [HTTPError("https://gateway/x", 503, "Unavailable", Message(), None)] * 4
    _raw, sleeps, wrapped = _retrying_model_fixture(monkeypatch, failures, model_id="gateway/model")
    with pytest.raises(LlmTransientError) as exc_info:
        wrapped.generate([])
    error: LlmTransientError = exc_info.value
    assert error.code == "llm-transient"
    assert error.target == "gateway/model"
    assert error.attempts == 4
    assert "sk-" not in str(error)
    assert len(sleeps) == 3


def test_retrying_model_does_not_retry_permanent_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A permanent error propagates unwrapped on its first call and never sleeps."""
    raw, sleeps, wrapped = _retrying_model_fixture(monkeypatch, [ValueError("bad request")])
    with pytest.raises(ValueError, match="bad request"):
        wrapped.generate([])
    assert raw.calls == 1
    assert sleeps == []


def test_retrying_model_delegates_attributes_and_generate_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    """The wrapper exposes provider attributes and forwards every generate keyword verbatim."""
    pytest.importorskip("smolagents")
    from tablassert.agent import make_retrying_model

    class Plain:
        model_id: str = "plain"
        supports_stop_parameter: bool = True
        custom_provider_attribute: str = "provider-value"

        def __init__(self) -> None:
            self.received: tuple[Any, dict[str, Any]] | None = None

        def generate(self, messages: Any, **kwargs: Any) -> Any:
            self.received = (messages, kwargs)
            return "response"

    raw: Plain = Plain()
    wrapped: Any = make_retrying_model(raw, sleep=lambda delay: None, rng=lambda: 0.0)
    messages: list[Any] = ["message"]
    assert wrapped.generate(messages, stop_sequences=["x"], response_format={"a": "b"}, tools_to_call_from=["t"], extra="value") == "response"
    assert raw.received == (messages, {"stop_sequences": ["x"], "response_format": {"a": "b"}, "tools_to_call_from": ["t"], "extra": "value"})
    assert wrapped.supports_stop_parameter is True
    assert wrapped.custom_provider_attribute == "provider-value"
    raw.flatten_messages_as_text = True  # type: ignore[attr-defined]
    wrapped = make_retrying_model(raw, sleep=lambda delay: None, rng=lambda: 0.0)
    assert wrapped.flatten_messages_as_text is True


def test_retrying_model_sanitizes_model_target_and_error_text(monkeypatch: pytest.MonkeyPatch) -> None:
    """An API-key-shaped model id or provider error cannot leak into the coded failure message."""
    import io

    pytest.importorskip("smolagents")
    from smolagents.models import Model  # pyright: ignore[reportMissingImports]

    import tablassert.net as net
    from tablassert.agent import make_retrying_model

    raw = Model()
    raw.model_id = "sk-live-secret-value"

    class ServiceUnavailableError(Exception):
        pass

    raw.generate = lambda messages, **kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
        ServiceUnavailableError("POST https://gateway/models/sk-live-secret-value -> 503: api_key=api-live-secret-value")
    )
    logs: io.StringIO = io.StringIO()

    class Logger:
        def warning(self, message: str, **kwargs: object) -> None:
            logs.write(message.format(**kwargs) + "\n")

        def error(self, message: str, **kwargs: object) -> None:
            logs.write(message.format(**kwargs) + "\n")

    monkeypatch.setattr(net, "logger", Logger())
    wrapped: Any = make_retrying_model(raw, sleep=lambda delay: None, rng=lambda: 0.0, secrets=("api-live-secret-value",))
    with pytest.raises(LlmTransientError) as exc_info:
        wrapped.generate([])
    message: str = str(exc_info.value)
    assert exc_info.value.target == "llm"
    assert "sk-live-secret-value" not in message
    assert "api-live-secret-value" not in message
    assert "[REDACTED]" in message
    assert "sk-live-secret-value" not in logs.getvalue()
    assert "api-live-secret-value" not in logs.getvalue()


def test_build_model_disables_smolagents_and_openai_retry_layers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raw provider models retain their type but have smolagents/OpenAI retries disabled."""
    pytest.importorskip("smolagents")
    model: Any = build_model("test-model", "http://localhost:9/v1", "sk-test")
    assert model.retryer.max_attempts == 1
    assert model.client.max_retries == 0
    lite: Any = build_model("test-model", "http://localhost:9/v1", "sk-test", backend="litellm")
    assert "LiteLLM" in type(lite).__name__
    assert lite.retryer.max_attempts == 1


def test_make_tools_full_mode_ships_the_four_tool_surface(tmp_path: Path) -> None:
    """US-002: full mode assembles EXACTLY the derive→build→answer surface, in order.

    The LLM no longer sees coverage tools: map_coverage/propose_config_edit stay pure helpers
    of the deterministic supervisor, so the agent's whole job is derive_config →
    build_and_audit (fixing only coded build errors) → final_answer. The derive modes are
    public API and stay exactly as before.
    """
    pytest.importorskip("smolagents")
    fullmap: Path = tmp_path / "fullmap.redb"

    full: list[Any] = make_tools(fullmap=fullmap, derive_mode="full")
    assert [tool.name for tool in full] == ["read_table", "pmc_article_context", "derive_config", "build_and_audit"]

    derive_only: list[Any] = make_tools(fullmap=fullmap, derive_mode="derive_only")
    assert [tool.name for tool in derive_only] == ["read_table", "pmc_article_context", "derive_config"]

    derive_coverage: list[Any] = make_tools(fullmap=fullmap, derive_mode="derive_coverage")
    assert [tool.name for tool in derive_coverage] == ["read_table", "pmc_article_context", "derive_config", "map_coverage"]


def test_build_agent_wires_checks_and_callback() -> None:
    """build_agent wires validate_table_config into final_answer_checks and a default step callback."""
    pytest.importorskip("smolagents")
    agent = build_agent(model=make_fake_model(), tools=[])
    assert agent is not None
    checks: Any = getattr(agent, "final_answer_checks", [])
    assert validate_table_config in checks
    assert getattr(agent, "step_callbacks", None) is not None


def test_fake_model_drives_agent_run_offline() -> None:
    """A FakeModel-driven agent.run reaches a final answer offline (no network, terminates)."""
    pytest.importorskip("smolagents")
    from smolagents import LogLevel  # pyright: ignore[reportMissingImports]

    agent: Any = build_agent(model=make_fake_model(), tools=[], max_steps=3, verbosity_level=LogLevel.ERROR)
    result = agent.run("Derive a Tablassert config.")
    # smolagents 1.26.0 returns an AgentText (a str subclass) holding the final answer.
    assert result is not None
    assert str(result).strip()
    assert validate_section(str(result)) is True


def test_instructions_carry_a_generated_predicate_cheatsheet() -> None:
    """The prompt teaches predicate<->class legality, and does so from the INSTALLED model.

    The ~30 KB Section schema the derive_config tool injects lists all predicates and all categories
    as flat enums with nothing tying the two together, which is how the agent came to recommend
    `gene_associated_with_condition` for gene~disease -- a predicate GeneToDiseaseAssociation forbids.
    """
    from tablassert.agent import predicate_cheatsheet

    # Fully rendered: no template placeholder survives into the live prompt.
    assert "{{PREDICATE_CHEATSHEET}}" not in INSTRUCTIONS
    assert predicate_cheatsheet() in INSTRUCTIONS

    # The flagship pair, generated from biolink-model rather than hand-written.
    assert "Gene ~ Disease -> GeneToDiseaseAssociation: affects, associated_with, contributes_to" in INSTRUCTIONS
    assert "demoted_edge_pct" in INSTRUCTIONS

    # And the relocation/disabled-field rules the pipeline enforces.
    assert "supporting_study_size" in INSTRUCTIONS  # named as a slot that does NOT reach the edge
    assert "adjusted_p_value" in INSTRUCTIONS  # the recommended alternative
    assert "species_context_qualifier` is" in INSTRUCTIONS
    assert "species_context_qualifier` is\n  auto-derived" not in INSTRUCTIONS


def test_instructions_do_not_recommend_a_class_forbidden_predicate() -> None:
    """Every predicate the prompt shows in an exemplar must be legal for that exemplar's pair."""
    import re

    from tablassert.lib import predicate_options

    # Exemplar (a) is gene~disease; whatever predicate it demonstrates must keep the class.
    exemplar: str = INSTRUCTIONS[INSTRUCTIONS.index("# (a) tutorial-table") : INSTRUCTIONS.index("# (b) ALAMV6")]
    match = re.search(r"predicate:\s*(\w+)", exemplar)
    assert match is not None
    legal = predicate_options("Gene", "Disease")
    assert legal is not None
    assert f"biolink:{match.group(1)}" in legal


def test_instructions_carry_detail_first_hardening() -> None:
    """Regression guard for the detail-first hardening: the prompt teaches the four struggle knobs.

    (a) goals are ordered breadth+detail FIRST and efficiency LAST; (b) the REGEX COOKBOOK teaches
    Rust-regex semantics (no backreferences/lookarounds) and single-quoted YAML patterns; (c)
    qualifiers get POSITIVE guidance (direction/aspect, nullable) not only bans; (d) predicate
    specificity is an explicit rule with predicate_advice named as the fix; (e) exemplar (d)
    demonstrates explode_by + a qualifier + regex + the statistical pair.
    """
    # (a) detail-first goal ordering
    assert "BREADTH + DETAIL" in INSTRUCTIONS
    assert "efficiency is scored LAST" in INSTRUCTIONS
    # (b) regex cookbook
    assert "# REGEX COOKBOOK" in INSTRUCTIONS
    assert "NO backreferences" in INSTRUCTIONS
    assert "NO lookarounds" in INSTRUCTIONS
    assert "SINGLE quotes" in INSTRUCTIONS
    # (c) positive qualifier guidance
    assert "object_direction_qualifier" in INSTRUCTIONS
    assert "object_aspect_qualifier" in INSTRUCTIONS
    assert "nullable: true" in INSTRUCTIONS
    assert "qualified_predicate: biolink:causes" in INSTRUCTIONS
    # (d) predicate specificity + actionable audit feedback
    assert "MOST-SPECIFIC predicate" in INSTRUCTIONS
    assert "predicate_advice" in INSTRUCTIONS
    assert "multivalued_suspects" in INSTRUCTIONS
    # (e) the rich exemplar (bounded to its own block, before the next `## ` section)
    start: int = INSTRUCTIONS.index("# (d) RICH")
    exemplar_d: str = INSTRUCTIONS[start : INSTRUCTIONS.index("\n## ", start)]
    assert 'explode_by: ";"' in exemplar_d
    assert "object_direction_qualifier" in exemplar_d
    assert "regex:" in exemplar_d
    assert "effect_size" in exemplar_d
    assert "effect_type" in exemplar_d


def test_instructions_exemplar_d_is_schema_valid() -> None:
    """Exemplar (d) is not decorative: it must validate against the Section schema it teaches."""
    import re

    from tablassert.agent import table_config_error

    # The exemplar runs from its comment header to the next `## ` section of the prompt.
    start: int = INSTRUCTIONS.index("# (d) RICH")
    end: int = INSTRUCTIONS.index("\n## ", start)
    exemplar_d: str = INSTRUCTIONS[start:end]
    # The exemplar body starts at the first `source:` line; comment lines (# ...) are prose.
    body: str = "\n".join(line for line in exemplar_d.splitlines() if not line.startswith("#"))
    body = body[body.index("source:") :]
    assert table_config_error(body) is None
    # And its predicate must be legal for its gene~disease pair (same guard as exemplar (a)).
    from tablassert.lib import predicate_options

    match = re.search(r"predicate:\s*(\w+)", body)
    assert match is not None
    legal = predicate_options("Gene", "Disease")
    assert legal is not None
    assert f"biolink:{match.group(1)}" in legal
