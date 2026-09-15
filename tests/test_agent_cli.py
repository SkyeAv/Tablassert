"""US-010: the ``tablassert agent`` cyclopts subcommand (wiring only, no live model).

Every test here runs in the BASE environment (no ``importorskip``): the command lazy-imports
``tablassert.agent`` (which never eagerly imports smolagents), the no-secret path fails BEFORE any
model is built, and the forwarding path monkeypatches ``run_supervisor``/``build_model`` so no real
agent or network call ever fires.
"""

from __future__ import annotations

import os
import sys
import tempfile
import types
from pathlib import Path

import pytest
import yaml
from cyclopts.exceptions import UnknownOptionError  # pyright: ignore[reportMissingImports]

from tablassert import extras
from tablassert.agent import ENV_API_BASE, ENV_API_KEY, ENV_MODEL_ID, load_optimized_instructions, save_optimized_instructions
from tablassert.cli import APP, agent
from tablassert.errors import MissingExtraError


def _graph_path() -> Path:
    """Return a valid caller-owned target graph for CLI wiring tests."""
    path: Path = Path(tempfile.gettempdir()) / f"tablassert-agent-cli-target-{os.getpid()}.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": "CLI_TARGET",
                "version": "1.0.0",
                "tables": [],
                "fullmap": "/tmp/fm",
                "rig": {
                    "source_info": {
                        "infores_id": "infores:cli-target",
                        "terms_of_use_info": {"license_name": "CC0"},
                        "data_access_locations": ["Test - https://example.org/data"],
                        "source_status": "unknown",
                    },
                    "ingest_info": {"utility": "CLI test.", "scope": "CLI test."},
                    "provenance_info": {"contributions": ["Test"]},
                    "artifact_base_url": "https://example.org/cli-target",
                    "artifact_base_path": "/tmp/cli-target-output",
                },
            },
            sort_keys=False,
        )
    )
    return path


@pytest.fixture(autouse=True)
def _extras_present(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report every optional extra as installed for this module's wiring tests.

    Why: ``tablassert agent`` preflights the ``[agent]``/``[optimize]`` extras, but these tests
    monkeypatch ``run_supervisor``/``run_gepa`` so no smolagents or dspy code ever runs — and CI
    installs neither extra. Stubbing the probe keeps each test about the flag plumbing it was
    written for. The preflight has its own tests below, which re-stub over this fixture.
    """
    monkeypatch.setattr(extras, "missing", lambda extra: ())


def test_agent_command_registered() -> None:
    """The ``agent`` subcommand is registered on the shared CLI ``APP``.

    Why: ``tablassert agent`` must be a discoverable peer of ``build-kg``/``validate``. Asserting
    against cyclopts' resolved-command mapping proves the ``@APP.command(name="agent")`` decorator
    actually attached it (a direct registration check, not a fragile ``--help`` scrape).
    """
    assert "agent" in APP.resolved_commands()


def test_agent_no_secret_fails_loud(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """With no model config anywhere, ``agent`` exits 2 and names the missing env var on stderr.

    Why: a secret is never defaulted or hardcoded. With the three ``TABLASSERT_AGENT_*`` vars unset
    and no flags given, the command must fail LOUD (``SystemExit(2)``) naming the model-id env var and
    the no-hardcode policy — and it must do so BEFORE building a model, so this path never touches
    smolagents (proving the secret check precedes any extra-triggering call).
    """
    monkeypatch.delenv(ENV_MODEL_ID, raising=False)
    monkeypatch.delenv(ENV_API_BASE, raising=False)
    monkeypatch.delenv(ENV_API_KEY, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        agent(["PMC1"], graph_configuration_file=_graph_path())
    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert ENV_MODEL_ID in captured.err
    assert "secret" in captured.err.lower() or "hardcode" in captured.err.lower()


def _set_model_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Satisfy the secret checks so a test can reach the checks that come after them."""
    monkeypatch.setenv(ENV_MODEL_ID, "env-model")
    monkeypatch.setenv(ENV_API_BASE, "env-base")
    monkeypatch.setenv(ENV_API_KEY, "env-key")


def test_agent_without_extra_names_the_install_command(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without the ``[agent]`` extra the command stops up front and prints the install command.

    Why: smolagents is otherwise only imported per-article inside ``build_agent``, so a user missing
    the extra would download an article from PMC before learning they cannot run at all. The preflight
    must name the absent distribution AND the exact pip command — a bare ``ModuleNotFoundError:
    smolagents`` does not tell anyone that ``tablassert[agent]`` is the fix.
    """
    _set_model_env(monkeypatch)
    monkeypatch.setattr(extras, "missing", lambda extra: ("smolagents",) if extra == "agent" else ())
    monkeypatch.setattr("tablassert.agent.run_supervisor", lambda *a, **k: pytest.fail("supervisor ran without the extra"))

    with pytest.raises(MissingExtraError) as excinfo:
        agent(["PMC1"], graph_configuration_file=_graph_path())

    message: str = str(excinfo.value)
    assert "smolagents" in message
    assert 'pip install "tablassert[agent]"' in message
    assert excinfo.value.extra == "agent"


def test_agent_optimize_without_optimize_extra_points_at_optimize(monkeypatch: pytest.MonkeyPatch) -> None:
    """``--optimize`` on an agent-ready install points at ``[optimize]``, not ``[agent]``.

    Why: dspy powers only the GEPA path and ships in its own extra. Naming ``[agent]`` here would
    send a user who already has it in a circle.
    """
    _set_model_env(monkeypatch)
    monkeypatch.setattr(extras, "missing", lambda extra: ("dspy",) if extra == "optimize" else ())
    monkeypatch.setattr("tablassert.agent.run_gepa", lambda *a, **k: pytest.fail("GEPA ran without the extra"))

    with pytest.raises(MissingExtraError) as excinfo:
        agent(["PMC1"], graph_configuration_file=_graph_path(), optimize=True)

    message: str = str(excinfo.value)
    assert 'pip install "tablassert[optimize]"' in message
    assert "tablassert[agent]" not in message


def test_agent_missing_secret_is_reported_before_missing_extra(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """With BOTH a missing secret and a missing extra, the secret is reported first.

    Why: the flag/secret checks are about the command the user just typed; the extras preflight is
    about their environment. Fixing an install only to be told the model id was never set is a worse
    loop than the reverse, so the ordering is pinned.
    """
    for env in (ENV_MODEL_ID, ENV_API_BASE, ENV_API_KEY):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setattr(extras, "missing", lambda extra: ("smolagents",))

    with pytest.raises(SystemExit) as exc_info:
        agent(["PMC1"], graph_configuration_file=_graph_path())

    assert exc_info.value.code == 2
    assert ENV_MODEL_ID in capsys.readouterr().err


def test_agent_env_fallback_and_forwarding(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """Model config resolves from env and every knob is forwarded to the supervisor verbatim.

    Why: the CLI is thin glue over ``run_supervisor``. With the three env vars set (and no flags),
    ``resolve_model_config`` must fill the model config from the environment, and the thresholds /
    target graph/path must reach the supervisor unchanged. ``run_supervisor`` and
    ``build_model`` are monkeypatched (module attributes the command looks up at call time) so no real
    agent runs; invoking the forwarded ``build_model_factory`` then proves the factory resolved the env
    config and handed it to ``build_model``.
    """
    monkeypatch.setenv(ENV_MODEL_ID, "env-model")
    monkeypatch.setenv(ENV_API_BASE, "env-base")
    monkeypatch.setenv(ENV_API_KEY, "env-key")

    captured: dict[str, object] = {}

    def fake_run_supervisor(pmc_ids: list[str], **kwargs: object) -> dict[str, object]:
        captured["pmc_ids"] = pmc_ids
        captured.update(kwargs)
        return {"records": {}, "metrics": {"mapped": 0, "skipped": 0}}

    build_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    retry_calls: list[dict[str, object]] = []

    def fake_build_model(*args: object, **kwargs: object) -> object:
        build_calls.append((args, kwargs))
        return object()

    def fake_retrying_model(model: object, **kwargs: object) -> object:
        retry_calls.append(kwargs)
        return model

    monkeypatch.setattr("tablassert.agent.run_supervisor", fake_run_supervisor)
    monkeypatch.setattr("tablassert.agent.build_model", fake_build_model)
    # The retry wrapper is faked for the same reason build_model is: the real one subclasses the
    # smolagents Model base, which _requires the [agent] extra CI never installs. The passthrough
    # still lets us assert the CLI forwards the resolved API key as `secrets`.
    monkeypatch.setattr("tablassert.agent.make_retrying_model", fake_retrying_model)

    agent(["PMC1", "PMC2"], graph_configuration_file=_graph_path(), map_threshold=0.7, max_improve_iters=5)

    assert captured["pmc_ids"] == ["PMC1", "PMC2"]
    target_graph = captured["graph"]
    assert target_graph.fullmap == Path("/tmp/fm").resolve()  # pyright: ignore[reportAttributeAccessIssue]
    assert captured["graph_path"] == _graph_path().resolve()
    assert captured["map_threshold"] == 0.7
    assert captured["max_improve_iters"] == 5
    assert captured["min_rows"] == 50

    agent(["PMC1"], graph_configuration_file=_graph_path(), min_rows=7)
    assert captured["min_rows"] == 7

    # The forwarded factory resolves config from the environment and builds via the patched build_model.
    factory = captured["build_model_factory"]
    assert callable(factory)
    factory()
    assert build_calls, "build_model_factory() must invoke build_model"
    args, kwargs = build_calls[0]
    assert args == ("env-model", "env-base", "env-key")
    assert kwargs == {"backend": "openai"}
    assert retry_calls == [{"secrets": ("env-key",)}], "the resolved API key is forwarded as secrets"
    # The summary line printed without error.
    assert "tablassert agent:" in capsys.readouterr().out


def test_agent_command_composes_retrying_model_for_agent_reflexion_and_judge(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """All three runtime model roles use retrying, with distillation OUTER of retrying."""
    _set_model_env(monkeypatch)
    import tablassert.agent as agent_mod

    base_models: list[object] = []
    retry_wrapped: list[object] = []
    distilled_wrapped: list[object] = []
    captured: dict[str, object] = {}

    class Base:
        pass

    def fake_build(*args: object, **kwargs: object) -> object:
        model = Base()
        base_models.append(model)
        return model

    def fake_retry(model: object, **kwargs: object) -> object:
        wrapper = {"kind": "retry", "inner": model}
        retry_wrapped.append(wrapper)
        return wrapper

    def fake_distill(model: object, recorder: object, **kwargs: object) -> object:
        wrapper = {"kind": "distill", "inner": model}
        distilled_wrapped.append(wrapper)
        return wrapper

    monkeypatch.setattr(agent_mod, "build_model", fake_build)
    monkeypatch.setattr(agent_mod, "make_retrying_model", fake_retry)
    monkeypatch.setattr(agent_mod, "make_distilling_model", fake_distill)
    monkeypatch.setattr(agent_mod, "make_prompt_callable", lambda model: model)
    monkeypatch.setattr(agent_mod, "run_supervisor", lambda pmc_ids, **kwargs: captured.update(kwargs) or {"records": {}, "metrics": {}})

    agent(["PMC1"], graph_configuration_file=_graph_path(), reflexion=True, judge_model="judge", distill=True)
    factory = captured["build_model_factory"]
    reflexion_factory = captured["reflexion_model_factory"]
    factory_model: dict[str, object] = factory()  # type: ignore[operator]
    reflexion_model: dict[str, object] = reflexion_factory()  # type: ignore[operator]
    judge_model: dict[str, object] = captured["judge_model"]  # type: ignore[assignment]

    assert len(base_models) == 3
    assert len(retry_wrapped) == 3
    # The inner agent's distilling wrapper is applied by run_supervisor, after its factory returns;
    # these two construction sites (reflexion and judge) are wrapped here.
    assert len(distilled_wrapped) == 2
    assert factory_model["kind"] == "retry"
    assert factory_model["inner"] in base_models
    assert reflexion_model["kind"] == "distill"
    reflexion_inner: dict[str, object] = reflexion_model["inner"]  # type: ignore[assignment]
    assert reflexion_inner["kind"] == "retry"
    assert judge_model["kind"] == "distill"
    judge_inner: dict[str, object] = judge_model["inner"]  # type: ignore[assignment]
    assert judge_inner["kind"] == "retry"
    assert all(isinstance(wrapper, dict) and wrapper["kind"] == "retry" for wrapper in retry_wrapped)
    assert "tablassert agent:" in capsys.readouterr().out


def test_agent_cli_flag_parsing() -> None:
    """A full argv parses into the command's bound args WITHOUT executing the body.

    Why: the documented UX is positional PMC ids plus a required target graph flag. cyclopts' ``parse_args``
    binds tokens to the signature without running the function, so this proves both target graph forms
    parse and the removed fullmap flag is rejected.
    """
    fn, bound, _ = APP.parse_args(["agent", "PMC9", "--configuration-file", "/tmp/graph.yaml", "--map-threshold", "0.5"], exit_on_error=False)
    assert fn is agent
    assert bound.args == (["PMC9"],)
    assert bound.kwargs["graph_configuration_file"] == Path("/tmp/graph.yaml")
    assert bound.kwargs["map_threshold"] == 0.5
    _, alias_bound, _ = APP.parse_args(["agent", "PMC9", "-f", "/tmp/graph.yaml"], exit_on_error=False)
    assert alias_bound.kwargs["graph_configuration_file"] == Path("/tmp/graph.yaml")
    with pytest.raises(UnknownOptionError):
        APP.parse_args(["agent", "PMC9", "--fullmap", "/tmp/fm"], exit_on_error=False)


def test_agent_min_rows_flag_parses() -> None:
    """The long and short minimum-row options bind to the integer threshold."""
    fn, bound, _ = APP.parse_args(["agent", "PMC9", "--configuration-file", "/tmp/graph.yaml", "--min-rows", "7"], exit_on_error=False)
    assert fn is agent
    assert bound.kwargs["min_rows"] == 7

    _, alias_bound, _ = APP.parse_args(["agent", "PMC9", "-f", "/tmp/graph.yaml", "-mr", "0"], exit_on_error=False)
    assert alias_bound.kwargs["min_rows"] == 0


def test_agent_negative_min_rows_exits_2(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """A negative threshold fails before extras, models, or supervisor work."""
    _set_model_env(monkeypatch)
    monkeypatch.setattr("tablassert.agent.run_supervisor", lambda *args, **kwargs: pytest.fail("supervisor must not run"))

    with pytest.raises(SystemExit) as exc_info:
        agent(["PMC1"], graph_configuration_file=_graph_path(), min_rows=-3)

    assert exc_info.value.code == 2
    assert "--min-rows" in capsys.readouterr().err


def test_agent_optimize_flag_parses() -> None:
    """``-o``/``--optimize`` parses to optimize=True without executing the body."""
    fn, bound, _ = APP.parse_args(["agent", "PMC9", "--configuration-file", str(_graph_path()), "-o"], exit_on_error=False)
    assert fn is agent
    assert bound.kwargs["optimize"] is True


def test_agent_distill_flag_parses() -> None:
    """``--distill`` and both shorthands parse to distill=True without executing the body."""
    for flag in ("--distill", "-d", "-dt"):
        fn, bound, _ = APP.parse_args(["agent", "PMC9", "--configuration-file", str(_graph_path()), flag], exit_on_error=False)
        assert fn is agent
        assert bound.kwargs["distill"] is True, flag


def test_agent_distill_forwards_a_recorder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """``--distill`` builds a DistillRecorder under <state-dir>/distill and forwards it.

    Why: recording is off by default (``None`` reaches the supervisor untouched); opting in must
    thread a recorder pointing at the state dir's ``distill/records.ndjson`` so the dataset
    accumulates in one append-only file across invocations.
    """
    _set_model_env(monkeypatch)
    captured: dict[str, object] = {}

    def fake_run_supervisor(pmc_ids: list[str], **kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"records": {}, "metrics": {}}

    monkeypatch.setattr("tablassert.agent.run_supervisor", fake_run_supervisor)

    agent(["PMC1"], graph_configuration_file=_graph_path(), state_dir=tmp_path)
    assert captured["distill_recorder"] is None  # off by default

    agent(["PMC1"], graph_configuration_file=_graph_path(), state_dir=tmp_path, distill=True)
    recorder: object = captured["distill_recorder"]
    assert recorder is not None
    assert recorder.path == tmp_path / "distill" / "records.ndjson"  # pyright: ignore[reportAttributeAccessIssue]
    assert "distilling LLM calls" in capsys.readouterr().out


def test_agent_distill_rejects_optimize(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """``--distill --optimize`` exits 2: the GEPA path bypasses the recording seam.

    Why: ``--optimize`` returns before the supervisor runs and its dspy LM never routes through
    the wrapped ``generate``, so the combination would silently record nothing.
    """
    _set_model_env(monkeypatch)
    monkeypatch.setattr("tablassert.agent.run_supervisor", lambda *a, **k: pytest.fail("supervisor must not run"))
    monkeypatch.setattr("tablassert.agent.run_gepa", lambda *a, **k: pytest.fail("GEPA must not run"))

    with pytest.raises(SystemExit) as exc_info:
        agent(["PMC1"], graph_configuration_file=_graph_path(), distill=True, optimize=True)

    assert exc_info.value.code == 2
    assert "--distill" in capsys.readouterr().err


def test_agent_optimize_persists_instructions(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """W6: ``--optimize`` runs GEPA (stubbed) and persists optimized instructions; the supervisor is NOT run."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    monkeypatch.setattr("tablassert.agent.make_dspy_lm", lambda *a, **k: object())

    def fake_run_gepa(**kwargs: object) -> dict[str, object]:
        assert kwargs.get("seed_instructions")  # the seed prompt is passed
        return {"optimized_instructions": "OPTIMIZED PROMPT", "optimized_descriptions": {"propose": "DESC"}, "stats": {}, "frontier": []}

    monkeypatch.setattr("tablassert.agent.run_gepa", fake_run_gepa)

    def fail_supervisor(*a: object, **k: object) -> object:
        raise AssertionError("run_supervisor must NOT run when --optimize is set")

    monkeypatch.setattr("tablassert.agent.run_supervisor", fail_supervisor)

    out: Path = tmp_path / "opt.yaml"
    agent(["PMC1"], graph_configuration_file=_graph_path(), optimize=True, instructions_out=out)

    assert out.is_file()
    assert load_optimized_instructions(out) == "OPTIMIZED PROMPT"
    assert "optimized instructions" in capsys.readouterr().out


def test_agent_instructions_file_forwarded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """W6: ``--instructions-file`` loads optimized instructions and forwards them to the supervisor."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    captured: dict[str, object] = {}

    def fake_run_supervisor(pmc_ids: list[str], **kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"records": {}, "metrics": {}}

    monkeypatch.setattr("tablassert.agent.run_supervisor", fake_run_supervisor)

    instr_file: Path = tmp_path / "instr.yaml"
    save_optimized_instructions(instr_file, "CUSTOM PROMPT")

    agent(["PMC1"], graph_configuration_file=_graph_path(), instructions_file=instr_file)
    assert captured["instructions"] == "CUSTOM PROMPT"


def test_agent_no_instructions_file_passes_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without ``--instructions-file`` the supervisor receives instructions=None (default INSTRUCTIONS)."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    captured: dict[str, object] = {}

    def fake_run_supervisor(pmc_ids: list[str], **kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"records": {}, "metrics": {}}

    monkeypatch.setattr("tablassert.agent.run_supervisor", fake_run_supervisor)

    agent(["PMC1"], graph_configuration_file=_graph_path())
    assert captured["instructions"] is None


@pytest.mark.parametrize("bad_threshold", [-1.0, 2.0, float("nan"), float("inf")])
def test_agent_judge_threshold_out_of_range_exits_2(
    bad_threshold: float, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """CodeRabbit: --judge-threshold outside [0, 1] (or non-finite) fails loud (exit 2) before any model runs."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    def fail_supervisor(*a: object, **k: object) -> object:
        raise AssertionError("run_supervisor must NOT run with an invalid --judge-threshold")

    monkeypatch.setattr("tablassert.agent.run_supervisor", fail_supervisor)

    with pytest.raises(SystemExit) as exc_info:
        agent(["PMC1"], graph_configuration_file=_graph_path(), judge_threshold=bad_threshold)
    assert exc_info.value.code == 2
    assert "judge-threshold" in capsys.readouterr().err


def test_agent_judge_threshold_valid_is_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    """A valid --judge-threshold (here 0.7) passes validation and reaches the supervisor unchanged."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    captured: dict[str, object] = {}

    def fake_run_supervisor(pmc_ids: list[str], **kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"records": {}, "metrics": {}}

    monkeypatch.setattr("tablassert.agent.run_supervisor", fake_run_supervisor)

    agent(["PMC1"], graph_configuration_file=_graph_path(), judge_threshold=0.7)
    assert captured["judge_threshold"] == 0.7


@pytest.mark.parametrize("bad_spec", ["PMC1=", "=DIR", "PMC1=   "])
def test_agent_local_rejects_empty_mapping_components(bad_spec: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """CodeRabbit: --local PMCid=DIR with a blank PMC id or blank DIR fails loud (exit 2), not Path('.')."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    def fail_supervisor(*a: object, **k: object) -> object:
        raise AssertionError("run_supervisor must NOT run with an invalid --local mapping")

    monkeypatch.setattr("tablassert.agent.run_supervisor", fail_supervisor)

    with pytest.raises(SystemExit) as exc_info:
        agent(["PMC1"], graph_configuration_file=_graph_path(), local=[bad_spec])
    assert exc_info.value.code == 2
    assert "--local" in capsys.readouterr().err


def test_agent_local_valid_mapping_forwarded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A valid --local PMCid=DIR mapping (existing dir) parses and reaches the supervisor as a dict."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    captured: dict[str, object] = {}

    def fake_run_supervisor(pmc_ids: list[str], **kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"records": {}, "metrics": {}}

    monkeypatch.setattr("tablassert.agent.run_supervisor", fake_run_supervisor)

    agent(["PMC1"], graph_configuration_file=_graph_path(), local=[f"PMC1={tmp_path}"])
    assert captured["local"] == {"PMC1": tmp_path}


def test_agent_optimize_forwards_backend_to_dspy_lm(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """CodeRabbit: --optimize forwards --backend to the GEPA reflection LM (not always openai)."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    lm_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_make_dspy_lm(*args: object, **kwargs: object) -> object:
        lm_calls.append((args, kwargs))
        return object()

    monkeypatch.setattr("tablassert.agent.make_dspy_lm", fake_make_dspy_lm)
    monkeypatch.setattr(
        "tablassert.agent.run_gepa", lambda **k: {"optimized_instructions": "X", "optimized_descriptions": {}, "stats": {}, "frontier": []}
    )

    out: Path = tmp_path / "opt.yaml"
    agent(["PMC1"], graph_configuration_file=_graph_path(), optimize=True, backend="litellm", instructions_out=out)

    # Without --task-model the CLI builds EXACTLY ONE LM (the reflection LM) — assert the count so a
    # regression that reorders/adds LM constructions cannot hide behind lm_calls[0].
    assert len(lm_calls) == 1
    args, kwargs = lm_calls[0]  # the reflection LM
    assert args == ("m", "b", "k")
    assert kwargs == {"backend": "litellm"}
    assert out.is_file()  # a successful compile still persists


def test_agent_optimize_gepa_error_exits_nonzero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """CodeRabbit: a failed GEPA compile (stats['error']) is NOT reported as optimized; exit 1, nothing saved."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    monkeypatch.setattr("tablassert.agent.make_dspy_lm", lambda *a, **k: object())
    monkeypatch.setattr(
        "tablassert.agent.run_gepa",
        lambda **k: {"optimized_instructions": "SEED", "optimized_descriptions": {}, "stats": {"error": "boom"}, "frontier": []},
    )

    out: Path = tmp_path / "opt.yaml"
    with pytest.raises(SystemExit) as exc_info:
        agent(["PMC1"], graph_configuration_file=_graph_path(), optimize=True, instructions_out=out)
    assert exc_info.value.code == 1
    assert not out.is_file()  # the unoptimized seed is NOT persisted
    assert "GEPA optimization failed" in capsys.readouterr().err


def test_make_dspy_lm_honors_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """CodeRabbit: make_dspy_lm maps backend -> litellm model string (openai/ prefix vs pass-through)."""
    import tablassert.agent as agent_mod

    captured: list[dict[str, object]] = []

    class _FakeLM:
        def __init__(
            self,
            model: str,
            api_base: object = None,
            api_key: object = None,
            temperature: object = None,
            max_tokens: object = None,
            timeout: object = None,
        ) -> None:
            captured.append(
                {"model": model, "api_base": api_base, "api_key": api_key, "temperature": temperature, "max_tokens": max_tokens, "timeout": timeout}
            )

    monkeypatch.setitem(sys.modules, "dspy", types.SimpleNamespace(LM=_FakeLM))

    agent_mod.make_dspy_lm("gpt-x", "base", "key")  # default backend=openai
    agent_mod.make_dspy_lm("anthropic/claude", "base", "key", backend="litellm")

    assert captured[0]["model"] == "openai/gpt-x"
    assert captured[1]["model"] == "anthropic/claude"
    # reasoning-model-safe defaults are passed through (a truncated config_yaml would stall GEPA)
    assert captured[0]["temperature"] == 1.0
    assert captured[0]["max_tokens"] == 16000
    # default timeout bounds each request so a stalled connection cannot hang the optimizer
    assert captured[0]["timeout"] == 600


@pytest.mark.parametrize("bad_threshold", [-1.0, 2.0, float("nan"), float("inf")])
def test_agent_biolink_threshold_out_of_range_exits_2(
    bad_threshold: float, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """--biolink-threshold outside [0, 1] (or non-finite) fails loud before any model runs."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    def fail_supervisor(*a: object, **k: object) -> object:
        raise AssertionError("run_supervisor must NOT run with an invalid --biolink-threshold")

    monkeypatch.setattr("tablassert.agent.run_supervisor", fail_supervisor)

    with pytest.raises(SystemExit) as exc_info:
        agent(["PMC1"], graph_configuration_file=_graph_path(), biolink_threshold=bad_threshold)
    assert exc_info.value.code == 2
    assert "biolink-threshold" in capsys.readouterr().err


def test_agent_biolink_threshold_defaults_to_report_only_and_forwards(monkeypatch: pytest.MonkeyPatch) -> None:
    """It defaults to 0.0 (report-only, preserving today's terminal behavior) and forwards verbatim."""
    monkeypatch.setenv(ENV_MODEL_ID, "m")
    monkeypatch.setenv(ENV_API_BASE, "b")
    monkeypatch.setenv(ENV_API_KEY, "k")

    captured: dict[str, object] = {}

    def fake_run_supervisor(pmc_ids: list[str], **kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"records": {}, "metrics": {}}

    monkeypatch.setattr("tablassert.agent.run_supervisor", fake_run_supervisor)

    agent(["PMC1"], graph_configuration_file=_graph_path())
    assert captured["biolink_threshold"] == 0.0

    agent(["PMC1"], graph_configuration_file=_graph_path(), biolink_threshold=0.95)
    assert captured["biolink_threshold"] == 0.95
