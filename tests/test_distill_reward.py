"""Outcome assembly (``distill_reward``): pure builders over a run's terminal state.

Every test here is BASE-ENV by design (no ``importorskip`` anywhere): ``distill_reward`` is pure
stdlib + ``yaml``, so the whole module is exercised exactly the way CI runs it — unlike the
supervisor-level end-to-end coverage in ``tests/test_agent_supervisor.py``, which skips without the
``[agent]`` extra and is therefore only ever bonus evidence.
"""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import json
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version
from pathlib import Path
from typing import Any

import pytest
import yaml

from tablassert import distill, distill_reward
from tablassert.errors import RewardConfigError


def _full_kwargs() -> dict[str, Any]:
    """A fully-populated ``build_outcome`` call: a MAPPED run with a judge verdict and a valid config."""
    return {
        "run_id": "a1b2c3d4e5f6:PMC11708054",
        "pmc_id": "PMC11708054",
        "model_id": "gpt-4o",
        "run_status": "MAPPED",
        "report": {
            "ok": True,
            "coverage_pct": 0.83,
            "measured": True,
            "head": False,
            "qc_pass_rate": 1.0,
            "biolink_valid_pct": 0.97,
            "biolink_valid_pct_strict": 0.91,
            "demoted_edge_pct": 0.04,
            "node_count": 1204,
            "edge_count": 3338,
            "unresolved": ["brca1", "mapk1"],
            "predicate_advice": [{"pair": "Gene-Gene"}],
            "multivalued_suspects": [],
            "error_codes": [],
        },
        "record": {"attempts": 2, "best_coverage": 0.83, "coverage_history": [0.5, 0.83], "section_coverages": [0.83], "config_chars": 940},
        "metrics": {
            "total_tool_calls": 9,
            "failed_tool_calls": 1,
            "wrong_tool_calls": 0,
            "redundant_tool_calls": 1,
            "total_tokens": 41233,
            "steps": 7,
        },
        "config_yaml": "provenance:\n  repo: PMC\n  publication: PMC11708054\n",
        "map_threshold": 0.25,
        "biolink_threshold": 0.0,
        "judge_threshold": 0.5,
        "judge_verdict": {"scores": {"schema_validity": 3, "biolink_validity": 2}, "normalized": 0.78, "rationale": "offline heuristic"},
        "timestamp": "2026-09-11T12:04:31+00:00",
    }


def test_build_outcome_emits_the_exact_canonical_key_set() -> None:
    """The outcome dict's key set IS ``OUTCOME_KEYS``, in order; each fixed struct IS its sub-tuple.

    Why: ``datasets.load_dataset("json")`` infers features from the first block of the first file
    and raises ``CastError`` when a later block of the append-only outcome corpus introduces or
    renames a column — and a STRUCT column is inferred the same way, so its sub-fields are pinned
    too. The key sets being exact (never a superset, never a subset) is what lets an outcome line
    from any release sit beside any other.
    """
    outcome: dict[str, Any] = distill_reward.build_outcome(**_full_kwargs())

    assert tuple(outcome) == distill.OUTCOME_KEYS  # ORDER is asserted, not just membership
    assert tuple(outcome["tool_calls"]) == distill.TOOL_CALL_KEYS
    assert tuple(outcome["gate"]) == distill.GATE_KEYS
    assert tuple(outcome["versions"]) == distill.VERSION_KEYS
    assert (outcome["record_type"], outcome["schema_version"]) == ("outcome", 2)
    assert (outcome["run_id"], outcome["pmc_id"], outcome["model_id"]) == ("a1b2c3d4e5f6:PMC11708054", "PMC11708054", "gpt-4o")
    assert outcome["timestamp"] == "2026-09-11T12:04:31+00:00"
    assert (outcome["run_status"], outcome["ok"], outcome["measured"], outcome["head"]) == ("MAPPED", True, True, False)
    assert (outcome["coverage_pct"], outcome["best_coverage"]) == (0.83, 0.83)
    assert (outcome["coverage_history_len"], outcome["section_coverages_len"]) == (2, 1)
    assert (outcome["node_count"], outcome["edge_count"], outcome["attempts"]) == (1204, 3338, 2)
    assert (outcome["tokens_total"], outcome["steps"]) == (41233, 7)


def test_build_outcome_tolerates_a_missing_or_partial_report() -> None:
    """A None/partial/legacy report defaults each figure per its declared type and NEVER raises.

    Why: the SKIPPED except branch captures runs that crashed BEFORE any build, and those negative
    examples are exactly the rows an outcome corpus cannot afford to lose — so assembly must degrade
    to explicit nulls (measurements), zeros (counts) and empty lists instead of failing. A figure the
    run never produced must be distinguishable from one it measured as zero, which is why the two
    neutral defaults differ by declared type.
    """
    bare: dict[str, Any] = distill_reward.build_outcome(
        run_id=None,
        pmc_id="PMC1",
        model_id=None,
        run_status="SKIPPED",
        report=None,
        record=None,
        metrics=None,
        config_yaml=None,
        map_threshold=0.25,
        biolink_threshold=0.0,
        judge_threshold=None,
    )

    assert tuple(bare) == distill.OUTCOME_KEYS  # the key set is total even when nothing was measured
    assert (bare["ok"], bare["measured"], bare["head"]) == (None, None, None)
    assert (bare["coverage_pct"], bare["best_coverage"], bare["biolink_valid_pct"], bare["qc_pass_rate"]) == (None, None, None, None)
    assert (bare["node_count"], bare["edge_count"], bare["attempts"]) == (0, 0, 0)
    assert (bare["unresolved_count"], bare["predicate_advice_count"], bare["multivalued_suspect_count"]) == (0, 0, 0)
    assert (bare["coverage_history_len"], bare["section_coverages_len"]) == (0, 0)
    assert bare["error_codes"] == []
    assert bare["config_chars"] is None  # declared int | None: no best config was persisted
    assert (bare["config_yaml_sha256"], bare["provenance_ok"]) == (None, None)
    assert bare["tool_calls"] == dict.fromkeys(distill.TOOL_CALL_KEYS, 0)
    assert (bare["tokens_total"], bare["steps"]) == (0, 0)
    assert (bare["judge_score"], bare["judge_dimensions"]) == (None, None)
    assert bare["gate"] == {"map_threshold": 0.25, "biolink_threshold": 0.0, "judge_threshold": None}

    partial: dict[str, Any] = distill_reward.build_outcome(
        run_id="a1b2c3d4e5f6:PMC2",
        pmc_id="PMC2",
        model_id="fake",
        run_status="MAPPED",
        report={"ok": True, "coverage_pct": 0.5},  # a legacy/fake report: most keys absent
        record={"attempts": 1},
        metrics={"steps": 2},
        config_yaml=None,
        map_threshold=0.25,
        biolink_threshold=0.0,
        judge_threshold=None,
    )
    assert (partial["ok"], partial["coverage_pct"]) == (True, 0.5)
    assert partial["measured"] is None  # absent from the report: unknown, never guessed
    assert partial["unresolved_count"] == 0
    assert partial["error_codes"] == []
    assert (partial["steps"], partial["tokens_total"]) == (2, 0)

    garbage: dict[str, Any] = {  # data-shaped garbage degrades, never raises
        "run_id": None,
        "pmc_id": None,
        "model_id": None,
        "run_status": "SKIPPED",
        "report": ["not", "a", "report"],
        "record": 42,
        "metrics": "junk",
        "config_yaml": None,
        "map_threshold": 0.25,
        "biolink_threshold": 0.0,
        "judge_threshold": None,
        "judge_verdict": "not-a-verdict",
    }
    non_mapping: dict[str, Any] = distill_reward.build_outcome(**garbage)
    assert (non_mapping["ok"], non_mapping["attempts"], non_mapping["judge_score"]) == (None, 0, None)


def test_build_outcome_counts_lists_and_hashes_the_config() -> None:
    """List-valued audit fields land as COUNTS; ``error_codes`` stays verbatim; the config is SHA-256'd.

    Why: the lists themselves are too bulky for a per-run corpus column, but their SIZES are the
    self-correction signal (how much mapping/advice/suspect work the run left behind); error_codes
    is the one list kept verbatim because it is short, enum-valued, and the hard-gate evidence. The
    hash is the terminal config's dedupe and DPO/KTO-pairing identity, so it must be reproducible
    from the config string alone with the stdlib — and must change when the config changes.
    """
    kwargs: dict[str, Any] = _full_kwargs()
    outcome: dict[str, Any] = distill_reward.build_outcome(**kwargs)

    assert (outcome["unresolved_count"], outcome["predicate_advice_count"], outcome["multivalued_suspect_count"]) == (2, 1, 0)
    assert outcome["error_codes"] == []
    expected_sha: str = hashlib.sha256(kwargs["config_yaml"].encode("utf-8")).hexdigest()
    assert outcome["config_yaml_sha256"] == expected_sha
    assert outcome["config_chars"] == 940  # the PERSISTED best config's length, from the record

    coded_report: dict[str, Any] = {**_full_kwargs()["report"], "error_codes": ["KGX_EMPTY", "QC_FAIL"]}
    coded_call: dict[str, Any] = {**kwargs, "report": coded_report}
    coded: dict[str, Any] = distill_reward.build_outcome(**coded_call)
    assert coded["error_codes"] == ["KGX_EMPTY", "QC_FAIL"]  # verbatim: values AND order

    edited_call: dict[str, Any] = {**kwargs, "config_yaml": kwargs["config_yaml"] + "\n# a one-line edit\n"}
    edited: dict[str, Any] = distill_reward.build_outcome(**edited_call)
    assert edited["config_yaml_sha256"] != expected_sha  # the hash tracks the CONFIG, not the run


def test_build_outcome_records_versions_and_gate_thresholds() -> None:
    """``versions`` carries live distribution versions; gate thresholds, judge metadata and qc land verbatim.

    Why: rewards are not comparable across biolink-model releases (v4.4.4 relocated the
    ``supporting_study_*`` slots), so an append-only corpus pooled over time must stay splittable by
    the exact versions that produced each row; the run's own thresholds make runs that gated
    differently comparable at weigh time. Judge output is LLM-generated metadata — present for
    analysis, null when no judge ran, and never reward input — and ``qc_pass_rate`` is
    convenience-only because ``build_and_audit`` hard-wires it to ``1.0 if qc else None``.
    """
    outcome: dict[str, Any] = distill_reward.build_outcome(**_full_kwargs())

    assert outcome["versions"]["tablassert"] == distribution_version("tablassert")
    try:
        expected_biolink: str | None = distribution_version("biolink-model")
    except PackageNotFoundError:
        expected_biolink = None
    assert outcome["versions"]["biolink_model"] == expected_biolink
    assert outcome["gate"] == {"map_threshold": 0.25, "biolink_threshold": 0.0, "judge_threshold": 0.5}
    assert outcome["judge_score"] == 0.78
    assert outcome["judge_dimensions"] == {"schema_validity": 3, "biolink_validity": 2}
    assert outcome["qc_pass_rate"] == 1.0

    no_judge_call: dict[str, Any] = {**_full_kwargs(), "judge_verdict": None}
    no_judge: dict[str, Any] = distill_reward.build_outcome(**no_judge_call)
    assert (no_judge["judge_score"], no_judge["judge_dimensions"]) == (None, None)

    no_gate_call: dict[str, Any] = {**_full_kwargs(), "judge_threshold": None}
    no_judge_gate: dict[str, Any] = distill_reward.build_outcome(**no_gate_call)
    assert no_judge_gate["gate"]["judge_threshold"] is None  # a run without the semantic gate records null, not a guess


def test_provenance_ok_reads_the_config_provenance_block() -> None:
    """Every section needs a provenance identity (publication or complete override) + a provided KL/AT pair.

    Why: ``provenance_ok`` is the config-derived completeness signal recorded on every outcome line
    as METADATA — the section schema requires a provenance block, so a schema-valid config is
    structurally True, which is exactly why the reward must never read it (the same structural-
    constant trap as ``qc_pass_rate``). An unparseable config yields None (unknown), never a guess;
    a parseable one yields a strict every-section verdict over the template-merged sections.
    """
    publication: str = yaml.safe_dump({"provenance": {"repo": "PMC", "publication": "PMC1"}})
    assert distill_reward.provenance_ok(publication) is True

    override: str = yaml.safe_dump({"provenance": {"override": {"publications": ["PMCID:PMC1"], "upstream_resource_ids": ["infores:my-upstream"]}}})
    assert distill_reward.provenance_ok(override) is True

    assert distill_reward.provenance_ok(yaml.safe_dump({"provenance": {"repo": "PMC"}})) is False  # no identity
    assert distill_reward.provenance_ok(yaml.safe_dump({"provenance": {"override": {}}})) is False  # an empty override identifies nothing
    assert distill_reward.provenance_ok(yaml.safe_dump({"provenance": {"publication": "PMC1", "knowledge_level": "not provided"}})) is False
    assert distill_reward.provenance_ok(yaml.safe_dump({"provenance": {"publication": "PMC1", "agent_type": "not_provided"}})) is False
    assert (
        distill_reward.provenance_ok(
            yaml.safe_dump({"provenance": {"override": {"publications": ["PMCID:PMC1"], "knowledge_level": "not provided"}}})
        )
        is False
    )
    assert distill_reward.provenance_ok(yaml.safe_dump({"statement": {"predicate": "related_to"}})) is False  # no provenance block

    # Template/sections shape: the template merges UNDER each section and EVERY section is checked.
    templated: str = yaml.safe_dump({"template": {"provenance": {"publication": "PMC1"}}, "sections": [{}, {"provenance": {"publication": "PMC2"}}]})
    assert distill_reward.provenance_ok(templated) is True
    one_bad: str = yaml.safe_dump(
        {"template": {"provenance": {"publication": "PMC1"}}, "sections": [{}, {"provenance": {"knowledge_level": "not provided"}}]}
    )
    assert distill_reward.provenance_ok(one_bad) is False  # one bad section sinks the run (it inherits the template's publication)

    assert distill_reward.provenance_ok("provenance: [unclosed") is None  # unparseable YAML
    assert distill_reward.provenance_ok("just a plain string") is None  # parses, but is not a config mapping
    assert distill_reward.provenance_ok(yaml.safe_dump({"sections": "not-a-list"})) is None  # an unreadable config shape
    assert (
        distill_reward.provenance_ok(yaml.safe_dump({"sections": [{"provenance": {"override": {}}}, {"provenance": {"publication": "PMC2"}}]}))
        is False
    )  # a bare override section identifies nothing
    assert distill_reward.provenance_ok(yaml.safe_dump({"template": {}, "sections": []})) is False  # no section exists to carry an identity
    with pytest.raises(TypeError):
        distill_reward.provenance_ok(None)  # pyright: ignore[reportArgumentType]  # a call-contract violation fails loud


def test_build_outcome_fails_loud_on_programmer_error() -> None:
    """A violated call contract raises ``TypeError`` immediately; only DATA is ever tolerated.

    Why: the derived layer's error contract is the opposite of the capture layer's — a mistyped
    threshold or a non-string run id is a bug in the CALLER, and silently recording it would poison
    an append-only corpus that can never be rewritten. Failing loud at assembly time is what keeps
    the guarded supervisor wrapper honest: the exception is logged with the real cause, not hidden
    inside a half-defaulted row.
    """
    kwargs: dict[str, Any] = _full_kwargs()

    def expect_type_error(**overrides: Any) -> None:
        call: dict[str, Any] = {**kwargs, **overrides}
        with pytest.raises(TypeError):
            distill_reward.build_outcome(**call)

    expect_type_error(map_threshold="high")
    expect_type_error(biolink_threshold=True)  # a bool is not a threshold
    expect_type_error(judge_threshold="0.5")
    expect_type_error(run_status=None)
    expect_type_error(pmc_id=42)
    expect_type_error(config_yaml=120)


# ───────────────────────────── deterministic reward (US-004) ─────────────────────────────

EDGE_REF: float = 3338.0  # matches _outcome()'s edge_count, so the breadth term saturates at exactly 1.0


def _outcome(**overrides: Any) -> dict[str, Any]:
    """A complete, well-formed MAPPED outcome dict for reward tests; ``**overrides`` applied shallowly."""
    outcome: dict[str, Any] = {
        "record_type": "outcome",
        "schema_version": 2,
        "run_id": "a1b2c3d4e5f6:PMC11708054",
        "pmc_id": "PMC11708054",
        "model_id": "gpt-4o",
        "run_status": "MAPPED",
        "ok": True,
        "measured": True,
        "head": False,
        "coverage_pct": 0.83,
        "biolink_valid_pct": 0.97,
        "demoted_edge_pct": 0.04,
        "edge_count": 3338,
        "tool_calls": {"total": 9, "failed": 1, "wrong": 0, "redundant": 1},
        "provenance_ok": True,
        "qc_pass_rate": 1.0,
        "judge_score": 0.78,
        "judge_dimensions": {"schema_validity": 3, "biolink_validity": 2},
    }
    outcome.update(overrides)
    return outcome


def _write_config(tmp_path: Path, content: str, name: str = "reward.yaml") -> Path:
    """Materialize one reward-config file under ``tmp_path`` and return its path."""
    path: Path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


def test_reward_default_coefficients_sum_to_one() -> None:
    """``RewardConfig()`` IS the documented default policy: the five coefficients sum to exactly 1.00.

    Why: the raw score is meant to be a convex combination — a fraction of a perfect build — which
    only holds if the coefficients sum to 1.0; a drifted default would silently rescale every
    weight the corpus produces. The dataclass must also be frozen + slots with exactly the eleven
    REQ-RW-11 knobs, because a policy mutated between two rows of one export would break the
    cross-row comparability the deterministic reward exists to guarantee.
    """
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()

    assert (config.w_coverage, config.w_biolink, config.w_specificity, config.w_cleanliness, config.w_breadth) == (0.40, 0.28, 0.17, 0.07, 0.08)
    coefficients: float = config.w_coverage + config.w_biolink + config.w_specificity + config.w_cleanliness + config.w_breadth
    assert coefficients == pytest.approx(1.0, abs=distill_reward.COEFFICIENT_SUM_TOLERANCE)
    assert (config.demoted_gate, config.demoted_penalty) == (0.50, 0.5)
    assert (config.redundant_gate, config.redundant_penalty) == (0.30, 0.7)
    assert (config.unmeasured_weight, config.edge_ref) == (0.0, None)

    assert len(dataclasses.fields(distill_reward.RewardConfig)) == 11  # exactly the REQ-RW-11 knob set
    assert tuple(field.name for field in dataclasses.fields(distill_reward.RewardConfig)) == distill_reward.REWARD_CONFIG_FIELDS
    assert hasattr(distill_reward.RewardConfig, "__slots__")
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.w_coverage = 0.9  # pyright: ignore[reportAttributeAccessIssue]  # a frozen policy rejects mutation, loudly


def test_reward_hard_gates_are_multiplicative_and_zero() -> None:
    """A gated row is EXACTLY 0.0 no matter how good its figures — gates multiply, they never add.

    Why: an additive gate would leave a perfect-figures failed build with a near-1.0 weight, and
    the LoRA would train on trajectories whose KGs never built. ``ok is not True``, ``head`` and a
    ``SKIPPED``/``FAILED`` status are the run's own verdicts that the example is not a success
    story; the only honest weight for them is 0.0, and stacking gates must not change that.
    """
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()
    perfect: dict[str, Any] = _outcome(
        coverage_pct=1.0, biolink_valid_pct=1.0, demoted_edge_pct=0.0, tool_calls={"total": 5, "failed": 0, "wrong": 0, "redundant": 0}
    )
    assert distill_reward.reward(perfect, config, edge_ref=EDGE_REF) == pytest.approx(1.0)  # the ungated ceiling

    gated_cases: list[dict[str, Any]] = [
        _outcome(coverage_pct=1.0, biolink_valid_pct=1.0, demoted_edge_pct=0.0, ok=False),
        _outcome(coverage_pct=1.0, biolink_valid_pct=1.0, demoted_edge_pct=0.0, ok=None),
        _outcome(coverage_pct=1.0, biolink_valid_pct=1.0, demoted_edge_pct=0.0, head=True),
        _outcome(coverage_pct=1.0, biolink_valid_pct=1.0, demoted_edge_pct=0.0, run_status="SKIPPED"),
        _outcome(coverage_pct=1.0, biolink_valid_pct=1.0, demoted_edge_pct=0.0, run_status="FAILED"),
        _outcome(coverage_pct=1.0, biolink_valid_pct=1.0, demoted_edge_pct=0.99, ok=False, head=True, run_status="SKIPPED"),
    ]
    for case in gated_cases:
        weight: float = distill_reward.reward(case, config, edge_ref=EDGE_REF)
        assert weight == 0.0  # exact, not approximate: an additive gate could never produce this


def test_reward_penalizes_generic_predicate_farming() -> None:
    """``demoted_edge_pct`` above the gate MULTIPLIES the raw score by ``demoted_penalty``.

    Why: a predicate the association class forbids never raises — it silently demotes the edge to
    bare ``biolink:Association`` and drops its qualifier/evidence slots, so coverage can be farmed
    with ``associated_with``. The gate is the ONLY anti-gaming signal, so it must shrink the weight
    (multiplicatively, on top of r_raw) rather than add a term a high-coverage farm could outrun —
    and it is a STRICT inequality, so a build exactly at the gate is not penalized.
    """
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()

    farmed: float = distill_reward.reward(_outcome(demoted_edge_pct=0.9), config, edge_ref=EDGE_REF)
    expected_farmed: float = (0.83 * 0.40 + 0.97 * 0.28 + 0.1 * 0.17 + (8 / 9) * 0.07 + 1.0 * 0.08) * 0.5
    assert farmed == pytest.approx(expected_farmed)  # specificity 0.1 AND the 0.5 multiplier

    at_gate: float = distill_reward.reward(_outcome(demoted_edge_pct=0.5), config, edge_ref=EDGE_REF)
    expected_at_gate: float = 0.83 * 0.40 + 0.97 * 0.28 + 0.5 * 0.17 + (8 / 9) * 0.07 + 1.0 * 0.08
    assert at_gate == pytest.approx(expected_at_gate)  # exactly at the gate: no penalty (strict >)
    assert farmed < at_gate


def test_reward_penalizes_redundant_tool_calls() -> None:
    """A redundant/total ratio above the gate MULTIPLIES the raw score by ``redundant_penalty``.

    Why: redundant-call spam is an EFFICIENCY failure mode (distinct from the failed+wrong
    correctness signal in the additive cleanliness term), and a multiplicative penalty keeps a
    high-coverage but wasteful trajectory from out-weighting a lean one. The ratio guard requires
    ``total > 0`` so a tool-free run is never ratio-penalized, and the comparison is strict.
    """
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()

    spammed: float = distill_reward.reward(_outcome(tool_calls={"total": 10, "failed": 0, "wrong": 0, "redundant": 4}), config, edge_ref=EDGE_REF)
    expected_raw: float = 0.83 * 0.40 + 0.97 * 0.28 + 0.96 * 0.17 + 1.0 * 0.07 + 1.0 * 0.08
    assert spammed == pytest.approx(expected_raw * 0.7)  # 4/10 > 0.30 trips the penalty

    at_gate: float = distill_reward.reward(_outcome(tool_calls={"total": 10, "failed": 0, "wrong": 0, "redundant": 3}), config, edge_ref=EDGE_REF)
    assert at_gate == pytest.approx(expected_raw)  # exactly 0.30 is NOT above the gate

    tool_free: float = distill_reward.reward(_outcome(tool_calls={"total": 0, "failed": 0, "wrong": 0, "redundant": 0}), config, edge_ref=EDGE_REF)
    assert tool_free == pytest.approx(expected_raw - 1.0 * 0.07)  # total 0: cleanliness 0.0, never a ratio penalty


def test_reward_ignores_judge_qc_and_provenance() -> None:
    """Judge scores, ``qc_pass_rate`` and ``provenance_ok`` move the weight by exactly nothing.

    Why: judge output is LLM-generated — a second hackable proxy that would let the judged model
    grade its own training data — while ``qc_pass_rate`` (``1.0 if qc else None``) and
    ``provenance_ok`` (structurally True for any schema-valid config) are constants that would hand
    every row free weight. They stay captured as METADATA; the reward must be blind to them and to
    any key outside its declared deterministic input set.
    """
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()
    baseline: float = distill_reward.reward(_outcome(), config, edge_ref=EDGE_REF)

    mutated: dict[str, Any] = _outcome(
        judge_score=None,
        judge_dimensions=None,
        qc_pass_rate=None,
        provenance_ok=False,
        mean_f1=0.99,  # not even an OUTCOME_KEYS field: the reward must not look
        some_future_field={"anything": 123},
    )
    assert distill_reward.reward(mutated, config, edge_ref=EDGE_REF) == baseline

    inflated: dict[str, Any] = _outcome(judge_score=1.0, judge_dimensions={"every": 3}, qc_pass_rate=1.0, provenance_ok=True)
    assert distill_reward.reward(inflated, config, edge_ref=EDGE_REF) == baseline


def test_reward_is_deterministic_across_repeated_calls() -> None:
    """Same outcome, same weight — across repeated calls, key order, and the two edge_ref sources.

    Why: the corpus is append-only and long-lived, so a weight that drifted between runs would make
    two exports of the same corpus incomparable; the function must be pure (no clock, no
    randomness, no dict-order dependence) and the explicit ``edge_ref`` argument must resolve
    identically to the config's file-level override.
    """
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()
    outcome: dict[str, Any] = _outcome()
    first: float = distill_reward.reward(outcome, config, edge_ref=EDGE_REF)
    for _ in range(25):
        assert distill_reward.reward(outcome, config, edge_ref=EDGE_REF) == first

    shuffled: dict[str, Any] = dict(reversed(list(outcome.items())))
    assert distill_reward.reward(shuffled, config, edge_ref=EDGE_REF) == first  # key order is not an input

    via_config: float = distill_reward.reward(outcome, distill_reward.RewardConfig(edge_ref=EDGE_REF), edge_ref=None)
    assert via_config == first  # the explicit argument and the config override resolve the same reference


def test_unmeasurable_inputs_contribute_zero_not_a_free_pass() -> None:
    """A null measurement contributes 0.0 — a fully unmeasurable build weights exactly 0.0.

    Why: treating an unmeasurable figure as neutral (or, worse, as perfect — the
    ``demoted_edge_pct = null -> specificity 1.0`` trap) hands unmeasured rows free weight, the
    same degeneracy documented for ``qc_pass_rate``. Null means "never measured", and a run whose
    figures were never measured has produced NO evidence the example is worth training on.
    """
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()

    unmeasurable: dict[str, Any] = _outcome(
        coverage_pct=None,
        biolink_valid_pct=None,
        demoted_edge_pct=None,
        edge_count=None,
        tool_calls={"total": 0, "failed": 0, "wrong": 0, "redundant": 0},
    )
    assert distill_reward.reward(unmeasurable, config, edge_ref=EDGE_REF) == 0.0  # every term unmeasured -> nothing earned

    coverage_only: float = distill_reward.reward(
        _outcome(
            coverage_pct=0.5,
            biolink_valid_pct=None,
            demoted_edge_pct=None,
            edge_count=None,
            tool_calls={"total": 0, "failed": 0, "wrong": 0, "redundant": 0},
        ),
        config,
        edge_ref=EDGE_REF,
    )
    assert coverage_only == pytest.approx(0.5 * 0.40)  # null specificity is 0.0, NOT the 0.17 a free pass would add

    no_demoted_penalty_on_null: float = distill_reward.reward(_outcome(demoted_edge_pct=None), config, edge_ref=EDGE_REF)
    expected: float = 0.83 * 0.40 + 0.97 * 0.28 + 0.0 * 0.17 + (8 / 9) * 0.07 + 1.0 * 0.08
    assert no_demoted_penalty_on_null == pytest.approx(expected)  # unmeasured is not "farming": no penalty, no free specificity


def test_built_unmeasured_is_gated_but_kept() -> None:
    """``BUILT_UNMEASURED`` / unmeasured rows floor at ``unmeasured_weight`` — kept, not selectable.

    Why: coverage was never certified for these runs, so there is no evidence the trajectory is a
    good example; but the record must stay in the corpus (as a zero-weight row / negative example)
    rather than being dropped. The floor is CONFIGURABLE — an operator who wants to keep such rows
    marginally selectable sets ``unmeasured_weight`` — and it must not silently revert to 0.0 or
    raise.
    """
    unmeasured: dict[str, Any] = _outcome(run_status="BUILT_UNMEASURED", measured=False, coverage_pct=None)

    assert distill_reward.reward(unmeasured, distill_reward.RewardConfig(), edge_ref=EDGE_REF) == 0.0  # the default floor
    floored: float = distill_reward.reward(unmeasured, distill_reward.RewardConfig(unmeasured_weight=0.25), edge_ref=EDGE_REF)
    assert floored == pytest.approx(0.25)  # the configured floor is honored exactly, never re-computed from figures

    null_measured: dict[str, Any] = _outcome(measured=None)  # a MAPPED row whose measurement flag is unknown
    assert distill_reward.reward(null_measured, distill_reward.RewardConfig(), edge_ref=EDGE_REF) == 0.0
    assert distill_reward.reward(null_measured, distill_reward.RewardConfig(unmeasured_weight=0.25), edge_ref=EDGE_REF) == pytest.approx(0.25)


def test_head_builds_are_gated_and_excluded_from_edge_ref() -> None:
    """``head`` builds weight 0.0 AND never enter the breadth reference.

    Why: a head build samples ~5 rows per section, so its edge_count is structurally smaller and
    not comparable to a full build's — letting one into ``edge_ref`` would deflate the log1p
    denominator and inflate every full build's breadth term, while weighting the head build itself
    would train on a deliberately truncated trajectory.
    """
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()
    head_build: dict[str, Any] = _outcome(head=True, coverage_pct=1.0, biolink_valid_pct=1.0, demoted_edge_pct=0.0)
    assert distill_reward.reward(head_build, config, edge_ref=EDGE_REF) == 0.0

    corpus: list[dict[str, Any]] = [
        _outcome(head=True, edge_count=99999),  # a head build's edge_count is not comparable, however large
        _outcome(edge_count=100),
    ]
    assert distill_reward.median_edge_ref(corpus) == 100.0  # the head build is excluded from the reference


def test_median_edge_ref_excludes_head_and_failed_builds() -> None:
    """``E_ref`` is the median edge_count over comparable builds only; None when there are none.

    Why: the breadth normalizer must describe what a real, successful, full build looks like in
    THIS corpus — failed builds (``ok is not True``), head builds, and rows with a null/zero/negative
    edge_count say nothing about achievable breadth, so they are filtered out. An empty or
    all-incomparable corpus yields None (the caller warns that breadth contributes 0.0) rather than
    inventing a reference.
    """
    corpus: list[dict[str, Any]] = [
        _outcome(edge_count=100),
        _outcome(edge_count=300),
        _outcome(edge_count=9999, ok=False),  # failed build: excluded
        _outcome(edge_count=8888, ok=None),  # unknown success: excluded
        _outcome(edge_count=5, head=True),  # head build: excluded
        _outcome(edge_count=None),  # unmeasured: excluded
        _outcome(edge_count=0),  # a zero-edge build is not a breadth reference
        _outcome(edge_count=-4),  # neither is a negative one
    ]
    assert distill_reward.median_edge_ref(corpus) == 200.0  # median of [100, 300]

    assert distill_reward.median_edge_ref([]) is None
    assert distill_reward.median_edge_ref([_outcome(head=True), _outcome(ok=False)]) is None
    assert distill_reward.median_edge_ref([_outcome(edge_count=250)]) == 250.0


def test_load_reward_config_rejects_unknown_keys_and_bad_sums(tmp_path: Path) -> None:
    """Every malformed config fails LOUD with the coded error — never a silently defaulted policy.

    Why: this file decides which examples train the LoRA, so a typo'd key, a mis-summed
    coefficient set, an out-of-range knob or a wrong-typed value must stop the weigh with an
    actionable message (naming the file, the key, the valid set, and — for a bad sum — the actual
    sum), not train a model on an unintended policy.
    """

    def expect_invalid(content: str | None, *fragments: str, name: str = "reward.yaml") -> None:
        path: Path = _write_config(tmp_path, content, name) if content is not None else tmp_path / name
        with pytest.raises(RewardConfigError) as excinfo:
            distill_reward.load_reward_config(path)
        assert excinfo.value.code == "reward-config-invalid"
        message: str = str(excinfo.value)
        for fragment in fragments:
            assert fragment in message

    expect_invalid("w_covrage: 0.4\n", "unknown key", "w_covrage", "w_coverage")  # a typo names itself AND the valid set
    expect_invalid("42: 0.5\n", "unknown key", "42")  # a non-string key is unknown, not coerced
    expect_invalid(
        "w_coverage: 0.5\nw_biolink: 0.5\nw_specificity: 0.5\nw_cleanliness: 0.5\nw_breadth: 0.5\n", "sum to 1.0", "2.5"
    )  # the actual sum is reported
    expect_invalid("w_coverage: -0.1\n", "non-negative", "w_coverage")
    expect_invalid("demoted_penalty: 0\n", "(0, 1]", "demoted_penalty")  # 0 would silently zero rows: that is what hard gates are for
    expect_invalid("redundant_penalty: 1.5\n", "(0, 1]", "redundant_penalty")  # > 1 would REWARD the failure mode
    expect_invalid("w_coverage: high\n", "must be a number", "w_coverage")
    expect_invalid("demoted_gate: yes\n", "must be a number")  # a bool is not a knob value
    expect_invalid("w_coverage: null\n", "must be a number")  # explicit null is not a number either
    expect_invalid("w_coverage: .nan\n", "must be finite")
    expect_invalid("edge_ref: 0\n", "edge_ref", "must be positive")  # the log1p denominator must be positive
    expect_invalid("edge_ref: -3\n", "edge_ref", "must be positive")
    expect_invalid("redundant_gate: 1.5\n", "[0, 1]", "redundant_gate")  # a fraction knob cannot exceed 1
    expect_invalid("unmeasured_weight: 2\n", "[0, 1]", "unmeasured_weight")
    expect_invalid("- a\n- b\n", "expected a mapping")  # a list document is not a policy
    expect_invalid("key: [unclosed\n", "not parseable YAML")
    expect_invalid("{bad json", "not parseable JSON", name="reward.json")
    expect_invalid("w_coverage: 0.4\n", "unsupported suffix", name="reward.toml")
    expect_invalid(None, "unreadable", name="missing.yaml")  # a file that does not exist
    with pytest.raises(TypeError):
        distill_reward.load_reward_config("reward.yaml")  # pyright: ignore[reportArgumentType]  # a call-contract violation, not bad data


def test_load_reward_config_accepts_yaml_and_json(tmp_path: Path) -> None:
    """YAML (.yaml/.yml) and JSON load to identical policies; omitted knobs take documented defaults.

    Why: the operator's policy file is the tuning surface — it must accept the two documented
    formats by suffix, coerce JSON/YAML numbers to floats, honor an explicit ``edge_ref: null`` as
    the corpus-median default, and validate the RESOLVED coefficient sum so a partial file that
    only pins ``edge_ref`` still loads against the default coefficients.
    """
    custom: dict[str, Any] = {
        "w_coverage": 0.5,
        "w_biolink": 0.2,
        "w_specificity": 0.15,
        "w_cleanliness": 0.1,
        "w_breadth": 0.05,
        "demoted_gate": 0.4,
        "demoted_penalty": 0.6,
        "redundant_gate": 0.2,
        "redundant_penalty": 0.8,
        "unmeasured_weight": 0.1,
        "edge_ref": 500,
    }
    from_yaml: distill_reward.RewardConfig = distill_reward.load_reward_config(_write_config(tmp_path, yaml.safe_dump(custom)))
    from_yml: distill_reward.RewardConfig = distill_reward.load_reward_config(_write_config(tmp_path, yaml.safe_dump(custom), name="reward.yml"))
    from_json: distill_reward.RewardConfig = distill_reward.load_reward_config(_write_config(tmp_path, json.dumps(custom), name="reward.json"))
    assert from_yaml == from_yml == from_json
    assert (from_yaml.w_coverage, from_yaml.w_biolink, from_yaml.w_specificity) == (0.5, 0.2, 0.15)
    assert (from_yaml.w_cleanliness, from_yaml.w_breadth) == (0.1, 0.05)
    assert (from_yaml.demoted_gate, from_yaml.demoted_penalty) == (0.4, 0.6)
    assert (from_yaml.redundant_gate, from_yaml.redundant_penalty) == (0.2, 0.8)
    assert (from_yaml.unmeasured_weight, from_yaml.edge_ref) == (0.1, 500.0)

    partial: distill_reward.RewardConfig = distill_reward.load_reward_config(_write_config(tmp_path, "edge_ref: 500\n", name="partial.yaml"))
    assert partial.edge_ref == 500.0
    assert partial.w_coverage == 0.40  # every omitted knob takes the documented default

    explicit_null: distill_reward.RewardConfig = distill_reward.load_reward_config(_write_config(tmp_path, "edge_ref: null\n", name="nullref.yaml"))
    assert explicit_null.edge_ref is None  # null spells the corpus-median default

    default_roundtrip: distill_reward.RewardConfig = distill_reward.load_reward_config(
        _write_config(
            tmp_path,
            json.dumps({"w_coverage": 0.40, "w_biolink": 0.28, "w_specificity": 0.17, "w_cleanliness": 0.07, "w_breadth": 0.08}),
            name="defaults.json",
        )
    )
    assert default_roundtrip == distill_reward.RewardConfig()


def test_reward_raises_on_a_malformed_outcome() -> None:
    """A missing required key or a wrong-typed value fails LOUD — never a silently invented weight.

    Why: weighting is the fail-loud counterpart of the fail-soft recorder. A row missing a field
    the reward reads (a pre-v2 or hand-built corpus) or carrying a string where a number is
    declared is a schema violation; defaulting it would fabricate a weight for an example that was
    never measured and quietly contaminate the training set.
    """
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()

    def expect(error: type[Exception], **overrides: Any) -> None:
        with pytest.raises(error):
            distill_reward.reward(_outcome(**overrides), config, edge_ref=EDGE_REF)

    missing: dict[str, Any] = _outcome()
    del missing["coverage_pct"]
    with pytest.raises(ValueError, match="coverage_pct"):
        distill_reward.reward(missing, config, edge_ref=EDGE_REF)
    missing_status: dict[str, Any] = _outcome()
    del missing_status["run_status"]
    with pytest.raises(ValueError, match="run_status"):
        distill_reward.reward(missing_status, config, edge_ref=EDGE_REF)

    expect(TypeError, coverage_pct="high")
    expect(TypeError, coverage_pct=True)  # a bool is not a measurement
    expect(TypeError, biolink_valid_pct="n/a")
    expect(TypeError, demoted_edge_pct="low")
    expect(TypeError, edge_count="many")
    expect(TypeError, ok="yes")  # a truthy string is not a flag
    expect(TypeError, head=1)
    expect(TypeError, measured="True")
    expect(TypeError, run_status=42)
    expect(TypeError, run_status=None)  # null is not a status string
    expect(TypeError, tool_calls=None)
    expect(TypeError, tool_calls="nine")
    expect(TypeError, tool_calls={"total": "9", "failed": 0, "wrong": 0, "redundant": 0})
    expect(TypeError, tool_calls={"total": None, "failed": 0, "wrong": 0, "redundant": 0})  # the pinned struct is never null
    expect(ValueError, tool_calls={"failed": 0, "wrong": 0, "redundant": 0})  # a missing tally
    expect(ValueError, edge_count=-5)  # a count cannot be negative (and log1p would be undefined at -1)

    with pytest.raises(TypeError):
        distill_reward.reward(["not", "a", "mapping"], config, edge_ref=EDGE_REF)  # pyright: ignore[reportArgumentType]
    with pytest.raises(ValueError, match="edge_ref"):
        distill_reward.reward(_outcome(), config, edge_ref=0.0)
    with pytest.raises(ValueError, match="edge_ref"):
        distill_reward.reward(_outcome(), distill_reward.RewardConfig(), edge_ref=-2.0)
    with pytest.raises(TypeError):
        distill_reward.reward(_outcome(), config, edge_ref="big")  # pyright: ignore[reportArgumentType]


def test_no_rl_or_reward_model_dependency_is_imported() -> None:
    """``distill_reward`` imports no RL/training stack and never references ``quality_score``.

    Why: this is SFT data selection, explicitly NOT RLHF — no reward model, no PPO/GRPO/DPO/KTO
    trainer, no ``trl``/``peft``/``transformers``/``torch``/``accelerate`` — and ``quality_score``
    is the GEPA path's composite carrying the two degenerate terms (constant ``qc``, gold-less
    ``mean_f1``) this module exists to replace. An executable AST scan, not review, is what keeps a
    future "convenient" import from smuggling either in; the module must stay stdlib + ``yaml``.
    """
    source_path: Path = Path(distill_reward.__file__)
    tree: ast.Module = ast.parse(source_path.read_text(encoding="utf-8"))

    imported: set[str] = set()
    referenced: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module.split(".")[0])
        elif isinstance(node, ast.Name):
            referenced.add(node.id)
        elif isinstance(node, ast.Attribute):
            referenced.add(node.attr)

    banned_imports: set[str] = {"trl", "peft", "transformers", "torch", "accelerate"}
    assert imported.isdisjoint(banned_imports), f"RL/training dependency imported: {imported & banned_imports}"
    assert "quality_score" not in referenced
    allowed: set[str] = set(sys.stdlib_module_names) | {"yaml", "tablassert"}
    assert imported <= allowed, f"non-stdlib, non-yaml import: {imported - allowed}"


# ─────────── derived rows: corpus I/O, join, flatten, select, normalization (US-005) ───────────


def _record(**overrides: Any) -> dict[str, Any]:
    """A complete v2 record line in canonical ``RECORD_KEYS`` order, with ``**overrides`` applied."""
    record: dict[str, Any] = dict.fromkeys(distill.RECORD_KEYS)
    record.update(
        {
            "record_type": "record",
            "schema_version": 2,
            "run_id": "inv1:PMC1",
            "timestamp": "2026-09-11T12:04:31+00:00",
            "purpose": "agent",
            "call_index": 0,
            "messages": [{"role": "user", "content": "build the KG"}, {"role": "assistant", "content": "sections:\n- statement: {}\n"}],
            "token_usage": {"input_tokens": 120, "output_tokens": 40},
            "input_tokens": 120,
            "output_tokens": 40,
            "n_messages": 2,
            "pmc_id": "PMC1",
            "model_id": "gpt-4o",
        }
    )
    record.update(overrides)
    return record


def _row(**overrides: Any) -> dict[str, Any]:
    """A canonical ``TRAIN_ROW_KEYS`` row for policy tests: every key present, ``**overrides`` applied."""
    row: dict[str, Any] = dict.fromkeys(distill_reward.TRAIN_ROW_KEYS)
    row.update({"outcome_matched": True, "weight": 0.0, "selected": False, "replicas": 0})
    row.update(overrides)
    return row


def _weighed(records: list[dict[str, Any]], outcomes: list[dict[str, Any]], *, edge_ref: float | None = None) -> list[dict[str, Any]]:
    """Join, then weigh each matched row from its NESTED outcome the way ``distill-weigh`` does.

    Why this shape: the join has no ``RewardConfig``/``edge_ref`` in scope, so a matched row comes
    back with a null ``weight`` and the caller derives it per ``run_id`` from the outcome line.
    """
    rows: list[dict[str, Any]]
    rows, _ = distill_reward.join_records_outcomes(records, outcomes)
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()
    weights: dict[str, float] = {}
    for outcome in outcomes:
        run_id: object = outcome.get("run_id")
        if isinstance(run_id, str):
            weights[run_id] = distill_reward.reward(outcome, config, edge_ref=edge_ref)
    for row in rows:
        run_id = row.get("run_id")
        if isinstance(run_id, str) and run_id in weights:
            row["weight"] = weights[run_id]
    return rows


def test_select_threshold_policy() -> None:
    """Policy A keeps ``weight >= threshold``: selected rows get exactly one replica, the rest zero.

    Why: this is RAFT/RFT rejection sampling (arXiv:2304.06767, arXiv:2308.01825) and the only
    selection TRL consumes with zero trainer code, because ``SFTConfig`` has no per-example
    sample-weight column. The comparison is inclusive — a row exactly at the cutoff is a kept
    example — and zero-weight rows stay in the output as ``selected = False``: they are the
    negative examples Agent-FLAN shows are worth keeping, so dropping them would be a silent
    corpus edit. An empty corpus selects nothing rather than raising.
    """
    rows: list[dict[str, Any]] = [_row(weight=0.9), _row(weight=0.75), _row(weight=0.74), _row(weight=0.0)]

    selected: list[dict[str, Any]] = distill_reward.select(rows, policy="threshold")
    assert [row["selected"] for row in selected] == [True, True, False, False]  # 0.75 is AT the default cutoff
    assert [row["replicas"] for row in selected] == [1, 1, 0, 0]
    assert [row["weight"] for row in selected] == [0.9, 0.75, 0.74, 0.0]  # weights pass through unchanged
    assert [row["policy"] for row in selected] == ["threshold"] * 4  # the policy is stamped for reproducibility
    assert [row["threshold"] for row in selected] == [0.75] * 4  # ... and so is the cutoff that produced it
    assert [row["run_id"] for row in selected] == [None] * 4  # a non-annotation column is untouched
    assert len(selected) == len(rows)  # zero-weight rows are emitted, never dropped

    strict: list[dict[str, Any]] = distill_reward.select(rows, policy="threshold", threshold=1.0)
    assert [row["selected"] for row in strict] == [False] * 4  # nothing weighs a perfect 1.0
    assert [row["threshold"] for row in strict] == [1.0] * 4

    permissive: list[dict[str, Any]] = distill_reward.select(rows, policy="threshold", threshold=0.0)
    assert [row["selected"] for row in permissive] == [True] * 4  # a 0.0 cutoff keeps every row, even weight 0.0

    assert distill_reward.select([], policy="threshold") == []  # an empty corpus is not an error


def test_select_best_of_n_is_deterministic_and_tie_breaks(tmp_path: Path) -> None:
    """Policy B keeps the top ``top_n`` per ``pmc_id`` with a TOTAL, reproducible ranking.

    Why: this is literally RAFT's ``y := argmax_j r(x, y_j)`` generalized to top-N, and the corpus
    is append-only and long-lived — two weighs of one corpus must emit byte-identical output, so
    the ranking may not depend on dict iteration order, filesystem order, or the sort
    implementation's stability. Hence the four documented keys (highest weight, then fewer
    attempts, then fewer failed tool calls, then the earlier call) plus the row's own position as
    the final tie-break, which makes the order total even for two rows identical on all four.
    Rows with a null ``pmc_id`` form their own group, and a null ranking figure sorts AFTER every
    known one rather than winning the tie by accident.
    """
    tied: list[dict[str, Any]] = [
        _row(pmc_id="PMC1", weight=0.9, outcome_attempts=3, outcome_tool_calls_failed=2, call_index=4),
        _row(pmc_id="PMC1", weight=0.9, outcome_attempts=1, outcome_tool_calls_failed=2, call_index=9),  # fewer attempts wins
        _row(pmc_id="PMC1", weight=0.9, outcome_attempts=1, outcome_tool_calls_failed=0, call_index=9),  # then fewer failed calls
        _row(pmc_id="PMC1", weight=0.9, outcome_attempts=1, outcome_tool_calls_failed=0, call_index=2),  # then the earlier call
        _row(pmc_id="PMC1", weight=0.9, outcome_attempts=1, outcome_tool_calls_failed=0, call_index=2),  # identical on all four:
    ]
    top_one: list[dict[str, Any]] = distill_reward.select(tied, policy="best-of-n", top_n=1)
    assert [row["selected"] for row in top_one] == [False, False, False, True, False]  # ... so the earlier input position wins
    assert [row["replicas"] for row in top_one] == [0, 0, 0, 1, 0]  # a kept row is emitted once, never duplicated
    assert [row["policy"] for row in top_one] == ["best-of-n"] * 5

    # Weight outranks every tie-break: a leaner trajectory that built a worse KG still loses.
    by_weight: list[dict[str, Any]] = [
        _row(pmc_id="PMC2", weight=0.4, outcome_attempts=1, outcome_tool_calls_failed=0, call_index=0),
        _row(pmc_id="PMC2", weight=0.8, outcome_attempts=9, outcome_tool_calls_failed=9, call_index=9),
    ]
    assert [row["selected"] for row in distill_reward.select(by_weight, policy="best-of-n", top_n=1)] == [False, True]

    # Groups are independent, a null pmc_id is its own group, and an unknown figure sorts last.
    grouped: list[dict[str, Any]] = [
        _row(pmc_id="PMC3", weight=0.2),
        _row(pmc_id="PMC3", weight=0.6),
        _row(pmc_id="PMC4", weight=0.1),  # a single-row group is always kept
        _row(pmc_id=None, weight=0.9),  # null pmc_id: its own group
        _row(pmc_id=None, weight=0.95),
        _row(pmc_id="PMC5", weight=0.7, outcome_attempts=None),  # an unmeasured attempt count sorts after a known one
        _row(pmc_id="PMC5", weight=0.7, outcome_attempts=5),
    ]
    kept: list[dict[str, Any]] = distill_reward.select(grouped, policy="best-of-n", top_n=1)
    assert [row["selected"] for row in kept] == [False, True, True, False, True, False, True]
    assert len(kept) == len(grouped)  # ranking annotates; it never reorders or drops
    assert [row["replicas"] for row in kept] == [0, 1, 1, 0, 1, 0, 1]  # a kept row is emitted once

    # The bare outcome spellings rank exactly like the canonical flat columns.
    bare: list[dict[str, Any]] = [_row(pmc_id="PMC6", weight=0.5, attempts=7), _row(pmc_id="PMC6", weight=0.5, attempts=1)]
    assert [row["selected"] for row in distill_reward.select(bare, policy="best-of-n", top_n=1)] == [False, True]

    # Determinism: repeated runs are equal, and their serialized bytes are identical.
    corpus: list[dict[str, Any]] = [*tied, *grouped, *by_weight]
    first: list[dict[str, Any]] = distill_reward.select(corpus, policy="best-of-n", top_n=2)
    second: list[dict[str, Any]] = distill_reward.select(corpus, policy="best-of-n", top_n=2)
    assert first == second
    assert [row["pmc_id"] for row in first] == [row["pmc_id"] for row in corpus]  # emitted in the ORIGINAL record order
    left: Path = tmp_path / "left.ndjson"
    right: Path = tmp_path / "right.ndjson"
    assert distill_reward.write_ndjson(left, first) == distill_reward.write_ndjson(right, second) == len(corpus)
    assert left.read_bytes() == right.read_bytes()  # two runs over one corpus: byte-identical output

    # Policy B has NO weight floor: it keeps the best AVAILABLE sample per prompt, so a zero-weight
    # singleton group is selected where policy A would drop it. That is REQ-RW-17 verbatim (RAFT's
    # argmax over the samples that exist) and the documented difference between the two policies.
    zero: list[dict[str, Any]] = [_row(pmc_id="PMC7", weight=0.0)]
    assert distill_reward.select(zero, policy="best-of-n")[0]["selected"] is True
    assert distill_reward.select(zero, policy="threshold")[0]["selected"] is False
    assert distill_reward.select(zero, policy="replication")[0]["selected"] is False  # policy C selects on weight > 0


def test_select_replication_is_monotone_and_bounded() -> None:
    """Policy C selects ``weight > 0`` and scales ``replicas`` monotonically across the weight spread.

    Why: replication is the only way to approximate importance weighting natively, because TRL has
    no per-example weight column — so the weight becomes a COUNT of how many times the trainer
    should see the row, bounded by ``1 + replication_k``. ``replicas`` is never a physical
    duplication: repeating rows here would inflate the file, destroy per-example identity under
    ``packing=True``, and double-count once the trainer applies its own replication. All-equal
    weights encode no gradient, so every selected row gets exactly one replica rather than an
    invented spread, and an all-zero corpus selects nothing.
    """
    rows: list[dict[str, Any]] = [_row(weight=0.0), _row(weight=0.25), _row(weight=0.5), _row(weight=1.0)]

    scaled: list[dict[str, Any]] = distill_reward.select(rows, policy="replication", replication_k=2)
    assert [row["selected"] for row in scaled] == [False, True, True, True]  # weight > 0, not >= a threshold
    assert [row["replicas"] for row in scaled] == [0, 1, 2, 3]  # 1 + round(2 * (w - 0.25) / 0.75)
    assert all(isinstance(row["replicas"], int) for row in scaled)  # a COUNT, so an int column
    assert len(scaled) == len(rows)  # one row per example, always

    monotone: list[int] = [row["replicas"] for row in scaled if row["selected"]]
    assert monotone == sorted(monotone)  # never inverted: a better example cannot replicate less
    assert all(1 <= count <= 1 + distill_reward.MAX_REPLICATION_K for count in monotone)

    flat: list[dict[str, Any]] = distill_reward.select([_row(weight=0.6), _row(weight=0.6), _row(weight=0.0)], policy="replication")
    assert [row["replicas"] for row in flat] == [1, 1, 0]  # all-equal weights: no gradient to encode

    empty: list[dict[str, Any]] = distill_reward.select([_row(weight=0.0), _row(weight=0.0)], policy="replication")
    assert [row["selected"] for row in empty] == [False, False]
    assert [row["replicas"] for row in empty] == [0, 0]

    zero_slope: list[dict[str, Any]] = distill_reward.select(rows, policy="replication", replication_k=0)
    assert [row["replicas"] for row in zero_slope] == [0, 1, 1, 1]  # k = 0 is pure filtering
    max_slope: list[dict[str, Any]] = distill_reward.select(rows, policy="replication", replication_k=3)
    assert [row["replicas"] for row in max_slope] == [0, 1, 2, 4]  # bounded by 1 + k
    assert [row["policy"] for row in max_slope] == ["replication"] * 4


def test_select_emits_the_same_key_set_for_every_policy() -> None:
    """All three policies emit the SAME key set — only the annotation values differ.

    Why: ``datasets`` infers its features from the first block of the first file, so a schema that
    depended on the policy chosen would make two exports of one corpus incomparable (and a
    re-weigh under a different policy would raise ``CastError`` instead of producing a dataset).
    Asserted both on canonical joined rows (where the key ORDER must survive too) and on a bare
    row that carries nothing but its weight (where the five annotations are all there is).
    """
    assert distill_reward.POLICIES == ("threshold", "best-of-n", "replication")
    assert distill_reward.SELECTION_KEYS == ("weight", "selected", "replicas", "policy", "threshold")
    assert (distill_reward.DEFAULT_THRESHOLD, distill_reward.DEFAULT_TOP_N, distill_reward.DEFAULT_REPLICATION_K) == (0.75, 2, 2)

    canonical: list[dict[str, Any]] = [_row(pmc_id="PMC1", weight=0.9), _row(pmc_id="PMC1", weight=0.2)]
    canonical_sets: list[tuple[str, ...]] = []
    for policy in distill_reward.POLICIES:
        emitted: list[dict[str, Any]] = distill_reward.select(canonical, policy=policy)
        assert all(tuple(row) == distill_reward.TRAIN_ROW_KEYS for row in emitted)  # order survives annotation
        canonical_sets.append(tuple(emitted[0]))
    assert len(set(canonical_sets)) == 1  # identical key tuples across policies

    bare: list[dict[str, Any]] = [{"weight": 0.9}, {"weight": 0.2}]
    bare_sets: list[frozenset[str]] = []
    for policy in distill_reward.POLICIES:
        emitted = distill_reward.select(bare, policy=policy)
        assert all(frozenset(row) == frozenset(distill_reward.SELECTION_KEYS) for row in emitted)
        bare_sets.append(frozenset(emitted[0]))
    assert len(set(bare_sets)) == 1

    values: dict[str, list[bool]] = {
        policy: [row["selected"] for row in distill_reward.select(canonical, policy=policy)] for policy in distill_reward.POLICIES
    }
    assert values["threshold"] == [True, False]  # the VALUES are where the policies differ
    assert values["best-of-n"] == [True, True]  # top_n defaults to 2, so a 2-row group keeps both
    assert values["replication"] == [True, True]  # weight > 0


def test_select_rejects_unknown_policy_and_out_of_range_knobs() -> None:
    """An unknown policy or an out-of-range knob fails LOUD, naming the valid set.

    Why: these knobs decide which examples train the LoRA, so a typo (``"best_of_n"``, a threshold
    of ``75``, a ``top_n`` of ``0``, a ``replication_k`` of ``30``) must stop the weigh with an
    actionable message rather than silently producing an empty or wildly over-replicated dataset
    that still looks like a success. Validation precedes any row work, so an empty corpus does not
    hide a bad knob, and a wrong TYPE is a ``TypeError`` (a call-contract violation) while an
    out-of-range value is a ``ValueError`` (a policy the module refuses).
    """
    rows: list[dict[str, Any]] = [_row(weight=0.9)]

    with pytest.raises(ValueError, match="best_of_n") as excinfo:
        distill_reward.select(rows, policy="best_of_n")
    message: str = str(excinfo.value)
    for policy in distill_reward.POLICIES:
        assert policy in message  # the valid set is named, so the fix is obvious
    with pytest.raises(ValueError, match="policy"):
        distill_reward.select([], policy="")  # knob validation precedes row handling
    with pytest.raises(ValueError, match="policy"):
        distill_reward.select(rows, policy=None)  # pyright: ignore[reportArgumentType]  # a non-str policy is simply unknown

    for bad_threshold in (-0.1, 1.5, 100.0):
        with pytest.raises(ValueError, match=r"\[0, 1\]"):
            distill_reward.select(rows, policy="threshold", threshold=bad_threshold)
    for wrong_type in ("0.75", True, float("nan"), float("inf")):
        with pytest.raises(TypeError):
            distill_reward.select(rows, policy="threshold", threshold=wrong_type)  # pyright: ignore[reportArgumentType]

    for bad_top_n in (0, -1):
        with pytest.raises(ValueError, match="top_n"):
            distill_reward.select(rows, policy="best-of-n", top_n=bad_top_n)
    for wrong_type in (1.5, "2", True):
        with pytest.raises(TypeError):
            distill_reward.select(rows, policy="best-of-n", top_n=wrong_type)  # pyright: ignore[reportArgumentType]

    for bad_k in (-1, 4, 30):
        with pytest.raises(ValueError, match=r"\[0, 3\]"):
            distill_reward.select(rows, policy="replication", replication_k=bad_k)
    for wrong_type in ("2", True, float("nan")):
        with pytest.raises(TypeError):
            distill_reward.select(rows, policy="replication", replication_k=wrong_type)  # pyright: ignore[reportArgumentType]

    assert distill_reward.select(rows, policy="replication", replication_k=3)[0]["selected"] is True  # the bounds are inclusive
    assert distill_reward.select(rows, policy="replication", replication_k=0)[0]["replicas"] == 1


def test_reward_config_is_frozen_and_select_does_not_mutate_rows() -> None:
    """The policy is immutable and ``select`` returns NEW dicts, leaving its input untouched.

    Why: a ``RewardConfig`` mutated between two rows of one export would silently break the
    cross-row comparability the deterministic reward exists to guarantee, and a ``select`` that
    annotated its input in place would make the caller's corpus depend on which policy ran last —
    so two policies could not be compared over one corpus, and a re-run would annotate already
    annotated rows. Frozenness is proven by direct assignment (``dataclasses.replace`` on a frozen
    dataclass does NOT raise, so it proves nothing).
    """
    config: distill_reward.RewardConfig = distill_reward.RewardConfig()
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.w_coverage = 0.9  # pyright: ignore[reportAttributeAccessIssue]  # a frozen policy rejects mutation, loudly
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.edge_ref = 10.0  # pyright: ignore[reportAttributeAccessIssue]
    assert hasattr(distill_reward.RewardConfig, "__slots__")  # slots: no per-instance __dict__ to smuggle state into

    rows: list[dict[str, Any]] = [_row(pmc_id="PMC1", weight=0.9), _row(pmc_id="PMC1", weight=0.2)]
    before: list[dict[str, Any]] = [dict(row) for row in rows]
    emitted: dict[str, list[dict[str, Any]]] = {policy: distill_reward.select(rows, policy=policy) for policy in distill_reward.POLICIES}

    assert rows == before  # no policy touched the caller's rows
    assert all(row["policy"] is None and row["selected"] is False for row in rows)  # ... so they stay unannotated
    for policy in distill_reward.POLICIES:
        assert all(emitted[policy][index] is not rows[index] for index in range(len(rows)))  # NEW dicts, not aliases
        assert [row["policy"] for row in emitted[policy]] == [policy] * 2

    reselected: list[dict[str, Any]] = distill_reward.select(emitted["threshold"], policy="replication")
    assert [row["policy"] for row in reselected] == ["replication"] * 2  # re-annotating overwrites, never accumulates
    assert [row["selected"] for row in emitted["threshold"]] == [True, False]  # the first annotation is unchanged


def test_emitted_rows_keep_the_preference_pairing_keys() -> None:
    """A weighed row keeps the pairing keys a future DPO/KTO path needs, with no re-recording.

    Why: TRL KTO consumes UNPAIRED ``{"prompt","completion","label"}`` data, and ``selected`` maps
    1:1 onto its ``label`` — so keeping ``pmc_id`` (the pairing key), the config identity hash,
    ``call_index`` and ``weight`` costs nothing now and is the difference between a corpus that can
    be re-used for preference training later and one that must be re-recorded. The config identity
    rides as the flattened ``outcome_config_yaml_sha256`` column: REQ-DS-5's canonical key set
    prefixes every outcome-derived column, and the prefix is what keeps it distinguishable from a
    record column of the same name.
    """
    records: list[dict[str, Any]] = [_record(run_id="inv1:PMC1", pmc_id="PMC1", call_index=3)]
    outcomes: list[dict[str, Any]] = [_outcome(run_id="inv1:PMC1", pmc_id="PMC1", config_yaml_sha256=hashlib.sha256(b"sections: []").hexdigest())]
    rows: list[dict[str, Any]] = _weighed(records, outcomes, edge_ref=3338.0)

    emitted: list[dict[str, Any]] = distill_reward.select(rows, policy="threshold", threshold=0.5)
    row: dict[str, Any] = emitted[0]
    assert (row["pmc_id"], row["call_index"]) == ("PMC1", 3)  # the record's own pairing keys
    assert row["outcome_config_yaml_sha256"] == hashlib.sha256(b"sections: []").hexdigest()  # the config identity
    assert isinstance(row["weight"], float)  # the reward
    assert row["weight"] > 0.0  # a matched, measured run earns weight
    assert isinstance(row["selected"], bool)  # KTO's `label`
    for key in ("pmc_id", "call_index", "weight", "selected", "outcome_config_yaml_sha256"):
        assert key in distill_reward.TRAIN_ROW_KEYS  # pinned in the canonical key set, not incidentally present


def test_read_ndjson_names_the_file_and_line_of_a_malformed_row(tmp_path: Path) -> None:
    """A malformed line fails LOUD, naming the FILE and the 1-based LINE NUMBER; blanks are skipped.

    Why: the corpus is append-only and a supervisor may be mid-write when a weigh reads it, so a
    torn last line is a live risk — silently truncating there would drop the newest examples and
    report a smaller corpus as if it were complete. Line numbers are PHYSICAL (a skipped blank line
    still counts), because that is what an editor shows the operator — and only ``\\n`` ends a line:
    U+2028/U+2029/U+0085 inside a value are ordinary characters (``str.splitlines()`` disagrees, and
    used to mis-report the number below). A line that parses to something other than an object is
    malformed too: every corpus line is one record or outcome.
    """
    corpus: Path = tmp_path / "records.ndjson"
    corpus.write_text('{"a": 1}\n\n   \n{"b": 2}\n', encoding="utf-8")
    assert distill_reward.read_ndjson(corpus) == [{"a": 1}, {"b": 2}]  # blank and whitespace-only lines carry nothing

    torn: Path = tmp_path / "torn.ndjson"
    torn.write_text('{"a": 1}\n\n{"run_id": "x", "ok": tru\n', encoding="utf-8")
    with pytest.raises(ValueError, match="malformed NDJSON") as excinfo:
        distill_reward.read_ndjson(torn)
    message: str = str(excinfo.value)
    assert str(torn) in message  # the FILE is named
    assert "line 3" in message  # ... and its PHYSICAL 1-based line number, blanks included

    separated: Path = tmp_path / "separated.ndjson"
    separated.write_text('{"text": "a\u2028b"}\n{"text": "c"}\n{"run_id": "x", "ok": tru\n', encoding="utf-8")
    with pytest.raises(ValueError, match=r"malformed NDJSON") as excinfo:
        distill_reward.read_ndjson(separated)
    shifted: str = str(excinfo.value)
    assert f"{separated} line 3:" in shifted  # the U+2028 on line 1 is NOT a line break: line 3 stays line 3
    assert f"{separated} line 1:" not in shifted  # splitlines() used to slice line 1 and blame it for the tear

    not_object: Path = tmp_path / "array.ndjson"
    not_object.write_text('{"a": 1}\n[1, 2]\n', encoding="utf-8")
    with pytest.raises(ValueError, match="line 2") as excinfo:
        distill_reward.read_ndjson(not_object)
    assert "expected a JSON object" in str(excinfo.value)
    for position, scalar in enumerate(("5\n", '"text"\n', "null\n", "true\n")):
        scalar_file: Path = tmp_path / f"scalar-{position}.ndjson"
        scalar_file.write_text(scalar, encoding="utf-8")
        with pytest.raises(ValueError, match="expected a JSON object"):
            distill_reward.read_ndjson(scalar_file)  # a scalar line is not a record

    blank: Path = tmp_path / "blank.ndjson"
    blank.write_text("\n\n", encoding="utf-8")
    assert distill_reward.read_ndjson(blank) == []  # an all-blank file is empty, not malformed
    empty: Path = tmp_path / "empty.ndjson"
    empty.write_text("", encoding="utf-8")
    assert distill_reward.read_ndjson(empty) == []

    with pytest.raises(FileNotFoundError):
        distill_reward.read_ndjson(tmp_path / "missing.ndjson")  # a missing corpus is an I/O error, never []
    with pytest.raises(TypeError):
        distill_reward.read_ndjson("records.ndjson")  # pyright: ignore[reportArgumentType]  # a call-contract violation


def test_ndjson_round_trips_the_unicode_line_separators_splitlines_would_break_on(tmp_path: Path) -> None:
    """U+2028, U+2029 and U+0085 inside a value are VALID NDJSON and round-trip losslessly.

    Why: the capture layer writes ``json.dumps(payload, ensure_ascii=False)`` plus one ``\\n``, so
    those three code points land RAW inside the string values of any LLM output over PMC full text
    (U+0085 also arises from latin-1 mojibake) and are perfectly valid NDJSON. ``str.splitlines()``
    treats all three as line breaks, so it sliced one valid record into two invalid ones: a good
    corpus became unreadable and aborted the weigh — the fail-loud contract firing on a non-error.
    Iterating the open handle gives ``\\n``-only semantics, matching both the writer in
    ``distill.py`` and ``is_outcome_file``.
    """
    separators: tuple[str, ...] = ("\u2028", "\u2029", "\u0085")  # escapes, not literals: keeps this file diff-safe
    rows: list[dict[str, Any]] = [
        _row(pmc_id=f"PMC{index}", weight=0.5, messages=[{"role": "assistant", "content": f"before{sep}after"}])
        for index, sep in enumerate(separators)
    ]
    corpus: Path = tmp_path / "separators.ndjson"
    assert distill_reward.write_ndjson(corpus, rows) == 3
    assert len(corpus.read_text(encoding="utf-8").split("\n")) == 4  # three PHYSICAL lines plus the trailing empty one

    round_tripped: list[dict[str, Any]] = distill_reward.read_ndjson(corpus)  # no raise: nothing was sliced
    assert round_tripped == rows  # lossless
    contents: list[str] = [str(row["messages"][0]["content"]) for row in round_tripped]
    assert contents == [f"before{sep}after" for sep in separators]  # each separator survives verbatim

    captured: Path = tmp_path / "captured.ndjson"
    recorder: distill.DistillRecorder = distill.DistillRecorder(captured, invocation_id="unicode")
    for separator in separators:
        recorder.record("agent", [{"role": "user", "content": f"a{separator}b"}])
    captured_rows: list[dict[str, Any]] = distill_reward.read_ndjson(captured)  # exercise the REAL capture writer
    assert [row["messages"][0]["content"] for row in captured_rows] == [f"a{separator}b" for separator in separators]


def test_is_outcome_file_detects_by_content_not_by_name(tmp_path: Path) -> None:
    """The discriminator is the first non-blank line's ``record_type``, never the filename.

    Why: an outcomes file that was renamed, relocated or merged into a differently named corpus
    must still be separated from the training records — ``distill-export`` globs every ``*.ndjson``
    in a directory, and one non-ChatML row in the exported dataset corrupts both consumers. Name-
    based detection would silently mis-partition on exactly the renamed file. An unreadable, empty,
    binary or malformed file is NOT an outcome file: this predicate only partitions, and
    ``read_ndjson`` is what fails loud on the malformed line afterwards.
    """
    outcome_line: str = json.dumps({"record_type": "outcome", "run_id": "inv1:PMC1"})
    record_line: str = json.dumps({"record_type": "record", "run_id": "inv1:PMC1"})

    misnamed_records: Path = tmp_path / "records.ndjson"
    misnamed_records.write_text(outcome_line + "\n", encoding="utf-8")
    assert distill_reward.is_outcome_file(misnamed_records) is True  # named like records, IS outcomes

    misnamed_outcomes: Path = tmp_path / "outcomes.ndjson"
    misnamed_outcomes.write_text(record_line + "\n", encoding="utf-8")
    assert distill_reward.is_outcome_file(misnamed_outcomes) is False  # named like outcomes, IS records

    leading_blanks: Path = tmp_path / "blanks.ndjson"
    leading_blanks.write_text("\n\n   \n" + outcome_line + "\n", encoding="utf-8")
    assert distill_reward.is_outcome_file(leading_blanks) is True  # the first NON-BLANK line decides

    v1_corpus: Path = tmp_path / "v1.ndjson"
    v1_corpus.write_text('{"messages": [], "purpose": "agent"}\n', encoding="utf-8")
    assert distill_reward.is_outcome_file(v1_corpus) is False  # a pre-v2 line has no record_type at all

    empty: Path = tmp_path / "empty.ndjson"
    empty.write_text("", encoding="utf-8")
    assert distill_reward.is_outcome_file(empty) is False
    blanks_only: Path = tmp_path / "blanks-only.ndjson"
    blanks_only.write_text("\n \n", encoding="utf-8")
    assert distill_reward.is_outcome_file(blanks_only) is False
    malformed: Path = tmp_path / "malformed.ndjson"
    malformed.write_text('{"record_type": "outco\n', encoding="utf-8")
    assert distill_reward.is_outcome_file(malformed) is False  # read_ndjson fails loud on it later
    not_object: Path = tmp_path / "list.ndjson"
    not_object.write_text('[{"record_type": "outcome"}]\n', encoding="utf-8")
    assert distill_reward.is_outcome_file(not_object) is False
    binary: Path = tmp_path / "binary.ndjson"
    binary.write_bytes(b"\xff\xfe\x00binary")
    assert distill_reward.is_outcome_file(binary) is False
    assert distill_reward.is_outcome_file(tmp_path / "missing.ndjson") is False  # absent: not an outcome file
    assert distill_reward.is_outcome_file(tmp_path) is False  # a directory is unreadable as a line source

    with pytest.raises(TypeError):
        distill_reward.is_outcome_file("outcomes.ndjson")  # pyright: ignore[reportArgumentType]  # a call-contract violation


def test_iter_record_files_returns_sorted_ndjson_paths(tmp_path: Path) -> None:
    """Every ``*.ndjson`` FILE under the directory, sorted by path; a missing directory is empty.

    Why: the corpus order must be a function of the directory's contents alone — never of
    filesystem enumeration order — or the emitted row order (and every rank-based policy) would
    drift between two weighs of one corpus. Non-``.ndjson`` siblings and a directory that happens
    to be named ``*.ndjson`` are excluded, because reading either would fail the weigh. Both kinds
    of NDJSON are returned: partitioning them is ``is_outcome_file``'s content-based job.
    """
    (tmp_path / "b-records.ndjson").write_text("{}\n", encoding="utf-8")
    (tmp_path / "a-outcomes.ndjson").write_text("{}\n", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("not a corpus\n", encoding="utf-8")
    (tmp_path / "nested.ndjson").mkdir()  # a directory named like a corpus file is not readable as one

    found: list[Path] = distill_reward.iter_record_files(tmp_path)
    assert found == [tmp_path / "a-outcomes.ndjson", tmp_path / "b-records.ndjson"]  # sorted, files only
    assert distill_reward.iter_record_files(tmp_path / "absent") == []  # "no corpus yet" is the caller's message
    with pytest.raises(TypeError):
        distill_reward.iter_record_files(str(tmp_path))  # pyright: ignore[reportArgumentType]


def test_join_matches_on_run_id_and_flags_unmatched_rows() -> None:
    """The join keeps EVERY record: matched rows carry the flattened outcome, unmatched rows nulls.

    Why: a record whose ``run_id`` is null (a v1 corpus, or a judge/reflexion row recorded outside
    a run scope) or whose outcome was never written (the run crashed before the supervisor's
    status decision) is still a recorded example. Dropping it would silently shrink the corpus and
    hide exactly the failure cases the negative examples are worth keeping, so it is emitted with
    ``outcome_matched = False``, every ``outcome_*`` column null, and a DEFINITIVE ``weight = 0.0``
    — no outcome exists to reward, so it can never earn one. Row order is record order, and
    ``stats`` is what tells ``distill-weigh`` whether zero matches means an empty corpus or a
    corpus it cannot join (REQ-DS-6).
    """
    records: list[dict[str, Any]] = [
        _record(run_id="inv1:PMC1", pmc_id="PMC1", call_index=0),
        _record(run_id="inv1:PMC2", pmc_id="PMC2", call_index=1),  # no outcome was ever written for this run
        _record(run_id=None, pmc_id="PMC3", call_index=2),  # recorded outside a run scope: unjoinable
    ]
    outcomes: list[dict[str, Any]] = [_outcome(run_id="inv1:PMC1", pmc_id="PMC1"), _outcome(run_id=None)]

    rows: list[dict[str, Any]]
    stats: dict[str, int]
    rows, stats = distill_reward.join_records_outcomes(records, outcomes)

    assert tuple(stats) == distill_reward.JOIN_STAT_KEYS
    assert stats == {"records": 3, "outcomes": 2, "matched": 1, "unmatched": 2, "duplicate_run_ids": 0}
    assert len(rows) == len(records)  # one row per record: nothing is dropped, nothing is duplicated
    assert [row["run_id"] for row in rows] == ["inv1:PMC1", "inv1:PMC2", None]  # record order is preserved
    assert all(tuple(row) == distill_reward.TRAIN_ROW_KEYS for row in rows)  # the full canonical key set on every row

    matched: dict[str, Any] = rows[0]
    assert matched["outcome_matched"] is True
    assert (matched["outcome_run_status"], matched["outcome_coverage_pct"], matched["outcome_edge_count"]) == ("MAPPED", 0.83, 3338)
    assert matched["outcome_tool_calls_total"] == 9  # the struct was flattened, not passed through
    assert matched["weight"] is None  # derived by the reward step, which needs a RewardConfig and an edge_ref
    assert (matched["selected"], matched["replicas"], matched["policy"], matched["threshold"]) == (False, 0, None, None)

    for position in (1, 2):
        unmatched: dict[str, Any] = rows[position]
        assert unmatched["outcome_matched"] is False
        assert all(unmatched[column] is None for column in distill_reward.OUTCOME_COLUMNS)  # every outcome column null
        assert (unmatched["weight"], unmatched["selected"], unmatched["replicas"]) == (0.0, False, 0)
        assert unmatched["messages"] == records[position]["messages"]  # the record's own columns survive
        assert unmatched["call_index"] == records[position]["call_index"]  # ... including the preference-pairing ones

    # Zero matched rows is a legitimate join result (the CLI turns it into exit 2, REQ-DS-6).
    unjoinable: list[dict[str, Any]]
    unjoinable, zero_stats = distill_reward.join_records_outcomes([_record(run_id="v1-only")], [_outcome(run_id="other")])
    assert zero_stats == {"records": 1, "outcomes": 1, "matched": 0, "unmatched": 1, "duplicate_run_ids": 0}
    assert [row["outcome_matched"] for row in unjoinable] == [False]

    empty_rows: list[dict[str, Any]]
    empty_rows, empty_stats = distill_reward.join_records_outcomes([], [])
    assert empty_rows == []
    assert empty_stats == {"records": 0, "outcomes": 0, "matched": 0, "unmatched": 0, "duplicate_run_ids": 0}

    # An unknown record key is KEPT (appended after the canonical set), never silently discarded.
    drifted: list[dict[str, Any]]
    drifted, _ = distill_reward.join_records_outcomes([_record(future_column="x")], [])
    assert drifted[0]["future_column"] == "x"
    assert tuple(drifted[0])[: len(distill_reward.TRAIN_ROW_KEYS)] == distill_reward.TRAIN_ROW_KEYS

    with pytest.raises(TypeError):
        distill_reward.join_records_outcomes(["not a mapping"], [])  # pyright: ignore[reportArgumentType]
    with pytest.raises(TypeError):
        distill_reward.join_records_outcomes([], [42])  # pyright: ignore[reportArgumentType]


def test_join_counts_duplicate_run_ids_instead_of_silently_overwriting() -> None:
    """A duplicate ``run_id`` keeps the LAST outcome AND increments ``duplicate_run_ids``.

    Why: the corpus is append-only, so a later line for one run supersedes an earlier one — but a
    duplicate means two supervisors wrote the same run's outcome (a re-run under one invocation id,
    or a torn append), and an operator must be able to SEE that rather than discover it as a
    weight that quietly changed. Last-wins with a counter is the semantics that keeps a re-weigh
    reproducible while making the collision visible in the manifest. An outcome with no ``run_id``
    is never indexed (it can join nothing) but still counts toward ``stats["outcomes"]``.
    """
    records: list[dict[str, Any]] = [_record(run_id="inv1:PMC1"), _record(run_id="inv1:PMC2")]
    outcomes: list[dict[str, Any]] = [
        _outcome(run_id="inv1:PMC1", coverage_pct=0.10, edge_count=10),
        _outcome(run_id="inv1:PMC2", coverage_pct=0.20),
        _outcome(run_id="inv1:PMC1", coverage_pct=0.95, edge_count=999),  # a later line for the same run
        _outcome(run_id=None, coverage_pct=0.50),  # unjoinable: counted, never indexed
    ]

    rows: list[dict[str, Any]]
    stats: dict[str, int]
    rows, stats = distill_reward.join_records_outcomes(records, outcomes)

    assert stats == {"records": 2, "outcomes": 4, "matched": 2, "unmatched": 0, "duplicate_run_ids": 1}
    assert (rows[0]["outcome_coverage_pct"], rows[0]["outcome_edge_count"]) == (0.95, 999)  # the LAST line won
    assert rows[1]["outcome_coverage_pct"] == 0.20  # an unduplicated run is untouched

    thrice: list[dict[str, Any]] = [_outcome(run_id="inv1:PMC1", coverage_pct=0.1), _outcome(run_id="inv1:PMC1", coverage_pct=0.2)]
    _, triple_stats = distill_reward.join_records_outcomes(records[:1], [*thrice, _outcome(run_id="inv1:PMC1", coverage_pct=0.3)])
    assert triple_stats["duplicate_run_ids"] == 2  # three lines for one run: two supersessions
    assert distill_reward.join_records_outcomes(records[:1], thrice)[1]["duplicate_run_ids"] == 1

    non_string: list[dict[str, Any]]
    non_string, id_stats = distill_reward.join_records_outcomes([_record(run_id=7)], [_outcome(run_id=7)])
    assert id_stats["matched"] == 0  # a non-string run id joins nothing on either side
    assert non_string[0]["outcome_matched"] is False


def test_flatten_outcome_emits_only_flat_stable_scalars() -> None:
    """Every outcome entry becomes flat ``outcome_``-prefixed SCALARS; no struct or list survives.

    Why: ``datasets`` infers a STRUCT column from the first block of the first file, so a sub-field
    that first appears in a later line of an append-only, multi-release corpus raises ``CastError``,
    and a key whose type varies is silently JSON-encoded into a string column
    (``JsonConfig.on_mixed_types = "use_json"``) — a silent demotion that corrupts a training corpus
    without an error. Flat scalars with a DECLARED type each are the only shape that survives, which
    is also why a wrong-typed legacy value flattens to null (the corpus's own "unmeasured"
    spelling) instead of creating a mixed-type column: ``reward`` still reads the nested outcome and
    fails loud there, so a malformed figure can never silently set a weight.
    """
    flat: dict[str, Any] = distill_reward.flatten_outcome(distill_reward.build_outcome(**_full_kwargs()))

    assert tuple(flat) == distill_reward.OUTCOME_COLUMNS  # the canonical column set, in order
    assert all(column.startswith(distill_reward.OUTCOME_COLUMN_PREFIX) for column in flat)
    assert all(value is None or isinstance(value, (str, int, float, bool)) for value in flat.values())  # scalars only
    assert not any(isinstance(value, (dict, list)) for value in flat.values())  # no struct, no list

    for key in distill.OUTCOME_KEYS:  # every captured entry is represented, so none can drift out
        assert any(column.startswith(f"{distill_reward.OUTCOME_COLUMN_PREFIX}{key}") for column in flat), key

    assert (flat["outcome_run_status"], flat["outcome_coverage_pct"], flat["outcome_edge_count"]) == ("MAPPED", 0.83, 3338)
    assert (flat["outcome_ok"], flat["outcome_measured"], flat["outcome_head"]) == (True, True, False)
    assert tuple(key for key in flat if key.startswith("outcome_tool_calls_")) == tuple(f"outcome_tool_calls_{sub}" for sub in distill.TOOL_CALL_KEYS)
    assert (flat["outcome_tool_calls_total"], flat["outcome_tool_calls_failed"], flat["outcome_tool_calls_redundant"]) == (9, 1, 1)
    assert (flat["outcome_gate_map_threshold"], flat["outcome_gate_biolink_threshold"], flat["outcome_gate_judge_threshold"]) == (0.25, 0.0, 0.5)
    assert tuple(key for key in flat if key.startswith("outcome_versions_")) == ("outcome_versions_tablassert", "outcome_versions_biolink_model")
    assert flat["outcome_versions_biolink_model"] is None or isinstance(flat["outcome_versions_biolink_model"], str)

    # The variable-shape entries become stable JSON TEXT, never a column per element.
    assert json.loads(flat["outcome_error_codes_json"]) == []
    assert json.loads(flat["outcome_judge_dimensions_json"]) == {"biolink_validity": 2, "schema_validity": 3}
    coded: dict[str, Any] = distill_reward.flatten_outcome(_outcome(error_codes=["BIO_LINK_INVALID"], judge_dimensions={"z": 1, "a": 2}))
    assert coded["outcome_error_codes_json"] == '["BIO_LINK_INVALID"]'
    assert coded["outcome_judge_dimensions_json"] == '{"a": 2, "z": 1}'  # keys sorted: byte-stable text

    # A wrong-typed legacy value flattens to null and cannot change the column set or its type.
    legacy: dict[str, Any] = distill_reward.flatten_outcome(
        _outcome(coverage_pct="high", edge_count=None, ok="yes", tool_calls=None, gate="0.25", error_codes="BIO_LINK_INVALID")
    )
    assert tuple(legacy) == distill_reward.OUTCOME_COLUMNS
    assert (legacy["outcome_coverage_pct"], legacy["outcome_edge_count"], legacy["outcome_ok"]) == (None, None, None)
    assert legacy["outcome_tool_calls_total"] is None  # an absent struct flattens to null sub-columns, not to a struct
    assert legacy["outcome_gate_map_threshold"] is None
    assert legacy["outcome_error_codes_json"] is None

    assert all(value is None for value in distill_reward.flatten_outcome({}).values())  # nothing captured: all null
    assert tuple(distill_reward.flatten_outcome({})) == distill_reward.OUTCOME_COLUMNS
    with pytest.raises(TypeError):
        distill_reward.flatten_outcome(["not", "a", "mapping"])  # pyright: ignore[reportArgumentType]


def test_train_row_key_set_is_canonical_and_fully_emitted(tmp_path: Path) -> None:
    """``TRAIN_ROW_KEYS`` is the canonical emitted set, and every row carries every key.

    Why: the derived layer inherits the capture layer's schema-uniformity rule for the same reason —
    ``datasets`` infers its features from the first block of the first file and raises ``CastError``
    on a column that first appears later, so a row whose key set depended on whether it matched an
    outcome would make the weighed dataset unloadable. Asserted end to end: through the join (both
    matched and unmatched rows), through every selection policy, and through a write/read
    round-trip, where the on-disk key order must be the canonical one.
    """
    expected: tuple[str, ...] = (*distill.RECORD_KEYS, "outcome_matched", *distill_reward.OUTCOME_COLUMNS, *distill_reward.SELECTION_KEYS)
    assert expected == distill_reward.TRAIN_ROW_KEYS  # the composition AND the order are pinned
    assert len(set(distill_reward.TRAIN_ROW_KEYS)) == len(distill_reward.TRAIN_ROW_KEYS)  # no duplicate column
    flattened: tuple[str, ...] = tuple(
        column for column in distill_reward.TRAIN_ROW_KEYS if column.startswith("outcome_") and column != "outcome_matched"
    )
    assert flattened == distill_reward.OUTCOME_COLUMNS  # every flattened column, and nothing else, carries the prefix
    assert "outcome_matched" in distill_reward.TRAIN_ROW_KEYS
    assert all(key in distill_reward.TRAIN_ROW_KEYS for key in distill.RECORD_KEYS)  # the record columns are all kept

    records: list[dict[str, Any]] = [_record(run_id="inv1:PMC1"), _record(run_id=None)]
    rows: list[dict[str, Any]]
    rows, _ = distill_reward.join_records_outcomes(records, [_outcome(run_id="inv1:PMC1")])
    for row in rows:
        assert tuple(row) == distill_reward.TRAIN_ROW_KEYS  # matched and unmatched alike
        assert all(key in row for key in distill_reward.TRAIN_ROW_KEYS)  # explicit nulls, never absent keys

    for policy in distill_reward.POLICIES:
        weighed: list[dict[str, Any]] = _weighed(records, [_outcome(run_id="inv1:PMC1")])
        for row in distill_reward.select(weighed, policy=policy):
            assert tuple(row) == distill_reward.TRAIN_ROW_KEYS

    written: Path = tmp_path / "nested" / "train.ndjson"
    count: int = distill_reward.write_ndjson(written, distill_reward.select(_weighed(records, [_outcome(run_id="inv1:PMC1")]), policy="threshold"))
    assert count == 2
    assert [tuple(row) for row in distill_reward.read_ndjson(written)] == [distill_reward.TRAIN_ROW_KEYS] * 2


def test_union_keys_and_normalize_rows_make_every_row_schema_identical() -> None:
    """The union makes every row's key set identical, with explicit nulls for absent keys.

    Why: ``load_dataset("json")`` infers ``features`` from the FIRST BLOCK OF THE FIRST FILE and
    casts every later block to it, raising ``CastError`` when a later block of an append-only corpus
    carries a column the inferred schema lacks. Normalizing every row to the corpus-wide union makes
    the inferred schema correct by construction — which is what lets a v1 corpus (no ``run_id``) sit
    beside a v2 one and still export. First-appearance order (not sorted) keeps the corpus's own
    canonical order, and normalizing never changes a VALUE's type: that is
    ``detect_type_conflicts``'s job to report.
    """
    v1: dict[str, Any] = {"messages": [{"role": "user", "content": "hi"}], "purpose": "agent"}
    v2: dict[str, Any] = {"purpose": "agent", "run_id": "inv1:PMC1", "schema_version": 2}
    rows: list[dict[str, Any]] = [v1, v2, {"extra": True}]

    union: tuple[str, ...] = distill_reward.union_keys(rows)
    assert union == ("messages", "purpose", "run_id", "schema_version", "extra")  # first-appearance order
    normalized: list[dict[str, Any]] = distill_reward.normalize_rows(rows)
    assert [tuple(row) for row in normalized] == [union] * 3  # every row is schema-identical
    assert normalized[0]["run_id"] is None  # an absent key becomes an explicit null, not a missing column
    assert normalized[1]["messages"] is None
    assert normalized[2]["extra"] is True
    assert normalized[0]["messages"] == v1["messages"]  # a present value is passed through untouched
    assert isinstance(normalized[1]["schema_version"], int)  # ... and keeps its type

    uniform: list[dict[str, Any]] = [_record(), _record(run_id="inv1:PMC2")]
    assert distill_reward.union_keys(uniform) == distill.RECORD_KEYS  # a uniform corpus unions to its own canonical order
    assert distill_reward.normalize_rows(uniform) == uniform  # normalization is a no-op on a uniform corpus

    assert distill_reward.union_keys([]) == ()  # an empty corpus has no schema
    assert distill_reward.normalize_rows([]) == []
    assert distill_reward.union_keys([{}, {}]) == ()
    assert distill_reward.normalize_rows([{}]) == [{}]

    weighed: list[dict[str, Any]]
    weighed, _ = distill_reward.join_records_outcomes([_record()], [_outcome()])
    assert distill_reward.union_keys(weighed) == distill_reward.TRAIN_ROW_KEYS  # a joined corpus is already uniform

    with pytest.raises(TypeError):
        distill_reward.union_keys(["not a mapping"])  # pyright: ignore[reportArgumentType]


def test_detect_type_conflicts_flags_a_silently_demoted_column() -> None:
    """A column whose non-null type varies across the corpus is reported, per key.

    Why: ``JsonConfig.on_mixed_types = "use_json"`` means ``datasets`` JSON-encodes such a column
    into a STRING instead of raising — a silent type demotion that corrupts a training corpus with
    no error anywhere. Scanning the WHOLE corpus (not the first block) is the point, because a type
    that first varies in block two is invisible to first-block inference. Nulls are skipped rather
    than reported as a type: null is the corpus's "unmeasured" spelling and every nullable column
    carries it, so counting it would flag every legitimate nullable column. ``int`` and ``float``
    are NOT conflated — that alternation is exactly the demotion hazard.
    """
    rows: list[dict[str, Any]] = [
        {"edge_count": 3338, "coverage_pct": 0.83, "pmc_id": "PMC1", "messages": [{"role": "user", "content": "hi"}]},
        {"edge_count": "3338", "coverage_pct": None, "pmc_id": "PMC2", "messages": [{"role": "user", "content": "yo"}]},
        {"edge_count": None, "pmc_id": None, "gate": {"map_threshold": 0.25}},
    ]
    observed: dict[str, set[str]] = distill_reward.detect_type_conflicts(rows)

    assert observed["edge_count"] == {"int", "str"}  # the demoted column, flagged with both type names
    assert len(observed["edge_count"]) > 1  # the caller's fail-loud condition
    assert observed["coverage_pct"] == {"float"}  # a null beside a float is not a conflict
    assert observed["pmc_id"] == {"str"}  # ... and neither is a null beside a string
    assert observed["messages"] == {"list"}
    assert observed["gate"] == {"dict"}
    assert set(observed) == {"edge_count", "coverage_pct", "pmc_id", "messages", "gate"}  # every observed key

    numeric: dict[str, set[str]] = distill_reward.detect_type_conflicts([{"weight": 1}, {"weight": 1.5}])
    assert numeric["weight"] == {"int", "float"}  # NOT conflated: datasets would stringify this column

    all_null: dict[str, set[str]] = distill_reward.detect_type_conflicts([{"unmeasured": None}, {"unmeasured": None}])
    assert all_null["unmeasured"] == set()  # an all-null column has no observed type
    assert distill_reward.detect_type_conflicts([]) == {}  # an empty corpus has no columns
    assert distill_reward.detect_type_conflicts([{}]) == {}

    weighed: list[dict[str, Any]]
    weighed, _ = distill_reward.join_records_outcomes([_record(run_id="inv1:PMC1"), _record(run_id=None)], [_outcome(run_id="inv1:PMC1")])
    clean: dict[str, set[str]] = distill_reward.detect_type_conflicts(weighed)
    assert all(len(names) <= 1 for names in clean.values()), {key: names for key, names in clean.items() if len(names) > 1}
    assert clean["outcome_coverage_pct"] == {"float"}  # a matched figure and a null: one type
    assert clean["weight"] == {"float"}  # the definitive 0.0 and the not-yet-derived null

    with pytest.raises(TypeError):
        distill_reward.detect_type_conflicts([["not", "a", "mapping"]])  # pyright: ignore[reportArgumentType]


def test_detect_type_conflicts_does_not_conflate_bool_and_int() -> None:
    """``bool`` and ``int`` count as ONE type; ``bool`` and ``str`` (or ``int``/``float``) do not.

    Why: Python's ``bool`` IS an ``int`` subclass and ``datasets`` maps the two predictably
    (``Value("bool")``/``Value("int64")``), so a column alternating ``True`` and ``1`` is not the
    silent-demotion hazard this scan exists to catch — flagging it would cry wolf on every flag
    column beside a count and train operators to ignore the real finding. The distinction still has
    to hold against a genuine conflict, which is why the string and float cases are asserted here
    too: collapsing bool into int must not collapse the scan's sensitivity with it.
    """
    mixed_bool_int: dict[str, set[str]] = distill_reward.detect_type_conflicts([{"ok": True}, {"ok": 1}, {"ok": False}, {"ok": 0}])
    assert mixed_bool_int["ok"] == {"int"}  # one type, so no conflict
    assert all(len(names) <= 1 for names in mixed_bool_int.values())

    bool_only: dict[str, set[str]] = distill_reward.detect_type_conflicts([{"selected": True}, {"selected": False}])
    assert bool_only["selected"] == {"int"}  # reported under the one canonical name for the int family

    bool_and_text: dict[str, set[str]] = distill_reward.detect_type_conflicts([{"ok": True}, {"ok": "yes"}])
    assert bool_and_text["ok"] == {"int", "str"}  # a truthy STRING beside a flag is a real conflict

    bool_and_float: dict[str, set[str]] = distill_reward.detect_type_conflicts([{"ok": True}, {"ok": 1.0}])
    assert bool_and_float["ok"] == {"int", "float"}  # bool collapses into int, not into float

    records: list[dict[str, Any]] = [_record(run_id="inv1:PMC1"), _record(run_id=None)]
    annotated: list[dict[str, Any]] = distill_reward.select(_weighed(records, [_outcome(run_id="inv1:PMC1")]), policy="threshold")
    flagged: dict[str, set[str]] = distill_reward.detect_type_conflicts(annotated)
    assert flagged["selected"] == {"int"}  # the bool columns stay clean
    assert flagged["outcome_matched"] == {"int"}
    assert flagged["weight"] == {"float"}  # and a weight is always a float: select coerces, and a null is skipped


def test_write_ndjson_overwrites_with_a_stable_key_order_and_returns_the_count(tmp_path: Path) -> None:
    """``write_ndjson`` creates the parent, OVERWRITES, keeps canonical key order, returns the count.

    Why: the derived layer is a reproducible build artifact of one reward config plus one policy, so
    a re-weigh must replace the dataset rather than append to it — appending would silently double
    every example and mix two schemas in one file. Canonical key order on disk is what makes two
    weighs byte-identical and what ``datasets`` infers its schema from, so it comes from
    ``TRAIN_ROW_KEYS`` rather than from whatever order the caller's dict happened to have. A value
    JSON cannot serialize raises instead of being stringified: fail-soft ``default=str`` belongs to
    the capture layer, and here it would quietly turn a structured column into text. The write is
    atomic (sibling temp + ``os.replace``), so a failure on a LATER row leaves no truncated dataset
    behind: US-006 points ``datasets``/a trainer at this path, and a half-written ``train.ndjson``
    would be consumed silently as a smaller training set instead of raising.
    """
    nested: Path = tmp_path / "out" / "deep" / "train.ndjson"  # the parent need not exist
    rows: list[dict[str, Any]] = [_row(pmc_id="PMC1", weight=0.9), _row(pmc_id="PMC2", weight=0.1)]
    assert distill_reward.write_ndjson(nested, rows) == 2  # the row count
    assert nested.is_file()
    lines: list[str] = nested.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2  # one object per line, no outer array, no separators
    assert [tuple(json.loads(line)) for line in lines] == [distill_reward.TRAIN_ROW_KEYS] * 2  # canonical order on disk

    assert distill_reward.write_ndjson(nested, rows[:1]) == 1  # overwrite, never append
    assert len(nested.read_text(encoding="utf-8").splitlines()) == 1

    scrambled: dict[str, Any] = dict(reversed(list(_row(weight=0.5).items())))
    reordered: Path = tmp_path / "reordered.ndjson"
    distill_reward.write_ndjson(reordered, [scrambled])
    assert tuple(json.loads(reordered.read_text(encoding="utf-8").splitlines()[0])) == distill_reward.TRAIN_ROW_KEYS

    extra: Path = tmp_path / "extra.ndjson"
    distill_reward.write_ndjson(extra, [{**_row(), "future_column": "x"}])
    written_keys: tuple[str, ...] = tuple(json.loads(extra.read_text(encoding="utf-8").splitlines()[0]))
    assert written_keys == (*distill_reward.TRAIN_ROW_KEYS, "future_column")  # an extra key is kept, after the canonical set

    unicode_path: Path = tmp_path / "unicode.ndjson"
    distill_reward.write_ndjson(unicode_path, [_row(pmc_id="PMCéµø")])
    assert "PMCéµø" in unicode_path.read_text(encoding="utf-8")  # ensure_ascii=False: text stays readable and byte-stable

    empty: Path = tmp_path / "empty.ndjson"
    assert distill_reward.write_ndjson(empty, []) == 0
    assert empty.read_text(encoding="utf-8") == ""  # the destination is still (re)created
    assert distill_reward.read_ndjson(empty) == []  # ... and round-trips

    assert distill_reward.read_ndjson(reordered)[0]["weight"] == 0.5  # a round-trip preserves values

    with pytest.raises(TypeError):
        distill_reward.write_ndjson(tmp_path / "bad.ndjson", [{"weight": {1, 2}}])  # a set is not serializable: loud, not stringified
    assert not (tmp_path / "bad.ndjson").exists()  # a failed write leaves no destination, not even an empty one
    with pytest.raises(TypeError):
        distill_reward.write_ndjson(tmp_path / "bad.ndjson", ["not a mapping"])  # pyright: ignore[reportArgumentType]
    with pytest.raises(TypeError):
        distill_reward.write_ndjson("train.ndjson", rows)  # pyright: ignore[reportArgumentType]

    later: Path = tmp_path / "nested" / "train.ndjson"
    with pytest.raises(TypeError, match="not JSON serializable"):
        distill_reward.write_ndjson(later, [*rows, {"weight": {1, 2}}])  # the bad value is in the LAST row
    assert not later.exists()  # rows 1..n did NOT reach the destination: nothing partial, nothing truncated
    assert list(later.parent.iterdir()) == []  # ... and the sibling temp was cleaned up, not left behind

    intact: Path = tmp_path / "intact.ndjson"
    assert distill_reward.write_ndjson(intact, rows) == 2
    before: str = intact.read_text(encoding="utf-8")
    with pytest.raises(TypeError):
        distill_reward.write_ndjson(intact, [*rows, {"weight": {1, 2}}])
    assert intact.read_text(encoding="utf-8") == before  # the previous dataset survives intact, un-truncated
    assert distill_reward.read_ndjson(intact) == rows  # ... and is still readable


def test_select_requires_a_weighed_row() -> None:
    """A row with a missing, null or mistyped ``weight`` fails LOUD instead of selecting on nothing.

    Why: ``weight`` is the only thing a policy ranks on, and the join deliberately leaves a matched
    row's weight null (it has neither a ``RewardConfig`` nor an ``edge_ref`` in scope). Defaulting
    that null to ``0.0`` would silently empty the training set — or, with a ``0.0`` threshold,
    select rows that were never measured — so an unweighed corpus is a caller bug and says so,
    naming the row's position.
    """
    with pytest.raises(ValueError, match="null 'weight'"):
        distill_reward.select([_row(weight=None)], policy="threshold")  # exactly what the join emits for a matched row

    no_key: dict[str, Any] = _row()
    del no_key["weight"]
    with pytest.raises(ValueError, match="no 'weight' key"):
        distill_reward.select([no_key], policy="threshold")

    joined: list[dict[str, Any]]
    joined, _ = distill_reward.join_records_outcomes([_record(run_id="inv1:PMC1")], [_outcome(run_id="inv1:PMC1")])
    with pytest.raises(ValueError, match="row 0"):
        distill_reward.select(joined, policy="best-of-n")  # a joined-but-unweighed corpus cannot be selected

    unjoinable: list[dict[str, Any]]
    unjoinable, _ = distill_reward.join_records_outcomes([_record(run_id=None)], [])
    assert distill_reward.select(unjoinable, policy="threshold")[0]["selected"] is False  # a definitive 0.0 IS a weight

    for bad in ("high", True, float("nan"), float("inf")):
        with pytest.raises(TypeError):
            distill_reward.select([_row(weight=bad)], policy="replication")
    with pytest.raises(TypeError):
        distill_reward.select(["not a mapping"], policy="threshold")  # pyright: ignore[reportArgumentType]
    with pytest.raises(ValueError, match="row 1"):
        distill_reward.select([_row(weight=0.5), _row(weight=None)], policy="threshold")  # the position is named
