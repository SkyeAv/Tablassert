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
