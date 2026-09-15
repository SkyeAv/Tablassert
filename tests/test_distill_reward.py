"""Outcome assembly (``distill_reward``): pure builders over a run's terminal state.

Every test here is BASE-ENV by design (no ``importorskip`` anywhere): ``distill_reward`` is pure
stdlib + ``yaml``, so the whole module is exercised exactly the way CI runs it — unlike the
supervisor-level end-to-end coverage in ``tests/test_agent_supervisor.py``, which skips without the
``[agent]`` extra and is therefore only ever bonus evidence.
"""

from __future__ import annotations

import hashlib
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version
from typing import Any

import pytest
import yaml

from tablassert import distill, distill_reward


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
