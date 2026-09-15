"""US-006: bounded reflexion and judge prompts (R5).

Every test is PURE and offline: ``compact_coverage_report`` and ``_truncate_for_prompt`` run on
hand-built dicts, and ``llm_propose_config_edit`` is driven by a recording plain-callable fake
model (``_call_judge`` calls ``judge_model(prompt)`` directly), so no smolagents / ``[agent]``
extra is needed and nothing sleeps or opens a socket.

WHY this file exists: fleet distill data showed the three largest of 534 LLM calls were ALL
reflexion prompts of 1.6M-2.3M characters (689,241 / 524,336 input tokens) -- past every model's
context window, so each failed outright after paying for the serialization.
"""

from __future__ import annotations

import copy
import json
from typing import Any

from tablassert.agent import (
    COVERAGE_PROMPT_CHARS,
    MAX_PROMPT_CONFIG_CHARS,
    MAX_PROMPT_CONTEXT_CHARS,
    MAX_PROMPT_REPORT_CHARS,
    UNRESOLVED_CAP,
    _build_judge_prompt,
    _truncate_for_prompt,
    compact_coverage_report,
    llm_propose_config_edit,
)


def _huge_report() -> dict[str, object]:
    """A coverage report with ~50,000 top-level unresolved terms across 3 sections x 2 columns."""
    terms: list[str] = [f"unresolved-gene-{i:05d}" for i in range(50_000)]
    sections: list[dict[str, object]] = []
    for section_index in range(3):
        per_column: dict[str, object] = {}
        for column_index in range(2):
            per_column[f"col{column_index}"] = {
                "coverage": 0.1,
                "total": 25_000,
                "resolved": 2_500,
                "unresolved": terms[section_index * 8_000 : (section_index + 1) * 8_000],
                "method": "column",
            }
        sections.append({"overall": 0.1, "measured": True, "per_column": per_column, "unresolved": terms[:9_000]})
    return {"overall": 0.1, "min": 0.1, "measured": True, "sections": sections, "unresolved": terms, "per_column": sections[0]["per_column"]}


def test_compact_coverage_report_caps_every_unresolved_list() -> None:
    """Every reachable ``unresolved`` list is capped with exactly one visible ``+N more`` marker."""
    compact: dict[str, object] = compact_coverage_report(_huge_report())

    top: object = compact["unresolved"]
    assert isinstance(top, list)
    assert len(top) == UNRESOLVED_CAP + 1
    assert top[-1] == f"+{50_000 - UNRESOLVED_CAP} more"
    assert top[:UNRESOLVED_CAP] == [f"unresolved-gene-{i:05d}" for i in range(UNRESOLVED_CAP)], "sorted order kept"

    sections: object = compact["sections"]
    assert isinstance(sections, list)
    for section in sections:
        assert isinstance(section, dict)
        section_list: object = section["unresolved"]
        assert isinstance(section_list, list)
        assert section_list[-1] == f"+{9_000 - UNRESOLVED_CAP} more"
        per_column: object = section["per_column"]
        assert isinstance(per_column, dict)
        for entry in per_column.values():
            assert isinstance(entry, dict)
            column_list: object = entry["unresolved"]
            assert isinstance(column_list, list)
            assert column_list[-1] == f"+{8_000 - UNRESOLVED_CAP} more"

    # The back-compat top-level per_column is capped too.
    back_compat: object = compact["per_column"]
    assert isinstance(back_compat, dict)
    back_column = back_compat["col1"]
    assert isinstance(back_column, dict)
    back_list: object = back_column["unresolved"]
    assert isinstance(back_list, list)
    assert back_list[-1] == f"+{8_000 - UNRESOLVED_CAP} more"

    # A list at or below the cap passes through with no marker.
    small: dict[str, object] = {"overall": 1.0, "measured": True, "unresolved": ["a", "b"], "per_column": {}, "sections": []}
    assert compact_coverage_report(small)["unresolved"] == ["a", "b"]


def test_compact_coverage_report_is_pure_and_non_mutating() -> None:
    """The input report is never modified; capped lists are fresh and nested owners are copies."""
    original: dict[str, object] = _huge_report()
    snapshot: dict[str, object] = copy.deepcopy(original)

    compact: dict[str, object] = compact_coverage_report(original)

    assert original == snapshot, "the input report must be untouched"
    assert compact is not original
    top: object = compact["unresolved"]
    original_top: object = original["unresolved"]
    assert isinstance(top, list)
    assert isinstance(original_top, list)
    assert top is not original_top, "a capped list must be a fresh list"
    sections: object = compact["sections"]
    assert isinstance(sections, list)
    assert sections[0] is not (original["sections"][0] if isinstance(original["sections"], list) else None), "an owner that gained a key is a copy"


def test_compact_coverage_report_tolerates_partial_shapes() -> None:
    """Totality: empty, non-dict, malformed sections/per_column, and the improve-loop fallback all work."""
    assert compact_coverage_report({}) == {}
    assert compact_coverage_report(None) == {}  # type: ignore[arg-type]
    assert compact_coverage_report([1, 2]) == {}  # type: ignore[arg-type]
    # A non-list sections value and non-dict per_column entries degrade, never raise.
    result: dict[str, object] = compact_coverage_report({"sections": "nope", "per_column": {"col": 7}, "unresolved": "not-a-list"})
    assert isinstance(result, dict)
    assert result["unresolved"] == "not-a-list", "a non-list unresolved is retained as-is"
    assert result["sections"] == "nope"
    assert result["per_column"] == {"col": 7}
    # The improve loop's fallback shape (agent.py's {"per_column": {}, "unresolved": []}) round-trips.
    fallback: dict[str, object] = compact_coverage_report({"per_column": {}, "unresolved": []})
    assert fallback["unresolved"] == []
    assert fallback["unresolved_count"] == 0
    assert fallback["per_column"] == {}


def test_compact_coverage_report_records_original_unresolved_count() -> None:
    """An ``unresolved_count`` sibling names each ORIGINAL list length so the scale signal survives."""
    compact: dict[str, object] = compact_coverage_report(_huge_report())
    assert compact["unresolved_count"] == 50_000
    sections: object = compact["sections"]
    assert isinstance(sections, list)
    for section in sections:
        assert isinstance(section, dict)
        assert section["unresolved_count"] == 9_000
        per_column: object = section["per_column"]
        assert isinstance(per_column, dict)
        for entry in per_column.values():
            assert isinstance(entry, dict)
            assert entry["unresolved_count"] == 8_000
            assert entry["total"] == 25_000, "aggregate counts preserved verbatim"
            assert entry["resolved"] == 2_500
            assert entry["coverage"] == 0.1
    assert compact["overall"] == 0.1
    assert compact["measured"] is True


def test_truncate_for_prompt_marks_omitted_chars() -> None:
    """Fitting text passes through; overflow carries the verbatim marker; bad limits raise."""
    assert _truncate_for_prompt("short", 100, what="anything") == "short"
    text: str = "x" * 250
    bounded: str = _truncate_for_prompt(text, 200, what="current config")
    assert bounded == "x" * 200 + "\n…[current config truncated: 50 of 250 chars omitted]"
    for bad in (0, -1):
        try:
            _truncate_for_prompt("t", bad, what="w")
            raise AssertionError("ValueError expected")
        except ValueError:
            pass


def test_reflexion_prompt_is_bounded_for_huge_coverage_report() -> None:
    """A 50,000-term report yields a <60,000-char prompt with both markers and original counts."""
    prompts: list[str] = []

    def recording_model(prompt: str) -> str:
        prompts.append(prompt)
        return "not a config"

    huge_context: str = "z" * (MAX_PROMPT_CONTEXT_CHARS + 50_000)
    huge_config: str = "y" * (MAX_PROMPT_CONFIG_CHARS + 30_000)
    result: str | None = llm_propose_config_edit(huge_config, _huge_report(), huge_context, model=recording_model)

    assert result is None, "the fake answer is not a valid config, so None is the contract"
    assert len(prompts) == 1
    prompt: str = prompts[0]
    assert len(prompt) < 60_000, f"reflexion prompt must be bounded, got {len(prompt)} chars"
    assert f"+{50_000 - UNRESOLVED_CAP} more" in prompt, "term-cap marker present verbatim"
    assert "…[current config truncated:" in prompt
    assert "…[article/table context truncated:" in prompt
    assert '"unresolved_count": 50000' in prompt, "the ORIGINAL scale is visible to the model"


def test_compact_coverage_report_is_deterministic() -> None:
    """Two compactions of the same report serialize byte-identically (no sampling, no re-sorting)."""
    report: dict[str, object] = _huge_report()
    first: str = json.dumps(compact_coverage_report(report), default=str, sort_keys=True)
    second: str = json.dumps(compact_coverage_report(report), default=str, sort_keys=True)
    assert first == second


def test_compact_coverage_report_overflow_drops_per_column_lists() -> None:
    """Beyond ``max_chars`` the per-column ``unresolved`` lists are dropped, counts kept, no raise.

    This drives the overflow path the huge-report regression never reaches (that report compacts
    to ~6K chars): very long individual terms make the term caps alone exceed 8,000 chars.
    """
    long_terms: list[str] = [f"term-{i}-" + "x" * 900 for i in range(30)]
    per_column: dict[str, object] = {"col": {"coverage": 0.0, "total": 30, "resolved": 0, "unresolved": long_terms, "method": "column"}}
    report: dict[str, object] = {
        "overall": 0.0,
        "min": 0.0,
        "measured": True,
        "sections": [{"overall": 0.0, "measured": True, "per_column": per_column, "unresolved": long_terms}],
        "unresolved": long_terms,
        "per_column": per_column,
    }
    compact: dict[str, object] = compact_coverage_report(report)
    assert isinstance(compact, dict), "the overflow path must stay total"
    # Per-column unresolved lists are gone (everywhere), counts and fractions retained.
    top_columns: object = compact["per_column"]
    assert isinstance(top_columns, dict)
    dropped = top_columns["col"]
    assert isinstance(dropped, dict)
    assert "unresolved" not in dropped
    assert dropped["unresolved_count"] == 30
    assert dropped["total"] == 30
    assert dropped["coverage"] == 0.0
    sections: object = compact["sections"]
    assert isinstance(sections, list)
    section = sections[0]
    assert isinstance(section, dict)
    section_columns: object = section["per_column"]
    assert isinstance(section_columns, dict)
    assert "unresolved" not in section_columns["col"]
    # Top-level and section-level lists keep their capped terms plus the visible marker.
    top_list: object = compact["unresolved"]
    assert isinstance(top_list, list)
    assert top_list[-1] == f"+{30 - UNRESOLVED_CAP} more"
    assert compact["unresolved_count"] == 30


def test_compact_coverage_report_many_sections_never_raises() -> None:
    """A 400-section report compacts without raising even though it still exceeds ``max_chars``."""
    section: dict[str, object] = {
        "overall": 0.5,
        "measured": True,
        "per_column": {"c": {"coverage": 0.5, "total": 10, "resolved": 5, "unresolved": [f"t{i}" for i in range(10)], "method": "column"}},
        "unresolved": [f"t{i}" for i in range(10)],
    }
    report: dict[str, object] = {
        "overall": 0.5,
        "min": 0.5,
        "measured": True,
        "sections": [dict(section) for _ in range(400)],
        "unresolved": [f"t{i}" for i in range(10)],
    }
    compact: dict[str, object] = compact_coverage_report(report)
    sections: object = compact["sections"]
    assert isinstance(sections, list)
    assert len(sections) == 400
    assert isinstance(json.dumps(compact, default=str), str), "still serializable after the strip"


def test_reflexion_prompt_survives_overflow_coverage_report() -> None:
    """An over-budget report still yields exactly ONE bounded LLM call, never a silent skip."""
    prompts: list[str] = []

    def recording_model(prompt: str) -> str:
        prompts.append(prompt)
        return "not a config"

    section: dict[str, object] = {
        "overall": 0.0,
        "measured": True,
        "per_column": {"c": {"coverage": 0.0, "total": 5, "resolved": 0, "unresolved": ["u1", "u2", "u3", "u4", "u5"], "method": "column"}},
        "unresolved": ["u1", "u2", "u3", "u4", "u5"],
    }
    report: dict[str, object] = {
        "overall": 0.0,
        "min": 0.0,
        "measured": True,
        "sections": [dict(section) for _ in range(400)],
        "unresolved": ["u1", "u2", "u3", "u4", "u5"],
    }
    result: str | None = llm_propose_config_edit("config: x", report, "context", model=recording_model)

    assert result is None, "the fake answer is not a valid config, so None is the contract"
    assert len(prompts) == 1, "the overflow report must not silently disable the reflexion call"
    prompt: str = prompts[0]
    assert len(prompt) < 60_000, f"prompt must stay bounded, got {len(prompt)} chars"
    assert prompt.count("…[coverage report truncated:") == 1, "the serialized block is visibly truncated once"


def test_judge_prompt_uses_compact_audit_report() -> None:
    """The judge prompt serializes the compact audit report, bounded at ``MAX_PROMPT_REPORT_CHARS``."""
    report: dict[str, Any] = {"ok": True, "unresolved": [f"term-{i}" for i in range(100_000)], "coverage_pct": 0.5, "kgx_path": "/tmp/x"}
    prompt: str = _build_judge_prompt("config: yes", report, {"steps": 3})
    assert len(prompt) < MAX_PROMPT_REPORT_CHARS + 2_000, f"judge prompt must be bounded, got {len(prompt)}"
    assert f"+{100_000 - UNRESOLVED_CAP} more" in prompt, "audit compaction marker present"
    assert "kgx_path" not in prompt, "only the COMPACT_AUDIT_KEYS survive"
    assert "coverage_pct" in prompt


def test_map_coverage_still_returns_full_unresolved_lists() -> None:
    """Public report APIs stay uncapped: compaction happens only at the prompt boundary.

    This drives the unmeasurable empty-report path (a real fullmap build is heavyweight); the
    measured-report contract — full uncapped ``unresolved`` lists from ``map_coverage`` — is
    pinned by ``tests/test_agent_coverage.py`` (``zzznotreal`` survives in the full lists), and
    ``map_coverage`` itself is untouched by US-006.
    """
    from pathlib import Path

    from tablassert.agent import map_coverage

    config: str = "source:\n  kind: text\n  local: /does/not/exist.tsv\nstatement:\n  subject: {method: column, encoding: A}\n  predicate: associated_with\n  object: {method: column, encoding: B}\nprovenance: {repo: PMC, publication: PMC1}\n"
    # An unreadable source is UNMEASURABLE -> the 4-key empty report (never raises).
    report: dict[str, object] = map_coverage(config, fullmap=Path("/tmp/fm"), workdir=Path("/tmp"))
    assert report["unresolved"] == []
    assert report["per_column"] == {}
    # And the empty-report compaction keeps that exact shape (no invention of sections).
    assert compact_coverage_report(report)["unresolved"] == []


def test_coverage_prompt_chars_constants_match_spec() -> None:
    """The four budgets match the spec's numbers and reuse ``UNRESOLVED_CAP`` (no second cap constant)."""
    assert COVERAGE_PROMPT_CHARS == 8_000
    assert MAX_PROMPT_CONTEXT_CHARS == 40_000
    assert MAX_PROMPT_CONFIG_CHARS == 8_000
    assert UNRESOLVED_CAP == 20
