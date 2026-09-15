from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from tablassert.enums import Comparisons, EncodingMethods, Repositories
from tablassert.errors import DOCS_URL, flatten_pydantic_error
from tablassert.models import Encoding, Provenance, Regex, Reindex, Text
from tablassert.progress import PipelineProgress


def _section_task(p: PipelineProgress) -> Any:
    """helper to read the active section task fields."""
    return next(t for t in p.progress.tasks if t.id == p.section_task)


def test_start_shows_in_flight_item_without_incrementing() -> None:
    """start marks the in flight item on the detail line without incrementing the counter."""
    p: PipelineProgress = PipelineProgress(total_stages=1)
    start, advance, sub_step = p.section_loop(3, "Collect")
    task: Any = _section_task(p)
    assert task.completed == 0
    start("my_table.yaml · abc123de")
    task = _section_task(p)
    assert task.completed == 0
    assert "my_table.yaml" in p.detail_text.plain
    assert "abc123de" in p.detail_text.plain
    assert callable(advance)
    assert callable(sub_step)


def test_advance_ticks_counter_without_changing_detail() -> None:
    """advance ticks the counter without changing the detail line."""
    p: PipelineProgress = PipelineProgress(total_stages=1)
    start, advance, _ = p.section_loop(3, "Collect")
    start("my_table.yaml · abc123de")
    advance()
    task: Any = _section_task(p)
    assert task.completed == 1
    assert "my_table.yaml" in p.detail_text.plain


def test_start_advance_cycle_shows_current_not_previous() -> None:
    """repeated start advance cycles keep detail synced to the current item not the last."""
    p: PipelineProgress = PipelineProgress(total_stages=1)
    start, advance, _ = p.section_loop(3, "Subgraph")
    start("first.yaml · deadbeef")
    advance()
    start("second.yaml · cafebabe")
    task: Any = _section_task(p)
    assert task.completed == 1
    assert "second.yaml" in p.detail_text.plain
    assert "cafebabe" in p.detail_text.plain
    assert "first.yaml" not in p.detail_text.plain


def test_sub_step_appends_phase_to_detail() -> None:
    """sub_step appends a phase tag to the detail line without ticking the counter."""
    p: PipelineProgress = PipelineProgress(total_stages=1)
    start, _, sub_step = p.section_loop(3, "Subgraph")
    start("first.yaml · deadbeef")
    sub_step("resolve")
    task: Any = _section_task(p)
    assert task.completed == 0
    assert "→" in p.detail_text.plain
    assert "resolve" in p.detail_text.plain


def test_advance_clears_sub_step_suffix() -> None:
    """advance clears the sub_step suffix so the detail line returns to just the current item."""
    p: PipelineProgress = PipelineProgress(total_stages=1)
    start, advance, sub_step = p.section_loop(3, "Subgraph")
    start("first.yaml · deadbeef")
    sub_step("resolve")
    advance()
    assert "→" not in p.detail_text.plain
    assert "resolve" not in p.detail_text.plain
    assert "first.yaml" in p.detail_text.plain


def test_stage_prints_completion_line_for_previous_stage(capsys: Any) -> None:
    """stage emits a green completion line for the previous stage before advancing."""
    p: PipelineProgress = PipelineProgress(total_stages=2)
    with p:
        p.stage("First")
        p.stage("Second")
    captured: Any = capsys.readouterr()
    assert "Stage 1" in captured.err
    assert "FIRST" in captured.err


def test_dynamic_loop_never_overflows_when_a_phase_total_resets_to_zero() -> None:
    """a same-phase real->0 total transition must not overflow the bar.

    Regression test: phase 0 emits a real total (the class-file count) for the
    per-file scatter, then ``total=0`` with ``completed`` in the millions for the
    equiv-merge. Every determinate bar (``total is not None``) must keep
    ``completed <= total``; indeterminate bars (``total is None``) are exempt.
    Also pins the phase-1 out-of-order clamp and the phase-2 upper-bound snap.
    """
    p: PipelineProgress = PipelineProgress(total_stages=1)
    update = p.dynamic_loop("Build")
    # Phase 0: per-file scatter (real total) then equiv-merge (total=0, huge completed).
    update(0, 1, 20, "a.jsonl")
    update(0, 20, 20, "t.jsonl")
    update(0, 1_000_000, 0, "merging equivalents")
    update(0, 5_000_000, 0, "merging equivalents")
    # Phase 1: out-of-order cumulative rows, indeterminate (clamp keeps monotonic).
    update(1, 900, 0, "x · 900 rows")
    update(1, 3, 0, "x · 3 rows")
    # Phase 2: upper-bound total then final snap to the exact count.
    update(2, 500, 800, "writing")
    update(2, 800, 800, "wrote 800 records")
    for task in p.progress.tasks:
        if task.total is not None:
            assert task.completed <= task.total


# code -> zero-arg callable that raises ValidationError (verbatim guard-test constructors, tests/test_models.py:285-363).
TRIGGERS: dict[str, Callable[[], object]] = {
    "comparison-bad-comparator-type": lambda: Reindex(column="A", comparison=Comparisons.EQ, comparator=5),
    "comparison-nonnumeric-comparator": lambda: Reindex(column="A", comparison=Comparisons.GT, comparator="x"),
    "config-rows-and-row-slice-conflict": lambda: Text(
        local=Path("./t.tsv"), url=["https://example.com/t.tsv"], kind="text", rows=[1], row_slice=[1, 5]
    ),  # pyright: ignore
    "regex-bad-pattern": lambda: Regex(pattern="(", replacement=" "),
    "regex-bad-replacement": lambda: Regex(pattern="ok", replacement="("),
    "encoding-bad-excel-column": lambda: Encoding(method=EncodingMethods.COLUMN, encoding="not_a_col_123"),  # pyright: ignore
    "encoding-bad-remove-entry": lambda: Encoding(method=EncodingMethods.VALUE, encoding="x", remove=["("]),  # pyright: ignore
    "provenance-bad-pmc-id": lambda: Provenance(repo=Repositories.PUBMED_CENTRAL, publication="12345"),  # pyright: ignore
}


@pytest.mark.parametrize("code", sorted(TRIGGERS))
def test_flatten_pydantic_error_surfaces_code_and_docs_url(code: str) -> None:
    """A coded validation error must surface its stable ``[code]`` plus docs URL.

    Why: operators triaging a failed config need the machine-stable error code and
    a direct link to its docs, not pydantic's generic ``[value_error]`` bucket.
    Pydantic v2 stores the raised ``TablassertValidationError`` under
    ``err["ctx"]["error"]`` with ``.code`` intact, so ``flatten_pydantic_error``
    can recover the code and label the fragment ``[<code>]``. ``_Coded.__str__``
    already appends the docs URL to the message, so it survives into ``msg``;
    this test still asserts it explicitly to lock the contract.
    """
    with pytest.raises(ValidationError) as exc:
        TRIGGERS[code]()
    flat: str = flatten_pydantic_error(exc.value)
    assert f"[{code}]" in flat
    assert f"{DOCS_URL}{code}" in flat


def test_flatten_pydantic_error_non_coded_falls_back_to_type() -> None:
    """A non-coded pydantic error keeps the existing ``[<type>]`` bracket.

    Why: only Tablassert-coded errors carry ``ctx.error.code``; a plain pydantic
    error (e.g. a missing required field, ``ctx is None``) must fall back to the
    pydantic ``type`` so the output never crashes on ``ctx`` access and never
    mislabels a builtin error as ``[value_error]``. Output must stay one line.
    """
    with pytest.raises(ValidationError) as exc:
        Reindex.model_validate({"column": "A"})
    flat: str = flatten_pydantic_error(exc.value)
    assert "[missing]" in flat
    assert "[value_error]" not in flat
    assert "\n" not in flat
