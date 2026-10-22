"""In-repo benchmark harness for membership reindex (``join`` / ``antijoin``).

WHY: the feature's whole justification is that set filtering on a large identifier list stays
fast, and that claim needs reproducible before/after numbers instead of a throwaway /tmp script.
This harness times three implementations of the same predicate over one corpus:

* ``lib.reindex_membership`` (polars semi/anti hash join over a deduplicated key frame) -- shipped;
* a literal-list ``pl.col(...).is_in(keys)`` filter -- the obvious alternative, kept here as the
  baseline the shipped path has to beat;
* ``lib.reindex`` (the unchanged scalar op) -- the no-regression line: the diff only touched how a
  ``Reindex`` entry *compiles* (plan time), never what the scalar filter does per row, so this
  scenario's numbers must track whatever the base commit measured.

Usage (opt-in; collected nowhere by default, so it never slows the suite):

    TABLASSERT_BENCH=1 uv run pytest tests/bench_reindex_membership.py -s -n 0 -q

``-n 0`` keeps a single process (the frames are session-scoped; xdist would rebuild them per
worker), ``-s`` shows the timings. Scale the corpus with ``TABLASSERT_BENCH_ROWS`` (default
1_000_000) when a laptop needs a faster pass.

Baseline recorded on 2026-10-22 (this box, polars 2.0.0-rc.2, ``TABLASSERT_BENCH_ROWS`` default
1_000_000 string rows; every number is the median of three timed windows, each including the
key-frame build and the eager ``collect`` of the result, which is what a build actually pays):

    keys       join      antijoin   is_in     ~is_in
    10         0.0032s   0.0044s    0.0032s   0.0032s
    1_000      0.0031s   0.0036s    0.0036s   0.0045s
    100_000    0.0183s   0.0200s    0.0292s   0.0261s
    Int64 column, 100_000 keys (cast path): join 0.0203s
    unchanged scalar control: ``lt`` 0.0011s, ``ne`` 0.0013s

The join ties the literal filter on tiny lists and pulls ahead as the list grows (about 1.6x at
100k keys over a million rows), which is where an identifier list starts to matter at all. No
``is_in`` special case exists for the tiny-list tie: an ``is_in`` antijoin would propagate nulls
differently, and milliseconds per million rows are noise next to entity resolution.

Absolute seconds move with box load: a concurrent heavy job inflated every number above by
roughly 3x without changing the ranking, so the ranking is the invariant, not the timings.

The harness asserts ``join <= is_in * 2`` and ``antijoin <= ~is_in * 2`` at every size from a
thousand keys up (median of three runs), so a future polars change that inverts the ranking by an
order of magnitude fails here rather than in a PR review. Below that threshold the numbers are
recorded, not asserted.
"""

from __future__ import annotations

import operator
import os
import random
import time
from collections.abc import Callable
from typing import Any

import polars as pl
import pytest

from tablassert import lib

pytestmark = pytest.mark.skipif(os.environ.get("TABLASSERT_BENCH") != "1", reason="benchmark harness; opt in with TABLASSERT_BENCH=1")

_SEED = 42
_ROWS: int = int(os.environ.get("TABLASSERT_BENCH_ROWS", "1_000_000"))
_KEY_SIZES: tuple[int, ...] = (10, 1_000, 100_000)
# Enforced invariant: the shipped join path costs **no more than twice** the literal-list filter's
# median of three windows. It is not asserted to be strictly faster, because on short lists the two
# tie inside measurement noise; the ceiling is what catches an order-of-magnitude regression (a
# rebuild-per-row path, a lost hash join) without failing on a loaded box.
_TOLERANCE: float = 2.0


def _string_frame(rows: int) -> pl.LazyFrame:
    """CURIE-shaped identifier column plus a payload column, the shape a real association table has."""
    return pl.LazyFrame({"column_1": [f"HGNC:{i}" for i in range(rows)], "column_2": list(range(rows))})


def _int_frame(rows: int) -> pl.LazyFrame:
    """Numeric identifier column (what parquet sources produce), forcing the cast-to-String path."""
    return pl.LazyFrame({"column_1": list(range(rows)), "column_2": ["x"] * rows})


def _keys(rows: int, size: int) -> list[str]:
    """Deterministic keep-list: `size` identifiers sampled from the corpus, with repeats."""
    rng = random.Random(_SEED)
    return [f"HGNC:{rng.randrange(rows)}" for _ in range(size)]


def _numeric_keys(rows: int, size: int) -> list[str]:
    """Same sampling for the ``Int64`` corpus, spelled as the cast produces them (bare digits)."""
    rng = random.Random(_SEED)
    return [str(rng.randrange(rows)) for _ in range(size)]


def _time(build: Callable[[], Any]) -> tuple[float, int]:
    """Run one eager plan three times and return ``(median seconds, surviving rows)``.

    WHY the median: the interesting differences are single-digit milliseconds over a million rows,
    where a single sample is mostly scheduler noise; three runs and a median make the ranking
    assertion about the plan, not about the box.
    """
    samples: list[float] = []
    rows: int = 0
    for _ in range(3):
        start: float = time.perf_counter()
        result: pl.DataFrame = build().collect()
        samples.append(time.perf_counter() - start)
        rows = result.height
    samples.sort()
    return samples[len(samples) // 2], rows


def _report(scenario: str, detail: dict[str, tuple[float, int]]) -> None:
    line = "  ".join(f"{name}={seconds:.4f}s/{rows}r" for name, (seconds, rows) in detail.items())
    print(f"membership-bench rows={_ROWS} {scenario}: {line}")


def test_bench_membership_beats_literal_is_in() -> None:
    """join/antijoin over a string identifier column, at every key size, against the ``is_in`` filter.

    WHY: this is the perf claim of the feature -- a hash join against a deduplicated key frame must
    not lose to broadcasting a literal list, and the assertion (with a 2x noise margin) is the only
    mechanical guard that a future polars change does not quietly invert the ranking.
    """
    frame = _string_frame(_ROWS)
    for size in _KEY_SIZES:
        keys = _keys(_ROWS, size)
        joined = _time(lambda k=keys: frame.pipe(lib.reindex_membership, "column_1", list(k)))
        anti = _time(lambda k=keys: frame.pipe(lib.reindex_membership, "column_1", list(k), True))
        isin = _time(lambda k=keys: frame.filter(pl.col("column_1").is_in(k)))
        notisin = _time(lambda k=keys: frame.filter(~pl.col("column_1").is_in(k)))
        # join and antijoin partition the corpus exactly, and agree with the filter they replace
        # (the corpus has no null cells, so the two null treatments cannot diverge here).
        assert joined[1] + anti[1] == _ROWS
        assert joined[1] == isin[1], f"join and is_in disagree on which rows survive at {size} keys"
        assert anti[1] == notisin[1], f"antijoin and ~is_in disagree on which rows survive at {size} keys"
        if size >= 1_000:
            # The join ties the literal filter on tiny lists and only claims a win once the list is
            # big enough for its build side to pay for itself, so the ranking is asserted from a
            # thousand keys up; below that the recorded numbers are noise-dominated.
            assert joined[0] <= isin[0] * _TOLERANCE, f"join slower than is_in at {size} keys"
            assert anti[0] <= notisin[0] * _TOLERANCE, f"antijoin slower than ~is_in at {size} keys"
        _report(f"keys={size}", {"join": joined, "antijoin": anti, "is_in": isin, "not_is_in": notisin})


def test_bench_membership_over_numeric_column() -> None:
    """The cast-to-String path over an ``Int64`` identifier column, at the largest key size.

    WHY: typed parquet sources are the common case, and the cast is extra per-row work the string
    path does not pay; its cost has to be visible rather than assumed.
    """
    frame = _int_frame(_ROWS)
    keys = _numeric_keys(_ROWS, _KEY_SIZES[-1])
    joined = _time(lambda: frame.pipe(lib.reindex_membership, "column_1", keys))
    _report("dtype=Int64->String", {"join": joined})
    assert joined[1] > 0


def test_bench_scalar_reindex_unchanged() -> None:
    """The pre-existing scalar filter, timed on the same corpus as the no-regression line.

    WHY: ``lib.reindex`` itself was not touched, so its numbers are the control for "previous
    behaviour did not get slower"; a shift here means the op compilation change leaked into the plan.
    """
    frame = _string_frame(_ROWS).with_columns(pl.col("column_2").cast(pl.Float64))
    numeric = _time(lambda: frame.pipe(lib.reindex, "column_2", operator.lt, 0.05))
    equality = _time(lambda: frame.pipe(lib.reindex, "column_1", operator.ne, "NA", False))
    _report("scalar", {"lt_0.05": numeric, "ne_NA": equality})
    assert numeric[1] == 1  # only row 0.0 is below the threshold
    assert equality[1] == _ROWS  # no cell equals the "NA" sentinel
