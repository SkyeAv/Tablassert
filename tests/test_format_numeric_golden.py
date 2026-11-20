from __future__ import annotations

import random

import polars as pl

from tablassert import lib, rs


def test_format_scientific_4e_golden_parity() -> None:
    """Rust ``format_scientific_4e`` matches Python ``{:.4e}`` across edge and random floats."""
    rng = random.Random(42)
    values: list[float | None] = [None, 0.0, -0.0, 1.0, -1.0, 1e-3, 0.05, 1e-12, 1e12, 1e100, 1e-100, float("nan"), float("inf"), float("-inf")]
    values.extend(rng.uniform(-1e6, 1e6) for _ in range(5000))
    values.extend(10.0 ** rng.uniform(-15, 5) for _ in range(5000))

    expected: list[str | None] = [None if v is None else f"{v:.4e}" for v in values]
    assert rs.format_scientific_4e(values) == expected


def test_format_numeric_end_to_end_p_value_parity() -> None:
    """``lib.format_numeric`` preserves scientific notation and null values via the Rust path."""
    lf: pl.LazyFrame = pl.DataFrame({"p_value": [0.001, 0.05, None, 1e-12], "gene": ["a", "b", "c", "d"]}).lazy()
    out: pl.DataFrame = lib.format_numeric(lf).collect()
    assert out["p_value"].to_list() == ["1.0000e-03", "5.0000e-02", None, "1.0000e-12"]
