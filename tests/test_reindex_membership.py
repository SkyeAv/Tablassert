"""Membership reindex (``join`` / ``antijoin``): semantics, op compilation, and failure paths.

WHY this module exists: the scalar ``reindex`` operators compare one cell against one value,
so "keep exactly these identifiers" was not expressible. ``lib.reindex_membership`` implements
the new comparisons as polars semi/anti hash joins, and ``lib.reindex_op`` decides which op a
config entry compiles to. Both carry semantics that are easy to regress silently (null handling,
row order, column dtypes, the scalar op argument shapes that the run cache digests), so each is
pinned here rather than only at the build level.

The frames below are named ``column_<n>`` because that is what ``idx`` produces upstream: the ops
run on raw, pre-resolution cells.
"""

from __future__ import annotations

import operator
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from tablassert import lib
from tablassert.errors import TablassertError
from tablassert.ingests import to_sections
from tablassert.models import Reindex


def _lf(**columns: list[Any]) -> pl.LazyFrame:
    return pl.LazyFrame(columns)  # pyright: ignore[reportArgumentType]


def _values(lf: pl.LazyFrame, col: str = "column_1") -> list[Any]:
    return lf.collect()[col].to_list()


def test_join_keeps_matching_rows_in_source_order() -> None:
    """join keeps exactly the matching rows, in the order the source had them.

    WHY: KGX row order and section output are downstream-observed, so the filter must not shuffle
    or multiply rows the way a naive inner join would.
    """
    df = _lf(column_1=["a", "b", "c", "a"], column_2=[1, 2, 3, 4])
    out = lib.reindex_membership(df, "column_1", ["a", "c"])
    assert _values(out) == ["a", "c", "a"]
    assert out.collect()["column_2"].to_list() == [1, 3, 4]


def test_antijoin_drops_matching_rows() -> None:
    """antijoin is the exact complement of join over the same non-null keys."""
    df = _lf(column_1=["a", "b", "c", "a"])
    assert _values(lib.reindex_membership(df, "column_1", ["a", "c"], anti=True)) == ["b"]


def test_join_drops_null_keys_and_antijoin_keeps_them() -> None:
    """Null cells follow join rules: they never match, so join drops them and antijoin keeps them.

    WHY: this is the one place membership deliberately differs from ``ne`` (where a null compares to
    null and the row is dropped). Pinning it stops a future "consistency" tweak from silently
    changing which rows a build emits.
    """
    df = _lf(column_1=["a", None, "b"])
    assert _values(lib.reindex_membership(df, "column_1", ["a"])) == ["a"]
    assert _values(lib.reindex_membership(df, "column_1", ["a"], anti=True)) == [None, "b"]


def test_empty_string_identifier_matches_empty_cells() -> None:
    """``""`` is an ordinary cell value: antijoin drops empty cells, join keeps only them."""
    df = _lf(column_1=["a", "", "b"])
    assert _values(lib.reindex_membership(df, "column_1", [""])) == [""]
    assert _values(lib.reindex_membership(df, "column_1", [""], anti=True)) == ["a", "b"]


def test_duplicate_keys_do_not_multiply_rows() -> None:
    """Repeated identifiers in the config behave like the deduplicated set."""
    df = _lf(column_1=["a", "a", "b"])
    assert _values(lib.reindex_membership(df, "column_1", ["a", "a", "a"])) == ["a", "a"]
    assert _values(lib.reindex_membership(df, "column_1", ["a", "a"], anti=True)) == ["b"]


def test_membership_preserves_columns_dtypes_and_adds_no_key_column() -> None:
    """The join key helper column is dropped again and every source column keeps its dtype."""
    df = _lf(column_1=[3, 4], column_2=["x", "y"])
    out = lib.reindex_membership(df, "column_1", ["3"])
    assert out.collect_schema().names() == ["column_1", "column_2"]
    assert out.collect_schema()["column_1"] == pl.Int64
    assert out.collect()["column_1"].to_list() == [3]


def test_non_string_column_matches_string_identifiers() -> None:
    """A typed source column (parquet Int64) is cast to String instead of failing on join keys."""
    df = _lf(column_1=[3, 4, 5])
    assert _values(lib.reindex_membership(df, "column_1", ["4", "5"])) == [4, 5]


def test_float_cells_read_in_their_cast_string_form() -> None:
    """Pinned caveat: Float64 cells read as ``"3.0"``, not ``"3"``.

    WHY: an identifier list spelled for integers silently matches nothing against a float column;
    the test documents the exact string form the cast produces so the behaviour cannot drift.
    """
    df = _lf(column_1=[3.0, 4.5])
    assert _values(lib.reindex_membership(df, "column_1", ["3.0"])) == [3.0]
    assert _values(lib.reindex_membership(df, "column_1", ["3"])) == []


def test_membership_and_scalar_filters_combine() -> None:
    """Multiple ``reindex`` entries still AND together, membership included, in either order."""
    df = _lf(column_1=["a", "b", "c"], column_2=[1.0, 5.0, 9.0])
    both = df.pipe(lib.reindex, "column_2", operator.lt, 6).pipe(lib.reindex_membership, "column_1", ["a", "b"])
    assert _values(both) == ["a", "b"]
    reversed_ = df.pipe(lib.reindex_membership, "column_1", ["a", "b", "c"]).pipe(lib.reindex, "column_2", operator.gt, 6)
    assert _values(reversed_) == ["c"]


def test_missing_column_raises_loudly() -> None:
    """Negative: a membership filter on an absent column errors instead of filtering nothing."""
    df = _lf(column_1=["a"])
    with pytest.raises(TablassertError, match="reindex-membership-column-missing"):
        lib.reindex_membership(df, "column_9", ["a"])


def test_empty_key_list_raises_loudly() -> None:
    """Negative: an empty identifier list would drop every row (or keep every row), so it errors.

    ``models.Reindex`` already refuses it at config time; this is the op-level backstop for any
    caller that builds the op list by hand.
    """
    df = _lf(column_1=["a"])
    with pytest.raises(TablassertError, match="comparison-membership-empty"):
        lib.reindex_membership(df, "column_1", [])


def test_reindex_op_compiles_join_and_antijoin() -> None:
    """join compiles to a non-anti membership op, antijoin to the anti one, keyed by column letter."""
    join = lib.reindex_op(Reindex(column="B", comparison="join", comparator=["x", "y"]))  # pyright: ignore
    anti = lib.reindex_op(Reindex(column="A", comparison="antijoin", comparator=["x"]))  # pyright: ignore
    assert join == (lib.reindex_membership, ("column_2", ["x", "y"], False))
    assert anti == (lib.reindex_membership, ("column_1", ["x"], True))


@pytest.mark.parametrize(
    ("comparison", "comparator", "expected"),
    [
        # The scalar shapes are exactly what the pre-membership comprehension emitted: four args
        # with cast=False for eq/ne, three args (cast defaults to True) for the numeric operators.
        ("eq", "NA", (lib.reindex, ("column_1", operator.eq, "NA", False))),
        ("ne", "NA", (lib.reindex, ("column_1", operator.ne, "NA", False))),
        ("lt", 0.05, (lib.reindex, ("column_1", operator.lt, 0.05))),
        ("ge", 100, (lib.reindex, ("column_1", operator.ge, 100))),
    ],
)
def test_reindex_op_keeps_scalar_op_shapes(comparison: str, comparator: Any, expected: tuple[Any, tuple[Any, ...]]) -> None:
    """Regression pin: widening the comparator type changed nothing for the six scalar comparisons.

    WHY: the run cache digests op tuples positionally, so an extra or reordered argument would
    invalidate every existing stored prefix even where behaviour is identical.
    """
    assert lib.reindex_op(Reindex(column="A", comparison=comparison, comparator=comparator)) == expected  # pyright: ignore


def test_reindex_op_rejects_membership_with_scalar_comparator() -> None:
    """Negative: the compile step refuses a membership comparison whose comparator is not a list.

    ``Reindex``'s validator enforces this pairing, so the guard only bites for hand-built models;
    it must fail loudly rather than emit an op that polars rejects later.
    """
    entry: Reindex = Reindex.model_construct(column="A", comparison="join", comparator="x")
    with pytest.raises(TablassertError, match="comparison-membership-comparator-required"):
        lib.reindex_op(entry)


def test_tcode_emits_membership_op_in_source_position() -> None:
    """A real config compiles membership into the source-op stage, alongside the scalar ``ne``."""
    config: dict[str, Any] = {
        "template": {
            "source": {
                "kind": "text",
                "local": "./data.tsv",
                "url": ["https://example.com/data.tsv"],
                "delimiter": "\t",
                "reindex": [
                    {"column": "A", "comparison": "ne", "comparator": "NA"},
                    {"column": "B", "comparison": "antijoin", "comparator": ["mapk1"]},
                ],
            },
            "statement": {
                "subject": {"method": "column", "encoding": "A"},
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "B"},
            },
            "provenance": {"repo": "PMC", "publication": "PMC0000000"},
        }
    }
    section: dict[str, Any] = to_sections(config, Path("t.yaml"))[0]
    tcode: lib.Tcode = lib.Tcode.model_validate({**section, "store": Path("out.parquet")})
    ops: list[tuple[Any, tuple[Any, ...]]] = tcode._source_ops()
    cleaned: list[tuple[Any, tuple[Any, ...]]] = tcode.clean(ops)
    assert (lib.reindex, ("column_1", operator.ne, "NA", False)) in cleaned
    assert (lib.reindex_membership, ("column_2", ["mapk1"], True)) in cleaned
    assert [op[0] for op in cleaned].index(lib.reindex_membership) > [op[0] for op in cleaned].index(lib.reindex)
