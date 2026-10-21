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
from pydantic import ValidationError

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
        ("le", 1.5, (lib.reindex, ("column_1", operator.le, 1.5))),
        ("gt", 2, (lib.reindex, ("column_1", operator.gt, 2))),
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


def test_reindex_op_routes_regex_entries() -> None:
    """A `regex: true` entry still compiles to `reindex_regex`, including its negate flag.

    WHY: `reindex_op` is the single dispatcher over all three filter families now, so the regex arm
    main shipped (`eq` keeps matches, `ne` negates) has to survive the refactor with the same tuple
    shape, or stored run-cache digests for regex configs would move.
    """
    keep = lib.reindex_op(Reindex(column="B", comparison="eq", comparator="^n/?a$", regex=True))  # pyright: ignore
    drop = lib.reindex_op(Reindex(column="A", comparison="ne", comparator="^junk$", regex=True))  # pyright: ignore
    assert keep == (lib.reindex_regex, ("column_2", "^n/?a$", False))
    assert drop == (lib.reindex_regex, ("column_1", "^junk$", True))


def test_membership_rejects_regex_comparator() -> None:
    """Negative: `regex` is meaningless on a membership entry, which matches fixed strings.

    WHY: main's validator rejected `regex` outside `eq`/`ne` from the numeric arm, and membership
    short-circuits before that arm, so the rule had to be restated here or `join` + `regex: true`
    would have configured a silently ignored pattern.
    """
    for comparison in ("join", "antijoin"):
        with pytest.raises(ValidationError, match="comparison-regex-requires-eq-ne"):
            Reindex(column="A", comparison=comparison, comparator=["x"], regex=True)  # pyright: ignore


def test_tcode_compiles_all_three_filter_families() -> None:
    """One section can mix a scalar, a regex and a membership condition, in that order.

    WHY: the three arms are mutually exclusive `if`s in the dispatcher, so a mixed config is the only
    shape that proves an earlier arm cannot swallow a later entry.
    """
    config: dict[str, Any] = {
        "template": {
            "source": {
                "kind": "text",
                "local": "./data.tsv",
                "url": ["https://example.com/data.tsv"],
                "delimiter": "\t",
                "reindex": [
                    {"column": "A", "comparison": "ne", "comparator": "NA"},
                    {"column": "B", "comparison": "eq", "comparator": "^\\d{4}$", "regex": True},
                    {"column": "C", "comparison": "antijoin", "comparator": ["mapk1"]},
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
    cleaned: list[tuple[Any, tuple[Any, ...]]] = tcode.clean(tcode._source_ops())
    filters: list[tuple[Any, tuple[Any, ...]]] = [op for op in cleaned if op[0] in (lib.reindex, lib.reindex_regex, lib.reindex_membership)]
    assert filters == [
        (lib.reindex, ("column_1", operator.ne, "NA", False)),
        (lib.reindex_regex, ("column_2", "^\\d{4}$", False)),
        (lib.reindex_membership, ("column_3", ["mapk1"], True)),
    ]


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


def test_join_string_path_returns_input_schema_unchanged() -> None:
    """The String fast path adds no projection: names, order and dtypes are the source's own.

    WHY: the fast path is chosen by dtype (`isinstance(schema[col], pl.String)`), so it needs its
    own schema assertion -- the Int64 test only proves the cast branch cleans up after itself.
    """
    df = _lf(column_1=["a", "b"], column_2=[1, 2])
    for anti in (False, True):
        out = lib.reindex_membership(df, "column_1", ["a"], anti=anti)
        assert out.collect_schema() == df.collect_schema()


def test_membership_survives_a_shuffled_source_and_keeps_left_order() -> None:
    """``maintain_order="left"`` is what makes source order a guarantee rather than an observation.

    WHY: polars documents default join order as unspecified ("might differ across Polars versions or
    even between different runs") and the dependency is an open range, so an unordered engine could
    reshuffle a section's rows without failing any other test. A shuffled corpus is the only shape
    that can tell left order from engine-chosen order.
    """
    identifiers = [f"HGNC:{i}" for i in range(400)]
    shuffled = [identifiers[i] for i in (7, 300, 1, 250, 99, 4, 180, 2)]
    df = _lf(column_1=shuffled)
    out = lib.reindex_membership(df, "column_1", ["HGNC:300", "HGNC:1", "HGNC:99"])
    assert _values(out) == ["HGNC:300", "HGNC:1", "HGNC:99"]


def test_nested_column_raises_loudly() -> None:
    """Negative: list/array/struct cells have no single string identity, so membership errors out.

    WHY: polars raises `InvalidOperationError` for casting a List column to String, and a Struct
    silently casts to a brace literal. Both would surface as an opaque engine error (or a filter that
    matches nothing) rather than the repo's coded error naming the column.
    """
    for dtype, frame in (
        (pl.List(pl.Int64), pl.LazyFrame({"column_1": [[1, 2]]})),
        (pl.Struct({"a": pl.Int64}), pl.LazyFrame({"column_1": [{"a": 1}]})),
        (pl.Array(pl.Int64, 2), pl.LazyFrame({"column_1": pl.Series([[1, 2]], dtype=pl.Array(pl.Int64, 2))})),
        (pl.Object, pl.LazyFrame({"column_1": pl.Series([object()], dtype=pl.Object)})),
        (pl.Binary, pl.LazyFrame({"column_1": pl.Series([b"abc"], dtype=pl.Binary)})),
    ):
        assert frame.collect_schema()["column_1"] == dtype
        with pytest.raises(TablassertError, match="reindex-membership-column-dtype-unsupported"):
            lib.reindex_membership(frame, "column_1", ["1"])


def test_string_columns_take_the_join_without_a_cast() -> None:
    """The String fast path joins the identifier list directly; only typed columns get the helper.

    WHY: the cast branch exists so a typed cell is compared in its string form. On an already-textual
    column it is pure overhead (an extra projection plus a helper column that can collide with source
    data), and nothing else in the suite can tell the two plans apart. `explain()` is the cheapest
    observable that pins which branch ran.
    """
    text = lib.reindex_membership(_lf(column_1=["a", "b"]), "column_1", ["a"]).explain(optimized=False)
    typed = lib.reindex_membership(_lf(column_1=[3, 4]), "column_1", ["3"]).explain(optimized=False)
    assert lib.MEMBERSHIP_KEY_COLUMN not in text
    assert lib.MEMBERSHIP_KEY_COLUMN in typed
    # both still filter correctly
    assert _values(lib.reindex_membership(_lf(column_1=["a", "b"]), "column_1", ["a"])) == ["a"]
    assert _values(lib.reindex_membership(_lf(column_1=[3, 4]), "column_1", ["3"])) == [3]


def test_helper_column_name_does_not_clobber_a_source_column() -> None:
    """The cast branch's join key never overwrites a source column that happens to share its name.

    WHY: unreachable through ``Tcode`` (``idx`` renames every source column to ``column_<n>``), but
    ``reindex_membership`` is a public op and an overwrite-then-drop would silently delete a column.
    """
    df = pl.LazyFrame({"column_1": [3, 4], lib.MEMBERSHIP_KEY_COLUMN: ["keep", "me"]})
    out = lib.reindex_membership(df, "column_1", ["3"])
    assert out.collect_schema().names() == ["column_1", lib.MEMBERSHIP_KEY_COLUMN]
    assert out.collect()[lib.MEMBERSHIP_KEY_COLUMN].to_list() == ["keep"]


def test_cast_branch_null_semantics_match_the_string_branch() -> None:
    """Null cells behave identically whether or not the column needed a cast.

    WHY: the two branches are separate polars plans, and a divergence (antijoin dropping nulls only
    on typed columns) would be invisible to the String-branch tests.
    """
    df = pl.LazyFrame({"column_1": [3, None, 5]})
    assert _values(lib.reindex_membership(df, "column_1", ["3", "5"])) == [3, 5]
    assert _values(lib.reindex_membership(df, "column_1", ["3"], anti=True)) == [None, 5]


def test_zero_match_is_silent_by_design() -> None:
    """A keep-list that matches nothing is a valid empty filter, not an error.

    WHY pinned explicitly: the tempting "helpful" change is to warn or raise on zero matches, but
    detecting it costs an extra pass over the frame on every build, and sections that legitimately
    filter everything to death are normal in these configs.
    """
    df = _lf(column_1=["a", "b"])
    assert lib.reindex_membership(df, "column_1", ["nope"]).collect().height == 0
    assert lib.reindex_membership(df, "column_1", ["nope"], anti=True).collect().height == 2
