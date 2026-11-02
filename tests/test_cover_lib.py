"""Targeted coverage for uncovered branches in ``tablassert.lib``.

Each test names the exact ``lib.py`` line(s) it exercises in its docstring so the
coverage intent is auditable. These complement ``tests/test_lib.py`` by driving the
small Tcode op helpers (``math_op``/``prefix``/``suffix``/``fill``/``explode``/
``excel``/``crop``/``pick``/``reindex``), the ``Tcode.collect`` quick-exit, and the
stale-tmp cleanup in ``compile_graph`` that the existing suite never reaches.
"""

from __future__ import annotations

import operator
from pathlib import Path
from typing import Any

import openpyxl
import polars as pl

import tablassert.lib as lib
from tablassert.enums import Functions, Tokens
from tablassert.ingests import from_yaml
from tablassert.lib import Tcode
from tablassert.runcache import prefix_digest


def test_math_op_copysign_applies_values_token() -> None:
    """``math_op`` casts (strict=False) then applies a native polars expression.

    Passing string cells proves the ``cast(pl.Float64, strict=False)`` coerces
    before ``copysign(values, -1)`` flips the sign via the ``Tokens.VALUES``
    placeholder.
    """
    lf: pl.LazyFrame = pl.LazyFrame({"x": ["4", "9"]})
    result: list[float] = lib.math_op(lf, "x", Functions.COPYSIGN, [Tokens.VALUES, -1]).collect()["x"].to_list()
    assert result == [-4.0, -9.0]


def test_math_op_pow_substitutes_literal_args() -> None:
    """The argument list feeds literal args (the ``2``) alongside the column.

    ``pow(x, 2)`` mixes ``Tokens.VALUES`` (replaced by the cell) with a literal,
    exercising both arms of the expression builder.
    """
    lf: pl.LazyFrame = pl.LazyFrame({"x": [2.0, 3.0]})
    result: list[float] = lib.math_op(lf, "x", Functions.POW, [Tokens.VALUES, 2]).collect()["x"].to_list()
    assert result == [4.0, 9.0]


def test_prefix_prepends_literal() -> None:
    """Lines 333-334: ``prefix`` casts the column to string and prepends a literal."""
    lf: pl.LazyFrame = pl.LazyFrame({"c": [1, 2]})
    result: list[str] = lib.prefix(lf, "c", "p_").collect()["c"].to_list()
    assert result == ["p_1", "p_2"]


def test_suffix_appends_literal() -> None:
    """Lines 338-339: ``suffix`` casts the column to string and appends a literal."""
    lf: pl.LazyFrame = pl.LazyFrame({"c": [1, 2]})
    result: list[str] = lib.suffix(lf, "c", "_s").collect()["c"].to_list()
    assert result == ["1_s", "2_s"]


def test_fill_forward_fills_trailing_null() -> None:
    """Lines 348-349: ``fill`` applies ``fill_null(strategy=...)`` in place.

    A null after a value is forward-filled; a leading null has no predecessor and
    stays null, confirming the strategy is really applied (not a no-op).
    """
    lf: pl.LazyFrame = pl.LazyFrame({"c": ["x", None, None]})
    result: list[str | None] = lib.fill(lf, "c", "forward").collect()["c"].to_list()
    assert result == ["x", "x", "x"]


def test_explode_splits_delimited_into_rows() -> None:
    """Lines 364-366: ``explode`` splits on a delimiter then explodes to one row per item."""
    lf: pl.LazyFrame = pl.LazyFrame({"c": ["a|b", "c"]})
    result: list[str] = lib.explode(lf, "c", "|").collect()["c"].to_list()
    assert result == ["a", "b", "c"]


def test_explode_and_split_list_parse_a_cell_identically() -> None:
    """``explode_by`` and ``split_by`` share one split; only the destination differs.

    Both route through ``split_expr``, so a delimited cell is read the same way for a
    node encoding and for an annotation -- items trimmed, blanks dropped (a trailing or
    doubled separator is a delimited-text artifact, not a value), nulls preserved. The
    ONLY difference is that ``explode`` fans the items out into rows while ``split_list``
    keeps them as an array on the row.
    """
    lf: pl.LazyFrame = pl.LazyFrame({"c": ["a; b", "x;;y", "p;", None]})

    arrays: list[list[str] | None] = lib.split_list(lf, "c", ";").collect()["c"].to_list()
    assert arrays == [["a", "b"], ["x", "y"], ["p"], None]

    # Exploding those same arrays is exactly what `explode` produces.
    rows: list[str | None] = lib.explode(lf, "c", ";").collect()["c"].to_list()
    assert rows == [item for array in arrays for item in (array or [None])]
    assert rows == ["a", "b", "x", "y", "p", None]


def test_excel_reads_sheet_without_header(tmp_path: Path) -> None:
    """Lines 410-417: ``excel`` reads a sheet via ``pl.read_excel`` with no header.

    ``calamine`` is not installed in this environment, so the ``openpyxl`` engine is
    passed explicitly; the executed lines (the read_excel call + ``df.lazy()``) are
    identical regardless of engine. Columns come back as positional ``column_<n>``.
    """
    wb: openpyxl.Workbook = openpyxl.Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Sheet1"
    ws.append(["a", "b"])
    ws.append(["c", "d"])
    xp: Path = tmp_path / "book.xlsx"
    wb.save(xp)

    df: pl.DataFrame = lib.excel(xp, "Sheet1", engine="openpyxl").collect()
    assert df.columns == ["column_1", "column_2"]
    assert df.rows() == [("a", "b"), ("c", "d")]


def test_parquet_reads_schema_and_renames_columns_positionally(tmp_path: Path) -> None:
    """``parquet`` scans lazily and maps the file's real names to positional ``column_<n>``.

    The config contract addresses columns positionally (``method: column`` encodings accept
    only Excel-style ``A-ZZ`` letters, resolved to ``column_<n>``), so the reader must rename
    whatever names the parquet carries to ``column_1..`` in schema order -- the same convention
    the headerless ``csv``/``excel`` readers produce -- and stay lazy for ``Tcode`` composition.
    """
    pp: Path = tmp_path / "data.parquet"
    pl.DataFrame({"gene_a": ["brca1", "brca1"], "gene_b": ["mapk1", "mapk1"], "p": [0.5, 0.01]}).write_parquet(pp)

    lf: pl.LazyFrame = lib.parquet(pp)
    assert isinstance(lf, pl.LazyFrame)
    df: pl.DataFrame = lf.collect()
    assert df.columns == ["column_1", "column_2", "column_3"]
    assert df["column_1"].to_list() == ["brca1", "brca1"]
    assert df["column_3"].to_list() == [0.5, 0.01]


def test_crop_slices_contiguous_range() -> None:
    """Lines 435-442: ``crop`` resolves integer ``[start, stop]`` bounds to a slice."""
    lf: pl.LazyFrame = pl.LazyFrame({"c": [1, 2, 3, 4]})
    result: list[int] = lib.crop(lf, [1, 3]).collect()["c"].to_list()
    assert result == [2, 3]


def test_crop_resolves_auto_bounds() -> None:
    """Lines 435-442: the ``Tokens.AUTO`` ternaries on 439/440 resolve to frame edges.

    ``AUTO`` start maps to offset 0; ``AUTO`` stop maps to the full height, so the
    three AUTO combinations cover both arms of each ternary.
    """
    lf: pl.LazyFrame = pl.LazyFrame({"c": [1, 2, 3, 4]})
    assert lib.crop(lf, [Tokens.AUTO, 2]).collect()["c"].to_list() == [1, 2]
    assert lib.crop(lf, [1, Tokens.AUTO]).collect()["c"].to_list() == [2, 3, 4]
    assert lib.crop(lf, [Tokens.AUTO, Tokens.AUTO]).collect()["c"].to_list() == [1, 2, 3, 4]


def test_pick_selects_rows_in_order() -> None:
    """Lines 459-461: ``pick`` collects, gathers the requested rows, and re-lazies.

    ``pick`` is the ``source.rows`` op (lib.py:661); it gathers the given row
    indices in order via ``pl.all().gather(...)`` and returns a LazyFrame.
    """
    lf: pl.LazyFrame = pl.LazyFrame({"c": [10, 20, 30]})
    result: pl.DataFrame = lib.pick(lf, [0, 2]).collect()
    assert result["c"].to_list() == [10, 30]


def test_reindex_filters_with_float_cast() -> None:
    """Lines 500-501: ``reindex`` casts to Float64 (cast=True) before comparing."""
    lf: pl.LazyFrame = pl.LazyFrame({"c": ["1", "9", "3"]})
    result: list[str] = lib.reindex(lf, "c", operator.lt, 5).collect()["c"].to_list()
    assert result == ["1", "3"]


def test_reindex_filters_without_cast() -> None:
    """Lines 500-501: the ``cast=False`` arm of the ternary compares raw string values."""
    lf: pl.LazyFrame = pl.LazyFrame({"c": ["a", "b", "a"]})
    result: list[str] = lib.reindex(lf, "c", operator.eq, "a", cast=False).collect()["c"].to_list()
    assert result == ["a", "a"]


def test_reindex_regex_keeps_matching_rows() -> None:
    """``reindex_regex`` keeps only rows whose cell matches (the ``eq`` filter arm).

    Why: the regex comparator exists so one filter drops a whole family of junk cells; an ``eq``
    that matched nothing would emit an empty graph, and one that matched everything would keep the
    ``N/A`` rows the config was written to remove.
    """
    lf: pl.LazyFrame = pl.LazyFrame({"c": ["N/A", "n/a", "ok1", "ok2"]})
    assert lib.reindex_regex(lf, "c", r"(?i)^n/?a$").collect()["c"].to_list() == ["N/A", "n/a"]


def test_reindex_regex_negate_keeps_non_matching_rows() -> None:
    """``negate=True`` is the ``ne`` arm: matching rows drop, every other row survives."""
    lf: pl.LazyFrame = pl.LazyFrame({"c": ["N/A", "n/a", "ok1", "ok2"]})
    assert lib.reindex_regex(lf, "c", r"(?i)^n/?a$", negate=True).collect()["c"].to_list() == ["ok1", "ok2"]


def test_reindex_regex_is_unanchored_without_markers() -> None:
    """Polars ``str.contains`` is a search, so anchoring stays the config author's job.

    Why: this is the documented contract shared with ``NodeEncoding.exclude_regex``. Assuming an
    implicit full-cell match here would drop rows a reviewer cannot see being dropped.
    """
    lf: pl.LazyFrame = pl.LazyFrame({"c": ["N/A", "NA", "N/A2"]})
    assert lib.reindex_regex(lf, "c", "^N/A$").collect()["c"].to_list() == ["N/A"]
    assert lib.reindex_regex(lf, "c", "N/A").collect()["c"].to_list() == ["N/A", "N/A2"]


def test_reindex_regex_compares_typed_columns_as_text() -> None:
    """A numeric column filters on its cast string form instead of raising.

    Why: polars ``str.contains`` on a Float64 column is an ``InvalidOperationError``, and a
    self-describing ``parquet`` source can hand the pipeline typed columns, so the op casts to
    String first and matches text like ``22.0``.
    """
    lf: pl.LazyFrame = pl.LazyFrame({"c": [1.5, 2.0, 22.0]})
    assert lib.reindex_regex(lf, "c", r"^2\d?\.0$").collect()["c"].to_list() == [2.0, 22.0]


def test_reindex_regex_drops_null_rows_in_both_directions() -> None:
    """A null cell matches nothing, so ``eq`` drops it and ``ne`` (``not_``) drops it too.

    Why: literal ``eq``/``ne`` already drop nulls (a null comparison is never true), so opting a
    filter into regex must not quietly change which rows survive for a reason nobody documented.
    """
    lf: pl.LazyFrame = pl.LazyFrame({"c": ["ok", None, "junk"]})
    assert lib.reindex_regex(lf, "c", "junk").collect()["c"].to_list() == ["junk"]
    assert lib.reindex_regex(lf, "c", "junk", negate=True).collect()["c"].to_list() == ["ok"]


def _reindex_ops(tmp_path: Path, fixtures_path: Path, reindex: list[dict[str, Any]]) -> list[tuple[Any, tuple[Any, ...]]]:
    """Return just the row-filter ops a config emits, in pipeline order.

    Args:
        tmp_path: Scratch directory for the section store path.
        fixtures_path: Repo fixture directory holding ``minimal_section.yaml``.
        reindex: The ``source.reindex`` entries to attach to that section.

    Returns:
        The ``(callable, args)`` pairs for ``reindex``/``reindex_regex`` only.
    """
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {
            **data,
            "source": {**data["source"], "reindex": reindex},
            "config": fixtures_path / "minimal_section.yaml",
            "store": tmp_path / "out.parquet",
        }
    )
    # ``_source_ops`` returns nested groups (the reindex block is one list), and ``clean`` is the
    # flattener the pipeline itself uses before dispatching ops.
    return [op for op in tcode_model.clean(tcode_model._source_ops()) if op[0] in (lib.reindex, lib.reindex_regex)]


def test_reindex_dispatch_keeps_legacy_ops_and_digests(tmp_path: Path, fixtures_path: Path) -> None:
    """Backwards compatibility: a `reindex` entry without `regex` emits exactly the pre-feature op.

    Why: run-cache keys are digests of these op tuples, so a legacy config whose tuple shape moved
    would rebuild every section for no reason, and a config whose filter *semantics* moved would
    change which rows survive a shipped build. Both are pinned against the literal tuples the code
    emitted before `Reindex.regex` existed, including the ``cast=False`` slot ``eq``/``ne`` already
    passed and the absent fourth slot the numeric operators never passed.
    """
    ops: list[tuple[Any, tuple[Any, ...]]] = _reindex_ops(
        tmp_path, fixtures_path, [{"column": "A", "comparison": "ne", "comparator": "N/A"}, {"column": "C", "comparison": "lt", "comparator": 0.05}]
    )
    legacy: list[tuple[Any, tuple[Any, ...]]] = [
        (lib.reindex, ("column_1", operator.ne, "N/A", False)),
        (lib.reindex, ("column_3", operator.lt, 0.05)),
    ]
    assert ops == legacy
    assert prefix_digest(ops) == prefix_digest(legacy), "a regex-less config must keep its pre-feature run-cache digest"


def test_reindex_dispatch_emits_regex_op_per_comparison(tmp_path: Path, fixtures_path: Path) -> None:
    """A `regex: true` entry emits `reindex_regex` with the negate flag derived from the comparison.

    Why: the negate slot is what separates `ne` (drop the junk rows) from `eq` (keep only the junk
    rows); a dispatch that mapped them alike would invert a section's row filter.
    """
    ops: list[tuple[Any, tuple[Any, ...]]] = _reindex_ops(
        tmp_path,
        fixtures_path,
        [
            {"column": "B", "comparison": "ne", "comparator": "^(?:n/?a|-{1,2})$", "regex": True},
            {"column": "A", "comparison": "eq", "comparator": "^ok\\d$", "regex": True},
        ],
    )
    assert ops == [(lib.reindex_regex, ("column_2", "^(?:n/?a|-{1,2})$", True)), (lib.reindex_regex, ("column_1", "^ok\\d$", False))]


def test_tcode_collect_quick_exit_returns_existing_store(fixtures_path: Path, tmp_path: Path) -> None:
    """Line 749: ``collect`` returns the store path immediately when it already exists."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    store: Path = tmp_path / "already_built.parquet"
    store.touch()
    tcode_model: Tcode = Tcode.model_validate(  # pyright: ignore
        {**data, "config": fixtures_path / "minimal_section.yaml", "store": store}
    )

    result: list[tuple[Any, tuple[Any]]] | Path = tcode_model.collect(Path("/tmp/fullmap.redb"))
    assert result == store


def test_compile_graph_unlinks_stale_tmp_outputs(monkeypatch: Any, tmp_path: Path, rig_factory: Any) -> None:
    """Lines 1145 & 1149: ``compile_graph`` deletes pre-existing ``.tmp`` outputs first.

    The existing suite always runs against a clean cwd, so the ``if e.exists()`` /
    ``if n.exists()`` unlink branches never fire. Here both stale tmp files exist
    (carrying a sentinel) before the call; after it, the sentinel is gone from the
    final NDJSON and no ``.tmp`` files remain, proving both unlinks ran.
    """
    monkeypatch.chdir(tmp_path)
    sub: Path = tmp_path / "sub.parquet"
    pl.DataFrame(
        {
            "subject": ["A"],
            "subject_name": ["Alpha"],
            "subject_category": ["gene"],
            "subject_taxon": [None],
            "subject_source": [None],
            "subject_source_version": [None],
            "subject_pre_resolution": ["A"],
            "object": ["X"],
            "object_name": ["Xray"],
            "object_category": ["disease"],
            "object_taxon": [None],
            "object_source": [None],
            "object_source_version": [None],
            "object_pre_resolution": ["X"],
            "predicate": ["biolink:related_to"],
            "knowledge_level": ["knowledge_assertion"],
            "agent_type": ["manual_agent"],
            "primary_knowledge_source": ["infores:un-kg"],
            "sources": [
                [{"resource_id": "infores:un-kg", "resource_role": "primary_knowledge_source", "source_record_urls": ["https://example.org/un.tsv"]}]
            ],
        }
    ).write_parquet(sub)

    stale_edges: Path = tmp_path / "un_1.0.0.edges.ndjson.tmp"
    stale_nodes: Path = tmp_path / "un_1.0.0.nodes.ndjson.tmp"
    stale_edges.write_text('{"sentinel":"STALE_EDGES"}\n')
    stale_nodes.write_text('{"sentinel":"STALE_NODES"}\n')

    lib.compile_graph([sub], "un", "1.0.0", rig_factory(tmp_path, infores_id="infores:un-kg"))

    edges: str = (tmp_path / "un_1.0.0.edges.ndjson").read_text()
    nodes: str = (tmp_path / "un_1.0.0.nodes.ndjson").read_text()
    assert "STALE_EDGES" not in edges
    assert "STALE_NODES" not in nodes
    assert '"subject":"A"' in edges
    assert not list(tmp_path.glob("*.tmp"))
