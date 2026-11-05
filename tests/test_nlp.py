from __future__ import annotations

import csv
import random
import time
from pathlib import Path

import polars as pl

from tablassert import rs
from tablassert.nlp import level_one, level_two

GOLDEN_FIXTURE: Path = Path(__file__).resolve().parents[1] / "rust" / "tests" / "fixtures" / "nlp_golden.tsv"


def test_level_one_golden_matches_rust_fixture() -> None:
    """The Python Rust-normalizer path agrees with the shared golden fixture.

    WHY: Rust fullmap keys and Python query terms must remain byte-for-byte
    equivalent; reading one fixture from both integration suites prevents the
    two sides from silently acquiring different normalization contracts.
    """
    with GOLDEN_FIXTURE.open(newline="", encoding="utf-8") as handle:
        rows: list[tuple[str, str]] = [(raw, expected) for raw, expected in csv.reader(handle, delimiter="\t") if raw != "raw"]

    rows = [("" if raw == "@EMPTY" else raw, "" if expected == "@EMPTY" else expected) for raw, expected in rows]
    raw_terms: list[str] = [raw for raw, _expected in rows]
    expected_terms: list[str] = [expected for _raw, expected in rows]
    assert raw_terms
    frame: pl.LazyFrame = pl.DataFrame({"name": raw_terms}).lazy()
    result: pl.DataFrame = level_one(frame, "name").collect()
    assert result["name"].to_list() == expected_terms


def test_level_one_performance_one_million_terms() -> None:
    """Normalize one million seeded multi-token terms within the Python bound.

    WHY: level-one normalization is a hot path for large tabular inputs; this
    absolute ceiling protects the Rust-backed batch implementation from
    regressing to per-row Python work while keeping the workload stable and
    independent of data generation, locale, network, or optional extras.
    Calibration: ~0.6s on a 12-core dev box and a stable ~3.2s on 2-core CI
    runners, so the 8.0s ceiling keeps >2x headroom on the slowest observed
    environment while still failing immediately (minutes at this N) for the
    regression classes it guards: `map_elements` per-row UDFs, serial loops,
    and accidental quadratic token work.
    """
    term_parts: tuple[str, ...] = ("Aspirin", "genes", "inhibiting", "tnf-alpha", "oral", "tablets", "alpha", "51")
    generator = random.Random(5005)
    terms: list[str] = [f"{generator.choice(term_parts)} {generator.choice(term_parts)}" for _ in range(1_000_000)]
    frame: pl.DataFrame = pl.DataFrame({"name": terms})
    lazy_frame: pl.LazyFrame = frame.lazy()

    level_one(lazy_frame, "name").collect()
    elapsed_samples: list[float] = []
    result: pl.DataFrame | None = None
    for _ in range(3):
        started: float = time.perf_counter()
        result = level_one(lazy_frame, "name").collect()
        elapsed_samples.append(time.perf_counter() - started)

    assert result is not None
    elapsed: float = sorted(elapsed_samples)[1]
    assert result.height == 1_000_000
    assert result.columns == ["name"]
    assert result.schema["name"] == pl.String
    assert result["name"].head(4).to_list() == ["tnf-alpha", "aspirin tnf-alpha", "51", "oral tablet"]
    assert elapsed <= 8.0, f"level_one median took {elapsed:.3f}s for 1,000,000 terms (samples={elapsed_samples!r})"


def test_level_one_strips_and_lowercases() -> None:
    """level one strips whitespace and lowercases."""
    lf: pl.LazyFrame = pl.DataFrame({"name": ["  Hello WORLD  ", "FOO"]}).lazy()
    result: pl.DataFrame = level_one(lf, "name").collect()
    assert result["name"].to_list() == ["hello world", "foo"]


def test_level_one_casts_integers() -> None:
    """level one casts integers to strings."""
    lf: pl.LazyFrame = pl.DataFrame({"val": [1, 2, 3]}).lazy()
    result: pl.DataFrame = level_one(lf, "val").collect()
    assert result["val"].to_list() == ["1", "2", "3"]


def test_level_one_already_clean() -> None:
    """level one handles already clean strings."""
    lf: pl.LazyFrame = pl.DataFrame({"name": ["clean"]}).lazy()
    result: pl.DataFrame = level_one(lf, "name").collect()
    assert result["name"].to_list() == ["clean"]


def test_level_one_preserves_other_columns() -> None:
    """level one preserves other columns."""
    lf: pl.LazyFrame = pl.DataFrame({"name": ["  Hello  "], "age": [42]}).lazy()
    result: pl.DataFrame = level_one(lf, "name").collect()
    assert result["age"].to_list() == [42]
    assert result["name"].to_list() == ["hello"]


def test_level_two_removes_nonword() -> None:
    """level two removes non-word characters by default."""
    lf: pl.LazyFrame = pl.DataFrame({"name": ["hello-world", "foo bar"]}).lazy()
    result: pl.DataFrame = level_two(lf, "name").collect()
    assert result["name_two"].to_list() == ["helloworld", "foobar"]


def test_level_two_creates_tagged_column() -> None:
    """level two creates tagged column."""
    lf: pl.LazyFrame = pl.DataFrame({"name": ["hello"]}).lazy()
    result: pl.DataFrame = level_two(lf, "name").collect()
    assert "name_two" in result.columns
    assert "name" in result.columns


def test_level_two_custom_regex() -> None:
    """level two with custom regex."""
    lf: pl.LazyFrame = pl.DataFrame({"name": ["hello123world"]}).lazy()
    result: pl.DataFrame = level_two(lf, "name", regex=r"\d+").collect()
    assert result["name_two"].to_list() == ["helloworld"]


def test_level_two_custom_tag() -> None:
    """level two with custom tag."""
    lf: pl.LazyFrame = pl.DataFrame({"name": ["hello world"]}).lazy()
    result: pl.DataFrame = level_two(lf, "name", tag="_clean").collect()
    assert "name_clean" in result.columns
    assert result["name_clean"].to_list() == ["helloworld"]


def test_level_two_non_ascii_matches_rust_build_side() -> None:
    """level two keeps non-ASCII word characters exactly like the build side.

    WHY: fullmap level-two keys were derived by an ASCII-only filter at build
    time but a Unicode-aware regex at query time, so non-ASCII terms could
    never match at level two (issue #251). Both sides now share one Rust
    filter; this pins the issue's reproduction table on the query path.
    """
    lf: pl.LazyFrame = pl.DataFrame({"name": ["au café lait", "β-catenin"]}).lazy()
    result: pl.DataFrame = level_two(lf, "name").collect()
    assert result["name_two"].to_list() == ["aucafélait", "βcatenin"]


def test_level_two_pipeline_matches_rust_batch() -> None:
    """The level-one then level-two query pipeline equals the Rust batch filter.

    WHY: mirrors the issue's reproduction command end to end: the strings the
    query side derives from a column must equal what ``rs.normalize_terms_l2``
    derives from the level-one forms, or fullmap level-two lookups silently
    miss.
    """
    lf: pl.LazyFrame = pl.DataFrame({"t": ["Café au lait", "β-catenin"]}).lazy()
    result: pl.DataFrame = level_two(level_one(lf, "t"), "t").collect()
    expected: list[str] = rs.normalize_terms_l2(["au café lait", "β-catenin"])
    assert result["t_two"].to_list() == ["aucafélait", "βcatenin"]
    assert result["t_two"].to_list() == expected


def test_level_two_keeps_nulls_and_empty_through_rust_path() -> None:
    """level two keeps nulls in place and empties empty strings.

    WHY: the default path round-trips through Rust (nulls never reach it), so
    null positions and blank cells must come back exactly where they went in,
    or term alignment across the ``{col}`` and ``{col}_two`` columns would
    silently corrupt downstream lookups.
    """
    lf: pl.LazyFrame = pl.DataFrame({"name": ["hello world", None, "a-b", ""]}).lazy()
    result: pl.DataFrame = level_two(lf, "name").collect()
    assert result["name_two"].to_list() == ["helloworld", None, "ab", ""]


def test_level_one_token_order_insensitive() -> None:
    """level one canonicalizes token order.

    WHY: "aspirin oral" and "oral aspirin" are the same drug term; sorting
    tokens to one canonical order makes term equality (and every downstream
    join/dedupe that rides on it) insensitive to word order.
    """
    lf: pl.LazyFrame = pl.DataFrame({"name": ["aspirin oral", "oral aspirin"]}).lazy()
    result: pl.DataFrame = level_one(lf, "name").collect()
    assert result["name"].to_list() == ["aspirin oral", "aspirin oral"]


def test_level_one_stems_to_common_root() -> None:
    """level one folds morphological variants onto their stem.

    WHY: plural/singular variants ("genes" vs "gene") must share one
    normalized key or lexical matching silently misses them; Porter2 stemming
    is what makes the fold happen.
    """
    lf: pl.LazyFrame = pl.DataFrame({"name": ["genes", "gene"]}).lazy()
    result: pl.DataFrame = level_one(lf, "name").collect()
    assert result["name"].to_list() == ["gene", "gene"]


def test_level_one_dedupes_and_collapses_whitespace() -> None:
    """level one drops repeated tokens and collapses whitespace runs.

    WHY: repeated words (a common artifact of concatenating synonyms) and
    stray spacing/tabs must not defeat term equality — both spellings of the
    same term fold onto one canonical value.
    """
    lf: pl.LazyFrame = pl.DataFrame({"name": ["aspirin   aspirin\t oral", "  oral  aspirin  "]}).lazy()
    result: pl.DataFrame = level_one(lf, "name").collect()
    assert result["name"].to_list() == ["aspirin oral", "aspirin oral"]


def test_level_one_preserves_nulls_and_empties() -> None:
    """level one keeps nulls null and empties empty.

    WHY: missing values are semantically different from blank terms — a null
    that turned into "" (or crashed the Rust call, which takes list[str])
    would silently corrupt downstream missing-value handling.
    """
    lf: pl.LazyFrame = pl.DataFrame({"name": [None, "", "   ", "aspirin"]}).lazy()
    result: pl.DataFrame = level_one(lf, "name").collect()
    assert result["name"].to_list() == [None, "", "", "aspirin"]


def test_level_one_passes_through_digits_and_punctuation() -> None:
    """level one leaves digit and punctuated identifiers verbatim.

    WHY: "tnf-alpha" and "51" are not English words — stemming them would
    corrupt the identifier, and guards downstream rely on digits surviving.
    """
    lf: pl.LazyFrame = pl.DataFrame({"name": ["tnf-alpha", "51"]}).lazy()
    result: pl.DataFrame = level_one(lf, "name").collect()
    assert result["name"].to_list() == ["tnf-alpha", "51"]


def test_level_one_unicode_lowercase() -> None:
    """level one folds non-ASCII casing via full Unicode lowercase.

    WHY: source casing is not ASCII-only ("ÄTHÉROGENIC"); ASCII-only lowering
    would leave Ä/É untouched and split one term into two. The folded token is
    then correctly skipped by the ASCII-only stemmer.
    """
    lf: pl.LazyFrame = pl.DataFrame({"name": ["ÄTHÉROGENIC"]}).lazy()
    result: pl.DataFrame = level_one(lf, "name").collect()
    assert result["name"].to_list() == ["äthérogenic"]


def test_level_one_leaves_other_columns_untouched() -> None:
    """level one normalizes one column and leaves siblings byte-identical.

    WHY: normalization is an in-place column rewrite — sibling columns (even
    when the normalized column holds nulls) must come through unchanged, or
    a frame that was merely cleaned would silently lose data.
    """
    lf: pl.LazyFrame = pl.DataFrame({"name": [None, "  Genes  "], "age": [42, 7], "weight": [1.5, 2.5]}).lazy()
    result: pl.DataFrame = level_one(lf, "name").collect()
    assert result["name"].to_list() == [None, "gene"]
    assert result["age"].to_list() == [42, 7]
    assert result["weight"].to_list() == [1.5, 2.5]
