from __future__ import annotations

from typing import TYPE_CHECKING

from tablassert import rs
from tablassert._lazy import LazyModule

if TYPE_CHECKING:
    import polars as pl
else:
    pl = LazyModule("polars")


def _normalize_terms_series(values: pl.Series) -> pl.Series:
    """Normalize one whole string column through the Rust level-one pipeline.

    Args:
        values: Raw column batch (already cast to string by the caller).

    Returns:
        Series with every non-null value replaced by its normalized form,
        nulls kept exactly where they were.

    Notes:
        Nulls never reach Rust: ``rs.normalize_terms`` takes ``list[str]``, and
        a polars null is Python ``None``, which pyo3 would reject outright. So
        the non-null values are normalized in one index-aligned batch and
        scattered back onto their original positions — the same whole-column
        ``map_batches`` shape ``coerce.py`` uses for ``effect_type``, minus the
        vocabulary resolution: normalization is row-wise, so the batch exists
        only to make one GIL-released Rust call per column instead of per row
        (``map_elements`` is forbidden in this repo).
    """
    text: pl.Series = values.cast(pl.String)
    indices: pl.Series = text.is_not_null().arg_true()
    normalized: pl.Series = pl.Series(rs.normalize_terms(text.drop_nulls().to_list()), dtype=pl.String)
    return text.scatter(indices, normalized)


def level_one(lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
    """Normalize a text column to its level-one form, in place.

    Each value goes through the compiled Rust normalizer
    (``rs.normalize_terms``): clean (trim + strip quotes) -> Unicode lowercase
    -> stem every all-ASCII-alphabetic token with the English Porter2
    (Snowball) stemmer -> dedupe -> byte-wise sort -> single-space join. The
    result overwrites ``col``.

    Consequences worth knowing:

    - Token order, duplicates, casing, and whitespace all fold onto one
      canonical key: ``"aspirin oral"``, ``"oral aspirin"``, ``"Aspirin
      ORAL"``, and ``"  aspirin aspirin   oral "`` normalize identically.
    - Morphological variants fold onto their stem (``"genes"`` -> ``"gene"``);
      Porter2 stems biomedical adjectives aggressively (``"acetic"`` ->
      ``"acet"``).
    - Tokens bearing digits or punctuation pass through unstemmed (``"51"``,
      ``"tnf-alpha"`` survive verbatim); non-ASCII text lowercases/folds but
      skips stemming.
    - Nulls stay null; ``""`` and whitespace-only values normalize to ``""``.

    Args:
        lf: Source LazyFrame.
        col: Name of the column to normalize in place.

    Returns:
        LazyFrame with the column cast to string and every non-null value
        replaced by its normalized form.
    """
    expr: pl.Expr = pl.col(col).cast(pl.String).map_batches(_normalize_terms_series, return_dtype=pl.String)
    return lf.with_columns(expr.alias(col))


def _level_two_series(values: pl.Series) -> pl.Series:
    """Level-two normalize one whole string column through the Rust level-two filter.

    Args:
        values: Raw column batch (already cast to string by the caller).

    Returns:
        Series with every non-null value replaced by its level-two form,
        nulls kept exactly where they were.

    Notes:
        Shares the exact batch shape of ``_normalize_terms_series`` (nulls never
        reach Rust; one GIL-released Rust call per column instead of per row,
        ``map_elements`` is forbidden in this repo) but applies the fullmap
        build-side level-two key filter instead of the level-one pipeline, so
        the query-side ``{col}_two`` keys are byte-identical to the keys
        ``emit_term`` stores at build time (issue #251).
    """
    text: pl.Series = values.cast(pl.String)
    indices: pl.Series = text.is_not_null().arg_true()
    normalized: pl.Series = pl.Series(rs.normalize_terms_l2(text.drop_nulls().to_list()), dtype=pl.String)
    return text.scatter(indices, normalized)


def level_two(
    lf: pl.LazyFrame,
    col: str,  # pyright: ignore
    regex: str = r"\W+",
    tag: str = "_two",
) -> pl.LazyFrame:
    """Remove non-word characters from a text column (level-two normalization).

    The cleaned values are written to a new column named ``f"{col}{tag}"`` rather
    than overwriting the source column.

    With the default pattern the column is cleaned through the compiled Rust
    filter (``rs.normalize_terms_l2``) that derives the fullmap build-side
    level-two keys, so query-side and build-side keys are byte-identical by
    construction: non-ASCII word characters (accents, Greek letters) survive
    while punctuation, symbols, and combining marks are dropped on both sides.
    A custom ``regex`` keeps the vectorized Polars path and is a generic text
    cleaner with no build-side key contract.

    Args:
        lf: Source LazyFrame.
        col: Name of the source column to clean.
        regex: Pattern whose matches are removed. Only the default non-word
            pattern (``\\W+``) routes through the shared Rust level-two filter;
            any other pattern is applied with Polars directly.
        tag: Suffix appended to ``col`` to form the output column name.

    Returns:
        LazyFrame with the new tagged column added.
    """
    if regex == r"\W+":
        expr: pl.Expr = pl.col(col).cast(pl.String).map_batches(_level_two_series, return_dtype=pl.String)
    else:
        expr = pl.col(col).str.replace_all(regex, "")
    col: str = col + tag
    return lf.with_columns(expr.alias(col))
