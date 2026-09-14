from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from tablassert import rs
from tablassert._lazy import LazyModule
from tablassert.biolink import Categories
from tablassert.errors import TablassertError
from tablassert.log import cat

logger = cat("FULLMAP")

_TERM_CACHE: OrderedDict[tuple[Path, float, str], list[tuple[int, int]] | None] = OrderedDict()
_TERM_CACHE_MAX: int = 100_000
_SOURCE_CACHE: dict[tuple[Path, float], tuple[list[str], list[str], list[str], str]] = {}

# One-time-per-process flag: set when a legacy-compat fallback first fires so the
# degraded-mode warning is logged once, not on every lookup against a stale extension.
_LEGACY_COMPAT_WARNED: bool = False

# Fullmap readers open the redb DB with a SHARED lock (redb >= 3 ``ReadOnlyDatabase``), so concurrent
# lookups -- even across processes -- never contend with each other. Only a WRITER (a ``build-fullmap``
# rebuild, exclusive lock) conflicts with readers; a lookup that lands in that brief rebuild window would
# otherwise raise ``Database already open`` and surface as a false 0.0 coverage. Retry briefly on that
# contention so a transient reader-vs-writer overlap does not corrupt a build/coverage result.
# Each lookup pins one primary-plus-shards file generation (cached handles are validated against the
# file's (dev, ino) on every use); a reader follows a rebuild on the NEXT lookup. "changing generation"
# is the Rust-side exhaustion token raised when no consistent bundle can be pinned mid-rebuild.
_LOCK_RETRY_TOKENS: tuple[str, ...] = ("already open", "acquire lock", "cannot acquire", "changing generation")
_LOCK_ATTEMPTS: int = 10
_LOCK_DELAY: float = 0.5


def is_lock_contention(error: BaseException) -> bool:
    """Whether ``error`` is transient fullmap contention (a redb lock error or the mid-rebuild
    "changing generation" bundle-pinning exhaustion), safe to retry via :func:`_call_with_lock_retry`.

    Callers that wrap a lookup in their OWN retry loop (e.g. ``build_and_audit``'s coverage
    retry) must NOT retry on this error: :func:`_call_with_lock_retry` already exhausted its
    backoff budget before the error escaped, so re-running it only multiplies the wait while
    (in GEPA) holding the process-wide build lock.
    """
    msg = str(error).lower()
    return any(token in msg for token in _LOCK_RETRY_TOKENS)


def _call_with_lock_retry(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call a redb-backed ``rs`` function, retrying transient lock or ``changing generation`` contention."""
    for attempt in range(_LOCK_ATTEMPTS):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # redb raises a generic error carrying the lock message; match on text
            if attempt < _LOCK_ATTEMPTS - 1 and is_lock_contention(exc):
                time.sleep(_LOCK_DELAY * (attempt + 1))  # linear backoff: 0.5s, 1.0s, 1.5s, ...
                continue
            raise
    return fn(*args, **kwargs)  # unreachable: the final loop iteration returns or raises above


def _warn_legacy_compat(reason: str) -> None:
    """Log once per process that a stale fullmap extension forced the slow path.

    Args:
        reason: Short description of which legacy fallback triggered.

    WHY: the legacy-compat fallbacks re-query the FULL term set on every call
    (bypassing ``_TERM_CACHE``) — correct but slow. A single warning surfaces that
    the running Rust extension is older than the Python package (a mismatched
    wheel) so operators can rebuild it, without spamming the log on every lookup.
    """
    global _LEGACY_COMPAT_WARNED
    if _LEGACY_COMPAT_WARNED:
        return
    logger.warning(
        "Legacy fullmap extension detected ({reason}); falling back to a full term re-query on every "
        "lookup. Rebuild the Rust extension (maturin develop) to restore the optimized pairs path.",
        reason=reason,
    )
    _LEGACY_COMPAT_WARNED = True


if TYPE_CHECKING:
    import polars as pl
else:
    pl = LazyModule("polars")


def empty_matches(column_context: bool) -> pl.DataFrame:
    """Build an empty fullmap matches DataFrame using the canonical query schema.

    Args:
        column_context: When ``True``, add the ``FREQUENCY`` column used by
            column-context ranking.

    Returns:
        Zero-row DataFrame with the fullmap matches schema.
    """
    schema: dict[str, object] = {
        "term": pl.String,
        "CURIE": pl.String,
        "PREFERRED_NAME": pl.String,
        "CATEGORY_NAME": pl.String,
        "TAXON_ID": pl.Int64,
        "SOURCE_NAME": pl.String,
        "SOURCE_VERSION": pl.String,
        "NLP_LEVEL": pl.Int64,
        "PR": pl.Int64,
    }

    if column_context:
        schema["FREQUENCY"] = pl.Int64

    return pl.DataFrame(schema=schema)  # pyright: ignore


def _db_cache_key(db: Path) -> tuple[Path, float]:
    """Build a cache key for a fullmap DB that invalidates on rebuild.

    Args:
        db: Path to the fullmap redb file.

    Returns:
        Canonical path and mtime seconds.

    Note:
        Since fullmap readers open read-only (shared lock, no file writes), only
        a ``build-fullmap`` rebuild touches the mtime -- so this key is stable
        across lookups and flips exactly when the DB is rebuilt. This mirrors
        the Rust-side generation boundary: a lookup pins one primary-plus-shards
        generation, and readers follow a rebuild on the next lookup.
    """
    resolved: Path = db.resolve()
    try:
        return resolved, resolved.stat().st_mtime
    except FileNotFoundError:
        return resolved, -1.0


def _remember_term(key: tuple[Path, float, str], value: list[tuple[int, int]] | None) -> None:
    """Store one term lookup in the bounded FIFO cache.

    Args:
        key: Cache key including database path, mtime, and term.
        value: Raw ``(curie_id, source_id)`` records, or ``None`` for misses.
    """
    _TERM_CACHE[key] = value
    while len(_TERM_CACHE) > _TERM_CACHE_MAX:
        _TERM_CACHE.popitem(last=False)


def _dimension_maps(db: Path, cache_key: tuple[Path, float]) -> tuple[list[str], list[str], list[str], str]:
    """Load cached prefix/category/source dimensions for a fullmap DB.

    Args:
        db: Path to the fullmap redb file.
        cache_key: Cache key from ``_db_cache_key``.

    Returns:
        Prefix, category, source, and source-version maps.
    """
    cached: tuple[list[str], list[str], list[str], str] | None = _SOURCE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    source_version: str = rs.fullmap_source_version()
    prefixes: list[str] = list(_call_with_lock_retry(rs.hydrate_prefixes, db))
    categories: list[str] = list(_call_with_lock_retry(rs.hydrate_categories, db))
    # A fullmap category the ``Categories`` enum cannot name is a silent hole in every
    # category-keyed guard (see ``biolink.CATEGORY_OVERRIDES``): ``filter_and_rank`` drops
    # those rows when a column sets ``avoid``, and they fall through to pair-derived classes
    # otherwise. Fail loudly ONCE per db (the cache above makes this fire per process) so a
    # vocabulary drift surfaces as an actionable warning instead of quietly degrading.
    unnameable: list[str] = sorted(set(categories) - set(_KNOWN_CATEGORIES))
    if unnameable:
        logger.warning(
            "fullmap {db} emits categories the config vocabulary cannot name: {cats} -- rows carrying them are dropped when a column sets `avoid` and fall through to pair-derived classes otherwise; widen biolink.CATEGORY_OVERRIDES to make them first-class",
            db=db,
            cats=unnameable,
        )
    value: tuple[list[str], list[str], list[str], str] = (prefixes, categories, list(_call_with_lock_retry(rs.hydrate_sources, db)), source_version)
    _SOURCE_CACHE.clear()
    _SOURCE_CACHE[cache_key] = value
    return value


def lookup_rows(db: Path, terms: list[str]) -> list[dict[str, object]]:
    """Lookup terms using the v2 raw-pair path and hydrate rows once per batch.

    Args:
        db: Path to the fullmap redb file.
        terms: Terms to query.

    Returns:
        Hydrated rows matching the legacy ``lookup_fullmap_terms`` shape.
    """
    if not terms:
        return []
    cache_key: tuple[Path, float] = _db_cache_key(db)
    pairs_by_term: dict[str, list[tuple[int, int]] | None] = {}
    misses: list[str] = []
    for term in terms:
        term_key: tuple[Path, float, str] = (cache_key[0], cache_key[1], term)
        if term_key in _TERM_CACHE:
            pairs_by_term[term] = _TERM_CACHE[term_key]
        else:
            misses.append(term)

    if misses:
        try:
            pair_rows: list[dict[str, object]] = _call_with_lock_retry(rs.lookup_fullmap_terms, db, misses, return_format="pairs")
        except TypeError as exc:
            # Only swallow the signature-mismatch TypeError from an old extension that
            # lacks return_format; any other TypeError is a real bug and must propagate.
            if "return_format" not in str(exc):
                raise
            # Legacy extension without return_format: re-query the FULL term set so
            # already-cached terms are not dropped from the returned rows.
            _warn_legacy_compat("no return_format support")
            return _call_with_lock_retry(rs.lookup_fullmap_terms, db, terms)
        if pair_rows and "records" not in pair_rows[0]:
            # Legacy row shape covers only `misses`; re-query the FULL term set so
            # already-cached terms are not dropped when _TERM_CACHE is partially warm.
            _warn_legacy_compat("legacy row shape")
            return _call_with_lock_retry(rs.lookup_fullmap_terms, db, terms)
        seen: set[str] = set()
        for row in pair_rows:
            term = str(row["term"])
            records_raw: list[tuple[int, int]] = cast(list[tuple[int, int]], row["records"])
            records: list[tuple[int, int]] = [(int(curie_id), int(source_id)) for curie_id, source_id in records_raw]
            pairs_by_term[term] = records
            _remember_term((cache_key[0], cache_key[1], term), records)
            seen.add(term)
        for term in misses:
            if term not in seen:
                pairs_by_term[term] = None
                _remember_term((cache_key[0], cache_key[1], term), None)

    curie_ids: list[int] = sorted({curie_id for pairs in pairs_by_term.values() if pairs for curie_id, _source_id in pairs})
    if not curie_ids:
        return []
    hydrated: list[dict[str, Any]] = _call_with_lock_retry(rs.hydrate_curies, db, curie_ids)
    curie_map: dict[int, dict[str, Any]] = dict(zip(curie_ids, hydrated, strict=True))
    prefixes, categories, sources, source_version = _dimension_maps(db, cache_key)
    rows: list[dict[str, object]] = []
    for term in terms:
        pairs: list[tuple[int, int]] | None = pairs_by_term.get(term)
        if not pairs:
            continue
        for curie_id, source_id in pairs:
            curie: dict[str, Any] = curie_map[curie_id]
            prefix: str = prefixes[int(curie["prefix_id"])]
            category: str = categories[int(curie["category_id"])]
            rows.append(
                {
                    "term": term,
                    "CURIE": f"{prefix}:{curie['local_id']}",
                    "PREFERRED_NAME": str(curie["preferred_name"]),
                    "CATEGORY_NAME": category,
                    "TAXON_ID": int(curie["taxon_id"]),
                    "SOURCE_NAME": sources[source_id],
                    "SOURCE_VERSION": source_version,
                }
            )
    return rows


def distinct(lf: pl.LazyFrame, l1: str, l2: str, col: str = "term") -> pl.LazyFrame:
    """Extract unique terms from two text-normalization columns as a LazyFrame.

    Each input column is de-duplicated independently and tagged with its NLP
    level (1 for ``l1``, 2 for ``l2``), then concatenated and de-duplicated
    again keeping the first (lowest-level) occurrence. Purely numeric values,
    common null sentinels, and a fixed list of generic labels are dropped.

    Args:
        lf: Source LazyFrame.
        l1: Level-one (lightly normalized) column name.
        l2: Level-two (heavily normalized) column name.
        col: Output column name for the unified term.

    Returns:
        LazyFrame with one ``col`` column plus ``nlp_level``.
    """
    t1: pl.LazyFrame = lf.select(pl.col(l1).alias(col)).unique()
    t1 = t1.with_columns(pl.lit(1).alias("nlp_level"))

    t2: pl.LazyFrame = lf.select(pl.col(l2).alias(col)).unique()
    t2 = t2.with_columns(pl.lit(2).alias("nlp_level"))

    terms: pl.LazyFrame = pl.concat([t1, t2]).unique(subset=[col], keep="first")

    bad: str = r"^\d+$|^(none|nan|na|null|unknown|not applicable|p_value|variable|result|exposure|expression|symbol)$|^$"
    return terms.filter(~pl.col(col).str.contains(bad))


def deduplicate_result(result: pl.DataFrame, column_context: bool) -> pl.DataFrame:
    """Sort and de-duplicate fullmap matches while retaining tied best candidates.

    When ``column_context`` is set, a per-category ``FREQUENCY`` column is
    attached and used as a high-priority tiebreaker (more common categories
    first). Lower-ranked rows are removed, but every distinct CURIE sharing a
    term's complete best ranking tuple is retained. Repeated rows for the same
    ``(term, CURIE)`` pair collapse to one row.

    Args:
        result: Joined matches with a ``CATEGORY_NAME`` column.
        column_context: Whether to compute/use the frequency tiebreaker.

    Returns:
        DataFrame with one or more best-ranked rows per ``term``, unique by
        ``(term, CURIE)``.
    """
    ranking_columns: list[str] = ["term", "PR", "NLP_LEVEL"]
    descending: list[bool] = [False, False, False]

    if column_context:
        frequency: pl.DataFrame = result.group_by("CATEGORY_NAME").agg(pl.len().alias("FREQUENCY"))
        result = result.join(frequency, on="CATEGORY_NAME", how="left")

        ranking_columns += ["FREQUENCY"]
        descending += [True]

    result = result.sort(ranking_columns, descending=descending)
    best_tiers: pl.DataFrame = result.select(ranking_columns).unique(subset=["term"], keep="first")
    result = result.join(best_tiers, on=ranking_columns, how="inner")
    result = result.unique(subset=["term", "CURIE"], keep="first")
    return result.sort([*ranking_columns, "CURIE"], descending=[*descending, False])


#: Every category name the config vocabulary can spell, sorted. ``filter_and_rank``
#: drops rows carrying any other ``CATEGORY_NAME`` when ``avoid`` is set: an ``avoid``
#: list is an allow-list by complement, and a complement can only name categories the
#: ``Categories`` enum knows -- a fullmap category outside the enum (a Biolink mixin
#: the ``Entity`` scan misses; see ``biolink.CATEGORY_OVERRIDES``) could never appear
#: in it, so without this guard it would sail through the hard filter.
_KNOWN_CATEGORIES: tuple[str, ...] = tuple(sorted(category.value for category in Categories))


def _category_values(categories: list[Any]) -> list[str]:
    """Normalize a ``prioritize``/``avoid`` list to plain category-name strings.

    The build pipeline supplies plain strings (Pydantic ``use_enum_values=True``
    unwraps the ``Categories`` members on ``NodeEncoding``), while direct callers
    may pass ``Categories`` members; accept both.

    Args:
        categories: Category names as ``Categories`` members or plain strings.

    Returns:
        Plain category-name strings.
    """
    return [c.value if isinstance(c, Enum) else c for c in categories]


def filter_and_rank(
    raw: pl.DataFrame,
    terms: pl.DataFrame,
    taxon: str | None,
    prioritize: list[Categories] | None,
    avoid: list[Categories] | None,
    column_context: bool,
    exclude_prefixes: list[str] | None = None,
    exclude_regex: list[str] | None = None,
) -> pl.DataFrame:
    """Join already-fetched redb rows against one column's own terms, then filter, rank, and dedup.

    Called by ``resolve_batch`` so it can reuse one shared redb fetch per column.

    Args:
        raw: Rows looked up from the fullmap redb.
        terms: Distinct terms for the column being resolved (from ``distinct``).
        taxon: Optional taxon filter applied to taxon-bearing matches; rows with TAXON_ID 0 are retained.
        prioritize: Categories to boost in ranking.
        avoid: Categories to drop entirely. When set, rows whose ``CATEGORY_NAME`` is
            not a ``Categories`` member are dropped as well -- an allow-list cannot
            name what the enum does not know (see :data:`_KNOWN_CATEGORIES`).
        column_context: Whether to compute/use category frequency as a tiebreaker.
        exclude_prefixes: CURIE namespace prefixes (text before the first ':') to drop.
        exclude_regex: Regex patterns; any CURIE matching one is dropped.

    Returns:
        Ranked matches DataFrame with one or more best-ranked rows per term,
        retaining distinct tied CURIEs and collapsing duplicate ``(term, CURIE)``
        rows.
    """
    # Resolve_batch reuses one shared redb fetch per column.
    if raw.height == 0:
        return empty_matches(column_context)

    result: pl.DataFrame = raw.join(terms, on="term", how="inner").rename({"nlp_level": "NLP_LEVEL"})
    if avoid:
        avoid_values: list[str] = _category_values(avoid)
        result = result.filter(~pl.col("CATEGORY_NAME").is_in(avoid_values))
        result = result.filter(pl.col("CATEGORY_NAME").is_in(_KNOWN_CATEGORIES))
    if taxon:
        taxon_id: int = int(taxon)
        result = result.filter((pl.col("TAXON_ID") == taxon_id) | (pl.col("TAXON_ID") == 0))
    if exclude_prefixes:
        curie_prefix: pl.Expr = pl.col("CURIE").str.split(":").list.first()
        result = result.filter(~curie_prefix.is_in(exclude_prefixes))
    if exclude_regex:
        for pattern in exclude_regex:
            result = result.filter(~pl.col("CURIE").str.contains(pattern))
    if result.height == 0:
        return empty_matches(column_context)

    if prioritize:
        priority_values: list[str] = _category_values(prioritize)
        priority: pl.Expr = pl.when(pl.col("CATEGORY_NAME").is_in(priority_values)).then(pl.lit(1)).otherwise(pl.lit(50))
    else:
        priority = pl.lit(50)
    pr_base: pl.Expr = (
        pl.when(pl.col("PREFERRED_NAME") == pl.col("term"))
        .then(pl.lit(1))
        .when((pl.col("PREFERRED_NAME").str.to_lowercase() == pl.col("term")) & (pl.col("NLP_LEVEL") == 1))
        .then(pl.lit(5))
        .otherwise(pl.lit(10))
    )
    result = result.with_columns((priority * pr_base).alias("PR"))
    return deduplicate_result(result, column_context)


def fullmap_db_path(fullmap: Path) -> Path:
    """Resolve a ``fullmap`` model field to the embedded redb file path.

    Accepts either the redb file directly or a base directory. When given a
    directory, checks for ``<dir>/fullmap.redb`` first, then falls back to
    ``<dir>/data/fullmap.redb``.

    Args:
        fullmap: File path or base directory from the ``Graph.fullmap`` field.

    Returns:
        Resolved path to ``fullmap.redb``.
    """
    if fullmap.is_file() or fullmap.suffix == ".redb":
        return fullmap
    direct: Path = fullmap / "fullmap.redb"
    if direct.is_file():
        return direct
    return fullmap / "data" / "fullmap.redb"


def log_unmatched(col: str, terms: pl.LazyFrame, matches: pl.DataFrame, section_hash: str | None, config_file: str | None) -> None:
    """Log level-one terms that did not resolve to any CURIE.

    Args:
        col: Column being resolved (for log context).
        terms: Distinct terms with their NLP level.
        matches: Matches actually resolved for this column.
        section_hash: Short section hash (for log context).
        config_file: Originating config file (for log context).
    """
    # Log unmatched entities.
    level_one: pl.LazyFrame = terms.filter(pl.col("nlp_level") == 1)
    antimatches: pl.LazyFrame = level_one.join(matches.lazy().select("term"), left_on="term", right_on="term", how="anti")

    # Collection point: requires eager.
    unnmatched: pl.DataFrame = antimatches.select("term").unique().collect()
    if unnmatched.height > 0:
        for term in unnmatched.get_column("term").to_list():
            logger.info("Unresolved term in {config} ({hash}) col {col}: {term!r}", config=config_file, hash=section_hash, col=col, term=term)


# (out_suffix, base_col, prefix, cast) coalesce specs for ``join_matches``: the
# level-one column wins over its ``_l2`` counterpart. Suffix "" aliases back to
# ``col`` itself; category/taxon carry biolink:/NCBITaxon: prefixes and taxon is
# cast to String.
_JOIN_COALESCE_SPECS: list[tuple[str, str, str, bool]] = [
    ("", "CURIE", "", False),
    ("_name", "PREFERRED_NAME", "", False),
    ("_category", "CATEGORY_NAME", "biolink:", False),
    ("_taxon", "TAXON_ID", "NCBITaxon:", True),
    ("_source", "SOURCE_NAME", "", False),
    ("_source_version", "SOURCE_VERSION", "", False),
    ("_nlp_level", "NLP_LEVEL", "", False),
]


def _coalesce_expr(col: str, suffix: str, base: str, prefix: str, cast_str: bool) -> pl.Expr:
    """Build one level-one-wins-over-level-two coalesce expression for ``join_matches``.

    Args:
        col: Base node column name the derived column hangs off.
        suffix: Output column suffix ("" aliases back to ``col`` itself).
        base: Level-one fullmap column name (level-two is ``f"{base}_l2"``).
        prefix: Literal prefix prepended to both branches ("" for none).
        cast_str: When True, cast both branches to String before prefixing.

    Returns:
        Coalesce expression aliased to ``col + suffix``.
    """
    l1: pl.Expr = pl.col(base).cast(pl.String) if cast_str else pl.col(base)
    l2: pl.Expr = pl.col(f"{base}_l2").cast(pl.String) if cast_str else pl.col(f"{base}_l2")
    if prefix:
        l1 = pl.lit(prefix) + l1
        l2 = pl.lit(prefix) + l2
    return pl.when(pl.col(base).is_not_null()).then(l1).otherwise(l2).alias(col + suffix)


def join_matches(lf: pl.LazyFrame, col: str, matches: pl.DataFrame, tag: str = "_two", drop_unresolved: bool = True) -> pl.LazyFrame:
    """Join ranked fullmap matches back into ``lf`` for one column.

    A source row may expand to one output row per distinct best-ranked CURIE
    when the fullmap ranking heuristics tie. Coalesces level-one and level-two
    hits per row (level one wins when present) and emits derived
    ``<col>_name``, ``<col>_category``,
    ``<col>_taxon``, ``<col>_source``, ``<col>_source_version`` and
    ``<col>_nlp_level`` columns.

    Args:
        lf: Source LazyFrame (will be collected eagerly for the join).
        col: Column being resolved.
        matches: Ranked matches for this column from ``filter_and_rank``.
        tag: Suffix used to derive the level-two column name.
        drop_unresolved: When True (default) rows whose ``col`` did not match are
            dropped — the behavior subject/object require. When False the row is
            kept and the column (plus its derived ``<col>_*`` columns) stays null;
            used by ``nullable`` qualifiers so a blank/unresolvable cell keeps the
            edge and the null-stripper omits the qualifier key.

    Returns:
        New LazyFrame with resolved columns. Tied best-ranked CURIEs are
        returned as separate rows; rows whose ``col`` did not match are dropped
        unless ``drop_unresolved`` is False.

    Notes:
        Split out of ``resolve`` so ``resolve_batch`` can apply per-column
        matches from one shared redb fetch. ``resolve_batch`` calls the eager
        twin directly so the whole batch materializes the frame only once.
    """
    # Collection point: join after redb query, then re-lazy.
    return _join_matches_eager(lf.collect(), col, matches, tag, drop_unresolved).lazy()


def _join_matches_eager(df: pl.DataFrame, col: str, matches: pl.DataFrame, tag: str = "_two", drop_unresolved: bool = True) -> pl.DataFrame:
    """Eager core of ``join_matches``: join ranked matches into an in-memory frame.

    Args:
        df: Source DataFrame, already materialized.
        col: Column being resolved.
        matches: Ranked matches for this column from ``filter_and_rank``.
        tag: Suffix used to derive the level-two column name.
        drop_unresolved: When True rows whose ``col`` did not match are dropped;
            when False the row is kept and the resolved columns stay null.

    Returns:
        DataFrame with resolved columns. Tied best-ranked CURIEs are returned as
        separate rows; rows whose ``col`` did not match are dropped unless
        ``drop_unresolved`` is False.
    """
    l1: str = col
    l2: str = l1 + tag

    result: pl.DataFrame = df.join(matches.filter(pl.col("NLP_LEVEL").eq(1)), left_on=l1, right_on="term", how="left", suffix="_l1")

    l2_matches: pl.DataFrame = matches.filter(pl.col("NLP_LEVEL").eq(2))
    result = result.join(l2_matches, left_on=l2, right_on="term", how="left", suffix="_l2")

    result = result.with_columns([_coalesce_expr(col, suffix, base, prefix, cast_str) for suffix, base, prefix, cast_str in _JOIN_COALESCE_SPECS])

    result = result.select(pl.exclude(r"^(CURIE|PREFERRED_NAME|CATEGORY_NAME|TAXON_ID|SOURCE_NAME|SOURCE_VERSION|NLP_LEVEL|PR|FREQUENCY)(_l2)?$"))
    result = result.select(pl.exclude(col + tag))
    result = result.with_columns(pl.col(f"{col}_taxon").replace("NCBITaxon:0", None))
    if drop_unresolved:
        # Subject/object (and strict qualifiers) drop rows that failed resolution; a
        # nullable qualifier keeps the edge and leaves the column null for the null-stripper.
        result = result.filter(pl.col(col).is_not_null())

    return result


class ResolveSpec(NamedTuple):
    """One node column's resolution settings for ``resolve_batch``."""

    col: str
    taxon: str | None = None
    prioritize: list[Categories] | None = None
    avoid: list[Categories] | None = None
    exclude_prefixes: list[str] | None = None
    exclude_regex: list[str] | None = None
    nullable: bool = False
    """When True, an unresolved/blank cell keeps its row instead of dropping the edge.

    Set only for ``nullable`` qualifiers; subject/object always resolve strict so an
    edge with a missing node is dropped, never emitted.
    """


def resolve_batch(
    lf: pl.LazyFrame,
    specs: list[ResolveSpec],
    db: Path,
    log: bool = True,
    section_hash: str | None = None,
    config_file: str | None = None,
    column_context: bool = True,
    tag: str = "_two",
    on_phase: Callable[[str], None] | None = None,
) -> pl.LazyFrame:
    """Resolve multiple node columns against one shared redb fetch.

    Each column still gets its own taxon/prioritize/avoid filtering and its own
    join back into ``lf``. Taxon constraints apply to every taxon-bearing
    match while retaining rows with no taxon metadata (TAXON_ID 0); distinct
    best-ranked CURIEs tied by all heuristics are retained as separate output
    rows. Only the redb round trip itself (``rs.lookup_fullmap_terms``) is pooled
    across columns.

    Args:
        lf: Source LazyFrame.
        specs: One ``ResolveSpec`` per column to resolve.
        db: Path to the fullmap redb file.
        log: When ``True``, log unmatched level-one terms.
        section_hash: Short section hash (for log context).
        config_file: Originating config file (for log context).
        column_context: Whether to compute/use category frequency as a tiebreaker.
        tag: Suffix used to derive level-two column names.
        on_phase: Optional callback fired with ``"resolve:<col>"`` before each
            column is processed, used to drive fine-grained progress UX.

    Returns:
        LazyFrame with resolved columns added. A source row may produce one row
        per distinct CURIE when the best ranking tier is tied.

    Raises:
        TablassertError: ``resolve-bad-specs`` when two specs share a column, or a
            spec's column or its ``<col> + tag`` normalization column is absent from
            the input schema. Checked schema-only (no ``collect``) before any term
            extraction or redb access, so bad specs never surface mid-build as a raw
            polars ``ColumnNotFoundError``.
    """
    # Each column still gets its own taxon/prioritize/avoid filtering and its own join back into lf;
    # only the redb round trip itself (rs.lookup_fullmap_terms) is pooled across columns.
    if not specs:
        return lf

    # Fail loudly on malformed specs BEFORE term collection or any redb access. A duplicated
    # spec would otherwise die mid-build in its second join_matches pass (the first pass drops
    # <col> + tag, so the second spec's level-two join hits a raw polars ColumnNotFoundError),
    # and a spec whose normalization columns were never produced would crash distinct().
    seen: set[str] = set()
    duplicates: list[str] = []
    for spec in specs:
        if spec.col in seen and spec.col not in duplicates:
            duplicates.append(spec.col)
        seen.add(spec.col)
    if duplicates:
        raise TablassertError(
            f"resolve_batch received more than one ResolveSpec for column(s) {', '.join(repr(col) for col in duplicates)}; "
            "each node column may be resolved at most once per batch.",
            code="resolve-bad-specs",
        )

    schema: pl.Schema = lf.collect_schema()
    missing: list[tuple[str, str]] = [(name, spec.col) for spec in specs for name in (spec.col, spec.col + tag) if name not in schema]
    if missing:
        detail: str = ", ".join(f"{name!r} (needed by the spec for {col!r})" for name, col in missing)
        raise TablassertError(
            f"resolve_batch specs reference column(s) absent from the input schema: {detail}. This usually means a node "
            "encoding was declared twice or against the wrong column, so its normalization columns were never produced.",
            code="resolve-bad-specs",
        )

    # Single collection point: the upstream plan (scan, encodings, NLP normalization)
    # executes exactly once here. Per-column term extraction, unmatched logging, and
    # the join backs all run against this in-memory frame, instead of re-executing
    # the whole lazy plan once per column as separate collects.
    df: pl.DataFrame = lf.collect()

    terms_by_col: dict[str, pl.DataFrame] = {spec.col: distinct(df.lazy(), spec.col, spec.col + tag).collect() for spec in specs}

    union_terms: list[str] = pl.concat([t.select("term") for t in terms_by_col.values()]).unique().get_column("term").to_list()

    rows: list[dict[str, object]] = lookup_rows(db, union_terms) if union_terms else []
    raw: pl.DataFrame = pl.DataFrame(rows)

    result: pl.DataFrame = df
    for spec in specs:
        if on_phase is not None:
            on_phase(f"resolve:{spec.col}")
        terms_df: pl.DataFrame = terms_by_col[spec.col]
        matches: pl.DataFrame = filter_and_rank(
            raw, terms_df, spec.taxon, spec.prioritize, spec.avoid, column_context, spec.exclude_prefixes, spec.exclude_regex
        )
        if log:
            log_unmatched(spec.col, terms_df.lazy(), matches, section_hash, config_file)
        result = _join_matches_eager(result, spec.col, matches, tag, drop_unresolved=not spec.nullable)

    return result.lazy()


def resolve(
    lf: pl.LazyFrame,
    col: str,
    db: Path,
    taxon: str | None = None,
    prioritize: list[Categories] | None = None,
    avoid: list[Categories] | None = None,
    exclude_prefixes: list[str] | None = None,
    exclude_regex: list[str] | None = None,
    log: bool = True,
    section_hash: str | None = None,
    config_file: str | None = None,
    column_context: bool = True,
    tag: str = "_two",
) -> pl.LazyFrame:
    """Case-dependent, provenance-rich named-entity recognition (single-column wrapper).

    Thin convenience wrapper around ``resolve_batch`` for callers resolving a
    single column.

    Args:
        lf: Source LazyFrame.
        col: Column to resolve.
        db: Path to the fullmap redb file.
        taxon: Optional taxon filter applied to taxon-bearing matches; rows with TAXON_ID 0 are retained.
        prioritize: Categories to boost in ranking.
        avoid: Categories to drop entirely (delegates to ``filter_and_rank``, which
            also drops categories the ``Categories`` enum cannot name).
        exclude_prefixes: CURIE namespace prefixes (text before the first ':') to drop.
        exclude_regex: Regex patterns; any resolved CURIE matching one is dropped.
        log: When ``True``, log unmatched level-one terms.
        section_hash: Short section hash (for log context).
        config_file: Originating config file (for log context).
        column_context: Whether to compute/use category frequency as a tiebreaker.
        tag: Suffix used to derive the level-two column name.

    Returns:
        LazyFrame with resolved columns added. A source row may produce one row
        per distinct CURIE when the best ranking tier is tied.
    """
    return resolve_batch(
        lf,
        [ResolveSpec(col, taxon, prioritize, avoid, exclude_prefixes, exclude_regex)],
        db,
        log=log,
        section_hash=section_hash,
        config_file=config_file,
        column_context=column_context,
        tag=tag,
    )
