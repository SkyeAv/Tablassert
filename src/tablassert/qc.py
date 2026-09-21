from __future__ import annotations

import re
from collections.abc import Callable
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

from tablassert._lazy import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import sentence_transformers
else:
    sentence_transformers = LazyModule("sentence_transformers")
    pl = LazyModule("polars")

from tablassert import extras
from tablassert.errors import QcRuntimeMissingError
from tablassert.log import cat
from tablassert.utils import BASE

logger = cat("QC")

MODEL: Path = BASE / "sapbert"

# Pass threshold for the SapBERT cosine-similarity stage. Carried over from the
# BioBERT gate; SapBERT's UMLS synonym-alignment training shifts true synonym
# pairs upward, so a tuning pass against real reject logs is the natural follow-up.
SIMILARITY_THRESHOLD: float = 0.5
FUZZ_RATIO_THRESHOLD: float = 70.0
FUZZ_PARTIAL_THRESHOLD: float = 80.0


def _qc_text(value: str) -> str:
    """Normalize separator noise before lexical QC scoring."""
    return re.sub(r"[^\w]+", " ", value.replace("?", "-")).strip().casefold()


def _qc_alias(value: str, aliases: dict[str, str]) -> str:
    """Resolve configured source-specific aliases before generic similarity stages."""
    return aliases.get(value.casefold(), value)


def _cascade(passed: pl.DataFrame, scored: pl.DataFrame, out: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Concat already-passed rows with a newly-scored frame and split on ``out``.

    Args:
        passed: Rows accumulated as passing from prior scoring steps.
        scored: Newly scored rows carrying the boolean ``out`` column.
        out: Name of the boolean pass/fail column.

    Returns:
        Tuple of ``(passed, pending)`` split on the ``out`` column.
    """
    pairs: pl.DataFrame = pl.concat((passed, scored))
    return pairs.filter(pl.col(out)), pairs.filter(~pl.col(out))


def _is_abbrev(short_form: str, long_form: str) -> bool:
    """Schwartz-Hearst (2003) check that ``short_form`` abbreviates ``long_form``.

    Scans ``short_form`` right-to-left, matching each alphanumeric character
    (case-insensitively) against ``long_form`` scanned right-to-left; the first
    character of ``short_form`` must match at a ``long_form`` word boundary.

    Args:
        short_form: Candidate abbreviation.
        long_form: Candidate expansion.

    Returns:
        True when ``short_form`` is a valid abbreviation of ``long_form``.
    """
    sf: str = short_form.strip()
    lf: str = long_form.strip()
    if len(sf) < 2 or len(sf) >= len(lf) or not any(c.isalnum() for c in sf):
        return False
    s_idx: int = len(sf) - 1
    l_idx: int = len(lf) - 1
    while s_idx >= 0 and l_idx >= 0:
        s_char: str = sf[s_idx].lower()
        if not s_char.isalnum():
            s_idx -= 1
            continue
        matched: bool = False
        while l_idx >= 0:
            # The first abbreviation character must land on a long-form word
            # boundary; keep scanning leftward until one does (or none exists).
            if lf[l_idx].lower() == s_char and (s_idx > 0 or l_idx == 0 or not lf[l_idx - 1].isalnum()):
                matched = True
                break
            l_idx -= 1
        if not matched:
            return False
        s_idx -= 1
        l_idx -= 1
    # Every remaining short-form character must be non-alphanumeric.
    return all(not c.isalnum() for c in sf[: s_idx + 1])


@cache
def get_sapbert() -> object:
    """Lazy-load and memoize the SapBERT sentence-transformer (``functools.cache``).

    Loads from the local cache at ``MODEL`` when present; otherwise downloads
    ``cambridgeltl/SapBERT-from-PubMedBERT-fulltext`` and saves it for future
    runs.

    Returns:
        The memoized ``SentenceTransformer`` instance.

    Raises:
        QcRuntimeMissingError: If ``sentence_transformers`` is not installed.
    """
    try:
        if MODEL.exists():
            model: object = sentence_transformers.SentenceTransformer(str(MODEL))  # pyright: ignore
        else:
            model = sentence_transformers.SentenceTransformer(  # pyright: ignore
                "cambridgeltl/SapBERT-from-PubMedBERT-fulltext"
            )
            MODEL.mkdir(parents=True, exist_ok=True)
            model.save(MODEL)  # pyright: ignore
    except ImportError as exc:
        raise QcRuntimeMissingError(extras.missing("qc")) from exc
    return model


def fullmap_audit(
    lf: pl.LazyFrame,
    col: str,
    section_hash: str,
    config_file: str,
    out: str = "passed",
    log: bool = True,
    on_phase: Callable[[str], None] | None = None,
    similarity_threshold: float = SIMILARITY_THRESHOLD,
    fuzzy_ratio_threshold: float = FUZZ_RATIO_THRESHOLD,
    fuzzy_partial_threshold: float = FUZZ_PARTIAL_THRESHOLD,
    aliases: dict[str, str] | None = None,
) -> pl.LazyFrame:
    """Audit that fullmap correctly processed source strings into CURIEs.

    Runs a four-stage cascade that progressively filters out correct
    resolutions and leaves suspected errors behind:

    1. Exact match between pre-resolution text and the resolved preferred
       name, or the source value already looks like a CURIE, or the resolved
       preferred name matches a small allow-list of exception prefixes.
    2. Fuzzy string similarity (RapidFuzz ratio / partial token sort).
    3. Abbreviation expansion (Schwartz-Hearst): the source text abbreviates
       the preferred name, or vice versa.
    4. SapBERT embedding cosine similarity for the remaining rows.

    Rows that fail every stage are treated as QC rejects and dropped from the
    output LazyFrame; when ``log`` is set, each reject is logged with the
    CURIE, original text, preferred name, and similarity scores.

    Args:
        lf: Source LazyFrame. Must expose ``col``, ``{col}_pre_resolution`` and
            ``{col}_name`` columns.
        col: Node column being audited.
        section_hash: Short section hash (for log context).
        config_file: Originating config file (for log context).
        out: Name of the boolean pass/fail column produced internally.
        log: When ``True``, log rejected CURIEs at INFO level.
        on_phase: Optional callback fired with ``"qc:exact"``, ``"qc:fuzzy"``,
            ``"qc:abbrev"`` and ``"qc:sapbert"`` at the start of each cascade
            stage (``qc:abbrev`` only fires when Stage 3 actually runs, and
            ``qc:sapbert`` only when Stage 4 does), used to drive fine-grained
            progress UX.

    Returns:
        LazyFrame containing only rows whose ``col`` value passed QC.

    Raises:
        QcRuntimeMissingError: If the ``[qc]`` extra is not installed.

    Notes:
        Collection point: pending pairs are handled eagerly because each stage
        needs the full set of survivors to batch-similarity-score them.
    """
    # The whole extra is checked up front, not just the package the next line needs:
    # scikit-learn is imported here but sentence-transformers only in Stage 4, so a
    # half-installed extra would otherwise fail after the audit had already run.
    extras.require("qc", required_by="the QC audit")

    # Stage 0: deletes suspected errors.
    from rapidfuzz import fuzz
    from rapidfuzz.process import cpdist
    from sklearn.metrics.pairwise import cosine_similarity

    original: str = f"{col}_pre_resolution"
    preferred: str = f"{col}_name"
    cols: list[str] = [col, original, preferred]

    # Stage 1: exact string matching or is CURIE (can stay lazy until filter).
    if on_phase is not None:
        on_phase("qc:exact")
    # Collection point: pending pairs require eager.
    df: pl.DataFrame = lf.collect()
    aliases = {str(key).casefold(): str(value) for key, value in (aliases or {}).items()}
    pairs: pl.DataFrame = df.select(cols).unique()
    pairs = pairs.with_columns(
        pl.struct(cols[1:]).map_elements(lambda row: _qc_alias(str(row[cols[1]]), aliases) == str(row[cols[2]]), return_dtype=pl.Boolean).alias(out)
    )

    passed: pl.DataFrame
    pending: pl.DataFrame
    passed, pending = _cascade(pairs.clear(), pairs, out)

    exempt_curies: str = r"^CHEBI|^PR|^UniProtKB|^NCBIGene|^UMLS|^UNII|^PUBCHEM|^MONDO"
    is_exempt: pl.DataFrame = pending.with_columns(pl.col(cols[0]).str.contains(exempt_curies).alias(out))
    passed, pending = _cascade(passed, is_exempt, out)

    is_curie: pl.DataFrame = pending.with_columns(pl.col(cols[1]).str.contains(":").alias(out))
    passed, pending = _cascade(passed, is_curie, out)

    exceptions: str = r"^LOC|^si:"
    is_exception: pl.DataFrame = pending.with_columns(pl.col(cols[2]).str.contains(exceptions).alias(out))
    passed, pending = _cascade(passed, is_exception, out)

    # Stage 2: fuzzy matching via RapidFuzz (batched).
    if on_phase is not None:
        on_phase("qc:fuzzy")
    originals: list[str] = [_qc_alias(str(value), aliases) for value in pending.get_column(cols[1]).to_list()]
    preferreds: list[str] = [str(value) for value in pending.get_column(cols[2]).to_list()]
    normalized_originals: list[str] = [_qc_text(value) for value in originals]
    normalized_preferreds: list[str] = [_qc_text(value) for value in preferreds]

    ratio_scores: object = cpdist(normalized_originals, normalized_preferreds, scorer=fuzz.ratio)
    partial_scores: object = cpdist(normalized_originals, normalized_preferreds, scorer=fuzz.partial_token_sort_ratio)
    token_scores: object = cpdist(normalized_originals, normalized_preferreds, scorer=fuzz.token_set_ratio)

    pending = pending.with_columns(
        [pl.Series("fuzz_ratio", ratio_scores), pl.Series("fuzz_partial", partial_scores), pl.Series("fuzz_token_set", token_scores)]
    )

    fuzz_mask: pl.Series = pl.Series(
        out,
        (ratio_scores >= fuzzy_ratio_threshold) | (partial_scores >= fuzzy_partial_threshold) | (token_scores >= fuzzy_partial_threshold),
        dtype=pl.Boolean,
    )
    masked_fuzz: pl.DataFrame = pending.with_columns(fuzz_mask)
    pairs = pl.concat((passed, masked_fuzz), how="diagonal")

    passed = pairs.filter(pl.col(out))
    pending = pairs.filter(~pl.col(out))

    # Quick exit if no rows need abbreviation/SapBERT QC.
    if pending.height == 0:
        return df.join(passed.select(col), on=col, how="semi").lazy()

    # Stage 3: abbreviation expansion (Schwartz-Hearst, deterministic).
    if on_phase is not None:
        on_phase("qc:abbrev")
    originals = pending.get_column(cols[1]).to_list()
    preferreds = pending.get_column(cols[2]).to_list()

    abbrev_mask: pl.Series = pl.Series(
        out, [_is_abbrev(o, p) or _is_abbrev(p, o) for o, p in zip(originals, preferreds, strict=True)], dtype=pl.Boolean
    )
    masked_abbrev: pl.DataFrame = pending.with_columns(abbrev_mask)
    pairs = pl.concat((passed, masked_abbrev), how="diagonal")

    passed = pairs.filter(pl.col(out))
    pending = pairs.filter(~pl.col(out))

    # Quick exit if no rows need SapBERT QC.
    if pending.height == 0:
        return df.join(passed.select(col), on=col, how="semi").lazy()

    # Stage 4: SapBERT embeddings (batched).
    if on_phase is not None:
        on_phase("qc:sapbert")
    originals = pending.get_column(cols[1]).to_list()
    preferreds = pending.get_column(cols[2]).to_list()

    embeddings: object = get_sapbert().encode(originals + preferreds)  # pyright: ignore
    n: int = len(originals)
    similarity: object = cosine_similarity(embeddings[:n], embeddings[n:]).diagonal()  # pyright: ignore

    pending = pending.with_columns(pl.Series("sapbert_similarity", similarity))  # pyright: ignore

    sapbert_mask: pl.Series = pl.Series(out, similarity >= similarity_threshold, dtype=pl.Boolean)  # pyright: ignore
    masked_sapbert: pl.DataFrame = pending.with_columns(sapbert_mask)
    pairs = pl.concat((passed, masked_sapbert), how="diagonal")

    passed = pairs.filter(pl.col(out))
    pending = pairs.filter(~pl.col(out))

    # Log rejected CURIEs.
    if log and pending.height > 0:
        has_sapbert: bool = "sapbert_similarity" in pending.columns
        curies: list[str] = pending.get_column(col).to_list()
        originals_list: list[str] = pending.get_column(original).to_list()
        preferreds_list: list[str] = pending.get_column(preferred).to_list()
        fuzz_partials: list[object] = pending.get_column("fuzz_partial").to_list()
        sapbert_sims: list[object] = pending.get_column("sapbert_similarity").to_list() if has_sapbert else []

        for i, c in enumerate(curies):
            fields: dict[str, object] = {
                "curie": c,
                "original": originals_list[i],
                "preferred": preferreds_list[i],
                "col": col,
                "fuzz": fuzz_partials[i],
                "config": config_file,
                "hash": section_hash,
            }
            template: str = "QC rejected {curie!r}: original={original!r} preferred={preferred!r} col={col} fuzz={fuzz} config={config} hash={hash}"
            if has_sapbert:
                fields["sapbert"] = sapbert_sims[i]
                template += " sapbert={sapbert}"
            logger.info(template, **fields)  # pyright: ignore

    return df.join(passed.select(col), on=col, how="semi").lazy()
