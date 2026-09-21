from __future__ import annotations

import json
import operator
import re
from collections.abc import Callable, Iterable
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import Field, NonNegativeInt

from tablassert import rs
from tablassert._lazy import LazyModule
from tablassert.biolink import (
    ALLOWED_EDGE_FIELDS,
    CLASS_FIELD_OVERRIDES,
    DISABLED_EDGE_FIELDS,
    ENUM_RANGED_QUALIFIERS,
    STUDY_METADATA_FIELDS,
    UNSATISFIABLE_EDGE_FIELDS,
    Categories,
    EdgeCategories,
    association_class,
    class_fields,
    is_multivalued,
    legal_predicates,
    numeric_slot_kind,
    resolve_association_class,
    resolve_node_category,
)
from tablassert.coerce import (
    coerce_columns,
    coerce_effect_size_columns,
    coerce_effect_type_columns,
    coerce_pvalue_columns,
    coerce_study_metadata_columns,
    coerce_study_size_columns,
    coerced_target,
    effect_size_target,
    effect_type_target,
    is_neglog10_column,
    pvalue_target,
    sig,
    study_size_target,
)
from tablassert.enums import EncodingMethods, Files, Functions, InformationResources, Repositories, Tokens
from tablassert.fullmap import ResolveSpec, fullmap_db_path, resolve, resolve_batch
from tablassert.log import cat
from tablassert.models import EDGE_ID_PLACEHOLDER, Encoding, NodeEncoding, Qualifier, RIGConfig, Section
from tablassert.nlp import level_one, level_two
from tablassert.qc import fullmap_audit
from tablassert.rig import (
    as_list,
    clean_values,
    compile_rig,
    curie_prefix,
    infores,
    normalize_biolink_category,
    rig_edge_type_info,
    rig_node_type_info,
    strip_nulls,
    unique_dicts,
)

if TYPE_CHECKING:
    import polars as pl
else:
    pl = LazyModule("polars")

logger = cat("PIPELINE")

__all__ = [
    "as_list",
    "clean_values",
    "coerce_columns",
    "coerce_effect_size_columns",
    "coerce_effect_type_columns",
    "coerce_pvalue_columns",
    "coerce_study_metadata_columns",
    "coerce_study_size_columns",
    "coerced_target",
    "compile_rig",
    "curie_prefix",
    "drop_zero_effect_size",
    "effect_size_target",
    "effect_type_target",
    "infores",
    "is_neglog10_column",
    "normalize_biolink_category",
    "predicate_options",
    "pvalue_target",
    "rig_edge_type_info",
    "rig_node_type_info",
    "sig",
    "strip_nulls",
    "study_size_target",
    "unique_dicts",
]


CATEGORY_PARENT: dict[str, str] = {
    # Curated leaf -> parent *role* map used to roll concrete categories up to the
    # role names that appear in Biolink association class names (e.g. Protein -> Gene
    # so a Protein subject matches GeneToDiseaseAssociation). This is a semantic
    # roll-up, NOT a literal Biolink is_a step -- in the model `protein` is not is_a
    # `gene`, `haplotype` is not is_a `genotype`, and `Variant` is an association-role
    # name rather than a class -- so it cannot be auto-derived from the model and is
    # maintained here. All keys/values are nonetheless valid Biolink categories.
    "SmallMolecule": "MolecularEntity",
    "MolecularEntity": "ChemicalEntity",
    "Drug": "MolecularMixture",
    "MolecularMixture": "ChemicalMixture",
    "ChemicalMixture": "ChemicalEntity",
    "Protein": "Gene",
    "SequenceVariant": "Variant",
    "Haplotype": "Genotype",
}


def parse_edge_name(name: str) -> tuple[str, list[str]] | None:
    """Parse ``{Subject}To{Object}Association`` into ``(subject, [object roles])``.

    Args:
        name: Edge category name (e.g. ``ChemicalEntityToGeneOrProteinAssociation``).

    Returns:
        Tuple of ``(subject_role, object_roles)`` with the trailing
        ``Association`` suffix removed and object side split on ``Or``, or
        ``None`` if ``name`` does not contain ``"To"``.
    """
    name = name.removesuffix("Association")
    if "To" not in name:
        return None
    subj: str = ""
    rest: str = ""
    subj, rest = name.split("To", 1)
    # Split on Camel-case "Or" tokens only, so embedded "Or" substrings such as
    # the one in "Organism" are not treated as object-role separators.
    return (subj, re.split(r"(?<=[a-z])Or(?=[A-Z])", rest))


@cache
def edge_tables() -> tuple[dict[str, str], dict[str, str]]:
    """Generate and cache ``CATEGORY_ROLE`` and ``EDGE_LOOKUP`` on first call.

    Returns:
        Tuple of ``(CATEGORY_ROLE, EDGE_LOOKUP)`` where ``CATEGORY_ROLE`` maps
        each ``CATEGORY_PARENT`` leaf to its root role (via the parent chain),
        and ``EDGE_LOOKUP`` maps ``"subj_role|obj_role"`` to the biolink CURIE
        of the corresponding ``EdgeCategories`` value (falling back to explicit
        overrides and ultimately to ``biolink:Association``).
    """
    # Flattened leaf -> root role (walks CATEGORY_PARENT chain to root).
    CATEGORY_ROLE: dict[str, str] = {}
    for leaf in CATEGORY_PARENT:
        role: str = leaf
        while role in CATEGORY_PARENT:
            role = CATEGORY_PARENT[role]
        CATEGORY_ROLE[leaf] = role

    # Auto-generated (subject role, object role) -> EdgeCategories.
    EDGE_MAP: dict[tuple[str, str], EdgeCategories] = {}
    for ec in EdgeCategories:
        if ec is not EdgeCategories.ASSOCIATION:
            parsed: tuple[str, list[str]] | None = parse_edge_name(ec.value)
            if parsed is not None:
                subj: str = ""
                objs: list[str] = []
                subj, objs = parsed
                for obj in objs:
                    EDGE_MAP[(subj, obj)] = ec

    # Non-standard names -- explicit overrides.
    EDGE_MAP[("ChemicalEntity", "Gene")] = EdgeCategories.CHEMICAL_GENE_INTERACTION_ASSOCIATION

    # Flattened "subj_role|obj_role" -> biolink CURIE (for polars replace_strict).
    EDGE_LOOKUP: dict[str, str] = {f"{s}|{o}": f"biolink:{ec.value}" for (s, o), ec in EDGE_MAP.items()}

    return CATEGORY_ROLE, EDGE_LOOKUP


@cache
def derived_edge_category(subject_category: str, object_category: str) -> str:
    """Return the edge category the build derives for a (subject, object) category pair.

    The pure-Python twin of the ``(subject role, object role)`` lookup :func:`edge_category`
    performs inside a LazyFrame, so an authoring-time caller can ask what class a statement
    would land in without building anything.

    Args:
        subject_category: Subject category, with or without the ``biolink:`` prefix.
        object_category: Object category, with or without the ``biolink:`` prefix.

    Returns:
        The unresolved edge category CURIE, ``"biolink:Association"`` when the pair has no
        specific mapping.
    """
    cat_role: dict[str, str]
    edge_lookup: dict[str, str]
    cat_role, edge_lookup = edge_tables()
    subject: str = subject_category.removeprefix("biolink:")
    obj: str = object_category.removeprefix("biolink:")
    key: str = f"{cat_role.get(subject, subject)}|{cat_role.get(obj, obj)}"
    return edge_lookup.get(key, f"biolink:{EdgeCategories.ASSOCIATION.value}")


def predicate_options(subject_category: str, object_category: str) -> frozenset[str] | None:
    """Return the predicates a (subject, object) category pair may carry without demotion.

    Composes :func:`derived_edge_category` with :func:`biolink.legal_predicates` to answer
    the question config authors (and the agent) actually have: *which predicate keeps this
    edge's specific association class?* A predicate outside this set is not an error -- it
    silently costs the edge its class via :func:`biolink.resolve_association_class`, taking
    every qualifier and evidence slot that class declared with it.

    Args:
        subject_category: Subject category, with or without the ``biolink:`` prefix.
        object_category: Object category, with or without the ``biolink:`` prefix.

    Returns:
        The permitted predicate CURIEs, or ``None`` when the pair derives an association
        class with an open ``predicate`` slot (anything is legal, nothing is specific).
    """
    return legal_predicates(derived_edge_category(subject_category, object_category))


def edge_category(lf: pl.LazyFrame, predicate: str | None = None, category_override: dict[str, str] | None = None) -> pl.LazyFrame:
    """Add the derived ``category`` column using native polars replace operations.

    The ``(subject role, object role)`` lookup alone routinely produces a category
    that contradicts the predicate: ``GeneToDiseaseAssociation`` restricts its
    ``predicate`` slot to ``contributes_to|associated_with|affects``, so a
    ``biolink:gene_associated_with_condition`` edge labelled with that category can
    never validate. When ``predicate`` is supplied the raw category is post-resolved
    by :func:`biolink.resolve_association_class`, which walks up the association
    hierarchy only as far as the predicate requires.

    ``category_override`` pins the association class per *object* category (the
    config's ``statement.category_override``): a row whose raw object category --
    before the role rollup that merges e.g. ``Disease`` and ``PhenotypicFeature``
    into one pair key -- appears in the map carries the pinned class instead of the
    pair-derived one. Pinned classes pass through the same predicate reconciliation.

    Args:
        lf: Source LazyFrame with ``subject category`` and ``object category``
            columns (biolink-prefixed).
        predicate: Section predicate CURIE used to reconcile the derived category.
        category_override: ``{object category (bare name): biolink: CURIE}`` map
            applied per row in place of the pair lookup.

    Returns:
        LazyFrame with a new list-typed ``category`` column containing the
        resolved biolink edge-category CURIE (falling back to
        ``biolink:Association`` when no specific mapping matches).
    """
    cat_role: dict[str, str]
    edge_lookup: dict[str, str]
    cat_role, edge_lookup = edge_tables()
    default: str = f"biolink:{EdgeCategories.ASSOCIATION.value}"
    if predicate:
        # The predicate is a section constant, so the reconciliation collapses to a
        # small raw-category -> resolved-category remap resolved once at plan time.
        edge_lookup = {k: f"biolink:{resolve_association_class(v, predicate).__name__}" for k, v in edge_lookup.items()}
        default = f"biolink:{resolve_association_class(default, predicate).__name__}"
        if category_override:
            category_override = {k: f"biolink:{resolve_association_class(v, predicate).__name__}" for k, v in category_override.items()}
    names: list[str] = lf.collect_schema().names()
    subject_col: str = "subject_category" if "subject_category" in names else "subject category"
    object_col: str = "object_category" if "object_category" in names else "object category"
    sr: pl.Expr = pl.col(subject_col).str.replace("biolink:", "").replace(cat_role).fill_null("")
    or_: pl.Expr = pl.col(object_col).str.replace("biolink:", "").replace(cat_role).fill_null("")
    derived: pl.Expr = pl.concat_str([sr, pl.lit("|"), or_]).replace_strict(edge_lookup, default=default)
    if category_override:
        # Key on the RAW object category: the role rollup above collapses exactly the
        # distinction (Disease vs PhenotypicFeature) the override exists to preserve.
        hit: pl.Expr = pl.col(object_col).str.replace("biolink:", "").replace_strict(category_override, default=None, return_dtype=pl.String)
        derived = pl.when(hit.is_not_null()).then(hit).otherwise(derived)
    return lf.with_columns(pl.concat_list(derived).alias("category"))


def prune_to_class(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Null out edge columns the row's own association class does not declare.

    ``ALLOWED_EDGE_FIELDS`` is a per-*family* allow-list: it says a column is a slot
    of *some* association class. Whether the specific class chosen for a given row
    accepts it is a separate question, and getting it wrong is the single largest
    source of ``extra_forbidden`` failures for qualifier fields on a class that has
    no such slot. Tablassert-disabled fields are removed before this class-specific
    masking so they cannot be rescued into study metadata. Slots granted to a class by
    :data:`biolink.CLASS_FIELD_OVERRIDES` survive the mask -- the grant is a deliberate
    step ahead of the pinned model, not a pruning target.

    Categories vary per row within a section, so this masks per row rather than
    dropping columns: values are nulled where the row's class rejects them, and the
    Rust null-stripper then removes the key entirely. Scalars are wrapped into
    one-element lists when the slot is multivalued on every class that declares it
    (a column has one dtype, so per-row wrapping is impossible; a slot Biolink
    declares scalar on some classes and multivalued on others stays scalar).

    Args:
        lf: Edges LazyFrame carrying a resolved ``category`` column.

    Returns:
        LazyFrame whose every remaining value is legal for its own row's class.
    """
    schema: pl.Schema = lf.collect_schema()
    names: list[str] = schema.names()
    disabled: list[str] = [c for c in names if c in DISABLED_EDGE_FIELDS]
    if disabled:
        lf = lf.drop(disabled)
        names = [c for c in names if c not in DISABLED_EDGE_FIELDS]
    if "category" not in names:
        return lf

    core: frozenset[str] = frozenset({"category", "subject", "object", "predicate", "id"})
    candidates: list[str] = [c for c in names if c not in core]
    if not candidates:
        return lf

    categories: list[str] = [f"biolink:{c.value}" for c in EdgeCategories]
    first: pl.Expr = pl.col("category").list.first()
    updates: list[pl.Expr] = []
    rescued: list[pl.Expr] = []
    for col in candidates:
        text: pl.Expr = (
            pl.col(col).list.eval(pl.element().cast(pl.String)).list.join(", ") if isinstance(schema[col], pl.List) else pl.col(col).cast(pl.String)
        )
        declares: dict[str, bool] = {cat: col in class_fields(association_class(cat)) for cat in categories}
        # CLASS_FIELD_OVERRIDES grants a slot to a class that does not declare it
        # (deliberately ahead of the pinned model); granted rows keep the value.
        accepts: dict[str, bool] = {
            cat: declares[cat] or col in CLASS_FIELD_OVERRIDES.get(cat.removeprefix("biolink:"), frozenset()) for cat in categories
        }
        # A closed-vocabulary slot additionally constrains the *value*. A qualifier
        # encoded from a column carries whatever the sheet holds, so the token can only
        # be checked here -- config-time validation sees no data.
        vocabulary: frozenset[str] | None = ENUM_RANGED_QUALIFIERS.get(col)
        ok: pl.Expr = first.replace_strict(accepts, default=False) if not all(accepts.values()) else pl.lit(True)
        if vocabulary is not None:
            ok = ok & text.is_in(list(vocabulary))
        if all(accepts.values()) and vocabulary is None:
            keep: pl.Expr = pl.col(col)
        elif not any(accepts.values()):
            continue  # Handled upstream by the allow-list / study routing.
        else:
            keep = pl.when(ok).then(pl.col(col)).otherwise(None)
            # Preserve what the class refuses rather than deleting it outright; the
            # value is real evidence, it just has no slot on this association class.
            rescued.append(pl.when(ok | text.is_null()).then(None).otherwise(pl.concat_str([pl.lit(f"{col}="), text])))
        # A column has one dtype, so the multivalued wrap must be uniform across rows:
        # wrap every value into a one-element list when every class declaring the slot
        # types it multivalued. `is_multivalued` is False for classes that do not declare
        # the slot at all, so the scan is restricted to declaring classes -- including
        # them is what produced a spuriously mixed per-row wrap that died in
        # strict_cast at collect. Rows whose class rejects the slot are already null and
        # stay null. A hypothetically mixed slot keeps its scalar rather than crashing.
        declaring: list[str] = [cat for cat in categories if declares[cat]]
        listed: dict[str, bool] = {cat: is_multivalued(association_class(cat), col) for cat in declaring}
        if listed and all(listed.values()) and not isinstance(schema[col], pl.List):
            # concat_list maps null -> [null]; the when preserves real nulls instead.
            keep = pl.when(keep.is_null()).then(None).otherwise(pl.concat_list(keep))
        updates.append(keep.alias(col))
    if rescued:
        updates.append(pl.concat_list(rescued).list.drop_nulls().alias(PRUNED_COLUMN))
    return lf.with_columns(updates) if updates else lf


def value(lf: pl.LazyFrame, col: str, x: object) -> pl.LazyFrame:
    """Add a new column populated with a literal value.

    Args:
        lf: Source LazyFrame.
        col: Name of the new column.
        x: Literal value to populate every row with.

    Returns:
        LazyFrame with the new literal column appended.
    """
    return lf.with_columns(pl.lit(x).alias(col))


def _retrieval_source(resource_id: str, resource_role: str, upstream: list[str] | None = None, urls: list[str] | None = None) -> pl.Expr:
    """Build one ``RetrievalSource`` struct expression.

    Every entry declares the same four fields so that :func:`retrieval_sources` can
    ``concat_list`` them into a single ``list[struct]`` column; absent list fields are
    typed nulls, which the Rust null-stripper removes from the emitted JSON.
    """
    empty: pl.Expr = pl.lit(None, dtype=pl.List(pl.String))
    return pl.struct(
        pl.lit(resource_id).alias("resource_id"),
        pl.lit(resource_role).alias("resource_role"),
        (pl.concat_list([pl.lit(x) for x in upstream]) if upstream else empty).alias("upstream_resource_ids"),
        (pl.concat_list([pl.lit(x) for x in urls]) if urls else empty).alias("source_record_urls"),
    )


PRUNED_COLUMN: str = "_pruned_by_class"
"""Internal handoff column: values `prune_to_class` removed, for the study to absorb.

Never reaches output -- :func:`inline_supporting_study` folds it into the
``StudyResult`` description and drops it.
"""

_NULL_LIKE_TEXT: tuple[str, ...] = ("", "na", "nan", "null", "none")


def _has_nonblank_study_value(col: str, dtype: Any) -> pl.Expr:
    """Return a row-wise presence test for a routed Study value."""
    value: pl.Expr = pl.col(col)
    if isinstance(dtype, pl.List):
        normalized: pl.Expr = value.list.eval(pl.element().cast(pl.String).str.strip_chars().str.to_lowercase())
        present: pl.Expr = normalized.list.eval(pl.element().is_not_null() & ~pl.element().is_in(_NULL_LIKE_TEXT)).list.sum()
        return present.gt(0).fill_null(False)
    kind: str | None = numeric_slot_kind(col)
    if kind == "int":
        number: pl.Expr = value.cast(pl.Float64, strict=False)
        return (number.is_finite() & (number >= 0) & (number.floor() == number)).fill_null(False)
    if kind == "float":
        return value.cast(pl.Float64, strict=False).is_finite().fill_null(False)
    if dtype == pl.String:
        text: pl.Expr = value.cast(pl.String).str.strip_chars().str.to_lowercase()
        return text.is_not_null() & ~text.is_in(_NULL_LIKE_TEXT)
    if dtype in {pl.Float32, pl.Float64}:
        return value.cast(pl.Float64, strict=False).is_finite().fill_null(False)
    return value.is_not_null()


def _study_metadata_expr(col: str, dtype: Any) -> pl.Expr:
    """Build a Study field expression, normalizing scalar method types to a list."""
    value: pl.Expr = pl.col(col)
    kind: str | None = numeric_slot_kind(col)
    if kind == "int":
        number: pl.Expr = value.cast(pl.Float64, strict=False)
        valid_count: pl.Expr = number.is_finite() & (number >= 0) & (number.floor() == number)
        return pl.when(valid_count).then(number.cast(pl.Int64, strict=False)).otherwise(None).alias(col)
    if kind == "float":
        return value.cast(pl.Float64, strict=False).alias(col)
    if col != "study_method_types":
        if dtype == pl.String:
            text: pl.Expr = value.cast(pl.String).str.strip_chars()
            return pl.when(text.is_null() | text.str.to_lowercase().is_in(_NULL_LIKE_TEXT)).then(None).otherwise(text).alias(col)
        return value.alias(col)

    if isinstance(dtype, pl.List):
        cleaned: pl.Expr = value.list.eval(pl.element().cast(pl.String).str.strip_chars())
        valid: pl.Expr = cleaned.list.eval(
            pl.when(pl.element().is_null() | pl.element().str.to_lowercase().is_in(_NULL_LIKE_TEXT)).then(None).otherwise(pl.element())
        ).list.drop_nulls()
        return pl.when(valid.list.len() == 0).then(pl.lit(None, dtype=pl.List(pl.String))).otherwise(valid).alias(col)

    text: pl.Expr = value.cast(pl.String).str.strip_chars()
    return (
        pl.when(text.is_null() | text.str.to_lowercase().is_in(_NULL_LIKE_TEXT))
        .then(pl.lit(None, dtype=pl.List(pl.String)))
        .otherwise(pl.concat_list(text))
        .alias(col)
    )


def inline_supporting_study(lf: pl.LazyFrame, study_id: str, study_name: str | None, identified: bool = True) -> pl.LazyFrame:
    """Attach table provenance and homeless statistics as an inlined Biolink ``Study``.

    Follows the COHD/ICEES pattern in ``translator-ingests``: the edge carries
    ``has_supporting_studies`` (``dict[str, Study]``, ``inlined: true`` on
    ``Association``) and each ``Study`` may carry ``has_study_results``. The Study is
    deliberately *not* written to the nodes file, matching those ingests.

    The struct is shaped by the current Biolink Model (4.4.4+):

    * ``Study.id`` is a real identifier -- the section's publication CURIE -- and
      ``Study.name`` a DISJOINT human label (worksheet or source filename); the two
      are never composed into one string.
    * Study-level metadata declared as annotations (``study_size``, ``study_cohort``,
      ``study_context``, ``study_date_range``, ``study_method_description``,
      ``study_method_types`` -- see :data:`biolink.STUDY_METADATA_FIELDS`) lands as
      real typed fields ON the Study. Biolink PR #1770 deprecated the old
      ``supporting_study_*`` association slots in favor of exactly these Study node
      properties; the coercion phase renames the legacy spellings onto them before
      this op runs.
    * Each ``StudyResult`` is identified by the scoped CURIE ``row:<N>`` (its source
      row; local to the containing inlined Study -- results are never standalone
      nodes) and carries NO name: row provenance is fully expressed by the id.
    * ``StudyResult.description`` preserves only what has no structured home:
      leftover unsatisfiable columns and class-pruned qualifiers, as ``col=value``
      entries.

    **Nothing to say, nothing emitted.** Biolink defines ``has supporting studies`` as
    "studies that produced information used as evidence", so the struct has to earn its
    place. An UNIDENTIFIED section -- one whose ``study_id`` fell back to the config
    stem because it declares no publication -- with no study metadata, no routed
    columns and nothing pruned would otherwise emit a Study named after a config file
    on every edge. That is a fabricated study, and no translator-ingests source models
    evidence that way. In that case the struct is skipped; the metadata/routed/sheet/row
    columns are dropped either way. A section WITH a publication always keeps the
    wrapper -- the publication CURIE is genuine provenance -- as does any section that
    has content to carry.

    ``study_id`` is a per-section constant, so it can key a static struct field.

    Args:
        lf: Edges LazyFrame after annotation and provenance ops.
        study_id: Stable study identifier (publication CURIE, or the config stem when
            unidentified).
        study_name: Human-readable study label (worksheet or source filename), or
            ``None`` to omit it.
        identified: Whether ``study_id`` names a real publication rather than falling
            back to the config stem. ``False`` lets a contentless study be skipped.

    Returns:
        LazyFrame with ``has_supporting_studies`` appended (when it carries anything)
        and the metadata/routed columns dropped.
    """
    schema: Any = lf.collect_schema()
    names: list[str] = schema.names()
    metadata: list[str] = sorted(c for c in names if c in STUDY_METADATA_FIELDS)
    routed: list[str] = sorted(
        c for c in names if c in UNSATISFIABLE_EDGE_FIELDS and c not in DISABLED_EDGE_FIELDS and c not in STUDY_METADATA_FIELDS
    )
    disabled: list[str] = sorted(c for c in names if c in DISABLED_EDGE_FIELDS)
    row: str = "extracted_from_row_number"
    has_row: bool = row in names
    pruned: bool = PRUNED_COLUMN in names
    content_columns: list[str] = [*metadata, *routed, *([PRUNED_COLUMN] if pruned else [])]
    has_content: pl.Expr = pl.lit(False)
    for col in content_columns:
        has_content = has_content | _has_nonblank_study_value(col, schema[col])

    drop: list[str] = [
        *metadata,
        *routed,
        *disabled,
        *([row] if has_row else []),
        *(["sheet_name"] if "sheet_name" in names else []),
        *([PRUNED_COLUMN] if pruned else []),
    ]
    if not identified and not content_columns:
        return lf.drop(drop)

    if not has_row and (routed or pruned):
        raise ValueError("StudyResult descriptions require `extracted_from_row_number` for a row:<N> id")

    normalized_name: str | None = None if study_name == study_id else study_name
    study_fields: list[pl.Expr] = [
        pl.lit(study_id).alias("id"),
        pl.lit(normalized_name, dtype=pl.String).alias("name"),
        *(_study_metadata_expr(c, schema[c]) for c in metadata),
    ]
    # A StudyResult exists only for a row that carries content worth anchoring: study
    # metadata, leftover unsatisfiable columns, or class-pruned values. A direct frame
    # without a row index can still carry Study metadata, but cannot invent a result id.
    if has_row and content_columns:
        result_id: pl.Expr = pl.concat_str([pl.lit("row:"), pl.col(row).cast(pl.String)])
        fields: list[pl.Expr] = [result_id.alias("id")]
        if routed or pruned:
            parts: list[pl.Expr] = []
            for col in routed:
                text: pl.Expr = pl.col(col).cast(pl.String).str.strip_chars()
                blank: pl.Expr = text.is_null() | (text.str.len_chars() == 0) | text.str.to_lowercase().is_in(_NULL_LIKE_TEXT)
                parts.append(pl.when(blank).then(pl.lit(None, dtype=pl.String)).otherwise(pl.concat_str([pl.lit(f"{col}="), text])))
            # Qualifiers the resolved association class refuses (see `prune_to_class`) are
            # appended to the routed leftovers. Build from whichever sources exist: an
            # empty list literal would be a zero-length series and fail to broadcast.
            summary: pl.Expr
            if parts and pruned:
                summary = pl.concat_list(parts).list.drop_nulls().list.concat(pl.col(PRUNED_COLUMN))
            elif parts:
                summary = pl.concat_list(parts)
            else:
                summary = pl.col(PRUNED_COLUMN)
            joined: pl.Expr = summary.list.drop_nulls().list.join("; ")
            fields.append(pl.when(joined.str.len_chars() == 0).then(pl.lit(None, dtype=pl.String)).otherwise(joined).alias("description"))
        result: pl.Expr = pl.concat_list(pl.struct(fields))
        study_fields.append(pl.when(has_content).then(result).otherwise(None).alias("has_study_results"))

    study: pl.Expr = pl.struct(study_fields)
    supporting: pl.Expr = pl.struct(study.alias(study_id))
    if not identified:
        supporting = pl.when(has_content).then(supporting).otherwise(None)
    out: pl.LazyFrame = lf.with_columns(supporting.alias("has_supporting_studies"))
    return out.drop(drop)


def retrieval_sources(
    lf: pl.LazyFrame,
    primary: str,
    upstream: list[str],
    urls: list[str],
    upstream_urls: dict[str, list[str]] | None = None,
    explicit: list[dict[str, Any]] | None = None,
) -> pl.LazyFrame:
    """Add the Biolink ``sources`` retrieval-provenance column.

    ``upstream_resource_ids`` and ``source_record_urls`` have ``domain: retrieval
    source`` in the Biolink Model, so they are properties of an entry in ``sources``
    -- not of the association. Emitting them flat on the edge makes every record fail
    validation with ``extra_forbidden``.

    Mirrors ``build_association_knowledge_sources()`` from
    ``translator-ingests/util/biolink.py``: the primary knowledge source lists the
    upstream resources, and each upstream resource additionally appears as its own
    ``supporting_data_source`` entry. By default the primary entry carries the
    section's source record URLs; when ``upstream_urls`` is given, URL placement is
    fully determined by that mapping instead -- each listed upstream entry carries
    its own ``source_record_urls`` and the primary entry emits none (the primary is
    the transforming resource, not a downloadable record).

    When ``explicit`` is given, exactly those entries are emitted, in order, and
    ``primary``/``upstream``/``urls``/``upstream_urls`` are ignored. Each entry
    template carries ``resource_id``, ``resource_role``, and optional
    ``upstream_resource_ids``/``source_record_urls``; ``source_record_urls`` values
    may contain the literal ``{edge_id}`` placeholder, which is NOT resolved here
    (the edge id is only assigned at the final dedup stage) but in a post-dedup
    sweep of the final edges NDJSON.

    Args:
        lf: Source LazyFrame.
        primary: Infores CURIE of the primary knowledge source.
        upstream: Infores CURIEs of upstream/supporting data sources.
        urls: Source record URLs for the primary entry (ignored when ``upstream_urls`` is set).
        upstream_urls: Optional per-upstream source record URLs keyed by infores CURIE.
        explicit: Optional explicit entry templates emitted verbatim, in order.

    Returns:
        LazyFrame with a ``sources`` ``list[struct]`` column appended.
    """
    if explicit is not None:
        entries: list[pl.Expr] = [
            _retrieval_source(entry["resource_id"], entry["resource_role"], entry.get("upstream_resource_ids"), entry.get("source_record_urls"))
            for entry in explicit
        ]
    elif upstream_urls is not None:
        entries = [_retrieval_source(primary, "primary_knowledge_source", upstream)]
        entries.extend(_retrieval_source(x, "supporting_data_source", urls=upstream_urls.get(x)) for x in upstream)
    else:
        entries = [_retrieval_source(primary, "primary_knowledge_source", upstream, urls)]
        entries.extend(_retrieval_source(x, "supporting_data_source") for x in upstream)
    return lf.with_columns(pl.concat_list(entries).alias("sources"))


def publications(lf: pl.LazyFrame, curies: str | list[str]) -> pl.LazyFrame:
    """Add publication CURIEs as a Biolink-compliant ``list[str]`` column.

    Args:
        lf: Source LazyFrame.
        curies: One publication CURIE or a list of publication CURIEs.

    Returns:
        LazyFrame with the new ``publications`` list column appended.
    """
    values: list[str] = [curies] if isinstance(curies, str) else curies
    return lf.with_columns(pl.concat_list([pl.lit(curie) for curie in values]).alias("publications"))


def column(lf: pl.LazyFrame, col: str, x: str) -> pl.LazyFrame:
    """Add a new column copied from an existing column.

    Args:
        lf: Source LazyFrame.
        col: Name of the new column.
        x: Name of the source column to copy.

    Returns:
        LazyFrame with the new column appended.
    """
    return lf.with_columns(pl.col(x).alias(col))


def math_op(lf: pl.LazyFrame, col: str, func: Functions, args: list[Literal[Tokens.VALUES] | float | int]) -> pl.LazyFrame:
    """Transform values in a column using a native polars expression.

    Args:
        lf: Source LazyFrame.
        col: Column to transform in place.
        func: One of the allowed ``Functions`` members (``pow``, ``copysign``).
        args: Argument list, where ``"values"`` is replaced by the current
            cell value.

    Returns:
        LazyFrame with the transformed column.

    Notes:
        ``strict=False`` tolerates residual non-numeric junk in numeric
        annotation columns. ``copysign(x, y)`` is expressed as ``|x|`` with
        the sign of ``y``, matching ``math.copysign`` including ``y == 0``.
    """
    values: pl.Expr = pl.col(col).cast(pl.Float64, strict=False)
    exprs: list[pl.Expr] = [values if a == Tokens.VALUES else pl.lit(a) for a in args]
    match Functions(func):
        case Functions.POW:
            out: pl.Expr = exprs[0].pow(exprs[1])
        case Functions.COPYSIGN:
            out = pl.when(exprs[1] >= 0).then(exprs[0].abs()).otherwise(-exprs[0].abs())
    return lf.with_columns(out.alias(col))


def numeric_columns(names: list[str]) -> list[str]:
    """Return column names that should be coerced and formatted as numbers.

    P-value columns by substring plus the exact ``effect_size``,
    ``number_of_cases`` and ``study_size`` fields. The old ``sample_size`` /
    ``relationship_strength`` / ``supporting_study_size`` names are absent on
    purpose: the column coercions rename them to ``study_size`` / ``effect_size``
    before ``clean_numeric`` / ``format_numeric`` run.

    Args:
        names: Schema column names to filter.

    Returns:
        Subset of ``names`` destined for numeric coercion/formatting.
    """
    # P-value columns by substring plus exact effect-size, case-count and study-size fields.
    exact: set[str] = {"effect_size", "number_of_cases", "study_size"}
    return [c for c in names if ("p_value" in c.lower()) or (c in exact)]


def clean_numeric(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Coerce numeric annotation columns to Float64, dropping non-numeric values to null.

    Only touches p-value, effect-size, number-of-cases and study-size columns.

    Args:
        lf: Source LazyFrame.

    Returns:
        LazyFrame with the matched columns cast to Float64 (no-op if none match).
    """
    # Only touches p-value, effect-size and study-size columns.
    cols: list[str] = numeric_columns(lf.collect_schema().names())
    if not cols:
        return lf
    return lf.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in cols])


def format_numeric(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Normalize numeric annotation columns for output.

    P-value columns (any name containing ``p_value``) are always emitted as
    controlled scientific-notation strings (``{:.4e}``, e.g. ``"1.0000e-03"``):
    notation is part of the output contract (see the tutorial's edge example), and
    shortest-repr JSON numbers would render the same values as ``0.0001`` / ``0.05``.
    Biolink types ``p_value`` / ``adjusted_p_value`` as ``float``, but the validation
    here and downstream runs in Pydantic's lax mode, which coerces the numeric string
    back -- so the notation control costs no KGX validity.

    Remaining numeric columns are emitted as real JSON numbers when the installed
    biolink model types them ``int`` / ``float`` -- ``effect_size`` (``float``,
    biolink-model#1774), ``number_of_cases`` (``int`` on the disease/phenotype
    ``Association`` classes) and ``study_size`` (``int`` on the inlined ``Study``,
    biolink-model#1770) -- and keep the controlled decimal general string notation
    (``{:.4g}``) only while a slot stays untyped, since such values end up in
    human-readable text. Null values stay null.

    Args:
        lf: Source LazyFrame.

    Returns:
        New LazyFrame (eagerly collected then re-lazied) with matched columns
        typed or formatted.

    Notes:
        Collection point: the column is formatted in one batch so notation is
        controlled across all rows at once.
    """
    # Collection point: batch formatting for notation control.
    df: pl.DataFrame = lf.collect()
    for c in numeric_columns(df.columns):
        df = df.with_columns(pl.col(c).cast(pl.Float64, strict=False).alias(c))
        if "p_value" in c.lower():
            # Scientific notation is the p-value output contract; the branch ordering is
            # the guard, so the numeric-slot short-circuits below can never fire for
            # these columns even though the model types them ``float``.
            fmt: str = "{:.4e}"
        else:
            kind: str | None = numeric_slot_kind(c)
            if kind == "float":
                continue
            if kind == "int":
                number: pl.Expr = pl.col(c).cast(pl.Float64, strict=False)
                valid_count: pl.Expr = number.is_finite() & (number >= 0) & (number.floor() == number)
                df = df.with_columns(pl.when(valid_count).then(number.cast(pl.Int64, strict=False)).otherwise(None).alias(c))
                continue
            fmt = "{:.4g}"
        formatted: list[str | None] = [None if v is None else fmt.format(v) for v in df[c].to_list()]
        df = df.with_columns(pl.Series(c, formatted))
    return df.lazy()


def split_expr(col: str, delimiter: str) -> pl.Expr:
    """Build the shared "split a delimited cell into items" expression.

    The single splitting primitive behind both delimiter-driven ops: ``explode_by``
    fans the items out into rows (node encodings), ``split_by`` keeps them as a real
    JSON array on the row (annotations). Only what happens to the items afterwards
    differs, so the parsing rules stay defined in exactly one place.

    Items are trimmed and blanks are dropped -- a trailing or doubled separator
    (``"a;b;"``, ``"a;;b"``) is a delimited-text artifact, not a value. A null cell
    stays null rather than becoming a one-element list of null.

    Args:
        col: Column whose string values should be split.
        delimiter: Separator to split on.

    Returns:
        Expression yielding a ``list[str]`` column (null preserved).
    """
    text: pl.Expr = pl.col(col).cast(pl.String)
    items: pl.Expr = text.str.split(delimiter).list.eval(pl.element().str.strip_chars()).list.drop_nulls()
    return pl.when(text.is_null()).then(None).otherwise(items.list.eval(pl.element().filter(pl.element() != "")))


def split_list(lf: pl.LazyFrame, col: str, delimiter: str) -> pl.LazyFrame:
    """Split a delimited cell into a real JSON array, in place.

    A column encoding is scalar by construction, so a multivalued Biolink slot such as
    ``has_evidence`` fed from an aggregated cell would otherwise be emitted as a single
    joined string -- and ``prune_to_class`` wraps that scalar into a
    one-element list, so the value survives Biolink validation while consumers iterate a
    single ``"a|b|c"`` blob instead of three ids.

    Same split as ``explode``, minus the fan-out: this is the one multivalued
    encoding (``split_by``), turning each row's cell into its own JSON array.

    Args:
        lf: Source LazyFrame.
        col: Annotation column to split.
        delimiter: Separator to split on.

    Returns:
        LazyFrame with ``col`` converted to a ``list[str]`` column, blanks dropped.
    """
    return lf.with_columns(split_expr(col, delimiter).alias(col))


def prefix(lf: pl.LazyFrame, col: str, prefix: str) -> pl.LazyFrame:
    expr: pl.Expr = pl.lit(prefix) + pl.col(col).cast(pl.String)
    return lf.with_columns(expr.alias(col))


def suffix(lf: pl.LazyFrame, col: str, suffix: str) -> pl.LazyFrame:
    expr: pl.Expr = pl.col(col).cast(pl.String) + pl.lit(suffix)
    return lf.with_columns(expr.alias(col))


def regex(lf: pl.LazyFrame, col: str, pattern: str, replacement: str = "") -> pl.LazyFrame:
    expr: pl.Expr = pl.col(col).cast(pl.String).str.replace_all(pattern, replacement)
    return lf.with_columns(expr.alias(col))


def fill(lf: pl.LazyFrame, col: str, method: str) -> pl.LazyFrame:
    expr: pl.Expr = pl.col(col).fill_null(strategy=method)  # pyright: ignore
    return lf.with_columns(expr.alias(col))


def explode(lf: pl.LazyFrame, col: str, delimiter: str) -> pl.LazyFrame:
    """Explode one row with delimited items into many rows.

    Args:
        lf: Source LazyFrame.
        col: Column whose string values should be split.
        delimiter: Delimiter to split on.

    Returns:
        LazyFrame with one row per item (the split column becomes a list
        before the explode).

    Notes:
        Shares ``split_expr`` with ``split_list`` (the ``split_by`` annotation op), so
        both read a delimited cell the same way: items trimmed, blanks dropped. A
        trailing/doubled separator therefore no longer fans out rows carrying ``""``,
        which only ever failed entity resolution and dropped the edge downstream.
    """
    return lf.with_columns(split_expr(col, delimiter).alias(col)).explode(col)


def idx(lf: pl.LazyFrame, col: str = "extracted_from_row_number") -> pl.LazyFrame:
    """Create a 1-based index column recording the original source row.

    Args:
        lf: Source LazyFrame.
        col: Name of the new index column.

    Returns:
        LazyFrame with the index column appended.

    Notes:
        Matches pre-8.0.0 behavior; ``inline_supporting_study`` consumes the
        column as the scoped ``StudyResult.id`` (``row:<N>``), not as edge text.
    """
    return lf.with_row_index(col, offset=1)


def csv(p: Path, sep: str) -> pl.LazyFrame:
    """Read a CSV or TSV source as a LazyFrame.

    Args:
        p: Path to the delimited text file.
        sep: Column delimiter character.

    Returns:
        LazyFrame over the file (no header inference; all columns raw).
    """
    return pl.scan_csv(source=p, separator=sep, has_header=False, infer_schema_length=None, truncate_ragged_lines=True)


def excel(p: Path, sheet: str, engine: str = "calamine") -> pl.LazyFrame:
    """Read an Excel sheet as a LazyFrame.

    Args:
        p: Path to the workbook.
        sheet: Sheet name to read.
        engine: Polars Excel engine name.

    Returns:
        LazyFrame over the sheet contents (no header inference).
    """
    df: pl.DataFrame = pl.read_excel(
        source=p,
        sheet_name=sheet,
        engine=engine,  # pyright: ignore
        has_header=False,
        infer_schema_length=None,
    )
    return df.lazy()


def crop(lf: pl.LazyFrame, row_slice: list[NonNegativeInt | Literal[Tokens.AUTO]]) -> pl.LazyFrame:
    """Take a contiguous slice from a LazyFrame.

    Args:
        lf: Source LazyFrame.
        row_slice: Two-element ``[start, stop]`` list, where either bound may
            be the literal ``Tokens.AUTO`` sentinel.

    Returns:
        LazyFrame restricted to the requested row range.

    Notes:
        Collection point: height must be computed eagerly to resolve
        ``Tokens.AUTO`` bounds.
    """
    df: pl.DataFrame = lf.collect()
    n: int = df.select(pl.len()).item()
    start: int | Literal[Tokens.AUTO] = row_slice[0]
    stop: int | Literal[Tokens.AUTO] = row_slice[1]
    offset: int = 0 if start == Tokens.AUTO else start  # pyright: ignore
    length: int = n if stop == Tokens.AUTO else (stop - offset)  # pyright: ignore
    df = df.slice(offset=offset, length=length)
    return df.lazy()


def pick(lf: pl.LazyFrame, rows: list[int]) -> pl.LazyFrame:
    """Pick an explicit list of rows from a LazyFrame.

    Args:
        lf: Source LazyFrame.
        rows: Row indices to retain.

    Returns:
        LazyFrame containing only the requested rows, in the given order.

    Notes:
        Collection point: ``gather()`` requires an eager frame, so the result
        is re-lazied afterwards.
    """
    df: pl.DataFrame = lf.collect()
    df = df.select(pl.all().gather(indices=rows))
    return df.lazy()


HEAD_ROWS: int = 5


def head(lf: pl.LazyFrame, n: int = HEAD_ROWS) -> pl.LazyFrame:
    """Randomly sample up to ``min(n, height)`` rows for a fast ``--head`` preview.

    Args:
        lf: Source LazyFrame.
        n: Maximum number of rows to keep.

    Returns:
        LazyFrame with at most ``n`` randomly chosen rows (fewer when the source
        has fewer than ``n``).

    Notes:
        ``LazyFrame`` has no ``.sample`` and ``DataFrame.sample`` raises when ``n``
        exceeds the height, so the frame is collected and sampled with
        ``min(n, height)`` (mirroring ``pick``), then re-lazied.
    """
    df: pl.DataFrame = lf.collect()
    return df.sample(n=min(n, df.height), shuffle=True).lazy()


def reindex(df: pl.LazyFrame, col: str, op: Callable, comp: str | int | float, cast: bool = True) -> pl.LazyFrame:
    """Reindex a LazyFrame by filtering rows on a column condition.

    Args:
        df: Source LazyFrame.
        col: Column to compare.
        op: Binary comparison callable (e.g. ``operator.lt``).
        comp: Right-hand comparison value.
        cast: When True, cast ``col`` to Float64 before comparison.

    Returns:
        LazyFrame containing only rows where ``op(col, comp)`` is true.
    """
    expr: pl.Expr = pl.col(col).cast(pl.Float64) if cast else pl.col(col)
    return df.filter(op(expr, comp))


def drop_not_significant(lf: pl.LazyFrame, col: str = "statistical_significance_qualifier") -> pl.LazyFrame:
    """Drop release-mode edges whose significance qualifier is ``not_significant``.

    Args:
        lf: Source LazyFrame.
        col: Significance qualifier column name.

    Returns:
        LazyFrame with non-significant edges removed.

    Notes:
        Only filters when the qualifier column exists; no-op for sections
        without a ``p_value`` column. ``ne_missing`` keeps null qualifiers
        (null p-value → null qualifier → kept, not dropped). The band value is
        the bare ``StatisticalSignificanceQualifierEnum`` token -- biolink-model
        4.4.4 types the slot with that enum, so no ``biolink:`` prefix exists.
    """
    names: list[str] = lf.collect_schema().names()
    if col not in names:
        return lf
    return lf.filter(pl.col(col).cast(pl.String).ne_missing("not_significant"))


def drop_zero_effect_size(lf: pl.LazyFrame, col: str = "effect_size") -> pl.LazyFrame:
    """Drop release-mode edges whose effect size is exactly zero.

    Args:
        lf: Source LazyFrame.
        col: Effect-size column name.

    Returns:
        LazyFrame with zero effect-size edges removed.

    Notes:
        Only filters when the effect-size column exists; no-op for sections
        without an ``effect_size`` column. Null effect sizes are kept (no
        score was detected for that row). The cast is non-strict (like
        ``clean_numeric``) and paired with ``ne_missing``: a non-numeric cell --
        e.g. a header row flowing through as data because the ``csv`` op reads
        with ``has_header=False`` -- becomes null and is kept, dying later at
        entity resolution instead of crashing the build with a strict-cast error.
    """
    names: list[str] = lf.collect_schema().names()
    if col not in names:
        return lf
    return lf.filter(pl.col(col).cast(pl.Float64, strict=False).ne_missing(0.0))


def drop_low_number_of_cases(lf: pl.LazyFrame, col: str = "number_of_cases", threshold: float = 10.0) -> pl.LazyFrame:
    """Drop release-mode ``applied_to_treat`` edges whose number of cases is below the threshold.

    Args:
        lf: Source LazyFrame.
        col: Number-of-cases column name.
        threshold: Minimum number of cases required to keep the edge.

    Returns:
        LazyFrame with low-number-of-cases edges removed.

    Notes:
        Only filters when the number-of-cases column exists; no-op for sections
        without a ``number_of_cases`` column. Null case counts are kept (no count
        was detected for that row). The cast is non-strict (like ``clean_numeric``)
        with ``fill_null(True)``: a non-numeric cell -- e.g. a header row flowing
        through as data because the ``csv`` op reads with ``has_header=False`` --
        becomes null and is kept, dying later at entity resolution instead of
        crashing the build with a strict-cast error. Only wired in for
        ``applied_to_treat`` sections at op-construction time, so it never
        touches other predicates.
    """
    names: list[str] = lf.collect_schema().names()
    if col not in names:
        return lf
    return lf.filter(pl.col(col).cast(pl.Float64, strict=False).ge(threshold).fill_null(True))


def idxname(col: Any) -> str:
    """Convert Excel-style column letters (e.g. ``"AA"``) to a polars-style ``column_<n>`` name.

    Args:
        col: Excel column letter (or any value whose ``str`` form is one).

    Returns:
        ``f"column_{idx}"`` where ``idx`` is the 1-based column number.
    """
    scol: str = str(col)
    idx: int = 0
    for char in scol:
        idx = idx * 26 + (ord(char) - 65 + 1)

    return f"column_{idx}"


def trim(lf: pl.LazyFrame, regex: str = r"^column_\d+$") -> pl.LazyFrame:
    """Remove columns whose names match the Excel-style ``column_<n>`` convention.

    Args:
        lf: Source LazyFrame.
        regex: Column-name pattern to drop.

    Returns:
        LazyFrame with matching columns excluded.
    """
    return lf.select(pl.exclude(regex))


def to_store(lf: pl.LazyFrame, p: Path, config_name: str) -> Path:
    """Collect and write a section LazyFrame to a parquet file.

    Args:
        lf: Section LazyFrame to materialize.
        p: Destination parquet path.
        config_name: Human-readable section name used in the empty-row warning.

    Returns:
        The parquet path written.

    Notes:
        Warns when the collected frame has zero rows.
    """
    df: pl.DataFrame = lf.collect()

    if df.height == 0:
        logger.warning("Section {config} ({hash}) produced 0 rows", config=config_name, hash=p.stem)
    df.write_parquet(p)

    return p


class Tcode(Section):
    """Extend ``Section`` with the per-section operation list that compiles a knowledge graph."""

    config: Path = Field(...)
    store: Path = Field(...)
    log: bool = Field(False)
    qc: bool = Field(False)
    release: bool = Field(False)
    head: bool = Field(False)
    name: str | None = Field(None)
    infores: str | None = Field(None)
    qc_similarity_threshold: float = Field(0.5)
    qc_fuzzy_ratio_threshold: float = Field(70.0)
    qc_fuzzy_partial_threshold: float = Field(80.0)
    qc_aliases: dict[str, str] = Field(default_factory=dict)

    def encoding(self: Self, x: Encoding, col: str, table_literal: bool = False) -> list[Any]:
        """Collect helper for Encoding classes.

        Builds the ordered list of (callable, args) tuples that implement an
        ``Encoding`` against a single column.

        Args:
            x: Encoding configuration.
            col: Target column name.
            table_literal: When True, also emit a tuple preserving the raw
                column under an ``original_`` prefix.

        Returns:
            Flat list of ``Any`` items (tuples or lists of tuples, possibly
            containing ``None`` placeholders) ready for ``clean`` to filter.
        """
        return [
            (value, (col, x.encoding)) if x.method == EncodingMethods.VALUE else None,
            (column, (col, idxname(x.encoding))) if x.method == EncodingMethods.COLUMN else None,
            (column, (f"original_{col}", col)) if table_literal else None,
            (fill, (col, x.fill)) if x.fill else None,
            (explode, (col, x.explode_by)) if x.explode_by else None,
            [(regex, (col, r.pattern, r.replacement)) for r in x.regex] if x.regex else None,
            [(regex, (col, r)) for r in x.remove] if x.remove else None,
            (prefix, (col, x.prefix)) if x.prefix else None,
            (suffix, (col, x.suffix)) if x.suffix else None,
            [(math_op, (col, t.function, t.arguments)) for t in x.transformations] if x.transformations else None,
        ]

    def node_prep(self: Self, x: NodeEncoding, col: str) -> list[Any]:
        """Collect helper for NodeEncoding classes.

        Encoding plus NLP normalization, before ``resolve_batch``.

        Args:
            x: NodeEncoding configuration.
            col: Target column name.

        Returns:
            Combined list of encoding tuples plus the pre-resolution column
            copy and ``level_one``/``level_two`` normalization tuples.
        """
        encoding: list[Any] = self.encoding(x, col, table_literal=True)
        prep: list[Any] = [(column, (f"{col}_pre_resolution", col)), (level_one, (col,)), (level_two, (col,))]
        return encoding + prep

    def clean(self: Self, tcode: list[tuple[Callable, Any]]) -> list[tuple[Callable, tuple[Any]]]:
        """Clean a Tcode list so it can be used with ``reduce`` from functools.

        Args:
            tcode: Raw list possibly containing ``None`` placeholders and
                nested lists of tuples.

        Returns:
            Flat list of ``(callable, tuple[Any, ...])`` pairs with falsy
            entries removed.
        """
        return [op for x in tcode if x for op in (self.clean(x) if isinstance(x, list) else [x])]

    def _source_ops(self: Self) -> list[Any]:
        """Collect the load/filter/clean/significance ops that precede entity resolution.

        Returns:
            Raw (possibly nested, possibly ``None``-padded) op list ready for
            ``clean``; order matches the pre-resolution pipeline exactly.
        """
        return [
            (csv, (self.source.local, self.source.delimiter)) if self.source.kind == Files.TEXT else None,  # pyright: ignore
            (excel, (self.source.local, self.source.sheet)) if self.source.kind == Files.EXCEL else None,  # pyright: ignore
            (idx, ()),
            (crop, (self.source.row_slice,)) if self.source.row_slice else None,
            (pick, (self.source.rows,)) if self.source.rows else None,
            [
                (reindex, (idxname(x.column), getattr(operator, x.comparison), x.comparator))
                if x.comparison not in ["ne", "eq"]
                else (reindex, (idxname(x.column), getattr(operator, x.comparison), x.comparator, False))
                for x in self.source.reindex
            ]
            if self.source.reindex
            else None,
            # --head preview: randomly sample min(HEAD_ROWS, height) rows before any encoding/resolve.
            (head, (HEAD_ROWS,)) if self.head else None,
            [
                op
                for x in self.annotations
                for op in [*self.encoding(x, x.annotation), *([(split_list, (x.annotation, x.split_by))] if x.split_by else [])]
            ]
            if self.annotations
            else None,
            # One op, not five: the coercions share the "clean" phase label and one plan,
            # so `compile_subgraph` emits the same single phase transition either way.
            (coerce_columns, ()),
            (clean_numeric, ()),
            # Drop insignificant rows before they ever reach the expensive fullmap resolution below.
            (sig, ()),
            (drop_not_significant, ()) if self.release else None,
            (drop_zero_effect_size, ()) if self.release else None,
            (drop_low_number_of_cases, ()) if self.release and self.statement.predicate == "applied_to_treat" else None,
        ]

    def _node_ops(self: Self, db: Path) -> list[Any]:
        """Collect the per-node encoding/resolve/QC ops shared across subject/object/qualifiers.

        Args:
            db: Path to the fullmap redb used for entity resolution.

        Returns:
            Raw op list: one ``node_prep`` block per node column, ``trim`` to drop
            the spent raw ``column_<n>`` columns, the single shared
            ``resolve_batch`` op, then per-column ``fullmap_audit`` ops when QC is
            enabled.
        """
        # Subject/object/qualifiers share one resolve_batch call instead of one per column.
        # Enum-ranged qualifiers are excluded from resolution: their range is a closed
        # Biolink vocabulary, so sending them through the fullmap would turn the required
        # token `increased` into the CURIE `UMLS:C0205217`, which the slot rejects.
        qualifiers: list[Qualifier] = self.statement.qualifiers or []
        node_columns: list[tuple[NodeEncoding, str]] = [
            (self.statement.subject, "subject"),
            (self.statement.object, "object"),
            *[(x, x.qualifier) for x in qualifiers if x.resolved],
        ]
        literals: list[Qualifier] = [x for x in qualifiers if not x.resolved]
        # A nullable qualifier keeps its edge when the cell is blank or unresolvable (the
        # column stays null and the null-stripper omits the key); subject/object and
        # strict qualifiers drop the row as before.
        specs: list[ResolveSpec] = [
            ResolveSpec(
                col,
                str(x.taxon) if x.taxon else None,
                x.prioritize,
                x.avoid,
                x.exclude_prefixes,
                x.exclude_regex,
                x.nullable if isinstance(x, Qualifier) else False,
            )
            for x, col in node_columns
        ]
        return [
            [self.node_prep(x, col) for x, col in node_columns],
            # Encode only: no pre-resolution snapshot and no NLP normalization, both of
            # which exist to feed entity resolution these columns never undergo.
            [self.encoding(x, x.qualifier) for x in literals],
            # Raw ``column_<n>`` columns are dead weight once the encodings above have
            # copied them into named slots; trim before resolution so the frame that
            # resolve_batch materializes and joins stays narrow.
            (trim, ()),
            (resolve_batch, (specs, db, self.log, self.store.stem, self.config.name, True, "_two")),
            # QC audits only the strict columns: a nullable qualifier's nulls are expected
            # (blank cell / no match), not resolution errors for the audit to delete.
            [
                (
                    fullmap_audit,
                    (
                        (col, self.store.stem, self.config.name, "passed", True)
                        if (
                            self.qc_similarity_threshold == 0.5
                            and self.qc_fuzzy_ratio_threshold == 70.0
                            and self.qc_fuzzy_partial_threshold == 80.0
                            and not self.qc_aliases
                        )
                        else (
                            col,
                            self.store.stem,
                            self.config.name,
                            "passed",
                            True,
                            None,
                            self.qc_similarity_threshold,
                            self.qc_fuzzy_ratio_threshold,
                            self.qc_fuzzy_partial_threshold,
                            self.qc_aliases,
                        )
                    ),
                )
                for x, col in node_columns
                if not (isinstance(x, Qualifier) and x.nullable)
            ]
            if self.qc
            else None,
        ]

    def _provenance_ops(self: Self) -> list[Any]:
        """Collect the edge/provenance/finalize ops that follow entity resolution.

        Returns:
            Raw op list: predicate and edge category, provenance metadata,
            then the format/write finalize ops. (``trim`` now runs earlier, in
            ``_node_ops``, so resolution joins never carry the spent raw
            ``column_<n>`` columns.)
        """
        override = self.provenance.override
        # The edge primary knowledge source is ALWAYS the explicit graph-level infores
        # (rig.source_info.infores_id); there is no implicit derivation from the graph
        # name, so a RIG and its edges can never disagree about the source identity.
        primary_knowledge_source: str | None = self.infores
        upstream_ids = override.upstream_resource_ids if override else upstream_resource_ids(self.provenance.repo)
        upstream_urls: dict[str, list[str]] | None = (
            {key: [str(u) for u in urls] for key, urls in override.upstream_source_record_urls.items()}
            if override and override.upstream_source_record_urls is not None
            else None
        )
        # An explicit `sources` template replaces the derived primary/upstream
        # emission entirely; the model already forbids combining it with
        # `upstream_resource_ids`/`upstream_source_record_urls`.
        explicit_sources: list[dict[str, Any]] | None = (
            [entry.model_dump(exclude_none=True) for entry in override.sources] if override and override.sources is not None else None
        )
        knowledge_level = override.knowledge_level if override else self.provenance.knowledge_level
        agent_type = override.agent_type if override else self.provenance.agent_type
        publication_values = override.publications if override else [publication_curie(self.provenance.repo, self.provenance.publication or "")]
        # The study is the table itself: one publication, one worksheet/table. The
        # publication CURIE is the study's IDENTIFIER; the worksheet or source filename
        # is its human-readable NAME. The two stay disjoint fields and are never composed
        # into one string, so neither ever duplicates the other.
        sheet: str | None = self.source.sheet if self.source.kind == Files.EXCEL else None  # pyright: ignore
        # No publication means no study to name: the fallback id is the config stem (the
        # YAML filename without its extension), not an identifier, so
        # `inline_supporting_study` skips the struct unless it has content to carry.
        identified: bool = bool(publication_values)
        study_id: str = publication_values[0] if publication_values else self.config.stem
        study_name: str | None = sheet if sheet is not None else Path(self.source.local).name
        if study_name == study_id:
            study_name = None
        # `use_enum_values` stores the override's enum keys/values as plain strings.
        category_override: dict[str, str] | None = (
            {obj: f"biolink:{pinned}" for obj, pinned in self.statement.category_override.items()} if self.statement.category_override else None
        )
        return [
            (value, ("predicate", "biolink:" + self.statement.predicate)),
            (edge_category, ("biolink:" + self.statement.predicate, category_override)),
            (value, ("knowledge_level", knowledge_level)),
            (value, ("agent_type", agent_type)),
            # Retrieval provenance lives only in the nested `sources` list (Biolink
            # RetrievalSource); current translator-ingests emits no flat
            # `primary_knowledge_source` scalar, so neither do we. A per-upstream URL
            # mapping (override.upstream_source_record_urls) re-homes the record URLs
            # from the primary entry onto the matching supporting entries; an explicit
            # `sources` template (override.sources) replaces the whole derivation.
            (retrieval_sources, (primary_knowledge_source, upstream_ids, [str(u) for u in self.source.url], upstream_urls, explicit_sources))
            if primary_knowledge_source
            else None,
            (publications, (publication_values,)) if publication_values else None,
            # Prune first so class-rejected values are handed to the study rather than lost.
            (prune_to_class, ()),
            # Format numeric columns BEFORE the study struct absorbs them, so the inlined
            # Study carries the model-typed value (`study_size` as an int, Biolink PR #1770)
            # rather than a raw float.
            (format_numeric, ()),
            (inline_supporting_study, (study_id, study_name, identified)),
            (trim, ()),
            (to_store, (self.store, self.config.name)),
        ]

    def collect(self: Self, db: Path) -> list[tuple[Callable, tuple[Any]]] | Path:
        """Build the ordered operation list that drives section transformation.

        Args:
            db: Path to the fullmap redb used for entity resolution.

        Returns:
            Either the existing store path (quick exit when the subgraph
            parquet is already present), or a cleaned list of
            ``(callable, args)`` tuples consumed by ``compile_subgraph``.
        """
        if self.store.is_file():
            # Quick exit if subgraph already exists.
            return self.store

        # Returns a list of: (function, (arguments)).
        tcode: list[Any] = [*self._source_ops(), *self._node_ops(db), *self._provenance_ops()]
        return self.clean(tcode)


PHASE_OF: dict[Callable, str] = {
    # Maps each Tcode op callable to a short lowercase phase label for progress UX.
    csv: "load",
    excel: "load",
    idx: "load",
    crop: "filter",
    pick: "filter",
    reindex: "filter",
    head: "filter",
    coerce_columns: "clean",
    clean_numeric: "clean",
    level_one: "resolve",
    level_two: "resolve",
    resolve: "resolve",
    resolve_batch: "resolve",
    fullmap_audit: "qc",
    column: "encode",
    edge_category: "edge",
    publications: "provenance",
    retrieval_sources: "provenance",
    inline_supporting_study: "provenance",
    prune_to_class: "finalize",
    split_list: "encode",
    sig: "significance",
    drop_not_significant: "significance",
    drop_zero_effect_size: "significance",
    drop_low_number_of_cases: "significance",
    format_numeric: "finalize",
    to_store: "write",
}

# Ops that accept an ``on_phase`` callback so ``compile_subgraph`` can forward fine-grained sub-phases.
PHASE_AWARE: frozenset[Callable] = frozenset({resolve_batch, fullmap_audit})

UNKNOWN_PHASE: str = "transform"

CACHE_PHASE: str = "cache"
"""Phase tag fired once for a section resumed from a run-cache snapshot.

It stands in for the phase tags of the skipped prefix ops (load → filter → clean → resolve …), so
progress shows a resumed section reading a sibling's snapshot instead of claiming to redo work it
never did. It is a label and nothing else: ``PHASE_OF``/``PHASE_AWARE`` are keyed on op callables,
so no lookup ever needs ``CACHE_PHASE``, and ``PipelineProgress.section_loop``'s ``sub_step``
renders any string it is handed.
"""

_VALUE_PROVENANCE_COLS: frozenset[str] = frozenset({"knowledge_level", "agent_type", "sheet_name"})


def _phase_of(fn: Callable, args: tuple[Any, ...]) -> str:
    """Resolve the phase label for an op, handling ``value()`` specially since it spans phases.

    Args:
        fn: Op callable.
        args: Op argument tuple.

    Returns:
        Short lowercase phase label (e.g. ``"load"``, ``"resolve"``,
        ``"edge"``); falls back to ``UNKNOWN_PHASE`` when unrecognized.
    """
    if fn is value:
        col: str = str(args[0]) if args else ""
        if col == "predicate":
            return "edge"
        if col in _VALUE_PROVENANCE_COLS:
            return "provenance"
        return UNKNOWN_PHASE
    return PHASE_OF.get(fn, UNKNOWN_PHASE)


def compile_subgraph(
    tcode: list[tuple[Callable, tuple[Any]]],
    *,
    on_phase: Callable[[str], None] | None = None,
    resume: tuple[int, pl.LazyFrame] | None = None,
    snapshot: Callable[[int, pl.LazyFrame], pl.LazyFrame | None] | None = None,
) -> Path:
    """Execute a Tcode operation list to build a subgraph parquet.

    Args:
        tcode: Cleaned list of ``(callable, args)`` tuples from ``Tcode.collect``.
        on_phase: Optional callback fired when the phase label changes, and
            forwarded into phase-aware ops (``resolve_batch``/``fullmap_audit``)
            so they can emit fine-grained sub-phases; used to drive progress UX.
        resume: ``(prefix_len, frame)`` seeding the accumulator with ``frame`` — a snapshot of
            what ``tcode[:prefix_len]`` produced — and skipping those first ``prefix_len`` ops,
            with ``on_phase(CACHE_PHASE)`` fired once in place of their phase tags. ``None`` (the
            default) runs every op from the first, i.e. the historical behavior.
        snapshot: Called as ``snapshot(position, frame)`` right after the op at each checkpoint
            position (``runcache.checkpoints(tcode)``) has run, with the frame ``tcode[:position]``
            produced. A callback may return a materialized replacement LazyFrame; when it does,
            that replacement becomes the accumulator for the remaining producer tail, so the
            stored prefix is not lazily recomputed by the producer's final write. ``None`` (the
            default) checkpoints nothing and never consults the cost guards.

    Returns:
        Path to the written subgraph parquet.

    Notes:
        ``resume``'s ``prefix_len`` must be ``< len(tcode)``: a section always replays its own
        final write op, which ``runcache.checkpoints`` guarantees by never selecting ``len(ops)``
        for a write-final list, so a resumed tail is never empty and the return is always the
        written ``Path``.

        The two new parameters are keyword-only and default to ``None`` so every existing call
        site — and ``agent.py``'s ``_reduce_ops`` mirror, which does not use the run cache —
        keeps its exact behavior. ``runcache`` is imported lazily inside the body because it
        imports this module at module level (``OP_COST`` is keyed on the real op callables); a
        top-level import here would be circular.
    """
    if not tcode:
        from tablassert.runcache import RunCacheError

        raise RunCacheError(
            "invalid run-cache resume: an empty subgraph instruction list has no final write operation", code="runcache-invalid-resume"
        )
    if resume is not None:
        prefix_len: int = resume[0]
        if not 0 <= prefix_len < len(tcode):
            from tablassert.runcache import RunCacheError

            raise RunCacheError(
                f"invalid run-cache resume: prefix_len={prefix_len} must satisfy 0 <= prefix_len < {len(tcode)} "
                "so the resumed tail contains the final write operation",
                code="runcache-invalid-resume",
            )

    checkpoint_positions: frozenset[int] = frozenset()
    if snapshot is not None:
        # Lazy and conditional: runcache imports lib at module level (OP_COST is keyed on the real
        # op callables), so a top-level import here would be a cycle — and the default path, which
        # checkpoints nothing, must not pay for the import at all.
        from tablassert.runcache import checkpoints

        checkpoint_positions = frozenset(checkpoints(tcode))
    last_phase: str | None = None
    acc: pl.LazyFrame | Path | None = None
    skip: int = 0
    if resume is not None:
        skip, acc = resume
        if on_phase is not None:
            on_phase(CACHE_PHASE)
    for position, op in enumerate(tcode):
        if position < skip:
            continue
        fn: Callable = op[0]
        args: tuple[Any, ...] = op[1]
        if on_phase is not None:
            phase: str = _phase_of(fn, args)
            if phase != last_phase:
                last_phase = phase
                on_phase(phase)
        if fn in PHASE_AWARE and on_phase is not None:
            acc = fn(acc, *args, on_phase=on_phase) if acc is not None else fn(*args, on_phase=on_phase)  # pyright: ignore
        else:
            acc = fn(acc, *args) if acc is not None else fn(*args)  # pyright: ignore
        if snapshot is not None and position + 1 in checkpoint_positions:
            # A checkpoint never lands on a write op (runcache.WRITE_OPS), so acc is the frame
            # tcode[:position + 1] produced, never the Path a final to_store returned. A producer
            # may return a scan of its stored snapshot here; replacing the accumulator at this
            # seam makes the final write consume that materialization instead of recomputing the
            # original lazy prefix.
            replacement: pl.LazyFrame | None = snapshot(position + 1, acc)  # pyright: ignore[reportArgumentType]
            if replacement is not None:
                acc = replacement
    return acc  # pyright: ignore


def normalize(edges: pl.LazyFrame, col: str, names: list[str] | None = None, infores_id: str | None = None) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Normalize disparate node columns into a unified format and remove them from edges.

    Emits Biolink ``NamedThing`` slots only. The fullmap's ``<col>_source`` (a file
    name such as ``gene.txt``) and ``<col>_source_version`` are build provenance, not
    node properties -- ``source``/``source_version`` are ``extra_forbidden`` on every
    Biolink node class, and the version belongs at graph level (the RIG), matching
    ``translator-ingests/util/metadata.py``. The taxon is emitted as ``in_taxon`` plus
    ``in_taxon_label``, the slots ``translator-ingests`` uses.

    Args:
        edges: Source edges LazyFrame containing ``<col>``, ``<col>_name``,
            ``<col>_category``, ``<col>_taxon``, ``<col>_source``, and
            ``<col>_source_version`` columns.
        col: Base node column name (e.g. ``"subject"``).
        names: Output column names for the produced nodes frame.
        infores_id: Graph-level infores CURIE recorded as ``provided_by``.

    Returns:
        Tuple of ``(partial_nodes, modified_edges)`` as LazyFrames.
    """
    if names is None:
        names = ["id", "name", "category", "in_taxon", "in_taxon_label"]
    cols: list[str] = [col, f"{col}_name", f"{col}_category", f"{col}_taxon", f"{col}_taxon_label"]
    available: list[str] = edges.collect_schema().names()
    # `<col>_taxon_label` is not produced by every fullmap revision.
    pairs: list[tuple[str, str]] = [(c, n) for c, n in zip(cols, names, strict=True) if c in available]
    nodes: pl.LazyFrame = edges.select([c for c, _ in pairs]).unique().rename(dict(pairs))
    # Ensures category has biolink: prefix.
    nodes = nodes.with_columns(
        pl.when(pl.col("category").str.starts_with("biolink:"))
        .then(pl.col("category"))
        .otherwise(pl.lit("biolink:") + pl.col("category"))
        .alias("category")
    )
    # Entity resolution can land on a class that cannot be emitted as a KGX node --
    # `Publication` requires `publication_type`, `ClinicalAttribute` requires
    # `has_attribute_type`, and mixins like `GenomicEntity` reject their own name in the
    # `category` literal. Demote those to the nearest emittable ancestor.
    # `replace` (not `replace_strict`) so nulls and unrecognized spellings pass through
    # untouched -- only categories that genuinely need demoting are rewritten.
    emittable: dict[str, str] = {
        curie: resolved for c in Categories for curie in (f"biolink:{c.value}",) if (resolved := resolve_node_category(curie)) != curie
    }
    nodes = nodes.with_columns(pl.col("category").replace(emittable).alias("category"))
    # Exports category within a list (null categories stay null for strip_nulls).
    nodes = nodes.with_columns(pl.when(pl.col("category").is_not_null()).then(pl.concat_list(pl.col("category"))).alias("category"))
    if "in_taxon" in nodes.collect_schema().names():
        # `in_taxon` is multivalued on Biolink `thing with taxon`.
        nodes = nodes.with_columns(pl.when(pl.col("in_taxon").is_not_null()).then(pl.concat_list(pl.col("in_taxon"))).alias("in_taxon"))
    if infores_id:
        nodes = nodes.with_columns(pl.concat_list(pl.lit(infores_id)).alias("provided_by"))
    # Drop every derived node column from the edges, including the ones not emitted.
    drop: list[str] = [c for c in (*cols[1:], f"{col}_source", f"{col}_source_version") if c in available]
    edges_out: pl.LazyFrame = edges.drop(drop)
    return nodes, edges_out


def publication_curie(repo: str, publication: str) -> str:
    """Build the publication CURIE.

    Uses the ``PMCID:`` namespace for PubMed Central and ``<REPO>:`` for
    other repositories.

    Args:
        repo: Repository identifier (a ``Repositories`` enum value).
        publication: Publication identifier (e.g. PMID or PMCID).

    Returns:
        CURIE string of the form ``"<prefix>:<publication>"``.
    """
    if repo == Repositories.PUBMED_CENTRAL:
        return "PMCID:" + publication
    return repo + ":" + publication


def upstream_resource_ids(repo: Repositories) -> list[str]:
    """Map a publication repository to Translator infores upstream source IDs.

    Args:
        repo: Repository enum value.

    Returns:
        Single-element list containing the matching infores identifier.
    """
    if repo == Repositories.PUBMED_CENTRAL:
        return [InformationResources.PUBMED_CENTRAL.value]
    return [InformationResources.PUBMED.value]


def dedup_stream(p_in: Path, is_edges: bool, domain: str = "TABLASSERT", uuid_fields: list[str] | None = None, on_collision: str = "error") -> None:
    """Remove null values from and deduplicate an NDJSON stream.

    Args:
        p_in: Path to the input ``.ndjson.tmp`` file.
        is_edges: When True, also add UUIDs to edges via the Rust deduper.
        domain: UUID namespace for derived edge ids (``Graph.uuid_namespace``).
            Ignored for nodes, whose ids are CURIEs rather than derived hashes.
        uuid_fields: Optional edge fields that constitute edge identity
            (``Graph.uuid_fields``). ``None`` hashes the whole record.
        on_collision: ``error`` aborts on two different edges deriving one id;
            ``merge`` folds them into one edge (``Graph.uuid_on_collision``). During a merge,
            distinct non-empty scalar ``original_*`` fields are sorted and rendered as
            ``A``, ``A|B``, or ``A|B|C``.

    Notes:
        Also adds UUIDs to edges. Edges deduplicate on their derived id, so the
        output can never carry the same id twice: an exact repeat is collapsed,
        while two genuinely different edges deriving one id abort the build with
        ``uuid-fields-not-a-key`` rather than shipping a duplicate -- unless
        ``on_collision="merge"``, which unions their list fields (sorted, so the
        output is merge-order independent), keeps the first value of conflicting
        scalars, and buffers one full record per unique id until end-of-stream.
        One scalar is exempt from first-wins: when a merged edge carries the
        build-internal ``supporting_case_ids`` list and either side carried
        ``number_of_cases``, the count is recomputed as the union length. The
        carrier is stripped from every edge record before write, in both modes,
        so it never ships in the final NDJSON.

    Returns:
        ``None``; writes the deduplicated stream alongside ``p_in`` with no
        ``.tmp`` suffix, then deletes ``p_in``.
    """
    p_out: Path = p_in.with_suffix("")

    if p_out.is_file():
        p_out.unlink()

    merged, scalar_conflicts = rs.dedup_ndjson(p_in, p_out, is_edges, domain, uuid_fields, on_collision)
    if merged:
        logger.warning(
            "uuid_on_collision=merge folded {merged} divergent edge records into their first-seen edges; {conflicts} conflicting scalar fields kept the first value",
            merged=merged,
            conflicts=scalar_conflicts,
        )

    p_in.unlink()


def _resolve_edge_id_placeholders(edges_path: Path) -> None:
    """Resolve ``{edge_id}`` placeholders in a final edges NDJSON file.

    The edge ``id`` is a deterministic content hash assigned by the Rust deduper
    after subgraphs are written, so explicit ``override.sources`` record URLs
    cannot embed it during the polars build: the literal placeholder is what
    gets hashed, and this post-dedup sweep substitutes each record's own id into
    every string inside every ``sources[].source_record_urls`` list. The pass is
    skipped entirely when no line contains the marker (cheap substring precheck,
    no full parse), leaving the file byte-identical.

    Args:
        edges_path: Path to the deduplicated ``*.edges.ndjson`` file.

    Returns:
        ``None``; rewrites ``edges_path`` in place via a temp file when any
        placeholder was resolved.
    """
    tmp_path: Path = edges_path.with_name(edges_path.name + ".placeholder.tmp")
    resolved: bool = False
    with edges_path.open("r", encoding="utf-8") as src, tmp_path.open("w", encoding="utf-8") as dst:
        for line in src:
            if EDGE_ID_PLACEHOLDER not in line:
                dst.write(line)
                continue
            record: dict[str, Any] = json.loads(line)
            for source in record.get("sources") or []:
                urls: list[str] | None = source.get("source_record_urls")
                if urls:
                    source["source_record_urls"] = [url.replace(EDGE_ID_PLACEHOLDER, record["id"]) for url in urls]
            dst.write(json.dumps(record, ensure_ascii=False) + "\n")
            resolved = True
    if resolved:
        tmp_path.replace(edges_path)
    else:
        tmp_path.unlink()


def fold_unknown_to_supporting_text(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Fold any non-Biolink edge column into ``supporting_text`` as ``col: value`` strings.

    Args:
        lf: Edges LazyFrame possibly carrying non-allowed columns.

    Returns:
        LazyFrame with all non-allowed columns folded into ``supporting_text``
        and dropped.

    Notes:
        Stays fully lazy; null/blank values produce no entry; sorted for
        deterministic output. Existing ``list[str]`` ``supporting_text`` has
        derived entries appended (never clobbered); scalar
        ``supporting_text`` (e.g. a ``method: value`` annotation) is coerced
        to ``list[str]`` first.
    """
    schema: pl.Schema = lf.collect_schema()
    schema_names: list[str] = schema.names()
    disabled: list[str] = sorted(c for c in schema_names if c in DISABLED_EDGE_FIELDS)
    unknown: list[str] = sorted(c for c in schema_names if c not in ALLOWED_EDGE_FIELDS and c not in DISABLED_EDGE_FIELDS)
    if not unknown:
        return lf.drop(disabled) if disabled else lf

    parts: list[pl.Expr] = []
    for col in unknown:
        # List-typed columns cannot be cast to String directly; join their elements so
        # a folded `sources`-style column degrades readably instead of raising.
        s: pl.Expr = (
            pl.col(col).list.eval(pl.element().cast(pl.String)).list.join(", ") if isinstance(schema[col], pl.List) else pl.col(col).cast(pl.String)
        ).str.strip_chars()
        blank: pl.Expr = s.is_null() | (s.str.len_chars() == 0)
        entry: pl.Expr = pl.when(blank).then(pl.lit(None, dtype=pl.String)).otherwise(pl.concat_str([pl.lit(f"{col}: "), s]))
        parts.append(entry)

    derived: pl.Expr = pl.concat_list(parts).list.drop_nulls()

    if "supporting_text" in schema_names:
        # Coerce scalar supporting_text to list[str] first, then append derived entries.
        existing: pl.Expr = pl.col("supporting_text") if isinstance(schema["supporting_text"], pl.List) else pl.concat_list(pl.col("supporting_text"))
        combined: pl.Expr = existing.list.concat(derived).list.drop_nulls()
    else:
        combined = derived

    return lf.with_columns(combined.alias("supporting_text")).drop([*unknown, *disabled])


def _collect_subframes(
    subgraphs: list[Path], on_phase: Callable[[str], None] | None = None, on_subgraph: Callable[[], None] | None = None, infores_id: str | None = None
) -> tuple[list[pl.LazyFrame], list[pl.LazyFrame]]:
    """Scan and normalize subgraph parquets into node/edge subframes.

    Covers the ``scan`` and ``normalize`` phases of ``compile_graph``; the
    phase boundaries inside the loop are the hook points for the US-009
    ``on_phase`` progress callback.

    Args:
        subgraphs: Section parquet paths to merge.
        on_phase: Optional callback fired with ``"scan"`` then ``"normalize"``
            for each subgraph, used to drive progress UX.
        on_subgraph: Optional callback fired once after each subgraph is
            processed, used to tick the progress bar.
        infores_id: Graph-level infores CURIE recorded as node ``provided_by``.

    Returns:
        Tuple of ``(subnodes, subedges)``: per-section node and edge
        LazyFrames. RIG summaries are computed later from the FINAL emitted
        KGX files, never from these intermediate frames.
    """
    subnodes: list[pl.LazyFrame] = []
    subedges: list[pl.LazyFrame] = []
    for s in subgraphs:
        # Phase: scan.
        if on_phase is not None:
            on_phase("scan")
        lf: pl.LazyFrame = pl.scan_parquet(s)

        # Phase: normalize. Only subject and object become nodes; qualifier columns stay as edge attributes.
        if on_phase is not None:
            on_phase("normalize")
        originals: list[str] = [c.removesuffix("_pre_resolution") for c in lf.collect_schema().names() if c.endswith("_pre_resolution")]
        node_cols: list[str] = [c for c in originals if c in ("subject", "object")]
        for col in node_cols:
            partial, lf = normalize(lf, col, infores_id=infores_id)
            subnodes.append(partial)
        # Drop internal pre-resolution snapshot columns from final edges.
        lf = lf.drop([c for c in lf.collect_schema().names() if c.endswith("_pre_resolution")])
        lf = fold_unknown_to_supporting_text(lf)
        subedges.append(lf)
        if on_subgraph is not None:
            on_subgraph()
    return subnodes, subedges


def _write_ndjson(
    subnodes: list[pl.LazyFrame],
    subedges: list[pl.LazyFrame],
    nodes_tmp: Path,
    edges_tmp: Path,
    name: str,
    version: str,
    rig: RIGConfig,
    section_sources: list[dict[str, object]] | None,
    on_phase: Callable[[str], None] | None = None,
    domain: str = "TABLASSERT",
    uuid_fields: list[str] | None = None,
    on_collision: str = "error",
) -> None:
    """Write, dedup, and RIG the KGX NDJSON outputs.

    Covers the ``write-nodes``, ``write-edges``, ``dedup`` and ``rig`` phases of
    ``compile_graph``; each commented phase boundary below is a hook point for
    the US-009 ``on_phase`` progress callback.

    Dedup assigns each edge ``id`` as the deterministic content hash of the
    PRE-resolution record -- the literal ``{edge_id}`` placeholder in explicit
    ``override.sources`` record URLs is what gets hashed -- and the placeholder
    sweep that follows substitutes the assigned id into the final edges file
    only, keeping ids deterministic.

    Args:
        subnodes: Per-section node LazyFrames from ``_collect_subframes``.
        subedges: Per-section edge LazyFrames from ``_collect_subframes``.
        nodes_tmp: Working ``.nodes.ndjson.tmp`` output path.
        edges_tmp: Working ``.edges.ndjson.tmp`` output path.
        name: Graph name (output stems and RIG).
        version: Graph version string.
        rig: Validated ``rig:`` graph-config section driving the RIG.
        section_sources: Per-section source descriptors for the RIG
            relevant-file cross-check.
        on_phase: Optional callback fired with ``"write-nodes"``,
            ``"write-edges"``, ``"dedup"`` and ``"rig"`` at each phase
            boundary, used to drive progress UX.
        domain: UUID namespace for derived edge ids (``Graph.uuid_namespace``).
        uuid_fields: Optional edge identity fields (``Graph.uuid_fields``).
        on_collision: Collision policy for divergent same-id edges
            (``Graph.uuid_on_collision``).
    """
    # Phase: write-nodes. Collection point: appending to output files.
    if on_phase is not None:
        on_phase("write-nodes")
    with nodes_tmp.open("a", encoding="utf-8") as f:
        for subnode in subnodes:
            eagernode: pl.DataFrame = subnode.collect().unique()
            eagernode.write_ndjson(f)

    # Phase: write-edges.
    if on_phase is not None:
        on_phase("write-edges")
    with edges_tmp.open("a", encoding="utf-8") as f:
        for subedge in subedges:
            eageredge: pl.DataFrame = subedge.collect().unique()
            eageredge.write_ndjson(f)

    # Phase: dedup.
    if on_phase is not None:
        on_phase("dedup")
    dedup_stream(edges_tmp, is_edges=True, domain=domain, uuid_fields=uuid_fields, on_collision=on_collision)
    # The deduper hashes the record WITH the literal `{edge_id}` placeholder still
    # in place, so edge ids stay deterministic regardless of this resolution pass.
    _resolve_edge_id_placeholders(edges_tmp.with_suffix(""))
    dedup_stream(nodes_tmp, is_edges=False)

    # Phase: rig. Summaries come from the FINAL deduplicated KGX files, and the
    # assembled document is audited before anything is written.
    if on_phase is not None:
        on_phase("rig")
    compile_rig(name, version, rig, section_sources, nodes_tmp.with_suffix(""), edges_tmp.with_suffix(""))


def compile_graph(
    subgraphs: list[Path],
    name: str,
    version: str,
    rig: RIGConfig | dict[str, Any],
    section_sources: list[dict[str, object]] | None = None,
    on_phase: Callable[[str], None] | None = None,
    on_subgraph: Callable[[], None] | None = None,
    uuid_fields: list[str] | None = None,
    uuid_domain: str | None = None,
    uuid_on_collision: str = "error",
) -> None:
    """Aggregate subgraph parquets for NDJSON KGX export using a lazy scan.

    Args:
        subgraphs: List of section parquet paths to merge.
        name: Graph name (used for output file stems and the RIG).
        version: Graph version string.
        rig: The ``rig:`` graph-config section driving the generated
            ``.RIG.yaml`` (source metadata, artifact bases, provenance).
            A plain dict is validated as :class:`RIGConfig` first, so
            programmatic callers get the same config-time errors as YAML.
        section_sources: Per-section source descriptors (``local``, ``urls``)
            used to cross-check configured RIG relevant-file entries.
        on_phase: Optional callback fired with the current phase label
            (``scan`` / ``normalize`` per subgraph, then ``write-nodes`` /
            ``write-edges`` / ``dedup`` / ``rig``), used to drive progress UX.
        on_subgraph: Optional callback fired once per processed subgraph,
            used to tick the progress bar.
        uuid_fields: Optional edge fields that constitute edge identity
            (``Graph.uuid_fields``). ``None`` hashes the whole edge record, so any
            change to any field re-mints the id.
        uuid_domain: Optional explicit UUID namespace. Defaults to the graph's
            infores when ``uuid_fields`` is set -- narrowing the hash inputs
            removes the accidental cross-graph uniqueness that full-record hashing
            provided -- and to ``TABLASSERT`` otherwise.
        uuid_on_collision: ``error`` aborts when two different edges derive one id;
            ``merge`` folds them into one edge instead (``Graph.uuid_on_collision``).

    Returns:
        ``None``; writes ``<name>_<version>.nodes.ndjson``,
        ``<name>_<version>.edges.ndjson``, and ``<name>_<version>.RIG.yaml``
        into ``rig.artifact_base_path`` (created when missing).

    Raises:
        TablassertError: With code ``rig-validation-failed`` when the
            generated RIG fails its built-in audit (nothing is then written).
        RuntimeError: Tagged ``uuid-fields-not-a-key`` when two genuinely
            different edges derive one id under the declared ``uuid_fields``
            and ``uuid_on_collision`` is ``error``.
    """
    rig_cfg: RIGConfig = rig if isinstance(rig, RIGConfig) else RIGConfig.model_validate(rig)
    out_dir: Path = Path(rig_cfg.artifact_base_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    p: Path = out_dir / f"{name}_{version}.tmp"

    e: Path = p.with_suffix(".edges.ndjson.tmp")  # For labeling.
    if e.exists():
        e.unlink()

    n: Path = p.with_suffix(".nodes.ndjson.tmp")
    if n.exists():
        n.unlink()

    domain: str = uuid_domain or (rig_cfg.source_info.infores_id if uuid_fields else "TABLASSERT")

    subnodes: list[pl.LazyFrame]
    subedges: list[pl.LazyFrame]
    subnodes, subedges = _collect_subframes(subgraphs, on_phase, on_subgraph, rig_cfg.source_info.infores_id)
    _write_ndjson(subnodes, subedges, n, e, name, version, rig_cfg, section_sources, on_phase, domain, uuid_fields, uuid_on_collision)


def resolve_many(
    col: str,
    entities: Iterable[str],
    fullmap: Path,
    taxon: str | None = None,
    prioritize: list[Categories] | None = None,
    avoid: list[Categories] | None = None,
    qc: bool = False,
    column_context: bool = True,
) -> list[dict[str, Any]]:
    """Resolve a batch of raw entity strings to Biolink-normalized records.

    Args:
        col: Source column name to normalize.
        entities: Raw entity strings to resolve.
        fullmap: Path to ``fullmap.redb`` (or its parent directory).
        taxon: Optional NCBITaxon constraint applied to taxon-bearing matches while retaining rows with no taxon metadata.
        prioritize: Optional Biolink categories preferred on ties.
        avoid: Optional Biolink categories deprioritized on ties.
        qc: When True, attach fullmap audit metadata to the result.
        column_context: Forwarded to ``resolve``.

    Returns:
        List of dicts mirroring a one-row-per-entity resolved frame, with
        internal pre-resolution snapshot columns dropped to match edge output.
    """
    series: pl.Series = pl.Series(col, entities)
    lf: pl.LazyFrame = series.to_frame().lazy()

    lf = column(lf, f"original_{col}", col)
    lf = column(lf, f"{col}_pre_resolution", col)
    lf = level_one(lf, col)
    lf = level_two(lf, col)

    lf = resolve(lf, col, fullmap_db_path(fullmap), taxon=taxon, prioritize=prioritize, avoid=avoid, column_context=column_context)
    if qc:
        lf = fullmap_audit(lf, col, "", "", log=qc)

    df: pl.DataFrame = lf.collect()
    # Drop internal pre-resolution snapshot columns to mirror edge output.
    df = df.drop([c for c in df.columns if c.endswith("_pre_resolution")])
    return df.to_dicts()
