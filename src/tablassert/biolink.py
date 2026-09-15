"""Biolink Model vocabulary derived from the ``biolink-model`` package.

This module sources Tablassert's Biolink categories, predicates, qualifiers,
knowledge levels, agent types, edge (association) categories, and allowed edge
fields directly from the official ``biolink-model`` PyPI package -- the same
LinkML-generated Pydantic serialization that ``NCATSTranslator/translator-ingests``
imports (``from biolink_model.datamodel.pydanticmodel_v2 import NamedThing,
Association, ...``) and that Koza validates against.

Deriving these values from the model -- instead of hand-maintaining copies that
can drift -- guarantees Tablassert's KGX output uses only terms that are valid
for the pinned Biolink version. Downstream translator-ingests can therefore
consume Tablassert output without running Koza or re-validating against the
Biolink Model.

Source mapping
--------------
- ``Categories`` / ``EdgeCategories`` / ``KnowledgeLevels`` / ``AgentTypes``:
  introspected from ``biolink_model.datamodel.pydanticmodel_v2`` (entity and
  association Pydantic classes plus the ``KnowledgeLevelEnum`` /
  ``AgentTypeEnum`` value sets).
- ``Predicates`` / ``Qualifiers``: walked from the bundled
  ``biolink_model.yaml`` slot hierarchy via ``linkml-runtime`` (a pinned
  dependency of ``biolink-model``), because predicates and qualifiers are LinkML
  *slots* rather than Pydantic classes.
- ``ALLOWED_EDGE_FIELDS``: the Biolink ``Association`` model fields (walked over
  the MRO) unioned with the qualifier slot names and a curated set of KGX /
  Tablassert edge columns that are not Biolink Association fields.
- ``EffectTypes``: the permissible ``effect_type`` values, derived from the
  installed ``biolink-model``'s ``EffectTypeEnum`` (shipped by PR #1774).

The enums are built dynamically at runtime from the model. For static type
checking, ``TYPE_CHECKING`` stub classes (declaring only the members referenced
by name elsewhere in the codebase) stand in for the dynamic classes; at runtime
the ``else`` branch builds the full enums. ``tablassert.cli`` imports the modules
that depend on this one (``models`` / ``lib`` / ``fullmap``) inside its pipeline
functions, so the one-time ``linkml-runtime`` parse cost is paid only when a build
or validate actually runs -- never on ``tablassert --help``.
"""

from __future__ import annotations

import inspect
import json
import re
from collections import Counter
from enum import Enum
from functools import cache
from importlib.resources import files
from typing import TYPE_CHECKING, Any, Literal, cast, get_args, get_origin

import biolink_model.datamodel.pydanticmodel_v2 as _bm

if TYPE_CHECKING:
    from pathlib import Path

    from linkml_runtime.utils.schemaview import SchemaView

__all__ = [
    "ALLOWED_EDGE_FIELDS",
    "BIOLINK_VERSION",
    "CATEGORY_OVERRIDES",
    "CLASS_FIELD_OVERRIDES",
    "DISABLED_EDGE_FIELDS",
    "EFFECT_TYPE_VALUES",
    "ENUM_RANGED_QUALIFIERS",
    "KNOWN_PENDING_EDGE_FIELDS",
    "STUDY_METADATA_FIELDS",
    "UNSATISFIABLE_EDGE_FIELDS",
    "AgentTypes",
    "Categories",
    "EdgeCategories",
    "EffectTypes",
    "KnowledgeLevels",
    "Predicates",
    "Qualifiers",
    "association_class",
    "class_fields",
    "is_multivalued",
    "is_pending_problem",
    "legal_predicates",
    "node_class",
    "numeric_slot_kind",
    "resolve_association_class",
    "resolve_node_category",
    "resolve_node_class",
    "validate_kgx",
    "validate_record",
]


def _screaming_snake(camel: str) -> str:
    """Convert a Biolink CamelCase name to a SCREAMING_SNAKE enum member name.

    Handles acronym boundaries so ``PhenotypicFeature`` -> ``PHENOTYPIC_FEATURE``
    and ``ChemicalGeneInteractionAssociation`` ->
    ``CHEMICAL_GENE_INTERACTION_ASSOCIATION``.

    Args:
        camel: CamelCase class name (e.g. ``"GeneToDiseaseAssociation"``).

    Returns:
        Upper-snake-cased member name (e.g. ``"GENE_TO_DISEASE_ASSOCIATION"``).
    """
    split: str = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", camel)
    split = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", split)
    return split.upper()


def _build_str_enum(name: str, values: list[str]) -> type[Enum]:
    """Build a ``str``-mixed :class:`~enum.Enum` from a list of string values.

    Member names are derived via :func:`_screaming_snake`; values are the verbatim
    Biolink terms. The resulting class is a subclass of both ``str`` and ``Enum``
    (equivalent to ``class Name(str, Enum)``), so it works as a Pydantic field type
    and compares equal to its string value.

    Args:
        name: Class name for the generated enum.
        values: Biolink term values (deduplicated, order-insensitive).

    Returns:
        A new ``str``/``Enum`` subclass.

    Raises:
        ValueError: If two distinct values map to the same member name.
    """
    members: dict[str, str] = {}
    for value in sorted(set(values)):
        member: str = _screaming_snake(value)
        if member in members:
            raise ValueError(f"{name}: duplicate enum member {member!r} for values {members[member]!r} and {value!r}")
        members[member] = value
    return cast("type[Enum]", Enum(name, members, type=str, module=__name__))


def _category_name(cls: Any) -> str:
    """Return the bare Biolink category name for a Pydantic model class.

    Reads the class's ``category`` field default (``["biolink:X", ...]``) and strips
    the ``biolink:`` prefix, falling back to the class name.

    Args:
        cls: A Pydantic class from ``pydanticmodel_v2``.

    Returns:
        The CamelCase category name (e.g. ``"Gene"``).
    """
    field: Any = cls.model_fields.get("category")
    if field is not None and isinstance(field.default, list):
        for value in field.default:
            if isinstance(value, str) and value.startswith("biolink:"):
                return value[len("biolink:") :]
    return str(cls.__name__)


CATEGORY_OVERRIDES: frozenset[str] = frozenset({"GenomicEntity"})
"""Biolink category names the ``Categories`` enum must carry ahead of the pinned model.

The enum is built from the ``pydanticmodel_v2`` classes that subclass ``Entity``, but
Biolink declares some node categories as MIXINS -- and a mixin is generated as a plain
``ConfiguredBaseModel`` that is NOT an ``Entity`` subclass (``GenomicEntity``'s MRO is
``GenomicEntity -> ConfiguredBaseModel -> BaseModel -> object``). The scan therefore
cannot see them, while the fullmap -- whose ``CATEGORY_NAME`` vocabulary comes from
BABEL, not from the Pydantic class tree -- assigns them to real terms (UMLS
"gene mutation" -> ``UMLS:C0678941``, the ``* wt Allele`` CUIs).

An unnameable category is a hole in every category-keyed config surface:
``NodeEncoding.avoid`` / ``prioritize`` and ``Statement.category_override`` keys are
validated against the enum, so an ``avoid``-list allow-list can never exclude a
category the resolver actually emits, and the row falls through to whatever the
(subject role, object role) pair derives -- for a mixin object with no
``CATEGORY_PARENT`` rollup that is bare ``biolink:Association``, whose
``prune_to_class`` then nulls every class-specific slot into the inlined
``has_supporting_studies`` junk drawer. (DAKP shipped 35 such edges before this
override existed.) ``fullmap.filter_and_rank`` additionally drops unnameable
categories when ``avoid`` is set, so a future vocabulary drift degrades to lost rows
rather than mis-classed edges.

Names here are unioned into :func:`_entity_category_names`, so each is a first-class
``Categories`` member: valid in ``avoid``/``prioritize``, valid as a
``category_override`` key, and included in generated complements. Node emission is
unaffected -- ``resolve_node_class`` walks the MRO independently of this enum.

A tripwire test asserts every override is still invisible to the ``Entity`` scan: the
moment a biolink-model release promotes a mixin to a real ``Entity`` subclass (or the
scan otherwise picks it up), the suite fails and the stale name is removed from
``CATEGORY_OVERRIDES`` -- the same philosophy as ``CLASS_FIELD_OVERRIDES``.
"""


def _entity_category_names() -> list[str]:
    """Collect all Biolink entity category names from the Pydantic model.

    A category is any ``pydanticmodel_v2`` class that subclasses ``Entity`` but is
    not an ``Association`` subclass (associations are edge categories, handled
    separately), plus the curated :data:`CATEGORY_OVERRIDES` mixin names the
    ``Entity`` scan cannot see. This mirrors the Biolink notion of a node category
    while also including the upper-level ``Entity`` / ``NamedThing`` classes.

    Returns:
        Sorted unique category names.
    """
    names: set[str] = set(CATEGORY_OVERRIDES)
    for cls in vars(_bm).values():
        if inspect.isclass(cls) and cls.__module__ == _bm.__name__ and issubclass(cls, _bm.Entity) and not issubclass(cls, _bm.Association):
            names.add(_category_name(cls))
    return sorted(names)


def _association_names() -> list[str]:
    """Collect all Biolink association (edge category) names from the Pydantic model.

    Returns:
        Sorted unique association class names, including ``Association`` itself.
    """
    names: set[str] = set()
    for cls in vars(_bm).values():
        if inspect.isclass(cls) and cls.__module__ == _bm.__name__ and issubclass(cls, _bm.Association):
            names.add(_category_name(cls))
    return sorted(names)


@cache
def _schema() -> SchemaView:
    """Load the bundled Biolink LinkML schema (cached, parsed once).

    Returns:
        A ``linkml-runtime`` ``SchemaView`` over ``biolink_model.yaml``.
    """
    from linkml_runtime.utils.schemaview import SchemaView

    yaml_path: str = str(files("biolink_model").joinpath("schema/biolink_model.yaml"))
    return SchemaView(yaml_path)


def _snake(name: str) -> str:
    """Convert a spaced LinkML slot name to snake_case (``"related to"`` -> ``"related_to"``)."""
    return name.replace(" ", "_")


@cache
def _slot_descendants(root: str) -> frozenset[str]:
    """Return all slot names that descend from ``root`` via ``is_a`` (exclusive of ``root``).

    Args:
        root: Spaced LinkML slot name (e.g. ``"related to"``).

    Returns:
        Frozenset of descendant slot names (spaced form).
    """
    children: dict[str, list[str]] = {}
    for name, slot in _schema().all_slots(imports=False).items():
        if slot.is_a:
            children.setdefault(slot.is_a, []).append(name)
    out: set[str] = set()
    stack: list[str] = [root]
    while stack:
        current: str = stack.pop()
        for child in children.get(current, []):
            if child not in out:
                out.add(child)
                stack.append(child)
    return frozenset(out)


def _predicate_values() -> list[str]:
    """All Biolink predicates: the ``related to`` slot hierarchy, snake_cased (root included)."""
    return sorted({_snake(p) for p in (_slot_descendants("related to") | {"related to"})})


def _qualifier_values() -> list[str]:
    """All Biolink qualifier slot names: the ``qualifier`` slot hierarchy, snake_cased (root included)."""
    return sorted({_snake(q) for q in (_slot_descendants("qualifier") | {"qualifier"})})


@cache
def _association_classes() -> tuple[type[Any], ...]:
    """Every Biolink ``Association`` Pydantic class, including ``Association`` itself.

    Association subclasses declare slots the base class does not (for example
    ``clinical_approval_status`` on ``EntityToDiseaseAssociation``). Callers that
    reason about "any field a Tablassert edge might legitimately carry" must
    consider the whole family, not just the base MRO.

    Returns:
        Tuple of association classes defined in ``pydanticmodel_v2``.
    """
    return tuple(cls for cls in vars(_bm).values() if inspect.isclass(cls) and cls.__module__ == _bm.__name__ and issubclass(cls, _bm.Association))


def _association_model_fields() -> set[str]:
    """All field names declared by *any* Biolink ``Association`` class.

    Unions ``model_fields`` across the entire association family rather than only
    walking ``Association.__mro__``. Walking the base MRO alone silently excludes
    subclass-only evidence slots -- ``clinical_approval_status`` and
    ``number_of_cases`` -- which then get demoted into ``supporting_text`` by
    :func:`lib.fold_unknown_to_supporting_text`.
    """
    fields: set[str] = set()
    for klass in _association_classes():
        fields |= set(klass.model_fields.keys())
    return fields


def _biolink_enum_values(enum_cls: type[Enum]) -> list[str]:
    """Sorted string values of a Biolink Pydantic enum class (e.g. ``KnowledgeLevelEnum``)."""
    return sorted(str(member.value) for member in enum_cls)


# Permissible ``effect_type`` values, derived from the installed ``biolink-model``'s
# ``EffectTypeEnum`` (the slot and its enum shipped with Biolink PR #1774). Kept as a
# plain tuple so ``coerce`` can consume it without touching the enum.
EFFECT_TYPE_VALUES: tuple[str, ...] = tuple(_biolink_enum_values(_bm.EffectTypeEnum))


def _annotation_choices(annotation: Any) -> frozenset[str] | None:
    """Return the closed value set of a Pydantic field annotation, or ``None`` if open.

    Biolink constrains some slots with a generated ``Enum`` (``DirectionQualifierEnum``)
    and others with a ``Literal[...]``. Both are closed vocabularies; a bare ``str``
    annotation is open. ``Optional[...]`` / ``list[...]`` wrappers are unwrapped.

    Args:
        annotation: A Pydantic ``FieldInfo.annotation``.

    Returns:
        Frozenset of permitted string values, or ``None`` when unconstrained.
    """
    if annotation is None or annotation is str:
        return None
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return frozenset(str(member.value) for member in annotation)
    origin: Any = get_origin(annotation)
    if origin is Literal:
        return frozenset(str(arg) for arg in get_args(annotation))
    args: tuple[Any, ...] = get_args(annotation)
    if args:
        # Optional[X] / list[X] / Union[...]: a closed set anywhere makes the slot closed.
        choices: set[str] = set()
        found: bool = False
        for arg in args:
            if arg is type(None):
                continue
            nested: frozenset[str] | None = _annotation_choices(arg)
            if nested is not None:
                choices |= nested
                found = True
        if found:
            return frozenset(choices)
    return None


@cache
def _field_owners() -> dict[str, frozenset[str]]:
    """Map every Pydantic field name to the set of Biolink classes that declare it.

    Used to distinguish slots that exist in the LinkML YAML but were never attached
    to a class (and so can never be serialized) from ones with a real home.
    """
    owners: dict[str, set[str]] = {}
    for cls in vars(_bm).values():
        if not (inspect.isclass(cls) and cls.__module__ == _bm.__name__):
            continue
        for field in getattr(cls, "model_fields", {}):
            owners.setdefault(field, set()).add(cls.__name__)
    return {field: frozenset(names) for field, names in owners.items()}


def _predicate_accepts(cls: type[Any], predicate: str) -> bool:
    """Whether an association class permits ``predicate`` on its ``predicate`` slot."""
    field: Any = cls.model_fields.get("predicate")
    if field is None:
        return False
    choices: frozenset[str] | None = _annotation_choices(field.annotation)
    return choices is None or predicate in choices


@cache
def association_class(category: str) -> type[Any]:
    """Resolve a ``biolink:X`` edge category CURIE to its Pydantic class.

    Falls back to ``Association`` for unknown or malformed categories.
    """
    name: str = category.removeprefix("biolink:")
    cls: Any = getattr(_bm, name, None)
    if inspect.isclass(cls) and issubclass(cls, _bm.Association):
        return cls
    return _bm.Association


@cache
def resolve_association_class(category: str, predicate: str) -> type[Any]:
    """Pick the most specific association class that actually permits ``predicate``.

    Tablassert derives a candidate edge category from the (subject role, object role)
    pair without consulting the predicate, which routinely produces contradictions --
    ``GeneToDiseaseAssociation`` restricts ``predicate`` to
    ``contributes_to|associated_with|affects``, so a
    ``biolink:gene_associated_with_condition`` edge labelled with that category can
    never validate.

    Walks the candidate's MRO most-specific-first and returns the first association
    ancestor whose ``predicate`` slot accepts the value, so specificity is only ever
    given up as far as correctness requires. ``Association`` (open ``predicate``) is
    the guaranteed floor.

    Args:
        category: Candidate edge category CURIE (``"biolink:GeneToDiseaseAssociation"``).
        predicate: Predicate CURIE (``"biolink:gene_associated_with_condition"``).

    Returns:
        The resolved association class.
    """
    for ancestor in association_class(category).__mro__:
        if inspect.isclass(ancestor) and issubclass(ancestor, _bm.Association) and _predicate_accepts(ancestor, predicate):
            return ancestor
    return _bm.Association


# Edge columns Tablassert emits that are not fields of any Biolink ``Association``
# class: synonym carryover from NamedThing and KGX denormalized fields. Unioned with
# the derived Biolink fields so ``ALLOWED_EDGE_FIELDS`` stays a superset of what the
# pipeline may legitimately emit (so ``lib.fold_unknown_to_supporting_text`` never
# folds a real edge column into ``supporting_text``).
#
# Deliberately NOT listed here, because none of them can be serialized onto an edge:
#   - ``source_record_urls`` / ``upstream_resource_ids`` -- ``domain: retrieval source``,
#     so they belong inside a ``sources`` entry, not on the association.
#   - ``supporting_study_*`` -- deprecated by Biolink PR #1770 and attached to no class
#     (see ``UNSATISFIABLE_EDGE_FIELDS``); coercion renames them onto the current
#     ``study_*`` slots (``STUDY_METADATA_FIELDS``), which hang off the inlined ``Study``.
#   - ``taxon`` -- a node property; no species-context edge is synthesized from it.
#
# ``supporting_case_ids`` is build-internal: it carries the per-row case-ID list that
# lets ``uuid_on_collision: merge`` recompute ``number_of_cases`` as the union size, and
# the Rust dedup pass strips it before any edge is written, so it never ships in the
# final NDJSON (which also keeps it out of ``validate_kgx`` and QC). No association
# class declares it, so ``lib.prune_to_class`` leaves the column untouched and no
# ``CLASS_FIELD_OVERRIDES`` grant is needed.
TABLASERT_EDGE_EXTRAS: frozenset[str] = frozenset(
    [
        "broad_synonym",
        "equivalent_identifiers",
        "evidence_direction",
        "evidence_type",
        "exact_synonym",
        "full_name",
        "information_content",
        "narrow_synonym",
        "provided_by",
        "related_synonym",
        "relation",
        "supporting_case_ids",
        "supporting_documents",
        "synonym",
        "xref",
    ]
)


if TYPE_CHECKING:
    # Static stubs for type checking only. They declare just the members referenced
    # by name elsewhere in the codebase; at runtime the ``else`` branch builds the
    # full enums dynamically from the Biolink Model.

    class Categories(str, Enum):
        DISEASE: Categories
        GENE: Categories
        GENOMIC_ENTITY: Categories
        NAMED_THING: Categories
        PHENOTYPIC_FEATURE: Categories
        PROTEIN: Categories

    class EdgeCategories(str, Enum):
        ASSOCIATION: EdgeCategories
        CHEMICAL_GENE_INTERACTION_ASSOCIATION: EdgeCategories

    class Predicates(str, Enum):
        RELATED_TO: Predicates
        TREATS: Predicates

    class Qualifiers(str, Enum):
        DISEASE_CONTEXT_QUALIFIER: Qualifiers
        OBJECT_DIRECTION_QUALIFIER: Qualifiers
        SUBJECT_CONTEXT_QUALIFIER: Qualifiers

    class KnowledgeLevels(str, Enum):
        NOT_PROVIDED: KnowledgeLevels
        STATISTICAL_ASSOCIATION: KnowledgeLevels

    class AgentTypes(str, Enum):
        DATA_ANALYSIS_PIPELINE: AgentTypes
        MANUAL_AGENT: AgentTypes

    class EffectTypes(str, Enum):
        ODDS_RATIO: EffectTypes
        SPEARMANS_RHO: EffectTypes

else:
    Categories = _build_str_enum("Categories", _entity_category_names())
    EdgeCategories = _build_str_enum("EdgeCategories", _association_names())
    Predicates = _build_str_enum("Predicates", _predicate_values())
    Qualifiers = _build_str_enum("Qualifiers", _qualifier_values())
    KnowledgeLevels = _build_str_enum("KnowledgeLevels", _biolink_enum_values(cast("type[Enum]", _bm.KnowledgeLevelEnum)))
    AgentTypes = _build_str_enum("AgentTypes", _biolink_enum_values(cast("type[Enum]", _bm.AgentTypeEnum)))
    # Derived from the installed model's ``EffectTypeEnum`` via ``EFFECT_TYPE_VALUES``.
    EffectTypes = _build_str_enum("EffectTypes", list(EFFECT_TYPE_VALUES))


_schema_definition: Any = _schema().schema
BIOLINK_VERSION: str = str(_schema_definition.version) if _schema_definition is not None else "unknown"
"""Version of the Biolink Model these values were derived from (e.g. ``"4.4.4"``)."""

UNSATISFIABLE_EDGE_FIELDS: frozenset[str] = frozenset(q.value for q in Qualifiers if q.value not in _field_owners()) | frozenset(
    field
    for field in (
        "sample_size",
        "statistical_significance_qualifier",
        "supporting_study_cohort",
        "supporting_study_context",
        "supporting_study_date_range",
        "supporting_study_method_description",
        "supporting_study_method_types",
        "supporting_study_size",
    )
    if field not in _field_owners()
)
"""Slot names that exist in the Biolink LinkML schema but on no Pydantic class.

``Qualifiers`` is derived from the LinkML *slot* hierarchy, which is strictly broader
than the set of slots actually attached to a class. Emitting one of these produces a
record that can never validate, so configs referencing them are rejected up front and
their values are routed onto the inlined ``Study`` instead. The set is derived from the
installed model: ``statistical_significance_qualifier`` left it when biolink-model 4.4.4
attached the slot to ``Association``, and the deprecated ``supporting_study_*`` names
leave it the day a release attaches them. ``relationship_strength`` is deliberately
absent: it is a legacy Tablassert alias that coercion renames to the real edge slot
``effect_size`` -- it never needs a study detour.
"""


STUDY_METADATA_FIELDS: frozenset[str] = frozenset(field for field in _bm.Study.model_fields if field.startswith("study_"))
"""The current Biolink ``Study`` node properties holding study-level metadata.

Derived from the installed model: ``study_size``, ``study_cohort``, ``study_context``,
``study_date_range``, ``study_method_description``, ``study_method_types``. Biolink PR
#1770 deprecated the old ``supporting_study_*`` association slots in favor of exactly
these properties (each carries ``deprecated_element_has_exact_replacement``), because a
multivalued ``has supporting studies`` edge cannot say which study a flat value belongs
to -- the metadata hangs off the ``Study`` itself. Tablassert therefore renames the
legacy annotation names onto these slots (``coerce.coerce_study_metadata_columns``) and
emits them on the inlined ``Study``, never on the edge and never as a ``StudyResult``
description dump.
"""


DISABLED_EDGE_FIELDS: frozenset[str] = frozenset({"species_context_qualifier"})
"""Edge fields Tablassert intentionally never emits or accepts.

This policy is separate from :data:`UNSATISFIABLE_EDGE_FIELDS`: the latter tracks
Biolink Model attachment and may change with a dependency release, while this set is
a stable Tablassert product decision. Keeping it separate prevents a future Biolink
release from making ``species_context_qualifier`` silently emittable again.
"""


CLASS_FIELD_OVERRIDES: dict[str, frozenset[str]] = {
    "EntityToDiseaseAssociation": frozenset(
        {
            "anatomical_context_qualifier",
            "disease_context_qualifier",
            "frequency_qualifier",
            "population_context_qualifier",
            "regulatory_approvals",
            "sex_qualifier",
            "temporal_context_qualifier",
        }
    ),
    "EntityToPhenotypicFeatureAssociation": frozenset(
        {
            "anatomical_context_qualifier",
            "disease_context_qualifier",
            "frequency_qualifier",
            "population_context_qualifier",
            "regulatory_approvals",
            "sex_qualifier",
            "temporal_context_qualifier",
        }
    ),
}
"""Per-class grants of edge fields the resolved association class does not declare.

Keys are bare association class names (``association_class(cat).__name__``), values
the slots ``lib.prune_to_class`` keeps on rows resolved to that class even though the
installed model attaches them elsewhere.

The motivating case is a DAKP contraindication edge: ``regulatory_approvals`` is a
canonical slot not yet attached by the installed model, while
``disease_context_qualifier`` is declared only on the
``ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation`` lineage -- so one edge can
natively carry both only through these explicit class-scoped grants. The same gap
covers DAKP's sparse qualifier stack (``anatomical_context_qualifier``,
``sex_qualifier``, ``population_context_qualifier``, ``frequency_qualifier``,
``temporal_context_qualifier``): each is a satisfiable Biolink qualifier slot that no
pinned class declares, so without a grant ``prune_to_class`` would null it off the
edge. Deliberately excluded: ``species_context_qualifier`` (in
``DISABLED_EDGE_FIELDS`` -- never emittable), ``temporal_interval_qualifier`` and
``severity_qualifier`` (in ``UNSATISFIABLE_EDGE_FIELDS`` -- attached to no class, so
a grant could never validate). Tablassert
deliberately emits the granted fields on the pinned classes ahead of the pinned model
(pending an upstream Biolink widening). ``_validation_record`` strips granted fields
before record validation so the deliberate gap is not reported as ``extra_forbidden``.

A tripwire test asserts every granted field is still absent from its class: the moment
a biolink-model release attaches the slot, the suite fails and the stale grant is
removed.
"""


ENUM_RANGED_QUALIFIERS: dict[str, frozenset[str]] = {
    qualifier.value: choices
    for qualifier in Qualifiers
    for owners in (_field_owners().get(qualifier.value, frozenset()),)
    if owners
    for choices in (
        frozenset().union(
            *(
                _annotation_choices(getattr(_bm, owner).model_fields[qualifier.value].annotation) or frozenset()
                for owner in owners
                if hasattr(_bm, owner)
            )
        ),
    )
    if choices
}
"""Qualifier slots whose range is a closed vocabulary rather than a CURIE.

``Qualifier`` config entries inherit ``NodeEncoding`` and are therefore entity-resolved
through the fullmap by default. That is correct for CURIE-ranged qualifiers
(``anatomical_context_qualifier`` -> ``UBERON:0001557``) but wrong for enum-ranged ones:
``object_direction_qualifier`` wants the token ``increased``, not ``UMLS:C0205217``.
Values for these slots are validated against the vocabulary and passed through
unresolved.
"""


ALLOWED_EDGE_FIELDS: frozenset[str] = (
    (
        frozenset(_association_model_fields())
        | {q.value for q in Qualifiers}
        | TABLASERT_EDGE_EXTRAS
        | frozenset().union(*CLASS_FIELD_OVERRIDES.values())
    )
    - UNSATISFIABLE_EDGE_FIELDS
    - DISABLED_EDGE_FIELDS
)
"""Authoritative biolink-compliant edge column allow-list.

Any column on an edge frame that is not in this set is folded into the
``supporting_text`` ``list[str]`` field by ``lib.fold_unknown_to_supporting_text()``
as a ``"column: value"`` string. Composed of the fields declared by *any* Biolink
association class, the derived qualifier slot names, the curated
``TABLASERT_EDGE_EXTRAS``, and explicit :data:`CLASS_FIELD_OVERRIDES` grants -- less the
slots that no Pydantic class can hold and the fields disabled by Tablassert policy.

Note this is a per-*family* allow-list: a field being permitted here does not mean the
specific association class chosen for a given edge accepts it. Per-record pruning
against the resolved class is done by ``lib.prune_to_class()``.
"""


KNOWN_PENDING_EDGE_FIELDS: frozenset[str] = TABLASERT_EDGE_EXTRAS - frozenset(_association_model_fields())
"""Curated edge extras the installed Biolink Model does not (yet) declare on any association.

Tablassert emits these deliberately -- the KGX denormalized carryovers (``synonym``,
``xref``, ``relation``, ...) -- so a Biolink class rejects them as ``extra_forbidden`` even
though the build is behaving as designed. The
statistical ``effect_size`` / ``effect_type`` pair is no longer pending since
biolink-model 4.4.4 shipped PR #1774.
:func:`is_pending_problem` uses this set
to separate "Tablassert is ahead of the pinned model" from "this record is genuinely
malformed", so a validity *score* is not dominated by a known, intentional gap.

Derived from the installed package, exactly like :data:`UNSATISFIABLE_EDGE_FIELDS`: a
field drops out of the set the moment a biolink-model release declares it, with no code
change.
"""


def legal_predicates(category: str) -> frozenset[str] | None:
    """Return the predicates an edge category's association class permits.

    The counterpart to :func:`resolve_association_class`: that function asks "given this
    predicate, how specific a class survives?", this one asks "given this class, which
    predicates keep it?". Tablassert has no other authoring-time answer -- a predicate the
    class forbids is never rejected, it silently demotes the edge toward ``Association``.

    Args:
        category: Edge category CURIE (``"biolink:GeneToDiseaseAssociation"``).

    Returns:
        The permitted predicate CURIEs, or ``None`` when the class leaves ``predicate``
        open (``Association`` itself, which accepts anything).
    """
    field: Any = association_class(category).model_fields.get("predicate")
    return None if field is None else _annotation_choices(field.annotation)


@cache
def node_class(category: str) -> type[Any]:
    """Resolve a ``biolink:X`` node category CURIE to its Pydantic class.

    Mirrors ``bmt.pydantic.get_node_class`` (used by ``translator-ingests``) without
    taking on the ``bmt`` dependency: Tablassert already knows the exact category it
    assigned, so a direct lookup is sufficient. Falls back to ``NamedThing``.
    """
    name: str = category.removeprefix("biolink:")
    cls: Any = getattr(_bm, name, None)
    if inspect.isclass(cls) and issubclass(cls, _bm.NamedThing):
        return cls
    return _bm.NamedThing


def class_fields(cls: type[Any]) -> frozenset[str]:
    """Field names a Pydantic class accepts (cached per class by the caller)."""
    return frozenset(cls.model_fields.keys())


def is_multivalued(cls: type[Any], field: str) -> bool:
    """Whether ``cls`` declares ``field`` as a list-valued slot.

    Biolink makes the same qualifier multivalued on some association classes and
    scalar on others, so a value carried across a class change may need wrapping.
    """
    info: Any = cls.model_fields.get(field)
    if info is None:
        return False
    annotation: Any = info.annotation
    if get_origin(annotation) is list:
        return True
    return any(get_origin(arg) is list for arg in get_args(annotation))


@cache
def _retrieval_source_id_required() -> bool:
    """Whether the installed model still requires the inherited source ``id`` field."""
    retrieval_source: Any = getattr(_bm, "RetrievalSource", None)
    id_field: Any = getattr(retrieval_source, "model_fields", {}).get("id")
    return id_field is not None and id_field.is_required()


def _validation_record(record: dict[str, Any], *, edge: bool) -> dict[str, Any]:
    """Apply only in-memory compatibility shims needed by the installed Biolink model.

    Two shims, both used solely for Pydantic validation -- neither changes the decoded
    KGX record or the files written by the pipeline:

    * ``RetrievalSource`` in the currently pinned model still requires the inherited
      ``Entity.id`` even though ``resource_id`` is the canonical provenance identifier
      emitted by Tablassert, so ``id`` is aliased in.
    * Fields granted by :data:`CLASS_FIELD_OVERRIDES` are stripped when the record's
      own class does not declare them: the grant is a deliberate, class-scoped step
      ahead of the pinned model, not a malformed record, so its ``extra_forbidden``
      is never reported.
    """
    if not edge:
        return record
    out: dict[str, Any] = record
    categories: Any = record.get("category") or []
    category: str = categories[0] if isinstance(categories, list) and categories else str(categories or "")
    granted: frozenset[str] = CLASS_FIELD_OVERRIDES.get(category.removeprefix("biolink:"), frozenset())
    undeclared: frozenset[str] = granted - class_fields(association_class(category)) if granted else frozenset()
    if undeclared:
        out = {key: value for key, value in out.items() if key not in undeclared}
    if not _retrieval_source_id_required():
        return out
    sources: Any = out.get("sources")
    if not isinstance(sources, list):
        return out
    normalized: list[Any] = []
    changed: bool = False
    for source in sources:
        if isinstance(source, dict) and "id" not in source and "resource_id" in source:
            normalized.append({**source, "id": source["resource_id"]})
            changed = True
        else:
            normalized.append(source)
    return {**out, "sources": normalized} if changed else out


def validate_record(record: dict[str, Any], *, edge: bool) -> list[str]:
    """Validate one KGX record against the Biolink class named by its ``category``.

    Args:
        record: A decoded NDJSON node or edge record.
        edge: ``True`` to dispatch on the association family, ``False`` for nodes.

    Returns:
        A list of ``"field: error-type"`` strings; empty when the record validates.
    """
    from pydantic import ValidationError

    categories: Any = record.get("category") or []
    category: str = categories[0] if isinstance(categories, list) and categories else str(categories or "")
    cls: type[Any] = association_class(category) if edge else node_class(category)
    try:
        cls(**_validation_record(record, edge=edge))
    except ValidationError as error:
        return [f"{'.'.join(str(part) for part in item['loc']) or '?'}: {item['type']}" for item in error.errors()]
    return []


def _scalar_types(annotation: Any) -> set[type]:
    """Unwrap ``Optional`` / ``list`` / ``Union`` down to the concrete scalar types."""
    if isinstance(annotation, type):
        return {annotation}
    out: set[type] = set()
    for arg in get_args(annotation):
        if arg is type(None):
            continue
        out |= _scalar_types(arg)
    return out


@cache
def numeric_slot_kind(field: str) -> str | None:
    """Return ``"int"`` / ``"float"`` when a Biolink slot has a numeric range.

    Tablassert stringifies its numeric annotation columns for notation control (p-value
    columns are ALWAYS scientific-notation strings, which Pydantic's lax validation
    coerces back for the ``float``-typed ``p_value`` / ``adjusted_p_value`` slots), but
    a non-p-value column that the installed model types ``int`` (``study_size`` on the
    inlined ``Study`` since ``biolink/biolink-model#1770``) or ``float`` (``effect_size``
    since #1774) must be emitted as a real JSON number. Consults every class declaring
    the field -- association slots AND the inlined ``Study`` metadata properties -- and
    is derived from the installed model, so the answer tracks whatever version is pinned.

    Args:
        field: Column/slot name.

    Returns:
        ``"int"``, ``"float"``, or ``None`` when the slot is not numeric (or unknown).
    """
    kinds: set[type] = set()
    for owner in _field_owners().get(field, frozenset()):
        cls: Any = getattr(_bm, owner, None)
        info: Any = getattr(cls, "model_fields", {}).get(field) if inspect.isclass(cls) else None
        if info is not None:
            kinds |= _scalar_types(info.annotation)
    if float in kinds:
        return "float"
    if int in kinds and bool not in kinds:
        return "int"
    return None


def is_pending_problem(problem: str) -> bool:
    """Whether a ``"field: error-type"`` problem is a known, intentional model gap.

    True only for an ``extra_forbidden`` rejection of a field in
    :data:`KNOWN_PENDING_EDGE_FIELDS` -- i.e. Tablassert emitted a column on purpose that
    the pinned Biolink Model has not declared yet. Every other failure is a real defect.
    """
    field, _, error_type = problem.rpartition(": ")
    return error_type == "extra_forbidden" and field in KNOWN_PENDING_EDGE_FIELDS


def validate_kgx(nodes_path: Path, edges_path: Path, limit: int = 20) -> dict[str, Any]:
    """Validate emitted KGX NDJSON files against the Biolink Pydantic model.

    Tablassert derives its *vocabulary* from the model but historically never
    instantiated a Biolink class against an emitted record, so a build could -- and did
    -- ship files where no record validated. This closes that loop: every node and edge
    is constructed as the class named by its own ``category``.

    Two pass rates are reported. ``valid`` is strict and drives ``ok`` (the CLI's
    non-zero exit). ``valid_excluding_pending`` additionally counts records whose *every*
    failure is a :func:`is_pending_problem` -- the score to optimize against, so a
    deliberate gap like the KGX denormalized carryovers is not mistaken for a
    malformed record. The two converge as future model releases absorb curated extras.

    Args:
        nodes_path: Path to ``<name>_<version>.nodes.ndjson``.
        edges_path: Path to ``<name>_<version>.edges.ndjson``.
        limit: Maximum number of example failures to retain per file.

    Returns:
        Mapping with per-file ``total`` / ``valid`` / ``valid_excluding_pending`` /
        ``failures`` counts, a ``missing`` flag, a ``problems`` histogram keyed by
        ``"field: error-type"``, up to ``limit`` ``examples``, and top-level ``ok`` /
        ``ok_excluding_pending`` flags.
    """
    report: dict[str, Any] = {"biolink_version": BIOLINK_VERSION, "ok": True, "ok_excluding_pending": True}
    for label, path, edge in (("nodes", nodes_path, False), ("edges", edges_path, True)):
        total: int = 0
        valid: int = 0
        valid_excluding_pending: int = 0
        problems: Counter[str] = Counter()
        examples: list[dict[str, Any]] = []
        # A missing path must never read as a clean bill of health: counting zero records
        # out of zero would otherwise exit 0 on a typo'd filename and hide a broken build.
        missing: bool = not path.is_file()
        if not missing:
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    total += 1
                    record: dict[str, Any] = json.loads(line)
                    errors: list[str] = validate_record(record, edge=edge)
                    if not errors:
                        valid += 1
                        valid_excluding_pending += 1
                        continue
                    if all(is_pending_problem(problem) for problem in errors):
                        valid_excluding_pending += 1
                    problems.update(errors)
                    if len(examples) < limit:
                        examples.append({"id": record.get("id"), "errors": errors})
        report[label] = {
            "total": total,
            "valid": valid,
            "valid_excluding_pending": valid_excluding_pending,
            "failures": total - valid,
            "missing": missing,
            "problems": dict(problems.most_common()),
            "examples": examples,
        }
        if missing or total != valid:
            report["ok"] = False
        if missing or total != valid_excluding_pending:
            report["ok_excluding_pending"] = False
    return report


# Node slots Tablassert can always populate from a fullmap hit. A class requiring
# anything outside this set cannot be emitted, because there is no source for the value.
FILLABLE_NODE_FIELDS: frozenset[str] = frozenset({"id", "name", "category", "provided_by", "in_taxon", "in_taxon_label"})


@cache
def resolve_node_class(category: str) -> type[Any]:
    """Pick the most specific node class Tablassert can actually emit for ``category``.

    Entity resolution can land on a class that is unusable as a KGX node: ``Publication``
    requires ``publication_type`` and ``ClinicalAttribute`` requires
    ``has_attribute_type`` -- neither of which a fullmap hit provides -- while mixins
    such as ``GenomicEntity`` reject their own name in the ``category`` literal.

    Walks the MRO most-specific-first and returns the first class whose required fields
    are all fillable and whose ``category`` vocabulary admits its own name, so
    specificity is only given up as far as correctness requires. ``NamedThing`` is the
    guaranteed floor.

    Args:
        category: Candidate node category CURIE (``"biolink:Publication"``).

    Returns:
        The resolved node class; emit ``category`` from its own default.
    """
    for ancestor in node_class(category).__mro__:
        if not (inspect.isclass(ancestor) and issubclass(ancestor, _bm.NamedThing)):
            continue
        required: set[str] = {name for name, info in ancestor.model_fields.items() if info.is_required()}
        if not required <= FILLABLE_NODE_FIELDS:
            continue
        choices: frozenset[str] | None = _annotation_choices(ancestor.model_fields["category"].annotation)
        if choices is not None and f"biolink:{ancestor.__name__}" not in choices:
            continue
        return ancestor
    return _bm.NamedThing


@cache
def resolve_node_category(category: str) -> str:
    """Resolve a node category CURIE to one that can actually be emitted."""
    return f"biolink:{resolve_node_class(category).__name__}"
