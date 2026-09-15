"""Tests for the Biolink-derived vocabulary in :mod:`tablassert.biolink`.

These replace the hand-maintained-enum tests that used to live in
``test_enums.py``. Beyond membership checks, they act as *drift guards*: the
derived sets are cross-checked against the ``biolink-model`` package itself (the
source of truth), and specific Biolink 4.4.3 renames / additions / removals are
asserted so that a change in the underlying model is surfaced loudly.
"""

from __future__ import annotations

import inspect
import json
from enum import Enum
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import biolink_model.datamodel.pydanticmodel_v2 as bm
import pytest

from tablassert.biolink import (
    ALLOWED_EDGE_FIELDS,
    BIOLINK_VERSION,
    CATEGORY_OVERRIDES,
    CLASS_FIELD_OVERRIDES,
    DISABLED_EDGE_FIELDS,
    EFFECT_TYPE_VALUES,
    KNOWN_PENDING_EDGE_FIELDS,
    TABLASERT_EDGE_EXTRAS,
    UNSATISFIABLE_EDGE_FIELDS,
    AgentTypes,
    Categories,
    EdgeCategories,
    EffectTypes,
    KnowledgeLevels,
    Predicates,
    Qualifiers,
    association_class,
    class_fields,
    is_pending_problem,
    legal_predicates,
    numeric_slot_kind,
    resolve_association_class,
    validate_kgx,
    validate_record,
)

if TYPE_CHECKING:
    from linkml_runtime.utils.schemaview import SchemaView


@pytest.fixture(scope="module")
def schema() -> SchemaView:
    """An independent ``SchemaView`` over the bundled Biolink schema (test oracle)."""
    from linkml_runtime.utils.schemaview import SchemaView

    return SchemaView(str(files("biolink_model").joinpath("schema/biolink_model.yaml")))


def _slot_names_snake(schema: SchemaView) -> set[str]:
    return {name.replace(" ", "_") for name in schema.all_slots()}


def _category_name(cls: Any) -> str:
    """Bare Biolink category name for a Pydantic model class (read on ``Any`` to dodge narrowing)."""
    field: Any = cls.model_fields.get("category")
    if field is not None and isinstance(field.default, list):
        for value in field.default:
            if isinstance(value, str) and value.startswith("biolink:"):
                return value[len("biolink:") :]
    return str(cls.__name__)


def _biolink_category_names() -> set[str]:
    """Canonical Biolink entity category names, read directly from the Pydantic model."""
    return {
        _category_name(cls)
        for cls in vars(bm).values()
        if inspect.isclass(cls) and cls.__module__ == bm.__name__ and issubclass(cls, bm.Entity) and not issubclass(cls, bm.Association)
    }


def _biolink_association_names() -> set[str]:
    """Canonical Biolink association names, read directly from the Pydantic model."""
    return {
        _category_name(cls) for cls in vars(bm).values() if inspect.isclass(cls) and cls.__module__ == bm.__name__ and issubclass(cls, bm.Association)
    }


# --- enum shape ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "enum", [Categories, EdgeCategories, Predicates, Qualifiers, KnowledgeLevels, AgentTypes, EffectTypes], ids=lambda e: e.__name__
)
def test_is_str_enum(enum: type) -> None:
    """Every derived vocabulary is a ``str``/``Enum`` subclass (usable as a Pydantic field type)."""
    assert issubclass(enum, str)
    assert issubclass(enum, Enum)


# --- spot-check members referenced by name elsewhere -------------------------------------------


def test_categories_has_gene() -> None:
    assert Categories.GENE == "Gene"


def test_categories_has_disease() -> None:
    assert Categories.DISEASE == "Disease"


def test_categories_has_protein() -> None:
    assert Categories.PROTEIN == "Protein"


def test_predicates_has_treats() -> None:
    assert Predicates.TREATS == "treats"


def test_predicates_has_related_to() -> None:
    assert Predicates.RELATED_TO == "related_to"


def test_qualifiers_has_disease_context() -> None:
    assert Qualifiers.DISEASE_CONTEXT_QUALIFIER == "disease_context_qualifier"


def test_knowledge_levels_has_statistical_association() -> None:
    assert KnowledgeLevels.STATISTICAL_ASSOCIATION == "statistical_association"


def test_knowledge_levels_has_not_provided() -> None:
    assert KnowledgeLevels.NOT_PROVIDED == "not_provided"


def test_agent_types_has_data_analysis_pipeline() -> None:
    assert AgentTypes.DATA_ANALYSIS_PIPELINE == "data_analysis_pipeline"


def test_agent_types_has_manual_agent() -> None:
    assert AgentTypes.MANUAL_AGENT == "manual_agent"


def test_effect_types_has_spearmans_rho() -> None:
    assert EffectTypes.SPEARMANS_RHO == "spearmans_rho"


def test_effect_types_has_odds_ratio() -> None:
    assert EffectTypes.ODDS_RATIO == "odds_ratio"


# --- provenance ---------------------------------------------------------------------------------


def test_biolink_version_present() -> None:
    """The derived vocabulary records a concrete Biolink Model version."""
    assert BIOLINK_VERSION
    assert BIOLINK_VERSION != "unknown"
    assert "." in BIOLINK_VERSION


# --- exact match against the biolink-model source of truth --------------------------------------


def test_knowledge_levels_match_biolink() -> None:
    """KnowledgeLevels is exactly the Biolink ``KnowledgeLevelEnum`` value set."""
    assert {e.value for e in KnowledgeLevels} == {e.value for e in cast("type[Enum]", bm.KnowledgeLevelEnum)}


def test_agent_types_match_biolink() -> None:
    """AgentTypes is exactly the Biolink ``AgentTypeEnum`` value set."""
    assert {e.value for e in AgentTypes} == {e.value for e in cast("type[Enum]", bm.AgentTypeEnum)}


def test_effect_types_match_pr1774() -> None:
    """EffectTypes is exactly the 25 permissible ``effect_type`` values from Biolink PR #1774.

    The expected set is recorded here to make the Biolink PR #1774 vocabulary
    explicit (``close_mappings`` is intentionally ignored); the installed
    ``bm.EffectTypeEnum`` remains the source of truth for the derived enum.
    """
    expected: set[str] = {
        "regression_coefficient",
        "log2_fold_change",
        "wald_ratio",
        "inverse_variance_weighted",
        "mr_egger",
        "weighted_median",
        "standardized_mean_difference",
        "cohens_d",
        "hedges_g",
        "glasss_delta",
        "strictly_standardized_mean_difference",
        "correlation_coefficient",
        "pearsons_r",
        "spearmans_rho",
        "kendalls_tau",
        "polychoric_correlation",
        "matthews_correlation_coefficient",
        "goodman_kruskal_gamma",
        "r2_linkage_disequilibrium",
        "odds_ratio",
        "relative_risk",
        "hazard_ratio",
        "eta_squared",
        "omega_squared",
        "root_mean_square_standardized_effect",
    }
    assert len(expected) == 25
    assert {e.value for e in EffectTypes} == expected
    assert set(EFFECT_TYPE_VALUES) == expected
    assert {e.value for e in cast("type[Enum]", bm.EffectTypeEnum)} == expected


def test_categories_match_biolink() -> None:
    """Categories is exactly the set of Biolink entity category names plus the overrides."""
    assert {c.value for c in Categories} == _biolink_category_names() | CATEGORY_OVERRIDES


def test_edge_categories_match_biolink() -> None:
    """EdgeCategories is exactly the set of Biolink association names."""
    assert {e.value for e in EdgeCategories} == _biolink_association_names()


def test_predicates_are_biolink_slots(schema: SchemaView) -> None:
    """Every predicate is a real Biolink slot (guarantees translator-ingests needs no re-validation)."""
    slots: set[str] = _slot_names_snake(schema)
    for predicate in Predicates:
        assert predicate.value in slots, predicate.value


def test_qualifiers_are_biolink_slots(schema: SchemaView) -> None:
    """Every qualifier is a real Biolink slot."""
    slots: set[str] = _slot_names_snake(schema)
    for qualifier in Qualifiers:
        assert qualifier.value in slots, qualifier.value


# --- Biolink drift markers ----------------------------------------------------------------------


def test_categories_reflect_4_4_3_renames() -> None:
    """Biolink 4.4.3 renamed the RNA classes to acronym CamelCase; the old spellings are gone."""
    values: set[str] = {c.value for c in Categories}
    assert "MicroRNA" in values
    assert "SiRNA" in values
    assert "RNAProduct" in values
    assert "Microrna" not in values
    assert "Sirna" not in values


def test_categories_include_upper_levels() -> None:
    values: set[str] = {c.value for c in Categories}
    assert "Entity" in values
    assert "NamedThing" in values
    assert "Gene" in values
    assert "PhenotypicFeature" in values


def test_predicates_reflect_4_4_3_changes() -> None:
    """4.4.3 added sensitivity predicates and retired the older response predicates."""
    values: set[str] = {p.value for p in Predicates}
    assert "affects_sensitivity_to" in values
    assert "related_to" in values
    assert "treats" in values
    # Retired/renamed in 4.4.3 -- must no longer be emitted.
    assert "assesses" not in values
    assert "response_affected_by" not in values


def test_qualifiers_reflect_4_4_3_additions() -> None:
    values: set[str] = {q.value for q in Qualifiers}
    assert "qualifier" in values
    assert "process_qualifier" in values
    assert "response_context_qualifier" in values


def test_edge_categories_include_associations() -> None:
    values: set[str] = {e.value for e in EdgeCategories}
    assert "Association" in values
    assert "GeneToDiseaseAssociation" in values
    assert "ChemicalGeneInteractionAssociation" in values
    assert len(values) > 100  # The installed Biolink model defines 100+ association subclasses.


# --- ALLOWED_EDGE_FIELDS ------------------------------------------------------------------------


def test_allowed_edge_fields_covers_required_columns() -> None:
    """Core KGX / Translator edge columns remain allowed (not folded into supporting_text)."""
    required: list[str] = [
        "subject",
        "predicate",
        "object",
        "category",
        "knowledge_level",
        "agent_type",
        "qualifiers",
        "qualified_predicate",
        "primary_knowledge_source",
        "publications",
        "sources",
        "has_supporting_studies",
    ]
    for col in required:
        assert col in ALLOWED_EDGE_FIELDS, col


def test_retrieval_source_slots_are_not_edge_columns() -> None:
    """``upstream_resource_ids`` / ``source_record_urls`` belong to ``RetrievalSource``.

    Both have ``domain: retrieval source`` in the model, so emitting them flat on an
    association is an ``extra_forbidden`` error. They must reach output only nested
    inside a ``sources`` entry.
    """
    for slot in ("upstream_resource_ids", "source_record_urls"):
        assert slot not in ALLOWED_EDGE_FIELDS, slot
        assert slot in bm.RetrievalSource.model_fields, slot
        assert slot not in bm.Association.model_fields, slot


def test_allowed_edge_fields_includes_effect_annotations() -> None:
    """PR #1774 ``effect_size`` / ``effect_type`` are allowed edge columns (reach the final edges)."""
    assert "effect_size" in ALLOWED_EDGE_FIELDS
    assert "effect_type" in ALLOWED_EDGE_FIELDS


def test_allowed_edge_fields_excludes_approval_ids() -> None:
    """``approval_ids`` is no longer an override-allowed edge column.

    The curated pass-through carve-out was removed: Biolink declares no such slot, so the
    column is folded into ``supporting_text`` like any other unknown edge field instead of
    being exempted from model validation.
    """
    assert "approval_ids" not in TABLASERT_EDGE_EXTRAS
    assert "approval_ids" not in ALLOWED_EDGE_FIELDS


def test_allowed_edge_fields_includes_class_override_grants_without_global_extras() -> None:
    """Canonical class grants reach the family allow-list without becoming global extras.

    This protects the compatibility boundary: ``regulatory_approvals`` is emitted as a
    real edge column for the two pinned classes, but is neither an arbitrary global
    allow-list entry nor a ``TABLASERT_EDGE_EXTRAS`` carryover.
    """
    assert "regulatory_approvals" in ALLOWED_EDGE_FIELDS
    assert "regulatory_approvals" not in TABLASERT_EDGE_EXTRAS
    assert "regulatory_approvals" not in KNOWN_PENDING_EDGE_FIELDS


def test_supporting_case_ids_is_an_allow_listed_pending_extra() -> None:
    """``supporting_case_ids`` is a curated, build-internal edge extra.

    Allow-listed so ``fold_unknown_to_supporting_text`` never folds it into
    ``supporting_text``, and pending because no Biolink association class declares it --
    which is also why ``prune_to_class`` leaves the column alone without any
    ``CLASS_FIELD_OVERRIDES`` grant. The Rust dedup pass strips it before write, so
    ``validate_kgx`` and QC never see it.
    """
    assert "supporting_case_ids" in TABLASERT_EDGE_EXTRAS
    assert "supporting_case_ids" in ALLOWED_EDGE_FIELDS
    assert "supporting_case_ids" in KNOWN_PENDING_EDGE_FIELDS


def test_allowed_edge_fields_includes_subclass_only_slots() -> None:
    """Slots declared only by ``Association`` *subclasses* are still allowed columns.

    Deriving the allow-list from the base ``Association`` MRO alone silently demotes
    evidence slots such as ``clinical_approval_status`` into ``supporting_text``.
    """
    subclass_only: set[str] = set()
    for cls in vars(bm).values():
        if inspect.isclass(cls) and issubclass(cls, bm.Association):
            subclass_only |= set(cls.model_fields) - set(bm.Association.model_fields)
    expected_subclass_only = {"clinical_approval_status", "number_of_cases"}
    assert expected_subclass_only <= subclass_only
    assert expected_subclass_only <= set(ALLOWED_EDGE_FIELDS)
    allowed_subclass_only: set[str] = subclass_only & set(ALLOWED_EDGE_FIELDS)
    assert allowed_subclass_only
    for slot in allowed_subclass_only:
        assert slot not in bm.Association.model_fields, slot
        assert slot in ALLOWED_EDGE_FIELDS, slot


def test_disabled_edge_fields_are_never_emittable() -> None:
    """Tablassert-disabled fields stay excluded even if a future Biolink model attaches them."""
    assert "species_context_qualifier" in DISABLED_EDGE_FIELDS
    assert DISABLED_EDGE_FIELDS.isdisjoint(ALLOWED_EDGE_FIELDS)


def test_class_field_overrides_track_the_installed_model() -> None:
    """Every granted field must still be absent from its class and emittable elsewhere.

    Tripwire: the moment a biolink-model release attaches a granted slot to the class,
    this fails and the stale grant is removed from ``CLASS_FIELD_OVERRIDES`` (same
    philosophy as the ``UNSATISFIABLE_EDGE_FIELDS`` derivation guard). A field the
    family allow-list would strip anyway must never be granted. The canonical
    ``regulatory_approvals`` grant is intentionally present on exactly the two
    association classes that need it while the installed model catches up, alongside
    DAKP's sparse qualifier stack (``anatomical_context_qualifier``,
    ``sex_qualifier``, ``population_context_qualifier``, ``frequency_qualifier``,
    ``temporal_context_qualifier`` -- each satisfiable, none declared by the pinned
    classes). Deliberately excluded from the grants: ``species_context_qualifier``
    (disabled) plus ``temporal_interval_qualifier`` and ``severity_qualifier``
    (unsatisfiable -- a grant could never validate).
    """
    assert {
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
    } == CLASS_FIELD_OVERRIDES
    for class_name, fields in CLASS_FIELD_OVERRIDES.items():
        cls: type[Any] = association_class(f"biolink:{class_name}")
        assert issubclass(cls, bm.Association), class_name
        for field in fields:
            assert field not in class_fields(cls), (class_name, field)
            assert field in ALLOWED_EDGE_FIELDS, (class_name, field)


def test_validate_record_tolerates_class_field_override_grants() -> None:
    """Canonical grants validate on both targets but remain forbidden on an unrelated class.

    The grant is a deliberate, class-scoped step ahead of the pinned model, so its
    ``extra_forbidden`` must not surface on either intended target -- while the same
    field on an ungranted class stays a real defect through the installed-model boundary.
    """
    base: dict[str, Any] = {
        "id": "e1",
        "subject": "CHEBI:1",
        "predicate": "biolink:associated_with",
        "object": "MONDO:0005148",
        "knowledge_level": "statistical_association",
        "agent_type": "data_analysis_pipeline",
        "regulatory_approvals": ["FDA:1"],
    }
    for category in ("EntityToDiseaseAssociation", "EntityToPhenotypicFeatureAssociation"):
        record: dict[str, Any] = {**base, "category": [f"biolink:{category}"]}
        assert validate_record(record, edge=True) == []
    control: dict[str, Any] = {**base, "category": ["biolink:GeneToDiseaseAssociation"]}
    assert "regulatory_approvals: extra_forbidden" in validate_record(control, edge=True)
    unknown: dict[str, Any] = {**base, "category": ["biolink:EntityToDiseaseAssociation"], "not_a_biolink_field": "x"}
    assert "not_a_biolink_field: extra_forbidden" in validate_record(unknown, edge=True)


def test_allowed_edge_fields_excludes_unattached_qualifiers() -> None:
    """Qualifier slots attached to no Pydantic class are not emittable.

    ``Qualifiers`` is walked from the LinkML *slot* hierarchy, which includes abstract
    grouping slots (``process_qualifier``, ``aspect_qualifier``) that no class declares.
    Emitting one produces a record that can never validate.
    """
    assert "process_qualifier" in {q.value for q in Qualifiers}
    assert "process_qualifier" in UNSATISFIABLE_EDGE_FIELDS
    assert "process_qualifier" not in ALLOWED_EDGE_FIELDS


def test_allowed_edge_fields_covers_every_satisfiable_qualifier() -> None:
    """Every qualifier slot with a real home is an allowed edge column."""
    satisfiable: set[str] = {q.value for q in Qualifiers} - set(UNSATISFIABLE_EDGE_FIELDS) - set(DISABLED_EDGE_FIELDS)
    assert satisfiable <= set(ALLOWED_EDGE_FIELDS)


def test_unsatisfiable_fields_are_derived_not_hardcoded() -> None:
    """``UNSATISFIABLE_EDGE_FIELDS`` must reflect the *installed* model.

    ``biolink/biolink-model#1770`` attaches the ``supporting_study_*`` slots to root
    ``association``; when that ships they become ordinary edge columns. Nothing may
    hardcode either state, so assert the set is exactly "declared but unattached".
    """
    owned: set[str] = set()
    for cls in vars(bm).values():
        if inspect.isclass(cls) and cls.__module__ == bm.__name__:
            owned |= set(getattr(cls, "model_fields", {}))
    for field in UNSATISFIABLE_EDGE_FIELDS:
        assert field not in owned, field


def test_resolve_association_class_reconciles_predicate() -> None:
    """A category whose predicate enum forbids the predicate is demoted, not emitted."""
    # GeneToDiseaseAssociation permits only contributes_to / associated_with / affects.
    assert resolve_association_class("biolink:GeneToDiseaseAssociation", "biolink:associated_with") is bm.GeneToDiseaseAssociation
    assert resolve_association_class("biolink:GeneToDiseaseAssociation", "biolink:gene_associated_with_condition") is bm.Association


def test_numeric_slot_kind_matches_model_ranges() -> None:
    """P-values are floats in Biolink, so they must not be emitted as strings."""
    assert numeric_slot_kind("p_value") == "float"
    assert numeric_slot_kind("adjusted_p_value") == "float"
    assert numeric_slot_kind("study_size") == "int"
    assert numeric_slot_kind("effect_size") == "float"
    assert numeric_slot_kind("subject") is None


def test_known_pending_fields_are_derived_not_hardcoded() -> None:
    """``KNOWN_PENDING_EDGE_FIELDS`` must reflect the *installed* model.

    It is exactly "curated Tablassert extra that no Biolink association declares". When
    ``biolink/biolink-model#1774`` shipped (biolink-model 4.4.4), ``effect_size`` /
    ``effect_type`` became real ``Association`` fields and dropped out of the set with no
    code change -- so nothing may hardcode either state.
    """
    owned: set[str] = set()
    for cls in vars(bm).values():
        if inspect.isclass(cls) and inspect.isclass(bm.Association) and issubclass(cls, bm.Association):
            owned |= set(getattr(cls, "model_fields", {}))
    assert frozenset(TABLASERT_EDGE_EXTRAS) - owned == KNOWN_PENDING_EDGE_FIELDS
    # Today's state, asserted so the pending exemption is visibly scoped: approval_ids
    # left the set when its curated pass-through override was removed, while the effect
    # pair left it when the model caught up.
    assert "approval_ids" not in KNOWN_PENDING_EDGE_FIELDS
    assert {"effect_size", "effect_type"} & KNOWN_PENDING_EDGE_FIELDS == set()
    assert KNOWN_PENDING_EDGE_FIELDS <= ALLOWED_EDGE_FIELDS


def test_is_pending_problem_only_exempts_extra_forbidden_pending_fields() -> None:
    """The exemption is narrow: a deliberate extra Biolink has not declared, and nothing else."""
    assert not is_pending_problem("approval_ids: extra_forbidden")
    # Same field, a REAL failure -> not exempt either way.
    assert not is_pending_problem("approval_ids: missing")
    # A real installed-model slot -> extra_forbidden is a real failure.
    # ``effect_size`` is a real Association slot since biolink-model 4.4.4 -> never exempt.
    assert not is_pending_problem("effect_size: extra_forbidden")
    # A genuinely malformed value on a real slot -> never exempt.
    assert not is_pending_problem("p_value: float_parsing")
    assert not is_pending_problem("subject: string_type")


def test_legal_predicates_answers_the_authoring_question() -> None:
    """The inverse of ``resolve_association_class``: which predicates KEEP this class?"""
    gene_to_disease: frozenset[str] | None = legal_predicates("biolink:GeneToDiseaseAssociation")
    assert gene_to_disease is not None
    assert gene_to_disease == {"biolink:affects", "biolink:associated_with", "biolink:contributes_to"}
    # The 723,595-edge failure from the biolink fix: forbidden here, so it demotes.
    assert "biolink:gene_associated_with_condition" not in gene_to_disease
    assert resolve_association_class("biolink:GeneToDiseaseAssociation", "biolink:gene_associated_with_condition") is bm.Association
    # Association leaves `predicate` open -> nothing to constrain, nothing to demote to.
    assert legal_predicates("biolink:Association") is None


def test_validate_kgx_never_passes_a_missing_file(tmp_path: Path) -> None:
    """A typo'd path must not read as a clean bill of health (0/0 valid used to exit 0)."""
    report: dict[str, Any] = validate_kgx(tmp_path / "absent.nodes.ndjson", tmp_path / "absent.edges.ndjson")
    assert report["ok"] is False
    assert report["ok_excluding_pending"] is False
    assert report["nodes"]["missing"] is True
    assert report["edges"]["missing"] is True


def test_validate_kgx_accepts_resource_id_only_retrieval_sources(tmp_path: Path) -> None:
    """Validation aliases ``resource_id`` to the pinned model's inherited ``id`` in memory only."""
    nodes: Path = tmp_path / "n.ndjson"
    edges: Path = tmp_path / "e.ndjson"
    nodes.write_text(json.dumps({"id": "HGNC:11998", "name": "TP53", "category": ["biolink:Gene"]}) + "\n")
    edge: dict[str, Any] = {
        "id": "e1",
        "subject": "HGNC:11998",
        "predicate": "biolink:associated_with",
        "object": "MONDO:0008903",
        "category": ["biolink:Association"],
        "knowledge_level": "statistical_association",
        "agent_type": "data_analysis_pipeline",
        "sources": [{"resource_id": "infores:test", "resource_role": "primary_knowledge_source"}],
    }
    edges.write_text(json.dumps(edge) + "\n")

    report: dict[str, Any] = validate_kgx(nodes, edges)
    assert "id" not in json.loads(edges.read_text())["sources"][0]
    assert report["edges"]["valid"] == 1
    assert report["ok"] is True


def test_validate_kgx_separates_pending_extras_from_real_failures(tmp_path: Path) -> None:
    """``valid_excluding_pending`` forgives a deliberate extra; ``valid`` stays strict."""
    nodes: Path = tmp_path / "n.ndjson"
    edges: Path = tmp_path / "e.ndjson"
    nodes.write_text(json.dumps({"id": "HGNC:11998", "name": "TP53", "category": ["biolink:Gene"]}) + "\n")
    base: dict[str, Any] = {
        "subject": "HGNC:11998",
        "predicate": "biolink:associated_with",
        "object": "MONDO:0008903",
        "category": ["biolink:GeneToDiseaseAssociation"],
        "knowledge_level": "statistical_association",
        "agent_type": "data_analysis_pipeline",
    }
    edges.write_text(
        # approval_ids is no longer a curated extra (its override was removed), so this
        # edge fails strictly AND is not forgiven by the pending exemption either.
        json.dumps({**base, "id": "e1", "approval_ids": "011111|022222"})
        + "\n"
        # A real defect: p_value is typed float, so a non-numeric string can never validate.
        + json.dumps({**base, "id": "e2", "p_value": "not-a-number"})
        + "\n"
        # The output contract: format_numeric emits p-values as scientific-notation strings;
        # lax validation coerces them back to the float slot, so this record is fully valid.
        + json.dumps({**base, "id": "e3", "p_value": "1.0000e-03"})
        + "\n"
        # biolink-model 4.4.4 shipped PR #1774: effect_size / effect_type are real slots,
        # so this edge is STRICTLY valid -- no pending exemption involved.
        + json.dumps({**base, "id": "e4", "effect_size": 1.5, "effect_type": "odds_ratio"})
        + "\n"
    )
    report: dict[str, Any] = validate_kgx(nodes, edges)
    assert report["edges"]["total"] == 4
    assert report["edges"]["valid"] == 2  # strict: the scientific-notation and effect-pair edges pass
    assert report["edges"]["valid_excluding_pending"] == 2  # the approval_ids edge is a real failure now
    assert report["ok"] is False
    assert report["ok_excluding_pending"] is False  # the real defect still fails
    assert "approval_ids: extra_forbidden" in report["edges"]["problems"]
    assert "effect_size: extra_forbidden" not in report["edges"]["problems"]


def test_category_overrides_track_the_installed_model() -> None:
    """Every override name must still be invisible to the Entity-subclass scan.

    Tripwire: the moment a biolink-model release promotes an overridden mixin to a
    real ``Entity`` subclass -- so ``_entity_category_names`` collects it natively --
    this fails and the stale name is removed from ``CATEGORY_OVERRIDES`` (same
    philosophy as ``test_class_field_overrides_track_the_installed_model``). Each
    name must simultaneously remain nameable in the ``Categories`` enum, which is
    the whole point of the grant.
    """
    assert frozenset({"GenomicEntity"}) == CATEGORY_OVERRIDES
    known: set[str] = {category.value for category in Categories}
    for name in CATEGORY_OVERRIDES:
        mixin: Any = getattr(bm, name, None)
        assert mixin is not None, name
        assert not issubclass(mixin, bm.Entity), f"{name} is now an Entity subclass; drop the override"
        assert name in known, name
