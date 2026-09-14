from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from tablassert import models
from tablassert.biolink import Categories
from tablassert.enums import Comparisons, EncodingMethods, Repositories
from tablassert.errors import BiolinkRelocationWarning, UnpairedEffectAnnotationWarning
from tablassert.ingests import from_yaml, to_sections
from tablassert.models import (
    Annotation,
    Encoding,
    Excel,
    Graph,
    ManualProvenance,
    NodeEncoding,
    Provenance,
    Qualifier,
    Regex,
    Reindex,
    Section,
    Statement,
    Text,
)

# Repository root (parent of tests/) and the shipped docs examples, used to prove the
# deprecation scaffold stays silent across the real example corpus.
ROOT: Path = Path(__file__).resolve().parent.parent
EXAMPLES: Path = ROOT / "docs" / "examples"


def test_section_from_minimal_yaml(fixtures_path: Path) -> None:
    """valid minimal text section."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    section: Section = Section(**data)  # pyright: ignore
    assert section.source.kind == "text"
    assert section.statement.subject.encoding == "BRCA1"
    assert section.statement.object.encoding == "TP53"
    assert section.provenance.repo == "PMC"


def test_graph_rig_section_is_required_and_defaults_apply(rig_factory: Any) -> None:
    """the required rig: section validates, with RIG-schema defaults where honest."""
    graph: Graph = Graph(  # pyright: ignore
        name="TEST", version="1.0.0", tables=[Path("./table.yaml")], fullmap=Path("./fullmap"), rig=rig_factory()
    )
    assert graph.infores_id == "infores:test-kg"
    # Default ingest category reflects what Tablassert builds: knowledge from tables.
    assert graph.rig.ingest_info.ingest_categories == ["translator_knowledge_creator"]
    # The UI prefix is optional; compose_ui_explanation always keeps the default text.
    assert graph.rig.ui_explanation is None


def test_graph_rejects_missing_rig_section() -> None:
    """a graph without a rig: section cannot build a PR-worthy RIG and is rejected."""
    data: dict[str, Any] = {"name": "TEST", "version": "1.0.0", "tables": [Path("./table.yaml")], "fullmap": Path("./fullmap")}
    with pytest.raises(ValidationError):
        Graph.model_validate(data)


def test_graph_rejects_legacy_top_level_rig_keys(rig_factory: Any) -> None:
    """old top-level RIG fields are rejected with a migration pointer, never silently mapped."""
    base: dict[str, Any] = {"name": "TEST", "version": "1.0.0", "tables": [Path("./table.yaml")], "fullmap": Path("./fullmap"), "rig": rig_factory()}
    for legacy_key in ("description", "contributions", "ui_explanation", "infores"):
        data: dict[str, Any] = {**base, legacy_key: "legacy value"}
        with pytest.raises(ValidationError) as exc_info:
            Graph.model_validate(data)
        assert "rig-legacy-keys" in str(exc_info.value)
        assert "rig:" in str(exc_info.value)


def test_graph_rejects_removed_enrichment_databases(rig_factory: Any) -> None:
    """graph rejects removed enrichment databases."""
    data: dict[str, Any] = {
        "name": "TEST",
        "version": "1.0.0",
        "tables": [Path("./table.yaml")],
        "fullmap": Path("./fullmap"),
        "rig": rig_factory(),
        "pubmed_db": Path("./PubMed.db"),
        "pmc_db": Path("./PMCSuppCaptions.db"),
    }
    with pytest.raises(ValidationError):
        Graph.model_validate(data)


def test_graph_rejects_qc_and_log_keys(rig_factory: Any) -> None:
    """graph rejects QC and log keys (moved to `build-kg` CLI flags)."""
    data: dict[str, Any] = {
        "name": "TEST",
        "version": "1.0.0",
        "tables": [Path("./table.yaml")],
        "fullmap": Path("./fullmap"),
        "rig": rig_factory(),
        "qc": True,
        "log": True,
    }
    with pytest.raises(ValidationError):
        Graph.model_validate(data)


def test_excel_source_valid() -> None:
    """valid minimal excel section."""
    source: Excel = Excel(local=Path("./test.xlsx"), url=["https://example.com/test.xlsx"], kind="excel", sheet="Sheet1")  # pyright: ignore
    assert source.kind == "excel"
    assert source.sheet == "Sheet1"


def test_text_source_valid() -> None:
    """valid text source."""
    source: Text = Text(local=Path("./test.tsv"), url=["https://example.com/test.tsv"], kind="text", delimiter="\t")  # pyright: ignore
    assert source.kind == "text"
    assert source.delimiter == "\t"


def test_section_missing_source_raises(fixtures_path: Path) -> None:
    """invalid section missing required source."""
    data: Any = from_yaml(fixtures_path / "invalid_section_missing_source.yaml")
    with pytest.raises(ValidationError):
        Section(**data)


def test_encoding_value_method() -> None:
    """encoding with value method."""
    enc: Encoding = Encoding(method="value", encoding="BRCA1")  # pyright: ignore
    assert enc.method == "value"
    assert enc.encoding == "BRCA1"


def test_encoding_column_method() -> None:
    """encoding with column method."""
    enc: Encoding = Encoding(method="column", encoding="A")  # pyright: ignore
    assert enc.method == "column"
    assert enc.encoding == "A"


def test_encoding_with_optional_fields() -> None:
    """encoding with optional fields."""
    enc: Encoding = Encoding(  # pyright: ignore
        method="value", encoding="test", prefix="PREFIX:", suffix=":SUFFIX", fill="forward", explode_by=";"
    )
    assert enc.prefix == "PREFIX:"
    assert enc.suffix == ":SUFFIX"
    assert enc.fill == "forward"
    assert enc.explode_by == ";"


def test_encoding_with_regex() -> None:
    """encoding with regex."""
    enc: Encoding = Encoding(  # pyright: ignore
        method="value", encoding="test", regex=[{"pattern": r"\s+", "replacement": " "}, {"pattern": r"\.$", "replacement": ""}]
    )
    assert len(enc.regex) == 2  # pyright: ignore
    assert enc.regex[0].pattern == r"\s+"  # pyright: ignore


def test_encoding_with_remove() -> None:
    """encoding with remove patterns."""
    enc: Encoding = Encoding(method="value", encoding="test", remove=[r"\[\d+\]", r"\s+"])  # pyright: ignore
    assert len(enc.remove) == 2  # pyright: ignore


def test_encoding_with_transformations() -> None:
    """encoding with transformations."""
    enc: Encoding = Encoding(  # pyright: ignore
        method="value", encoding=2.0, transformations=[{"function": "pow", "arguments": ["values", 2]}]
    )
    assert len(enc.transformations) == 1  # pyright: ignore
    assert enc.transformations[0].function == "pow"  # pyright: ignore


def test_encoding_rejects_removed_list_method() -> None:
    """method: list is removed; every encoding class fails with the migration pointer."""
    with pytest.raises(ValidationError, match="was removed"):
        Encoding(method="list", encoding=["a"])  # pyright: ignore
    with pytest.raises(ValidationError, match="was removed"):
        Annotation(annotation="has_evidence", method="list", encoding=["a"])  # pyright: ignore
    with pytest.raises(ValidationError, match="was removed"):
        NodeEncoding(method="list", encoding=["a"])  # pyright: ignore
    with pytest.raises(ValidationError, match="was removed"):
        Qualifier(qualifier="object_direction_qualifier", method="list", encoding=["increased"])  # pyright: ignore


def test_encoding_rejects_a_list_encoding() -> None:
    """The encoding field is scalar-only now; a list value is rejected outright."""
    with pytest.raises(ValidationError):
        Encoding(method="value", encoding=["a"])  # pyright: ignore
    with pytest.raises(ValidationError):
        Encoding(method="column", encoding=["A"])  # pyright: ignore


def test_annotation_split_by_accepts_a_column_encoding() -> None:
    """split_by is the one multivalued encoding and rides a column encoding."""
    ann: Annotation = Annotation(annotation="has_evidence", method="column", encoding="D", split_by="|")  # pyright: ignore
    assert ann.split_by == "|"  # pyright: ignore


def test_annotation_split_by_requires_a_column_method() -> None:
    """split_by splits per-row text, so a literal value encoding rejects it."""
    with pytest.raises(ValidationError, match="requires `method: column`"):
        Annotation(annotation="has_evidence", method="value", encoding="a|b", split_by="|")  # pyright: ignore


def test_annotation_split_by_rejects_an_empty_separator() -> None:
    """An empty separator splits into characters -- the failure the JSON array prevents."""
    with pytest.raises(ValidationError, match="non-empty separator"):
        Annotation(annotation="has_evidence", method="column", encoding="D", split_by="")  # pyright: ignore


def _section_with(fixtures_path: Path, *annotations: dict[str, Any]) -> Section:
    """Validate the minimal fixture section carrying the given annotations.

    Args:
        fixtures_path: Directory holding the shipped test fixtures.
        annotations: Annotation dicts to attach to the section.

    Returns:
        The validated Section.
    """
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    data["annotations"] = list(annotations)
    return Section.model_validate(data)


def test_section_drops_an_unpaired_effect_size_with_a_warning(fixtures_path: Path) -> None:
    """A bare effect size no longer fails the section: it is dropped with a warning and the edge is kept.

    The build would have shipped the uninterpretable value anyway (0.85 of *what*?), so failing the
    whole section lost strictly more evidence than dropping the one annotation. The warning names the
    annotation and the section source so the author can find and pair it.
    """
    with pytest.warns(UnpairedEffectAnnotationWarning, match="Dropped unpaired `effect_size` annotation from section `test.tsv`"):
        section: Section = _section_with(fixtures_path, {"annotation": "effect_size", "method": "column", "encoding": "C"})
    assert section.annotations is None  # the ONLY annotation was the unpaired one, so none remain


@pytest.mark.parametrize("alias", ["odds ratio", "relationship_strength"])
def test_section_drops_an_unpaired_effect_size_alias_with_a_warning(fixtures_path: Path, alias: str) -> None:
    """Aliases coerced to ``effect_size`` are judged (and dropped) exactly like the canonical spelling.

    The clean phase renames these before the build sees them, so the pairing decision must judge the
    coerced target; the warning still echoes the author's spelling plus its target so it is actionable.

    Args:
        fixtures_path: Directory holding the shipped test fixtures.
        alias: Source spelling that coerces to ``effect_size`` (including the legacy name).
    """
    with pytest.warns(UnpairedEffectAnnotationWarning, match=rf"Dropped unpaired `{alias}` \(coerced to `effect_size`\)"):
        section: Section = _section_with(fixtures_path, {"annotation": alias, "method": "column", "encoding": "C"})
    assert section.annotations is None


def test_section_drops_an_unpaired_effect_type_with_a_warning(fixtures_path: Path) -> None:
    """The reverse direction: an ``effect_type`` without an ``effect_size`` is dropped with a warning.

    Biolink PR #1774 only populates an effect type alongside a numeric effect size (the build nulls
    it anyway), so the config-time drop changes nothing downstream -- it just stops bouncing the
    section and tells the author what happened.
    """
    with pytest.warns(UnpairedEffectAnnotationWarning, match="Dropped unpaired `effect_type` annotation from section `test.tsv`"):
        section: Section = _section_with(fixtures_path, {"annotation": "effect_type", "method": "value", "encoding": "spearmans_rho"})
    assert section.annotations is None


def test_section_drops_only_the_unpaired_half_and_keeps_the_rest(fixtures_path: Path) -> None:
    """The drop is surgical: unrelated annotations survive untouched alongside the missing sibling.

    Guards against the drop blanking the whole annotation list (which would silently lose the
    p-value evidence the section declared correctly).
    """
    with pytest.warns(UnpairedEffectAnnotationWarning):
        section: Section = _section_with(
            fixtures_path,
            {"annotation": "p_value", "method": "column", "encoding": "C"},
            {"annotation": "effect_size", "method": "column", "encoding": "D"},
        )
    assert section.annotations is not None
    assert [annotation.annotation for annotation in section.annotations] == ["p_value"]


@pytest.mark.parametrize(("size", "kind"), [("effect_size", "effect_type"), ("odds ratio", "effect type"), ("relationship_strength", "effect_type")])
def test_section_keeps_paired_effect_annotations_without_warning(fixtures_path: Path, size: str, kind: str) -> None:
    """Declared as a pair -- canonical or aliased, column or literal -- both annotations survive silently.

    Args:
        fixtures_path: Directory holding the shipped test fixtures.
        size: Spelling coercing to ``effect_size``.
        kind: Spelling coercing to ``effect_type``.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("error", UnpairedEffectAnnotationWarning)
        section: Section = _section_with(
            fixtures_path,
            {"annotation": size, "method": "column", "encoding": "C"},
            {"annotation": kind, "method": "value", "encoding": "spearmans_rho"},
        )
    assert section.annotations is not None
    assert [annotation.annotation for annotation in section.annotations] == [size, kind]


def test_section_without_effect_annotations_is_unaffected(fixtures_path: Path) -> None:
    """No effect annotation at all -- absent or unrelated -- is not a false positive for the drop."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", UnpairedEffectAnnotationWarning)
        assert Section.model_validate(from_yaml(fixtures_path / "minimal_section.yaml")).annotations is None
        section: Section = _section_with(fixtures_path, {"annotation": "p_value", "method": "column", "encoding": "C"})
    assert section.annotations is not None


def test_merged_section_drops_a_template_level_unpaired_effect_type(fixtures_path: Path) -> None:
    """The drop applies after ``template``/``sections`` expansion, so every entry path sees it.

    A constant ``effect_type`` on the template pairs with each section's own ``effect_size`` column
    because the lists are concatenated BEFORE validation; with no ``effect_size`` anywhere, the
    merged section must drop the template-level ``effect_type`` rather than keep a half the build
    would null.
    """
    raw: Any = from_yaml(fixtures_path / "minimal_section_with_sections.yaml")
    raw["template"]["annotations"] = [{"annotation": "effect_type", "method": "value", "encoding": "correlation_coefficient"}]
    sections: list[dict[str, Any]] = to_sections(raw, fixtures_path / "minimal_section_with_sections.yaml")  # pyright: ignore
    for merged in sections:
        merged.pop("config", None)
        with pytest.warns(UnpairedEffectAnnotationWarning, match="Dropped unpaired `effect_type`"):
            section: Section = Section.model_validate(merged)
        # The merged list was [effect_type (template), p_value (section)]; only the unpaired
        # half is dropped, so the section's own p_value survives untouched.
        assert section.annotations is not None
        assert [annotation.annotation for annotation in section.annotations] == ["p_value"]


def test_node_encoding_with_taxon() -> None:
    """NodeEncoding with taxon."""
    node: NodeEncoding = NodeEncoding(method="value", encoding="BRCA1", taxon=9606)  # pyright: ignore
    assert node.taxon == 9606


def test_node_encoding_defaults_to_human_taxon() -> None:
    """NodeEncoding defaults omitted taxon to Homo sapiens."""
    node: NodeEncoding = NodeEncoding(method="value", encoding="BRCA1")  # pyright: ignore
    assert node.taxon == 9606


def test_node_encoding_explicit_null_disables_taxon() -> None:
    """NodeEncoding preserves explicit null taxon to disable taxon filtering."""
    node: NodeEncoding = NodeEncoding(method="value", encoding="BRCA1", taxon=None)  # pyright: ignore
    assert node.taxon is None


def test_qualifier_rejects_disabled_species_context_qualifier() -> None:
    """species_context_qualifier is disabled and cannot be manually declared."""
    with pytest.raises(ValidationError) as exc_info:
        models.Qualifier(qualifier="species_context_qualifier", method="value", encoding="Homo sapiens")  # pyright: ignore
    assert "field-disabled" in str(exc_info.value)


def test_annotation_rejects_disabled_species_context_qualifier() -> None:
    """The disabled field cannot re-enter through the generic annotation entry point."""
    with pytest.raises(ValidationError) as exc_info:
        Annotation(annotation="species_context_qualifier", method="value", encoding="NCBITaxon:9606")  # pyright: ignore
    assert "field-disabled" in str(exc_info.value)


def test_qualifier_nullable_defaults_false() -> None:
    """nullable defaults to False so existing strict edge-drop behavior is unchanged."""
    qualifier: Qualifier = Qualifier(qualifier="disease_context_qualifier", method="column", encoding="A")  # pyright: ignore
    assert qualifier.nullable is False


def test_qualifier_nullable_accepted_on_column() -> None:
    """nullable: true is accepted for a method: column qualifier (the only meaningful case)."""
    qualifier: Qualifier = Qualifier(  # pyright: ignore
        qualifier="disease_context_qualifier", method="column", encoding="A", nullable=True
    )
    assert qualifier.nullable is True


def test_qualifier_nullable_rejected_on_literal() -> None:
    """nullable: true on a literal qualifier can never be null and is rejected at config time."""
    with pytest.raises(ValidationError) as exc_info:
        models.Qualifier(  # pyright: ignore
            qualifier="disease_context_qualifier", method="value", encoding="MONDO:0000001", nullable=True
        )
    assert "qualifier-nullable-literal" in str(exc_info.value)


def test_statement_rejects_duplicate_resolved_qualifiers() -> None:
    """Declaring the same CURIE-ranged qualifier twice is rejected at config time.

    Two entries emit two ResolveSpecs for the one column; the fullmap join drops
    ``<col>_two`` after the first pass, so the second crashes mid-build. Failing
    here turns that into an actionable config error.
    """
    with pytest.raises(ValidationError) as exc_info:
        Statement(  # pyright: ignore
            subject={"method": "value", "encoding": "A"},
            object={"method": "value", "encoding": "B"},
            qualifiers=[
                {"qualifier": "anatomical_context_qualifier", "method": "value", "encoding": "UBERON:0000061"},
                {"qualifier": "anatomical_context_qualifier", "method": "value", "encoding": "UBERON:0001557"},
            ],
        )
    assert "qualifier-duplicated" in str(exc_info.value)


def test_statement_rejects_duplicate_literal_qualifiers() -> None:
    """Duplicate enum-ranged literal qualifiers are rejected exactly like resolved ones.

    Literal qualifiers skip entity resolution, but they still produce one output
    column per key, so a repeated key corrupts the same join step.
    """
    with pytest.raises(ValidationError) as exc_info:
        Statement(  # pyright: ignore
            subject={"method": "value", "encoding": "A"},
            object={"method": "value", "encoding": "B"},
            qualifiers=[
                {"qualifier": "object_direction_qualifier", "method": "value", "encoding": "increased"},
                {"qualifier": "object_direction_qualifier", "method": "value", "encoding": "decreased"},
            ],
        )
    assert "qualifier-duplicated" in str(exc_info.value)


def test_statement_rejects_mixed_resolved_and_literal_same_key() -> None:
    """A column-driven and a literal entry under one key are still one key too many.

    Detection keys on the qualifier name alone, regardless of encoding method,
    because the clash happens at column naming, not at value sourcing.
    """
    with pytest.raises(ValidationError) as exc_info:
        Statement(  # pyright: ignore
            subject={"method": "value", "encoding": "A"},
            object={"method": "value", "encoding": "B"},
            qualifiers=[
                {"qualifier": "anatomical_context_qualifier", "method": "value", "encoding": "UBERON:0000061"},
                {"qualifier": "anatomical_context_qualifier", "method": "column", "encoding": "C"},
            ],
        )
    assert "qualifier-duplicated" in str(exc_info.value)


def test_statement_accepts_distinct_qualifier_keys() -> None:
    """Distinct keys — resolved and literal alike — remain valid after the duplicate guard."""
    stmt: Statement = Statement(  # pyright: ignore
        subject={"method": "value", "encoding": "A"},
        object={"method": "value", "encoding": "B"},
        qualifiers=[
            {"qualifier": "anatomical_context_qualifier", "method": "value", "encoding": "UBERON:0000061"},
            {"qualifier": "object_direction_qualifier", "method": "value", "encoding": "increased"},
        ],
    )
    assert stmt.qualifiers is not None
    assert len(stmt.qualifiers) == 2


def test_statement_accepts_single_empty_and_null_qualifiers() -> None:
    """A single qualifier, ``qualifiers: []``, and ``qualifiers: null`` all keep validating.

    The duplicate guard must not reject any currently-valid shape, including the
    empty list that downstream treats identically to None.
    """
    single: Statement = Statement(  # pyright: ignore
        subject={"method": "value", "encoding": "A"},
        object={"method": "value", "encoding": "B"},
        qualifiers=[{"qualifier": "disease_context_qualifier", "method": "value", "encoding": "MONDO:0005575"}],
    )
    assert single.qualifiers is not None
    assert len(single.qualifiers) == 1

    empty: Statement = Statement(subject={"method": "value", "encoding": "A"}, object={"method": "value", "encoding": "B"}, qualifiers=[])  # pyright: ignore
    assert empty.qualifiers == []

    null: Statement = Statement(subject={"method": "value", "encoding": "A"}, object={"method": "value", "encoding": "B"}, qualifiers=None)  # pyright: ignore
    assert null.qualifiers is None


def test_statement_accepts_category_override() -> None:
    """A per-object-category override of valid Categories -> EdgeCategories pairs validates."""
    stmt: Statement = Statement(  # pyright: ignore
        subject={"method": "value", "encoding": "A"},
        object={"method": "value", "encoding": "B"},
        predicate="associated_with",
        category_override={"Disease": "EntityToDiseaseAssociation", "PhenotypicFeature": "EntityToPhenotypicFeatureAssociation"},
    )
    assert stmt.category_override == {"Disease": "EntityToDiseaseAssociation", "PhenotypicFeature": "EntityToPhenotypicFeatureAssociation"}


def test_category_surfaces_accept_fullmap_mixin_categories() -> None:
    """``CATEGORY_OVERRIDES`` names are first-class on every category-keyed surface.

    ``GenomicEntity`` is a Biolink mixin the ``Categories`` enum's Entity-subclass scan
    cannot see, yet the fullmap assigns it to real terms -- so it must validate in
    ``avoid``/``prioritize`` and as a ``category_override`` key, or an allow-list can
    never exclude it and the row falls through to bare ``biolink:Association``.
    """
    node: NodeEncoding = NodeEncoding(method="value", encoding="A", avoid=["GenomicEntity"], prioritize=["GenomicEntity"])  # pyright: ignore
    assert "GenomicEntity" in (node.avoid or [])
    assert "GenomicEntity" in (node.prioritize or [])
    stmt: Statement = Statement(  # pyright: ignore
        subject={"method": "value", "encoding": "A"},
        object={"method": "value", "encoding": "B"},
        predicate="treats",
        category_override={"GenomicEntity": "EntityToDiseaseAssociation"},
    )
    assert stmt.category_override == {"GenomicEntity": "EntityToDiseaseAssociation"}


def test_statement_rejects_unknown_override_value() -> None:
    """Override values must be EdgeCategories members; arbitrary names fail at config time."""
    with pytest.raises(ValidationError):
        Statement(  # pyright: ignore
            subject={"method": "value", "encoding": "A"}, object={"method": "value", "encoding": "B"}, category_override={"Disease": "NotAClass"}
        )


def test_statement_rejects_non_association_override_value() -> None:
    """A node category is not an association class, even though it is a valid enum elsewhere."""
    with pytest.raises(ValidationError):
        Statement(  # pyright: ignore
            subject={"method": "value", "encoding": "A"}, object={"method": "value", "encoding": "B"}, category_override={"Disease": "Disease"}
        )


def test_statement_rejects_unknown_override_key() -> None:
    """Override keys must be Categories members (the resolved object category)."""
    with pytest.raises(ValidationError):
        Statement(  # pyright: ignore
            subject={"method": "value", "encoding": "A"},
            object={"method": "value", "encoding": "B"},
            category_override={"NotACategory": "EntityToDiseaseAssociation"},
        )


def test_statement_warns_when_predicate_demotes_an_override() -> None:
    """Pinning a class whose predicate slot rejects the section predicate warns, not fails.

    ``GeneToDiseaseAssociation`` restricts ``predicate`` to
    ``contributes_to|associated_with|affects``, so ``gene_associated_with_condition``
    walks the emitted category up the hierarchy -- dropping the subclass-only slots the
    author pinned the class for. The build still reconciles silently; the warning is
    what makes the demotion discoverable.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        Statement(  # pyright: ignore
            subject={"method": "value", "encoding": "A"},
            object={"method": "value", "encoding": "B"},
            predicate="gene_associated_with_condition",
            category_override={"Disease": "GeneToDiseaseAssociation"},
        )
    messages: list[str] = [str(w.message) for w in caught if issubclass(w.category, BiolinkRelocationWarning)]
    assert any("GeneToDiseaseAssociation" in m and "gene_associated_with_condition" in m for m in messages)


def test_statement_no_warning_when_override_accepts_predicate() -> None:
    """A pinned class that accepts the section predicate validates silently."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        Statement(  # pyright: ignore
            subject={"method": "value", "encoding": "A"},
            object={"method": "value", "encoding": "B"},
            predicate="associated_with",
            category_override={"Disease": "EntityToDiseaseAssociation"},
        )
    assert not [w for w in caught if issubclass(w.category, BiolinkRelocationWarning)]


def test_node_encoding_with_prioritize_avoid() -> None:
    """NodeEncoding with prioritize and avoid."""
    node: NodeEncoding = NodeEncoding(  # pyright: ignore
        method="value", encoding="BRCA1", prioritize=[Categories.GENE, Categories.PROTEIN], avoid=[Categories.DISEASE]
    )
    assert len(node.prioritize) == 2  # pyright: ignore
    assert len(node.avoid) == 1  # pyright: ignore


def test_node_encoding_exclude_fields_default_none() -> None:
    """NodeEncoding exclude_prefixes/exclude_regex default to None.

    US-M3: the CURIE-exclusion fields are purely additive; an untouched config must
    keep both as None so the resolution hot path stays byte-for-byte unchanged.
    """
    node: NodeEncoding = NodeEncoding(method="value", encoding="BRCA1")  # pyright: ignore
    assert node.exclude_prefixes is None
    assert node.exclude_regex is None


def test_node_encoding_exclude_regex_invalid_rejected() -> None:
    """Guard: every `exclude_regex` entry must be a polars-compatible regular expression.

    Catches an invalid CURIE-exclusion pattern at config time instead of deep inside a
    multi-hour build; reuses the `regex-bad-pattern` code from the `Regex.pattern` probe.
    """
    with pytest.raises(ValidationError) as exc_info:
        NodeEncoding(method=EncodingMethods.VALUE, encoding="x", exclude_regex=["("])  # pyright: ignore
    assert "regex-bad-pattern" in str(exc_info.value)


def test_node_encoding_exclude_regex_empty_pattern_rejected() -> None:
    """Guard: an empty or whitespace-only `exclude_regex` entry is rejected.

    An empty pattern compiles, but polars ``str.contains("")`` matches EVERY CURIE, so it
    would silently drop all resolution candidates (data loss). Fail loudly at config time
    with the same `regex-bad-pattern` code instead of emptying a build hours in.
    """
    for bad in ("", "   "):
        with pytest.raises(ValidationError) as exc_info:
            NodeEncoding(method=EncodingMethods.VALUE, encoding="x", exclude_regex=[bad])  # pyright: ignore
        assert "regex-bad-pattern" in str(exc_info.value)


def test_node_encoding_exclude_empty_lists_noop() -> None:
    """Empty exclude lists validate and are preserved verbatim.

    Downstream treats ``[]`` identically to ``None`` (a pure no-op), so the model must
    accept and round-trip empty lists rather than coercing or rejecting them.
    """
    node: NodeEncoding = NodeEncoding(  # pyright: ignore
        method="value", encoding="BRCA1", exclude_prefixes=[], exclude_regex=[]
    )
    assert node.exclude_prefixes == []
    assert node.exclude_regex == []


def test_statement_default_predicate() -> None:
    """statement with default predicate."""
    stmt: Statement = Statement(  # pyright: ignore
        subject={"method": "value", "encoding": "A"}, object={"method": "value", "encoding": "B"}
    )
    assert stmt.predicate == "related_to"


def test_statement_explicit_predicate() -> None:
    """statement with explicit predicate."""
    stmt: Statement = Statement(  # pyright: ignore
        subject={"method": "value", "encoding": "A"}, object={"method": "value", "encoding": "B"}, predicate="treats"
    )
    assert stmt.predicate == "treats"


def test_reindex_valid() -> None:
    """reindex valid construction."""
    ri: Reindex = Reindex(column="A", comparison="ne", comparator="")  # pyright: ignore
    assert ri.column == "A"
    assert ri.comparison == "ne"
    assert ri.comparator == ""


def test_reindex_numeric_comparator() -> None:
    """reindex with numeric comparator."""
    ri: Reindex = Reindex(column="B", comparison="gt", comparator=0)  # pyright: ignore
    assert ri.comparator == 0


def test_provenance_valid() -> None:
    """provenance valid construction."""
    p: Provenance = Provenance(  # pyright: ignore
        repo="PMC",  # pyright: ignore[reportArgumentType]
        publication="PMC0000000",
    )
    assert p.repo == "PMC"
    assert p.publication == "PMC0000000"
    assert p.knowledge_level == "statistical_association"
    assert p.agent_type == "data_analysis_pipeline"


def test_provenance_custom_knowledge_level_and_agent_type() -> None:
    """provenance accepts custom KL/AT values."""
    p: Provenance = Provenance(  # pyright: ignore
        repo="PMID",  # pyright: ignore[reportArgumentType]
        publication="12345678",
        knowledge_level="prediction",  # pyright: ignore[reportArgumentType]
        agent_type="computational_model",  # pyright: ignore[reportArgumentType]
    )
    assert p.knowledge_level == "prediction"
    assert p.agent_type == "computational_model"


def test_manual_provenance_validates_prefixes() -> None:
    """manual provenance accepts explicit upstream-infores/PMCID values for non-PMC sources."""
    override = ManualProvenance(  # pyright: ignore
        upstream_resource_ids=["infores:external-source"],
        publications=["PMCID:PMC1234567"],
        knowledge_level="knowledge_assertion",  # pyright: ignore[reportArgumentType]
        agent_type="manual_agent",  # pyright: ignore[reportArgumentType]
    )
    assert override.upstream_resource_ids == ["infores:external-source"]
    assert override.publications == ["PMCID:PMC1234567"]


def test_manual_provenance_rejects_unprefixed_values() -> None:
    """manual provenance rejects non-infores upstream sources and non-PMCID publications."""
    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(upstream_resource_ids=["external-source"])  # pyright: ignore
    assert "override-bad-upstream-infores" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(publications=["PMC1234567"])  # pyright: ignore
    assert "override-bad-publication" in str(exc_info.value)


def test_manual_provenance_accepts_upstream_source_record_urls() -> None:
    """per-upstream record URLs validate when keyed by a declared upstream infores."""
    override = ManualProvenance(  # pyright: ignore
        upstream_resource_ids=["infores:external-source"], upstream_source_record_urls={"infores:external-source": ["https://example.org/dataset"]}
    )
    assert [str(u) for u in (override.upstream_source_record_urls or {})["infores:external-source"]] == ["https://example.org/dataset"]


def test_manual_provenance_rejects_mismatched_upstream_url_keys() -> None:
    """URL mapping keys must be infores CURIEs declared in ``upstream_resource_ids``."""
    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(  # pyright: ignore
            upstream_resource_ids=["infores:external-source"], upstream_source_record_urls={"infores:other": ["https://example.org/dataset"]}
        )
    assert "override-bad-upstream-urls" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(  # pyright: ignore
            upstream_resource_ids=["infores:external-source"], upstream_source_record_urls={"external-source": ["https://example.org/dataset"]}
        )
    assert "override-bad-upstream-urls" in str(exc_info.value)


def test_manual_provenance_rejects_infores_key() -> None:
    """manual provenance no longer accepts a per-section ``infores`` key.

    The primary ``sources`` entry always derives from the graph-level
    infores; manual infores CURIEs belong in ``upstream_resource_ids``. A stray
    ``infores`` key is now an unknown field, and ``TablaBase`` forbids extras.
    """
    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(infores="infores:external-kg")  # pyright: ignore
    assert "infores" in str(exc_info.value)


def test_manual_provenance_accepts_explicit_sources_template() -> None:
    """an explicit ``sources`` template emits valid RetrievalSource entries verbatim."""
    override = ManualProvenance(  # pyright: ignore
        sources=[
            {
                "resource_id": "infores:multiomics-drugapprovals",
                "resource_role": "aggregator_knowledge_source",
                "upstream_resource_ids": ["infores:dailymed", "infores:faers"],
                "source_record_urls": ["https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id={edge_id}"],
            },
            {"resource_id": "infores:faers", "resource_role": "primary_knowledge_source"},
            {"resource_id": "infores:dailymed", "resource_role": "supporting_data_source"},
        ]
    )
    assert override.sources is not None
    assert [entry.resource_id for entry in override.sources] == ["infores:multiomics-drugapprovals", "infores:faers", "infores:dailymed"]
    assert override.sources[0].source_record_urls == ["https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id={edge_id}"]
    assert override.sources[1].upstream_resource_ids is None


def test_manual_provenance_sources_rejects_bad_role() -> None:
    """``sources.resource_role`` must be a Biolink ResourceRoleEnum value."""
    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(sources=[{"resource_id": "infores:external-source", "resource_role": "knowledge_source"}])  # pyright: ignore
    assert "override-bad-sources" in str(exc_info.value)


def test_manual_provenance_sources_rejects_non_infores_ids() -> None:
    """``sources`` resource_id and upstream entries must be infores CURIEs."""
    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(sources=[{"resource_id": "external-source", "resource_role": "primary_knowledge_source"}])  # pyright: ignore
    assert "override-bad-sources" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(  # pyright: ignore
            sources=[{"resource_id": "infores:external-source", "resource_role": "primary_knowledge_source", "upstream_resource_ids": ["dailymed"]}]
        )
    assert "override-bad-sources" in str(exc_info.value)


def test_manual_provenance_sources_rejects_non_url_record_urls() -> None:
    """``sources.source_record_urls`` entries must be http(s) URLs once ``{edge_id}`` is stripped."""
    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(  # pyright: ignore
            sources=[{"resource_id": "infores:external-source", "resource_role": "primary_knowledge_source", "source_record_urls": ["not-a-url"]}]
        )
    assert "override-bad-sources" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(  # pyright: ignore
            sources=[
                {
                    "resource_id": "infores:external-source",
                    "resource_role": "primary_knowledge_source",
                    "source_record_urls": ["ftp://example.org/{edge_id}"],
                }
            ]
        )
    assert "override-bad-sources" in str(exc_info.value)


def test_manual_provenance_sources_rejects_upstream_field_combinations() -> None:
    """``sources`` is mutually exclusive with the upstream fields it subsumes."""
    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(  # pyright: ignore
            sources=[{"resource_id": "infores:external-source", "resource_role": "primary_knowledge_source"}],
            upstream_resource_ids=["infores:upstream-source"],
        )
    assert "override-bad-sources" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(  # pyright: ignore
            sources=[{"resource_id": "infores:external-source", "resource_role": "primary_knowledge_source"}],
            upstream_source_record_urls={"infores:upstream-source": ["https://example.org/dataset"]},
        )
    assert "override-bad-sources" in str(exc_info.value)


def test_manual_provenance_sources_rejects_incoherent_templates() -> None:
    """``sources`` must be non-empty, deduplicated, and carry a primary/aggregator entry."""
    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(sources=[])  # pyright: ignore
    assert "override-bad-sources" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(  # pyright: ignore
            sources=[
                {"resource_id": "infores:external-source", "resource_role": "primary_knowledge_source"},
                {"resource_id": "infores:external-source", "resource_role": "supporting_data_source"},
            ]
        )
    assert "override-bad-sources" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        ManualProvenance(  # pyright: ignore
            sources=[
                {"resource_id": "infores:external-source", "resource_role": "supporting_data_source"},
                {"resource_id": "infores:other-source", "resource_role": "supporting_data_source"},
            ]
        )
    assert "override-bad-sources" in str(exc_info.value)


def test_provenance_override_replaces_publication_requirement() -> None:
    """publication is required unless manual provenance override is set."""
    p = Provenance(override={"upstream_resource_ids": ["infores:external-source"], "publications": ["PMCID:PMC1234567"]})  # pyright: ignore
    assert p.publication is None
    assert p.override is not None

    with pytest.raises(ValidationError) as exc_info:
        Provenance()  # pyright: ignore
    assert "provenance-missing-publication" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        Provenance(publication="PMC1234567", override={"upstream_resource_ids": ["infores:external-source"]})  # pyright: ignore
    assert "provenance-publication-and-override" in str(exc_info.value)


def test_graph_rig_infores_validates_infores_prefix(rig_factory: Any) -> None:
    """rig.source_info.infores_id must be an infores CURIE."""
    graph = Graph(  # pyright: ignore
        name="TEST", version="1.0.0", tables=[Path("./table.yaml")], fullmap=Path("./fullmap"), rig=rig_factory(infores_id="infores:external-kg")
    )
    assert graph.infores_id == "infores:external-kg"
    with pytest.raises(ValidationError) as exc_info:
        Graph(  # pyright: ignore
            name="TEST", version="1.0.0", tables=[Path("./table.yaml")], fullmap=Path("./fullmap"), rig=rig_factory(infores_id="external-kg")
        )
    assert "rig-bad-infores" in str(exc_info.value)


def test_annotation_valid() -> None:
    """annotation valid construction."""
    a: Annotation = Annotation(annotation="p_value", method="column", encoding="E")  # pyright: ignore
    assert a.annotation == "p_value"
    assert a.encoding == "E"


def test_annotation_case_is_canonicalized_for_regulatory_approvals() -> None:
    """Canonical class grants accept casing and whitespace through generic normalization."""
    for declared in ("regulatory_approvals", "REGULATORY_APPROVALS", " Regulatory_Approvals "):
        ann: Annotation = Annotation(annotation=declared, method="column", encoding="E")  # pyright: ignore
        assert ann.annotation == "regulatory_approvals"  # pyright: ignore


def test_section_rejects_extra_fields() -> None:
    """section rejects extra fields."""
    with pytest.raises(ValidationError):
        Section(
            source={"local": "./t.tsv", "url": ["https://example.com/t.tsv"], "kind": "text"},
            statement={"subject": {"method": "value", "encoding": "A"}, "object": {"method": "value", "encoding": "B"}},
            provenance={"repo": "PMC", "publication": "PMC000"},
            unknown_field="bad",  # pyright: ignore
        )


def test_section_with_row_slice() -> None:
    """section with row slice."""
    source: Text = Text(  # pyright: ignore
        local=Path("./test.tsv"), url=["https://example.com/test.tsv"], kind="text", row_slice=[2, "auto"]
    )
    assert source.row_slice == [2, "auto"]


def test_section_with_rows() -> None:
    """section with rows."""
    source: Text = Text(local=Path("./test.tsv"), url=["https://example.com/test.tsv"], kind="text", rows=[1, 2, 5])  # pyright: ignore
    assert source.rows == [1, 2, 5]


def test_source_url_accepts_multiple() -> None:
    """a source records every URL in order (multiple URLs per section)."""
    source: Text = Text(local=Path("./t.tsv"), url=["https://a.example.com/x", "https://b.example.com/y"], kind="text")  # pyright: ignore
    assert [str(u).rstrip("/") for u in source.url] == ["https://a.example.com/x", "https://b.example.com/y"]


def test_source_url_rejects_scalar() -> None:
    """`url` is a list; a legacy scalar URL is rejected."""
    with pytest.raises(ValidationError):
        Text.model_validate({"local": "./t.tsv", "url": "https://example.com/x.tsv", "kind": "text"})  # pyright: ignore


def test_source_url_rejects_empty() -> None:
    """an empty `url` list is rejected (at least one URL is required)."""
    with pytest.raises(ValidationError):
        Text(local=Path("./t.tsv"), url=[], kind="text")  # pyright: ignore


def test_section_rows_and_row_slice_accept_zero() -> None:
    """Guard: `rows` and `row_slice` are zero-based, so 0 is a valid index.

    WHY: both fields were typed ``PositiveInt``, which rejects 0 even though ``rows`` is
    documented as zero-based (example ``[0, 2, 5]``), the runtime ``pick()`` gathers by
    zero-based index, and ``crop()`` already types its bounds as ``NonNegativeInt``. A
    ``rows``/``row_slice`` starting at the first row (index 0) must validate, while a
    genuinely negative index is still rejected.
    """
    rows_source: Text = Text(local=Path("./t.tsv"), url=["https://example.com/t.tsv"], kind="text", rows=[0, 2, 5])  # pyright: ignore
    assert rows_source.rows == [0, 2, 5]

    slice_source: Text = Text(local=Path("./t.tsv"), url=["https://example.com/t.tsv"], kind="text", row_slice=[0, 50])  # pyright: ignore
    assert slice_source.row_slice == [0, 50]

    with pytest.raises(ValidationError):
        Text(local=Path("./t.tsv"), url=["https://example.com/t.tsv"], kind="text", rows=[-1, 2])  # pyright: ignore

    with pytest.raises(ValidationError):
        Text(local=Path("./t.tsv"), url=["https://example.com/t.tsv"], kind="text", row_slice=[-1, 50])  # pyright: ignore


def test_section_with_reindex() -> None:
    """section with reindex."""
    source: Text = Text(  # pyright: ignore
        local=Path("./test.tsv"), url=["https://example.com/test.tsv"], kind="text", reindex=[{"column": "A", "comparison": "ne", "comparator": ""}]
    )
    assert len(source.reindex) == 1  # pyright: ignore
    assert source.reindex[0].column == "A"  # pyright: ignore


def test_section_with_qualifiers() -> None:
    """section with qualifiers."""
    section: Section = Section(  # pyright: ignore
        source={"local": "./t.tsv", "url": ["https://example.com/t.tsv"], "kind": "text"},
        statement={
            "subject": {"method": "value", "encoding": "A"},
            "object": {"method": "value", "encoding": "B"},
            "qualifiers": [{"qualifier": "disease_context_qualifier", "method": "value", "encoding": "MONDO:0005575"}],
        },
        provenance={"repo": "PMC", "publication": "PMC000"},
    )
    assert len(section.statement.qualifiers) == 1  # pyright: ignore


def test_section_with_annotations() -> None:
    """section with annotations."""
    data: dict[str, Any] = {
        "source": {"local": "./t.tsv", "url": ["https://example.com/t.tsv"], "kind": "text"},
        "statement": {"subject": {"method": "value", "encoding": "A"}, "object": {"method": "value", "encoding": "B"}},
        "provenance": {"repo": "PMC", "publication": "PMC000"},
        "annotations": [
            {"annotation": "p_value", "method": "column", "encoding": "E"},
            {"annotation": "sample_size", "method": "value", "encoding": 28},
        ],
    }
    section: Section = Section.model_validate(data)  # pyright: ignore
    assert len(section.annotations) == 2  # pyright: ignore


def test_reindex_eq_with_non_string_comparator_rejected() -> None:
    """Guard: `eq`/`ne` reindex filters require a str comparator.

    Catches a misconfigured row filter at config time instead of deep inside a multi-hour build.
    """
    with pytest.raises(ValidationError) as exc_info:
        Reindex(column="A", comparison=Comparisons.EQ, comparator=5)
    assert "comparison-bad-comparator-type" in str(exc_info.value)


def test_reindex_ordering_with_non_numeric_comparator_rejected() -> None:
    """Guard: ordering comparisons (`gt`/`ge`/`lt`/`le`) require a numeric comparator.

    Catches a misconfigured row filter at config time instead of deep inside a multi-hour build.
    """
    with pytest.raises(ValidationError) as exc_info:
        Reindex(column="A", comparison=Comparisons.GT, comparator="x")
    assert "comparison-nonnumeric-comparator" in str(exc_info.value)


def test_source_with_both_rows_and_row_slice_rejected() -> None:
    """Guard: a source cannot declare both explicit `rows` and a `row_slice`.

    Catches an ambiguous row-selection config at config time instead of deep inside a multi-hour build.
    """
    with pytest.raises(ValidationError) as exc_info:
        Text(local=Path("./t.tsv"), url=["https://example.com/t.tsv"], kind="text", rows=[1], row_slice=[1, 5])  # pyright: ignore
    assert "config-rows-and-row-slice-conflict" in str(exc_info.value)


def test_regex_with_invalid_pattern_rejected() -> None:
    """Guard: a regex `pattern` must be a polars-compatible regular expression.

    Catches an invalid replacement pattern at config time instead of deep inside a multi-hour build.
    """
    with pytest.raises(ValidationError) as exc_info:
        Regex(pattern="(", replacement=" ")
    assert "regex-bad-pattern" in str(exc_info.value)


def test_regex_with_invalid_replacement_rejected() -> None:
    """Guard: a regex `replacement` must itself be a polars-compatible regular expression.

    Catches an invalid replacement value at config time instead of deep inside a multi-hour build.
    """
    with pytest.raises(ValidationError) as exc_info:
        Regex(pattern="ok", replacement="(")
    assert "regex-bad-replacement" in str(exc_info.value)


def test_encoding_column_method_with_non_column_encoding_rejected() -> None:
    """Guard: the `column` encoding method requires an Excel-style column name (A-ZZ).

    Catches a mistyped source column reference at config time instead of deep inside a multi-hour build.
    """
    with pytest.raises(ValidationError) as exc_info:
        Encoding(method=EncodingMethods.COLUMN, encoding="not_a_col_123")  # pyright: ignore
    assert "encoding-bad-excel-column" in str(exc_info.value)


def test_encoding_remove_with_invalid_regex_rejected() -> None:
    """Guard: every `remove` entry must be a polars-compatible regular expression.

    Catches an invalid text-cleaning pattern at config time instead of deep inside a multi-hour build.
    """
    with pytest.raises(ValidationError) as exc_info:
        Encoding(method=EncodingMethods.VALUE, encoding="x", remove=["("])  # pyright: ignore
    assert "encoding-bad-remove-entry" in str(exc_info.value)


def test_provenance_pmc_repo_with_non_pmc_publication_rejected() -> None:
    """Guard: a PubMed Central `repo` requires a publication id starting with `PMC`.

    Catches a mismatched provenance namespace at config time instead of deep inside a multi-hour build.
    """
    with pytest.raises(ValidationError) as exc_info:
        Provenance(repo=Repositories.PUBMED_CENTRAL, publication="12345")  # pyright: ignore
    assert "provenance-bad-pmc-id" in str(exc_info.value)


def test_no_deprecation_warnings_for_current_fixtures(fixtures_path: Path, recwarn: pytest.WarningsRecorder) -> None:
    """US-M4: an EMPTY DEPRECATED_KEYS registry makes the before-hook a silent no-op.

    Every shipped fixture and docs example must load and validate with ZERO UserWarnings,
    proving the deprecation scaffold never regresses today's corpus. Scoped to UserWarning so
    unrelated DeprecationWarnings (multiprocessing/polars) cannot interfere with the assertion,
    and excluding BiolinkRelocationWarning, which is not a deprecation: the tutorial's
    `study_size` is the supported way to record a study size (it lands on the inlined
    Study), and its relocation notice is asserted by its own test below.
    """
    # Single-section fixture validates directly.
    Section.model_validate(from_yaml(fixtures_path / "minimal_section.yaml"))

    # Template/sections configs expand via to_sections; the stamped `config` path is a
    # Tcode-only key, so drop it before validating the pure Section schema.
    for path in (fixtures_path / "minimal_section_with_sections.yaml", EXAMPLES / "tutorial-table.yaml"):
        raw: Any = from_yaml(path)
        sections: list[dict[str, Any]] = to_sections(raw, path)  # pyright: ignore
        for section in sections:
            section.pop("config", None)
            Section.model_validate(section)

    # Graph example validates directly.
    Graph.model_validate(from_yaml(EXAMPLES / "tutorial-graph.yaml"))

    assert [w for w in recwarn if issubclass(w.category, UserWarning) and not issubclass(w.category, BiolinkRelocationWarning)] == []


def test_annotation_warns_when_the_slot_cannot_reach_the_edge() -> None:
    """An annotation whose value is relocated says so; one that reaches the edge stays silent."""
    # Deprecated supporting-study spelling -> study-level metadata on the inlined Study.
    with pytest.warns(BiolinkRelocationWarning, match="study-level metadata"):
        Annotation.model_validate({"annotation": "supporting_study_size", "method": "column", "encoding": "D"})
    # An alias the clean-phase coercions rename to a Study metadata slot names the coerced target.
    with pytest.warns(BiolinkRelocationWarning, match="coerced to `study_size`"):
        Annotation.model_validate({"annotation": "sample size", "method": "column", "encoding": "F"})
    # The canonical Study metadata spelling relocates too -- it never rides the edge.
    with pytest.warns(BiolinkRelocationWarning, match="Study.study_cohort"):
        Annotation.model_validate({"annotation": "study_cohort", "method": "value", "encoding": "FINNGEN"})
    # Not an association slot at all, and no statistical coercion claims it -> folded into supporting_text.
    with pytest.warns(BiolinkRelocationWarning, match="folded into `supporting_text`"):
        Annotation.model_validate({"annotation": "overlap", "method": "column", "encoding": "E"})
    # Real association slots and aliases the coercions rename to canonical edge slots
    # (the pipeline emits those on the edge) are silent.
    with warnings.catch_warnings():
        warnings.simplefilter("error", BiolinkRelocationWarning)
        for name in (
            "p_value",
            "adjusted_p_value",
            "effect_size",
            "effect_type",
            "regulatory_approvals",
            "adjusted p value",
            "odds ratio",
            "q_value",
        ):
            Annotation.model_validate({"annotation": name, "method": "column", "encoding": "C"})


def test_deprecated_key_in_registry_warns_but_still_validates(monkeypatch: pytest.MonkeyPatch) -> None:
    """US-M4: a registered deprecated key soft-warns but never blocks validation.

    Monkeypatching a REAL current field name (`delimiter`) into DEPRECATED_KEYS makes the
    before-hook emit a UserWarning while `extra="forbid"` still accepts the key and the value
    round-trips intact -- isolating "warn, don't fail". monkeypatch restores the empty registry
    so no other test observes the temporary entry.
    """
    monkeypatch.setitem(models.DEPRECATED_KEYS, "delimiter", "delimiter is deprecated; use 'sep' instead")
    with pytest.warns(UserWarning, match="sep"):
        source: Text = Text(local=Path("./t.tsv"), url=["https://example.com/t.tsv"], kind="text", delimiter="\t")  # pyright: ignore
    assert source.delimiter == "\t"


def test_deprecated_hook_is_noop_for_non_dict_input(recwarn: pytest.WarningsRecorder) -> None:
    """US-M4: the before-hook passes non-mapping raw input through untouched.

    pydantic hands a mode="before" validator whatever raw value model_validate receives, so the
    hook must neither warn nor mutate non-dicts (None/list/instance); normal validation still
    rejects the bad shape, proving the scaffold only ever observes dict keys.
    """
    with pytest.raises(ValidationError):
        Section.model_validate(["not", "a", "mapping"])
    assert [w for w in recwarn if issubclass(w.category, UserWarning)] == []


def test_graph_uuid_fields_default_to_the_historic_namespace(rig_factory: Any) -> None:
    """an unset `uuid_fields` keeps the whole-record hash under the TABLASSERT domain."""
    graph: Graph = Graph(name="TEST", version="1.0.0", tables=[Path("./table.yaml")], fullmap=Path("./fullmap"), rig=rig_factory())
    assert graph.uuid_fields is None
    assert graph.uuid_domain is None
    # Byte-compatible with pre-16.0.0 output: default-configured graphs derive as before.
    assert graph.uuid_namespace == "TABLASSERT"


def test_graph_uuid_fields_namespace_on_the_graph_infores(rig_factory: Any) -> None:
    """declaring `uuid_fields` moves the domain onto the graph's own infores.

    Hashing a subset removes the accidental cross-graph uniqueness full-record hashing
    provided -- two graphs asserting one triple would otherwise mint one id.
    """
    graph: Graph = Graph(
        name="TEST",
        version="1.0.0",
        tables=[Path("./table.yaml")],
        fullmap=Path("./fullmap"),
        rig=rig_factory(),
        uuid_fields=["subject", "predicate", "object"],
    )
    assert graph.uuid_namespace == "infores:test-kg"


def test_graph_uuid_domain_overrides_the_derived_namespace(rig_factory: Any) -> None:
    """an explicit `uuid_domain` wins, so sharded graphs can share one id space."""
    base: dict[str, Any] = {"name": "TEST", "version": "1.0.0", "tables": [Path("./table.yaml")], "fullmap": Path("./fullmap"), "rig": rig_factory()}
    with_fields: Graph = Graph.model_validate({**base, "uuid_fields": ["subject"], "uuid_domain": "infores:shared"})
    assert with_fields.uuid_namespace == "infores:shared"
    # Meaningful on its own: renamespace a full-record hash without narrowing it.
    without_fields: Graph = Graph.model_validate({**base, "uuid_domain": "infores:shared"})
    assert without_fields.uuid_namespace == "infores:shared"


def test_graph_uuid_fields_canonicalize_casing(rig_factory: Any) -> None:
    """any casing canonicalizes onto the allow-listed spelling, as annotations do."""
    graph: Graph = Graph.model_validate(
        {
            "name": "TEST",
            "version": "1.0.0",
            "tables": [Path("./table.yaml")],
            "fullmap": Path("./fullmap"),
            "rig": rig_factory(),
            "uuid_fields": ["Subject", "PREDICATE", " object "],
        }
    )
    # The Rust deduper matches record keys exactly, so the stored spellings must be canonical.
    assert graph.uuid_fields == ["subject", "predicate", "object"]


@pytest.mark.parametrize(
    ("uuid_fields", "reason"),
    [
        ([], "empty"),
        (["subject", "subject"], "repeats"),
        (["subject", "id"], "may not contain `id`"),
        (["subject", "not_a_real_field"], "never emitted"),
    ],
)
def test_graph_rejects_uuid_fields_that_cannot_identify_an_edge(rig_factory: Any, uuid_fields: list[str], reason: str) -> None:
    """a `uuid_fields` list that cannot be a key is rejected at config time.

    Each of these would derive an id from nothing, from `id` itself, or from a field no
    edge carries -- collapsing every edge in the graph onto one identifier.
    """
    data: dict[str, Any] = {
        "name": "TEST",
        "version": "1.0.0",
        "tables": [Path("./table.yaml")],
        "fullmap": Path("./fullmap"),
        "rig": rig_factory(),
        "uuid_fields": uuid_fields,
    }
    with pytest.raises(ValidationError) as exc_info:
        Graph.model_validate(data)
    assert "uuid-bad-fields" in str(exc_info.value)
    assert reason in str(exc_info.value)


def test_graph_uuid_on_collision_defaults_to_error(rig_factory: Any) -> None:
    """an unset `uuid_on_collision` keeps the abort-on-divergence behavior."""
    graph: Graph = Graph(name="TEST", version="1.0.0", tables=[Path("./table.yaml")], fullmap=Path("./fullmap"), rig=rig_factory())
    assert graph.uuid_on_collision == "error"


def test_graph_uuid_on_collision_merge_is_accepted_with_uuid_fields(rig_factory: Any) -> None:
    """`merge` opts divergent same-id edges into folding instead of aborting."""
    graph: Graph = Graph(
        name="TEST",
        version="1.0.0",
        tables=[Path("./table.yaml")],
        fullmap=Path("./fullmap"),
        rig=rig_factory(),
        uuid_fields=["subject", "predicate", "object"],
        uuid_on_collision="merge",
    )
    assert graph.uuid_on_collision == "merge"


def test_graph_uuid_on_collision_merge_requires_uuid_fields(rig_factory: Any) -> None:
    """`merge` without `uuid_fields` is rejected at config time.

    Under the whole-record hash every field is an identity field, so divergent records
    can never share an id -- a merge policy would silently never fire.
    """
    data: dict[str, Any] = {
        "name": "TEST",
        "version": "1.0.0",
        "tables": [Path("./table.yaml")],
        "fullmap": Path("./fullmap"),
        "rig": rig_factory(),
        "uuid_on_collision": "merge",
    }
    with pytest.raises(ValidationError) as exc_info:
        Graph.model_validate(data)
    assert "uuid-merge-without-fields" in str(exc_info.value)


def test_graph_uuid_on_collision_rejects_unknown_values(rig_factory: Any) -> None:
    """only `error` and `merge` are meaningful policies; typos fail at config time."""
    data: dict[str, Any] = {
        "name": "TEST",
        "version": "1.0.0",
        "tables": [Path("./table.yaml")],
        "fullmap": Path("./fullmap"),
        "rig": rig_factory(),
        "uuid_fields": ["subject"],
        "uuid_on_collision": "merg",
    }
    with pytest.raises(ValidationError):
        Graph.model_validate(data)
