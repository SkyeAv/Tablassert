from __future__ import annotations

import re
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, NonNegativeInt, PositiveInt, field_validator, model_validator

from tablassert._lazy import LazyModule
from tablassert.biolink import (
    ALLOWED_EDGE_FIELDS,
    BIOLINK_VERSION,
    DISABLED_EDGE_FIELDS,
    ENUM_RANGED_QUALIFIERS,
    STUDY_METADATA_FIELDS,
    UNSATISFIABLE_EDGE_FIELDS,
    AgentTypes,
    Categories,
    EdgeCategories,
    KnowledgeLevels,
    Predicates,
    Qualifiers,
    resolve_association_class,
)
from tablassert.coerce import coerced_target
from tablassert.enums import (
    Comparisons,
    ContentCategories,
    DataFormats,
    EncodingMethods,
    Files,
    FillMethods,
    Functions,
    IngestCategories,
    ModelingCategories,
    ProvisionMechanisms,
    Repositories,
    SourceStatuses,
    Tokens,
)
from tablassert.errors import BiolinkRelocationWarning, TablassertErrorCodes, TablassertValidationError, UnpairedEffectAnnotationWarning

if TYPE_CHECKING:
    import polars as pl
else:
    pl = LazyModule("polars")


# Deprecated config keys -> guidance. EMPTY today: the before-hook below is a silent no-op until a
# future rename registers a key here to WARN on it. The hook only warns and returns the data
# unchanged — a renamed key that is no longer a valid field is STILL rejected by extra="forbid"
# unless the future hook also pops/translates it; registering a key here supplies the warning half only.
DEPRECATED_KEYS: dict[str, str] = {}


def _shown(name: str, target: str) -> str:
    """Render an annotation name for a message, naming its coerced target when it differs.

    Args:
        name: Raw annotation name as the author wrote it.
        target: Canonical name from ``coerced_target``.

    Returns:
        Backtick-quoted name, annotated with the coerced target when coercion renamed it.
    """
    return f"`{name}` (coerced to `{target}`)" if target != name else f"`{name}`"


def _section_source_label(source: Excel | Text) -> str:
    """Render a section's source for a diagnostic message: the local path, plus the sheet for Excel.

    Args:
        source: The section's validated source.

    Returns:
        A backtick-quoted local path, suffixed with the worksheet name when the source is an
        Excel file reading one.
    """
    label: str = f"`{source.local}`"
    if isinstance(source, Excel) and source.sheet is not None:
        label += f" (sheet `{source.sheet}`)"
    return label


class TablaBase(BaseModel):
    model_config: ConfigDict = ConfigDict(  # pyright: ignore
        str_strip_whitespace=False, validate_assignment=True, use_enum_values=True, extra="forbid", populate_by_name=True
    )

    @model_validator(mode="before")
    @classmethod
    def _warn_deprecated_keys(cls, data: Any) -> Any:
        """Soft-warn when a raw input key is registered in ``DEPRECATED_KEYS``.

        Pure observation: returns ``data`` unchanged and never rejects (``extra="forbid"``
        still governs truly-unknown keys). The registry is EMPTY today, so this is a silent
        no-op; a future field rename registers its old key here to warn instead of hard-fail.
        Note: with ``validate_assignment=True`` pydantic also re-runs this on attribute set,
        so a registered key warns on assignment too — inert while the registry is empty.
        """
        if isinstance(data, dict):
            for key in data:
                if key in DEPRECATED_KEYS:
                    warnings.warn(DEPRECATED_KEYS[key], UserWarning, stacklevel=2)
        return data


class Reindex(TablaBase):
    column: str = Field(..., pattern=r"^[A-Z]{1,3}$", description="Source column letters used for row filtering.", examples=["A", "AA"])
    comparison: Comparisons = Field(
        Comparisons.NE, description="Comparison operator used in reindex filtering.", examples=[Comparisons.NE, Comparisons.EQ, Comparisons.GT]
    )
    comparator: str | int | float = Field(..., description="Right-side value compared against the selected column.", examples=["N/A", 0, 1.5])

    @model_validator(mode="after")
    def comparison_datatypes(self: Self) -> Self:
        x: Comparisons = self.comparison
        y: str | int | float = self.comparator

        if x == Comparisons.NE or x == Comparisons.EQ:
            if not isinstance(y, str):
                raise TablassertValidationError(
                    f"`eq`/`ne` comparisons require a str comparator, got {type(y).__name__}.", code="comparison-bad-comparator-type"
                )
        else:
            if not isinstance(y, (int, float)):
                raise TablassertValidationError(
                    f"Comparisons other than `eq`/`ne` require a float or int comparator, got {type(y).__name__}.",
                    code="comparison-nonnumeric-comparator",
                )

        return self


class BaseSource(TablaBase):
    local: Path = Field(..., description="Local path to read from or download into.")
    url: list[HttpUrl] = Field(
        ...,
        min_length=1,
        description="One or more remote source URL(s) recorded as provenance; emitted in the edge `sources` list under the primary entry's `source_record_urls` list and in the RIG. When `provenance.override.upstream_source_record_urls` is set, these URLs serve the RIG only and the per-upstream mapping determines edge placement instead. Format-validated only; not fetched.",
    )

    rows: list[NonNegativeInt] | None = Field(None, description="Zero-based row indices kept after any row_slice crop.", examples=[[0, 2, 5]])
    row_slice: list[NonNegativeInt | Literal[Tokens.AUTO]] | None = Field(
        None,
        description="Two-value row bounds [start, stop]; each value can be an index or 'auto'.",
        examples=[[1, 50], [Tokens.AUTO, 100], [5, Tokens.AUTO]],
    )

    @model_validator(mode="after")
    def no_rows_and_slice(self: Self) -> Self:
        if self.rows and self.row_slice:
            raise TablassertValidationError(
                "Cannot specify both `rows` and `row_slice` in the same section.", code="config-rows-and-row-slice-conflict"
            )

        return self

    reindex: list[Reindex] | None = Field(
        None,
        description="Sequential row filters applied using source column values.",
        examples=[[{"column": "A", "comparison": Comparisons.NE, "comparator": ""}]],
    )


class Excel(BaseSource):
    kind: Literal[Files.EXCEL] = Field(Files.EXCEL, description="Source kind; must be 'excel'.")
    sheet: str | None = Field("Sheet1", description="Worksheet name to read from the workbook.")


class Text(BaseSource):
    kind: Literal[Files.TEXT] = Field(Files.TEXT, description="Source kind; must be 'text'.")
    delimiter: str | None = Field(",", description="Field delimiter for headerless text/CSV scanning.", examples=[",", "\t", "|"])


class Regex(TablaBase):
    pattern: int | float | str = Field(..., description="Regex pattern passed to string replacement.", examples=["\\s+", "\\.$"])

    @field_validator("pattern", mode="after")
    @classmethod
    def polars_compatible_pattern(cls, pattern: int | float | str) -> int | float | str:
        try:
            pl.Series([""]).str.contains(str(pattern))
        except Exception as e:
            raise TablassertValidationError(f"`pattern` must be a polars-compatible regex, got {pattern!r}: {e}", code="regex-bad-pattern") from e

        return pattern

    replacement: int | float | str = Field(..., description="Replacement value used when the pattern matches.", examples=[" ", "", 0])

    @field_validator("replacement", mode="after")
    @classmethod
    def polars_compatible_replacement(cls, replacement: int | float | str) -> int | float | str:
        try:
            pl.Series([""]).str.contains(str(replacement))
        except Exception as e:
            raise TablassertValidationError(
                f"`replacement` must be a polars-compatible regex, got {replacement!r}: {e}", code="regex-bad-replacement"
            ) from e

        return replacement


class Math(TablaBase):
    function: Functions = Field(..., description="Math function applied during numeric transformation.")
    arguments: list[Literal[Tokens.VALUES] | float | int] = Field(
        ..., description="Function arguments; use 'values' to inject the current value.", examples=[[Tokens.VALUES, 2], [-1, Tokens.VALUES]]
    )


class Encoding(TablaBase):
    method: EncodingMethods = Field(
        EncodingMethods.VALUE,
        description="Interpret `encoding` as a literal value or source column letters.",
        examples=[EncodingMethods.VALUE, EncodingMethods.COLUMN],
    )
    encoding: str | int | float = Field(..., description="Literal value or source column letters.", examples=["A", "BRCA1", 1.0])

    @model_validator(mode="before")
    @classmethod
    def reject_removed_list_method(cls, data: Any) -> Any:
        """Fail configs still declaring the removed ``method: list`` with a migration pointer.

        ``method: list`` (a literal list emitted as one fixed JSON array on every row)
        was removed: ``split_by`` on a ``method: column`` annotation is the one
        multivalued encoding now, and it covers the per-row case the literal never
        could. A bare pydantic enum error would only say the value is invalid, so
        this hook turns the stale config into the actionable coded error the
        migration needs.
        """
        if isinstance(data, dict) and data.get("method") == "list":
            raise TablassertValidationError(
                "`method: list` was removed; for a multivalued annotation use `method: column` with `split_by` "
                "to split each cell's delimited text into a JSON array (subject/object/qualifier nodes are single entities).",
                code="encoding-list-method-removed",
            )
        return data

    @model_validator(mode="after")
    def excel_style_columns(self: Self) -> Self:
        if self.method == EncodingMethods.COLUMN:
            x = self.encoding
            if not re.search(r"^[A-Z]{1,3}$", str(x)):
                raise TablassertValidationError(f"`encoding` must be an Excel-style column name (A-ZZ), got {x!r}.", code="encoding-bad-excel-column")

        return self

    regex: list[Regex] | None = Field(
        None,
        description="Ordered regex replacements applied to encoded text.",
        examples=[[{"pattern": "\\s+", "replacement": " "}, {"pattern": "\\.$", "replacement": ""}]],
    )
    fill: FillMethods | None = Field(
        None, description="Null fill strategy applied after value extraction.", examples=[FillMethods.FORWARD, FillMethods.ZERO]
    )
    remove: list[int | float | str] | None = Field(
        None, description="Regex patterns removed from text (replace with empty string).", examples=[["\\[\\d+\\]", "\\s+"]]
    )

    @field_validator("remove", mode="after")
    @classmethod
    def polars_compatible_replacement(cls, remove: list[int | float | str] | None) -> list[int | float | str] | None:
        if remove:
            for r in remove:
                try:
                    pl.Series([""]).str.contains(str(r))
                except Exception as e:
                    raise TablassertValidationError(
                        f"`remove` entries must be polars-compatible regular expressions, got {r!r}: {e}", code="encoding-bad-remove-entry"
                    ) from e

        return remove

    prefix: str | None = Field(None, description="String prepended to the encoded value.")
    suffix: str | None = Field(None, description="String appended to the encoded value.")
    explode_by: str | None = Field(None, description="Delimiter used to split a value into multiple rows.", examples=[";", "|"])
    transformations: list[Math] | None = Field(
        None,
        description="Ordered math operations applied to numeric values.",
        examples=[[{"function": Functions.POW, "arguments": [Tokens.VALUES, 2]}]],
    )


class NodeEncoding(Encoding):
    taxon: PositiveInt | None = Field(
        9606,
        description=(
            "NCBI taxon id used to constrain taxon-scoped entity resolution. Defaults to 9606 (Homo sapiens); "
            "set null to disable. Applies to all taxon-bearing categories (genes, diseases, phenotypes, proteins, etc.)."
        ),
        examples=[9606, 10090],
    )
    prioritize: list[Categories] | None = Field(
        None, description="Biolink categories ranked higher during entity resolution.", examples=[[Categories.GENE, Categories.PROTEIN]]
    )
    avoid: list[Categories] | None = Field(
        None, description="Biolink categories excluded during entity resolution.", examples=[[Categories.DISEASE, Categories.PHENOTYPIC_FEATURE]]
    )
    exclude_prefixes: list[str] | None = Field(
        None, description="CURIE namespace prefixes (text before the first ':') dropped during entity resolution.", examples=[["OMIM", "NCBIGene"]]
    )
    exclude_regex: list[str] | None = Field(
        None,
        description="Regex patterns; any resolved CURIE matching one is dropped during entity resolution (case-sensitive).",
        examples=[["^OMIM:\\d+$"]],
    )

    @field_validator("exclude_regex", mode="after")
    @classmethod
    def polars_compatible_exclude_regex(cls, exclude_regex: list[str] | None) -> list[str] | None:
        if exclude_regex:
            for pattern in exclude_regex:
                # An empty pattern compiles but str.contains("") matches EVERY CURIE, silently dropping
                # all candidates; reject empty/whitespace-only entries loudly at config time.
                if not str(pattern).strip():
                    raise TablassertValidationError(
                        f"`exclude_regex` entries must be non-empty patterns (an empty pattern matches every CURIE), got {pattern!r}.",
                        code="regex-bad-pattern",
                    )
                try:
                    pl.Series([""]).str.contains(str(pattern))
                except Exception as e:
                    raise TablassertValidationError(
                        f"`exclude_regex` entries must be polars-compatible regular expressions, got {pattern!r}: {e}", code="regex-bad-pattern"
                    ) from e

        return exclude_regex


class Qualifier(NodeEncoding):
    qualifier: Qualifiers = Field(
        ...,
        description="Qualifier predicate key used as the output qualifier column.",
        examples=[Qualifiers.OBJECT_DIRECTION_QUALIFIER, Qualifiers.SUBJECT_CONTEXT_QUALIFIER],
    )
    nullable: bool = Field(
        False,
        description=(
            "When True, a blank or unresolvable ``method: column`` cell keeps the edge and omits the "
            "qualifier for that row (the column stays null and the null-stripper drops the key); when "
            "False (default) such a row is dropped, exactly like an unresolved subject/object. Only "
            "meaningful for ``method: column`` — a literal qualifier can never be null."
        ),
    )

    @property
    def vocabulary(self: Self) -> frozenset[str] | None:
        """Closed value set for this qualifier, or ``None`` when it is CURIE-ranged."""
        return ENUM_RANGED_QUALIFIERS.get(str(self.qualifier))

    @property
    def resolved(self: Self) -> bool:
        """Whether this qualifier's values go through fullmap entity resolution.

        Only CURIE-ranged qualifiers are resolved. Enum-ranged ones carry a literal
        token from a closed Biolink vocabulary and must be passed through verbatim.
        """
        return self.vocabulary is None

    @model_validator(mode="after")
    def reject_disabled_qualifiers(self: Self) -> Self:
        """Reject qualifier fields that Tablassert has intentionally disabled.

        The disabled policy is separate from Biolink's current slot attachment: a
        dependency release must not make a field that Tablassert does not support
        silently configurable again.
        """
        field: str = str(self.qualifier)
        if field in DISABLED_EDGE_FIELDS:
            raise TablassertValidationError(
                f"{field} is disabled in Tablassert; it is neither derived nor accepted as a configured qualifier or annotation.",
                code="field-disabled",
            )
        return self

    @model_validator(mode="after")
    def reject_unusable_qualifiers(self: Self) -> Self:
        """Reject qualifier slots that no Biolink Pydantic class can hold.

        ``Qualifiers`` is derived from the LinkML *slot* hierarchy, which is strictly
        broader than the set of slots actually attached to a class. Emitting one of
        these produces an edge that can never validate, so fail at config time with a
        pointer rather than silently at ingest time.
        """
        if str(self.qualifier) in UNSATISFIABLE_EDGE_FIELDS:
            raise TablassertValidationError(
                f"{self.qualifier} is declared in the Biolink schema but attached to no association class "
                f"in biolink-model {BIOLINK_VERSION}, so it cannot be emitted on an edge. "
                "Use a concrete subtype of it, or record the value as an annotation.",
                code="qualifier-unsatisfiable",
            )
        return self

    @model_validator(mode="after")
    def enum_ranged_values_are_literals(self: Self) -> Self:
        """Validate literal values for enum-ranged qualifiers against their vocabulary.

        Qualifiers inherit :class:`NodeEncoding` and are therefore entity-resolved
        through the fullmap by default. That is right for CURIE-ranged qualifiers
        (``anatomical_context_qualifier`` -> ``UBERON:0001557``) and wrong for
        enum-ranged ones: ``object_direction_qualifier`` wants the token ``increased``,
        not the resolved CURIE ``UMLS:C0205217``.
        """
        vocabulary: frozenset[str] | None = self.vocabulary
        if vocabulary is None or self.method != EncodingMethods.VALUE:
            return self
        literal: str = str(self.encoding).strip()
        if literal not in vocabulary:
            preview: str = ", ".join(sorted(vocabulary)[:8])
            raise TablassertValidationError(
                f"{self.qualifier} has a closed Biolink vocabulary; got {literal!r}. Permitted values include: {preview}...",
                code="qualifier-bad-value",
            )
        return self

    @model_validator(mode="after")
    def reject_nullable_literal_qualifiers(self: Self) -> Self:
        """Reject ``nullable: true`` on literal qualifiers.

        ``nullable`` only has meaning for a ``method: column`` qualifier: a blank or
        unresolved cell keeps the edge and the qualifier is omitted for that row. A
        ``method: value`` qualifier is a config-time constant that can never be blank,
        so ``nullable`` would be dead config that misleads the reader. Fail loudly at
        config time instead (the removed ``method: list`` is already rejected upstream
        by :meth:`Encoding.reject_removed_list_method`).
        """
        if self.nullable and self.method != EncodingMethods.COLUMN:
            raise TablassertValidationError(
                "`nullable` only applies to `method: column` qualifiers; a literal qualifier can never be null.", code="qualifier-nullable-literal"
            )
        return self


class Statement(TablaBase):
    subject: NodeEncoding = Field(..., description="Subject node encoding and mapping configuration.")
    object: NodeEncoding = Field(..., description="Object node encoding and mapping configuration.")
    predicate: Predicates = Field(Predicates.RELATED_TO, description="Predicate connecting subject and object nodes.")
    qualifiers: list[Qualifier] | None = Field(None, description="Optional qualifier nodes attached to the statement.")
    category_override: dict[Categories, EdgeCategories] | None = Field(
        None,
        description=(
            "Optional per-object-category association-class override: maps a resolved object category (bare name, e.g. `Disease`) "
            "to the association category its rows carry (e.g. `EntityToDiseaseAssociation`), used in place of the derived "
            "(subject, object) pair lookup. Rows whose object category is absent from the map derive as before."
        ),
    )

    @model_validator(mode="after")
    def reject_duplicate_qualifiers(self: Self) -> Self:
        """Reject qualifier keys declared more than once in one statement.

        Every qualifier entry becomes its own ``ResolveSpec`` keyed by the column
        named after the qualifier key, and the fullmap join drops the ``<col>_two``
        working column right after the first resolve pass. A duplicated key thus
        makes the second pass crash on the already-dropped column deep into a
        multi-hour build, so fail at config time instead.
        """
        seen: set[str] = set()
        for q in self.qualifiers or []:
            key: str = str(q.qualifier)
            if key in seen:
                raise TablassertValidationError(
                    f"{key} is declared more than once in `qualifiers`; each qualifier key may appear only once per statement, "
                    "so remove or merge the duplicate entry.",
                    code="qualifier-duplicated",
                )
            seen.add(key)
        return self

    @model_validator(mode="after")
    def warn_when_an_override_is_demoted_by_the_predicate(self: Self) -> Self:
        """Warn when a pinned association class cannot carry the section predicate.

        A ``category_override`` value still passes through
        :func:`biolink.resolve_association_class` at build time: when the pinned class
        restricts its ``predicate`` slot and the section predicate is not in it, the
        emitted category silently walks up to an ancestor -- taking every subclass-only
        slot the author pinned the class *for* with it. The value is never lost (the
        reconciliation keeps the edge valid), so this is a warning, not an error.
        """
        for obj_category, pinned in (self.category_override or {}).items():
            # `use_enum_values` stores both sides as plain strings.
            resolved: str = resolve_association_class(f"biolink:{pinned}", f"biolink:{self.predicate}").__name__
            if resolved != pinned:
                warnings.warn(
                    f"category_override pins `{pinned}` for `{obj_category}` objects, but that class does not accept "
                    f"predicate `{self.predicate}`, so those rows are emitted as `{resolved}` instead -- any slots only "
                    f"`{pinned}` declares are pruned off the edge.",
                    BiolinkRelocationWarning,
                    stacklevel=2,
                )
        return self


def validate_infores_curie(value: str, code: TablassertErrorCodes) -> str:
    """Validate an ``infores:`` CURIE used for Biolink knowledge-source fields."""
    if not value.startswith("infores:"):
        raise TablassertValidationError(f"InfoRes values must start with `infores:`, got {value!r}.", code=code)
    return value


EDGE_ID_PLACEHOLDER: str = "{edge_id}"
"""Placeholder for the final edge id inside ``override.sources`` record URLs.

The edge ``id`` is a deterministic content hash computed during the final dedup
stage -- after subgraphs are written -- so a per-edge URL cannot embed it during
the table build. The literal placeholder is emitted as-is and resolved against
the deduplicated ``*.edges.ndjson`` in a post-dedup sweep.
"""

RESOURCE_ROLES: tuple[str, ...] = ("primary_knowledge_source", "aggregator_knowledge_source", "supporting_data_source")
"""Valid Biolink ``ResourceRoleEnum`` values for retrieval ``sources`` entries.

Kept as literals instead of importing ``ResourceRoleEnum`` from the generated
``biolink_model`` Pydantic classes so config validation stays cheap; any other
value fails KGX validation downstream.
"""


class SourceOverride(TablaBase):
    """One explicit retrieval-``sources`` entry template for manual provenance.

    Each entry becomes one Biolink ``RetrievalSource`` struct on the edge's
    ``sources`` list, replacing the derived primary/upstream emission entirely.
    """

    resource_id: str = Field(description="Infores CURIE of this retrieval source entry.", examples=["infores:my-source"])
    resource_role: str = Field(
        description="Biolink ResourceRoleEnum value for this entry.",
        examples=["primary_knowledge_source", "aggregator_knowledge_source", "supporting_data_source"],
    )
    upstream_resource_ids: list[str] | None = Field(
        None, description="Upstream infores CURIEs carried by this entry.", examples=[["infores:my-upstream"]]
    )
    source_record_urls: list[str] | None = Field(
        None,
        description=f"Source record URLs carried by this entry; `{EDGE_ID_PLACEHOLDER}` is replaced with the final edge id after dedup.",
        examples=[["https://example.org/edge?id={edge_id}"]],
    )

    @field_validator("resource_id", mode="after")
    @classmethod
    def infores_resource_id(cls, value: str) -> str:
        return validate_infores_curie(value, "override-bad-sources")

    @field_validator("resource_role", mode="after")
    @classmethod
    def biolink_resource_role(cls, value: str) -> str:
        if value not in RESOURCE_ROLES:
            raise TablassertValidationError(f"`resource_role` must be one of {list(RESOURCE_ROLES)}, got {value!r}.", code="override-bad-sources")
        return value

    @field_validator("upstream_resource_ids", mode="after")
    @classmethod
    def infores_upstream_resource_ids(cls, values: list[str] | None) -> list[str] | None:
        if values is None:
            return None
        for value in values:
            validate_infores_curie(value, "override-bad-sources")
        return values

    @field_validator("source_record_urls", mode="after")
    @classmethod
    def url_or_edge_id_template(cls, values: list[str] | None) -> list[str] | None:
        # Typed as plain str (not HttpUrl) so the `{edge_id}` placeholder survives
        # validation; after removing placeholder occurrences the rest must still be
        # an absolute http(s) URL.
        if values is None:
            return None
        for value in values:
            stripped: str = value.replace(EDGE_ID_PLACEHOLDER, "")
            if not stripped.startswith(("https://", "http://")):
                raise TablassertValidationError(
                    f"`source_record_urls` entries must be http(s) URLs (optionally containing `{EDGE_ID_PLACEHOLDER}`), got {value!r}.",
                    code="override-bad-sources",
                )
        return values


class ManualProvenance(TablaBase):
    """Manually-specified provenance for non-PMID/PMC source graphs.

    When present under :class:`Provenance`, these values replace the legacy
    repo/publication-derived provenance while keeping the same KL/AT defaults.
    The primary ``sources`` entry (``resource_role: primary_knowledge_source``)
    derives from the graph-level ``rig.source_info.infores_id`` unless an
    explicit ``sources`` template is given; manual infores CURIEs otherwise
    belong in ``upstream_resource_ids``.
    """

    sources: list[SourceOverride] | None = Field(
        None,
        description="Explicit retrieval-`sources` entry templates replacing the derived primary/upstream emission entirely; mutually exclusive with `upstream_resource_ids` and `upstream_source_record_urls`, which it subsumes.",
    )
    upstream_resource_ids: list[str] = Field(
        default_factory=list,
        description="Manual upstream source infores CURIEs emitted instead of the repo-derived source map; the sanctioned place for manual infores.",
        examples=[["infores:my-upstream"]],
    )
    upstream_source_record_urls: dict[str, list[HttpUrl]] | None = Field(
        None,
        description="Per-upstream source record URLs keyed by infores CURIE; every key must appear in `upstream_resource_ids`. When set, the section's `source.url` values are NOT emitted on the primary `sources` entry (RIG use only) — each listed upstream supporting entry carries its own `source_record_urls` instead.",
        examples=[{"infores:my-upstream": ["https://example.org/dataset"]}],
    )
    publications: list[str] | None = Field(
        None,
        description="Publication CURIEs emitted verbatim; currently PMCID CURIEs are required for manual provenance.",
        examples=[["PMCID:PMC1234567"]],
    )
    knowledge_level: KnowledgeLevels = Field(
        KnowledgeLevels.STATISTICAL_ASSOCIATION, description="Biolink KL/AT knowledge level applied to produced edges."
    )
    agent_type: AgentTypes = Field(AgentTypes.DATA_ANALYSIS_PIPELINE, description="Biolink KL/AT agent type responsible for produced edges.")

    @field_validator("upstream_resource_ids", mode="after")
    @classmethod
    def upstream_infores_curies(cls, values: list[str]) -> list[str]:
        for value in values:
            validate_infores_curie(value, "override-bad-upstream-infores")
        return values

    @field_validator("publications", mode="after")
    @classmethod
    def pmcid_publications(cls, values: list[str] | None) -> list[str] | None:
        if values is None:
            return None
        for value in values:
            if not value.startswith("PMCID:"):
                raise TablassertValidationError(
                    f"Manual provenance publications must start with `PMCID:`, got {value!r}.", code="override-bad-publication"
                )
        return values

    @model_validator(mode="after")
    def sources_template_is_coherent(self: Self) -> Self:
        if self.sources is None:
            return self
        if self.upstream_resource_ids or self.upstream_source_record_urls is not None:
            raise TablassertValidationError(
                "`sources` is mutually exclusive with `upstream_resource_ids` and `upstream_source_record_urls`; the explicit template subsumes both.",
                code="override-bad-sources",
            )
        if not self.sources:
            raise TablassertValidationError("`sources` must contain at least one entry when set.", code="override-bad-sources")
        resource_ids: list[str] = [entry.resource_id for entry in self.sources]
        if len(set(resource_ids)) != len(resource_ids):
            raise TablassertValidationError("`sources` entries must have unique `resource_id` values.", code="override-bad-sources")
        if not any(entry.resource_role in ("primary_knowledge_source", "aggregator_knowledge_source") for entry in self.sources):
            raise TablassertValidationError(
                "`sources` must include at least one `primary_knowledge_source` or `aggregator_knowledge_source` entry.", code="override-bad-sources"
            )
        return self

    @model_validator(mode="after")
    def upstream_urls_match_resource_ids(self: Self) -> Self:
        if self.upstream_source_record_urls is None:
            return self
        for key in self.upstream_source_record_urls:
            validate_infores_curie(key, "override-bad-upstream-urls")
        unknown = sorted(set(self.upstream_source_record_urls) - set(self.upstream_resource_ids))
        if unknown:
            raise TablassertValidationError(
                f"`upstream_source_record_urls` keys must appear in `upstream_resource_ids`, got {unknown}.", code="override-bad-upstream-urls"
            )
        return self


class Provenance(TablaBase):
    repo: Repositories = Field(Repositories.PUBMED_CENTRAL, description="Publication identifier namespace prefix.")
    publication: str | None = Field(
        None,
        description="Repository-local publication id appended as repo:publication; required unless manual provenance override is set.",
        examples=["12345678", "PMC1234567"],
    )
    knowledge_level: KnowledgeLevels = Field(
        KnowledgeLevels.STATISTICAL_ASSOCIATION, description="Biolink KL/AT knowledge level applied to produced edges."
    )
    agent_type: AgentTypes = Field(AgentTypes.DATA_ANALYSIS_PIPELINE, description="Biolink KL/AT agent type responsible for produced edges.")
    override: ManualProvenance | None = Field(
        None, description="Manual provenance values for non-PMID/PMC sources; when set, replace repo/publication-derived provenance."
    )

    @model_validator(mode="after")
    def is_valid_pmc_id(self: Self) -> Self:
        if self.override is not None:
            if self.publication is not None:
                raise TablassertValidationError(
                    "Specify either `publication` or `override`, not both, in provenance.", code="provenance-publication-and-override"
                )
            return self
        if self.publication is None:
            raise TablassertValidationError("`publication` is required unless provenance.override is set.", code="provenance-missing-publication")
        if self.repo == Repositories.PUBMED_CENTRAL and not re.search(r"^PMC\d+", self.publication):
            raise TablassertValidationError(
                f"PubMed Central publications must start with `PMC`, got {self.publication!r}.", code="provenance-bad-pmc-id"
            )

        return self


class Annotation(Encoding):
    annotation: str = Field(..., description="Output column name that receives this encoded annotation.", examples=["p_value", "cohort"])
    split_by: str | None = Field(
        None, description="Separator splitting each cell of a `method: column` annotation into a real JSON array.", examples=["|", ";"]
    )

    @field_validator("annotation", mode="after")
    @classmethod
    def clean_annotation(cls, annotation: str) -> str:
        cleaned: str = annotation.strip()
        lowered: str = cleaned.lower()
        # Allow-listed slots may carry uppercase: any casing the author declares
        # canonicalizes onto the allow-listed spelling verbatim, so the emitted edge
        # field preserves its case instead of being lowercased into an unknown name.
        return next((field for field in ALLOWED_EDGE_FIELDS if field.lower() == lowered), lowered)

    @model_validator(mode="after")
    def reject_disabled_annotations(self: Self) -> Self:
        """Reject disabled edge fields even when they arrive through ``annotations``."""
        if self.annotation in DISABLED_EDGE_FIELDS:
            raise TablassertValidationError(
                f"{self.annotation} is disabled in Tablassert; it is neither derived nor accepted as a configured qualifier or annotation.",
                code="field-disabled",
            )
        return self

    @model_validator(mode="after")
    def split_by_requires_a_column(self) -> Self:
        """Enforce that ``split_by`` carries a real separator and a ``method: column`` encoding.

        ``split_by`` is the one multivalued encoding: it turns each cell's own
        delimited text into a real JSON array, per row. A ``value`` encoding is a
        scalar literal with no per-row text to split, so it rejects ``split_by``.
        """
        if self.split_by is None:
            return self
        if self.method != EncodingMethods.COLUMN:
            raise TablassertValidationError(
                "`split_by` splits a column's per-row text and requires `method: column`.", code="annotation-split-by-requires-column"
            )
        if not self.split_by:
            # An empty separator splits into individual characters -- exactly the
            # character-walking failure the JSON array exists to prevent.
            raise TablassertValidationError("`split_by` must be a non-empty separator.", code="annotation-split-by-empty")

        return self

    @model_validator(mode="after")
    def warn_when_the_slot_cannot_reach_the_edge(self) -> Self:
        # Deliberately a WARNING, not an error like the Qualifier guards above: the value is never
        # lost, only relocated, and rejecting would break configs that build correctly today. Silence
        # is the real problem -- an author asking for `supporting_study_size` has no way to discover
        # that Biolink attaches it to no class and the pipeline rerouted it.
        name: str = str(self.annotation)
        # Judge the coerced target, not the raw alias: the clean-phase column coercions rename
        # statistical aliases to their canonical slot before any relocation runs, so
        # `adjusted p value` reaches the edge as `adjusted_p_value` and warning on the alias
        # is a false positive.
        target: str = coerced_target(name)
        shown: str = _shown(name, target)
        if target in STUDY_METADATA_FIELDS:
            warnings.warn(
                f"{shown} is study-level metadata, so its value is carried on the inlined supporting study as "
                f"`Study.{target}` rather than emitted on the edge itself (biolink-model {BIOLINK_VERSION} "
                "replaced the deprecated `supporting_study_*` association slots with Study node properties). "
                "Use a slot a Biolink association declares (e.g. `p_value`, `adjusted_p_value`) if you need "
                "it on the edge itself.",
                BiolinkRelocationWarning,
                stacklevel=2,
            )
        elif target in UNSATISFIABLE_EDGE_FIELDS:
            warnings.warn(
                f"{shown} is declared in biolink-model {BIOLINK_VERSION} but attached to no association class, "
                "so it cannot be emitted on an edge; its value is routed onto the inlined supporting study "
                "instead. Use a slot a Biolink association declares (e.g. `p_value`, `adjusted_p_value`) if you "
                "need it on the edge itself.",
                BiolinkRelocationWarning,
                stacklevel=2,
            )
        elif target not in ALLOWED_EDGE_FIELDS:
            warnings.warn(
                f"{shown} is not a Biolink association slot, so it is folded into `supporting_text` as a "
                f'"{name}: <value>" string rather than emitted as its own edge field.',
                BiolinkRelocationWarning,
                stacklevel=2,
            )
        return self


class Section(TablaBase):
    """Pydantic section model and coercion target for a single table configuration."""

    source: Excel | Text = Field(..., description="Input source definition for reading tabular rows.")
    statement: Statement = Field(..., description="Subject-object statement mapping for this section.")
    provenance: Provenance = Field(..., description="Provenance metadata applied to all produced edges.")
    annotations: list[Annotation] | None = Field(None, description="Optional extra encoded columns added to each row.")

    @model_validator(mode="after")
    def drop_unpaired_effect_annotations(self) -> Self:
        """Drop an ``effect_size`` / ``effect_type`` annotation declared without its sibling.

        A bare effect size is uninterpretable -- 0.85 of *what*, an odds ratio or a Spearman
        rho? -- and Biolink PR #1774 only populates ``effect_type`` alongside a numeric
        ``effect_size``, so the build nulls an unpaired type outright (see
        ``coerce.coerce_effect_type_columns``). Neither half carries evidence alone, so
        validation DROPS the unpaired annotation with an :class:`UnpairedEffectAnnotationWarning`
        naming what was dropped and from where, and keeps the section and its edges: the value was
        going to be discarded by the build anyway, and failing the section would lose the rest of
        the table with it.

        Lives on ``Section`` rather than ``Annotation`` because an annotation cannot see its
        siblings. Validation runs after ``ingests.to_sections`` expands ``template``/``sections``,
        so a constant declared once on the template pairs with every section's own column -- and
        an unpaired half declared on the template is dropped from every expanded section.

        Names are judged by their coerced target, not their raw spelling, so the aliases the
        clean phase renames (``odds ratio``, the legacy ``relationship_strength``) are seen
        exactly as the build sees them.
        """
        annotations: list[Annotation] = self.annotations or []
        targets: set[str] = {coerced_target(str(annotation.annotation)) for annotation in annotations}
        has_size: bool = "effect_size" in targets
        has_kind: bool = "effect_type" in targets
        if has_size == has_kind:
            return self  # paired or both absent -- nothing to drop

        dropped_target: str = "effect_size" if has_size else "effect_type"
        sibling: str = "effect_type" if has_size else "effect_size"
        where: str = _section_source_label(self.source)
        kept: list[Annotation] = []
        for annotation in annotations:
            name: str = str(annotation.annotation)
            if coerced_target(name) == dropped_target:
                reason: str = (
                    "a bare effect size is uninterpretable without a sibling `effect_type` (0.85 of what -- an odds ratio or a Spearman rho?)"
                    if dropped_target == "effect_size"
                    else "Biolink PR #1774 only populates an effect type alongside a numeric effect size, so the build nulls it"
                )
                warnings.warn(
                    f"Dropped unpaired {_shown(name, dropped_target)} annotation from section {where}: {reason}. "
                    f"Add a sibling `{sibling}` annotation to keep it; the section and its edges are kept regardless.",
                    UnpairedEffectAnnotationWarning,
                    stacklevel=2,
                )
            else:
                kept.append(annotation)
        # validate_assignment re-runs this validator on the set; the kept list is balanced, so it returns at the guard above.
        self.annotations = kept or None
        return self


# --- Resource Ingest Guide configuration ----------------------------------- #
# The `rig:` graph-config section. The nested shape mirrors the released RIG
# schema (biolink/resource-ingest-guide-schema) so a generated `.RIG.yaml` is
# always schema-shaped; fields the generator derives at build time (target
# edge/node summaries, generated artifact file entries) live outside these
# models and are composed by `tablassert.rig`.


def _contains_url(value: str) -> bool:
    """Whether a free-text RIG location string carries an http(s) or file URL.

    ``file://`` is accepted because unpublished/local artifact bases (agent
    measurement builds) are honest local URIs; a PR to upstream should swap them
    for public https locations, which the docs call out.
    """
    return "http://" in value or "https://" in value or "file://" in value


class RIGTermsOfUseInfo(TablaBase):
    """Terms-of-use / license assessment for the ingested source.

    Mirrors the RIG schema's ``TermsOfUseInformation``. At least one field must
    carry a real assessment -- an all-empty object is rejected so a generated
    RIG can never ship without a documented terms position.
    """

    terms_of_use_url: str | None = Field(
        None, description="URL of the source's terms-of-use or license page.", examples=["https://ctdbase.org/about/legal.jsp"]
    )
    terms_of_use_description: str | None = Field(None, description="Free-text summary of the source's terms of use.")
    license_name: str | None = Field(None, description="Name of an established license used by the source.", examples=["CC BY 4.0"])
    license_url: str | None = Field(None, description="URL of the established license.", examples=["https://creativecommons.org/licenses/by/4.0/"])

    @model_validator(mode="after")
    def non_empty_assessment(self: Self) -> Self:
        for url in (self.terms_of_use_url, self.license_url):
            if url is not None and not _contains_url(str(url)):
                raise TablassertValidationError(f"RIG terms-of-use URLs must be http(s) URLs, got {url!r}.", code="rig-terms-empty")
        if not any(
            v is not None and str(v).strip() for v in (self.terms_of_use_url, self.terms_of_use_description, self.license_name, self.license_url)
        ):
            raise TablassertValidationError(
                "rig.source_info.terms_of_use_info needs at least one of terms_of_use_url, terms_of_use_description, "
                "license_name, or license_url; assess the upstream source terms before building a PR-worthy RIG.",
                code="rig-terms-empty",
            )
        return self


class RIGSourceInfo(TablaBase):
    """``source_info`` section of the generated RIG."""

    infores_id: str = Field(
        ...,
        description="Infores CURIE of the source this graph ingests (also the default edge primary_knowledge_source).",
        examples=["infores:my-kg"],
    )
    name: str | None = Field(None, description="Human-readable name of the source.")
    description: str | None = Field(None, description="Description of the source, its scope, and how its knowledge is produced.")
    citations: list[str] | None = Field(None, description="Citations (PMIDs, DOIs, URLs, or free text) describing the source.")
    terms_of_use_info: RIGTermsOfUseInfo = Field(..., description="Terms-of-use / license assessment for the source.")
    data_access_locations: list[str] = Field(
        ...,
        min_length=1,
        description="Where the upstream source data can be accessed; one or more entries, each containing an http(s) or file URL.",
        examples=[["Source downloads - https://example.org/downloads/"]],
    )
    data_provision_mechanisms: list[ProvisionMechanisms] | None = Field(None, description="How the source distributes its data.")
    data_formats: list[DataFormats] | None = Field(None, description="Serialization formats of the source data.")
    data_versioning_and_releases: str | None = Field(None, description="How the source versions and releases its data.")
    source_status: SourceStatuses = Field(..., description="Maintenance status of the source.")
    additional_notes: list[str] | None = Field(None, description="Additional source notes not captured by dedicated fields.")

    @field_validator("infores_id", mode="after")
    @classmethod
    def infores_curie(cls, value: str) -> str:
        return validate_infores_curie(value, "rig-bad-infores")

    @field_validator("data_access_locations", mode="after")
    @classmethod
    def locations_carry_urls(cls, values: list[str]) -> list[str]:
        for value in values:
            if not str(value).strip() or not _contains_url(str(value)):
                raise TablassertValidationError(
                    f"rig.source_info.data_access_locations entries must each contain an http(s) or file URL, got {value!r}.",
                    code="rig-bad-access-location",
                )
        return values


class RIGRelevantFile(TablaBase):
    """One ``relevant_files`` entry (RIG schema ``RelevantFiles``)."""

    file_name: str = Field(..., min_length=1, description="Name of the file (or endpoint/table).")
    location: str = Field(..., description="URL where the file was accessed.")
    description: str | None = Field(None, description="Brief description of the file's content and utility.")

    @field_validator("location", mode="after")
    @classmethod
    def location_is_url(cls, value: str) -> str:
        if not _contains_url(str(value)):
            raise TablassertValidationError(
                f"RIG relevant-file locations must be http(s) or file URLs, got {value!r}.", code="rig-bad-access-location"
            )
        return value


class RIGIncludedContent(TablaBase):
    """One ``included_content`` entry (RIG schema ``IncludedContent``)."""

    file_name: str = Field(..., min_length=1, description="Name of the file content is included from.")
    included_records: str = Field(..., min_length=1, description="Description of the record types included in the ingest.")
    fields_used: str | None = Field(None, description="Source fields that are part of or inform the ingest.")


class RIGFilteredContent(TablaBase):
    """One ``filtered_content`` entry (RIG schema ``FilteredContent``)."""

    file_name: str = Field(..., min_length=1, description="Name of the file content was filtered from.")
    filtered_records: str = Field(..., min_length=1, description="Description of the excluded record types.")
    rationale: str = Field(..., min_length=1, description="Rationale for excluding the indicated content.")


class RIGFutureContentConsideration(TablaBase):
    """One ingest-level ``future_considerations`` entry."""

    category: ContentCategories = Field(..., description="Graph representation the considered content maps to.")
    consideration: str = Field(..., min_length=1, description="What additional content should be considered and why.")
    relevant_files: str | None = Field(None, description="Source file(s) providing the considered content.")


class RIGFutureModelingConsideration(TablaBase):
    """One target-level ``future_considerations`` entry."""

    category: ModelingCategories | None = Field(None, description="General category of the modeling consideration.")
    consideration: str = Field(..., min_length=1, description="The modeling change to consider, and why.")


class RIGIngestInfo(TablaBase):
    """``ingest_info`` section of the generated RIG.

    ``utility`` and ``scope`` are semantic claims about the graph and must be
    authored, not invented. ``relevant_files`` and ``included_content`` entries
    for the GENERATED KGX artifacts are composed by the RIG generator from
    ``artifact_base_url`` / ``artifact_base_path`` plus the observed graph; entries
    listed here are preserved alongside them (use them for upstream source files).
    """

    ingest_categories: list[IngestCategories] = Field(
        default_factory=lambda: [IngestCategories.TRANSLATOR_KNOWLEDGE_CREATOR],
        min_length=1,
        description="Type of source being ingested, from the ingesting system's perspective.",
    )
    utility: str = Field(..., min_length=1, description="Why the source was ingested and its utility for Translator use cases.")
    scope: str = Field(..., min_length=1, description="High-level narrative of the knowledge included and excluded in this ingest.")
    relevant_files: list[RIGRelevantFile] | None = Field(None, description="Upstream source files in scope for the ingest.")
    included_content: list[RIGIncludedContent] | None = Field(None, description="Record types included from the relevant files.")
    filtered_content: list[RIGFilteredContent] | None = Field(None, description="Record types filtered out, and why.")
    future_considerations: list[RIGFutureContentConsideration] | None = Field(None, description="Content additions/changes for future iterations.")
    additional_notes: list[str] | None = Field(None, description="Additional ingest notes not captured by dedicated fields.")


class RIGSupportingDataSourceInfo(TablaBase):
    """One ``supporting_data_source_info`` entry for data-derived graphs."""

    infores_id: str = Field(..., description="Infores CURIE of the upstream supporting data source.")
    name: str | None = Field(None, description="Human-readable name of the supporting data source.")
    description: str | None = Field(None, description="Brief description of the supporting data source.")
    terms_of_use_info: RIGTermsOfUseInfo = Field(..., description="Terms-of-use assessment for the supporting data source.")
    relevant_files: list[RIGRelevantFile] = Field(..., min_length=1, description="Source files (or endpoints) supplying the supporting data.")

    @field_validator("infores_id", mode="after")
    @classmethod
    def infores_curie(cls, value: str) -> str:
        return validate_infores_curie(value, "rig-bad-infores")


class RIGTargetInfoExtras(TablaBase):
    """Target-level notes; edge/node type summaries are always generated."""

    future_considerations: list[RIGFutureModelingConsideration] | None = Field(None, description="Modeling changes to consider in future iterations.")
    additional_notes: list[str] | None = Field(None, description="Additional mapping/modeling notes.")


class RIGProvenanceInfo(TablaBase):
    """``provenance_info`` section of the generated RIG."""

    contributions: list[str] = Field(
        ..., min_length=1, description='Contributor statements: who contributed and how (e.g. "Name - code author, data modeling").'
    )
    artifacts: list[str] | None = Field(None, description="Links/descriptions of external provenance artifacts (tickets, surveys, repos).")


class RIGConfig(TablaBase):
    """The required ``rig:`` graph-config section driving `.RIG.yaml` generation.

    Carries every human-authored RIG fact. The generator derives only the
    mechanical pieces (generated artifact file entries, observed target
    summaries) and validates the composed document before writing it, so no
    invalid or placeholder-laden RIG is ever emitted.
    """

    name: str | None = Field(None, description="Human-readable RIG name; defaults to '<graph name> v<version> Resource Ingest Guide'.")
    supporting_data_source_info: list[RIGSupportingDataSourceInfo] | None = Field(
        None, description="Upstream data sources a data-derived graph derives its knowledge from."
    )
    source_info: RIGSourceInfo = Field(..., description="Information about the source of the ingest.")
    ingest_info: RIGIngestInfo = Field(..., description="Rationale and scope of the ingest, including included/excluded content.")
    target_info: RIGTargetInfoExtras | None = Field(None, description="Optional target-level future considerations and notes.")
    ui_explanation: str | None = Field(
        None,
        description=(
            "Optional per-edge-type UI explanation prefix. The built-in Tablassert explanation is ALWAYS appended "
            "after it, so the generated text is this value followed by the default provenance explanation."
        ),
    )
    source_files: list[str] | None = Field(
        None,
        description=(
            "Upstream source file names/URLs listed as `source_files` on every generated `edge_type_info` entry. "
            "Authoritative config value; never scraped from edge `source_record_urls`."
        ),
    )
    provenance_info: RIGProvenanceInfo = Field(..., description="Who contributed to the ingest and how.")
    artifact_base_url: str = Field(
        ...,
        description="Public URL prefix for the generated KGX artifacts; each `.nodes.ndjson`/`.edges.ndjson` name is appended to build RIG file locations.",
        examples=["https://example.org/translator-ingests/my-kg"],
    )
    artifact_base_path: Path = Field(
        ...,
        description="Local output directory the generated KGX artifacts are written to; each artifact name is appended for the output path cross-check.",
        examples=["./published/translator-ingests/my-kg"],
    )

    @field_validator("artifact_base_url", mode="after")
    @classmethod
    def artifact_url_is_url(cls, value: str) -> str:
        text: str = str(value).strip()
        if not text.startswith(("http://", "https://", "file://")):
            raise TablassertValidationError(
                f"rig.artifact_base_url must start with http://, https://, or file://, got {value!r}.", code="rig-bad-artifact-url"
            )
        return text.rstrip("/")


DEFAULT_RIG_CONTRIBUTIONS: list[str] = ["Tablassert: KGX and RIG generation"]
DEFAULT_RIG_UI_EXPLANATION: str = (
    "Source Tablassert data provides assertions derived from configured tabular records. "
    "The source record used to create this Translator edge was transformed into a "
    "Biolink association using the subject, predicate, object, provenance, and "
    "annotation mappings declared in Tablassert's Table Configuration."
)


def default_rig_contributions() -> list[str]:
    return DEFAULT_RIG_CONTRIBUTIONS.copy()


#: Top-level graph keys that moved under `rig:`. Their presence is a hard error:
#: silently mapping them would hide which of the two spellings a config means, and a
#: half-migrated graph would build a RIG missing fields the author thinks they set.
LEGACY_RIG_KEYS: dict[str, str] = {
    "description": "rig.source_info.description",
    "contributions": "rig.provenance_info.contributions",
    "ui_explanation": "rig.ui_explanation",
    "infores": "rig.source_info.infores_id",
}


class Graph(TablaBase):
    """Pydantic graph configuration model."""

    name: str = Field(..., description="Graph name written into output metadata.")
    version: str = Field(..., description="Graph version label.")
    tables: list[Path] = Field(..., description="Paths to table YAML files included in this graph.", examples=[["tables/tutorial-table.yaml"]])
    fullmap: Path = Field(..., description="Base fullmap directory or fullmap redb file for entity resolution.", examples=[".fullmap"])
    rig: RIGConfig = Field(..., description="Resource Ingest Guide metadata emitted as <name>_<version>.RIG.yaml.")
    uuid_fields: list[str] | None = Field(
        default=None,
        description="Edge fields that constitute edge identity; only these feed the derived edge `id`, so an attribute-only change leaves it alone. Unset hashes the whole record.",
        examples=[["subject", "predicate", "object", "publications", "has_supporting_studies"]],
    )
    uuid_domain: str | None = Field(
        default=None,
        description="Explicit UUID namespace. Defaults to `rig.source_info.infores_id` when `uuid_fields` is set, `TABLASSERT` otherwise. Set it only when graphs must deliberately share an id space.",
        examples=["infores:multiomicskg"],
    )
    qc_similarity_threshold: float = Field(default=0.5, ge=0.0, le=1.0, description="SapBERT cosine threshold for QC.")
    qc_fuzzy_ratio_threshold: float = Field(default=70.0, ge=0.0, le=100.0, description="RapidFuzz ratio threshold for QC.")
    qc_fuzzy_partial_threshold: float = Field(default=80.0, ge=0.0, le=100.0, description="RapidFuzz token threshold for QC.")
    qc_aliases: dict[str, str] = Field(default_factory=dict, description="QC source-text aliases applied before similarity scoring.")
    uuid_on_collision: Literal["error", "merge"] = Field(
        default="error",
        description="What to do when two different edges derive one id under `uuid_fields`: `error` aborts the build with `uuid-fields-not-a-key`; `merge` folds the records into one edge (list fields unioned and sorted, scalars first-wins). Requires `uuid_fields`, and buffers one full record per unique id, which is why it is opt-in.",
    )

    @model_validator(mode="after")
    def validate_uuid_fields(self: Self) -> Self:
        """Reject a `uuid_fields` list that cannot identify an edge.

        Every entry must be a real emittable edge field, or the id would silently derive
        from nothing and every edge in the graph would collide. `id` itself is rejected
        because it is the value being derived. Casing follows `Annotation.clean_annotation`
        so `Subject` and `subject` both work and mixed-case Biolink slots survive. Also
        rejects `uuid_on_collision: merge` without `uuid_fields`: under the whole-record
        hash no two different records can share an id, so there is nothing to merge.
        """
        if self.uuid_fields is None:
            if self.uuid_on_collision == "merge":
                raise TablassertValidationError(
                    "`uuid_on_collision: merge` requires `uuid_fields`. With the whole-record hash every field is an identity field, so two different records can never derive one id and there is nothing to merge.",
                    code="uuid-merge-without-fields",
                )
            return self
        if not self.uuid_fields:
            raise TablassertValidationError(
                "`uuid_fields` was given as an empty list. Omit the key entirely to hash the whole record, or name the fields that identify an edge.",
                code="uuid-bad-fields",
            )
        canonical: list[str] = []
        for field in self.uuid_fields:
            lowered: str = field.strip().lower()
            canonical.append(next((allowed for allowed in ALLOWED_EDGE_FIELDS if allowed.lower() == lowered), lowered))
        if len(set(canonical)) != len(canonical):
            duplicated: str = ", ".join(sorted({f for f in canonical if canonical.count(f) > 1}))
            raise TablassertValidationError(f"`uuid_fields` repeats: {duplicated}. Each field may appear once.", code="uuid-bad-fields")
        if "id" in canonical:
            raise TablassertValidationError("`uuid_fields` may not contain `id`: the edge id is what these fields derive.", code="uuid-bad-fields")
        unknown: list[str] = sorted(f for f in canonical if f not in ALLOWED_EDGE_FIELDS)
        if unknown:
            raise TablassertValidationError(
                f"`uuid_fields` names fields that are never emitted on an edge: {', '.join(unknown)}. "
                "An edge id derived from an absent field would be identical for every edge sharing the "
                "remaining fields. Unknown columns fold into `supporting_text`; name that instead if you "
                "meant to include them.",
                code="uuid-bad-fields",
            )
        # Persist the canonicalized spellings so the Rust deduper matches record keys exactly.
        object.__setattr__(self, "uuid_fields", canonical)
        return self

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_rig_keys(cls, data: Any) -> Any:
        """Fail configs still carrying RIG metadata at the top level.

        Runs before field validation. ``extra='forbid'`` alone would reject these keys
        too, but with a generic 'extra fields not permitted' message; this hook names
        each stale key and its new home under ``rig:`` so migration is mechanical.
        """
        if isinstance(data, dict):
            found: list[str] = sorted(k for k in data if k in LEGACY_RIG_KEYS)
            if found:
                moves: str = "; ".join(f"`{k}` -> `{LEGACY_RIG_KEYS[k]}`" for k in found)
                raise TablassertValidationError(
                    f"RIG metadata moved under the required `rig:` section: {moves}. "
                    "Top-level placement is rejected so a build can never emit a RIG that "
                    "silently disagrees with its graph config.",
                    code="rig-legacy-keys",
                )
        return data

    @property
    def infores_id(self) -> str:
        """Graph-level primary knowledge source infores (from ``rig.source_info.infores_id``)."""
        return self.rig.source_info.infores_id

    @property
    def uuid_namespace(self) -> str:
        """UUID domain for this graph's edge ids.

        Hashing only ``uuid_fields`` removes the accidental cross-graph uniqueness that
        full-record hashing provided: two graphs asserting the same triple from the same
        publication would derive the same id. Namespacing on the graph's own infores makes
        that structurally impossible. With no ``uuid_fields`` the domain stays the historic
        ``TABLASSERT`` constant, so default-configured graphs keep deriving as before.
        """
        if self.uuid_domain is not None:
            return self.uuid_domain
        return self.infores_id if self.uuid_fields else "TABLASSERT"
