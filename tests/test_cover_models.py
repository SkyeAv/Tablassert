"""Coverage tests for uncovered validator/helper branches in ``models`` and ``biolink``.

Each test targets a specific source line that the existing suite never executes:
the ``None`` short-circuit branch of the publication field validator in
:mod:`tablassert.models`, and the duplicate-member guard plus class-name fallback in
:mod:`tablassert.biolink`. The ``None`` branches are only reachable by passing the
field explicitly (``TablaBase`` does not set ``validate_default``, so omitted fields
never run their after-validators), which the happy-path tests never do.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from tablassert.biolink import _build_str_enum, _category_name
from tablassert.models import Excel, Graph, ManualProvenance, Parquet, Section, Text, _section_source_label


def test_manual_provenance_explicit_none_publications_short_circuits() -> None:
    """``models.py:331`` — ``ManualProvenance.pmcid_publications`` returns ``None`` for an explicit ``None``.

    The validator's ``if values is None: return None`` guard is only hit when ``publications``
    is passed explicitly as ``None``; the default path never runs the after-validator. This
    proves the guard short-circuits before the ``PMCID:`` prefix loop.
    """
    override: ManualProvenance = ManualProvenance(publications=None)  # pyright: ignore
    assert override.publications is None


def test_graph_explicit_none_rig_ui_explanation_short_circuits(rig_factory: Any) -> None:
    """an explicit ``ui_explanation: null`` stays ``None`` (the composer keeps only the default).

    Passing ``None`` explicitly exercises the optional field's ``None`` branch, which the
    default-omission path never distinguishes — both must produce the default-only explanation.
    """
    rig_data: dict[str, Any] = rig_factory()
    rig_data["ui_explanation"] = None
    graph: Graph = Graph.model_validate(  # pyright: ignore
        {"name": "TEST", "version": "1.0.0", "tables": ["./table.yaml"], "fullmap": "./fullmap", "rig": rig_data}
    )
    assert graph.rig.ui_explanation is None


def test_build_str_enum_raises_on_duplicate_member_name() -> None:
    """``biolink.py:96`` — ``_build_str_enum`` raises ``ValueError`` when two values collide.

    ``_screaming_snake`` upper-cases, so the distinct values ``"Gene"`` and ``"gene"`` both map
    to the member name ``GENE``. ``sorted(set(...))`` keeps both (they are not equal), so the
    second insertion finds ``GENE`` already present and trips the duplicate-member guard, which
    the real Biolink-derived vocabularies (all unique) never trigger.
    """
    with pytest.raises(ValueError, match="duplicate enum member 'GENE'"):
        _build_str_enum("DuplicateProbe", ["Gene", "gene"])


def test_category_name_falls_back_to_class_name() -> None:
    """``biolink.py:118`` — ``_category_name`` returns ``cls.__name__`` when no ``biolink:`` category exists.

    Every real ``pydanticmodel_v2`` entity carries a ``category`` default of ``["biolink:X", ...]``,
    so the trailing ``return str(cls.__name__)`` fallback is unreachable from the model walk. A bare
    Pydantic model with ``model_fields`` but no ``category`` field drives ``field is not None`` false
    and executes the fallback, returning the class name verbatim.
    """

    class NoCategory(BaseModel):
        x: int = 1

    assert _category_name(NoCategory) == "NoCategory"


def test_parquet_source_parses_and_forbids_extras() -> None:
    """The ``Parquet`` source parses a minimal config and rejects format-specific fields.

    Parquet is self-describing (typed columns, real headers, no sheets or delimiters), so unlike
    ``Excel`` (``sheet``) and ``Text`` (``delimiter``) it adds no fields beyond ``BaseSource``;
    ``TablaBase``'s ``extra="forbid"`` must therefore reject an ``Excel``-style ``sheet`` key
    instead of silently accepting (and ignoring) it.
    """
    source: Parquet = Parquet.model_validate({"kind": "parquet", "local": "./data.parquet", "url": ["https://example.com/data.parquet"]})
    assert source.kind == "parquet"
    assert not hasattr(source, "sheet")
    assert not hasattr(source, "delimiter")
    with pytest.raises(ValidationError, match="extra_forbidden"):
        Parquet.model_validate({"kind": "parquet", "local": "./data.parquet", "url": ["https://example.com/data.parquet"], "sheet": "Sheet1"})


def test_section_accepts_all_three_source_kinds() -> None:
    """``Section.source`` validates ``kind: parquet`` alongside the existing ``text``/``excel``.

    The union is pydantic's smart union on the ``kind`` literal, so adding a third member must
    keep every existing kind parsing (no union ambiguity) and resolve each config to the model
    its ``kind`` names. This is the config-author-facing contract of the new source kind.
    """
    base: dict[str, Any] = {
        "statement": {"subject": {"method": "value", "encoding": "BRCA1"}, "object": {"method": "value", "encoding": "TP53"}},
        "provenance": {"repo": "PMC", "publication": "PMC0000000"},
    }
    for kind, model in [("text", Text), ("excel", Excel), ("parquet", Parquet)]:
        payload: dict[str, Any] = {**base, "source": {"kind": kind, "local": f"./table.{kind}", "url": [f"https://example.com/table.{kind}"]}}
        section: Section = Section.model_validate(payload)
        assert section.source.kind == kind
        assert type(section.source) is model


def test_section_source_label_per_kind() -> None:
    """The diagnostic label appends the sheet only for Excel; text and parquet render the bare path.

    ``_section_source_label`` feeds read-failure diagnostics, so a new kind must render without
    hitting the Excel-only ``source.sheet`` access (parquet has no ``sheet`` attribute at all).
    """
    urls: list[Any] = ["https://example.com/table"]
    # `local` is a Path, so a leading "./" normalizes away in the rendered label.
    assert _section_source_label(Text.model_validate({"kind": "text", "local": "./t.tsv", "url": urls})) == "`t.tsv`"
    assert _section_source_label(Parquet.model_validate({"kind": "parquet", "local": "./t.parquet", "url": urls})) == "`t.parquet`"
    excel: Excel = Excel.model_validate({"kind": "excel", "local": "./t.xlsx", "url": urls, "sheet": "S1"})
    assert _section_source_label(excel) == "`t.xlsx` (sheet `S1`)"
