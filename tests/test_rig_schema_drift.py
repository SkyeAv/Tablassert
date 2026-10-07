"""RIG drift tests: the Pydantic mirror and the generator vs the canonical LinkML schema.

Tablassert mirrors ``biolink/resource-ingest-guide-schema`` (class ``ReferenceIngestGuide``)
in hand-written Pydantic classes (``models.RIG*``) and composes the final document in
``rig.build_rig_document``. Nothing validated that mirror against the released schema, so
upstream slot renames or additions drifted silently until a downstream
``linkml-validate`` failure. These tests pin three layers: field-set lockstep (Test A),
generated-document conformance (Test B), and vendored-copy staleness (Test C).

The schema is vendored at ``tests/fixtures/resource_ingest_guide_schema.yaml``; the
validator is the dependency-free LinkML subset in ``tests/_rig_linkml.py``.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

from tablassert.models import RIGConfig
from tablassert.rig import build_rig_document
from tests._rig_linkml import load_schema, validate_document

SCHEMA_PATH: Path = Path(__file__).parent / "fixtures" / "resource_ingest_guide_schema.yaml"

#: Mirror class -> schema class, for the FULL mirrors whose field sets must be equal.
FULL_MIRRORS: tuple[tuple[str, str], ...] = (
    ("RIGSourceInfo", "SourceInformation"),
    ("RIGTermsOfUseInfo", "TermsOfUseInformation"),
    ("RIGIngestInfo", "IngestInformation"),
    ("RIGSupportingDataSourceInfo", "SupportingDataSourceInformation"),
    ("RIGProvenanceInfo", "ProvenanceInformation"),
    ("RIGRelevantFile", "RelevantFiles"),
    ("RIGIncludedContent", "IncludedContent"),
    ("RIGFilteredContent", "FilteredContent"),
    ("RIGFutureContentConsideration", "FutureContentConsiderations"),
    ("RIGFutureModelingConsideration", "FutureModelingConsiderations"),
)

#: Tablassert-specific keys on the ``rig:`` CONFIG that feed the generator and are
#: deliberately stripped from the emitted document (never schema slots). Keeping them
#: named here makes every NEW mirror-only key a visible decision instead of silent drift.
CONFIG_ONLY_KEYS: frozenset[str] = frozenset({"artifact_base_path", "artifact_base_url", "source_files", "ui_explanation"})


@pytest.fixture
def schema() -> dict[str, Any]:
    """The vendored LinkML schema document."""
    return load_schema(SCHEMA_PATH)


def _mirror_fields(models_module: Any, py_name: str) -> set[str]:
    """Field names of one RIG mirror class (``model_fields`` keys)."""
    return set(getattr(models_module, py_name).model_fields)


class TestMirrorLockstep:
    def test_full_mirrors_match_schema_slots(self, schema: dict[str, Any]) -> None:
        """Every full mirror's field set equals its schema class's attribute set.

        Both directions matter: a mirror field the schema lacks emits a slot downstream
        validators will reject as unknown; a schema slot the mirror lacks means upstream
        grew and Tablassert cannot author it.
        """
        from tablassert import models

        for py_name, schema_name in FULL_MIRRORS:
            mirror: set[str] = _mirror_fields(models, py_name)
            canonical: set[str] = set(schema["classes"][schema_name].get("attributes", {}))
            assert mirror == canonical, (
                f"{py_name} drifted from {schema_name}: mirror-only={sorted(mirror - canonical)} schema-only={sorted(canonical - mirror)}"
            )

    def test_target_info_extras_are_a_schema_subset(self, schema: dict[str, Any]) -> None:
        """``RIGTargetInfoExtras`` mirrors only the authorable TargetInformation slots.

        The remaining schema slots (``edge_type_info`` / ``node_type_info`` /
        ``infores_id``) are composed by ``rig.build_rig_document`` from observed graph
        facts, so the mirror is a deliberate subset; a mirror key outside the schema is
        still drift and fails.
        """
        from tablassert import models

        mirror: set[str] = _mirror_fields(models, "RIGTargetInfoExtras")
        canonical: set[str] = set(schema["classes"]["TargetInformation"].get("attributes", {}))
        assert mirror <= canonical, f"RIGTargetInfoExtras carries non-schema keys: {sorted(mirror - canonical)}"

    def test_rig_config_covers_schema_requirements_plus_config_only(self, schema: dict[str, Any]) -> None:
        """``RIGConfig`` carries every required ReferenceIngestGuide slot and only
        documented config-only extras beyond the schema's attribute set."""
        from tablassert import models

        mirror: set[str] = _mirror_fields(models, "RIGConfig")
        canonical: set[str] = set(schema["classes"]["ReferenceIngestGuide"].get("attributes", {}))
        assert mirror - canonical <= CONFIG_ONLY_KEYS, f"undocumented mirror-only keys: {sorted((mirror - canonical) - CONFIG_ONLY_KEYS)}"
        for key, attribute in schema["classes"]["ReferenceIngestGuide"].get("attributes", {}).items():
            if attribute.get("required"):
                assert key in mirror, f"schema-required slot {key!r} missing from RIGConfig"


class TestGeneratedDocument:
    def test_built_rig_conforms_to_schema(self, tmp_path: Path, rig_factory: Callable[..., dict[str, Any]]) -> None:
        """A RIG generated from a real (minimal) build validates against the schema.

        This is the end-state guarantee: whatever the mirror permits, the emitted
        document itself must satisfy the canonical schema -- unknown keys, missing
        required slots, and illegal enum values all fail here with a class+slot path.
        """
        from tablassert.models import RIGConfig

        rig: RIGConfig = RIGConfig.model_validate(rig_factory(tmp_path, infores_id="infores:drift-kg"))
        nodes_path: Path = tmp_path / "DRIFT_1.0.0.nodes.ndjson"
        edges_path: Path = tmp_path / "DRIFT_1.0.0.edges.ndjson"
        nodes_path.write_text('{"id":"HGNC:1","category":["biolink:Gene"]}\n', encoding="utf-8")
        edges_path.write_text("", encoding="utf-8")
        document: dict[str, Any] = build_rig_document(
            "DRIFT",
            "1.0.0",
            rig,
            nodes_path,
            edges_path,
            node_type_info=[{"node_category": "biolink:Gene", "source_identifier_types": ["HGNC"]}],
            edge_type_info=[],
            node_count=1,
            edge_count=0,
            node_fields=["category", "id"],
            edge_fields=[],
        )
        errors = validate_document(document, schema=load_schema(SCHEMA_PATH))
        assert not errors, f"generated RIG violates the schema: {[str(error) for error in errors]}"

    def test_validator_rejects_unknown_and_missing(self, schema: dict[str, Any]) -> None:
        """The subset validator actually bites: unknown keys and missing required slots fail.

        Guards against a vacuous validator (a checker that passes everything would make
        Test B meaningless).
        """
        document: dict[str, Any] = {"name": "x", "definitely_not_a_slot": 1}
        errors = validate_document(document, schema=schema)
        assert any("unknown slot" in error.reason for error in errors)
        missing: set[str] = {error.path.rsplit(".", 1)[-1] for error in errors if "required slot missing" in error.reason}
        for required, attribute in schema["classes"]["ReferenceIngestGuide"]["attributes"].items():
            if attribute.get("required"):
                assert required in missing, f"validator did not flag missing required slot {required!r}"


@pytest.mark.network
class TestVendoredSchemaStaleness:
    def test_vendored_copy_matches_upstream(self) -> None:
        """The vendored schema is byte-identical to the pinned upstream commit's file.

        Skips when offline (CI/dev without network) so the suite stays deterministic;
        when it runs, a non-identical copy means upstream changed and the vendored file
        must be re-vendored and the drift reviewed.
        """
        from tablassert import net

        upstream_ref = "f870823cfde8"
        url = f"https://raw.githubusercontent.com/biolink/resource-ingest-guide-schema/{upstream_ref}/src/resource_ingest_guide_schema/schema/resource_ingest_guide_schema.yaml"
        body: str | None
        try:
            body = net.http_get_text(url)
        except Exception as error:
            pytest.skip(f"network unavailable, staleness check skipped: {error}")
        # Semantic (not byte) equality: the vendored copy carries three header comment
        # lines recording the source, and yaml ignores comments. Any MODEL change
        # (classes, attributes, enums) trips this assertion.
        assert yaml.safe_load(body) == load_schema(SCHEMA_PATH), f"vendored schema drifted from upstream; re-vendor from {url} and review the diff"


def test_vendored_schema_parses() -> None:
    """The vendored file must stay a parseable LinkML doc with the expected shape."""
    schema: dict[str, Any] = load_schema(SCHEMA_PATH)
    assert "ReferenceIngestGuide" in schema["classes"]
    assert schema["classes"]["ReferenceIngestGuide"]["attributes"]
