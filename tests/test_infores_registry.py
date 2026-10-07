"""Tests for infores registry validation (loader, collection, classification).

The registry check closes the gap where only the ``infores:`` prefix was validated, so
these tests protect three invariants: the bundled snapshot is real and parseable, the
collector reads every provenance field Tablassert emits, and classification never
mistakes a locally-minted (allowlisted) CURIE for an unregistered one. Everything runs
offline: the bundled snapshot is read in place, and synthetic tiny snapshots + NDJSON
fixtures exercise the error paths.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from tablassert.errors import TablassertError, error_code_of
from tablassert.infores_registry import INFORES_PREFIX, REGISTRY_SNAPSHOT_PATH, load_registry_snapshot, validate_infores

REAL_REGISTERED = "infores:monarchinitiative"
REAL_REGISTERED_TWO = "infores:semmeddb"


def write_ndjson(path: Path, records: list[dict[str, Any]]) -> Path:
    """Write one NDJSON fixture file; records emit exactly as the build does (json.dumps)."""
    path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
    return path


def tiny_registry(tmp_path: Path, stanzas: list[dict[str, Any]] | None = None, *, drop_list: bool = False) -> frozenset[str]:
    """Build an injectable registry set from a synthetic catalog document."""
    if drop_list:
        return load_registry_snapshot(_write_catalog(tmp_path, {"unrelated": []}))
    return frozenset(stanza["id"] for stanza in (stanzas or default_stanzas()) if isinstance(stanza, dict) and stanza.get("id"))


def default_stanzas() -> list[dict[str, Any]]:
    """Two synthetic resources mirroring the upstream stanza shape (explicit ``id`` field)."""
    return [{"id": REAL_REGISTERED, "name": "Monarch Initiative"}, {"id": REAL_REGISTERED_TWO, "name": "SemMedDB"}]


def _write_catalog(tmp_path: Path, document: Any) -> Path:
    path = tmp_path / "catalog.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return path


class TestSnapshotLoader:
    def test_bundled_snapshot_is_real(self) -> None:
        """The vendored snapshot must be present, parseable, and carry the catalog's scale.

        Guards against the snapshot file being dropped from package data or truncated in
        transit: both would silently turn every build's CURIEs 'unregistered'.
        """
        identifiers = load_registry_snapshot()
        assert len(identifiers) >= 400
        assert REAL_REGISTERED in identifiers
        assert REAL_REGISTERED_TWO in identifiers

    def test_missing_file_fails_loudly(self, tmp_path: Path) -> None:
        """A missing snapshot must raise, never read as an empty registry (exit-clean bug)."""
        with pytest.raises(TablassertError) as excinfo:
            load_registry_snapshot(tmp_path / "absent.yaml")
        assert error_code_of(excinfo.value) == "infores-registry-unreadable"

    def test_non_dict_document_fails_loudly(self, tmp_path: Path) -> None:
        """Yaml that parses to a non-mapping (e.g. a bare list) has no stanza list."""
        with pytest.raises(TablassertError) as excinfo:
            load_registry_snapshot(_write_catalog(tmp_path, ["infores:a"]))
        assert error_code_of(excinfo.value) == "infores-registry-unreadable"

    def test_document_without_stanza_list_fails_loudly(self, tmp_path: Path) -> None:
        """A dict without ``information_resources`` is truncated, not empty."""
        with pytest.raises(TablassertError) as excinfo:
            tiny_registry(tmp_path, drop_list=True)
        assert error_code_of(excinfo.value) == "infores-registry-unreadable"

    def test_empty_stanza_list_fails_loudly(self, tmp_path: Path) -> None:
        """An empty stanza list would classify every real CURIE unregistered: corrupt, not clean."""
        with pytest.raises(TablassertError) as excinfo:
            load_registry_snapshot(_write_catalog(tmp_path, {"information_resources": []}))
        assert error_code_of(excinfo.value) == "infores-registry-unreadable"

    def test_stanzas_without_ids_are_ignored(self, tmp_path: Path) -> None:
        """Non-resource stanzas (no ``id``) ride along in upstream docs; only ids count."""
        stanzas = [{"name": "metadata stanza"}, {"id": REAL_REGISTERED}]
        assert tiny_registry(tmp_path, stanzas) == frozenset({REAL_REGISTERED})


class TestClassification:
    def test_edge_fields_classified(self, tmp_path: Path) -> None:
        """Edges: registered pks + sources[].resource_id pass; unknown pks are unregistered."""
        edges = write_ndjson(
            tmp_path / "e.ndjson",
            [
                {
                    "id": "urn:1",
                    "primary_knowledge_source": [REAL_REGISTERED, "infores:totally-made-up"],
                    "sources": [{"resource_id": REAL_REGISTERED_TWO, "upstream_resource_ids": ["infores:also-made-up"]}],
                }
            ],
        )
        nodes = write_ndjson(tmp_path / "n.ndjson", [{"id": "NCBIGene:1", "provided_by": [REAL_REGISTERED]}])
        report = validate_infores(nodes, edges, registry=frozenset({REAL_REGISTERED, REAL_REGISTERED_TWO}))
        assert report["registered"] == 2
        assert report["unregistered"] == 2
        assert report["malformed"] == 0
        assert report["ok"] is True
        assert report["ok_strict"] is False
        assert {"curie": "infores:totally-made-up", "where": "edges", "field": "primary_knowledge_source", "problem": "unregistered"} in report[
            "examples"
        ]

    def test_scalar_primary_knowledge_source_coerced(self, tmp_path: Path) -> None:
        """Hand-built fixtures may carry a scalar where the build emits a list: still read."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": "urn:1", "primary_knowledge_source": REAL_REGISTERED}])
        nodes = write_ndjson(tmp_path / "n.ndjson", [{"id": "x"}])
        report = validate_infores(nodes, edges, registry=frozenset({REAL_REGISTERED}))
        assert report["registered"] == 1
        assert report["unregistered"] == 0

    def test_non_infores_value_is_malformed(self, tmp_path: Path) -> None:
        """A non-infores value in an upstream field is malformed output, not merely unregistered."""
        edges = write_ndjson(
            tmp_path / "e.ndjson", [{"id": "urn:1", "sources": [{"resource_id": "citeseer", "upstream_resource_ids": ["PMID:123"]}]}]
        )
        nodes = write_ndjson(tmp_path / "n.ndjson", [{"id": "x"}])
        report = validate_infores(nodes, edges, registry=frozenset({REAL_REGISTERED}))
        assert report["malformed"] == 2
        assert report["ok"] is False
        assert report["ok_strict"] is False
        assert all(example["problem"] == "malformed" for example in report["examples"])

    def test_allowlist_absolves_locally_minted(self, tmp_path: Path) -> None:
        """Locally-minted graph CURIEs are allowed, counted separately, and never fail strict."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": "urn:1", "primary_knowledge_source": ["infores:my-local-kg"]}])
        nodes = write_ndjson(tmp_path / "n.ndjson", [{"id": "x"}])
        report = validate_infores(nodes, edges, registry=frozenset({REAL_REGISTERED}), allow=("infores:my-local-kg",))
        assert report["allowed"] == 1
        assert report["unregistered"] == 0
        assert report["ok_strict"] is True

    def test_rig_document_scanned(self, tmp_path: Path) -> None:
        """The RIG's source_info.infores_id and target primary_knowledge_sources are checked."""
        rig = tmp_path / "rig.yaml"
        rig.write_text(
            yaml.safe_dump(
                {
                    "source_info": {"infores_id": REAL_REGISTERED},
                    "target_info": {"edge_type_info": [{"primary_knowledge_sources": ["infores:rig-only-made-up"]}]},
                }
            ),
            encoding="utf-8",
        )
        edges = write_ndjson(tmp_path / "e.ndjson", [])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        report = validate_infores(nodes, edges, rig_path=rig, registry=frozenset({REAL_REGISTERED}))
        assert report["registered"] == 1
        assert report["unregistered"] == 1
        assert {
            "curie": "infores:rig-only-made-up",
            "where": "rig",
            "field": "target_info.edge_type_info.[0].primary_knowledge_sources.[0]",
            "problem": "unregistered",
        } in report["examples"]

    def test_missing_files_flagged(self, tmp_path: Path) -> None:
        """A typo'd filename must never read as a clean bill of health (validate-kgx contract)."""
        report = validate_infores(tmp_path / "absent.ndjson", tmp_path / "also-absent.ndjson", registry=frozenset({REAL_REGISTERED}))
        assert report["missing"] == {"nodes": True, "edges": True}
        assert report["ok"] is False
        assert report["ok_strict"] is False

    def test_empty_files_are_clean(self, tmp_path: Path) -> None:
        """Zero records out of zero emitted CURIEs is a legitimate pass, unlike a missing file."""
        edges = write_ndjson(tmp_path / "e.ndjson", [])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        report = validate_infores(nodes, edges, registry=frozenset({REAL_REGISTERED}))
        assert report == {**report, "registered": 0, "unregistered": 0, "malformed": 0, "ok": True, "ok_strict": True}

    def test_duplicate_observations_counted_once(self, tmp_path: Path) -> None:
        """The same CURIE on ten edges is one distinct unregistered CURIE, not ten failures."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": f"urn:{i}", "primary_knowledge_source": ["infores:made-up"]} for i in range(10)])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        report = validate_infores(nodes, edges, registry=frozenset({REAL_REGISTERED}))
        assert report["unregistered"] == 1

    def test_examples_capped_at_limit(self, tmp_path: Path) -> None:
        """The example list is capped so a pathological build cannot flood the report."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": f"urn:{i}", "primary_knowledge_source": [f"infores:made-up-{i}"]} for i in range(30)])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        report = validate_infores(nodes, edges, registry=frozenset({REAL_REGISTERED}), limit=5)
        assert len(report["examples"]) == 5
        assert report["unregistered"] == 30

    def test_example_cap_prioritizes_unregistered(self, tmp_path: Path) -> None:
        """Unregistered examples exhaust the cap first; malformed stays counted, not shown.

        Also pins the defensive skip: a non-dict ``sources`` entry is ignored rather than
        crashing the walk (a malformed provenance shape must not take down validation).
        """
        edges = write_ndjson(
            tmp_path / "e.ndjson",
            [
                {"id": "urn:1", "primary_knowledge_source": "infores:made-up", "sources": [{"resource_id": "not-a-curie"}]},
                {"id": "urn:2", "sources": "junk"},
            ],
        )
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        report = validate_infores(nodes, edges, registry=frozenset({REAL_REGISTERED}), limit=1)
        assert [example["problem"] for example in report["examples"]] == ["unregistered"]
        assert report["malformed"] == 1

    def test_bundled_snapshot_default_registry(self, tmp_path: Path) -> None:
        """Without an injected registry, the bundled snapshot is the authority end to end."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": "urn:1", "primary_knowledge_source": [REAL_REGISTERED]}])
        nodes = write_ndjson(tmp_path / "n.ndjson", [{"id": "x"}])
        report = validate_infores(nodes, edges)
        assert report["registered"] == 1
        assert report["registry"]["entries"] >= 400
        assert report["registry"]["source"] == str(REGISTRY_SNAPSHOT_PATH)


def test_prefix_constant_matches_validator() -> None:
    """The malformed boundary is the same ``infores:`` prefix models.py enforces.

    ``models.validate_infores_curie`` carries its own prefix literal, so a one-sided
    change would let syntactically-valid-but-foreign CURIEs pass config validation and
    then be classified unregistered here (or vice versa): the two must stay in lockstep.
    """
    from tablassert.models import TablassertValidationError, validate_infores_curie

    assert INFORES_PREFIX == "infores:"
    assert validate_infores_curie("infores:some-resource", "rig-bad-infores") == "infores:some-resource"
    with pytest.raises(TablassertValidationError):
        validate_infores_curie("PMID:123", "rig-bad-infores")
