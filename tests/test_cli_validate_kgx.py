"""CLI-level tests for the merged ``tablassert validate-kgx`` gate.

The identity classifier is covered in ``test_infores_registry.py``; these tests pin the
merged command's contract: structural failures always fail, the identity half is
advisory by default and strict only on ``--strict``, missing inputs never pass in any
mode, ``--registry off`` skips identity entirely, ``--prune`` classifies the PRUNED
file, and ``--registry refresh`` fetches through the bounded-retry network layer and
caches the document. Edge fixtures reuse the DAKP-shaped otherwise-valid edge so
identity verdicts are not drowned in structural noise.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from tablassert import cli, infores_registry

REGISTERED = "infores:monarchinitiative"


def valid_edge(curie: str, edge_id: str = "urn:1") -> dict[str, Any]:
    """A minimal edge that passes the structural gate, carrying one knowledge source."""
    return {
        "id": edge_id,
        "subject": "HGNC:11998",
        "predicate": "biolink:associated_with",
        "object": "MONDO:0008903",
        "category": ["biolink:GeneToDiseaseAssociation"],
        "knowledge_level": "statistical_association",
        "agent_type": "data_analysis_pipeline",
        "primary_knowledge_source": curie,
    }


def write_ndjson(path: Path, records: list[dict[str, Any]]) -> Path:
    """Write one NDJSON fixture file."""
    path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
    return path


def run(cli_kwargs: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> str:
    """Invoke the command directly (the registered callback is the plain function)."""
    cli.validate_kgx_command(**cli_kwargs)
    return capsys.readouterr().err


class TestIdentityGate:
    def test_advisory_default_warns_and_passes(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Unregistered CURIEs warn and exit 0 without --strict: the advisory contract."""
        edges = write_ndjson(tmp_path / "e.ndjson", [valid_edge("infores:made-up")])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        out = run({"nodes": nodes, "edges": edges}, capsys)
        assert "1 unregistered" in out
        assert "pass --strict" in out
        # The structural verdict is unaffected by identity warnings.
        assert "KGX output is Biolink-compliant." in out

    def test_strict_fails_on_unregistered(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """--strict is the CI gate: unregistered CURIEs exit 1."""
        edges = write_ndjson(tmp_path / "e.ndjson", [valid_edge("infores:made-up")])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        with pytest.raises(SystemExit) as excinfo:
            run({"nodes": nodes, "edges": edges, "strict": True}, capsys)
        assert excinfo.value.code == 1

    def test_strict_passes_registered(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """A fully registered graph reads as a clean bill under --strict."""
        edges = write_ndjson(tmp_path / "e.ndjson", [valid_edge(REGISTERED)])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        out = run({"nodes": nodes, "edges": edges, "strict": True}, capsys)
        assert "All emitted infores CURIEs are registered." in out
        assert "KGX output is Biolink-compliant." in out

    def test_allow_infores_absolves_locally_minted(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """The graph's own CURIE is exempt via --allow-infores and strict stays green."""
        edges = write_ndjson(tmp_path / "e.ndjson", [valid_edge("infores:my-local-kg")])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        out = run({"nodes": nodes, "edges": edges, "allow_infores": ["infores:my-local-kg"], "strict": True}, capsys)
        assert "1 allowed" in out
        assert "All emitted infores CURIEs are registered." in out

    def test_malformed_fails_strict(self, tmp_path: Path) -> None:
        """Non-infores values are defects too: --strict fails on them."""
        edges = write_ndjson(tmp_path / "e.ndjson", [valid_edge("PMID:123")])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        with pytest.raises(SystemExit) as excinfo:
            cli.validate_kgx_command(nodes=nodes, edges=edges, strict=True)
        assert excinfo.value.code == 1

    def test_rig_values_are_classified(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """--rig includes the RIG document's infores values in the classification."""
        edges = write_ndjson(tmp_path / "e.ndjson", [valid_edge(REGISTERED)])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        rig = tmp_path / "rig.yaml"
        rig.write_text(yaml.safe_dump({"source_info": {"infores_id": "infores:rig-made-up"}}), encoding="utf-8")
        with pytest.raises(SystemExit) as excinfo:
            run({"nodes": nodes, "edges": edges, "rig": rig, "strict": True}, capsys)
        assert excinfo.value.code == 1

    def test_missing_rig_fails_even_advisory(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """A typo'd --rig path exits 1 without --strict: never a silent pass."""
        edges = write_ndjson(tmp_path / "e.ndjson", [valid_edge(REGISTERED)])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        with pytest.raises(SystemExit) as excinfo:
            run({"nodes": nodes, "edges": edges, "rig": tmp_path / "absent.yaml"}, capsys)
        assert excinfo.value.code == 1
        assert "rig: file not found" in capsys.readouterr().err

    def test_missing_structural_input_fails(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """A missing nodes file fails the structural half; identity does not echo it."""
        edges = write_ndjson(tmp_path / "e.ndjson", [])
        with pytest.raises(SystemExit) as excinfo:
            run({"nodes": tmp_path / "absent.ndjson", "edges": edges}, capsys)
        assert excinfo.value.code == 1
        out = capsys.readouterr().err
        assert "file not found" in out
        assert "KGX output is not Biolink-compliant." in out

    def test_structural_failures_still_fail_under_the_default(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """A structurally broken edge exits 1 even when its identity is registered."""
        broken = {**valid_edge(REGISTERED), "p_value": "not-a-number"}
        edges = write_ndjson(tmp_path / "e.ndjson", [broken])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        with pytest.raises(SystemExit) as excinfo:
            run({"nodes": nodes, "edges": edges}, capsys)
        assert excinfo.value.code == 1
        assert "KGX output is not Biolink-compliant." in capsys.readouterr().err

    def test_registry_off_skips_identity_entirely(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """--registry off restores pure structural validation: no identity output at all."""
        edges = write_ndjson(tmp_path / "e.ndjson", [valid_edge("infores:made-up")])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        out = run({"nodes": nodes, "edges": edges, "registry": "off"}, capsys)
        assert "registry:" not in out
        assert "unregistered" not in out
        assert "KGX output is Biolink-compliant." in out


class TestPruneInterplay:
    def test_prune_classifies_the_pruned_file(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Identity scans the post-prune state: only what ships is judged.

        Why: ``--prune`` rewrites the edges file in place, so a pre-prune scan would
        report CURIEs from records the rewrite deleted -- and both the strict verdict
        and the printed counts would disagree with the artifact on disk. The dropped
        record here carries the only registered CURIE, so a pre-prune scan would print
        ``1 registered``; the merged command must print ``0 registered`` and fail on
        the surviving unregistered edge.
        """
        dropped = {**valid_edge(REGISTERED, edge_id="urn:dropped"), "p_value": "not-a-number"}
        kept = valid_edge("infores:made-up", edge_id="urn:kept")
        edges = write_ndjson(tmp_path / "e.ndjson", [dropped, kept])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        with pytest.raises(SystemExit) as excinfo:
            run({"nodes": nodes, "edges": edges, "prune": True, "strict": True}, capsys)
        assert excinfo.value.code == 1
        out = capsys.readouterr().err
        assert "0 registered" in out
        assert "1 unregistered" in out

    def test_prune_passes_when_the_survivors_are_registered(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """A prune that leaves only registered edges exits 0 on both halves."""
        dropped = {**valid_edge("infores:made-up", edge_id="urn:dropped"), "p_value": "not-a-number"}
        kept = valid_edge(REGISTERED, edge_id="urn:kept")
        edges = write_ndjson(tmp_path / "e.ndjson", [dropped, kept])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        out = run({"nodes": nodes, "edges": edges, "prune": True, "strict": True}, capsys)
        assert "KGX output is Biolink-compliant." in out
        assert "All emitted infores CURIEs are registered." in out


class TestRegistryRefresh:
    def test_registry_refresh_fetches_and_caches(self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
        """--registry refresh pulls the live catalog via the retry layer and caches the body."""
        catalog = yaml.safe_dump({"information_resources": [{"id": REGISTERED}]})
        monkeypatch.setattr(infores_registry.net, "http_get_text", lambda url, **kwargs: catalog)
        monkeypatch.setattr(infores_registry, "registry_cache_path", lambda: tmp_path / "cache" / "infores_catalog.yaml")
        edges = write_ndjson(tmp_path / "e.ndjson", [valid_edge(REGISTERED)])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        out = run({"nodes": nodes, "edges": edges, "registry": "refresh", "strict": True}, capsys)
        assert "(refreshed)" in out
        assert (tmp_path / "cache" / "infores_catalog.yaml").read_text(encoding="utf-8") == catalog

    def test_registry_refresh_rejects_garbage_body(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A fetched body without the stanza list raises and never poisons the cache."""
        monkeypatch.setattr(infores_registry.net, "http_get_text", lambda url, **kwargs: yaml.safe_dump({"unrelated": []}))
        monkeypatch.setattr(infores_registry, "registry_cache_path", lambda: tmp_path / "cache.yaml")
        edges = write_ndjson(tmp_path / "e.ndjson", [])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        with pytest.raises(infores_registry.TablassertError) as excinfo:
            cli.validate_kgx_command(nodes=nodes, edges=edges, registry="refresh")
        assert "information_resources" in str(excinfo.value)
        assert not (tmp_path / "cache.yaml").exists()

    def test_refresh_rejects_empty_stanza_list_before_caching(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A fetched body with an empty stanza list fails and never reaches the cache.

        Such a body would classify every real CURIE unregistered on later inspection;
        it must be rejected at fetch time, not cached as a valid-looking registry.
        """
        from tablassert.errors import TablassertError, error_code_of

        monkeypatch.setattr(infores_registry.net, "http_get_text", lambda url, **kwargs: yaml.safe_dump({"information_resources": []}))
        cache = tmp_path / "cache.yaml"
        monkeypatch.setattr(infores_registry, "registry_cache_path", lambda: cache)
        edges = write_ndjson(tmp_path / "e.ndjson", [])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        with pytest.raises(TablassertError) as excinfo:
            cli.validate_kgx_command(nodes=nodes, edges=edges, registry="refresh")
        assert error_code_of(excinfo.value) == "infores-registry-unreadable"
        assert not cache.exists()


def test_non_json_lines_count_as_malformed(tmp_path: Path) -> None:
    """A line that is not JSON at all is malformed output, never a traceback.

    Mirrors validate-kgx's explicit handling of its own non-JSON lines: the report
    stays structured so one corrupt line cannot crash a build gate.
    """
    from tablassert.infores_registry import validate_infores

    edges = tmp_path / "e.ndjson"
    edges.write_text('{"id": "urn:1", "primary_knowledge_source": ["infores:monarchinitiative"]}\nnot json at all\n', encoding="utf-8")
    nodes = write_ndjson(tmp_path / "n.ndjson", [])
    report = validate_infores(nodes, edges, registry=frozenset({REGISTERED}))
    assert report["malformed"] == 1
    assert report["registered"] == 1
    assert report["ok"] is False
    assert any(example["field"] == "<not-json>" for example in report["examples"])


def test_corrupt_rig_raises_coded_error(tmp_path: Path) -> None:
    """An unparsable --rig yaml raises the coded error, not a raw yaml traceback."""
    from tablassert.errors import TablassertError, error_code_of
    from tablassert.infores_registry import validate_infores

    edges = write_ndjson(tmp_path / "e.ndjson", [])
    nodes = write_ndjson(tmp_path / "n.ndjson", [])
    rig = tmp_path / "rig.yaml"
    rig.write_text("source_info: [unclosed", encoding="utf-8")
    with pytest.raises(TablassertError) as excinfo:
        validate_infores(nodes, edges, rig_path=rig, registry=frozenset({REGISTERED}))
    assert error_code_of(excinfo.value) == "infores-registry-unreadable"


def test_help_renders_every_flag() -> None:
    """The help page must self-describe: an agent picks flags from help alone."""
    import io

    from rich.console import Console

    buffer = io.StringIO()
    cli.APP.help_print(["validate-kgx"], console=Console(file=buffer, width=120, legacy_windows=False))
    help_text = " ".join(buffer.getvalue().replace("│", " ").split())
    for flag in ("--nodes", "--edges", "--rig", "--allow-infores", "--registry", "--strict", "--limit", "--prune"):
        assert flag in help_text, f"help missing {flag}"
    for token in ("snapshot", "refresh", "off"):
        assert token in help_text, f"help missing registry mode {token}"
