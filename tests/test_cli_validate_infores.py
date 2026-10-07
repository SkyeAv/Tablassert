"""CLI-level tests for ``tablassert validate-infores``.

The core classifier is covered in ``test_infores_registry.py``; these tests pin the
command's contract: advisory by default, strict only on the flag, missing inputs never
a pass in any mode, ``--registry off`` skips membership, and ``--registry refresh``
fetches through the bounded-retry network layer and caches the document.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from tablassert import cli, infores_registry

REGISTERED = "infores:monarchinitiative"


def write_ndjson(path: Path, records: list[dict[str, Any]]) -> Path:
    """Write one NDJSON fixture file."""
    path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
    return path


def run(cli_kwargs: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> str:
    """Invoke the command directly (the registered callback is the plain function)."""
    cli.validate_infores_command(**cli_kwargs)
    return capsys.readouterr().err


class TestValidateInforesCommand:
    def test_advisory_default_exits_zero(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Unregistered CURIEs warn and exit 0 without --strict: the advisory contract."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": "urn:1", "primary_knowledge_source": ["infores:made-up"]}])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        out = run({"nodes": nodes, "edges": edges}, capsys)
        assert "1 unregistered" in out
        assert "pass --strict" in out

    def test_strict_fails_on_unregistered(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """--strict is the CI gate: unregistered CURIEs exit 1."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": "urn:1", "primary_knowledge_source": ["infores:made-up"]}])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        with pytest.raises(SystemExit) as excinfo:
            run({"nodes": nodes, "edges": edges, "strict": True}, capsys)
        assert excinfo.value.code == 1

    def test_strict_passes_registered(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """A fully registered graph reads as a clean bill under --strict."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": "urn:1", "primary_knowledge_source": [REGISTERED]}])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        out = run({"nodes": nodes, "edges": edges, "strict": True}, capsys)
        assert "All emitted infores CURIEs are registered." in out

    def test_allow_infores_absolves_locally_minted(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """The graph's own CURIE is exempt via --allow-infores and strict stays green."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": "urn:1", "primary_knowledge_source": ["infores:my-local-kg"]}])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        out = run({"nodes": nodes, "edges": edges, "allow_infores": ["infores:my-local-kg"], "strict": True}, capsys)
        assert "1 allowed" in out
        assert "All emitted infores CURIEs are registered." in out

    def test_missing_input_fails_in_advisory_mode(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """A typo'd path exits 1 even without --strict: never a silent pass."""
        edges = write_ndjson(tmp_path / "e.ndjson", [])
        with pytest.raises(SystemExit) as excinfo:
            run({"nodes": tmp_path / "absent.ndjson", "edges": edges}, capsys)
        assert excinfo.value.code == 1
        assert "file not found" in capsys.readouterr().err

    def test_malformed_fails_strict(self, tmp_path: Path) -> None:
        """Non-infores values are defects too: --strict fails on them."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": "urn:1", "primary_knowledge_source": ["PMID:123"]}])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        with pytest.raises(SystemExit) as excinfo:
            cli.validate_infores_command(nodes=nodes, edges=edges, strict=True)
        assert excinfo.value.code == 1

    def test_registry_off_skips_membership(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """--registry off reports structure only and never claims registry verification."""
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": "urn:1", "primary_knowledge_source": ["infores:made-up"]}])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        out = run({"nodes": nodes, "edges": edges, "registry": "off", "strict": True}, capsys)
        assert "0 entries" in out
        assert "infores membership check skipped (--registry off)." in out

    def test_registry_refresh_fetches_and_caches(self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
        """--registry refresh pulls the live catalog via the retry layer and caches the body."""
        catalog = yaml.safe_dump({"information_resources": [{"id": REGISTERED}]})
        monkeypatch.setattr(infores_registry.net, "http_get_text", lambda url, **kwargs: catalog)
        monkeypatch.setattr(infores_registry, "registry_cache_path", lambda: tmp_path / "cache" / "infores_catalog.yaml")
        edges = write_ndjson(tmp_path / "e.ndjson", [{"id": "urn:1", "primary_knowledge_source": [REGISTERED]}])
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
            cli.validate_infores_command(nodes=nodes, edges=edges, registry="refresh")
        assert "information_resources" in str(excinfo.value)
        assert not (tmp_path / "cache.yaml").exists()

    def test_rig_option_scanned(self, tmp_path: Path) -> None:
        """--rig includes the RIG document's infores values in the classification."""
        edges = write_ndjson(tmp_path / "e.ndjson", [])
        nodes = write_ndjson(tmp_path / "n.ndjson", [])
        rig = tmp_path / "rig.yaml"
        rig.write_text(yaml.safe_dump({"source_info": {"infores_id": "infores:rig-made-up"}}), encoding="utf-8")
        with pytest.raises(SystemExit) as excinfo:
            cli.validate_infores_command(nodes=nodes, edges=edges, rig=rig, strict=True)
        assert excinfo.value.code == 1

    def test_non_json_lines_count_as_malformed(self, tmp_path: Path) -> None:
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

    def test_corrupt_rig_raises_coded_error(self, tmp_path: Path) -> None:
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
            cli.validate_infores_command(nodes=nodes, edges=edges, registry="refresh")
        assert error_code_of(excinfo.value) == "infores-registry-unreadable"
        assert not cache.exists()

    def test_help_renders_every_flag(self) -> None:
        """The help page must self-describe: an agent picks flags from help alone."""
        import io

        from rich.console import Console

        buffer = io.StringIO()
        cli.APP.help_print(["validate-infores"], console=Console(file=buffer, width=120, legacy_windows=False))
        help_text = " ".join(buffer.getvalue().replace("│", " ").split())
        for flag in ("--nodes", "--edges", "--rig", "--allow-infores", "--registry", "--strict", "--limit"):
            assert flag in help_text, f"help missing {flag}"
        assert "snapshot" in help_text
        assert "refresh" in help_text
        assert "off" in help_text
