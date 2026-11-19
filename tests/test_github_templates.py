from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

from tablassert import ingests
from tablassert.errors import TablassertError


def test_parse_github_ref_requires_pinned_ref() -> None:
    assert ingests.parse_github_ref("github:org/repo/templates/gene.yaml@v1.0.0") == ("org", "repo", "templates/gene.yaml", "v1.0.0")
    for bad in (
        "https://example.org/t.yaml",
        "github:org/repo/t.yaml",
        "github:org/repo@v1",
        "github:org/repo/../../etc/passwd@v1",
        "github:org/repo/templates/a.yaml?raw=1@v1",
        "github:org/repo/templates/a.yaml@v1#frag",
        "github:../repo/t.yaml@v1",
    ):
        with pytest.raises(TablassertError, match="template-bad-github-ref"):
            ingests.parse_github_ref(bad)


def test_fetch_github_template_does_not_cache_invalid_yaml_or_http_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from urllib.error import URLError

    cache_dir = tmp_path / "cache"

    def _fail_urlopen(_req: Any, timeout: int = 30) -> Any:
        raise URLError("offline")

    monkeypatch.setattr(ingests, "urlopen", _fail_urlopen)
    with pytest.raises(TablassertError, match="template-bad-github-ref"):
        ingests.fetch_github_template("github:org/repo/t.yaml@v1", cache_dir=cache_dir)
    assert not cache_dir.exists()

    class _BadResp(BytesIO):
        def __enter__(self) -> _BadResp:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(ingests, "urlopen", lambda _req, timeout=30: _BadResp(b"- just a list\n"))
    with pytest.raises(TablassertError, match="template-bad-github-ref"):
        ingests.fetch_github_template("github:org/repo/t.yaml@v1", cache_dir=cache_dir)
    assert not cache_dir.exists()


def test_to_sections_inherits_and_caches_github_template(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    remote_yaml = b"""template:
  source:
    kind: text
    local: data.tsv
    delimiter: "\\t"
    url:
      - https://example.org/data.tsv
  statement:
    subject:
      method: column
      encoding: A
    predicate: associated_with
    object:
      method: column
      encoding: B
  provenance:
    repo: PMID
    publication: "12345678"
"""
    fetches: list[str] = []

    class _Resp(BytesIO):
        def __enter__(self) -> _Resp:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    def _fake_urlopen(req: Any, timeout: int = 30) -> _Resp:
        fetches.append(req.full_url)
        return _Resp(remote_yaml)

    monkeypatch.setattr(ingests, "urlopen", _fake_urlopen)
    cache_dir = tmp_path / "cache"
    table_path = tmp_path / "table.yaml"

    instructions = {
        "template": {"from": "github:SkyeAv/Tablassert-templates/gene_disease.yaml@abc123", "statement": {"predicate": "related_to"}},
        "sections": [{}],
    }

    first = ingests.to_sections(instructions, table_path, cache_dir=cache_dir)
    assert len(fetches) == 1
    assert fetches[0] == "https://raw.githubusercontent.com/SkyeAv/Tablassert-templates/abc123/gene_disease.yaml"
    assert first[0]["statement"]["predicate"] == "related_to"
    assert first[0]["statement"]["subject"]["encoding"] == "A"

    # Second call must hit the local cache without calling urlopen again.
    second = ingests.to_sections(instructions, table_path, cache_dir=cache_dir)
    assert len(fetches) == 1
    assert second == first
