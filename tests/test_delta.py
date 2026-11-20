from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from tablassert import extras, lib
from tablassert.errors import MissingExtraError
from tablassert.lib import Tcode, delta
from tablassert.models import Delta


def test_delta_model_and_version_pinning() -> None:
    src = Delta.model_validate({"kind": "delta", "local": "./data/tbl", "url": ["https://example.org/tbl"], "version": 3})
    assert src.kind == "delta"
    assert src.version == 3


def test_delta_reader_requires_extra_when_absent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(extras, "find_spec", lambda name: None)
    with pytest.raises(MissingExtraError, match="deltalake"):
        delta(tmp_path / "tbl")


def test_delta_reader_renames_columns_positionally_and_forwards_version(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(extras, "require", lambda *a, **_kw: None)
    seen: list[tuple[str, int | None]] = []

    def _fake_scan_delta(source: str, **kwargs: Any) -> pl.LazyFrame:
        seen.append((source, kwargs.get("version")))
        return pl.LazyFrame({"gene": ["BRCA1"], "disease": ["MONDO:0007254"]})

    monkeypatch.setattr(pl, "scan_delta", _fake_scan_delta)
    monkeypatch.setattr(lib, "pl", pl)
    df = delta(tmp_path / "tbl", version=7).collect()
    assert seen == [(str(tmp_path / "tbl"), 7)]
    assert df.columns == ["column_1", "column_2"]
    assert df["column_1"].to_list() == ["BRCA1"]


def test_tcode_dispatches_delta_source(tmp_path: Path) -> None:
    tc = Tcode.model_validate(
        {
            "config": tmp_path / "t.yaml",
            "store": tmp_path / "s.parquet",
            "source": {"kind": "delta", "local": tmp_path / "tbl", "version": 2, "url": ["https://example.org/tbl"]},
            "statement": {
                "subject": {"method": "column", "encoding": "A"},
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "B"},
            },
            "provenance": {"repo": "PMID", "publication": "12345678"},
        }
    )
    ops = tc._source_ops()
    assert (delta, (tmp_path / "tbl", 2)) in ops
