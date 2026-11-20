from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import polars as pl
import pytest

from tablassert import extras, lib
from tablassert.errors import MissingExtraError
from tablassert.lib import Tcode, iceberg
from tablassert.models import Iceberg


def test_iceberg_model_and_snapshot_pinning() -> None:
    src = Iceberg.model_validate(
        {
            "kind": "iceberg",
            "catalog": {"uri": "sqlite:///catalog.db"},
            "table_identifier": "ns.tbl",
            "snapshot_id": 42,
            "url": ["https://example.org/iceberg"],
        }
    )
    assert src.kind == "iceberg"
    assert src.snapshot_id == 42


def test_iceberg_reader_requires_extra_when_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extras, "find_spec", lambda name: None)
    with pytest.raises(MissingExtraError, match="pyiceberg"):
        iceberg({"uri": "sqlite:///catalog.db"}, "ns.tbl")


def test_iceberg_reader_renames_columns_and_forwards_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extras, "require", lambda *a, **_kw: None)
    seen: list[tuple[str, str, int | None]] = []

    class _FakeCatalog:
        def load_table(self, ident: str) -> str:
            return f"loaded:{ident}"

    fake_catalog_mod = SimpleNamespace(load_catalog=lambda name, **_kw: _FakeCatalog())
    fake_pkg = SimpleNamespace(catalog=fake_catalog_mod)
    monkeypatch.setitem(sys.modules, "pyiceberg", fake_pkg)
    monkeypatch.setitem(sys.modules, "pyiceberg.catalog", fake_catalog_mod)

    def _fake_scan_iceberg(table: Any, **kwargs: Any) -> pl.LazyFrame:
        seen.append(("scan", str(table), kwargs.get("snapshot_id")))
        return pl.LazyFrame({"gene": ["BRCA1"], "disease": ["MONDO:0007254"]})

    monkeypatch.setattr(pl, "scan_iceberg", _fake_scan_iceberg)
    monkeypatch.setattr(lib, "pl", pl)
    df = iceberg({"uri": "sqlite:///catalog.db"}, "ns.tbl", snapshot_id=99).collect()
    assert seen == [("scan", "loaded:ns.tbl", 99)]
    assert df.columns == ["column_1", "column_2"]


def test_tcode_dispatches_iceberg_source(tmp_path: Path) -> None:
    tc = Tcode.model_validate(
        {
            "config": tmp_path / "t.yaml",
            "store": tmp_path / "s.parquet",
            "source": {
                "kind": "iceberg",
                "catalog": {"uri": "sqlite:///catalog.db"},
                "table_identifier": "ns.tbl",
                "snapshot_id": 12,
                "url": ["https://example.org/iceberg"],
            },
            "statement": {
                "subject": {"method": "column", "encoding": "A"},
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "B"},
            },
            "provenance": {"repo": "PMID", "publication": "12345678"},
        }
    )
    ops = tc._source_ops()
    assert (iceberg, ({"uri": "sqlite:///catalog.db"}, "ns.tbl", 12)) in ops
