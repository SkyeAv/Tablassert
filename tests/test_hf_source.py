from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest

from tablassert import extras
from tablassert.errors import MissingExtraError
from tablassert.lib import Tcode, hf
from tablassert.models import HFSource


def test_hf_source_model_defaults_url_and_pinning() -> None:
    src = HFSource.model_validate({"kind": "hf", "repo_id": "org/my-dataset", "revision": "abc123"})
    assert src.kind == "hf"
    assert src.split == "train"
    assert str(src.url[0]) == "https://huggingface.co/datasets/org/my-dataset"


def test_hf_reader_requires_extra_when_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extras, "find_spec", lambda name: None)
    with pytest.raises(MissingExtraError, match="datasets"):
        hf("org/my-dataset")


def test_hf_reader_renames_columns_positionally_and_forwards_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extras, "require", lambda *a, **_kw: None)
    seen: list[tuple[str, str | None, str, str | None]] = []

    def _fake_load_dataset(repo_id: str, **kwargs: Any) -> Any:
        seen.append((repo_id, kwargs.get("name"), kwargs["split"], kwargs.get("revision")))
        tbl = pa.table({"gene": ["BRCA1"], "disease": ["MONDO:0007254"]})
        return SimpleNamespace(data=SimpleNamespace(table=tbl))

    monkeypatch.setitem(sys.modules, "datasets", SimpleNamespace(load_dataset=_fake_load_dataset))
    df = hf("org/my-dataset", config="v1", split="test", revision="rev1").collect()
    assert seen == [("org/my-dataset", "v1", "test", "rev1")]
    assert df.columns == ["column_1", "column_2"]
    assert df["column_1"].to_list() == ["BRCA1"]


def test_tcode_dispatches_hf_source(tmp_path: Path) -> None:
    tc = Tcode.model_validate(
        {
            "config": tmp_path / "t.yaml",
            "store": tmp_path / "s.parquet",
            "source": {"kind": "hf", "repo_id": "org/my-dataset", "revision": "rev1"},
            "statement": {
                "subject": {"method": "column", "encoding": "A"},
                "predicate": "associated_with",
                "object": {"method": "column", "encoding": "B"},
            },
            "provenance": {"repo": "PMID", "publication": "12345678"},
        }
    )
    ops = tc._source_ops()
    assert (hf, ("org/my-dataset", None, "train", "rev1")) in ops
