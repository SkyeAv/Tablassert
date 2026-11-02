"""A failed dedup pass must never destroy an earlier successful graph artifact."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tablassert.lib import dedup_stream


@pytest.mark.parametrize("is_edges", [False, True])
def test_malformed_input_preserves_existing_output(tmp_path: Path, is_edges: bool) -> None:
    """Parsing can fail after output creation; the last complete release must survive."""
    source = tmp_path / "graph.ndjson.tmp"
    destination = source.with_suffix("")
    source.write_text('{"id":"NCBIGene:1","name":"one"}\n{bad json}\n')
    destination.write_bytes(b"last complete release\n")
    before = set(tmp_path.iterdir())

    with pytest.raises(RuntimeError):
        dedup_stream(source, is_edges=is_edges)

    assert destination.read_bytes() == b"last complete release\n"
    assert source.exists()
    assert set(tmp_path.iterdir()) == before


def test_collision_preserves_existing_output(tmp_path: Path) -> None:
    """A declared-key collision must not replace the graph with a partial first record."""
    source = tmp_path / "graph.edges.ndjson.tmp"
    destination = source.with_suffix("")
    records = [
        {"subject": "NCBIGene:1", "object": "NCBIGene:2", "predicate": "biolink:related_to", "supporting_text": [text]}
        for text in ("first", "different")
    ]
    source.write_text("".join(json.dumps(record) + "\n" for record in records))
    destination.write_bytes(b"last complete release\n")

    with pytest.raises(RuntimeError, match="uuid-fields-not-a-key"):
        dedup_stream(source, is_edges=True, uuid_fields=["subject", "object", "predicate"])

    assert destination.read_bytes() == b"last complete release\n"
    assert source.exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["graph.edges.ndjson", "graph.edges.ndjson.tmp"]


def test_success_replaces_output_and_removes_input(tmp_path: Path) -> None:
    """Successful node dedup retains the original replacement and input-cleanup behavior."""
    source = tmp_path / "graph.nodes.ndjson.tmp"
    destination = source.with_suffix("")
    record = {"id": "NCBIGene:1", "name": "one"}
    source.write_text((json.dumps(record) + "\n") * 2)
    destination.write_bytes(b"old release\n")

    dedup_stream(source, is_edges=False)

    assert [json.loads(line) for line in destination.read_text().splitlines()] == [record]
    assert not source.exists()
    assert list(tmp_path.iterdir()) == [destination]


def test_merge_report_keeps_public_filename(tmp_path: Path) -> None:
    """Staging must not strand the fold report under a temporary output filename."""
    source = tmp_path / "graph.edges.ndjson.tmp"
    destination = source.with_suffix("")
    records = [{"subject": "NCBIGene:1", "object": "NCBIGene:2", "predicate": "biolink:related_to", "name": name} for name in ("first", "different")]
    source.write_text("".join(json.dumps(record) + "\n" for record in records))

    dedup_stream(source, is_edges=True, uuid_fields=["subject", "object", "predicate"], on_collision="merge")

    report = destination.with_name(destination.name + ".foldreport.json")
    assert json.loads(report.read_text()) == {"biolink:related_to": {"name": 1}}
    assert len(destination.read_text().splitlines()) == 1
    assert not source.exists()
    assert set(tmp_path.iterdir()) == {destination, report}


def test_write_failure_preserves_output_and_cleans_stage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failing native writer may leave partial bytes; staging must be cleaned on exceptions."""
    source = tmp_path / "graph.nodes.ndjson.tmp"
    destination = source.with_suffix("")
    source.write_text('{"id":"NCBIGene:1"}\n')
    destination.write_bytes(b"old release\n")

    def fail_write(_input: Path, output: Path, *_args: Any) -> tuple[int, int]:
        output.write_bytes(b"partial")
        raise OSError("disk write failed")

    monkeypatch.setattr("tablassert.lib.rs.dedup_ndjson", fail_write)
    with pytest.raises(OSError, match="disk write failed"):
        dedup_stream(source, is_edges=False)

    assert destination.read_bytes() == b"old release\n"
    assert set(tmp_path.iterdir()) == {source, destination}


def test_suffixless_input_is_rejected_without_data_loss(tmp_path: Path) -> None:
    """Removing a nonexistent suffix would alias input/output and unlink the newly published file."""
    source = tmp_path / "graph"
    source.write_bytes(b'{"id":"NCBIGene:1"}\n')
    before = source.read_bytes()

    with pytest.raises(ValueError, match="removable suffix"):
        dedup_stream(source, is_edges=False)

    assert source.read_bytes() == before
    assert list(tmp_path.iterdir()) == [source]
