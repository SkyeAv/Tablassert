"""Base-environment tests for the distill-weigh training-row command."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from tablassert.cli import distill_weigh
from tablassert.distill import RECORD_KEYS
from tablassert.distill_reward import SELECTION_KEYS, TRAIN_ROW_KEYS


def _record(run_id: str | None, *, purpose: str = "agent", call_index: int = 0, pmc_id: str = "PMC1") -> dict[str, object]:
    """Return a minimal schema-v2 record."""
    return {
        **dict.fromkeys(RECORD_KEYS),
        "record_type": "record",
        "schema_version": 2,
        "run_id": run_id,
        "purpose": purpose,
        "call_index": call_index,
        "messages": [{"role": "user", "content": "hello"}],
        "pmc_id": pmc_id,
    }


def _outcome(run_id: str | None, *, edge_count: int = 100, ok: bool = True, coverage: float = 1.0) -> dict[str, object]:
    """Return a complete rewardable outcome."""
    return {
        "record_type": "outcome",
        "schema_version": 2,
        "run_id": run_id,
        "timestamp": "fixed",
        "pmc_id": "PMC1",
        "model_id": "model",
        "run_status": "MAPPED",
        "ok": ok,
        "measured": True,
        "head": False,
        "coverage_pct": coverage,
        "best_coverage": coverage,
        "coverage_history_len": 1,
        "section_coverages_len": 1,
        "biolink_valid_pct": 1.0,
        "biolink_valid_pct_strict": 1.0,
        "demoted_edge_pct": 0.0,
        "node_count": 10,
        "edge_count": edge_count,
        "unresolved_count": 0,
        "predicate_advice_count": 0,
        "multivalued_suspect_count": 0,
        "error_codes": [],
        "attempts": 1,
        "config_chars": 10,
        "config_yaml_sha256": "hash",
        "provenance_ok": True,
        "qc_pass_rate": 1.0,
        "tool_calls": {"total": 1, "failed": 0, "wrong": 0, "redundant": 0},
        "tokens_total": 1,
        "steps": 1,
        "judge_score": None,
        "judge_dimensions": None,
        "gate": {"map_threshold": 0.25, "biolink_threshold": 0.0, "judge_threshold": None},
        "versions": {"tablassert": "18.0.0", "biolink_model": "4.4.4"},
    }


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    """Write deterministic JSON lines."""
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _corpus(tmp_path: Path, *, records: list[dict[str, object]] | None = None, outcomes: list[dict[str, object]] | None = None) -> Path:
    """Create a small records/outcomes corpus."""
    directory = tmp_path / "distill"
    directory.mkdir()
    _write(directory / "records.ndjson", records or [_record("run:PMC1"), _record("missing", pmc_id="PMC2")])
    _write(directory / "renamed.ndjson", outcomes or [_outcome("run:PMC1")])
    return directory


def test_distill_weigh_writes_a_weighted_dataset_and_manifest(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The command writes flat rows, last-wins outcomes, and reproducibility metadata.

    Why: this is the user-facing boundary where append-only capture becomes training data, so row
    cardinality, key shape, duplicate accounting, and manifest provenance must be checked together.
    """
    directory = _corpus(tmp_path, outcomes=[_outcome("run:PMC1", edge_count=10), _outcome("run:PMC1", edge_count=100)])
    output = tmp_path / "train" / "rows.ndjson"
    distill_weigh(distill_dir=directory, out=output)
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    manifest = json.loads(output.with_name("rows.ndjson.manifest.json").read_text(encoding="utf-8"))
    assert len(rows) == 2
    assert all(tuple(row) == TRAIN_ROW_KEYS for row in rows)
    assert rows[0]["outcome_edge_count"] == 100
    assert rows[1]["outcome_matched"] is False
    assert manifest["join_stats"] == {"records": 2, "outcomes": 2, "matched": 1, "unmatched": 1, "duplicate_run_ids": 1}
    assert manifest["edge_ref"] == 100.0
    assert manifest["edge_ref_source"] == "derived"
    assert manifest["rows_written"] == 2
    assert "selected" in capsys.readouterr().out


@pytest.mark.parametrize("policy", ["threshold", "best-of-n", "replication"])
def test_distill_weigh_supports_every_selection_policy(tmp_path: Path, policy: str) -> None:
    """Each documented selection policy writes the canonical annotations.

    Why: policy dispatch is a CLI boundary, so pure selector coverage alone would not catch a
    misspelled option, an omitted manifest policy, or a policy-specific write failure.
    """
    directory = _corpus(tmp_path)
    output = tmp_path / f"{policy}.ndjson"
    distill_weigh(distill_dir=directory, out=output, policy=policy)
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    manifest = json.loads(output.with_name(f"{policy}.ndjson.manifest.json").read_text(encoding="utf-8"))
    assert len(rows) == 2
    assert manifest["policy"] == policy
    assert all(set(SELECTION_KEYS) <= set(row) for row in rows)
    if policy == "best-of-n":
        assert sum(row["selected"] for row in rows) == 2
    if policy == "replication":
        assert sum(row["selected"] for row in rows) == 1


def test_distill_weigh_rejects_directory_outputs_and_manifest_collisions(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Directory destinations and manifest collisions fail before corpus processing.

    Why: deriving a manifest from an empty output name previously raised an uncaught ValueError,
    while collisions could overwrite the training artifact or feed derived files back as input.
    """
    directory = _corpus(tmp_path)
    with pytest.raises(SystemExit) as out_exc:
        distill_weigh(distill_dir=directory, out=Path("."))
    assert out_exc.value.code == 2
    assert "must name a file" in capsys.readouterr().err

    with pytest.raises(SystemExit) as dir_exc:
        distill_weigh(distill_dir=directory, out=tmp_path)
    assert dir_exc.value.code == 2
    assert "must name a file" in capsys.readouterr().err

    output = tmp_path / "rows.ndjson"
    with pytest.raises(SystemExit) as collision_exc:
        distill_weigh(distill_dir=directory, out=output, manifest=output)
    assert collision_exc.value.code == 2
    assert "different paths" in capsys.readouterr().err

    with pytest.raises(SystemExit) as manifest_exc:
        distill_weigh(distill_dir=directory, out=output, manifest=directory)
    assert manifest_exc.value.code == 2
    assert "must name a file" in capsys.readouterr().err


def test_distill_weigh_rejects_manifest_inside_input_dir(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A manifest nested under the raw corpus is rejected.

    Why: derived JSON beside append-only capture would be rediscovered by later corpus scans and
    make the same input directory change meaning after one successful weigh.
    """
    directory = _corpus(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        distill_weigh(distill_dir=directory, out=tmp_path / "rows.ndjson", manifest=directory / "manifest.json")
    assert exc_info.value.code == 2
    assert "inside --distill-dir" in capsys.readouterr().err


def test_distill_weigh_rejects_empty_record_and_outcome_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Empty raw files fail with a specific missing-content error.

    Why: an empty sink can result from a crashed or interrupted capture and must not produce a
    plausible training artifact with silently missing records or outcomes.
    """
    for empty_name, valid_name, expected in [
        ("records.ndjson", "outcomes.ndjson", "contain no records"),
        ("outcomes.ndjson", "records.ndjson", "no outcomes"),
    ]:
        directory = tmp_path / empty_name.replace(".ndjson", "")
        directory.mkdir()
        (directory / empty_name).write_text("", encoding="utf-8")
        _write(directory / valid_name, [_outcome("r")] if valid_name.startswith("outcome") else [_record("r")])
        with pytest.raises(SystemExit) as exc_info:
            distill_weigh(distill_dir=directory, out=tmp_path / f"{empty_name}.out.ndjson")
        assert exc_info.value.code == 2
        assert expected in capsys.readouterr().err


def test_distill_weigh_rejects_bad_reward_config(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """An invalid reward configuration exits 2 instead of falling back to defaults.

    Why: reward coefficients decide which rows train, so a typo or malformed file must be visible
    to the operator rather than silently changing selection semantics.
    """
    directory = _corpus(tmp_path)
    config = tmp_path / "bad.yaml"
    config.write_text("w_coverage: not-a-number\n", encoding="utf-8")
    with pytest.raises(SystemExit) as exc_info:
        distill_weigh(distill_dir=directory, out=tmp_path / "rows.ndjson", reward_config=config)
    assert exc_info.value.code == 2
    assert "Reward config invalid" in capsys.readouterr().err


def test_distill_weigh_records_edge_ref_overrides(tmp_path: Path) -> None:
    """Explicit and reward-config edge references resolve as overrides in the manifest.

    Why: the breadth denominator changes reward values and therefore selection; the manifest must
    make whether it was derived or overridden auditable for both supported override surfaces.
    """
    directory = _corpus(tmp_path)
    explicit = tmp_path / "explicit.ndjson"
    distill_weigh(distill_dir=directory, out=explicit, edge_ref=25.0)
    explicit_manifest = json.loads(explicit.with_name("explicit.ndjson.manifest.json").read_text(encoding="utf-8"))
    assert explicit_manifest["edge_ref"] == 25.0
    assert explicit_manifest["edge_ref_source"] == "overridden"

    config = tmp_path / "reward.yaml"
    config.write_text("edge_ref: 25\n", encoding="utf-8")
    configured = tmp_path / "configured.ndjson"
    distill_weigh(distill_dir=directory, out=configured, reward_config=config)
    configured_manifest = json.loads(configured.with_name("configured.ndjson.manifest.json").read_text(encoding="utf-8"))
    assert configured_manifest["edge_ref"] == 25.0
    assert configured_manifest["edge_ref_source"] == "overridden"


def test_distill_weigh_retains_unrankable_final_call_rows(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Final-call filtering retains rows lacking a provable ranking pair.

    Why: null/non-integer call metadata and non-string run IDs cannot prove that a row is not
    final, so silently dropping them would violate the retained-unmatched invariant.
    """
    unrankable_run = _record(None)
    non_string_run = _record("r2")
    non_string_run["run_id"] = 42
    directory = _corpus(tmp_path, records=[_record("run:PMC1", call_index=0), _record("run:PMC1", call_index=1), unrankable_run, non_string_run])
    output = tmp_path / "rows.ndjson"
    distill_weigh(distill_dir=directory, out=output, final_call_only=True)
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    manifest = json.loads(output.with_name("rows.ndjson.manifest.json").read_text(encoding="utf-8"))
    assert [row["call_index"] for row in rows] == [1, 0, 0]
    assert manifest["unrankable_final_call"] == 2
    assert "cannot be proven non-final" in capsys.readouterr().err


def test_distill_weigh_rejects_derived_and_mixed_input_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Derived markers and mixed record/outcome files are rejected by content inspection.

    Why: filename-based partitioning cannot distinguish a prior weighted artifact or a concatenated
    sink, and silently joining either can duplicate rows or misclassify outcome data as records.
    """
    directory = _corpus(tmp_path)
    _write(directory / "derived.ndjson", [{"record_type": "record", "weight": 0.5, "selected": True, "policy": "threshold", "outcome_matched": True}])
    with pytest.raises(SystemExit) as derived_exc:
        distill_weigh(distill_dir=directory, out=tmp_path / "derived-out.ndjson")
    assert derived_exc.value.code == 2
    assert "derived training output" in capsys.readouterr().err

    mixed = tmp_path / "mixed"
    mixed.mkdir()
    _write(mixed / "records.ndjson", [_record("r"), _outcome("r")])
    with pytest.raises(SystemExit) as mixed_exc:
        distill_weigh(distill_dir=mixed, out=tmp_path / "mixed-out.ndjson")
    assert mixed_exc.value.code == 2
    assert "mixed record/outcome" in capsys.readouterr().err


def test_distill_weigh_manifest_uses_canonical_counts_and_absolute_sources(tmp_path: Path) -> None:
    """The manifest has one canonical count spelling and absolute source provenance.

    Why: stable names prevent downstream readers from choosing between duplicate aliases, while
    resolved paths make manifests comparable when the command is invoked from different directories.
    """
    directory = _corpus(tmp_path)
    output = tmp_path / "rows.ndjson"
    distill_weigh(distill_dir=directory, out=output)
    manifest = json.loads(output.with_name("rows.ndjson.manifest.json").read_text(encoding="utf-8"))
    assert "selected_count" in manifest
    assert "selected" not in manifest
    assert "unmatched_count" in manifest
    assert "unmatched" not in manifest
    assert set(manifest["distinct"]) == {"pmc_id", "config_yaml_sha256"}
    assert all(Path(path).is_absolute() for path in manifest["source_files"])
    assert all(Path(path).is_absolute() for paths in manifest["source_files_by_kind"].values() for path in paths)


def test_distill_weigh_manifest_contains_diversity_and_join_counts(tmp_path: Path) -> None:
    """The manifest exposes the resolved diversity and join accounting needed for audit.

    Why: selection can reduce prompt/config diversity even when row counts look healthy, so both
    before/after nested counters and canonical selected/unmatched totals must be machine-readable.
    """
    directory = _corpus(tmp_path)
    output = tmp_path / "rows.ndjson"
    distill_weigh(distill_dir=directory, out=output)
    manifest = json.loads(output.with_name("rows.ndjson.manifest.json").read_text(encoding="utf-8"))
    assert manifest["selected_count"] == 1
    assert manifest["unmatched_count"] == 1
    assert manifest["distinct"]["pmc_id"] == {"before_selection": 2, "after_selection": 1}
    assert manifest["distinct"]["config_yaml_sha256"] == {"before_selection": 1, "after_selection": 1}


def test_distill_weigh_writes_unmatched_rows_with_zero_weight(tmp_path: Path) -> None:
    """Unmatched records remain visible as unselected zero-weight rows.

    Why: unmatched rows are audit data rather than training candidates; dropping them or marking
    them selectable would hide corpus-join problems and corrupt downstream selection counts.
    """
    directory = _corpus(tmp_path)
    output = tmp_path / "rows.ndjson"
    distill_weigh(distill_dir=directory, out=output)
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    unmatched = [row for row in rows if row["outcome_matched"] is False]
    assert len(rows) == 2
    assert len(unmatched) == 1
    assert unmatched[0]["weight"] == 0.0
    assert unmatched[0]["selected"] is False
    assert unmatched[0]["replicas"] == 0


def test_distill_weigh_manifest_source_paths_survive_a_relative_invocation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A relative ``--distill-dir`` still yields resolved absolute manifest source paths.

    Why: the manifest is the reproducibility record for a training artifact, so two identical runs
    launched from different working directories must produce byte-comparable provenance.
    """
    directory = _corpus(tmp_path)
    other_cwd = tmp_path / "elsewhere"
    other_cwd.mkdir()
    monkeypatch.chdir(other_cwd)
    output = tmp_path / "rows.ndjson"
    distill_weigh(distill_dir=Path("../distill"), out=output)
    manifest = json.loads(output.with_name("rows.ndjson.manifest.json").read_text(encoding="utf-8"))
    assert manifest["source_files"] == [str(directory / "records.ndjson"), str(directory / "renamed.ndjson")]
    assert manifest["source_files_by_kind"]["records"] == [str(directory / "records.ndjson")]
    assert manifest["source_files_by_kind"]["outcomes"] == [str(directory / "renamed.ndjson")]


def test_distill_weigh_requires_no_optional_extra() -> None:
    """The command source has no optional-extra preflight or gated dependency.

    Why: weighing is deliberately available in the base installation; requiring the distill extra
    here would prevent the core path from producing the NDJSON consumed by later export steps.
    """
    source = Path(__file__).parents[1] / "src/tablassert/cli.py"
    text = source.read_text(encoding="utf-8")
    command = text[text.index('@APP.command(name="distill-weigh")') : text.index('@APP.command(name="distill-export")')]
    assert "extras.require" not in command
    assert "extras.is_installed" not in command


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("missing", "existing directory"),
        ("empty", "no .ndjson"),
        ("records_only", "no outcomes"),
        ("outcomes_only", "only outcome"),
        ("malformed", "malformed NDJSON"),
        ("unmatched", "pre-v2 corpus"),
        ("bad_policy", "unknown --policy"),
        ("bad_threshold", "--threshold"),
        ("bad_top_n", "--top-n"),
        ("bad_replication", "--replication-k"),
        ("bad_purpose", "unrecognized --purpose"),
        ("bad_edge_ref", "--edge-ref"),
        ("inside", "loads every *.ndjson"),
    ],
)
def test_distill_weigh_fails_loud_on_every_bad_input(tmp_path: Path, capsys: pytest.CaptureFixture[str], case: str, expected: str) -> None:
    """Every specified malformed corpus or knob exits 2 with actionable stderr.

    Why: silently producing a plausible-looking zero-weight file is worse than rejecting a torn,
    stale, or misconfigured corpus because it can contaminate a training run undetected.
    """
    directory = tmp_path / "distill"
    output = tmp_path / "out.ndjson"
    kwargs: dict[str, object] = {"distill_dir": directory, "out": output}
    if case == "missing":
        pass
    elif case == "empty":
        directory.mkdir()
    elif case == "records_only":
        directory.mkdir()
        _write(directory / "records.ndjson", [_record("r")])
    elif case == "outcomes_only":
        directory.mkdir()
        _write(directory / "outcomes.ndjson", [_outcome("r")])
    elif case == "malformed":
        directory.mkdir()
        (directory / "records.ndjson").write_text('{"record_type":"record"}\nnot-json\n', encoding="utf-8")
        _write(directory / "outcomes.ndjson", [_outcome("r")])
    elif case == "unmatched":
        directory = _corpus(tmp_path, records=[_record("missing")], outcomes=[_outcome("other")])
        kwargs["distill_dir"] = directory
    elif case == "bad_policy":
        directory = _corpus(tmp_path)
        kwargs.update(distill_dir=directory, policy="wat")
    elif case == "bad_threshold":
        directory = _corpus(tmp_path)
        kwargs.update(distill_dir=directory, threshold=2.0)
    elif case == "bad_top_n":
        directory = _corpus(tmp_path)
        kwargs.update(distill_dir=directory, top_n=0)
    elif case == "bad_replication":
        directory = _corpus(tmp_path)
        kwargs.update(distill_dir=directory, replication_k=4)
    elif case == "bad_purpose":
        directory = _corpus(tmp_path)
        kwargs.update(distill_dir=directory, purpose="other")
    elif case == "bad_edge_ref":
        directory = _corpus(tmp_path)
        kwargs.update(distill_dir=directory, edge_ref=0.0)
    else:
        directory = _corpus(tmp_path)
        kwargs.update(distill_dir=directory, out=directory / "train.ndjson")
    with pytest.raises(SystemExit) as exc_info:
        distill_weigh(**kwargs)  # type: ignore[arg-type]
    assert exc_info.value.code == 2
    assert expected.lower() in capsys.readouterr().err.lower()


def test_distill_weigh_rejects_out_inside_distill_dir(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """An output nested under the corpus is rejected before any write.

    Why: distill-export globs every NDJSON in its input directory, so accepting this path would
    make a later export load both raw records and derived rows as if they were one corpus.
    """
    directory = _corpus(tmp_path)
    with pytest.raises(SystemExit):
        distill_weigh(distill_dir=directory, out=directory / "train.ndjson")
    assert "loads every *.ndjson" in capsys.readouterr().err


def test_distill_weigh_fails_loud_when_no_record_joins_an_outcome(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A nonempty but wholly unmatched corpus is rejected.

    Why: an all-zero dataset can look successful while actually indicating a pre-v2 corpus or a
    crashed run whose outcome sink was never written.
    """
    directory = _corpus(tmp_path, records=[_record("missing")], outcomes=[_outcome("other")])
    with pytest.raises(SystemExit):
        distill_weigh(distill_dir=directory, out=tmp_path / "rows.ndjson")
    assert "pre-v2" in capsys.readouterr().err


def test_distill_weigh_default_purpose_is_agent_and_all_disables_the_filter(tmp_path: Path) -> None:
    """The default excludes judge rows while purpose=all includes them.

    Why: judge and reflexion conversations are distinct trajectories and should not silently enter
    the default supervised corpus, while an explicit all-purpose request must remain available.
    """
    records = [_record("run:PMC1", purpose="agent"), _record("run:PMC1", purpose="judge", call_index=1)]
    directory = _corpus(tmp_path, records=records)
    default_out = tmp_path / "default.ndjson"
    all_out = tmp_path / "all.ndjson"
    distill_weigh(distill_dir=directory, out=default_out)
    distill_weigh(distill_dir=directory, out=all_out, purpose="all")
    assert len(default_out.read_text(encoding="utf-8").splitlines()) == 1
    assert len(all_out.read_text(encoding="utf-8").splitlines()) == 2


def test_distill_weigh_final_call_only_keeps_the_highest_call_index(tmp_path: Path) -> None:
    """Final-call filtering retains only the highest call_index per run.

    Why: the final conversation is the most complete trajectory and intermediate calls can contain
    recovery noise that should not be mixed into the default training artifact.
    """
    directory = _corpus(tmp_path, records=[_record("run:PMC1", call_index=0), _record("run:PMC1", call_index=2)])
    output = tmp_path / "rows.ndjson"
    distill_weigh(distill_dir=directory, out=output, final_call_only=True)
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert rows[0]["call_index"] == 2


def test_distill_weigh_is_byte_reproducible_across_runs(tmp_path: Path) -> None:
    """Identical inputs produce byte-identical output and manifest files.

    Why: reproducibility lets an operator audit selection changes as policy/config changes rather
    than confusing timestamp or filesystem noise with a training-data change.
    """
    directory = _corpus(tmp_path)
    first = tmp_path / "first.ndjson"
    second = tmp_path / "second.ndjson"
    distill_weigh(distill_dir=directory, out=first)
    distill_weigh(distill_dir=directory, out=second)
    assert hashlib.sha256(first.read_bytes()).digest() == hashlib.sha256(second.read_bytes()).digest()
    assert (
        hashlib.sha256(first.with_name("first.ndjson.manifest.json").read_bytes()).digest()
        == hashlib.sha256(second.with_name("second.ndjson.manifest.json").read_bytes()).digest()
    )


def test_distill_weigh_warns_and_records_null_edge_ref(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Head-only or otherwise incomparable outcomes warn and retain a null edge reference.

    Why: a corpus with no comparable full build is still useful, but breadth must not acquire a
    fabricated denominator that changes its reward silently.
    """
    outcome = _outcome("run:PMC1", edge_count=0)
    outcome["head"] = True
    directory = _corpus(tmp_path, outcomes=[outcome])
    output = tmp_path / "rows.ndjson"
    distill_weigh(distill_dir=directory, out=output)
    manifest = json.loads(output.with_name("rows.ndjson.manifest.json").read_text(encoding="utf-8"))
    assert manifest["edge_ref"] is None
    assert "breadth contributes 0.0" in capsys.readouterr().err


def test_distill_weigh_applies_a_reward_config_file(tmp_path: Path) -> None:
    """A YAML reward configuration changes the resolved reward policy in the manifest.

    Why: operators must be able to retune data selection without re-recording the append-only
    corpus, while still retaining the exact resolved configuration beside the output.
    """
    directory = _corpus(tmp_path)
    config = tmp_path / "reward.yaml"
    config.write_text("w_coverage: 0.0\nw_biolink: 0.28\nw_specificity: 0.17\nw_cleanliness: 0.07\nw_breadth: 0.48\n", encoding="utf-8")
    output = tmp_path / "rows.ndjson"
    distill_weigh(distill_dir=directory, out=output, reward_config=config)
    manifest = json.loads(output.with_name("rows.ndjson.manifest.json").read_text(encoding="utf-8"))
    assert manifest["reward_config"]["w_coverage"] == 0.0
    assert manifest["reward_config"]["w_breadth"] == 0.48


def test_weighed_ndjson_round_trips_through_distill_export(tmp_path: Path) -> None:
    """A weighed ``train.ndjson`` exports as-is with no extra flag (REQ-EXP-7 composition).

    Why: ``distill-weigh`` deliberately emits the full canonical key set on every row (unknown
    record keys appended after it), so the export must load that directory without a schema flag,
    without re-joining, and without treating the sidecar manifest (``*.json``) as corpus input.
    """
    pytest.importorskip("datasets")
    from datasets import load_from_disk  # pyright: ignore[reportMissingImports]

    from tablassert.cli import distill_export

    corpus: Path = _corpus(tmp_path)  # two records + one outcome under <tmp>/distill
    train: Path = tmp_path / "train.ndjson"
    distill_weigh(distill_dir=corpus, out=train)
    hf_dataset: Path = tmp_path / "hf-dataset"

    distill_export(distill_dir=tmp_path, out=hf_dataset)  # the weighed directory IS the export input

    dataset = load_from_disk(str(hf_dataset))
    assert len(dataset) == 2  # both weighed rows; the manifest and the raw corpus directory were not loaded
    assert "weight" in dataset.column_names
    assert "selected" in dataset.column_names
    assert sorted(path.name for path in tmp_path.iterdir()) == ["distill", "hf-dataset", "train.ndjson", "train.ndjson.manifest.json"]
