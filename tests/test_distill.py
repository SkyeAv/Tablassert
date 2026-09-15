"""``--distill`` capture: the ChatML NDJSON recorder, the generate-wrapping model, and export.

The recorder tests are PURE Python (base env, no ``importorskip``): ``tablassert.distill`` is
zero-dependency by design and serializes duck-typed messages. The wrapping-model tests drive the
real ``make_distilling_model`` over ``make_fake_model`` and so skip cleanly without the ``[agent]``
extra; the export test skips without ``[distill]``.
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from tablassert import distill
from tablassert.agent import capture_run_outcome, distill_dir, make_distilling_model, make_fake_model


class _Msg:
    """ChatMessage-shaped duck type (``role``/``content``/optional ``token_usage``)."""

    def __init__(self, role: object, content: object, token_usage: object = None) -> None:
        self.role = role
        self.content = content
        self.token_usage = token_usage


class _Role:
    """Enum-shaped role (MessageRole carries a ``value``)."""

    def __init__(self, value: str) -> None:
        self.value = value


class _Usage:
    def __init__(self, input_tokens: int, output_tokens: int) -> None:
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens


class _CapturingLogger:
    """Stand-in for the module ``logger`` that records the ``warning`` messages it is handed."""

    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.warnings.append(message)


def _read_records(path: Path) -> list[dict[str, Any]]:
    """Parse an NDJSON file into a list of records, asserting strict one-object-per-line."""
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_serialize_message_accepts_dicts_and_chatmessage_shapes() -> None:
    """Both plain dicts and ChatMessage-shaped objects serialize to ChatML role/content.

    Why: the wrapper sees smolagents ``ChatMessage`` objects, but tests and defensive callers may
    hand in dicts; both must land in the same ``{"role", "content"}`` shape Studio auto-detects.
    """
    assert distill.serialize_message({"role": "user", "content": "hi"}) == {"role": "user", "content": "hi"}
    assert distill.serialize_message(_Msg("assistant", "<code>...</code>")) == {"role": "assistant", "content": "<code>...</code>"}


def test_serialize_message_unwraps_enum_roles_and_stringifies_content() -> None:
    """A ``MessageRole`` enum serializes to its plain value; non-string content is stringified.

    Why: ``str(MessageRole.USER)`` would leak ``"MessageRole.USER"`` into the training corpus,
    and multimodal part-lists would serialize as Python reprs — both poison a chat template.
    """
    record: dict[str, str] = distill.serialize_message(_Msg(_Role("system"), ["part-a", "part-b"]))
    assert record["role"] == "system"
    assert isinstance(record["content"], str)


def test_serialize_token_usage_extracts_counts_or_none() -> None:
    """Token usage rides along when the response carries it, else the field is ``None``."""
    assert distill.serialize_token_usage(_Msg("assistant", "x", _Usage(10, 5))) == {"input_tokens": 10, "output_tokens": 5}
    assert distill.serialize_token_usage(_Msg("assistant", "x")) is None
    assert distill.serialize_token_usage(None) is None


def test_recorder_writes_complete_chatml_records(tmp_path: Path) -> None:
    """Each record is one JSON line: full messages + appended assistant response + metadata.

    Why: Unsloth Studio auto-maps a top-level ``messages`` column on JSONL upload ONLY when every
    line is a standalone object (no outer array), and the assistant reply must be the final
    message for the line to be a complete SFT example.
    """
    path: Path = tmp_path / "nested" / distill.RECORDS_FILENAME  # parent created lazily
    recorder = distill.DistillRecorder(path)
    recorder.record(
        "agent",
        [_Msg(_Role("system"), "You derive configs."), _Msg("user", "Derive PMC1.")],
        _Msg(_Role("assistant"), "<code>final_answer(...)</code>", _Usage(100, 20)),
        pmc_id="PMC1",
        model_id="big-model",
    )

    records = _read_records(path)
    assert len(records) == 1
    record = records[0]
    assert [m["role"] for m in record["messages"]] == ["system", "user", "assistant"]
    assert record["messages"][-1]["content"] == "<code>final_answer(...)</code>"
    assert record["purpose"] == "agent"
    assert record["pmc_id"] == "PMC1"
    assert record["model_id"] == "big-model"
    assert record["call_index"] == 0
    assert record["token_usage"] == {"input_tokens": 100, "output_tokens": 20}
    assert isinstance(record["timestamp"], str)


def test_recorder_appends_across_invocations(tmp_path: Path) -> None:
    """A SECOND recorder over the same file appends — the dataset accumulates run over run.

    Why: the fine-tuning corpus is built by repeated ``tablassert agent --distill`` invocations;
    each constructs a fresh ``DistillRecorder``, so append (never truncate) is the load-bearing
    behavior. ``call_index`` restarts per invocation; ``timestamp`` disambiguates runs.
    """
    path: Path = tmp_path / distill.RECORDS_FILENAME
    first = distill.DistillRecorder(path)
    first.record("agent", [_Msg("user", "run one")], _Msg("assistant", "a"), pmc_id="PMC1")
    second = distill.DistillRecorder(path)  # a later invocation
    second.record("judge", [_Msg("user", "run two")], _Msg("assistant", "b"))

    records = _read_records(path)
    assert len(records) == 2
    assert [r["purpose"] for r in records] == ["agent", "judge"]
    assert [r["call_index"] for r in records] == [0, 0]  # per-invocation counter


def test_recorder_without_response_still_records(tmp_path: Path) -> None:
    """A call with no response object records the prompt messages alone (best-effort)."""
    path: Path = tmp_path / distill.RECORDS_FILENAME
    distill.DistillRecorder(path).record("reflexion", [_Msg("user", "propose an edit")])
    (record,) = _read_records(path)
    assert [m["role"] for m in record["messages"]] == ["user"]
    assert record["token_usage"] is None


def test_recorder_never_raises_into_the_run(tmp_path: Path) -> None:
    """An unwritable sink (path IS a directory) logs and swallows instead of breaking a batch.

    Why: recording is observability, not pipeline logic — a distillation failure must never skip
    an article mid-supervisor-run.
    """
    recorder = distill.DistillRecorder(tmp_path)  # opening a directory for append fails
    recorder.record("agent", [_Msg("user", "x")], _Msg("assistant", "y"))  # must not raise


def test_record_key_set_is_canonical_and_fully_emitted(tmp_path: Path) -> None:
    """Every record line carries EVERY ``RECORD_KEYS`` key, in that order, ``null`` when unknown.

    Why: ``datasets.load_dataset("json")`` infers features from the first block of the FIRST file
    and raises ``CastError`` once a later block of this append-only corpus introduces a column the
    inferred schema lacks. Emitting the full key set with explicit ``null``s is the only mitigation
    that survives appending across versions, so the tuple and its exhaustive emission are pinned.
    """
    assert distill.RECORD_KEYS == (
        "record_type",
        "schema_version",
        "run_id",
        "timestamp",
        "purpose",
        "call_index",
        "messages",
        "token_usage",
        "input_tokens",
        "output_tokens",
        "n_messages",
        "pmc_id",
        "model_id",
    )
    assert (distill.SCHEMA_VERSION, distill.RECORD_TYPE_RECORD, distill.RECORD_TYPE_OUTCOME) == (2, "record", "outcome")
    assert (distill.RECORDS_FILENAME, distill.OUTCOMES_FILENAME) == ("records.ndjson", "outcomes.ndjson")

    path: Path = tmp_path / distill.RECORDS_FILENAME
    distill.DistillRecorder(path).record("judge", [_Msg("user", "score this")])  # no meta, no response: maximal unknowns

    (record,) = _read_records(path)
    assert tuple(record) == distill.RECORD_KEYS  # ORDER is asserted, not just membership
    assert record["record_type"] == "record"
    assert record["schema_version"] == 2
    for absent in ("run_id", "token_usage", "input_tokens", "output_tokens", "pmc_id", "model_id"):
        assert absent in record, f"{absent} must be an explicit null, never omitted"
        assert record[absent] is None, f"{absent} must be null when unknown"


def test_records_are_schema_uniform_across_purposes(tmp_path: Path) -> None:
    """``agent``, ``judge`` and ``reflexion`` lines in one file share an IDENTICAL key set.

    Why: this pins the latent v1 drift bug. ``agent.py`` records with ``meta={"pmc_id": ...}`` while
    ``cli.py`` wraps the judge and reflexion models with NO meta, so v1 wrote two different key sets
    into the same append-only file (plus a third when ``model_id`` was unrecoverable). Mixed key
    sets are exactly what makes ``distill-export`` die with a ``CastError`` later.
    """
    path: Path = tmp_path / distill.RECORDS_FILENAME
    recorder = distill.DistillRecorder(path)
    recorder.record("agent", [_Msg("user", "derive")], _Msg("assistant", "ok"), pmc_id="PMC1", model_id="big")
    recorder.record("judge", [_Msg("user", "score")], _Msg("assistant", "8"))  # mirrors cli.py's judge wrap: no meta
    recorder.record("reflexion", [_Msg("user", "reflect")], _Msg("assistant", "edit"))  # mirrors cli.py's reflexion wrap

    records = _read_records(path)
    assert [r["purpose"] for r in records] == ["agent", "judge", "reflexion"]
    assert {tuple(r) for r in records} == {distill.RECORD_KEYS}  # one and only one key order across all purposes
    assert [r["pmc_id"] for r in records] == ["PMC1", None, None]


def test_run_id_is_stamped_per_begin_run_scope(tmp_path: Path) -> None:
    """``run_id`` is ``null`` before ``begin_run``, then ``"<invocation_id>:<pmc_id>"`` per article.

    Why: ``run_id`` is the join key between a training row and the outcome of the run that produced
    it, at the (invocation, PMC article) granularity an outcome actually exists at. An injectable
    ``invocation_id`` keeps that deterministic in tests; rows recorded outside a run scope must stay
    writable (recording never fails a run) and simply be unjoinable.
    """
    path: Path = tmp_path / distill.RECORDS_FILENAME
    recorder = distill.DistillRecorder(path, invocation_id="a1b2c3d4e5f6")
    recorder.record("agent", [_Msg("user", "before any run")])  # outside a begin_run scope
    assert recorder.begin_run("PMC1") == "a1b2c3d4e5f6:PMC1"
    recorder.record("agent", [_Msg("user", "first article")])
    assert recorder.begin_run("PMC2") == "a1b2c3d4e5f6:PMC2"  # a new scope replaces the old one
    recorder.record("judge", [_Msg("user", "second article")])

    assert [r["run_id"] for r in _read_records(path)] == [None, "a1b2c3d4e5f6:PMC1", "a1b2c3d4e5f6:PMC2"]
    assert not (tmp_path / "unexpected").exists()  # begin_run does no I/O
    generated = distill.DistillRecorder(tmp_path / "other.ndjson")
    assert generated.run_id is None
    assert len(generated.invocation_id) == 12  # a generated id is a 12-hex-char uuid4 slice
    assert generated.invocation_id != distill.DistillRecorder(tmp_path / "o2.ndjson").invocation_id


def test_v2_record_preserves_v1_values(tmp_path: Path) -> None:
    """For an unchanged caller, every v1 key keeps its v1 name, type and meaning; v2 is additive.

    Why: the corpus is append-only, so v1 lines already on disk must stay readable next to v2 lines.
    Renaming or retyping any carried-over column would demote it to a JSON-encoded string under
    ``datasets``' ``on_mixed_types = "use_json"`` — a silent corruption rather than a loud failure.
    """
    path: Path = tmp_path / distill.RECORDS_FILENAME
    distill.DistillRecorder(path).record(
        "agent",
        [_Msg(_Role("system"), "You derive configs."), _Msg("user", "Derive PMC1.")],
        _Msg(_Role("assistant"), "<code>final_answer(...)</code>", _Usage(100, 20)),
        pmc_id="PMC1",
        model_id="big-model",
    )

    (record,) = _read_records(path)
    assert record["messages"] == [
        {"role": "system", "content": "You derive configs."},
        {"role": "user", "content": "Derive PMC1."},
        {"role": "assistant", "content": "<code>final_answer(...)</code>"},
    ]
    assert record["purpose"] == "agent"
    assert record["call_index"] == 0
    assert isinstance(record["call_index"], int)
    assert record["token_usage"] == {"input_tokens": 100, "output_tokens": 20}
    assert record["pmc_id"] == "PMC1"
    assert record["model_id"] == "big-model"
    assert isinstance(record["timestamp"], str)
    assert record["timestamp"].endswith("+00:00")  # still a tz-aware UTC ISO-8601 stamp


def test_record_derives_n_messages_and_token_scalars(tmp_path: Path) -> None:
    """``n_messages`` counts the messages INCLUDING the appended assistant turn; token scalars mirror ``token_usage``.

    Why: TRL's ``SFTConfig.max_length`` defaults to 1024 with ``truncation_mode="keep_start"`` and
    then DROPS examples left fully masked, so a multi-turn CodeAgent trajectory can vanish silently.
    Per-example size must be queryable without re-parsing ``messages``, and the flat ``int|null``
    mirrors spare a ``datasets`` consumer from reaching inside the ``token_usage`` struct column.
    """
    path: Path = tmp_path / distill.RECORDS_FILENAME
    recorder = distill.DistillRecorder(path)
    recorder.record("agent", [_Msg("system", "s"), _Msg("user", "u")], _Msg("assistant", "a", _Usage(100, 20)))
    recorder.record("agent", [_Msg("user", "u")])  # no response at all: no assistant turn, no usage

    with_response, without_response = _read_records(path)
    assert with_response["n_messages"] == 3 == len(with_response["messages"])
    assert (with_response["input_tokens"], with_response["output_tokens"]) == (100, 20)
    assert without_response["n_messages"] == 1 == len(without_response["messages"])
    assert without_response["token_usage"] is None
    assert (without_response["input_tokens"], without_response["output_tokens"]) == (None, None)


def test_unknown_meta_key_is_written_and_warned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A meta key outside ``RECORD_KEYS`` is still written, but logs a warning naming it.

    Why: the extra column is the extensibility escape hatch, yet it is ALSO the exact shape that
    breaks an append-only ``datasets`` load — a key that first appears mid-corpus. A named warning
    is the schema-drift canary; refusing the key, or raising, would violate the fail-soft contract.
    """
    cap = _CapturingLogger()
    monkeypatch.setattr(distill, "logger", cap)
    path: Path = tmp_path / distill.RECORDS_FILENAME
    recorder = distill.DistillRecorder(path)

    recorder.record("agent", [_Msg("user", "x")], pmc_id="PMC1", attempt_no=3)  # pmc_id is canonical, attempt_no is not

    (record,) = _read_records(path)
    assert record["attempt_no"] == 3
    assert record["pmc_id"] == "PMC1"
    assert tuple(record)[: len(distill.RECORD_KEYS)] == distill.RECORD_KEYS  # the canonical block stays first and intact
    assert len(cap.warnings) == 1
    assert "attempt_no" in cap.warnings[0]
    assert "pmc_id" not in cap.warnings[0]  # a canonical meta key must NOT warn


def test_record_outcome_writes_a_sibling_file_and_leaves_records_alone(tmp_path: Path) -> None:
    """``record_outcome`` appends to ``outcomes.ndjson`` beside ``records.ndjson`` — and ONLY there.

    Why: ``distill-export`` globs ``*.ndjson`` and Unsloth Studio ingests ``records.ndjson`` directly
    as ChatML, so an outcome row interleaved into the training file would corrupt both consumers and
    change the meaning of a file existing corpora already depend on. A sibling file keeps the corpus
    pure and makes the join on ``run_id`` an explicit later step; the training file must be byte-
    identical before and after an outcome lands.
    """
    records_path: Path = tmp_path / "nested" / distill.RECORDS_FILENAME  # parent created lazily
    outcomes_path: Path = records_path.parent / distill.OUTCOMES_FILENAME
    recorder = distill.DistillRecorder(records_path, invocation_id="a1b2c3d4e5f6")
    assert recorder.begin_run("PMC11708054") == "a1b2c3d4e5f6:PMC11708054"
    recorder.record("agent", [_Msg("user", "derive")], _Msg("assistant", "done"), pmc_id="PMC11708054")
    assert not outcomes_path.exists()  # a training record never creates the outcome sink
    before: bytes = records_path.read_bytes()

    recorder.record_outcome({"run_status": "MAPPED", "ok": True, "coverage_pct": 0.83, "error_codes": []})

    assert outcomes_path.parent == records_path.parent  # sibling: same directory, different file
    assert records_path.read_bytes() == before  # append-only: not rewritten, migrated or reordered
    assert [r["record_type"] for r in _read_records(records_path)] == ["record"]  # no outcome row leaked in
    assert recorder.call_index == 1  # an outcome is not a training record
    (outcome,) = _read_records(outcomes_path)
    assert (outcome["record_type"], outcome["schema_version"]) == ("outcome", 2)
    assert outcome["run_id"] == "a1b2c3d4e5f6:PMC11708054"  # inherited from the open begin_run scope
    assert (outcome["run_status"], outcome["ok"], outcome["coverage_pct"], outcome["error_codes"]) == ("MAPPED", True, 0.83, [])


def test_record_outcome_never_raises_and_does_not_advance_call_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed outcome write warns and returns; a successful one never advances ``call_index``.

    Why: outcome capture is observability, so an unwritable sink or an unserializable payload must
    never break a supervisor run (a crashed run's negative example matters more than the line). And
    ``call_index`` exists so downstream filtering can keep each run's final record — an outcome line
    counting as a record would silently shift that index on every run.
    """
    cap = _CapturingLogger()
    monkeypatch.setattr(distill, "logger", cap)

    blocker: Path = tmp_path / "blocker"  # a regular file where the sink's PARENT directory belongs
    blocker.write_text("not a directory", encoding="utf-8")
    broken = distill.DistillRecorder(blocker / distill.RECORDS_FILENAME)
    broken.record_outcome({"run_status": "SKIPPED"})  # unwritable sink: must not raise
    assert not (tmp_path / distill.OUTCOMES_FILENAME).exists()  # nothing stray written outside the blocker
    assert broken.call_index == 0
    assert [warning for warning in cap.warnings if "outcome" in warning]  # the failure is named, not silent

    cap.warnings.clear()
    circular: list[Any] = []  # a circular reference defeats json.dumps even with default=str
    circular.append(circular)
    path: Path = tmp_path / "distill" / distill.RECORDS_FILENAME
    recorder = distill.DistillRecorder(path)
    recorder.record("agent", [_Msg("user", "one")], _Msg("assistant", "a"))
    recorder.record("agent", [_Msg("user", "two")], _Msg("assistant", "b"))
    recorder.record_outcome({"judge_dimensions": circular})  # unserializable payload: must not raise
    assert recorder.call_index == 2  # a failed outcome write is not a record either
    assert [warning for warning in cap.warnings if "outcome" in warning]

    recorder.begin_run("PMC1")
    recorder.record_outcome({"run_status": "MAPPED"})
    recorder.record_outcome({"run_status": "SKIPPED"})
    recorder.record("agent", [_Msg("user", "three")], _Msg("assistant", "c"))
    assert recorder.call_index == 3  # two outcomes interleaved, still three records
    assert [r["call_index"] for r in _read_records(path)] == [0, 1, 2]  # RECORDS only, per invocation
    assert len(_read_records(recorder.outcome_path)) == 2


def test_outcome_key_set_is_canonical_and_fully_emitted(tmp_path: Path) -> None:
    """Every outcome line carries EVERY ``OUTCOME_KEYS`` key, in that order, ``null`` when unknown.

    Why: ``datasets.load_dataset("json")`` infers features from the first block of the FIRST file and
    raises ``CastError`` once a later block of this append-only corpus introduces a column the
    inferred schema lacks — the outcome corpus lives across releases too, so the tuple and its
    exhaustive emission are pinned exactly as for records. ``record_type``/``schema_version`` are
    forced by the sink (never trusted from the caller) so the content discriminator a consumer reads
    can never be misstamped.
    """
    assert distill.OUTCOME_KEYS == (
        "record_type",
        "schema_version",
        "run_id",
        "timestamp",
        "pmc_id",
        "model_id",
        "run_status",
        "ok",
        "measured",
        "head",
        "coverage_pct",
        "best_coverage",
        "coverage_history_len",
        "section_coverages_len",
        "biolink_valid_pct",
        "biolink_valid_pct_strict",
        "demoted_edge_pct",
        "node_count",
        "edge_count",
        "unresolved_count",
        "predicate_advice_count",
        "multivalued_suspect_count",
        "error_codes",
        "attempts",
        "config_chars",
        "config_yaml_sha256",
        "provenance_ok",
        "qc_pass_rate",
        "tool_calls",
        "tokens_total",
        "steps",
        "judge_score",
        "judge_dimensions",
        "gate",
        "versions",
    )
    assert distill.TOOL_CALL_KEYS == ("total", "failed", "wrong", "redundant")
    assert distill.GATE_KEYS == ("map_threshold", "biolink_threshold", "judge_threshold")
    assert distill.VERSION_KEYS == ("tablassert", "biolink_model")

    path: Path = tmp_path / distill.RECORDS_FILENAME
    recorder = distill.DistillRecorder(path)
    recorder.record_outcome({})  # a run that measured nothing at all: maximal unknowns

    (outcome,) = _read_records(recorder.outcome_path)
    assert tuple(outcome) == distill.OUTCOME_KEYS  # ORDER is asserted, not just membership
    assert (outcome["record_type"], outcome["schema_version"]) == ("outcome", 2)
    assert isinstance(outcome["timestamp"], str)  # stamped at write time, so the column is never null
    assert outcome["timestamp"].endswith("+00:00")
    measured = {key for key, value in outcome.items() if value is not None}
    assert measured == {"record_type", "schema_version", "timestamp", "tool_calls", "gate", "versions"}  # fixed structs stay structs

    recorder.begin_run("PMC9")
    recorder.record_outcome(
        {
            "run_id": "explicit:PMC9",  # an explicit caller stamp wins over the open run scope
            "timestamp": "2026-09-11T12:04:31+00:00",
            "model_id": "gpt-4o",
            "ok": False,
            "head": False,
            "edge_count": 3338,
            "attempts": 2,
            "error_codes": ["KGX_EMPTY"],
        }
    )
    populated = _read_records(recorder.outcome_path)[1]
    assert tuple(populated) == distill.OUTCOME_KEYS  # a measured line is key-identical to a bare one
    assert (populated["run_id"], populated["timestamp"]) == ("explicit:PMC9", "2026-09-11T12:04:31+00:00")
    assert (populated["model_id"], populated["ok"], populated["edge_count"], populated["attempts"]) == ("gpt-4o", False, 3338, 2)
    assert populated["error_codes"] == ["KGX_EMPTY"]
    assert populated["coverage_pct"] is None  # an unmeasured figure is an explicit null, never omitted


def test_record_outcome_appends_across_invocations(tmp_path: Path) -> None:
    """A SECOND recorder over the same directory appends outcome lines — runs accumulate, never truncate.

    Why: one ``pmc_id`` can reach the outcome sink twice (a rerun or a resume) and the corpus is
    append-only, so the two runs are distinguished ONLY by ``run_id``. Both invocations' lines must
    share one key order, and none of them may land in ``records.ndjson``.
    """
    path: Path = tmp_path / "distill" / distill.RECORDS_FILENAME
    first = distill.DistillRecorder(path, invocation_id="first-invoc")
    first.begin_run("PMC1")
    first.record("agent", [_Msg("user", "run one")], _Msg("assistant", "a"))
    first.record_outcome({"run_status": "MAPPED", "coverage_pct": 0.8})

    second = distill.DistillRecorder(path, invocation_id="second-invc")  # a later invocation
    second.begin_run("PMC1")  # the SAME article, re-run
    second.record("agent", [_Msg("user", "run two")], _Msg("assistant", "b"))
    second.record_outcome({"run_status": "SKIPPED"})

    assert first.outcome_path == second.outcome_path == path.parent / distill.OUTCOMES_FILENAME
    outcomes = _read_records(second.outcome_path)
    assert [o["run_id"] for o in outcomes] == ["first-invoc:PMC1", "second-invc:PMC1"]
    assert [o["run_status"] for o in outcomes] == ["MAPPED", "SKIPPED"]
    assert {tuple(o) for o in outcomes} == {distill.OUTCOME_KEYS}  # one and only one key order
    records = _read_records(path)
    assert len(records) == 2  # the training file gained only the two records
    assert [r["call_index"] for r in records] == [0, 0]  # per-invocation counter, records only


def test_record_outcome_normalizes_nested_structs_and_warns_on_unknown_keys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Fixed nested structs are projected onto their sub-tuple; extra keys warn like ``record``'s canary.

    Why: a struct column is schema-inferred too — a sub-field that first appears in a later line
    triggers the same ``CastError`` as a new top-level column, and a bare-``null`` struct alternating
    with a populated one risks ``on_mixed_types="use_json"`` silently demoting the column to a
    string. Partial structs are therefore filled with explicit nulls and never emitted as ``null``;
    unknown keys still land (extensibility) but warn, mirroring ``record``'s meta canary.
    """
    cap = _CapturingLogger()
    monkeypatch.setattr(distill, "logger", cap)
    path: Path = tmp_path / distill.RECORDS_FILENAME
    recorder = distill.DistillRecorder(path)

    recorder.record_outcome(
        {
            "tool_calls": {"total": 9, "failed": 1},
            "gate": {},
            "versions": {"biolink_model": "4.4.4"},
            "judge_dimensions": {"schema_validity": 3},
            "latency_ms": 1200,
        }
    )

    (outcome,) = _read_records(recorder.outcome_path)
    assert outcome["tool_calls"] == {"total": 9, "failed": 1, "wrong": None, "redundant": None}
    assert outcome["gate"] == dict.fromkeys(distill.GATE_KEYS)
    assert outcome["versions"] == {"tablassert": None, "biolink_model": "4.4.4"}
    assert outcome["judge_dimensions"] == {"schema_validity": 3}  # open dict: passed through, not pinned
    assert outcome["latency_ms"] == 1200  # extensibility escape hatch still lands
    assert tuple(outcome)[: len(distill.OUTCOME_KEYS)] == distill.OUTCOME_KEYS  # canonical block first, extras last
    assert len(cap.warnings) == 1
    assert "latency_ms" in cap.warnings[0]

    cap.warnings.clear()
    recorder.record_outcome({"tool_calls": 9, "gate": None})  # a non-mapping struct: caller error, never fatal
    degraded = _read_records(recorder.outcome_path)[1]
    assert degraded["tool_calls"] == dict.fromkeys(distill.TOOL_CALL_KEYS)  # all-null struct, not null
    assert degraded["gate"] == dict.fromkeys(distill.GATE_KEYS)  # None degrades the same way, silently
    assert len(cap.warnings) == 1  # only the malformed one warns; None is just unmeasured
    assert "tool_calls" in cap.warnings[0]
    assert "mapping" in cap.warnings[0]


def test_capture_run_outcome_is_guarded_and_needs_no_agent_extra(tmp_path: Path) -> None:
    """``capture_run_outcome`` assembles + appends one full outcome line — with NO ``[agent]`` extra.

    Why: the supervisor wiring (``begin_run``, the status-decision call site, the except-branch call
    site) is end-to-end tested only in ``tests/test_agent_supervisor.py`` behind a module-level
    ``importorskip("smolagents")`` — which SKIPS in CI, so it is bonus evidence, never the proof.
    THIS module has no importorskip and runs in the base environment CI actually uses, so it is the
    load-bearing proof that the wrapper is importable and callable there: it must drive a real
    ``DistillRecorder`` to a schema-uniform outcome line joined on the open ``begin_run`` scope.
    """
    recorder = distill.DistillRecorder(tmp_path / distill.RECORDS_FILENAME, invocation_id="a1b2c3d4e5f6")
    recorder.begin_run("PMC1")

    capture_run_outcome(
        recorder,
        run_id=recorder.run_id,
        pmc_id="PMC1",
        model_id="fake-model",
        run_status="MAPPED",
        report={
            "ok": True,
            "coverage_pct": 0.83,
            "measured": True,
            "head": False,
            "qc_pass_rate": 1.0,
            "biolink_valid_pct": 0.9,
            "node_count": 12,
            "edge_count": 30,
            "unresolved": ["brca1"],
            "predicate_advice": [],
            "multivalued_suspects": [],
            "error_codes": [],
        },
        record={"attempts": 2, "best_coverage": 0.83, "coverage_history": [0.5, 0.83], "section_coverages": [0.83], "config_chars": 120},
        metrics={"total_tool_calls": 3, "failed_tool_calls": 1, "wrong_tool_calls": 0, "redundant_tool_calls": 0, "total_tokens": 500, "steps": 4},
        config_yaml="provenance:\n  repo: PMC\n  publication: PMC1\n",
        map_threshold=0.25,
        biolink_threshold=0.0,
        judge_threshold=None,
    )

    (outcome,) = _read_records(recorder.outcome_path)
    assert tuple(outcome) == distill.OUTCOME_KEYS  # the canonical key set, in canonical order
    assert outcome["run_id"] == "a1b2c3d4e5f6:PMC1"  # joined on the open begin_run scope
    assert (outcome["run_status"], outcome["ok"], outcome["coverage_pct"]) == ("MAPPED", True, 0.83)
    assert outcome["unresolved_count"] == 1  # the list landed as a count
    assert outcome["error_codes"] == []
    assert outcome["tool_calls"] == {"total": 3, "failed": 1, "wrong": 0, "redundant": 0}
    assert outcome["gate"] == {"map_threshold": 0.25, "biolink_threshold": 0.0, "judge_threshold": None}
    assert outcome["provenance_ok"] is True
    assert outcome["config_yaml_sha256"] is not None
    assert recorder.call_index == 0  # an outcome is not a training record
    assert not recorder.path.exists()  # nothing was written to the ChatML records file


def test_capture_run_outcome_swallows_a_raising_recorder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A raising recorder, a recorder without ``record_outcome``, or bad kwargs: logged + swallowed.

    Why: outcome capture is observability, not pipeline logic — the corpus row matters (a crashed
    run is the negative example the weighting step needs), but a capture failure must never change a
    supervisor run's status, abort a batch, or mask the run's own error. The guard therefore
    swallows assembly errors (``build_outcome`` failing loud on a violated call contract) and sink
    errors alike, and names each failure in a warning instead of staying silent.
    """
    cap = _CapturingLogger()
    monkeypatch.setattr("tablassert.agent.logger", cap)

    class ExplodingRecorder:
        def record_outcome(self, outcome: object) -> None:
            raise OSError("disk full")

    kwargs: dict[str, Any] = {
        "run_id": None,
        "pmc_id": "PMC1",
        "model_id": None,
        "run_status": "SKIPPED",
        "report": None,
        "record": None,
        "metrics": None,
        "config_yaml": None,
        "map_threshold": 0.25,
        "biolink_threshold": 0.0,
        "judge_threshold": None,
    }
    capture_run_outcome(ExplodingRecorder(), **kwargs)  # the sink raises: must not propagate
    assert len(cap.warnings) == 1  # the failure is named, not silent

    capture_run_outcome(object(), **kwargs)  # no record_outcome at all: a logged no-op
    capture_run_outcome(ExplodingRecorder(), run_status="SKIPPED")  # assembly itself fails (missing kwargs): still swallowed
    assert len(cap.warnings) == 3
    assert not (tmp_path / distill.OUTCOMES_FILENAME).exists()  # no stray partial write


def test_distill_module_has_no_third_party_imports() -> None:
    """Every module ``distill.py`` imports is stdlib or ``tablassert`` itself — checked mechanically.

    Why: recording must work in ANY install that can run the agent, including one without the
    ``[distill]`` or ``[agent]`` extras. A third-party import here would make ``--distill`` raise at
    import time in a base install, so the invariant is verified by AST rather than by convention.
    """
    source: Path = Path(distill.__file__)
    tree: ast.Module = ast.parse(source.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module is not None:
            roots.add(node.module.split(".")[0])

    assert roots, "the AST walk found no imports at all — the check would pass vacuously"
    third_party: set[str] = {root for root in roots if root != "tablassert" and root not in sys.stdlib_module_names}
    assert third_party == set(), f"distill.py must stay zero-dependency; third-party imports found: {sorted(third_party)}"


def test_distill_dir_is_a_pure_path_helper() -> None:
    """``distill_dir(root)`` is ``root/distill`` with no I/O, matching the sibling helpers."""
    root: Path = Path(".tablassert") / "agent"
    assert distill_dir(root) == root / "distill"
    assert not distill_dir(root).exists()  # pure: nothing created


def test_distilling_model_records_every_generate_call(tmp_path: Path) -> None:
    """The wrapper delegates to the real model and appends one tagged record per call.

    Why: wrapping ``generate`` is the single capture seam — the inner agent, judge, and reflexion
    all route through it. The wrapped model's ``model_id`` is recovered for the record, and the
    response passes through untouched (the run must behave exactly as if unwrapped).
    """
    pytest.importorskip("smolagents")
    recorder = distill.DistillRecorder(tmp_path / distill.RECORDS_FILENAME)
    wrapped = make_distilling_model(make_fake_model(), recorder, purpose="agent", meta={"pmc_id": "PMC7"})

    response = wrapped.generate([_Msg(_Role("user"), "derive a config")])  # pyright: ignore[reportAttributeAccessIssue]
    assert "final_answer" in str(getattr(response, "content", ""))  # the fake's answer passes through
    wrapped.generate([_Msg(_Role("user"), "second call")])  # pyright: ignore[reportAttributeAccessIssue]

    records = _read_records(recorder.path)
    assert len(records) == 2
    for index, record in enumerate(records):
        assert record["purpose"] == "agent"
        assert record["pmc_id"] == "PMC7"
        assert record["call_index"] == index
        assert record["messages"][-1]["role"] == "assistant"
    assert "final_answer" in records[0]["messages"][-1]["content"]


def test_distilling_model_survives_a_raising_recorder(tmp_path: Path) -> None:
    """A recorder that raises does not change the wrapped model's behavior."""

    class ExplodingRecorder:
        def record(self, *args: object, **kwargs: object) -> None:
            raise OSError("disk full")

    pytest.importorskip("smolagents")
    wrapped = make_distilling_model(make_fake_model(), ExplodingRecorder(), purpose="agent")
    response = wrapped.generate([_Msg("user", "still works")])  # pyright: ignore[reportAttributeAccessIssue]
    assert "final_answer" in str(getattr(response, "content", ""))


def test_distill_export_writes_an_hf_dataset(tmp_path: Path) -> None:
    """``distill-export`` turns recorded NDJSON into a ``save_to_disk`` dataset.

    Why: the raw NDJSON already loads in Studio; this command exists for ``datasets``-native
    workflows, and its output must round-trip through ``load_from_disk``.
    """
    pytest.importorskip("datasets")
    from datasets import load_from_disk  # pyright: ignore[reportMissingImports]

    from tablassert.cli import distill_export

    ndjson_dir: Path = tmp_path / "distill"
    recorder = distill.DistillRecorder(ndjson_dir / distill.RECORDS_FILENAME)
    recorder.record("agent", [_Msg("user", "derive"), _Msg("assistant", "thinking")], _Msg("assistant", "done"), pmc_id="PMC1")
    out: Path = tmp_path / "hf-dataset"

    distill_export(distill_dir=ndjson_dir, out=out)

    dataset = load_from_disk(str(out))
    assert len(dataset) == 1
    assert dataset[0]["purpose"] == "agent"
    assert dataset[0]["messages"][-1]["content"] == "done"


def test_distill_export_fails_loud_on_an_empty_dir(tmp_path: Path) -> None:
    """No recorded NDJSON means exit 2 naming the fix — not a cryptic datasets error."""
    from tablassert.cli import distill_export

    with pytest.raises(SystemExit) as exc_info:
        distill_export(distill_dir=tmp_path / "empty", out=tmp_path / "out")
    assert exc_info.value.code == 2
