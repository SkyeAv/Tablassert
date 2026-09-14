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
from tablassert.agent import distill_dir, make_distilling_model, make_fake_model


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
