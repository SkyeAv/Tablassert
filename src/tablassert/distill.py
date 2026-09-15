"""Distillation capture: record agent LLM traffic as an HF-ready ChatML NDJSON dataset.

When ``tablassert agent --distill`` is set, every ``model.generate`` call made during the run
(inner CodeAgent turns, the semantic judge, tier-2 reflexion) is appended as ONE JSON object per
line to ``<state_dir>/distill/records.ndjson``. The schema is ChatML — a top-level ``messages``
list of ``{"role", "content"}`` dicts — which Unsloth Studio auto-detects on JSONL upload and
``datasets.load_dataset("json", ...)`` loads directly. Strict JSONL: no outer array, no commas
between lines.

Schema v2 (``SCHEMA_VERSION``) is SCHEMA-UNIFORM: every line carries EVERY key in
:data:`RECORD_KEYS`, in that order, with an explicit JSON ``null`` where a value is unknown.
That is load-bearing, not cosmetic — ``datasets.load_dataset("json")`` infers its features from
the first block of the first file and raises ``CastError`` when a later block of an append-only
corpus introduces a column the inferred schema lacks. On top of the v1 columns (``messages``,
``purpose``, ``call_index``, ``timestamp``, ``token_usage``, ``pmc_id``, ``model_id``, all
unchanged in name, type and meaning) v2 adds ``record_type`` (a content discriminator, so a
training row is distinguishable from an outcome row without looking at the filename),
``schema_version``, ``run_id`` (``"<invocation_id>:<pmc_id>"``, the join key to a run's outcome,
``null`` outside a :meth:`DistillRecorder.begin_run` scope), and the derived size columns
``input_tokens``/``output_tokens``/``n_messages`` (TRL's ``SFTConfig.max_length`` defaults to
1024 and silently drops fully-masked examples, so per-example size must be queryable without
re-parsing ``messages``). Metadata columns ride alongside for filtering (e.g. keeping only
MAPPED runs) and are ignored by TRL's trainer.

A run's KG-build OUTCOME is written to a SIBLING :data:`OUTCOMES_FILENAME` by
:meth:`DistillRecorder.record_outcome`, never interleaved into ``records.ndjson`` — that file is a
pure ChatML training corpus which Unsloth Studio ingests directly and ``distill-export`` globs, so a
non-ChatML row would corrupt both consumers. The two files are joined later, at weigh time, on
``run_id``; outcome lines are schema-uniform over :data:`OUTCOME_KEYS` for the same ``CastError``
reason, and they never advance ``call_index`` (which counts training records only).

This module is ZERO-dependency by design — recording must work in any install that can run the
agent, and must NEVER break a batch: every write is guarded and failures only log.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from tablassert.log import cat

logger = cat("AGENT")

#: Default dataset filename inside ``distill_dir(state_dir)``; append-only across runs so a
#: fine-tuning corpus accumulates over many invocations.
RECORDS_FILENAME: Final[str] = "records.ndjson"

#: Sidecar filename for per-run outcome lines, written alongside :data:`RECORDS_FILENAME` and
#: joined back to records on ``run_id``.
OUTCOMES_FILENAME: Final[str] = "outcomes.ndjson"

#: Version stamped on every line this module writes. Bump when the key set changes so a consumer
#: can tell an old block of an append-only corpus from a new one.
SCHEMA_VERSION: Final[int] = 2

#: ``record_type`` value for a training row (one recorded ``model.generate`` call).
RECORD_TYPE_RECORD: Final[str] = "record"

#: ``record_type`` value for a per-run outcome row; the discriminator lets a consumer separate the
#: two by content rather than by filename.
RECORD_TYPE_OUTCOME: Final[str] = "outcome"

#: The canonical, ordered record key set. EVERY key appears on EVERY record line (explicit ``null``
#: when unknown): ``datasets`` infers its schema from the first block of the first file, so a key
#: that first appears in a later line of an append-only corpus raises ``CastError`` on load.
RECORD_KEYS: Final[tuple[str, ...]] = (
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

#: The canonical, ordered outcome key set — one line per supervisor run, in :data:`OUTCOMES_FILENAME`.
#: EVERY key appears on EVERY outcome line (explicit ``null`` when a figure was unmeasurable) for the
#: same reason as :data:`RECORD_KEYS`: ``datasets`` infers features from the first block of the first
#: file and raises ``CastError`` on a column that first appears later in an append-only corpus. Only
#: figures ALREADY computed at the run's status-decision point belong here — capture performs no new
#: build, audit, coverage or KGX read. Weights are deliberately absent: they are derived at weigh
#: time, so retuning a coefficient never requires re-recording the corpus.
OUTCOME_KEYS: Final[tuple[str, ...]] = (
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

#: Fixed sub-key set of the ``tool_calls`` struct — the step-callback reliability tallies. A struct
#: column is schema-inferred like any other, so its fields are pinned as well.
TOOL_CALL_KEYS: Final[tuple[str, ...]] = ("total", "failed", "wrong", "redundant")

#: Fixed sub-key set of the ``gate`` struct — the run's own thresholds, without which a reward is not
#: comparable across runs that gated differently.
GATE_KEYS: Final[tuple[str, ...]] = ("map_threshold", "biolink_threshold", "judge_threshold")

#: Fixed sub-key set of the ``versions`` struct. Rewards are not comparable across ``biolink-model``
#: releases (v4.4.4 relocated the ``supporting_study_*`` slots), so an append-only corpus pooled over
#: time must stay splittable by version.
VERSION_KEYS: Final[tuple[str, ...]] = ("tablassert", "biolink_model")

#: The :data:`OUTCOME_KEYS` entries that hold a FIXED struct, mapped to their pinned sub-key tuple.
#: ``judge_dimensions`` is absent on purpose: its keys are the judge's own dimension names, declared
#: beside the judge, so this module cannot pin them without importing the agent layer.
_OUTCOME_STRUCT_KEYS: Final[dict[str, tuple[str, ...]]] = {"tool_calls": TOOL_CALL_KEYS, "gate": GATE_KEYS, "versions": VERSION_KEYS}


def _role_name(role: object) -> str:
    """Coerce a message role (``MessageRole`` enum or plain string) to its plain name."""
    value: object = getattr(role, "value", role)
    return str(value)


def serialize_message(message: object) -> dict[str, str]:
    """Serialize one smolagents ``ChatMessage`` (or ``{"role", "content"}`` dict) to ChatML.

    All access is guarded so an unexpected message shape degrades to a best-effort dict rather
    than raising into the run. Non-string content (e.g. multimodal part lists) is stringified.
    """
    if isinstance(message, dict):
        role: object = message.get("role", "user")
        content: object = message.get("content", "")
    else:
        role = getattr(message, "role", "user")
        content = getattr(message, "content", "")
    return {"role": _role_name(role), "content": content if isinstance(content, str) else str(content)}


def serialize_messages(messages: object) -> list[dict[str, str]]:
    """Serialize a ``model.generate`` message list to a ChatML ``messages`` column."""
    if not isinstance(messages, (list, tuple)):
        return []
    return [serialize_message(message) for message in messages]


def serialize_token_usage(response: object) -> dict[str, int] | None:
    """Extract ``{"input_tokens", "output_tokens"}`` from a response's ``token_usage``, if any."""
    usage: object = getattr(response, "token_usage", None)
    if usage is None:
        return None
    input_tokens: object = getattr(usage, "input_tokens", None)
    output_tokens: object = getattr(usage, "output_tokens", None)
    if not isinstance(input_tokens, (int, float)) and not isinstance(output_tokens, (int, float)):
        return None
    return {
        "input_tokens": int(input_tokens) if isinstance(input_tokens, (int, float)) else 0,
        "output_tokens": int(output_tokens) if isinstance(output_tokens, (int, float)) else 0,
    }


def canonical_struct(value: object, keys: tuple[str, ...]) -> dict[str, object]:
    """Project ``value`` onto a fixed sub-key tuple, filling absent fields with explicit ``null``.

    Why: a struct column is schema-inferred exactly like a top-level one, so a sub-field that first
    appears in a later line of an append-only corpus raises ``CastError``, and a bare ``null`` struct
    alternating with a populated one risks ``on_mixed_types = "use_json"`` silently JSON-encoding the
    whole column into a string. Emitting the full sub-tuple always — never ``null`` — keeps the
    column's type stable for the corpus's lifetime. A non-mapping ``value`` degrades to all-``null``
    instead of raising, because capture is fail-soft.
    """
    if not isinstance(value, Mapping):
        return {key: None for key in keys}  # noqa: C420
    return {key: value.get(key) for key in keys}


class DistillRecorder:
    """Append-only NDJSON sink for distillation records; never raises into the run.

    ``record(purpose, messages, response, **meta)`` serializes the conversation, APPENDS the
    assistant response as the final message (so each line is a complete ChatML training
    example), and writes one JSON line. The parent directory is created lazily on first write.
    ``call_index`` counts records written by THIS recorder (per invocation), letting downstream
    filtering keep only each run's final (most complete) record; outcome lines never advance it.

    ``invocation_id`` identifies this recorder instance (injectable for deterministic tests);
    :meth:`begin_run` combines it with a PMC id into the ``run_id`` stamped on every subsequent
    record, which is the granularity at which a run outcome exists.

    :meth:`record_outcome` writes that run's outcome to the sibling :attr:`outcome_path`, leaving
    ``records.ndjson`` a pure ChatML training file.
    """

    def __init__(self, path: Path, *, invocation_id: str | None = None) -> None:
        self.path: Path = Path(path)
        self.call_index: int = 0
        self.invocation_id: str = invocation_id if invocation_id is not None else uuid.uuid4().hex[:12]
        self.run_id: str | None = None

    @property
    def outcome_path(self) -> Path:
        """The sibling ``outcomes.ndjson`` this recorder's outcome lines are appended to.

        Derived from :attr:`path` rather than cached in ``__init__`` so it cannot drift from the
        record sink it must sit beside — the join in the weigh step globs one directory.
        """
        return self.path.parent / OUTCOMES_FILENAME

    def begin_run(self, pmc_id: str) -> str:
        """Open a run scope for ``pmc_id``: set and return ``"<invocation_id>:<pmc_id>"``.

        Pure state mutation, no I/O — it cannot fail a run. Records written before the first
        ``begin_run`` (or by a bare recorder in a test) carry ``run_id: null`` and are simply
        unjoinable, never invalid.
        """
        self.run_id = f"{self.invocation_id}:{pmc_id}"
        return self.run_id

    def record(self, purpose: str, messages: object, response: object = None, **meta: Any) -> None:
        """Append one schema-uniform v2 record; any serialization or I/O failure is logged and swallowed."""
        conversation: list[dict[str, str]] = serialize_messages(messages)
        if response is not None:
            conversation.append(serialize_message(response))
        usage: dict[str, int] | None = serialize_token_usage(response)
        # Seeded in RECORD_KEYS order with explicit ``None`` defaults; ``meta`` fills the known
        # slots IN PLACE (dict update never reorders an existing key), so key order is stable.
        # Comprehension, not ``dict.fromkeys``: fromkeys infers a Literal key type that pyright
        # strict rejects as invariant-incompatible with ``dict[str, object]``.
        record: dict[str, object] = {key: None for key in RECORD_KEYS}  # noqa: C420
        record.update(
            {
                "record_type": RECORD_TYPE_RECORD,
                "schema_version": SCHEMA_VERSION,
                "run_id": self.run_id,
                "timestamp": datetime.now(UTC).isoformat(),
                "purpose": purpose,
                "call_index": self.call_index,
                "messages": conversation,
                "token_usage": usage,
                "input_tokens": usage["input_tokens"] if usage is not None else None,
                "output_tokens": usage["output_tokens"] if usage is not None else None,
                "n_messages": len(conversation),
            }
        )
        for key, value in meta.items():
            if key not in record:  # extensibility, but a schema-drift canary: warn, never raise
                logger.warning(f"distill: meta key {key!r} is outside RECORD_KEYS; it will not appear on every line of {self.path}")
            record[key] = value
        if self._append_json_line(self.path, record, f"{purpose} call"):
            self.call_index += 1  # counts RECORDS actually written; outcome lines never advance it

    def record_outcome(self, outcome: Mapping[str, Any]) -> None:
        """Append one schema-uniform outcome line to the sibling ``outcomes.ndjson``; never raises.

        Why a sibling file: ``distill-export`` globs ``*.ndjson`` and Unsloth Studio ingests
        ``records.ndjson`` directly as ChatML, so an outcome row interleaved there would put a
        non-training row in front of both consumers and change the meaning of a file existing corpora
        already depend on. Kept separate, ``records.ndjson`` stays a pure training file and the join
        back onto it (on ``run_id``) is an explicit, testable step — which is also why ``call_index``,
        a record counter, is NOT advanced here.

        The caller supplies the measured figures; this sink owns the on-disk invariants: the full
        :data:`OUTCOME_KEYS` set in canonical order with explicit ``null`` for anything unmeasured,
        each fixed nested struct projected onto its own sub-tuple (:func:`canonical_struct`), and
        ``record_type``/``schema_version`` forced rather than trusted, so the content discriminator a
        consumer reads can never be misstamped. ``run_id`` falls back to the open :meth:`begin_run`
        scope and ``timestamp`` to the write time, keeping both columns non-null and stably typed.
        Fail-soft like every write in this module: an unwritable sink warns and returns.
        """
        line: dict[str, object] = {key: None for key in OUTCOME_KEYS}  # noqa: C420
        for key, value in outcome.items():
            if key in _OUTCOME_STRUCT_KEYS:
                continue  # normalized onto the pinned sub-tuple below
            if key not in line:  # extensibility, but a schema-drift canary: warn, never raise
                logger.warning(f"distill: outcome key {key!r} is outside OUTCOME_KEYS; it will not appear on every line of {self.outcome_path}")
            line[key] = value
        supplied_run_id: object = outcome.get("run_id")
        supplied_timestamp: object = outcome.get("timestamp")
        line.update(
            {
                "record_type": RECORD_TYPE_OUTCOME,
                "schema_version": SCHEMA_VERSION,
                "run_id": self.run_id if supplied_run_id is None else supplied_run_id,
                "timestamp": datetime.now(UTC).isoformat() if not isinstance(supplied_timestamp, str) else supplied_timestamp,
            }
        )
        for key, sub_keys in _OUTCOME_STRUCT_KEYS.items():
            raw: object = outcome.get(key)
            if raw is not None and not isinstance(raw, Mapping):
                logger.warning(f"distill: outcome key {key!r} must be a mapping over {sub_keys}; got {type(raw).__name__}, writing nulls")
            line[key] = canonical_struct(raw, sub_keys)
        self._append_json_line(self.outcome_path, line, "outcome")

    def _append_json_line(self, path: Path, payload: Mapping[str, object], label: str) -> bool:
        """Serialize ``payload`` and append it as ONE NDJSON line to ``path``; ``True`` if it landed.

        The single guarded write path shared by :meth:`record` and :meth:`record_outcome`: capture is
        observability, never pipeline logic, so a serialization or I/O failure is logged and swallowed
        instead of propagating into a supervisor run. Returning ``False`` rather than raising lets the
        caller skip post-write bookkeeping — a line that never reached disk must not advance
        ``call_index``.
        """
        try:
            line: str = json.dumps(payload, ensure_ascii=False, default=str)
            path.parent.mkdir(parents=True, exist_ok=True)  # parent created lazily on first write
            with path.open("a", encoding="utf-8") as handle:  # append-only: never truncate or reorder
                handle.write(line + "\n")
        except Exception as exc:  # recording must never break a batch
            logger.warning(f"distill: failed to record {label} to {path}: {exc}")
            return False
        return True
