from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from tablassert.log import cat

logger = cat("QC")

# Human-readable phrasing for each assertion, used by format_violations.
_MESSAGES: dict[str, str] = {
    "file-missing": "file not found",
    "malformed-lines": "empty or malformed JSON lines",
    "whitespace-values": "values with leading/trailing whitespace",
    "empty-or-null-values": "null or empty values",
    "duplicate-node-ids": "duplicate node ids",
    "duplicate-edge-ids": "duplicate edge ids",
    "unnamed-nodes": "nodes with no name or an empty name",
    "unidentified-nodes": "nodes with no id or an empty id",
    "incomplete-edges": "edges missing subject, predicate, or object",
    "demoted-edges": "edges demoted to bare biolink:Association despite a declared category_override",
    "undeclared-nodes": "nodes referenced by edges but not declared in the nodes file",
    "isolated-nodes": "declared nodes participating in no edge",
}


@dataclass
class StudyViolation:
    """One failed study assertion over the final KGX NDJSON files.

    Attributes:
        check: Assertion key into ``_MESSAGES`` (e.g. ``"duplicate-node-ids"``).
        label: Which file the violation belongs to (``"nodes"`` or ``"edges"``).
        count: Exact number of offending records/values.
        examples: Capped list of example offenders for the stderr summary.
    """

    check: str
    label: str
    count: int
    examples: list[str] = field(default_factory=list)


@dataclass
class _FileScan:
    """Accumulated facts from one streamed pass over an NDJSON file."""

    ids: set[str]
    #: Edge ``id`` values already seen. Kept apart from ``ids``, which on an edge
    #: scan holds the subject/object CURIEs the declared/isolated cross-check needs.
    edge_ids: set[str]
    duplicate_ids: Counter[str]
    whitespace: Counter[str]
    empty_null: Counter[str]
    unnamed: Counter[str]
    idless: Counter[str]
    incomplete: Counter[str]
    #: Edges whose resolved ``category`` is bare ``biolink:Association``, keyed by
    #: predicate. A demotion is only a violation when the build declared a
    #: ``category_override`` (see :func:`study_kgx`); the count is collected either way.
    demoted: Counter[str]
    malformed: int
    missing: bool
    path: Path


def _is_empty_or_null(value: object) -> bool:
    """True for JSON null, strings that strip to empty, and empty containers (recursively).

    Deliberately STRICTER than the writer's strip_nulls (``rust/src/json.rs``): the
    writer scrubs dict entries at every depth but passes array scalars
    (``["x", ""]``) and emptied nested objects (``[{}]``) through verbatim, and
    the study asserts the stronger contract -- no null or empty value anywhere
    -- so the first such shape to reach an emitted file fails loudly instead of
    shipping. Null-like *strings* (``"NA"``, ``"NaN"``, ``"null"``, ``"none"``)
    are also dropped by the writer but are neither null nor empty, so they are
    deliberately not flagged.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list):
        return not value or any(_is_empty_or_null(item) for item in value)
    if isinstance(value, dict):
        return not value or any(_is_empty_or_null(item) for item in value.values())
    return False


def _scan_ndjson(path: Path, *, edge: bool) -> _FileScan:
    """Stream one NDJSON file, collecting the facts the study assertions need.

    Args:
        path: Path to a ``.nodes.ndjson`` or ``.edges.ndjson`` file.
        edge: ``True`` to collect referenced ids from ``subject``/``object`` and
            assert the three core edge slots are present; ``False`` to collect
            declared node ``id``s and track duplicates, nodes with no name or
            an empty name, and nodes with no id or an empty id.

    Returns:
        A :class:`_FileScan`; ``missing`` is set (and nothing else) when the
        file does not exist, so a typo'd path can never read as a clean pass.
    """
    scan: _FileScan = _FileScan(
        set(), set(), Counter(), Counter(), Counter(), Counter(), Counter(), Counter(), Counter(), 0, not path.is_file(), path
    )
    if scan.missing:
        return scan
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                scan.malformed += 1
                continue
            try:
                record: object = json.loads(line)
            except json.JSONDecodeError:
                scan.malformed += 1
                continue
            if not isinstance(record, dict):
                scan.malformed += 1
                continue
            for key, value in record.items():
                # `original_*` fields are verbatim copies of the source-table cell
                # (written by lib.Tcode.encoding under an `original_` prefix before any
                # regex/normalization runs), so leading/trailing whitespace there is
                # faithful to the source, not a defect to flag. Safe only because every
                # `original_` producer is such a verbatim copy; a future slot that merely
                # starts with `original_` would escape this check and must be revisited here.
                if not key.startswith("original_") and isinstance(value, str) and value != value.strip():
                    scan.whitespace[key] += 1
                # The original_* exemption covers whitespace only: the writer drops
                # null and strip-empty strings everywhere -- verbatim copies included
                # -- so an empty original_* value is never legitimate output. A
                # whitespace-only value intentionally trips this check; on non-original
                # fields it also trips the whitespace check above.
                if _is_empty_or_null(value):
                    scan.empty_null[key] += 1
            if edge:
                # The deduper keys edges on their derived id, so a duplicate here means
                # something bypassed it (a hand-built file, or two files concatenated).
                # KGX requires edge ids to be unique, so assert it independently rather
                # than trusting the writer -- the symmetric check to duplicate-node-ids.
                edge_id: object = record.get("id")
                if isinstance(edge_id, str):
                    if edge_id in scan.edge_ids:
                        scan.duplicate_ids[edge_id] += 1
                    scan.edge_ids.add(edge_id)
                for role in ("subject", "object"):
                    ident: object = record.get(role)
                    if isinstance(ident, str):
                        scan.ids.add(ident)
                # An edge without all three core slots is unusable downstream: KGX
                # consumers traverse subject -> predicate -> object. Flag a missing
                # key, a null, or a strip-empty string per slot, counted under the
                # slot name (e.g. `predicate (2)`) for the examples list. The
                # writer's strip_nulls deletes a null slot outright, so on pipeline
                # output a hit means the slot was null upstream and the record
                # shipped broken -- exactly what this assertion exists to catch.
                for slot in ("subject", "predicate", "object"):
                    slot_value: object = record.get(slot)
                    if slot_value is None or (isinstance(slot_value, str) and not slot_value.strip()):
                        scan.incomplete[slot] += 1
                # Bare ``biolink:Association`` means the row lost every specific class:
                # ``lib.edge_category`` derives a class from the (subject role, object
                # role) pair -- optionally pinned per object category by the section's
                # ``category_override`` -- and ``biolink.resolve_association_class``
                # walks up the hierarchy until the predicate is legal, with
                # ``Association`` as the floor. ``prune_to_class`` then nulls every
                # class-specific slot into the inlined ``has_supporting_studies`` junk
                # drawer, so a demoted edge ships without its evidence fields. Counted
                # per predicate; :func:`study_kgx` decides whether it is a violation.
                categories: object = record.get("category")
                category: str = categories[0] if isinstance(categories, list) and categories else str(categories or "")
                if category == "biolink:Association":
                    predicate: object = record.get("predicate")
                    scan.demoted[predicate if isinstance(predicate, str) and predicate else "<no predicate>"] += 1
            else:
                ident = record.get("id")
                if isinstance(ident, str):
                    # Rust dedup only removes byte-identical lines, so a repeated id
                    # here means two nodes share an id with different content.
                    if ident in scan.ids:
                        scan.duplicate_ids[ident] += 1
                    scan.ids.add(ident)
                # A node with no id is unusable downstream: the id is the graph's join
                # key -- edges reference nodes only through it. Flag a missing key,
                # a null, or a strip-empty string, mirroring the name assertion.
                # Since the id is exactly what is absent, examples key on the node's
                # name (or `<no name>`).
                node_id: str = ident if isinstance(ident, str) else "<no id>"
                name: object = record.get("name")
                if ident is None or (isinstance(ident, str) and not ident.strip()):
                    scan.idless[name if isinstance(name, str) and name.strip() else "<no name>"] += 1
                # A node with no name is unusable downstream: KGX consumers key display
                # and merging off `name`. Flag a missing key, a null, or a string that
                # strips to empty. On pipeline output the writer's strip_nulls
                # (rust/src/json.rs is_bad_token) has already removed empty and null-like
                # names ("NA"/"NaN"/"null"/"none"), so the missing-key branch is what
                # fires there; the other branches guard hand-crafted files. Non-string,
                # non-null names pass -- no writer emits them. Offenders are keyed by
                # node id (or `<no id>` when the record has no string id) for the
                # examples list.
                if name is None or (isinstance(name, str) and not name.strip()):
                    scan.unnamed[node_id] += 1
    return scan


def study_kgx(nodes_path: Path, edges_path: Path, *, example_limit: int = 10, category_override_declared: bool = False) -> list[StudyViolation]:
    """Assert over the final KGX NDJSON files, in the spirit of studyKGtsvs.pl.

    Streams both files once each and checks: duplicate node ids, duplicate edge
    ids, nodes with no
    name or an empty name, nodes with no id or an empty id, edges missing any
    of ``subject``/``predicate``/``object``, nodes referenced by edges but
    never declared (``undeclared``), declared nodes participating in no edge
    (``isolated``), empty/malformed lines, string values carrying leading/trailing
    whitespace, and null or empty values in any field (a stronger contract than
    the writer's strip_nulls). When ``category_override_declared``, it also fails
    on edges demoted to bare ``biolink:Association``: a section that pins the
    association class per object category promises every row lands on a pinned
    class, so a demotion means a row escaped the pin -- typically an object
    category the config vocabulary could not name (see
    ``biolink.CATEGORY_OVERRIDES``) -- and the edge shipped without the
    class-specific slots ``prune_to_class`` nulled into
    ``has_supporting_studies``. Every check is an assertion -- the caller decides
    whether violations fail the build.

    Args:
        nodes_path: Path to ``<name>_<version>.nodes.ndjson``.
        edges_path: Path to ``<name>_<version>.edges.ndjson``.
        example_limit: Maximum number of examples retained per violation.
        category_override_declared: Whether any built section declared a
            ``statement.category_override``; gates the demoted-edge assertion.

    Returns:
        A list of :class:`StudyViolation`; empty when every assertion passes.
    """
    nodes: _FileScan = _scan_ndjson(nodes_path, edge=False)
    edges: _FileScan = _scan_ndjson(edges_path, edge=True)

    violations: list[StudyViolation] = []
    for label, scan in (("nodes", nodes), ("edges", edges)):
        if scan.missing:
            violations.append(StudyViolation("file-missing", label, 1, [str(scan.path)]))
            continue
        if scan.malformed:
            violations.append(StudyViolation("malformed-lines", label, scan.malformed))
        if scan.whitespace:
            examples: list[str] = [f"{field_name} ({n})" for field_name, n in scan.whitespace.most_common(example_limit)]
            violations.append(StudyViolation("whitespace-values", label, sum(scan.whitespace.values()), examples))
        if scan.empty_null:
            examples = [f"{field_name} ({n})" for field_name, n in scan.empty_null.most_common(example_limit)]
            violations.append(StudyViolation("empty-or-null-values", label, sum(scan.empty_null.values()), examples))
    if nodes.duplicate_ids:
        examples = [ident for ident, _ in nodes.duplicate_ids.most_common(example_limit)]
        violations.append(StudyViolation("duplicate-node-ids", "nodes", len(nodes.duplicate_ids), examples))
    if edges.duplicate_ids:
        examples = [ident for ident, _ in edges.duplicate_ids.most_common(example_limit)]
        violations.append(StudyViolation("duplicate-edge-ids", "edges", len(edges.duplicate_ids), examples))
    if nodes.unnamed:
        examples = [ident for ident, _ in nodes.unnamed.most_common(example_limit)]
        violations.append(StudyViolation("unnamed-nodes", "nodes", sum(nodes.unnamed.values()), examples))
    if nodes.idless:
        examples = [name for name, _ in nodes.idless.most_common(example_limit)]
        violations.append(StudyViolation("unidentified-nodes", "nodes", sum(nodes.idless.values()), examples))
    if edges.incomplete:
        examples = [f"{slot} ({n})" for slot, n in edges.incomplete.most_common(example_limit)]
        violations.append(StudyViolation("incomplete-edges", "edges", sum(edges.incomplete.values()), examples))
    if category_override_declared and edges.demoted:
        examples = [f"{predicate} ({n})" for predicate, n in edges.demoted.most_common(example_limit)]
        violations.append(StudyViolation("demoted-edges", "edges", sum(edges.demoted.values()), examples))
    if not nodes.missing and not edges.missing:
        undeclared: list[str] = sorted(edges.ids - nodes.ids)
        if undeclared:
            violations.append(StudyViolation("undeclared-nodes", "edges", len(undeclared), undeclared[:example_limit]))
        isolated: list[str] = sorted(nodes.ids - edges.ids)
        if isolated:
            violations.append(StudyViolation("isolated-nodes", "nodes", len(isolated), isolated[:example_limit]))
    return violations


def format_violations(violations: list[StudyViolation]) -> str:
    """Render violations as a human-readable, one-line-per-assertion summary.

    Args:
        violations: The failed assertions returned by :func:`study_kgx`.

    Returns:
        Newline-joined summary lines, e.g.
        ``nodes: 3 duplicate node ids (e.g. HGNC:5, HGNC:6)``.
    """
    lines: list[str] = []
    for violation in violations:
        line: str = f"{violation.label}: {violation.count} {_MESSAGES[violation.check]}"
        if violation.examples:
            line += f" (e.g. {', '.join(violation.examples)})"
        lines.append(line)
    return "\n".join(lines)
