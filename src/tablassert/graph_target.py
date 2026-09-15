"""Caller-owned graph configuration preparation and mutation helpers.

The agent deliberately targets a graph YAML supplied by the user instead of creating an
agent-owned aggregate graph.  This module keeps the path/validation boundary separate from
the agent loop so the CLI can fail before it builds a model or fetches a PMC payload.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pydantic
import yaml

from tablassert.errors import GraphValidationError, flatten_pydantic_error
from tablassert.ingests import from_yaml
from tablassert.models import Graph


@dataclass(frozen=True)
class PreparedGraph:
    """A validated target graph plus paths resolved for the in-process agent build.

    ``path`` is the absolute YAML file that will be modified in place.  ``graph`` is a
    validated copy used by agent execution: the target graph's semantic metadata is
    preserved, while path-valued fields that the temporary build must access are resolved
    relative to the graph file.  The source YAML is never rewritten by preparation.
    """

    path: Path
    graph: Graph


def _absolute_from_graph(path: Path, value: Path) -> Path:
    """Resolve a graph-owned path relative to the graph YAML's directory."""
    candidate: Path = value.expanduser()
    return (candidate if candidate.is_absolute() else path.parent / candidate).resolve()


def _read_valid_target(path: Path) -> dict[str, Any]:
    """Read and validate a caller-owned Graph YAML without changing it."""
    raw: object = from_yaml(path)
    if not isinstance(raw, dict):
        raise GraphValidationError(path, f"expected a YAML mapping, got {type(raw).__name__}")
    try:
        Graph.model_validate(raw)
    except pydantic.ValidationError as exc:
        raise GraphValidationError(path, flatten_pydantic_error(exc)) from exc
    return raw


def prepare_graph(configuration_file: Path) -> PreparedGraph:
    """Resolve and validate an agent target graph before any model or article work.

    Relative ``fullmap`` and ``rig.artifact_base_path`` values are resolved against the
    graph YAML's directory in the in-memory copy.  Existing table YAMLs and their contents
    are intentionally left untouched; newly generated agent configs are normalized by the
    agent before persistence.

    Args:
        configuration_file: User-supplied graph YAML path.

    Returns:
        A :class:`PreparedGraph` with an absolute target path and validated build graph.

    Raises:
        GraphValidationError: If the YAML does not satisfy the Graph model.
        OSError: If the target graph cannot be read.
    """
    target: Path = configuration_file.expanduser().resolve()
    raw: dict[str, Any] = _read_valid_target(target)
    graph: Graph = Graph.model_validate(raw)

    prepared: Graph = graph.model_copy(deep=True)
    prepared.fullmap = _absolute_from_graph(target, prepared.fullmap)
    prepared.rig.artifact_base_path = _absolute_from_graph(target, prepared.rig.artifact_base_path)
    return PreparedGraph(path=target, graph=prepared)


@contextlib.contextmanager
def _target_lock(target: Path) -> Iterator[None]:
    """Serialize target-graph read/modify/write operations with a sidecar ``flock``."""
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path: Path = Path(f"{target}.lock")
    with lock_path.open("a") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def append_successful_config(target_graph: Path, pmc_id: str, config_path: Path) -> Path:
    """Replace one PMC entry and append its new absolute table config in place.

    The target is reread while its sidecar lock is held, so concurrent agents cannot lose
    one another's table entries.  Existing graph metadata and unrelated table entries are
    copied verbatim.  A malformed target raises without quarantining or overwriting the
    caller-owned file.
    """
    target: Path = target_graph.expanduser().resolve()
    config: Path = config_path.expanduser().resolve()
    if not target.is_file():
        raise FileNotFoundError(f"Target graph configuration does not exist: {target}")
    if not config.is_file():
        raise FileNotFoundError(f"Successful table configuration does not exist: {config}")

    with _target_lock(target):
        document: dict[str, Any] = _read_valid_target(target)
        tables: object = document.get("tables")
        if not isinstance(tables, list):  # Graph validation above makes this defensive only.
            raise GraphValidationError(target, "tables field must be a list")
        document["tables"] = [entry for entry in tables if Path(str(entry)).stem != pmc_id] + [str(config)]
        try:
            Graph.model_validate(document)
        except pydantic.ValidationError as exc:
            raise GraphValidationError(target, flatten_pydantic_error(exc)) from exc

        tmp: Path = target.with_name(f".{target.name}.tmp")
        try:
            tmp.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
            os.replace(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)
    return target


__all__: tuple[str, ...] = ("PreparedGraph", "append_successful_config", "prepare_graph")
