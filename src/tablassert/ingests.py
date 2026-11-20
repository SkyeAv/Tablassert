from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml
from yaml import CSafeLoader

from tablassert.errors import YamlParseError

SourceLocation = tuple[Path, int, int]
LocationMap = dict[tuple[str | int, ...], SourceLocation]


def fastmerge(a: list[Any] | dict[str, Any], b: list[Any] | dict[str, Any]) -> Any:
    """Recursively merge ``b`` into ``a`` in place and return the result.

    Mirrors the legacy deepmerge config but with the slow generic walk stripped
    out: dict-on-dict merges recurse, list-on-list extends, and any other
    scalar-vs-anything collision falls back to ``b`` overwriting ``a``.

    Args:
        a: Left operand (mutated in place when it is a container).
        b: Right operand; wins on scalar collisions.

    Returns:
        The merged value (same object as ``a`` when types aligned, else ``b``).
    """
    if isinstance(a, dict) and isinstance(b, dict):
        for k, v in b.items():
            if k in a:
                av: Any = a[k]
                if isinstance(av, dict) and isinstance(v, dict):
                    fastmerge(av, v)
                elif isinstance(av, list) and isinstance(v, list):
                    av.extend(v)
                else:
                    a[k] = v
            else:
                a[k] = v
        return a

    if isinstance(a, list) and isinstance(b, list):
        a.extend(b)
        return a
    return b


def _collect_yaml_locations(node: yaml.Node | None, path: Path, prefix: tuple[str | int, ...], out: LocationMap) -> None:
    """Recursively record 1-based ``(path, line, column)`` for every YAML mapping/sequence node."""
    if node is None:
        return
    if prefix:
        out[prefix] = (path, node.start_mark.line + 1, node.start_mark.column + 1)
    if isinstance(node, yaml.MappingNode):
        for key_node, value_node in node.value:
            key: object = key_node.value
            if isinstance(key, (str, int)):
                child_prefix: tuple[str | int, ...] = (*prefix, key)
                out[child_prefix] = (path, value_node.start_mark.line + 1, value_node.start_mark.column + 1)
                _collect_yaml_locations(value_node, path, child_prefix, out)
    elif isinstance(node, yaml.SequenceNode):
        for idx, item_node in enumerate(node.value):
            _collect_yaml_locations(item_node, path, (*prefix, idx), out)


def from_yaml_with_locations(p: Path) -> tuple[object, LocationMap]:
    """Read a YAML config file and return ``(parsed_data, location_map)``.

    Raises:
        YamlParseError: With exact ``file:line:column`` when PyYAML reports a ``problem_mark``.
    """
    text: str = p.read_text(encoding="utf-8")
    try:
        data: object = yaml.load(text, Loader=CSafeLoader)
    except yaml.constructor.ConstructorError:
        raise
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        line: int | None = mark.line + 1 if mark is not None else None
        col: int | None = mark.column + 1 if mark is not None else None
        problem: str = str(getattr(exc, "problem", None) or exc)
        raise YamlParseError(p, line, col, problem) from exc
    locations: LocationMap = {}
    loader = yaml.SafeLoader(text)
    try:
        root: yaml.Node | None = loader.get_single_node()
        _collect_yaml_locations(root, p, (), locations)
    finally:
        loader.dispose()
    return data, locations


def from_yaml(p: Path) -> object:
    """Read a YAML config file into a Python object.

    Args:
        p: Path to the YAML file.

    Returns:
        Parsed YAML content (typically a ``dict``).
    """
    with p.open("r") as f:
        # CSafeLoader (not CLoader): config files may be untrusted, and CLoader honors
        # tags like !!python/object/apply that construct arbitrary Python objects -> RCE.
        # CSafeLoader keeps libyaml's C speed while refusing unsafe object construction.
        try:
            return yaml.load(f, Loader=CSafeLoader)
        except yaml.constructor.ConstructorError:
            raise
        except yaml.YAMLError as exc:
            mark = getattr(exc, "problem_mark", None)
            line: int | None = mark.line + 1 if mark is not None else None
            col: int | None = mark.column + 1 if mark is not None else None
            problem: str = str(getattr(exc, "problem", None) or exc)
            raise YamlParseError(p, line, col, problem) from exc


class _IndentedSafeDumper(yaml.SafeDumper):
    """SafeDumper that indents block sequences under their parent mapping key."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> Any:
        return super().increase_indent(flow, False)


def to_yaml(p: Path, data: object) -> None:
    """Write dict-like data to YAML preserving declared key order.

    Block sequences are indented under their parent mapping key (PyYAML's
    default indentless style makes nested RIG entries hard to scan), and the
    line width is relaxed so long prose fields are not chopped mid-sentence.

    Args:
        p: Destination path.
        data: Object to serialize.
    """
    with p.open("w") as f:
        # `yaml.dump` with an explicit SafeDumper subclass: `safe_dump` accepts no
        # Dumper argument, and the subclass still refuses unsafe object construction.
        yaml.dump(data, f, Dumper=_IndentedSafeDumper, sort_keys=False, default_flow_style=False, allow_unicode=True, width=120)


def to_sections(instructions: dict[str, Any], table: Path) -> list[dict[str, Any]]:
    """Expand a table config dict into a list of section dicts.

    Each section is the deep merge of the table-level ``template`` over a
    per-section entry. The originating ``table`` path is stamped into each
    section as ``config``, and the section's 1-based position in the file
    (document order) as ``section_index`` so provenance can name the inlined
    Study after its origin ("<table config> SECTION <n>").

    Args:
        instructions: Parsed YAML config with optional ``template`` and
            ``sections`` keys.
        table: Path of the source table file (recorded on each section).

    Returns:
        One merged dict per section.
    """
    template: dict[str, Any] = instructions.get("template", {})
    template["config"] = table
    sections: list[dict[str, Any]] = instructions.get("sections", [{}])
    merged: list[dict[str, Any]] = [fastmerge(deepcopy(template), x) for x in sections]
    for index, section in enumerate(merged, start=1):
        section["section_index"] = index
    return merged


def to_sections_with_locations(instructions: dict[str, Any], table: Path, locations: LocationMap) -> tuple[list[dict[str, Any]], list[LocationMap]]:
    """Expand a table config and preserve per-section YAML source locations.

    Template-inherited keys keep their ``template.<path>`` coordinates in ``table``;
    section-level overrides overwrite with their ``sections.<i>.<path>`` coordinates.
    """
    merged: list[dict[str, Any]] = to_sections(instructions, table)
    template_locs: LocationMap = {key[1:]: loc for key, loc in locations.items() if len(key) > 1 and key[0] == "template"}
    section_locs: list[LocationMap] = []
    for idx in range(len(merged)):
        loc_map: LocationMap = dict(template_locs)
        for key, loc in locations.items():
            if len(key) > 2 and key[0] == "sections" and key[1] == idx:
                loc_map[key[2:]] = loc
        section_locs.append(loc_map)
    return merged, section_locs
