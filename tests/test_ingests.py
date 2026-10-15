from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from yaml.constructor import ConstructorError

from tablassert.ingests import fastmerge, from_yaml, to_sections


def test_fastmerge_nested_dicts() -> None:
    """fastmerge merges nested dicts."""
    a: dict[str, Any] = {"x": 1, "inner": {"a": 1}}
    b: dict[str, Any] = {"y": 2, "inner": {"b": 2}}
    result: dict[str, Any] = fastmerge(a, b)
    assert result == {"x": 1, "y": 2, "inner": {"a": 1, "b": 2}}


def test_fastmerge_overwrites_scalars() -> None:
    """fastmerge overwrites scalar values."""
    a: dict[str, Any] = {"x": 1}
    b: dict[str, Any] = {"x": 2}
    result: dict[str, Any] = fastmerge(a, b)
    assert result == {"x": 2}


def test_fastmerge_extends_lists() -> None:
    """fastmerge extends lists."""
    a: list[int] = [1, 2]
    b: list[int] = [3, 4]
    result: list[int] = fastmerge(a, b)
    assert result == [1, 2, 3, 4]


def test_fastmerge_adds_new_keys() -> None:
    """fastmerge adds new keys from B."""
    a: dict[str, Any] = {"x": 1}
    b: dict[str, Any] = {"y": 2}
    result: dict[str, Any] = fastmerge(a, b)
    assert "y" in result
    assert result["y"] == 2


def test_fastmerge_dict_with_list_values() -> None:
    """fastmerge handles list valued keys."""
    a: dict[str, Any] = {"items": [1]}
    b: dict[str, Any] = {"items": [2]}
    result: dict[str, Any] = fastmerge(a, b)
    assert result["items"] == [1, 2]


def test_fastmerge_returns_b_on_type_mismatch() -> None:
    """fastmerge returns B when types differ."""
    a: dict[str, Any] = {"x": "string"}
    b: dict[str, Any] = {"x": 42}
    result: dict[str, Any] = fastmerge(a, b)
    assert result["x"] == 42


def test_fastmerge_in_place() -> None:
    """fastmerge modifies a in place."""
    a: dict[str, Any] = {"x": 1}
    b: dict[str, Any] = {"y": 2}
    result: dict[str, Any] = fastmerge(a, b)
    assert result is a


def test_from_yaml_reads_file(fixtures_path: Path) -> None:
    """from_yaml reads file to dict."""
    data: Any = from_yaml(fixtures_path / "minimal_section.yaml")
    assert isinstance(data, dict)
    assert "source" in data


def test_from_yaml_rejects_unsafe_python_tag(tmp_path: Path) -> None:
    """from_yaml uses CSafeLoader, so unsafe Python object tags are NOT executed.

    WHY: config files may be untrusted. The old ``CLoader`` honored tags like
    ``!!python/object/apply:os.system``, constructing arbitrary Python objects and
    executing code on load (RCE). ``CSafeLoader`` keeps libyaml's C speed but refuses
    to construct unknown/unsafe tags, raising ``ConstructorError`` instead — so a
    malicious config is rejected loudly and its payload never runs.
    """
    marker: Path = tmp_path / "pwned.txt"
    malicious: Path = tmp_path / "malicious.yaml"
    malicious.write_text(f'exploit: !!python/object/apply:os.system ["touch {marker}"]\n')

    with pytest.raises(ConstructorError):
        from_yaml(malicious)
    assert not marker.exists()  # the tagged command was never executed


def test_to_sections_expands_template(fixtures_path: Path) -> None:
    """to_sections expands template with sections."""
    data: Any = from_yaml(fixtures_path / "minimal_section_with_sections.yaml")
    table: Path = Path("test_config.yaml")
    sections: list[dict[str, Any]] = to_sections(data, table)
    assert len(sections) == 2


def test_to_sections_merges_template(fixtures_path: Path) -> None:
    """to_sections merges template into each section."""
    data: Any = from_yaml(fixtures_path / "minimal_section_with_sections.yaml")
    table: Path = Path("test_config.yaml")
    sections: list[dict[str, Any]] = to_sections(data, table)
    # each section should inherit provenance from template
    for section in sections:
        assert "provenance" in section


def test_to_sections_adds_config(fixtures_path: Path) -> None:
    """to_sections adds config path."""
    data: Any = from_yaml(fixtures_path / "minimal_section_with_sections.yaml")
    table: Path = Path("test_config.yaml")
    sections: list[dict[str, Any]] = to_sections(data, table)
    for section in sections:
        assert section["config"] == table  # pyright: ignore


def test_to_sections_unique_objects(fixtures_path: Path) -> None:
    """to_sections each section has unique object."""
    data: Any = from_yaml(fixtures_path / "minimal_section_with_sections.yaml")
    table: Path = Path("test_config.yaml")
    sections: list[dict[str, Any]] = to_sections(data, table)
    # sections override object encoding from the sections list
    obj_encodings: list[str] = []
    for section in sections:
        obj_encoding: Any = section["statement"]["object"]["encoding"]  # pyright: ignore
        obj_encodings.append(obj_encoding)
    assert obj_encodings[0] != obj_encodings[1]
