from __future__ import annotations

import hashlib
import re
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import yaml
from yaml import CSafeLoader

from tablassert.errors import TablassertError


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
        return yaml.load(f, Loader=CSafeLoader)


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


_GITHUB_SLUG_RE: re.Pattern[str] = re.compile(r"^[A-Za-z0-9_.-]+$")


def parse_github_ref(spec: str) -> tuple[str, str, str, str]:
    """Parse and validate a pinned ``github:<owner>/<repo>/<path>@<ref>`` template reference."""
    if not spec.startswith("github:") or "@" not in spec:
        raise TablassertError(f"Remote template `from` must use `github:<owner>/<repo>/<path>@<ref>`, got {spec!r}.", code="template-bad-github-ref")
    body, ref = spec.removeprefix("github:").rsplit("@", 1)
    parts: list[str] = body.split("/", 2)
    if len(parts) != 3 or not all(parts) or not ref:
        raise TablassertError(f"Remote template `from` must use `github:<owner>/<repo>/<path>@<ref>`, got {spec!r}.", code="template-bad-github-ref")
    owner, repo, path = parts
    if not _GITHUB_SLUG_RE.match(owner) or owner in (".", "..") or not _GITHUB_SLUG_RE.match(repo) or repo in (".", ".."):
        raise TablassertError(f"Invalid GitHub owner/repo in template ref {spec!r}.", code="template-bad-github-ref")
    path_segments: list[str] = path.split("/")
    if any(seg in ("", ".", "..") for seg in path_segments) or any(c in path for c in ("?", "#", "\\")) or any(c.isspace() for c in path):
        raise TablassertError(f"Invalid file path in GitHub template ref {spec!r}.", code="template-bad-github-ref")
    if ".." in ref or ref.startswith("/") or ref.endswith("/") or any(c in ref for c in ("?", "#", "\\")) or any(c.isspace() for c in ref):
        raise TablassertError(f"Invalid git ref in GitHub template ref {spec!r}.", code="template-bad-github-ref")
    return owner, repo, path, ref


def fetch_github_template(spec: str, *, cache_dir: Path | None = None) -> tuple[dict[str, Any], str]:
    """Fetch (or read from local cache) a GitHub-hosted YAML template using CSafeLoader."""
    owner, repo, path, ref = parse_github_ref(spec)
    cache_root: Path = cache_dir or Path(".tablassert/templates")
    key: str = hashlib.sha256(f"{owner}/{repo}/{ref}/{path}".encode()).hexdigest()
    cached_file: Path = cache_root / f"{key}.yaml"

    from_cache: bool = cached_file.is_file()
    if from_cache:
        raw_bytes: bytes = cached_file.read_bytes()
    else:
        url: str = f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{path}"
        req = Request(url, headers={"User-Agent": "tablassert"})
        try:
            with urlopen(req, timeout=30) as resp:
                raw_bytes = resp.read()
        except (URLError, OSError) as exc:
            raise TablassertError(f"Failed to fetch remote template {spec!r}: {exc}", code="template-bad-github-ref") from exc

    try:
        parsed: object = yaml.load(raw_bytes.decode("utf-8"), Loader=CSafeLoader)
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise TablassertError(f"Remote template {spec!r} failed YAML parsing: {exc}", code="template-bad-github-ref") from exc
    if not isinstance(parsed, dict):
        raise TablassertError(f"Remote template {spec!r} did not parse as a YAML mapping.", code="template-bad-github-ref")

    if not from_cache:
        cache_root.mkdir(parents=True, exist_ok=True)
        cached_file.write_bytes(raw_bytes)
    digest: str = hashlib.sha256(raw_bytes).hexdigest()
    return parsed, digest


def to_sections(instructions: dict[str, Any], table: Path, *, cache_dir: Path | None = None) -> list[dict[str, Any]]:
    """Expand a table config dict into a list of section dicts.

    Each section is the deep merge of the table-level ``template`` over a
    per-section entry. If ``template.from`` references a pinned GitHub YAML
    (``github:owner/repo/path.yaml@ref``), it is fetched/cached and merged under
    the local ``template``.
    """
    template: dict[str, Any] = deepcopy(instructions.get("template", {}))
    remote_ref: object = template.pop("from", None)
    select_key: str = str(template.pop("select", "template"))
    if remote_ref is not None:
        if not isinstance(remote_ref, str):
            raise TablassertError(f"Remote template `from` must be a string, got {type(remote_ref).__name__}.", code="template-bad-github-ref")
        remote_doc, _remote_digest = fetch_github_template(remote_ref, cache_dir=cache_dir)
        selected: object = remote_doc.get(select_key)
        if not isinstance(selected, dict):
            raise TablassertError(f"Remote template {remote_ref!r} has no mapping under key {select_key!r}.", code="template-bad-github-ref")
        template = fastmerge(deepcopy(selected), template)

    template["config"] = table
    sections: list[dict[str, Any]] = instructions.get("sections", [{}])
    merged: list[dict[str, Any]] = [fastmerge(deepcopy(template), deepcopy(x)) for x in sections]
    for index, section in enumerate(merged, start=1):
        section["section_index"] = index
    return merged
