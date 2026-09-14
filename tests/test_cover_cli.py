"""Targeted coverage for previously-uncovered branches in ``src/tablassert/cli.py``.

Each test executes a specific uncovered line range (noted in its docstring) using the
established offline monkeypatch patterns from ``test_cli_progress.py`` /
``test_fullmap.py``: the HTTP seam (``cli.urlopen``) is faked with ``_FakeResponse``,
network discovery (``cli.babel_urls``) and the Rust build (``rs.build_fullmap_db``) are
stubbed, and all artifacts land in ``tmp_path``. No network, no real Rust build.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import shutil
import subprocess
import sys
import tarfile
from email.message import Message
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.error import HTTPError, URLError

import pytest
from cyclopts.exceptions import UnknownOptionError  # pyright: ignore[reportMissingImports]

from tablassert import cli, extras, rs
from tablassert.cli import build_fullmap_pipeline, build_kg, download_babel_file, download_babel_file_aria2c, validate_graph_pipeline
from tablassert.distill_reward import normalize_rows, union_keys
from tablassert.errors import BabelDownloadError, GraphValidationError, QcRuntimeMissingError
from tablassert.ingests import to_yaml
from tablassert.progress import PipelineProgress


class _FakeResponse:
    """Offline stand-in for ``urllib``'s context-managed HTTPResponse (mirrors test_cli_progress)."""

    def __init__(self, data: bytes, status: int, content_length: str | None) -> None:
        self._stream: io.BytesIO = io.BytesIO(data)
        self._status: int = status
        self.headers: dict[str, str] = {"Content-Length": content_length} if content_length is not None else {}

    def getcode(self) -> int:
        return self._status

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


def test_distill_export_skips_outcome_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The export partition excludes content-classified outcome files before loading.

    Why: outcome rows share the ``*.ndjson`` glob with records but are not training examples;
    importing ``datasets`` before this partition would make the base-environment guard impossible.
    """
    from tablassert.cli import distill_export

    distill_dir: Path = tmp_path / "distill"
    distill_dir.mkdir()
    (distill_dir / "records.ndjson").write_text('{"record_type":"record","value":1}\n', encoding="utf-8")
    (distill_dir / "renamed.ndjson").write_text('{"record_type":"outcome","value":2}\n', encoding="utf-8")
    loaded: list[Path] = []

    class _Dataset:
        def __len__(self) -> int:
            return 1

        def save_to_disk(self, path: str) -> None:
            Path(path).mkdir()

    datasets_module = ModuleType("datasets")

    def load_dataset(_format: str, *, data_files: list[str], split: str) -> _Dataset:
        assert split == "train"
        assert len(data_files) == 1
        temporary: Path = Path(data_files[0])
        loaded.append(temporary)
        rows = [json.loads(line) for line in temporary.read_text(encoding="utf-8").splitlines()]
        assert rows == [{"record_type": "record", "value": 1}]
        return _Dataset()

    datasets_module.load_dataset = load_dataset  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "datasets", datasets_module)
    monkeypatch.setattr(extras, "require", lambda *args, **kwargs: None)
    distill_export(distill_dir=distill_dir, out=tmp_path / "dataset")
    assert len(loaded) == 1
    assert not loaded[0].exists()


def test_distill_export_fails_loud_when_only_outcomes_are_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """An outcomes-only directory exits 2 before optional dependencies are checked.

    Why: a successful preflight followed by a cryptic ``datasets`` failure would hide the actual
    corpus mistake; the content partition must explain that records are absent in base installs.
    """
    from tablassert.cli import distill_export

    distill_dir: Path = tmp_path / "distill"
    distill_dir.mkdir()
    (distill_dir / "anything.ndjson").write_text('{"record_type":"outcome","run_id":"r"}\n', encoding="utf-8")
    monkeypatch.setattr(extras, "require", lambda *args, **kwargs: pytest.fail("optional extra was checked too early"))
    with pytest.raises(SystemExit) as exc_info:
        distill_export(distill_dir=distill_dir, out=tmp_path / "dataset")
    assert exc_info.value.code == 2
    assert "only outcome" in capsys.readouterr().err


def test_distill_export_normalizes_a_corpus_spanning_schema_versions() -> None:
    """The pure normalization helpers union and fill keys across v1/v2-shaped rows.

    Why: this is the load-bearing fix for ``datasets`` first-block inference and must remain
    testable without importing the optional export dependency.
    """
    rows: list[dict[str, object]] = [
        {"record_type": "record", "schema_version": 1, "messages": []},
        {"record_type": "record", "schema_version": 2, "run_id": "r2", "messages": [], "pmc_id": "PMC2"},
    ]
    assert union_keys(rows) == ("record_type", "schema_version", "messages", "run_id", "pmc_id")
    assert normalize_rows(rows) == [
        {"record_type": "record", "schema_version": 1, "messages": [], "run_id": None, "pmc_id": None},
        {"record_type": "record", "schema_version": 2, "messages": [], "run_id": "r2", "pmc_id": "PMC2"},
    ]


def test_distill_export_fails_loud_on_a_type_conflict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """A corpus-wide non-null type conflict is rejected before optional loading.

    Why: ``datasets`` silently JSON-encodes varying types into a string, which is data corruption
    rather than a recoverable schema mismatch for a training dataset.
    """
    from tablassert.cli import distill_export

    distill_dir: Path = tmp_path / "distill"
    distill_dir.mkdir()
    (distill_dir / "records.ndjson").write_text('{"record_type":"record","score":1}\n{"record_type":"record","score":"one"}\n', encoding="utf-8")
    monkeypatch.setattr(extras, "require", lambda *args, **kwargs: pytest.fail("optional extra was checked too early"))
    with pytest.raises(SystemExit) as exc_info:
        distill_export(distill_dir=distill_dir, out=tmp_path / "dataset")
    assert exc_info.value.code == 2
    error = capsys.readouterr().err
    assert "score" in error
    assert "int" in error
    assert "str" in error
    assert "JSON-encode" in error


def test_validate_graph_pipeline_rejects_malformed_graph(tmp_path: Path) -> None:
    """Cover cli.py:314-315 — ``validate_graph_pipeline`` raises ``GraphValidationError``.

    A graph YAML missing required keys (``tables``/``fullmap``/``rig``) fails ``Graph.model_validate``,
    hitting the ``except pydantic.ValidationError`` -> ``raise GraphValidationError`` path that
    ``validate``'s graph branch delegates to. Existing tests only reach this via ``build_pipeline``
    (``_load_graph``) or via an invalid *table*; this drives the validate-graph path directly.
    """
    graph_file: Path = tmp_path / "graph.yaml"
    to_yaml(graph_file, {"name": "TEST", "version": "1.0.0"})
    with pytest.raises(GraphValidationError) as exc_info:
        validate_graph_pipeline(graph_file, PipelineProgress(total_stages=2))
    assert exc_info.value.code == "graph-validation-failed"


def test_download_babel_file_reuses_cached_final(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cover cli.py:408-409 — a cached final file is reused without any HTTP fetch.

    When ``{filename}`` already exists, ``download_babel_file`` logs the reuse and returns
    immediately. ``urlopen`` is rigged to fail loudly so the test proves the network seam is
    never touched on the cache-hit path.
    """
    final: Path = tmp_path / "f.gz"
    final.write_bytes(b"cached-bytes")

    def _no_network(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("urlopen must not be called when the final file is cached")

    monkeypatch.setattr(cli, "urlopen", _no_network)
    out: Path = download_babel_file("f.gz", "https://example.com/f.gz", tmp_path)
    assert out == final
    assert out.read_bytes() == b"cached-bytes"


def test_download_babel_file_restarts_when_server_ignores_range(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cover cli.py:423 — server answers HTTP 200 (not 206) to a Range request => warn + restart.

    A non-empty ``.part`` makes ``offset > 0`` so a ``Range`` header is sent. The fake server
    ignores it (status 200), which fires the "Server ignored Range header" warning and forces
    ``mode="wb"``: the stale partial bytes are discarded, so the final file is exactly the fresh
    payload (NOT ``old + new``). The captured warning asserts the branch ran.
    """
    (tmp_path / "f.gz.part").write_bytes(b"OLD" * 10)  # offset > 0 => Range header sent
    payload: bytes = b"NEW" * 20
    monkeypatch.setattr(cli, "urlopen", lambda request, timeout: _FakeResponse(payload, 200, str(len(payload))))

    warnings: list[str] = []

    class _RecordingLogger:
        def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
            warnings.append(message)

        def info(self, *args: Any, **kwargs: Any) -> None:
            return None

    monkeypatch.setattr(cli, "download_logger", _RecordingLogger())

    out: Path = download_babel_file("f.gz", "https://example.com/f.gz", tmp_path)
    assert any("ignored Range header" in w for w in warnings)
    assert out.read_bytes() == payload  # restarted in "wb" mode: stale .part discarded, not appended


def test_download_babel_file_raises_after_exhausting_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every network attempt fails => per-attempt warning, exponential backoff, then raise.

    WHY: ``urlopen`` always raises ``URLError``; ``time.sleep`` is stubbed to record the
    backoffs (keeping the test instant). Backoff is exponential (``5 * 2 ** (attempt-1)``,
    capped at 60s) and only fires when another attempt remains, so with ``retries=3`` attempts
    1 and 2 sleep 5s and 10s and the FINAL attempt skips the dead sleep before the loop exits
    via ``raise BabelDownloadError`` carrying the last error.
    """
    sleeps: list[float] = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    def _fail(request: Any, timeout: int) -> None:
        raise URLError("boom")

    monkeypatch.setattr(cli, "urlopen", _fail)
    with pytest.raises(BabelDownloadError):
        download_babel_file("f.gz", "https://example.com/f.gz", tmp_path, retries=3)
    assert sleeps == [5, 10]  # exponential backoff; no sleep after the final attempt


def test_download_babel_file_404_raises_immediately(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-retryable 4xx (404) raises ``BabelDownloadError`` on the FIRST attempt.

    WHY: a 404/403 (e.g. a mistyped ``--version``) can never succeed on retry; burning every
    attempt with backoff wastes ~25s. The ``HTTPError`` handler re-raises immediately for
    non-retryable 4xx (anything 400-499 except the transient 408/429), so ``urlopen`` is called
    exactly once and no backoff sleeps fire before the error surfaces.
    """
    sleeps: list[float] = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    attempts: list[int] = []

    def _404(request: Any, timeout: int) -> None:
        attempts.append(1)
        raise HTTPError("https://example.com/f.gz", 404, "Not Found", Message(), None)

    monkeypatch.setattr(cli, "urlopen", _404)
    with pytest.raises(BabelDownloadError):
        download_babel_file("f.gz", "https://example.com/f.gz", tmp_path, retries=5)
    assert len(attempts) == 1  # failed fast on the first attempt, no retries
    assert sleeps == []  # no backoff before raising


def test_download_babel_file_retries_transient_http_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A retryable ``HTTPError`` (5xx, or 408/429) warns, backs off, and retries.

    WHY: only non-retryable 4xx fail fast; 5xx and the transient 408/429 codes fall through to
    the SAME exponential backoff as network errors. With ``retries=3`` the retryable HTTPError
    path sleeps 5s then 10s (``5*2**0``, ``5*2**1``) and skips the sleep after the final attempt
    before raising — proving the backoff guard fires in the HTTPError branch too.
    """
    sleeps: list[float] = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    def _500(request: Any, timeout: int) -> None:
        raise HTTPError("https://example.com/f.gz", 500, "Internal Server Error", Message(), None)

    monkeypatch.setattr(cli, "urlopen", _500)
    with pytest.raises(BabelDownloadError):
        download_babel_file("f.gz", "https://example.com/f.gz", tmp_path, retries=3)
    assert sleeps == [5, 10]  # exponential backoff, skipped after the final attempt


def test_download_babel_file_zero_retries_raises_without_attempt(tmp_path: Path) -> None:
    """Cover the defensive ``or RuntimeError("no attempts made")`` operand on cli.py:440.

    With ``retries=0`` the attempt loop body never runs, so ``last_error`` stays ``None`` and the
    raise falls through to the ``RuntimeError`` fallback. A real, reachable edge (empty ``range``);
    no network or sleep is involved.
    """
    with pytest.raises(BabelDownloadError):
        download_babel_file("f.gz", "https://example.com/f.gz", tmp_path, retries=0)


def test_download_babel_file_aria2c_reuses_cached_complete_without_binary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A complete final file is reused before binary detection or subprocess execution."""
    final: Path = tmp_path / "f.gz"
    final.write_bytes(b"cached-bytes")

    def _resolve_must_not_run() -> str:
        raise AssertionError("aria2c resolver must not run for a complete cache hit")

    def _run_must_not_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise AssertionError("subprocess.run must not run for a complete cache hit")

    monkeypatch.setattr(cli, "_resolve_aria2_binary", _resolve_must_not_run)
    monkeypatch.setattr(cli.subprocess, "run", _run_must_not_run)
    out: Path = download_babel_file_aria2c("f.gz", "https://example.com/f.gz", tmp_path)
    assert out == final
    assert out.read_bytes() == b"cached-bytes"


def test_download_babel_file_aria2c_runs_resume_retry_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """aria2c helper uses subprocess without shell and passes resume/retry flags."""
    monkeypatch.setattr(cli, "_resolve_aria2_binary", lambda: "/usr/bin/aria2c")
    commands: list[list[str]] = []

    def _fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        assert kwargs["shell"] is False
        destination = Path(command[command.index("--dir") + 1])
        filename = command[command.index("--out") + 1]
        destination.mkdir(parents=True, exist_ok=True)
        (destination / filename).write_bytes(b"downloaded")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(cli.subprocess, "run", _fake_run)
    out: Path = download_babel_file_aria2c("f.gz", "https://example.com/f.gz", tmp_path, retries=7)
    assert out == tmp_path / "f.gz"
    assert out.read_bytes() == b"downloaded"
    assert len(commands) == 1
    command = commands[0]
    assert command[0] == "/usr/bin/aria2c"
    assert "--continue=true" in command
    assert "--max-tries" in command
    assert command[command.index("--max-tries") + 1] == "7"
    assert "--retry-wait" in command
    assert command[command.index("--retry-wait") + 1] == "5"
    assert "--summary-interval=0" in command
    assert "--show-console-readout=false" in command
    assert command[-1] == "https://example.com/f.gz"


def test_resolve_aria2_binary_returns_bundled_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """The resolver returns the bundled ``aria2c.ARIA2C`` path as a string."""

    class FakeAria2c:
        ARIA2C = Path("/tmp/aria2c")

    monkeypatch.setattr(cli, "import_module", lambda name: FakeAria2c())
    assert cli._resolve_aria2_binary() == "/tmp/aria2c"


def test_resolve_aria2_binary_missing_export_raises_importerror(monkeypatch: pytest.MonkeyPatch) -> None:
    """A shadowed ``aria2c`` module without ``ARIA2C`` still hits the clean missing-extra path."""

    class BrokenAria2c:
        pass

    monkeypatch.setattr(cli, "import_module", lambda name: BrokenAria2c())
    with pytest.raises(ImportError, match=r"aria2c\.ARIA2C"):
        cli._resolve_aria2_binary()


def test_download_babel_file_aria2c_missing_extra_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Opting into aria2c fails loud when the ``[aria2]`` extra is unavailable."""

    def _missing_extra() -> str:
        raise ImportError("No module named 'aria2c'")

    def _run_must_not_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise AssertionError("subprocess.run must not run when the [aria2] extra is missing")

    monkeypatch.setattr(cli, "_resolve_aria2_binary", _missing_extra)
    monkeypatch.setattr(cli.sys, "platform", "linux")
    monkeypatch.setattr(cli.subprocess, "run", _run_must_not_run)
    with pytest.raises(BabelDownloadError) as excinfo:
        download_babel_file_aria2c("f.gz", "https://example.com/f.gz", tmp_path)
    # Quoted: unquoted brackets glob in zsh, so the command as printed must be runnable as-is.
    assert 'pip install "tablassert[aria2]"' in str(excinfo.value)


def test_download_babel_file_aria2c_missing_extra_on_macos_raises_platform_hint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The unsupported macOS path explains that bundled aria2c wheels are unavailable."""

    def _missing_extra() -> str:
        raise ImportError("No module named 'aria2c'")

    def _run_must_not_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise AssertionError("subprocess.run must not run when the [aria2] extra is unsupported")

    monkeypatch.setattr(cli, "_resolve_aria2_binary", _missing_extra)
    monkeypatch.setattr(cli.sys, "platform", "darwin")
    monkeypatch.setattr(cli.subprocess, "run", _run_must_not_run)
    with pytest.raises(BabelDownloadError) as excinfo:
        download_babel_file_aria2c("f.gz", "https://example.com/f.gz", tmp_path)
    message = str(excinfo.value)
    assert "no macOS wheels" in message
    assert "drop --aria2c" in message


def test_download_babel_file_aria2c_zero_retries_raises_without_unlimited_aria2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``retries=0`` is rejected before aria2 can interpret it as unlimited retries."""

    def _resolve_must_not_run() -> str:
        raise AssertionError("aria2c resolver must not run when retries is invalid")

    monkeypatch.setattr(cli, "_resolve_aria2_binary", _resolve_must_not_run)
    with pytest.raises(BabelDownloadError):
        download_babel_file_aria2c("f.gz", "https://example.com/f.gz", tmp_path, retries=0)


def test_download_babel_file_aria2c_subprocess_oserror_raises_typed_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """OS errors from launching aria2c surface as ``BabelDownloadError``."""
    monkeypatch.setattr(cli, "_resolve_aria2_binary", lambda: "/usr/bin/aria2c")

    def _raise_oserror(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise OSError("exec failed")

    monkeypatch.setattr(cli.subprocess, "run", _raise_oserror)
    with pytest.raises(BabelDownloadError):
        download_babel_file_aria2c("f.gz", "https://example.com/f.gz", tmp_path)


def test_download_babel_file_aria2c_success_without_complete_file_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Even an exit-0 aria2c run must leave a final file without a resume control file."""
    monkeypatch.setattr(cli, "_resolve_aria2_binary", lambda: "/usr/bin/aria2c")

    def _fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        destination = Path(command[command.index("--dir") + 1])
        filename = command[command.index("--out") + 1]
        destination.mkdir(parents=True, exist_ok=True)
        (destination / filename).write_bytes(b"partial")
        (destination / f"{filename}.aria2").write_bytes(b"resume-state")
        return subprocess.CompletedProcess(command, 0, stdout="done", stderr="")

    monkeypatch.setattr(cli.subprocess, "run", _fake_run)
    with pytest.raises(BabelDownloadError):
        download_babel_file_aria2c("f.gz", "https://example.com/f.gz", tmp_path)


def test_download_babel_file_aria2c_preserves_control_file_on_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An existing aria2 control file means the target is partial and must be resumed, not reused."""
    final: Path = tmp_path / "f.gz"
    control: Path = tmp_path / "f.gz.aria2"
    final.write_bytes(b"partial")
    control.write_bytes(b"resume-state")
    monkeypatch.setattr(cli, "_resolve_aria2_binary", lambda: "/usr/bin/aria2c")
    commands: list[list[str]] = []

    def _fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="dropped connection")

    monkeypatch.setattr(cli.subprocess, "run", _fake_run)
    with pytest.raises(BabelDownloadError):
        download_babel_file_aria2c("f.gz", "https://example.com/f.gz", tmp_path)
    assert len(commands) == 1  # final + .aria2 was NOT treated as a complete cache hit
    assert final.read_bytes() == b"partial"
    assert control.read_bytes() == b"resume-state"  # failure path preserves aria2 resume metadata


def test_build_kg_qc_without_the_extra_stops_before_the_build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``--qc`` without the ``[qc]`` extra fails immediately, naming the install command.

    Why this matters more than any other extras check: the QC audit is the LAST stage of the
    pipeline, running only after entity resolution has finished. Without the preflight a user
    waits out the whole build before learning scikit-learn was never installed — so the test
    asserts ``run`` was never reached, not merely that an error was raised.
    """
    config: Path = tmp_path / "graph.yaml"
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: pytest.fail("the build started without the [qc] extra"))
    monkeypatch.setattr(extras, "missing", lambda extra: ("scikit-learn", "sentence-transformers"))

    with pytest.raises(QcRuntimeMissingError) as excinfo:
        build_kg(config, qc=True)

    message: str = str(excinfo.value)
    assert "scikit-learn" in message
    assert 'pip install "tablassert[qc]"' in message


def test_build_kg_without_qc_never_probes_the_extra(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A plain ``build-kg`` runs on a base install, extra absent or not.

    Why: QC is opt-in. Gating every build on an extra nobody asked for would break the
    base install this project promises.
    """
    config: Path = tmp_path / "graph.yaml"
    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: calls.append(args))
    monkeypatch.setattr(extras, "missing", lambda extra: pytest.fail("probed an extra for a build that never runs QC"))

    build_kg(config)
    assert len(calls) == 1


def test_build_fullmap_command_force_build_passes_aria2c_flag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``build-fullmap --force --aria2c`` delegates both flags, plus the always-on allowlist.

    With the download-first default, only ``--force`` reaches ``build_fullmap_pipeline``; the
    default path is covered by :func:`test_build_fullmap_command_defaults_to_prebuilt_download`.
    The built-in taxon allowlist rides along on every source build — there is no flag for it.
    """
    output: Path = tmp_path / "fullmap.redb"
    cache: Path = tmp_path / "downloads"
    calls: list[tuple[Any, ...]] = []

    def _fake_run(stages: int, fn: Any, arg: Path, **kwargs: Any) -> None:
        calls.append((stages, fn, arg, kwargs))

    monkeypatch.setattr(cli, "run", _fake_run)
    monkeypatch.setattr(extras, "missing", lambda extra: ())
    cli.build_fullmap(output=output, cache=cache, version="v", aria2c=True, force=True)
    assert calls == [
        (3, cli.build_fullmap_pipeline, output, {"cache": cache, "version": "v", "aria2c": True, "taxon_allowlist": cli.load_taxon_allowlist()})
    ]


def test_build_fullmap_aria2c_without_the_extra_stops_before_downloading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--aria2c`` without the ``[aria2]`` extra exits 2 before any download starts.

    Why: the flag is otherwise resolved on the FIRST download, after BABEL URL discovery has
    already hit the network. A flag that cannot work should cost nothing.
    """
    monkeypatch.setattr(cli.sys, "platform", "linux")
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: pytest.fail("the download started without the [aria2] extra"))
    monkeypatch.setattr(extras, "missing", lambda extra: ("aria2",))

    with pytest.raises(SystemExit) as excinfo:
        cli.build_fullmap(output=tmp_path / "fullmap.redb", aria2c=True)

    assert excinfo.value.code == 2
    assert 'pip install "tablassert[aria2]"' in capsys.readouterr().err


def test_build_fullmap_aria2c_is_not_checked_when_the_db_already_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """An existing DB still short-circuits, even with ``--aria2c`` and no ``[aria2]`` extra.

    Why: that path downloads nothing, so the flag is moot. Failing a command that was going to
    be a no-op would turn a harmless leftover flag into an error.
    """
    output: Path = tmp_path / "fullmap.redb"
    output.write_bytes(b"existing-db")
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: pytest.fail("an existing DB must short-circuit"))
    monkeypatch.setattr(extras, "missing", lambda extra: ("aria2",))
    # The reuse short-circuit now requires the recorded allowlist identity to match; this
    # test is about the aria2c preflight ORDER, not the probe (covered separately).
    monkeypatch.setattr(cli, "fullmap_matches_allowlist", lambda existing, ids: True)

    cli.build_fullmap(output=output, aria2c=True)  # must not raise
    assert "already present" in capsys.readouterr().err


def test_build_fullmap_aria2c_on_macos_says_drop_the_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """On macOS the preflight says to drop ``--aria2c``, never to install the extra.

    Why: the ``aria2`` distribution publishes no macOS wheels, so telling a mac user to install
    the extra is a dead end — the fix there is the default Python downloader.
    """
    monkeypatch.setattr(cli.sys, "platform", "darwin")
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: pytest.fail("the download started on an unsupported platform"))
    monkeypatch.setattr(extras, "missing", lambda extra: ("aria2",))

    with pytest.raises(SystemExit) as excinfo:
        cli.build_fullmap(output=tmp_path / "fullmap.redb", aria2c=True)

    assert excinfo.value.code == 2
    message: str = capsys.readouterr().err
    assert "drop --aria2c" in message
    assert "tablassert[aria2]" not in message


def test_build_fullmap_aria2c_flag_parses() -> None:
    """``build-fullmap`` accepts ``--aria2c`` and ``-a`` but no generated negative alias."""

    def parse(argv: list[str]) -> dict[str, Any]:
        fn, bound, _ = cli.APP.parse_args(argv, exit_on_error=False)
        assert fn is cli.build_fullmap
        return dict(bound.arguments)

    assert parse(["build-fullmap"]) == {}
    assert parse(["build-fullmap", "--aria2c"])["aria2c"] is True
    assert parse(["build-fullmap", "-a"])["aria2c"] is True
    with pytest.raises(UnknownOptionError):
        parse(["build-fullmap", "--no-aria2c"])


def test_build_kg_configuration_file_flag_parses(tmp_path: Path) -> None:
    """Guard: ``build-kg``'s config binds positionally AND via ``-f``/``--configuration-file``.

    ``build-kg`` previously exposed its configuration file ONLY positionally, while ``validate``
    accepted ``--configuration-file``/``-f``. This pins the now-consistent parsing (``-f`` belongs
    to the configuration file). The bound argument is keyed by the ``graph_configuration_file``
    parameter name. It also locks the removal of the deprecated ``--table-config``/``-tc`` and
    ``--fullmap`` options (parsing them now raises). cyclopts' ``parse_args`` binds tokens WITHOUT
    executing the command, so no build runs.
    """
    config: Path = tmp_path / "graph.yaml"

    def parse(argv: list[str]) -> dict[str, Any]:
        fn, bound, _ = cli.APP.parse_args(argv, exit_on_error=False)
        assert fn is build_kg
        return dict(bound.arguments)

    # Positional usage is unchanged.
    assert parse(["build-kg", str(config)])["graph_configuration_file"] == config
    # The configuration file now also binds via -f and --configuration-file (matches validate).
    assert parse(["build-kg", "-f", str(config)])["graph_configuration_file"] == config
    assert parse(["build-kg", "--configuration-file", str(config)])["graph_configuration_file"] == config
    # The removed --table-config/--fullmap options are now rejected (locks the removal).
    for removed in (["--table-config"], ["--fullmap", str(config)]):
        with pytest.raises(UnknownOptionError):
            parse(["build-kg", str(config), *removed])
    # The removed ``-tc`` is likewise unusable: with the threads option (and its ``-t`` alias)
    # gone, cyclopts rejects the cluster's leading ``-t`` as an unknown option.
    with pytest.raises(UnknownOptionError):
        parse(["build-kg", str(config), "-tc"])


def test_build_fullmap_pipeline_reports_download_progress(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cover cli.py:653 — ``report_progress`` renders ``_download_detail`` into the sub-step.

    Drives the REAL ``build_fullmap_pipeline`` download stage: ``babel_urls`` is stubbed to one
    class + one synonym file, ``cli.urlopen`` serves a small payload, and the REAL
    ``download_babel_file`` fires its ``on_progress`` (= the ``report_progress`` closure) per
    chunk, executing line 653. ``_download_detail`` is wrapped (delegating to the real impl) to
    observe the ``(downloaded, total)`` values; ``rs.build_fullmap_db`` is stubbed so Stage 3 needs
    no Rust. The downloaded files and the detail calls are both asserted.
    """
    payload: bytes = b"q" * 100
    monkeypatch.setattr(cli, "urlopen", lambda request, timeout: _FakeResponse(payload, 200, str(len(payload))))

    def _fake_babel_urls(version: str, endpoints: tuple[str, ...], pattern: object) -> list[tuple[str, str]]:
        if endpoints == cli.BABEL_CLASS_ENDPOINTS:
            return [("c.gz", "https://example.com/c.gz")]
        return [("s.gz", "https://example.com/s.gz")]

    monkeypatch.setattr(cli, "babel_urls", _fake_babel_urls)

    detail_calls: list[tuple[int, int]] = []
    real_detail = cli._download_detail

    def _recording_detail(downloaded: int, total: int) -> str:
        detail_calls.append((downloaded, total))
        return real_detail(downloaded, total)

    monkeypatch.setattr(cli, "_download_detail", _recording_detail)

    built: list[tuple[Any, ...]] = []

    def _fake_build(output: Path, class_files: list[Path], synonym_files: list[Path], progress: Any = None, taxon_allowlist: Any = None) -> None:
        built.append((output, class_files, synonym_files))

    monkeypatch.setattr(rs, "build_fullmap_db", _fake_build)

    output: Path = tmp_path / "fullmap.redb"
    cache: Path = tmp_path / "downloads"
    build_fullmap_pipeline(output, PipelineProgress(total_stages=3), cache=cache, version="v")

    # Line 653 fired for each downloaded file (one chunk each), ending at the full payload size.
    assert detail_calls == [(len(payload), len(payload)), (len(payload), len(payload))]
    # The real downloader spooled both files to their class/synonym cache dirs.
    assert (cache / "classes" / "c.gz").read_bytes() == payload
    assert (cache / "synonyms" / "s.gz").read_bytes() == payload
    # Stage 3 received the downloaded paths.
    assert built == [(output, [cache / "classes" / "c.gz"], [cache / "synonyms" / "s.gz"])]


def test_build_fullmap_pipeline_uses_aria2c_when_opted_in(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The aria2c flag switches both class and synonym loops to the aria2 helper and simple progress text."""

    def _fake_babel_urls(version: str, endpoints: tuple[str, ...], pattern: object) -> list[tuple[str, str]]:
        if endpoints == cli.BABEL_CLASS_ENDPOINTS:
            return [("c.gz", "https://example.com/c.gz")]
        return [("s.gz", "https://example.com/s.gz")]

    monkeypatch.setattr(cli, "babel_urls", _fake_babel_urls)

    def _python_download_must_not_run(*args: Any, **kwargs: Any) -> Path:
        raise AssertionError("Python downloader must not run when aria2c=True")

    monkeypatch.setattr(cli, "download_babel_file", _python_download_must_not_run)
    aria_calls: list[tuple[str, str, Path]] = []

    def _fake_aria2c(filename: str, url: str, destination: Path, retries: int = 5) -> Path:
        aria_calls.append((filename, url, destination))
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / filename
        path.write_bytes(b"downloaded")
        return path

    monkeypatch.setattr(cli, "download_babel_file_aria2c", _fake_aria2c)
    built: list[tuple[Any, ...]] = []

    def _fake_build(output: Path, class_files: list[Path], synonym_files: list[Path], progress: Any = None, taxon_allowlist: Any = None) -> None:
        built.append((output, class_files, synonym_files))

    monkeypatch.setattr(rs, "build_fullmap_db", _fake_build)

    class _RecordingProgress:
        def __init__(self) -> None:
            self.sub_steps: list[str] = []
            self.advances: int = 0

        def stage(self, name: str) -> None:
            pass

        def section_loop(self, total: int, label: str) -> tuple[Any, Any, Any]:
            def start(detail: str) -> None:
                pass

            def advance() -> None:
                self.advances += 1

            def sub_step(phase: str) -> None:
                self.sub_steps.append(phase)

            return start, advance, sub_step

        def dynamic_loop(self, label: str) -> Any:
            return lambda *args: None

        def end_section_task(self) -> None:
            pass

    progress = _RecordingProgress()
    output: Path = tmp_path / "fullmap.redb"
    cache: Path = tmp_path / "downloads"
    build_fullmap_pipeline(output, progress, cache=cache, version="v", aria2c=True)  # type: ignore[arg-type]

    assert aria_calls == [("c.gz", "https://example.com/c.gz", cache / "classes"), ("s.gz", "https://example.com/s.gz", cache / "synonyms")]
    assert progress.sub_steps.count("aria2c downloading") == 2
    assert progress.advances == 4  # two discovery entries + two downloaded files
    assert built == [(output, [cache / "classes" / "c.gz"], [cache / "synonyms" / "s.gz"])]


# --- prebuilt fullmap downloader (download-first default; --force rebuilds from BABEL) ---


def _write_archive(filename: str, url: str, destination: Path, on_progress: object = None) -> Path:
    """Fake ``download_babel_file`` that just materializes the archive on disk (no network)."""
    destination.mkdir(parents=True, exist_ok=True)
    path: Path = destination / filename
    path.write_bytes(b"archive-bytes")
    return path


def _build_tiny_tar_zst(path: Path, members: dict[str, bytes]) -> None:
    """Create a tiny ``.tar.zst`` hermetically (native 3.14+ tarfile, else the ``zstd`` binary)."""
    if sys.version_info >= (3, 14):
        with tarfile.open(path, "w|zst") as tar:
            for name, data in members.items():
                info: tarfile.TarInfo = tarfile.TarInfo(name=name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
        return
    zstd: str | None = shutil.which("zstd")
    if zstd is None:
        pytest.skip("no native zstd tarfile and no zstd binary to build a test archive")
    tar_path: Path = path.with_suffix(".tar")
    with tarfile.open(tar_path, "w") as tar:
        for name, data in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    subprocess.run([zstd, "-q", "-f", "-o", str(path), str(tar_path)], check=True)


def test_prebuilt_fullmap_urls_derive_installed_version() -> None:
    """The release directory is the INSTALLED Tablassert version, never a hardcoded literal.

    WHY: the RENCI layout is ``.../fullmap/<tablassert-version>/``; pinning a literal would
    break on every release. The URLs must embed the live installed-package version plus the
    BABEL snapshot, and expose the fixed ``fullmap.tar.zst`` / ``sha256sum.txt`` filenames.
    """
    from importlib.metadata import version as get_version

    release: str = get_version("tablassert")
    archive: str
    checksum: str
    archive, checksum = cli._prebuilt_fullmap_urls("2026jul22")
    assert archive == f"https://stars.renci.org/var/babel_outputs/2026jul22/fullmap/{release}/fullmap.tar.zst"
    assert checksum == f"https://stars.renci.org/var/babel_outputs/2026jul22/fullmap/{release}/sha256sum.txt"
    # The version slot is exactly the live installed version, compared as a PATH SEGMENT: a
    # substring check false-positives because a stale stamp like "8.1.0" is contained in a
    # legitimate "18.1.0" release directory. Any hardcoded literal still fails the full-URL
    # equality above, which is built from `release`.
    archive_slot: str = archive.split("/fullmap/", 1)[1].split("/", 1)[0]
    checksum_slot: str = checksum.split("/fullmap/", 1)[1].split("/", 1)[0]
    assert archive_slot == release
    assert checksum_slot == release


def test_fetch_prebuilt_sha256_parses_fullmap_entry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The ``fullmap.tar.zst`` digest is extracted from sha256sum.txt; other lines ignored."""
    digest: str = "0" * 64
    body: str = f"aaaa  other.gz\n{digest}  fullmap.tar.zst\nbbbb  fullmap.s0.redb\n"
    monkeypatch.setattr(cli, "urlopen", lambda request, timeout: _FakeResponse(body.encode(), 200, str(len(body))))
    assert cli._fetch_prebuilt_sha256("https://example.com/sha256sum.txt") == digest


def test_fetch_prebuilt_sha256_handles_binary_mode_star(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Binary-mode sha256sum prefixes the filename with ``*``; still matched."""
    digest: str = "a" * 64
    body: str = f"{digest} *fullmap.tar.zst\n"
    monkeypatch.setattr(cli, "urlopen", lambda request, timeout: _FakeResponse(body.encode(), 200, str(len(body))))
    assert cli._fetch_prebuilt_sha256("https://example.com/sha256sum.txt") == digest


def test_fetch_prebuilt_sha256_returns_none_on_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing/unreadable checksum file returns ``None`` (best-effort), not an error."""

    def _404(request: Any, timeout: int) -> None:
        raise HTTPError("https://example.com/sha256sum.txt", 404, "Not Found", Message(), None)

    monkeypatch.setattr(cli, "urlopen", _404)
    assert cli._fetch_prebuilt_sha256("https://example.com/sha256sum.txt") is None


def test_fetch_prebuilt_sha256_returns_none_when_no_fullmap_entry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A checksum file without a ``fullmap.tar.zst`` line returns ``None`` (no match to verify)."""
    body: str = "aaaa  other.gz\nbbbb  fullmap.s0.redb\n"  # no fullmap.tar.zst entry
    monkeypatch.setattr(cli, "urlopen", lambda request, timeout: _FakeResponse(body.encode(), 200, str(len(body))))
    assert cli._fetch_prebuilt_sha256("https://example.com/sha256sum.txt") is None


def test_fetch_prebuilt_fullmap_downloads_verifies_and_extracts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end orchestration: download archive -> verify checksum -> extract beside output.

    The download, checksum fetch, and Rust extraction are faked so the test asserts the
    ORCHESTRATION: the seam receives the downloaded archive + target output, the files it
    installs (primary + shards beside ``output`` named after its stem) survive, the verify
    branch actually ran against the downloaded bytes, and the archive is removed afterwards.
    """
    archive_bytes: bytes = b"pretend-archive"
    digest: str = hashlib.sha256(archive_bytes).hexdigest()

    def _fake_download(filename: str, url: str, destination: Path, on_progress: object = None) -> Path:
        destination.mkdir(parents=True, exist_ok=True)
        path: Path = destination / filename
        path.write_bytes(archive_bytes)
        if on_progress is not None:
            on_progress(len(archive_bytes), len(archive_bytes))  # type: ignore[operator]
        return path

    monkeypatch.setattr(cli, "download_babel_file", _fake_download)
    monkeypatch.setattr(cli, "_fetch_prebuilt_sha256", lambda url: digest)

    calls: list[tuple[Path, Path, object]] = []

    def _fake_extract(archive: Path, output: Path, on_phase: object, taxon_allowlist: object = None) -> None:
        calls.append((archive, output, taxon_allowlist))
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"PRIMARY")
        (output.parent / f"{output.stem}.s0.redb").write_bytes(b"SHARD0")
        (output.parent / f"{output.stem}.s1.redb").write_bytes(b"SHARD1")

    monkeypatch.setattr(cli, "_extract_prebuilt_fullmap", _fake_extract)

    output: Path = tmp_path / "data" / "fullmap.redb"
    cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="2026jul22", taxon_allowlist=[9606])

    # The required filter reaches the Rust extractor, which validates the archive's
    # recorded identity before installing anything.
    assert calls == [(output.parent / "fullmap.tar.zst", output, [9606])]
    assert output.read_bytes() == b"PRIMARY"
    assert (output.parent / "fullmap.s0.redb").read_bytes() == b"SHARD0"
    assert (output.parent / "fullmap.s1.redb").read_bytes() == b"SHARD1"
    assert not (output.parent / "fullmap.tar.zst").exists()  # archive removed after extraction


def test_fetch_prebuilt_fullmap_checksum_mismatch_unlinks_and_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A checksum mismatch deletes the corrupt archive and raises so the build falls back.

    WHY: extracting a corrupt 46 GB archive would waste disk/time and produce a broken DB.
    The mismatch path fails loud (PrebuiltFullmapUnavailable) AND removes the bad archive so
    the next attempt re-downloads instead of re-verifying garbage.
    """
    monkeypatch.setattr(cli, "download_babel_file", _write_archive)
    monkeypatch.setattr(cli, "_fetch_prebuilt_sha256", lambda url: "f" * 64)  # never matches
    monkeypatch.setattr(cli, "_extract_prebuilt_fullmap", lambda *a, **k: pytest.fail("extract must not run on a checksum mismatch"))

    output: Path = tmp_path / "fullmap.redb"
    with pytest.raises(cli.PrebuiltFullmapUnavailable, match="checksum mismatch"):
        cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="v")
    assert not (output.parent / "fullmap.tar.zst").exists()  # corrupt archive removed


def test_fetch_prebuilt_fullmap_missing_checksum_proceeds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When RENCI publishes no sha256sum.txt, extraction proceeds (warned, not blocked)."""
    monkeypatch.setattr(cli, "download_babel_file", _write_archive)
    monkeypatch.setattr(cli, "_fetch_prebuilt_sha256", lambda url: None)
    extracted: dict[str, bool] = {"ran": False}

    def _fake_extract(archive: Path, output: Path, on_phase: object, taxon_allowlist: object = None) -> None:
        extracted["ran"] = True
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"PRIMARY")

    monkeypatch.setattr(cli, "_extract_prebuilt_fullmap", _fake_extract)
    output: Path = tmp_path / "fullmap.redb"
    cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="v")
    assert extracted["ran"] is True
    assert output.read_bytes() == b"PRIMARY"


def test_fetch_prebuilt_fullmap_download_failure_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed archive download surfaces as PrebuiltFullmapUnavailable (-> build fallback)."""

    def _fail(filename: str, url: str, destination: Path, on_progress: object = None) -> Path:
        raise BabelDownloadError(url, 1, RuntimeError("network down"))

    monkeypatch.setattr(cli, "download_babel_file", _fail)
    monkeypatch.setattr(cli, "_fetch_prebuilt_sha256", lambda url: None)
    output: Path = tmp_path / "fullmap.redb"
    with pytest.raises(cli.PrebuiltFullmapUnavailable, match="download failed"):
        cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="v")


def test_fetch_prebuilt_fullmap_aria2c_uses_shared_helper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``aria2c=True`` routes the archive through ``download_babel_file_aria2c`` (PR #80's seam).

    WHY: the 46 GB prebuilt is the ideal aria2 use case, and aria2 discovery is moving into
    the optional ``[aria2]`` extra (PR #80). Pinning that the prebuilt path calls the SAME
    shared helper the build uses (not ``shutil.which`` directly) means it inherits #80's
    bundled binary automatically; the Python downloader must NOT run in this mode.
    """

    def _python_must_not_run(*args: Any, **kwargs: Any) -> Path:
        raise AssertionError("Python downloader must not run when aria2c=True")

    monkeypatch.setattr(cli, "download_babel_file", _python_must_not_run)
    aria_calls: list[tuple[str, str, Path]] = []

    def _fake_aria2c(filename: str, url: str, destination: Path, retries: int = 5) -> Path:
        aria_calls.append((filename, url, destination))
        destination.mkdir(parents=True, exist_ok=True)
        (destination / filename).write_bytes(b"archive-bytes")
        return destination / filename

    monkeypatch.setattr(cli, "download_babel_file_aria2c", _fake_aria2c)
    monkeypatch.setattr(cli, "_fetch_prebuilt_sha256", lambda url: None)

    def _fake_extract(archive: Path, output: Path, on_phase: object, taxon_allowlist: object = None) -> None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"PRIMARY")

    monkeypatch.setattr(cli, "_extract_prebuilt_fullmap", _fake_extract)

    output: Path = tmp_path / "data" / "fullmap.redb"
    cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="2026jul22", aria2c=True)

    assert len(aria_calls) == 1
    assert aria_calls[0][0] == "fullmap.tar.zst"
    assert aria_calls[0][1].endswith("/fullmap.tar.zst")
    assert aria_calls[0][2] == output.parent


def test_fetch_prebuilt_fullmap_custom_output_name_renames_shards(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A custom ``--output`` stem gets shards named ``<stem>.s<N>.redb`` beside it.

    WHY: the read path derives shard names from the primary stem (``<stem>.s<N>.redb``). The
    renaming itself is now ENFORCED BY THE RUST EXTRACTOR (it installs ``<stem>.sN.redb``
    directly); this orchestration test fakes the seam and pins that ``fetch_prebuilt_fullmap``
    passes the custom ``output`` through untouched. US-004 covers the real Rust rename e2e.
    """
    monkeypatch.setattr(cli, "download_babel_file", _write_archive)
    monkeypatch.setattr(cli, "_fetch_prebuilt_sha256", lambda url: None)

    def _fake_extract(archive: Path, output: Path, on_phase: object, taxon_allowlist: object = None) -> None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"PRIMARY")
        (output.parent / f"{output.stem}.s0.redb").write_bytes(b"SHARD0")
        (output.parent / f"{output.stem}.s15.redb").write_bytes(b"SHARD15")

    monkeypatch.setattr(cli, "_extract_prebuilt_fullmap", _fake_extract)
    output: Path = tmp_path / "store" / "mydb.redb"
    cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="v")

    assert output.read_bytes() == b"PRIMARY"
    assert (output.parent / "mydb.s0.redb").read_bytes() == b"SHARD0"
    assert (output.parent / "mydb.s15.redb").read_bytes() == b"SHARD15"


def test_extract_prebuilt_fullmap_seam_wraps_rust_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A Rust extraction failure is wrapped as ``PrebuiltFullmapUnavailable`` (-> build fallback).

    WHY: the Rust extension raises ``RuntimeError`` with actionable context; the seam must
    translate it so ``build-fullmap``'s fallback catches one exception type, keeping the
    stable "failed to extract prebuilt archive" prefix and the original as ``__cause__``.
    """
    original: RuntimeError = RuntimeError("build_id mismatch: expected abc, got def")

    def _raiser(archive: Path, output: Path, progress: object = None, taxon_allowlist: object = None) -> None:
        raise original

    # The seam imports ``from tablassert import rs`` INSIDE the function, so patch the
    # attribute on the ``rs`` module itself.
    monkeypatch.setattr(rs, "extract_prebuilt_fullmap", _raiser)
    with pytest.raises(cli.PrebuiltFullmapUnavailable, match="failed to extract prebuilt archive") as exc_info:
        cli._extract_prebuilt_fullmap(tmp_path / "fullmap.tar.zst", tmp_path / "fullmap.redb", on_phase=lambda p: None)
    assert exc_info.value.__cause__ is original


def test_extract_prebuilt_fullmap_seam_does_not_wrap_keyboard_interrupt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``KeyboardInterrupt`` propagates UNWRAPPED (it is a ``BaseException``, not an error).

    WHY: the seam catches ``Exception`` only; wrapping Ctrl-C would misreport a user abort as
    a bad archive and trigger the from-scratch build fallback instead of stopping.
    """

    def _interrupt(archive: Path, output: Path, progress: object = None, taxon_allowlist: object = None) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(rs, "extract_prebuilt_fullmap", _interrupt)
    with pytest.raises(KeyboardInterrupt):
        cli._extract_prebuilt_fullmap(tmp_path / "fullmap.tar.zst", tmp_path / "fullmap.redb", on_phase=lambda p: None)


def test_load_taxon_allowlist_is_top_100() -> None:
    """The built-in YAML artifact is a deterministic 100-entry positive-ID list."""
    ids = cli.load_taxon_allowlist()
    assert len(ids) == 100
    assert len(set(ids)) == 100
    assert ids[:4] == [9606, 10090, 9913, 559292]
    assert ids[-1] == 1074311


def test_build_fullmap_command_defaults_to_prebuilt_download(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Default (no ``--force``) tries the prebuilt download FIRST, not a from-scratch build.

    WHY: 'download from here first (before building)' is the requested default. The command
    calls ``run(2, fetch_prebuilt_fullmap, ...)`` and returns on success WITHOUT invoking the
    3-stage build. (``output`` absent => no skip-if-exists short-circuit.)
    """
    calls: list[tuple[Any, ...]] = []

    def _fake_run(stages: int, fn: Any, arg: Path, **kwargs: Any) -> None:
        calls.append((stages, fn, arg, kwargs))

    monkeypatch.setattr(cli, "run", _fake_run)
    monkeypatch.setattr(extras, "missing", lambda extra: ())  # --aria2c preflight: report [aria2] as installed
    output: Path = tmp_path / "fullmap.redb"  # does not exist
    cli.build_fullmap(output=output, version="v", aria2c=True)
    assert calls == [(2, cli.fetch_prebuilt_fullmap, output, {"version": "v", "aria2c": True, "taxon_allowlist": cli.load_taxon_allowlist()})]


def test_fullmap_matches_allowlist_reads_the_recorded_identity(tmp_path: Path) -> None:
    """The reuse probe accepts ONLY a database built with exactly the current allowlist.

    WHY: ``build-fullmap`` reuses whatever database already sits at ``--output``, and every
    database it installs is filtered by the built-in top-100 taxa. An unfiltered leftover
    (or one filtered by a different list) resolves a DIFFERENT term set, so it must report
    not-reusable -- as must an absent, foreign, or unreadable file, which is a rebuild
    trigger rather than an error.
    """
    ids: list[int] = cli.load_taxon_allowlist()
    filtered: Path = _build_tiny_fullmap(tmp_path / "filtered", ids)
    assert cli.fullmap_matches_allowlist(filtered, ids) is True
    # The identity is set-derived: order and duplicates cannot change it.
    assert cli.fullmap_matches_allowlist(filtered, [*reversed(ids), ids[0]]) is True
    # A different allowlist means a different database.
    assert cli.fullmap_matches_allowlist(filtered, [9606]) is False
    # Unfiltered -- the shape an older Tablassert left behind -- is not reusable.
    unfiltered: Path = _build_tiny_fullmap(tmp_path / "unfiltered", None)
    assert cli.fullmap_matches_allowlist(unfiltered, ids) is False
    # Absent and foreign files report False instead of raising.
    assert cli.fullmap_matches_allowlist(tmp_path / "absent.redb", ids) is False
    foreign: Path = tmp_path / "foreign.redb"
    foreign.write_bytes(b"this is definitely not a redb database")
    assert cli.fullmap_matches_allowlist(foreign, ids) is False


def test_build_fullmap_reuses_only_a_database_built_with_the_current_allowlist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A matching filtered DB short-circuits; an unfiltered leftover is rebuilt instead.

    WHY: the skip-if-exists short-circuit used to trust ANY non-empty file at ``--output``.
    With the allowlist always on, a database an older Tablassert left there is not the
    database this command promises, so it must fall through to a rebuild rather than be
    silently reused.
    """
    ids: list[int] = cli.load_taxon_allowlist()
    filtered: Path = _build_tiny_fullmap(tmp_path / "filtered", ids)

    def _run_must_not_run(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("a database matching the current allowlist must short-circuit")

    monkeypatch.setattr(cli, "run", _run_must_not_run)
    cli.build_fullmap(output=filtered, version="v")
    assert "already present" in capsys.readouterr().err

    # --force rebuilds even the matching database.
    forced: list[tuple[int, Any]] = []
    monkeypatch.setattr(cli, "run", lambda stages, fn, arg, **kwargs: forced.append((stages, fn)))
    cli.build_fullmap(output=filtered, version="v", force=True)
    assert forced == [(3, cli.build_fullmap_pipeline)]

    # An unfiltered leftover is NOT reused: the command proceeds to the prebuilt attempt.
    unfiltered: Path = _build_tiny_fullmap(tmp_path / "unfiltered", None)
    attempted: list[tuple[int, Any]] = []
    monkeypatch.setattr(cli, "run", lambda stages, fn, arg, **kwargs: attempted.append((stages, fn)))
    cli.build_fullmap(output=unfiltered, version="v")
    assert attempted == [(2, cli.fetch_prebuilt_fullmap)]


def test_build_fullmap_taxon_allowlist_flag_is_gone() -> None:
    """The filter is unconditional, so ``--taxon-allowlist`` no longer parses (locks removal)."""

    def parse(argv: list[str]) -> dict[str, Any]:
        fn, bound, _ = cli.APP.parse_args(argv, exit_on_error=False)
        assert fn is cli.build_fullmap
        return dict(bound.arguments)

    assert parse(["build-fullmap"]) == {}
    for removed in ("--taxon-allowlist", "--no-taxon-allowlist"):
        with pytest.raises(UnknownOptionError):
            parse(["build-fullmap", removed])


def test_build_fullmap_command_falls_back_to_build_on_prebuilt_unavailable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When the prebuilt fetch raises, the command falls back to a from-scratch build.

    WHY: a version with no published prebuilt (or a download/extract failure) must not abort;
    it builds from BABEL. Both ``run`` calls fire: the fetch (which raises) then the build.
    """
    calls: list[tuple[Any, ...]] = []
    state: dict[str, int] = {"n": 0}

    def _fake_run(stages: int, fn: Any, arg: Path, **kwargs: Any) -> None:
        calls.append((stages, fn, arg, kwargs))
        state["n"] += 1
        if state["n"] == 1:
            raise cli.PrebuiltFullmapUnavailable("no prebuilt for this version")

    monkeypatch.setattr(cli, "run", _fake_run)
    monkeypatch.setattr(extras, "missing", lambda extra: ())  # --aria2c preflight: report [aria2] as installed
    output: Path = tmp_path / "fullmap.redb"  # absent
    cache: Path = tmp_path / "c"
    cli.build_fullmap(output=output, cache=cache, version="v", aria2c=True)
    assert len(calls) == 2
    assert calls[0][0] == 2
    assert calls[0][1] is cli.fetch_prebuilt_fullmap
    assert calls[1] == (
        3,
        cli.build_fullmap_pipeline,
        output,
        {"cache": cache, "version": "v", "aria2c": True, "taxon_allowlist": cli.load_taxon_allowlist()},
    )


def test_build_fullmap_force_flag_parses() -> None:
    """``--force``/``-f`` parse True; no generated ``--no-force`` alias (``negative=""``)."""

    def parse(argv: list[str]) -> dict[str, Any]:
        fn, bound, _ = cli.APP.parse_args(argv, exit_on_error=False)
        assert fn is cli.build_fullmap
        return dict(bound.arguments)

    assert parse(["build-fullmap"]) == {}  # defaults are absent when no flag is passed
    assert parse(["build-fullmap", "--force"])["force"] is True
    assert parse(["build-fullmap", "-f"])["force"] is True
    with pytest.raises(UnknownOptionError):
        parse(["build-fullmap", "--no-force"])


# --- REAL end-to-end prebuilt tests: the Rust extractor runs unmocked (US-004) ---

# BABEL-format NDJSON lines mirroring the shapes pinned in rust/tests/common/mod.rs
# (CLASS_LINES / SYNONYM_LINES). Two class + two synonym gzip files prove the
# multi-file build path; the names deliberately yield level-one lowercase terms that
# resolve to KNOWN CURIEs (cross-checked against rust/tests/build_golden.rs).
_REAL_CLASS_LINES_HGNC: tuple[str, ...] = ('{"id":"HGNC:1","equivalent_identifiers":[{"identifier":"NCBIGene:100"}]}', '{"id":"HGNC:6"}')
_REAL_CLASS_LINES_MONDO: tuple[str, ...] = ('{"id":"MONDO:2","equivalent_identifiers":[{"identifier":"DOID:999"}]}',)
_REAL_SYNONYM_LINES_HGNC: tuple[str, ...] = (
    '{"curie":"HGNC:1","preferred_name":"Alpha Gene","names":["Alpha Gene","alpha"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}',
    '{"curie":"HGNC:2","preferred_name":"café","names":["café","naïve"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}',
    '{"curie":"HGNC:6","preferred_name":"Shared Hit","names":["shared"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}',
    '{"curie":"HGNC:10","preferred_name":"Multi A","names":["multi","alpha"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}',
)
_REAL_SYNONYM_LINES_MONDO: tuple[str, ...] = (
    '{"curie":"MONDO:2","preferred_name":"Shared Disease","names":["shared"],"types":["Disease"],"taxa":["NCBITaxon:0"]}',
)
# Level-one lowercase forms of the fixture names/preferred names above; every one
# resolves (golden expectations: alpha -> HGNC:1+HGNC:10, alpha gene -> HGNC:1,
# café/naïve -> HGNC:2, shared -> HGNC:6+MONDO:2, multi -> HGNC:10).
_REAL_RESOLVING_TERMS: list[str] = ["alpha", "alpha gene", "café", "naïve", "shared", "multi"]


def _write_gzip_ndjson(path: Path, lines: tuple[str, ...]) -> Path:
    """Write BABEL-format gzip NDJSON (the on-disk shape ``build_fullmap_db`` downloads)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    return path


def _build_real_force_fullmap(directory: Path, taxon_allowlist: list[int] | None = None) -> Path:
    """Build a REAL fullmap with ``rs.build_fullmap_db`` and return the primary path.

    Two gzip class files + two gzip synonym files exercise the multi-file, gzip-aware
    build path exactly as ``build_fullmap_pipeline`` Stage 3 does. ``taxon_allowlist=None``
    builds unfiltered -- the shape an archive published before the allowlist became the
    default has.
    """
    classes: list[Path] = [
        _write_gzip_ndjson(directory / "classes" / "HGNC.ndjson.gz", _REAL_CLASS_LINES_HGNC),
        _write_gzip_ndjson(directory / "classes" / "MONDO.ndjson.gz", _REAL_CLASS_LINES_MONDO),
    ]
    synonyms: list[Path] = [
        _write_gzip_ndjson(directory / "synonyms" / "HGNC.ndjson.gz", _REAL_SYNONYM_LINES_HGNC),
        _write_gzip_ndjson(directory / "synonyms" / "MONDO.ndjson.gz", _REAL_SYNONYM_LINES_MONDO),
    ]
    output: Path = directory / "force" / "fullmap.redb"
    rs.build_fullmap_db(output, classes, synonyms, taxon_allowlist=taxon_allowlist)
    return output


def _build_tiny_fullmap(directory: Path, taxon_allowlist: list[int] | None) -> Path:
    """Build a minimal REAL fullmap (one synonym row) and return the primary path.

    The cheap fixture for allowlist-identity tests: only ``META.taxon_allowlist`` matters
    there, so a full 17-file bundle would be wasted work. ``taxon_allowlist=None`` builds
    unfiltered.
    """
    directory.mkdir(parents=True, exist_ok=True)
    synonyms: Path = directory / "SRC.ndjson"
    synonyms.write_text(
        '{"curie":"HGNC:1","preferred_name":"Alpha Gene","names":["alpha"],"types":["Gene"],"taxa":["NCBITaxon:9606"]}\n', encoding="utf-8"
    )
    output: Path = directory / "fullmap.redb"
    rs.build_fullmap_db(output, [], [synonyms], taxon_allowlist=taxon_allowlist)
    return output


def _force_shards(primary: Path) -> list[Path]:
    """The 16 shard files ``build_fullmap_db`` lands beside the primary."""
    return [primary.parent / f"{primary.stem}.s{index}.redb" for index in range(16)]


def _pack_fullmap_bundle(archive: Path, files: list[Path]) -> None:
    """Pack redb files into a real ``tar.zst`` under their basenames (root-level members)."""
    _build_tiny_tar_zst(archive, {path.name: path.read_bytes() for path in files})


def _stage_prebuilt_download(monkeypatch: pytest.MonkeyPatch, archive: Path) -> None:
    """Point ``fetch_prebuilt_fullmap``'s download seam at a locally staged archive.

    The fake downloader copies the staged bytes into the download dir as the requested
    filename (same idiom as ``_write_archive``); the checksum fetch returns ``None`` so
    verification is skipped, like the existing missing-checksum orchestration tests.
    """

    def _fake_download(filename: str, url: str, destination: Path, on_progress: object = None) -> Path:
        destination.mkdir(parents=True, exist_ok=True)
        path: Path = destination / filename
        shutil.copyfile(archive, path)
        return path

    monkeypatch.setattr(cli, "download_babel_file", _fake_download)
    monkeypatch.setattr(cli, "_fetch_prebuilt_sha256", lambda url: None)


def _sorted_lookup_rows(db: Path, terms: list[str]) -> list[dict[str, Any]]:
    """``lookup_fullmap_terms`` rows sorted by (term, CURIE) so order cannot flake."""
    return sorted(rs.lookup_fullmap_terms(db, terms), key=lambda row: (row["term"], row["CURIE"]))


def test_fetch_prebuilt_fullmap_real_archive_matches_force_build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """THE equivalence contract: an extracted prebuilt equals a force build, for real.

    WHY: users must be able to trust "download prebuilt" as a byte-faithful substitute
    for hours of BABEL building. This pins that promise through the REAL Rust pipeline:
    ``rs.build_fullmap_db`` builds a genuine 17-file bundle from gzip NDJSON, the bundle
    is packed into a real ``tar.zst``, the download is faked (bytes only, no network),
    and ``fetch_prebuilt_fullmap`` runs the REAL streaming zstd+tar extractor with ZERO
    extraction monkeypatching — then both DBs must answer every lookup identically.
    A custom ``--output`` stem (``mymap``) proves stem-naming flows through the Rust
    rename path.
    """
    force_db: Path = _build_real_force_fullmap(tmp_path / "build")
    staging: Path = tmp_path / "staging"
    staging.mkdir()
    archive: Path = staging / "fullmap.tar.zst"
    _pack_fullmap_bundle(archive, [force_db, *_force_shards(force_db)])
    _stage_prebuilt_download(monkeypatch, archive)

    output: Path = tmp_path / "extracted" / "mymap.redb"
    cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="v")

    # Exactly the primary + s0..s15 renamed after the CUSTOM stem — no other redb files.
    expected: list[str] = sorted(["mymap.redb", *(f"mymap.s{index}.redb" for index in range(16))])
    landed: list[str] = sorted(path.name for path in output.parent.iterdir() if path.suffix == ".redb")
    assert landed == expected
    assert not (output.parent / "fullmap.tar.zst").exists()  # archive deleted after extraction
    assert not (output.parent / ".mymap.prebuilt-extract.d").exists()  # no temp-dir residue

    # The extracted DB answers every lookup EXACTLY like the force-built one.
    extracted_rows: list[dict[str, Any]] = _sorted_lookup_rows(output, _REAL_RESOLVING_TERMS)
    force_rows: list[dict[str, Any]] = _sorted_lookup_rows(force_db, _REAL_RESOLVING_TERMS)
    assert extracted_rows  # sanity: the fixture terms really do resolve
    assert extracted_rows == force_rows
    # Spot-pin semantic content so a silent empty-schema DB cannot pass by equality alone.
    assert {(row["term"], row["CURIE"]) for row in extracted_rows} >= {
        ("alpha", "HGNC:1"),
        ("alpha", "HGNC:10"),
        ("alpha gene", "HGNC:1"),
        ("café", "HGNC:2"),
        ("naïve", "HGNC:2"),
        ("shared", "HGNC:6"),
        ("shared", "MONDO:2"),
        ("multi", "HGNC:10"),
    }


def test_fetch_prebuilt_fullmap_real_corrupt_archive_keeps_archive_and_lands_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Garbage (non-zstd) archive bytes fail loud through the real Rust seam.

    WHY: a torn download must never leave a half-extracted "DB" the read path would
    open. The seam wraps the Rust error as ``PrebuiltFullmapUnavailable`` (triggering
    the force-build fallback), nothing lands beside the output, and — by design for
    extraction failures — the archive is KEPT so the failure is diagnosable.
    """
    archive: Path = tmp_path / "staging" / "fullmap.tar.zst"
    archive.parent.mkdir()
    archive.write_bytes(b"this is definitely not a zstd stream")
    _stage_prebuilt_download(monkeypatch, archive)

    output: Path = tmp_path / "extracted" / "mymap.redb"
    with pytest.raises(cli.PrebuiltFullmapUnavailable, match="failed to extract prebuilt archive"):
        cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="v")

    assert not output.exists()
    assert [path for path in output.parent.iterdir() if path.suffix == ".redb"] == []
    assert not (output.parent / ".mymap.prebuilt-extract.d").exists()
    assert (output.parent / "fullmap.tar.zst").exists()  # kept on extraction failure


def test_fetch_prebuilt_fullmap_real_shards_only_archive_rejects_missing_primary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An archive with ONLY the 16 shards (no primary) is rejected before renaming.

    WHY: shard files are useless without the primary (CURIES/PREFIXES/META live there).
    A mispackaged archive must fail validation with a named cause, not install an
    orphan shard set.
    """
    force_db: Path = _build_real_force_fullmap(tmp_path / "build")
    staging: Path = tmp_path / "staging"
    staging.mkdir()
    archive: Path = staging / "fullmap.tar.zst"
    _pack_fullmap_bundle(archive, _force_shards(force_db))  # no primary member
    _stage_prebuilt_download(monkeypatch, archive)

    output: Path = tmp_path / "extracted" / "mymap.redb"
    with pytest.raises(cli.PrebuiltFullmapUnavailable, match="no primary"):
        cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="v")

    assert not output.exists()
    assert [path for path in output.parent.iterdir() if path.suffix == ".redb"] == []
    assert (output.parent / "fullmap.tar.zst").exists()  # kept on extraction failure


def test_fetch_prebuilt_fullmap_real_archive_missing_one_shard_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Primary + only s0..s14 is an inconsistent shard set and must not land.

    WHY: lookups hashing into the absent shard would fail at read time, silently
    corrupting resolution. The Rust validator compares the archive's shard set
    against the primary's META-advertised count BEFORE any rename, naming the gap.
    """
    force_db: Path = _build_real_force_fullmap(tmp_path / "build")
    staging: Path = tmp_path / "staging"
    staging.mkdir()
    archive: Path = staging / "fullmap.tar.zst"
    _pack_fullmap_bundle(archive, [force_db, *_force_shards(force_db)[:15]])  # drop s15
    _stage_prebuilt_download(monkeypatch, archive)

    output: Path = tmp_path / "extracted" / "mymap.redb"
    with pytest.raises(cli.PrebuiltFullmapUnavailable, match=r"inconsistent shard set.*missing \[15\]"):
        cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="v")

    assert not output.exists()
    assert [path for path in output.parent.iterdir() if path.suffix == ".redb"] == []
    assert (output.parent / "fullmap.tar.zst").exists()  # kept on extraction failure


def test_fetch_prebuilt_fullmap_real_unfiltered_archive_rejected_when_an_allowlist_is_required(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A prebuilt published WITHOUT the required filter never lands (real Rust seam).

    WHY: every database ``build-fullmap`` installs is filtered by the built-in top-100 taxa,
    so an archive built unfiltered is not that database. Rust compares the archive's recorded
    ``META.taxon_allowlist`` identity BEFORE any rename, so the failure is
    ``PrebuiltFullmapUnavailable`` (-> a filtered source build) and nothing lands beside the
    output.
    """
    force_db: Path = _build_real_force_fullmap(tmp_path / "build")  # unfiltered
    staging: Path = tmp_path / "staging"
    staging.mkdir()
    archive: Path = staging / "fullmap.tar.zst"
    _pack_fullmap_bundle(archive, [force_db, *_force_shards(force_db)])
    _stage_prebuilt_download(monkeypatch, archive)

    output: Path = tmp_path / "extracted" / "mymap.redb"
    with pytest.raises(cli.PrebuiltFullmapUnavailable, match="not filtered by the required taxon allowlist"):
        cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="v", taxon_allowlist=[9606])

    assert not output.exists()
    assert [path for path in output.parent.iterdir() if path.suffix == ".redb"] == []
    assert not (output.parent / ".mymap.prebuilt-extract.d").exists()


def test_fetch_prebuilt_fullmap_real_filtered_archive_matching_the_allowlist_is_installed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The archive this version publishes for itself passes its own identity requirement.

    WHY: the identity gate must never reject a correctly filtered prebuilt — that is the fast
    path ``build-fullmap`` exists to keep. The requirement is passed out of order with a
    duplicate, pinning that it is matched as the same normalized set the build recorded.
    """
    force_db: Path = _build_real_force_fullmap(tmp_path / "build", taxon_allowlist=[9606])
    staging: Path = tmp_path / "staging"
    staging.mkdir()
    archive: Path = staging / "fullmap.tar.zst"
    _pack_fullmap_bundle(archive, [force_db, *_force_shards(force_db)])
    _stage_prebuilt_download(monkeypatch, archive)

    output: Path = tmp_path / "extracted" / "mymap.redb"
    cli.fetch_prebuilt_fullmap(output, PipelineProgress(total_stages=2), version="v", taxon_allowlist=[9606, 9606])

    expected: list[str] = sorted(["mymap.redb", *(f"mymap.s{index}.redb" for index in range(16))])
    landed: list[str] = sorted(path.name for path in output.parent.iterdir() if path.suffix == ".redb")
    assert landed == expected
    extracted_rows: list[dict[str, Any]] = _sorted_lookup_rows(output, _REAL_RESOLVING_TERMS)
    assert extracted_rows == _sorted_lookup_rows(force_db, _REAL_RESOLVING_TERMS)
    assert extracted_rows  # sanity: the filtered fixture really does resolve terms
