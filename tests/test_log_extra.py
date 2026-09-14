"""Tests for the optional ``[log]`` extra: no logging at all when loguru is absent.

loguru is an optional extra, so ``tablassert.log`` must import cleanly without it and must
produce no log artifacts -- no sink file, no log directory, no startup warning. The no-op
path is exercised by reloading the module with ``sys.modules["loguru"]`` poisoned and
``utils.BASE`` pointed at a temp directory; each fixture reloads once more on teardown so
later tests in this worker see the real module state.
"""

from __future__ import annotations

import importlib
import logging
import sys
import tomllib
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest

import tablassert.log as tablassert_log
import tablassert.utils as tablassert_utils

PYPROJECT: Path = Path(__file__).resolve().parents[1] / "pyproject.toml"

# The loguru surface tablassert calls. A base install must not AttributeError on any of it.
CALLED_METHODS: tuple[str, ...] = ("configure", "bind", "add", "remove", "info", "warning", "error", "debug")


def test_log_extra_declared_and_loguru_not_core() -> None:
    with PYPROJECT.open("rb") as f:
        project = tomllib.load(f)["project"]
    extra = project["optional-dependencies"]["log"]
    assert any(dep.startswith("loguru") for dep in extra)
    assert not any(dep.startswith("loguru") for dep in project["dependencies"])


@pytest.fixture
def blocked_loguru(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[ModuleType]:
    monkeypatch.setattr(tablassert_utils, "BASE", tmp_path)
    monkeypatch.setitem(sys.modules, "loguru", None)
    yield importlib.reload(tablassert_log)
    monkeypatch.undo()
    importlib.reload(tablassert_log)


def test_without_loguru_the_logger_is_the_no_op(blocked_loguru: ModuleType) -> None:
    assert isinstance(blocked_loguru.logger, blocked_loguru._NullLogger)
    assert isinstance(blocked_loguru.cat("TEST"), blocked_loguru._NullLogger)
    for name in CALLED_METHODS:
        assert callable(getattr(blocked_loguru.logger, name)), f"the no-op logger lost .{name}, which tablassert calls"


def test_without_loguru_no_log_directory_is_created(blocked_loguru: ModuleType, tmp_path: Path) -> None:
    assert tmp_path / "log" == blocked_loguru.LOGASSERT
    assert tmp_path / "log" / "tablassert.log" == blocked_loguru._LOG_FILE
    assert not blocked_loguru.LOGASSERT.exists(), "a base install must not create the log directory"
    assert not any(tmp_path.iterdir()), "importing tablassert.log without loguru created artifacts"


def test_without_loguru_logging_emits_nothing(blocked_loguru: ModuleType, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    logger = blocked_loguru.cat("TEST")
    logger.info("f-string style message with no fields")
    logger.info("brace style {name}={value}", name="x", value=1)
    logger.info("mentions {missing} placeholder", other=1)
    logger.warning("warn {x}", x=2)
    logger.error("err")
    logger.debug("dbg")
    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("", "")
    assert not any(tmp_path.iterdir()), "logging without loguru wrote to disk"


def test_without_loguru_sinks_are_no_ops(blocked_loguru: ModuleType, tmp_path: Path) -> None:
    lines: list[str] = []
    sink_file: Path = tmp_path / "sink.log"
    logger = blocked_loguru.cat("TEST")

    file_sink_id: int = logger.add(sink_file, level="INFO", format=blocked_loguru.LOG_FORMAT)
    callable_sink_id: int = logger.add(lines.append, level="INFO")
    assert isinstance(file_sink_id, int)
    assert isinstance(callable_sink_id, int)

    logger.info("written {what}", what="line")
    logger.remove(file_sink_id)
    logger.remove(callable_sink_id)
    logger.remove()
    logger.info("after remove")

    assert lines == []
    assert not sink_file.exists()


def test_without_loguru_configure_and_bind_stay_silent(blocked_loguru: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    blocked_loguru.logger.configure(extra={"category": "PIPELINE"})
    rebound = blocked_loguru.logger.bind(category="OTHER")
    rebound.info("bound {what}", what="message")
    assert capsys.readouterr() == ("", "")


def test_without_loguru_import_warns_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    """A missing optional extra is not a defect, so importing must not warn about it."""
    monkeypatch.setattr(tablassert_utils, "BASE", tmp_path)
    monkeypatch.setitem(sys.modules, "loguru", None)
    try:
        with caplog.at_level(logging.DEBUG):
            importlib.reload(tablassert_log)
        assert caplog.records == []
        assert capsys.readouterr() == ("", "")
    finally:
        monkeypatch.undo()
        importlib.reload(tablassert_log)
