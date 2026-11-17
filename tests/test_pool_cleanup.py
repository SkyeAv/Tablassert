"""Worker pools must allow process finalizers to release logging semaphores."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from tablassert import cli


@pytest.mark.parametrize("fails", [False, True])
def test_pipeline_pool_cleanup(monkeypatch: Any, fails: bool) -> None:
    events: list[str] = []

    class Pool:
        def close(self) -> None:
            events.append("close")

        def join(self) -> None:
            events.append("join")

        def terminate(self) -> None:
            events.append("terminate")

    monkeypatch.setattr(cli, "Pool", Pool)
    if fails:
        with pytest.raises(RuntimeError, match="stage failed"), cli._pipeline_pool():
            raise RuntimeError("stage failed")
    else:
        with cli._pipeline_pool():
            events.append("work")
    assert events == (["terminate", "join"] if fails else ["work", "close", "join"])


@pytest.mark.skipif(sys.platform == "win32", reason="forkserver is unavailable on Windows")
def test_logging_workers_exit_without_semaphore_warning(tmp_path: Path) -> None:
    pytest.importorskip("loguru")
    script = tmp_path / "pool_logging.py"
    script.write_text(
        "import multiprocessing as mp\n"
        "def work(value):\n"
        "    from tablassert.log import cat\n"
        "    cat('TEST').info('worker completed')\n"
        "    return value\n"
        "if __name__ == '__main__':\n"
        "    mp.set_start_method('forkserver')\n"
        "    from tablassert import cli\n"
        "    cli.Pool = lambda: mp.get_context('forkserver').Pool(2)\n"
        "    with cli._pipeline_pool() as pool:\n"
        "        assert pool.map(work, range(4)) == list(range(4))\n"
    )
    result = subprocess.run([sys.executable, str(script)], cwd=tmp_path, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert "leaked semaphore" not in result.stderr, result.stderr
