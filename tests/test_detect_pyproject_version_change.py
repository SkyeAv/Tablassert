"""Exercise the push-version detector with real force-pushed Git history."""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
ACTION = ROOT / ".github" / "actions" / "detect-pyproject-version-change" / "action.yml"


def _action_script() -> str:
    """Extract the embedded Python so the test runs exactly what Actions runs."""
    text = ACTION.read_text(encoding="utf-8")
    start_marker = "        python - <<'PY'\n"
    end_marker = "\n        PY\n"
    start = text.index(start_marker) + len(start_marker)
    end = text.index(end_marker, start)
    return textwrap.dedent(text[start:end])


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True, stderr=subprocess.PIPE).strip()


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", ".")
    _git(repo, "commit", "--quiet", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _write_snapshot(repo: Path, version: str, note: str) -> None:
    (repo / "pyproject.toml").write_text(f'[project]\nversion = "{version}"\n', encoding="utf-8")
    (repo / "notes.txt").write_text(f"{note}\n", encoding="utf-8")


def _force_pushed_checkout(tmp_path: Path, new_version: str) -> tuple[Path, str]:
    """Return a shallow checkout whose push-event before commit is not present."""
    remote = tmp_path / "remote.git"
    source = tmp_path / "source"
    checkout = tmp_path / "checkout"
    subprocess.run(["git", "init", "--quiet", "--bare", "--initial-branch=main", str(remote)], check=True, capture_output=True)
    subprocess.run(["git", "init", "--quiet", "--initial-branch=main", str(source)], check=True, capture_output=True)
    _git(source, "config", "user.name", "Test")
    _git(source, "config", "user.email", "test@example.com")
    _git(source, "remote", "add", "origin", str(remote))

    _write_snapshot(source, "1.0.0", "base")
    _commit(source, "base")
    _git(source, "push", "--quiet", "origin", "HEAD:main")

    _write_snapshot(source, "1.0.0", "old")
    before = _commit(source, "old")
    _git(source, "push", "--quiet", "origin", "HEAD:main")

    _git(source, "checkout", "--quiet", "--detach", "HEAD~1")
    _write_snapshot(source, new_version, "rewritten")
    _commit(source, "rewritten")
    _git(source, "push", "--quiet", "--force", "origin", "HEAD:main")

    subprocess.run(
        ["git", "clone", "--quiet", "--depth", "1", "--branch", "main", f"file://{remote}", str(checkout)], check=True, capture_output=True
    )
    missing = subprocess.run(
        ["git", "-C", str(checkout), "cat-file", "-e", f"{before}^{{commit}}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False
    )
    assert missing.returncode != 0, "test setup requires an orphaned before commit"
    return checkout, before


def _run_action(checkout: Path, before: str, output: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY"):
        env.pop(name, None)
    env.update({"BEFORE_SHA": before, "GITHUB_EVENT_NAME": "push", "GITHUB_OUTPUT": str(output)})
    return subprocess.run([sys.executable, "-c", _action_script()], cwd=checkout, env=env, capture_output=True, text=True, check=False)


@pytest.mark.parametrize(("new_version", "changed"), [("1.0.0", "false"), ("1.1.0", "true")])
def test_force_push_fetches_missing_before_commit(tmp_path: Path, new_version: str, changed: str) -> None:
    checkout, before = _force_pushed_checkout(tmp_path, new_version)
    output = tmp_path / "github-output"

    result = _run_action(checkout, before, output)

    assert result.returncode == 0, result.stderr
    values = dict(line.split("=", 1) for line in output.read_text(encoding="utf-8").splitlines())
    assert values == {"changed": changed, "should_publish": changed, "tag": f"v{new_version}"}
    assert _git(checkout, "rev-parse", f"{before}^{{commit}}") == before


def test_unfetchable_before_commit_fails_explicitly(tmp_path: Path) -> None:
    checkout, _ = _force_pushed_checkout(tmp_path, "1.0.0")
    missing = "a" * 40

    result = _run_action(checkout, missing, tmp_path / "github-output")

    assert result.returncode == 1
    assert "Unable to fetch previous commit" in result.stderr
    assert missing in result.stderr
