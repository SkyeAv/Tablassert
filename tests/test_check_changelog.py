"""Tests for scripts/check_changelog.py.

The check gates every pull request on a new Unreleased changelog entry, so a
bug here either blocks all contributions with false failures or silently lets
undocumented changes merge. These tests pin the substantive-entry definition
(bullets only, inside the Unreleased body, relative to the merge base), the
exemption prefixes, the actionable failure messages, and the end-to-end git
plumbing (scratch repositories) including the offline skip contract.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_changelog.py"

_spec = importlib.util.spec_from_file_location("check_changelog", SCRIPT_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover - only if the script vanishes
    raise ImportError(f"cannot load check script at {SCRIPT_PATH}")
check_module = importlib.util.module_from_spec(_spec)
sys.modules["check_changelog"] = check_module
_spec.loader.exec_module(check_module)

BASE_CHANGELOG = """# Changelog

## 1.0.0 - 2026-01-01

- Old release.
"""


def test_new_bullets_finds_entries_in_both_shapes() -> None:
    """A bullet counts when added into an existing Unreleased body or into one the PR itself introduces."""
    base = "# Changelog\n\n## Unreleased\n\n### Added\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n"
    head = "# Changelog\n\n## Unreleased\n\n### Added\n\n- New feature note.\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n"
    assert check_module.new_bullets(base, head) == ["- New feature note."]
    no_section_base = BASE_CHANGELOG
    introduced = "# Changelog\n\n## Unreleased\n\n- Brand new entry.\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n"
    assert check_module.new_bullets(no_section_base, introduced) == ["- Brand new entry."]


def test_new_bullets_rejects_non_substantive_additions() -> None:
    """Whitespace, subheadings, bare dashes, and released-section edits never satisfy the check."""
    base = "# Changelog\n\n## Unreleased\n\n### Added\n\n- Existing pending entry.\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n"
    # Only a subheading and blank lines are added under Unreleased.
    head = "# Changelog\n\n## Unreleased\n\n### Added\n\n### Fixed\n\n- Existing pending entry.\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n"
    assert check_module.new_bullets(base, head) == []
    # A bullet is added, but below the Unreleased section (in released history).
    history_edit = (
        "# Changelog\n\n## Unreleased\n\n### Added\n\n- Existing pending entry.\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n- Smuggled new bullet.\n"
    )
    assert check_module.new_bullets(base, history_edit) == []
    # A bare dash carries no content.
    assert not check_module.is_bullet("- ")


def test_evaluate_verdicts_and_messages() -> None:
    """Each outcome is distinct: pass names the entry, failures are actionable, exemptions explain themselves."""
    base = "# Changelog\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n"
    head = "# Changelog\n\n## Unreleased\n\n- The one new entry.\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n"
    code, message = check_module.evaluate(base, head, None, "feat: something", ())
    assert code == 0
    assert "- The one new entry." in message
    code, message = check_module.evaluate(base, base, None, "feat: something", ())
    assert code == 1
    assert "## Unreleased" in message
    assert "CHANGELOG.md" in message
    code, message = check_module.evaluate(base, head, "chore(release): 2.0.0", "feat: something", check_module.DEFAULT_EXEMPT_PREFIXES)
    assert code == 0
    assert "PR title" in message
    code, message = check_module.evaluate(base, head, None, "chore(deps): bump polars", check_module.DEFAULT_EXEMPT_PREFIXES)
    assert code == 0
    assert "HEAD commit subject" in message
    code, _ = check_module.evaluate(base, base, None, "feat: something", check_module.DEFAULT_EXEMPT_PREFIXES)
    assert code == 1


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A scratch repository with one release commit on main."""
    scratch = tmp_path / "repo"
    scratch.mkdir()
    _git(scratch, "init", "-b", "main")
    _git(scratch, "config", "user.email", "test@example.com")
    _git(scratch, "config", "user.name", "Test")
    _commit(scratch, "CHANGELOG.md", BASE_CHANGELOG, "chore(release): 1.0.0")
    return scratch


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)


def _commit(repo: Path, path: str, text: str, message: str) -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    _git(repo, "add", path)
    _git(repo, "commit", "-m", message)


def _branch(repo: Path, name: str) -> None:
    _git(repo, "checkout", "-b", name)


def run_check(repo: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    """Run the script against a scratch repository the way the hook and CI do."""
    return subprocess.run([sys.executable, str(SCRIPT_PATH), "--repo", str(repo), *extra], capture_output=True, text=True)


def test_pass_when_entry_added(repo: Path) -> None:
    """The happy path: a PR that adds an Unreleased bullet exits 0 and says so."""
    _branch(repo, "feature")
    _commit(repo, "CHANGELOG.md", "# Changelog\n\n## Unreleased\n\n- Added the widget.\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n", "feat: widget")
    result = run_check(repo, "--base", "main")
    assert result.returncode == 0, result.stderr
    assert "PASS" in result.stdout
    assert "- Added the widget." in result.stdout


def test_fail_when_only_released_history_touched(repo: Path) -> None:
    """Adding a bullet to a released section is not an Unreleased entry: exit 1 with remediation."""
    _branch(repo, "feature")
    _commit(repo, "CHANGELOG.md", "# Changelog\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n- Sneaky late bullet.\n", "feat: widget")
    result = run_check(repo, "--base", "main")
    assert result.returncode == 1
    assert "CHANGELOG.md" in result.stderr
    assert "## Unreleased" in result.stderr


def test_fail_when_entry_missing_entirely(repo: Path) -> None:
    """A PR with no changelog change fails and names the required file and section."""
    _branch(repo, "feature")
    _commit(repo, "src/new.py", "X = 1\n", "feat: widget")
    result = run_check(repo, "--base", "main")
    assert result.returncode == 1
    assert "CHANGELOG.md" in result.stderr


def test_merge_base_diff_ignores_main_side_changes(repo: Path) -> None:
    """Changes merged to main after the branch neither fail nor satisfy the feature PR's check."""
    _branch(repo, "feature")
    _commit(repo, "src/new.py", "X = 1\n", "feat: widget")
    _git(repo, "checkout", "main")
    _commit(repo, "CHANGELOG.md", "# Changelog\n\n## Unreleased\n\n- Another PR's entry.\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n", "feat: other")
    _git(repo, "checkout", "feature")
    result = run_check(repo, "--base", "main")
    assert result.returncode == 1, "main-side entries must not count for the feature branch"
    _commit(repo, "CHANGELOG.md", "# Changelog\n\n## Unreleased\n\n- The widget entry.\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n", "feat: widget")
    result = run_check(repo, "--base", "main")
    assert result.returncode == 0, result.stderr
    assert "- The widget entry." in result.stdout


def test_base_without_changelog_file(tmp_path: Path) -> None:
    """A base that never had CHANGELOG.md is treated as empty, so a new section passes.

    Uses its own repository because the shared fixture's base commit already
    carries a changelog; here the branch point predates the file entirely.
    """
    scratch = tmp_path / "repo-no-changelog-base"
    scratch.mkdir()
    _git(scratch, "init", "-b", "main")
    _git(scratch, "config", "user.email", "test@example.com")
    _git(scratch, "config", "user.name", "Test")
    _commit(scratch, "README.md", "# Scratch\n", "docs: readme")
    _branch(scratch, "feature")
    _commit(scratch, "CHANGELOG.md", "# Changelog\n\n## Unreleased\n\n- Fresh start.\n", "feat: widget")
    result = run_check(scratch, "--base", "main")
    assert result.returncode == 0, result.stderr


def test_unresolvable_base_fails_loudly(repo: Path) -> None:
    """An unknown base ref is a plumbing error (exit 2) with a fetch hint, never a silent pass."""
    result = run_check(repo, "--base", "origin/no-such-branch")
    assert result.returncode == 2
    assert "cannot resolve merge base" in result.stderr
    assert "git fetch" in result.stderr


def test_fetch_base_success_with_local_remote(tmp_path: Path) -> None:
    """With a real remote, --fetch-base refreshes the ref and still enforces the entry."""
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "-b", "main")
    _git(origin, "config", "user.email", "test@example.com")
    _git(origin, "config", "user.name", "Test")
    _commit(origin, "CHANGELOG.md", BASE_CHANGELOG, "chore(release): 1.0.0")
    work = tmp_path / "work"
    subprocess.run(["git", "clone", "--quiet", str(origin), str(work)], check=True, capture_output=True, text=True)
    _git(work, "config", "user.email", "test@example.com")
    _git(work, "config", "user.name", "Test")
    _branch(work, "feature")
    _commit(
        work, "CHANGELOG.md", "# Changelog\n\n## Unreleased\n\n- Cloned and enforced.\n\n## 1.0.0 - 2026-01-01\n\n- Old release.\n", "feat: widget"
    )
    result = run_check(work, "--base", "origin/main", "--fetch-base")
    assert result.returncode == 0, result.stderr


def test_fetch_base_failure_skips_loudly(repo: Path) -> None:
    """Offline hook runs skip with an explicit warning (CI is authoritative) instead of blocking pushes."""
    result = run_check(repo, "--base", "origin/main", "--fetch-base")
    assert result.returncode == 0
    assert "skipped" in result.stderr
    assert "origin/main" in result.stderr


def test_pr_title_exemption_end_to_end(repo: Path) -> None:
    """A release-roll PR title exempts the check in a real invocation."""
    _branch(repo, "release")
    _commit(repo, "CHANGELOG.md", "# Changelog\n\n## 1.0.1 - 2026-02-02\n\n- Old release.\n", "chore(release): 1.0.1")
    result = run_check(repo, "--base", "main", "--pr-title", "chore(release): 1.0.1")
    assert result.returncode == 0
    assert "skipped" in result.stdout
