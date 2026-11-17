# AGENTS.md: Tablassert

Tablassert turns biomedical spreadsheets into KGX knowledge graphs for NCATS Translator. Python package in `src/tablassert/` plus a PyO3 Rust extension in `rust/` built with maturin; uv-managed. Guidance below is evidence-keyed; fix anything stale when touched.

## Build / Test / Lint

- One-time setup: `make setup` (`uv sync --group dev --extra cli --extra qc --extra log`, then debug `maturin develop`).  [verified]
- After any Rust change, or when the extension may be stale: `make dev` (debug build; `make build` for release-mode performance checks only).  [verified]
- Full local gate before a PR: `make check` (ruff lint + format check + pyright + vulture + deptry dead-code scans + pytest + cargo test + clippy -D warnings).  [verified]
- Release version bump: `make patch` / `make minor` / `make major` bumps `pyproject.toml`, `uv.lock`, `CITATION.cff` and rolls the `CHANGELOG.md` Unreleased section (changelog prose stays manual).  [verified]
- Python tests: `uv run pytest`  [verified]; parallel via pytest-xdist (`-n auto` in pyproject addopts, coverage inline); use `-n 0` for one serial test while debugging.
- Rust tests: `cargo test --manifest-path rust/Cargo.toml`  (evidence: ci)
- CI is path-gated by a `changes` job: `autofix` (`ruff check --fix`, `ruff format`, `markdownlint --fix`, `cargo fmt`; fixes are committed back to same-repo PR branches, unfixable violations fail), `pyright` plus `vulture` and `deptry` dead-code scans, full `pytest` (python or rust changes), a 5-file docs-consistency pytest subset (`python-smoke`, docs/markdown-only PRs), `mkdocs build --strict` (docs changes), `html-validate` (markdown changes), `cargo clippy --all-targets -- -D warnings` + `cargo machete` + `cargo test` (rust changes).  (evidence: ci)
- `.github/workflows/security.yml` is deliberately NOT path-gated (advisories publish without diffs): gitleaks secret scan, `pip-audit --strict` on the locked env, and cargo-audit on `rust/Cargo.lock` run on every PR, weekly on main, and on demand.  (evidence: ci)
- After editing `pyproject.toml`, relock; `uv lock --check` runs in pre-commit and catches a stale lockfile.  [verified]

## Conventions

- Ruff owns Python style: line-length 150, double quotes, `skip-magic-trailing-comma = true`; rule set E4/E7/E9, F, B, SIM, C4, PT, RUF, TID, I, PIE, RET, UP with UP042 ignored. Treat `ruff check` / `ruff format` as the interface, not individual codes.  (evidence: pyproject)
- Conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `perf:`, `refactor:`, `test:`); breaking changes marked `feat!:` / `fix!:`; releases are `chore(release): X.Y.Z` commits.  (evidence: history x150)
- Pre-commit has two stages by design: fast auto-fixing hooks (ruff, ruff-format, cargo-fmt, uv-lock-check) plus gitleaks secret scanning on commit; pyright, cargo-clippy, vulture, deptry and cargo-machete on pre-push. Install with `uv run pre-commit install`.  (evidence: pre-commit-config)
- The suite is offline except tests marked `network`.  (evidence: pyproject markers)

## Gotchas

- A venv or worktree missing the `[qc]` extra shows test and pyright failures from absent numpy/sklearn/torch/sentence-transformers; that is environmental, not a regression. Sync with `--extra qc`.  (evidence: memory x2)
- `make dev` (debug) and `make build` (release) install into the same environment; a later debug build replaces the release extension and vice versa.  (evidence: docs)
- Full `build-fullmap` runs decompression-heavy and memory-hungry; do not launch one without confirming free RAM first.  (evidence: memory)

## Git / PR workflow

- PRs are squash-merged on GitHub; feature branches are kept on origin after merge.  (evidence: history, memory)
- Branch naming: `feat/<topic>`, `fix/<topic>`, or Ralph/US story branches.  (evidence: history)
- A `chore(release): X.Y.Z` version bump on main triggers the tag and PyPI publish workflows (version-change detection on `pyproject.toml`).  (evidence: ci)
- CodeRabbit (CHILL profile) reviews every PR; address its actionable comments before merging.  (evidence: memory x2)
- Docs: MkDocs site under `docs/`, `mkdocs build --strict` in CI; user-facing docs must stay in sync with CLI flags (there is a docs-coverage test).  (evidence: ci, history)
