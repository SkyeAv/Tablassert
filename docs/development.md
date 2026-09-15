# Development

Set up a Tablassert dev environment and learn the contributor workflow: build the Rust extension,
run the test and docs gates locally, and lint/format. The canonical contributor guide is [`CONTRIBUTING.md`](https://github.com/SkyeAv/Tablassert/blob/main/CONTRIBUTING.md).

## Setup from source

```bash
git clone https://github.com/SkyeAv/Tablassert.git
cd Tablassert
uv sync --group dev --extra cli --extra qc --extra log
uv run maturin develop --manifest-path rust/Cargo.toml
uv run tablassert --help
```

Or use the task runner:

```bash
make setup
```

## Daily loop

```bash
# Rebuild the editable Rust extension after Rust changes
make dev

# Run the full local gate before committing
make check
```

Useful focused checks:

```bash
uv run ruff check .
uv run pyright
uv run pytest tests/test_lib.py::test_idxname_single_letter
cargo test --manifest-path rust/Cargo.toml
```

## Documentation

This site is built with MkDocs Material. After editing anything under `docs/` or `mkdocs.yml`, validate the whole site locally before committing:

```bash
uv run mkdocs build --strict
```

`--strict` promotes any warning (a dead cross-link, a nav entry pointing at a missing file, or a missing referenced doc) to a build failure, so documentation drift is caught here rather than only on the deployed site. Style and PR expectations live in the canonical contributor guide, [`CONTRIBUTING.md`](https://github.com/SkyeAv/Tablassert/blob/main/CONTRIBUTING.md).

## Fullmap builds

Fullmap build environment variables are documented in [Fullmap: Build Tunables](fullmap.md#build-tunables-environment). If a full build hits `EMFILE` / `Too many open files`, raise the NOFILE limit before rerunning:

```bash
ulimit -n 65535
```
