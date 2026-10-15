# Installation

Install Tablassert's Python API, then add the `[cli]` extra to use the `tablassert` command and any of the `rt` / `aria2` / `qc` / `log` extras that match how you will use it (runtime compatibility, accelerated fullmap downloads, auditing mappings, or loguru-backed logging).

## Prerequisites

- **Python 3.11 or higher**: Tablassert requires Python 3.11+ for compatibility with modern tooling
- **UV package manager**: Recommended for fast, reliable dependency management

### Installing UV

See the [official UV installation guide](https://github.com/astral-sh/uv) for your platform:

```bash
# Linux/macOS with curl
curl -LsSf https://astral.sh/uv/install.sh | sh

# or with pip (any platform)
pip install uv
```

## Installation Methods

### Method 1: Install from PyPI

Recommended for most users. Install into your project's environment to use the Python API (`pip install "tablassert"` or `uv add tablassert`), or install the standalone command-line tool with `uv tool install "tablassert[cli]"`. QC and other extras are opt-in.

```bash
uv tool install "tablassert[cli]"   # or: pip install "tablassert[cli]"
```

#### Optional Extras

| Extra | Description | Includes |
|---|---|---|
| `cli` | `tablassert` command and rich terminal progress | `cyclopts`, `rich` |
| `rt` | Runtime-compatible Polars build | `polars[rtcompat]` |
| `aria2` | Bundled aria2c downloader, used automatically by `build-fullmap` when installed (Linux/Windows wheels only) | `aria2==0.0.1b0` (imports as `aria2c`, bundles aria2c) |
| `qc` | QC runtime (exact match, fuzzy match, abbreviation expansion, then a SapBERT embedding audit) | `scikit-learn`, `sentence-transformers` (`torch` + `numpy` arrive transitively; `rapidfuzz` is a core dependency) |
| `log` | loguru-backed file/progress logging (rotation, enqueue) | `loguru` |

Install any extra the same way: `uv tool install "tablassert[<extra>]"` or
`pip install "tablassert[<extra>]"`, and combine them as `"tablassert[cli,qc]"`. Wheels ship for
Linux and macOS; elsewhere pip builds from the sdist, which needs a Rust toolchain.

!!! note "`[aria2]` platform and license notes"
    The `[aria2]` extra depends on the PyPI `aria2` package, which imports as `aria2c` and bundles a static aria2c binary. Its wheels are available for Linux and Windows only; the extra ships no macOS wheels, so a normal macOS install resolves to Tablassert's Python downloader.

    The bundled aria2c dependency is GPL-2.0. Tablassert remains Apache-2.0 and does not vendor aria2c, but redistributors who ship the optional extra should review GPL-2.0 obligations.

Excel (`.xlsx`) input is read through Polars' `calamine` engine, which ships with the base install
(`fastexcel`). Workbooks that calamine cannot parse are rejected with its error; there is no fallback
engine.

#### When an extra is missing

Reaching a feature whose extra was never installed is a normal, recoverable mistake, so Tablassert
never lets it surface as a bare `ModuleNotFoundError`. Every one of these paths names the absent
distribution **and** the command that fixes it, in this shape (the first sentence lists the missing
distributions; the second is the install command):

```text
Install the [qc] extra: pip install "tablassert[qc]" (uv: uv tool install "tablassert[qc]")
```

Where the gap is knowable up front, it is reported up front rather than mid-run (one row below is the
exception: `build-fullmap`'s `[aria2]` check is a downloader probe that picks a downloader and never
fails, not a failure report):

| Command | Checked | When |
|---|---|---|
| `tablassert` | `[cli]` | Before importing the command application, so a base Python-API install reports the exact install command instead of a bare module error |
| `build-kg --qc` | `[qc]` | Before the build starts: the QC audit runs at the very end of the build, so a late failure would cost the entire entity-resolution pass |
| `build-fullmap` | `[aria2]` | Before the first download, to pick the downloader: bundled aria2c when the extra is installed, Python downloader otherwise (announced on stderr and logged either way; never a missing-extra failure) |

A partially installed extra names every package it is still missing, so installing them is one step
rather than a retry loop. Library calls that reach an optional import directly (for example
`fullmap_audit()`) raise the same message at that point.

The `rt` extra is the exception: it installs `polars[rtcompat]`, which imports as plain `polars`, so
it cannot be detected by inspection. It is suggested when polars itself fails to import; the usual
cause is a CPU that lacks the instructions the default polars wheel requires.

The `log` extra is the other exception: it never fails at all. Without loguru, Tablassert produces
no logs: it does not create `.tablassert/log/`, write `tablassert.log`, forward messages to the
progress display, honor `build-kg --log`, or warn about the missing extra. Install
`pip install "tablassert[log]"` for loguru-backed file and progress logging (rotation, enqueue).

### Method 2: Install from GitHub main

Use this when you want the latest main-branch build.

```bash
uv tool install "tablassert[cli] @ git+https://github.com/SkyeAv/Tablassert.git@main"
```

### Method 3: Development install from source (contributors)

Editable install plus the development tools; needs `uv` and a Rust toolchain.

```bash
git clone https://github.com/SkyeAv/Tablassert.git
cd Tablassert
uv sync --group dev --extra cli --extra qc --extra log
uv run maturin develop --manifest-path rust/Cargo.toml
uv run tablassert --help
```

This creates `.venv/`, installs the development dependencies, and builds the local PyO3 extension;
the `tablassert` command is available through `uv run`. To install the CLI as a tool from a local
checkout instead, use `uv tool install ".[cli]"`. See [Development](development.md) and
[`CONTRIBUTING.md`](https://github.com/SkyeAv/Tablassert/blob/main/CONTRIBUTING.md) for the daily
edit/check loop.

## Verifying Installation

Confirm the CLI is on your path (use `uv run tablassert --help` for the in-repo dev environment):

```bash
tablassert --help
```

You should see the Tablassert CLI help message with available commands.

## Development Setup

For a source checkout, use Method 3 above, then the task runner:

```bash
make setup
make check
```

```bash
uv run pre-commit install
```

`make setup` syncs dependencies and builds the editable extension; `make check` runs the whole gate;
the hook install wires the fast lint/format hooks for commit and push. Gate contents, the daily loop,
and the docs build live in [Development](development.md) and
[`CONTRIBUTING.md`](https://github.com/SkyeAv/Tablassert/blob/main/CONTRIBUTING.md). To pick up
`main`, re-run Method 3's `uv sync` and `maturin develop`.

## Troubleshooting

### Python version

Tablassert requires Python 3.11+. On version errors, check `python --version` and pin a supported
release:

```bash
uv python install 3.11
uv python pin 3.11
```

### QC runtime

If `build-kg --qc` reports a missing QC runtime, install the `qc` extra (torch / sentence-transformers
SapBERT backend for the audit stage):

```bash
pip install "tablassert[qc]"
```
