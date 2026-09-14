# Installation

Get a working `tablassert` install, then pick the `rt` / `aria2` / `qc` / `agent` / `optimize` / `distill` / `log` extras that match how you will
use it (runtime compatibility, accelerated fullmap downloads, auditing mappings, running the autonomous agent, GEPA prompt optimization, distillation dataset export, or loguru-backed logging).

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

### Method 1: Development Installation with UV (Recommended for contributors)

Best for development, testing, and active work on Tablassert.

```bash
# Clone the repository
git clone https://github.com/SkyeAv/Tablassert.git
cd Tablassert

# Install development dependencies and optional QC runtime
uv sync --group dev --extra qc --extra log

# Build the editable Rust extension into the uv environment
uv run maturin develop --manifest-path rust/Cargo.toml

# Verify the CLI
uv run tablassert --help
```

This creates a virtual environment in `.venv/`, installs the development dependencies, and builds the local PyO3 extension. The `tablassert` command is available through `uv run`. See [Development](development.md) and the repository `CONTRIBUTING.md` for the daily edit/check loop.

### Method 2: Install from PyPI

Recommended for most users. The base install builds knowledge graphs from CSV/TSV sources; QC and other
extras are opt-in.

```bash
# Option A: Install from PyPI with UV
uv tool install tablassert

# Option B: Install from PyPI with pip
pip install tablassert
```

#### Optional Extras

| Extra | Description | Includes |
|---|---|---|
| `rt` | Runtime-compatible Polars build | `polars[rtcompat]` |
| `aria2` | Bundled aria2c downloader for `build-fullmap --aria2c` (Linux/Windows wheels only) | `aria2==0.0.1b0` (imports as `aria2c`, bundles aria2c) |
| `qc` | QC runtime (exact → fuzzy → abbreviation → SapBERT audit) | `scikit-learn`, `sentence-transformers` (`torch` + `numpy` arrive transitively; `rapidfuzz` is a core dependency) |
| `agent` | Autonomous PMC → KG agent (`tablassert agent`) | `smolagents`, `litellm` |
| `optimize` | GEPA prompt optimization (`tablassert agent --optimize`) | `dspy` |
| `distill` | Distillation dataset export (`tablassert distill-export` → on-disk Hugging Face dataset) | `datasets>=3.0.0` |
| `log` | loguru-backed file/progress logging (rotation, enqueue) | `loguru` |

```bash
# Install with runtime-compatible Polars
# (for CPUs without the required Polars instructions)
uv tool install "tablassert[rt]"

# pip equivalents
pip install "tablassert[rt]"

# Install the bundled aria2c downloader
uv tool install "tablassert[aria2]"
pip install "tablassert[aria2]"

# Install the QC runtime
uv tool install "tablassert[qc]"
pip install "tablassert[qc]"

# Install the autonomous agent
uv tool install "tablassert[agent]"
pip install "tablassert[agent]"
```

!!! note "`[aria2]` platform and license notes"
    The `[aria2]` extra depends on the PyPI `aria2` package, which imports as `aria2c` and bundles a static aria2c binary. Its wheels are available for Linux and Windows only; on macOS, omit `--aria2c` and use Tablassert's default Python downloader.

    The bundled aria2c dependency is GPL-2.0. Tablassert remains Apache-2.0 and does not vendor aria2c, but redistributors who ship the optional extra should review GPL-2.0 obligations.

Excel (`.xlsx`) input is read through Polars' `calamine` engine, which ships with the base install
(`fastexcel`). A handful of workbooks calamine rejects are readable by the pure-Python fallback
engine: `pip install openpyxl`.

#### When an extra is missing

Reaching a feature whose extra was never installed is a normal, recoverable mistake, so Tablassert
never lets it surface as a bare `ModuleNotFoundError`. Every one of these paths reports the absent
distribution **and** the command that fixes it:

```text
Missing optional dependencies 'scikit-learn', 'sentence-transformers', required by the QC audit.
Install the [qc] extra: pip install "tablassert[qc]" (uv: uv tool install "tablassert[qc]")
```

Where the gap is knowable up front, it is reported up front rather than mid-run:

| Command | Checked | When |
|---|---|---|
| `build-kg --qc` | `[qc]` | Before the build starts: the QC audit runs at the very end of the build, so a late failure would cost the entire entity-resolution pass |
| `tablassert agent` | `[agent]` | After flag validation, before any model is built or any article fetched |
| `tablassert agent --optimize` | `[agent]` + `[optimize]` | Same point; both are reported at once |
| `tablassert distill-export` | `[distill]` | After the recorded-NDJSON input check (an empty `--distill-dir` is reported first, since that typo is the faster loop to close) and before `datasets` is imported |
| `build-fullmap --aria2c` | `[aria2]` | Before any download starts |

A partially installed extra names every package it is still missing, so installing them is one step
rather than a retry loop. Library calls that reach an optional import directly (for example
`fullmap_audit()` or the agent's lazy `dspy` import) raise the same message at that point.

Recording needs no extra beyond `[agent]` itself: `tablassert agent --distill` writes ChatML
NDJSON with zero additional extra dependencies, while only the export step (`tablassert
distill-export`) additionally requires the `distill` extra.

The `rt` extra is the exception: it installs `polars[rtcompat]`, which imports as plain `polars`, so
it cannot be detected by inspection. It is suggested when polars itself fails to import; the usual
cause is a CPU that lacks the instructions the default polars wheel requires.

The `log` extra is the other exception: it never fails at all. Without loguru, Tablassert produces
no logs: it does not create `.tablassert/log/`, write `tablassert.log`, forward messages to the
progress display, honor `build-kg --log`, or warn about the missing extra. Install
`pip install "tablassert[log]"` for loguru-backed file and progress logging (rotation, enqueue).

### Method 3: Install from GitHub main

Use this when you want the latest main-branch build.

```bash
# Install from main branch
uv tool install git+https://github.com/SkyeAv/Tablassert.git@main
```

### Method 4: Install from local source

For contributors testing local repository changes.

```bash
# Clone the repository
git clone https://github.com/SkyeAv/Tablassert.git
cd Tablassert

# Install Tablassert CLI tool from local source
uv tool install .
```

## Verifying Installation

Confirm the CLI is on your path (use `uv run tablassert --help` for the in-repo dev environment):

```bash
tablassert --help
```

You should see the Tablassert CLI help message with available commands.

## Development Setup

For contributing to Tablassert, use the source install above, then run the local task runner:

```bash
make setup
make check
```

The underlying stable gates are `ruff check` / `ruff format --check`, `pyright`, `pytest`, and `cargo fmt --check` / `cargo test` / `cargo clippy --all-targets -- -D warnings` (see [Development](development.md)).

Install pre-commit hooks to run the fast lint/format checks automatically before commits (the full gates run in CI):

```bash
uv run pre-commit install
```

## Upgrading Development Installation

To upgrade to the latest version:

```bash
# Pull latest changes
git pull origin main

# Update dependencies and rebuild the editable extension
uv sync --group dev --extra qc --extra log
uv run maturin develop --manifest-path rust/Cargo.toml
```

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
