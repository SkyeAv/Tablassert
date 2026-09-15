.PHONY: setup build dev test test-rust lint fmt fmt-check typecheck check clean

# Install Python dev dependencies, CLI/QC/log extras, and the editable Rust extension.
setup:
	uv sync --group dev --extra cli --extra qc --extra log
	uv run maturin develop --manifest-path rust/Cargo.toml

# Build and install the Rust extension in release mode for local performance checks.
build:
	uv run maturin develop --release --manifest-path rust/Cargo.toml

# Build and install the Rust extension in debug mode for normal development.
dev:
	uv run maturin develop --manifest-path rust/Cargo.toml

# Run the Python test suite.
test:
	uv run pytest

# Run the Rust test suite.
test-rust:
	cargo test --manifest-path rust/Cargo.toml

# Run Python lint checks.
lint:
	uv run ruff check .

# Format Python and Rust code.
fmt:
	uv run ruff format .
	cargo fmt --manifest-path rust/Cargo.toml

# Check Python and Rust formatting without changing files.
fmt-check:
	uv run ruff format --check .
	cargo fmt --check --manifest-path rust/Cargo.toml

# Run Python type checks.
typecheck:
	uv run pyright

# Run the full local quality gate.
check: lint fmt-check typecheck test test-rust
	cargo clippy --manifest-path rust/Cargo.toml --all-targets -- -D warnings

# Remove local build, test, documentation, and compiled-extension artifacts.
clean:
	rm -rf build dist site htmlcov .coverage .pytest_cache .ruff_cache rust/target src/tablassert/*.so
