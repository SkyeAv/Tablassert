.PHONY: setup build dev test test-rust lint fmt fmt-check typecheck check clean paper paper-check paper-pdf paper-image

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
	rm -rf build dist site htmlcov .coverage .pytest_cache .ruff_cache rust/target src/tablassert/*.so .paperdraft paper/paper.pdf

# Validate the JOSS paper draft: front matter, citation resolution, word budgets.
paper-check:
	uv run pytest tests/test_paper.py -q

# Clone + patch the JOSS paperdraft sources once; the stamp file keeps the patches
# single-application across repeated make runs.
.paperdraft/.patched:
	git clone --depth 1 https://github.com/openjournals/whedon.git .paperdraft/whedon
	# Qualify the base image registry so podman-based runtimes (no unqualified-search
	# registries) resolve it exactly like Docker does, and pin tlmgr to the frozen
	# TeXLive 2021 historic repo: the base image (pandoc/latex:2.11.2 ships TeXLive 2021)
	# points tlmgr at the live tlnet, which now serves a newer TeXLive, so the stock
	# tlmgr install calls silently install nothing.
	sed -i 's|^FROM pandoc/latex|FROM docker.io/pandoc/latex|' .paperdraft/whedon/paperdraft.Dockerfile
	sed -i '/^RUN tlmgr update --self/i RUN tlmgr option repository https://ftp.math.utah.edu/pub/tex/historic/systems/texlive/2021/tlnet-final' .paperdraft/whedon/paperdraft.Dockerfile
	# The JOSS template loads biblatex before pandoc's polyglossia and the frozen
	# TeXLive 2021 polyglossia forbids that order; pandoc citeproc renders the
	# bibliography anyway, so the biblatex block is vestigial and gets stripped.
	perl -0pi -e 's/\\usepackage\[\n  backend=biber,\n%[^\n]*\n%[^\n]*\n\]\{biblatex\}\n\\bibliography\{[^}]*\}\n//; s/\\renewcommand\{\\bibfont\}\{[^{}]*\}\n//' .paperdraft/whedon/resources/joss/latex.template
	touch .paperdraft/.patched

# Build the JOSS paperdraft compiler image from the openjournals/whedon resources.
# whedon is archived (frozen 2024-04-16) but its paperdraft.Dockerfile and LaTeX templates
# are still the official JOSS compilation path; the old openjournals/whedon Docker Hub
# image is gone, so the image is built locally. Docker layer cache makes this a no-op
# after the first build.
paper-image: .paperdraft/.patched
	docker build -f .paperdraft/whedon/paperdraft.Dockerfile -t paperdraft .paperdraft/whedon

# Compile paper/paper.md to paper/paper.pdf with the JOSS paperdraft image.
paper-pdf: paper-image
	docker run --rm -v $(PWD)/paper:/paper -w /paper paperdraft paper.md

# Full JOSS paper gate: validate the draft, then compile the PDF.
paper: paper-check paper-pdf
