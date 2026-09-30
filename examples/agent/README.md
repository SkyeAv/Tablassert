# Agent prompt-optimization artifacts

Experimental: these artifacts belong to the experimental `[agent]` surface
(`--optimize`, `--distill`, the QC scripts). Their formats, flags, and paths may change without
notice or a deprecation cycle.

These artifacts come from running the Tablassert `[agent]` GEPA prompt-optimization path
(`tablassert agent --optimize`) against a Qwen OpenAI-compatible endpoint.

## Files

- **`optimized_instructions.yaml`**: a GEPA-optimized agent prompt (the `instructions` the inner
  `CodeAgent` runs with), plus the per-predictor `descriptions`. Load it directly to skip the
  optimization cost in production:

  ```bash
  tablassert agent PMC11947420 --configuration-file /path/to/graph.yaml \
    --instructions-file examples/agent/optimized_instructions.yaml
  ```

  Compared to the built-in `INSTRUCTIONS`, this prompt adds explicit, feedback-derived guidance:
  a `source.url`-is-required rule, a `prioritize` entity-type mapping table (raw column headers like
  `Symbol`/`HGNC` are invalid; map them to biolink types), a per-error-code recovery cheat-sheet,
  section de-duplication limits, and a **source-path-fidelity** rule (copy the candidate table's exact
  absolute path into `source.local` verbatim).

- **`gepa-dataset.yaml`**: an example GEPA dataset (two open-access PMC gene tables). Each entry carries
  `table_summary` + `coverage_feedback` (the program inputs) and optionally `fullmap` / `workdir` /
  `head` so the GEPA metric scores each proposed config with **real** fullmap coverage.
- **`QC_REPORT.md` / `QC_REVIEW.md`**: the assay report and the LLM-as-judge review produced by
  `qc/qc_report.py` and `qc/qc_reviewer.py` from a shared agent state dir. Every per-PMC entry in both
  carries the config's `sha256` so a future report/review/config drift is detectable.
- **`qc/`**: the two QC scripts. `qc_report.py` is deterministic (no LLM); `qc_reviewer.py` calls the
  judge LM (needs `QWEN_TOKEN_PLAN_URL` / `QWEN_TOKEN_PLAN_API_KEY`) and accepts
  `<STATE_DIR> --rerender` to rebuild the markdown from an existing `qc_review.json` without
  re-querying the judge.

## Reproducing the optimization

The dataset paths (`fullmap`, `workdir`, and the table paths embedded in `table_summary`) are
**machine-specific**, so adapt them to your environment first. Then:

```bash
export TABLASSERT_AGENT_MODEL_ID="qwen3.8-max-preview"     # strong reflection LM
export TABLASSERT_AGENT_API_BASE="https://YOUR-ENDPOINT/v1"
export TABLASSERT_AGENT_API_KEY="sk-***"

tablassert agent PMC11947420 --configuration-file /path/to/graph.yaml --optimize \
  --dataset examples/agent/gepa-dataset.yaml \
  --task-model qwen3.6-flash \
  --max-metric-calls 30 \
  --instructions-out examples/agent/optimized_instructions.yaml
```

(`--task-model` is the fast LM for the many program evaluations.)

GEPA best practice (and what the flags above do): a **strong reflection LM** (`--model-id`) proposes the
few instruction edits, while a **fast task LM** (`--task-model`) runs the many candidate evaluations.
`--max-metric-calls` bounds the budget; GEPA parallelizes the candidate LM forward passes with its
library default (the coverage-scoring builds stay serialized on the process-wide `_GEPA_BUILD_LOCK`).

## QC state-directory requirement

Configs generated from `gepa-dataset.yaml` point `source.local` at tables under the **GEPA run's state
dir** (here `.tablassert/gepa/downloads/...`, since the dataset's `workdir` is `.tablassert/gepa`), but
`qc/qc_reviewer.py` only reads a config's `source.local` when it resolves INSIDE its own
`STATE_DIR/downloads` allowlist (an injection defense, see `get_table_summary`). So the QC scripts must
be pointed at the SAME state dir that holds `downloads/`, or the tables must be staged there:

```bash
# assay + judge the GEPA run's own state dir (recommended: paths already line up)
uv run python examples/agent/qc/qc_report.py .tablassert/gepa
uv run python examples/agent/qc/qc_reviewer.py .tablassert/gepa
```

(The committed `QC_REPORT.md` / `QC_REVIEW.md` were generated from a dedicated `.tablassert/qc-assay`
state dir whose `downloads/` holds the same open-access tables.) A config whose `source.local` falls
outside the QC `downloads/` dir is reported as `(source.local is outside the QC downloads dir; not read)`
rather than read or sent to the judge.
