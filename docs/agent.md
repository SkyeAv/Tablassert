# Autonomous Agent (`[agent]` extra)

**Why this exists:** hand-authoring a Tablassert config for every PMC supplementary table does not scale.
The optional `[agent]` extra makes it autonomous: point it at **PubMed Central (PMC)** article IDs and it
**derives the config for you**, then builds, audits, and iteratively improves the graph — the
improve loop is deterministic supervisor Python, not more LLM calls — until the entity
resolution *maps* (coverage threshold). The outcome is an **NCATS Translator-compliant KGX knowledge
graph** per article, a claim the loop verifies rather than asserts, by constructing every emitted
record as its own Biolink class (see [Biolink validity](#biolink-validity)), with the whole loop
scored on **quality / cost / wrong tool calls**.

Under the hood it is built on [smolagents](https://github.com/huggingface/smolagents) `CodeAgent` (a
ReAct loop) and [DSPy](https://dspy.ai) GEPA for prompt optimization.

!!! warning "Optional extra"
    The base `tablassert` package does **not** require any of this. `smolagents` and `dspy` are imported
    **lazily** in `tablassert.agent`, so the base install and its test suite are unaffected. Install the
    extra with `pip install "tablassert[agent]"`. GEPA prompt optimization (`--optimize`) additionally needs
    the `[optimize]` extra (`dspy`): `pip install "tablassert[optimize]"`.

## Installation

```bash
pip install "tablassert[agent]"

# GEPA prompt optimization (--optimize) additionally needs dspy:
pip install "tablassert[agent,optimize]"
```

The extra requires (lower bounds, so any newer version satisfies it):

| Package | Requirement | Role |
| --- | --- | --- |
| `smolagents` | `smolagents>=1.26.0` | `CodeAgent` ReAct loop, `OpenAIModel`/`LiteLLMModel`, tools |
| `litellm` | `litellm>=1.93.0` | optional fallback / rate-limiting model backend |

The `[optimize]` extra (only needed for `agent --optimize`) requires (a lower bound):

| Package | Requirement | Role |
| --- | --- | --- |
| `dspy` | `dspy>=3.2.1` | `dspy.GEPA` black-box prompt optimization |

## PMC-AWS data source

Tables are fetched from the **new** PMC open-access S3 bucket, the sanctioned bulk path.

| | |
| --- | --- |
| **Bucket** | `s3://pmc-oa-opendata` (us-east-1) |
| **Access** | world-readable, **free**, no credentials (`--no-sign-request`); *not* requester-pays |
| **Download** | public HTTPS `https://pmc-oa-opendata.s3.amazonaws.com/<prefix>/<file>` |
| **Layout** | one prefix per article-version, e.g. `PMC11708054.1/`, containing `PMC<n>.<v>.xml` (JATS), `.pdf`, `.txt`, `.json` (metadata) and the media/supplementary files |

`fetch_pmc_article(pmc_id, outdir)` downloads the **useful** payload for the **latest** article version,
failing fast (cheap checks before any large download and before any model call):

1. Enumerates version prefixes via S3 `list-objects-v2` (`?list-type=2&prefix=PMC<n>.&delimiter=/`) and
   selects the **latest** version (numeric, so `PMC<n>.10` beats `PMC<n>.2`); older versions are ignored.
2. Checks the latest version's `.json` metadata for open access (`is_pmc_openaccess` / a `CC*`
   `license_code`), **before** any large download (not open access ⇒ `PermissionError` immediately).
3. Enumerates the version's objects (`?list-type=2&prefix=PMC<n>.<v>/`) and confirms a data table is
   present (a file with extension `.xlsx .xls .csv .tsv`), **before** any large download (none ⇒
   `FileNotFoundError`).
4. Downloads only the **useful** files to `outdir/<prefix>/<file>` and returns their paths: the main text
   (`.xml`/`.nxml`/`.txt`), the `.json` metadata, and every data table. Binary media (images,
   `.docx`, the article `.pdf` — every version ships JATS `.xml`, so the PDF is redundant) are skipped.
   `fetch_pmc_tables` remains as a thin wrapper returning only the table files.

The main text and every candidate table are wired into the agent TWICE, deliberately: the supervisor
pre-renders the `pmc_article_context` summary (JATS title/abstract/outline/supplementary manifest),
a head preview of **every** qualifying candidate table **and every qualifying Excel worksheet**, and a
per-column **`column_digest`** block per previewed table/worksheet directly into the task text
(`render_task_context`), so the agent can author a config with **zero inspection tool calls**. Each
digest scans the first **500 data rows** and reports, per column, the **fraction of non-null cells
containing each separator** (`;`, `|`, `,`, `/`) plus non-null/distinct counts, max cell length, and
sample values — enough for `explode_by`/`split_by` detection without a single tool call. Previews and
digests are rendered inside the data fences (untrusted data, never instructions; see
[Prompt-injection defenses](#prompt-injection-defenses)). The
`pmc_article_context` / `read_table` tools stay registered as fallbacks for rows beyond a preview or a
digest's 500-row scan window (for
Excel, `read_table` lists **all worksheets** and reads a chosen one via `sheet=` (set `source.sheet`
in the config). Small tables and worksheets are filtered before this context is rendered; see
[Small-table guard](#small-table-guard).

!!! failure "The old paths are dead"
    The legacy `s3://pmc-open-access` bucket, the FTP `oa_file_list.csv`, and the per-article `tar.gz`
    bundles were **deprecated and removed (Aug 2026)**. The PMC website `/bin/` URLs are unreliable
    (404/HTML stubs) and batch-scraping the website is prohibited. Use the S3 bucket only.

!!! note "Coverage + licensing"
    Only the **open-access subset** of PMC (~half) is available here. Articles are **CC-BY**: cite the
    source and DOI (e.g. PMC11708054 → [10.1128/mbio.01679-24](https://doi.org/10.1128/mbio.01679-24)).

### Local payloads (non-open-access articles)

Only the open-access subset of PMC is fetchable from the bucket. To run the **same** derive/build/improve
pipeline on an article you already hold locally (e.g. a non-open-access paper), pass `--local`:

```bash
# one directory used for every PMC id
tablassert agent PMC11708054 --configuration-file ./graph.yaml --local ./payloads/PMC11708054

# per-article directories
tablassert agent PMC1 PMC2 --configuration-file ./graph.yaml --local PMC1=./payloads/p1 PMC2=./payloads/p2
```

A local payload directory holds the table(s) and (optionally) the article main text. When `--local` is
given for an id, the supervisor locates the files there and **does not fetch from PMC-AWS**; each section's
`source.local` points at the local file (set `source.url` to the original download link if you want the
config to be re-fetchable). A `--local` directory that does not exist fails loud (exit 2).

## Model configuration

The agent talks to an **OpenAI-compatible** endpoint (e.g. a Qwen endpoint). Configuration comes from
CLI flags **and** environment variables: **secrets are never hardcoded**, and the command **fails
loudly** if any required value is unset.

| Flag | Env var | Purpose |
| --- | --- | --- |
| `--model-id`, `-m` | `TABLASSERT_AGENT_MODEL_ID` | model identifier |
| `--api-base`, `-ab` | `TABLASSERT_AGENT_API_BASE` | OpenAI-compatible base URL |
| `--api-key`, `-ak` | `TABLASSERT_AGENT_API_KEY` | API key (secret) |
| `--backend`, `-b` | n/a | `openai` (default) or `litellm` |

```bash
export TABLASSERT_AGENT_MODEL_ID="qwen3-max"
export TABLASSERT_AGENT_API_BASE="https://YOUR-ENDPOINT.example.com/v1"   # placeholder
export TABLASSERT_AGENT_API_KEY="sk-***"                                  # placeholder: never commit a real key
```

If a value is missing, `tablassert agent` prints a message naming the exact flag/env var and exits
non-zero **before** any model call.

## Running it for real

With network access and a configured endpoint:

```bash
tablassert agent PMC11708054 PMC12345678 \
  --configuration-file ./graph.yaml \
  --map-threshold 0.25 \
  --max-improve-iters 3 \
  --max-steps 20 \
  --min-rows 50 \
  --state-dir .tablassert/agent
```

The required target is `--configuration-file`/`-f`; it supplies the fullmap, graph identity, RIG,
artifact metadata, and existing table list. Flags: `--max-steps`/`-ms`, `--min-rows`/`-mr`,
`--map-threshold`/`-mt`, `--max-improve-iters`/`-mi`, `--state-dir`/`-sd`, `--backend {openai,litellm}`/`-b`, plus `--local`/`-l`, `--reflexion`,
`--judge-model`, `--judge-threshold`, `--biolink-threshold`, the `--distill`/`-d`/`-dt` recording flag, and the `--optimize`/`-o` prompt-optimization flags
(`--instructions-file`, `--instructions-out`, `--max-metric-calls`, `--dataset`, `--task-model`).
The [CLI reference: `agent`](cli.md#agent) is the authoritative flag table; the list here is a compact
reminder.

### Small-table guard

The agent's default small-table guard is **50 non-empty data rows** (`--min-rows 50` or `-mr 50`).
The count follows the same polars parsing used by the pipeline: the header is not counted, quoted
embedded newlines remain part of one row, and rows blank across every column do not count. It applies
to both delimited files (`.csv`/`.tsv`) and Excel worksheets (`.xlsx`/`.xls`). Set `--min-rows 0`
to disable the guard; negative values fail before the agent starts.

The supervisor applies the guard after a payload is downloaded or located locally and **before it
constructs the LLM agent**. A delimited file below the threshold is removed from the candidate list.
A workbook remains available when at least one worksheet qualifies, but small worksheets are omitted
from previews and explicitly listed as excluded; the preview cap is spent only on qualifying sheets.
Unreadable files are retained fail-open so the existing `read_table` fallback can report the concrete
parse error instead of silently dropping a file. If every readable table is too small, the supervisor
records an actionable `SKIPPED` reason (including the observed row counts) without constructing a
model or running a build. The runtime task then names the qualifying sheets to focus on and instructs
the agent not to author sections for excluded sheets.

### What the supervisor does

The **outer supervisor is deterministic Python** (not an LLM); smolagents' #1 practice is deterministic
control flow over agentic decisions. For each PMC id it:

1. **Fetches** the latest-version article payload (`fetch_pmc_article`: main text + metadata + all tables;
   fails fast on not-open-access / no-table), filters out below-threshold tables and worksheets, and
   presents only qualifying candidates to the agent. If no readable candidate qualifies, it records
   `SKIPPED` before constructing the inner model.
2. Runs the **inner `CodeAgent`** to *derive* an initial table config. The task already contains the
   article summary, head previews, and `column_digest` separator statistics of every table/worksheet,
   so the canonical path is a fixed **derive → build → answer** workflow over the four-tool surface
   (`derive_config` → `build_and_audit` → final answer, target: 3 steps or fewer); `read_table` /
   `pmc_article_context` remain fallbacks only for rows beyond a digest's 500-row scan window. The
   agent rebuilds **only on a coded build error** — fixing exactly the field the error names, at most
   twice — and never loops on coverage: coverage improvement is the supervisor's job (step 4). Every
   section is gated by the Section JSON schema. The agent maps **each** mappable table/worksheet as its
   own section, **one config per paper** (see below).
3. **Builds + audits** in one deterministic mega-tool (`build_and_audit`: validate → build → QC → coverage
   → **Biolink validity**). The LLM sees a **compact** observation — exactly the 12 high-signal keys
   (verdict, coded errors + codes, coverage/Biolink/demoted-edge scores, `predicate_advice`,
   `multivalued_suspects`, node/edge counts, the `head` flag, and `unresolved` capped at 20 entries
   with a visible `+N more` marker) — while the pure `build_and_audit` function still hands the
   supervisor the **full** report (artifact paths, bookkeeping, strict-Biolink internals). The report is
   *actionable*, not just a score: a nonzero `demoted_edge_pct`
   comes with `predicate_advice` (the legal predicates for the demoted category pair), unresolved terms
   that still contain a separator surface as `multivalued_suspects` (a missed `explode_by`), and every
   report carries a `head` fidelity flag so sampled edge counts are never compared against full builds.
4. **Improves** coverage with **deterministic Python — never the LLM** — while coverage `< map_threshold`
   and budget remains: tier 1 feeds `map_coverage` feedback (called as a pure function) to a **ranked**
   list of distinct `propose_config_edit` candidates (also pure), scores them with fast head builds, and
   accepts the first full build that is **strictly better — iff no worse on coverage *or* Biolink
   validity and strictly better on one** (monotonic:
   regressions on either axis are rejected, so a coverage win can no longer be bought with invalid KGX) —
   and an edit that shrinks the full-build **edge count** by more than 25% is rejected even with a gain
   (the detail-first objective: the biggest solid config wins). The deterministic proposer covers
   **four** knob families: NodeEncoding knobs (`prioritize`/`avoid`/`regex`/`remove`/`exclude_*`),
   **`explode_by`** (added when unresolved terms still carry a separator), and — fed the audit report —
   a **demoted-predicate fix** (the first legal predicate from `predicate_advice`). Only tier 2, the
   OPT-IN `--reflexion` path, spends an LLM call, and only after tier 1 stalls; it
   may additionally change qualifiers, `split_by`, node categories, and the source.
5. **Records** metrics, **checkpoints**, and moves to the next config.

A config that won't map after `--max-improve-iters` is marked `SKIPPED: <reason>` and the supervisor
advances: one difficult article never aborts the batch. A config that **builds** but whose fullmap
coverage **cannot be measured** (an unreproducible source frame) is marked `BUILT_UNMEASURED`, a
terminal **non-failure** that is neither a certified `MAPPED` nor counted as a `SKIPPED`; the best
config is still written and is reusable by the full pipeline. Coverage measurement itself is
multi-cwd: a relative `source.local` is resolved against the build workdir as well as the current
directory before a config is declared unmeasurable.

### Optional gates: reflexion improver & semantic judge

Two opt-in extensions layer on top of the deterministic improve loop (both reuse the configured
endpoint; neither is required):

- **`--reflexion`**: when the deterministic `propose_config_edit` stalls, a tier-2 LLM reflexion
  improver reflects on the coverage feedback and proposes an edit that may change predicate/source
  (same model config).
- **`--judge-model` / `--judge-threshold`**: a semantic judge scores the built output; when
  `--judge-model` is set, `MAPPED` additionally requires the normalized score to clear
  `--judge-threshold` (`0.5` when unset). Without `--judge-model` the coverage gate alone decides.
- **`--biolink-threshold`**: `MAPPED` additionally requires the built KGX's Biolink pass rate to
  clear it. Defaults to `0.0` (report only): the rate is always measured and recorded, and raising
  the threshold turns that measurement into a terminal gate. See
  [Biolink validity](#biolink-validity) below.

### Distilling a fine-tuning dataset (`--distill`)

`--distill` (short: `-d`, `-dt`) records **every LLM call of the run** — the inner agent's
multi-turn conversations, plus the judge and reflexion calls when those gates are enabled — as one
ChatML JSON object per line, appended to `<state-dir>/distill/records.ndjson`. Every record line
carries the same canonical 13-key v2 set, with an explicit `null` where a value is unknown, so a
key never first appears partway down an append-only file:

```json
{"record_type": "record", "schema_version": 2, "run_id": "a1b2c3d4e5f6:PMC11708054", "timestamp": "...", "purpose": "agent", "call_index": 0, "messages": [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}], "token_usage": {"input_tokens": 512, "output_tokens": 128}, "input_tokens": 512, "output_tokens": 128, "n_messages": 3, "pmc_id": "PMC11708054", "model_id": "..."}
```

Alongside the records, the supervisor appends **one outcome line per run** to a sibling
`outcomes.ndjson` in the same directory: the terminal status, the build/audit figures, the
tool-call tallies, the gate thresholds, and the package versions — the full 35-key outcome set,
schema-uniform with explicit nulls in the same way (abbreviated here):

```json
{"record_type": "outcome", "schema_version": 2, "run_id": "a1b2c3d4e5f6:PMC11708054", "run_status": "MAPPED", "ok": true, "measured": true, "head": false, "coverage_pct": 0.86, "biolink_valid_pct": 1.0, "demoted_edge_pct": 0.02, "edge_count": 412, "tool_calls": {"total": 5, "failed": 0, "wrong": 0, "redundant": 0}}
```

`run_id` (`<invocation-id>:<pmc-id>`) is the join key between the two files. They are separate
because the two halves exist at different times: a record is appended the moment each model call
completes, mid-run, while the outcome — build verdict, coverage, Biolink validity, demoted-edge
fraction — is only known once the supervisor has decided the run's terminal status, so it is
written exactly once, at the end of the run. Both files are **append-only** and never rewritten in
place: a corpus accumulates over many batches, and retuning a weight never requires re-recording
it. The `messages` column is plain ChatML, which Unsloth Studio auto-detects on JSONL upload (no
column mapping needed). Recording is zero-dependency and never breaks a run: a failed write is
logged, not raised. `--distill` is not supported with `--optimize` (the GEPA path bypasses the
recording seam).

**Weighing.** [`tablassert distill-weigh`](cli.md#distill-weigh) joins the two files on `run_id`,
computes a deterministic reward weight per record, applies a selection policy, and writes one flat
training row per input record plus a reproducibility manifest. Flags: `--distill-dir`/`-dd` and
`--out`/`-o` (both required), `--policy`/`-p` (default `threshold`), `--threshold`/`-t` (`0.75`),
`--top-n`/`-tn` (`2`), `--replication-k`/`-rk` (`2`), `--reward-config`/`-rc` (a YAML/JSON policy
override), `--edge-ref` (a breadth-reference override), `--purpose` (default `agent`; the literal
`all` disables filtering), `--final-call-only` (keep only each run's highest `call_index` — the
most complete conversation), and `--manifest` (default `<out>.manifest.json`). Keep `--out`
**outside** `--distill-dir`: [`tablassert distill-export`](cli.md#distill-export) loads every
`*.ndjson` in its input directory, so a weighed file placed there would be re-ingested as raw
corpus. The weigh → export composition (export requires the `[distill]` extra):

```bash
tablassert distill-weigh --distill-dir .tablassert/agent/distill --out ./training/train.ndjson
tablassert distill-export --distill-dir ./training --out ./hf-dataset
```

**The reward** is a deterministic function of the captured outcome — same outcome in, same weight
out — and it is **tunable policy with documented defaults**, not a law: every knob is a
`RewardConfig` field overridable via `--reward-config`, and retuning never touches the corpus. The
defaults are Tablassert's own policy (they mirror the shape of the supervisor's quality score with
its two degenerate terms replaced), not a borrowed standard. Five additive terms whose
coefficients sum to 1.00:

- `coverage_pct` — completeness / entity-resolution success — **0.40**
- `biolink_valid_pct` — semantic validity of the built edges — **0.28**
- `specificity = 1 - demoted_edge_pct` — predicate specificity vs generic fallback — **0.17**
- `cleanliness = 1 - (failed + wrong) / total tool calls` — tool-call correctness — **0.07**
- `breadth = clamp(log1p(edge_count) / log1p(edge_ref), 0, 1)` — non-degeneracy — **0.08**

The hard gates then apply **multiplicatively**, never additively: `ok is not True`, `head is
True`, or a `SKIPPED`/`FAILED` status zero the row outright; `BUILT_UNMEASURED` (or coverage never
measured) floors the row at `unmeasured_weight` (default `0.0`) — the record is kept, just not
selectable. On top of the raw score, two farming penalties multiply: `demoted_edge_pct > 0.50`
scales by **0.5** (generic-predicate farming), and a redundant tool-call share above **0.30**
scales by **0.7**. The breadth reference `edge_ref` defaults to the **corpus median** edge count
over comparable builds (head builds and failed builds excluded, deduplicated to the last outcome
per run id); `--edge-ref` or the config file pins it instead, and a corpus with no comparable
build warns and lets breadth contribute 0.0. The reward reads only deterministic fields — never
the judge score, `qc_pass_rate`, or `provenance_ok`, which are recorded as metadata only.

**Selection policies.** All three are computed at weigh time (re-recording is never needed) and
annotate every row with the same five keys (`weight`, `selected`, `replicas`, `policy`,
`threshold`):

| Policy | What it does | Prefer when | Caveat |
| --- | --- | --- | --- |
| `threshold` (default) | `selected = weight >= --threshold` (0.75); each selected row counts once | The zero-code path — the only selection TRL consumes with no trainer code | A miscalibrated corpus can empty the selection; the manifest's `selected_count` makes that visible |
| `best-of-n` | Groups by `pmc_id`, ranks each group by weight (ties: fewer attempts, fewer failed tool calls, earlier call), keeps the top `--top-n` (2) | Several attempts per article: keep the best trajectory per prompt while preserving distinct paths | **No weight floor**: a prompt whose every trajectory weighs 0.0 still contributes its top-ranked row, so gated-failed trajectories can be selected — the manifest's `selected_zero_weight` exposes this |
| `replication` | `selected = weight > 0`; `replicas` scales from 1 to `1 + --replication-k` (default 2, capped at 3) across the selected rows' weight spread | Soft importance weighting with no custom loss code | `replicas` is a **count on one row**, never physical duplication — the training sampler must expand the rows |

**The goal is LoRA/QLoRA supervised fine-tuning — explicitly not RLHF.** There is no reward model,
no PPO/GRPO, and no online RL anywhere in this pipeline; the reward is a deterministic scoring
function used for data selection. The weight drives row filtering, ranking, and replication
because TRL's `SFTConfig` has **no per-example sample-weight column** — selection and replication
are the only zero-code weighting mechanisms an SFT trainer offers.

Consumption caveats that are easy to get wrong:

- `SFTConfig.max_length` defaults to **1024** with `truncation_mode="keep_start"`: long multi-turn
  agent trajectories are silently truncated, and TRL then **drops** examples left fully masked.
  Size it from the recorded `n_messages`/`tokens_total` metadata — set `max_length=None` or to at
  least the corpus's p99.
- `assistant_only_loss=True` trains on the assistant (agent) turns only — usually what you want
  for a config-authoring agent.
- `packing=True` makes any effective weight **token-proportional rather than row-proportional**
  and destroys per-example identity, which quietly undermines replication.
- Extra metadata columns (every `outcome_*` column, `weight`, `selected`, and friends) are
  **ignored** by TRL, not fatal — safe to keep them in the file.

Honest limitations. SFT on curated optimal trajectories stabilizes output **format and schema
compliance** — for a YAML-config-writing agent whose supervisor is deterministic Python, that is
the honest deliverable of a LoRA. It is **not** evidence-backed for out-of-distribution
generalization (Chu et al., arXiv:2501.17161, App. C.1); do not promise OOD gains. Two
reward-hacking caveats: `demoted_edge_pct` gating exists because coverage can be **farmed with a
generic predicate** — a predicate the derived association class forbids never raises, it silently
demotes the edge, so a config can map perfectly while emitting bare `biolink:Association` edges —
and over-hard selection is a measured Goodhart risk (Gao et al., arXiv:2210.10760). Hold out a
differently-scored validation set rather than trusting the same weight that selected the training
rows.

### Biolink validity

Coverage answers *did the terms resolve?* It says nothing about whether the resulting records are
consumable. The agent therefore validates **its own output**: after each build, `build_and_audit`
constructs every emitted node and edge as the Biolink Pydantic class named by its own `category`,
the same check [`tablassert validate-kgx`](cli.md#validate-kgx) runs, and the same classes
`translator-ingests` builds. Six fields land in the audit report:

| Field | Meaning |
| --- | --- |
| `biolink_valid_pct` | Pass rate excluding known-pending fields. **This is the scored number.** |
| `biolink_valid_pct_strict` | Pass rate with no exemptions, so the pending gap stays visible |
| `biolink_problems` | Top `"field: error-type"` failures with counts, for self-correction |
| `demoted_edge_pct` | Fraction of edges that fell back to bare `biolink:Association` |
| `predicate_advice` | Per demoted (predicate, subject, object) group: the derived association class and the **legal predicates** — the exact fix, not just the symptom |
| `multivalued_suspects` | Entity columns whose unresolved terms still contain a separator (`;`, `\|`, `,`) — a missed `explode_by`, with the literal separator to declare |

**`demoted_edge_pct` is the predicate signal.** Tablassert derives an edge's association class from
the (subject category, object category) pair, then `resolve_association_class` gives up as much of
that class as the predicate requires. A predicate the class forbids is **never an error**: it
silently demotes the edge and discards every qualifier and evidence slot that class declared. So
`gene_associated_with_condition` on a gene~disease table builds cleanly, maps perfectly, and produces
`biolink:Association` edges. `predicate_advice` turns that signal into a fix, and the deterministic
proposer applies it automatically when handed the audit report.

The prompt now carries a **generated legal-predicate table** for the category pairs the agent meets in
practice, rendered at import from the installed `biolink-model` (via `lib.predicate_options`) so it
cannot drift from the model the build validates against:

```text
- Gene ~ Disease -> GeneToDiseaseAssociation: affects, associated_with, contributes_to
- SequenceVariant ~ Gene -> VariantToGeneAssociation: condition_associated_with_gene, …
- any predicate is safe for: Gene~Gene, Gene~Pathway, ChemicalEntity~Disease, …
```

- An annotation like `supporting_study_size` or `sample_size` names study-level metadata.
  biolink-model 4.4.4 ([PR #1770](https://github.com/biolink/biolink-model/pull/1770))
  deprecated the old `supporting_study_*` association slots and replaced them with `Study`
  node properties, so the value is carried on the edge's inlined supporting `Study` (as
  `study_size`, `study_cohort`, and related fields) rather than emitted on the edge.
  `relationship_strength` is not one of these. It is a legacy alias coerced to the real edge
  slot `effect_size`. Names that are not association slots at all (`fold_change` alone,
  `z_score`, and similar names) are folded into `supporting_text`. Authoring any relocated
  name emits a `BiolinkRelocationWarning` naming where the value actually went, a warning,
  not an error: nothing is lost, and every existing config keeps building.
- Enum-ranged qualifiers take a literal token (`object_direction_qualifier: increased`), never a
  CURIE, and are deliberately **not** entity-resolved. `map_coverage` skips them for the same reason
  the build does, so they no longer depress a config's coverage score for working correctly.

### Multi-section configs (one per paper)

The agent authors **one table config per paper** that may contain **multiple sections**, one per
mappable supplementary table/worksheet. The config is shaped as `{template, sections}`:

- **`template`** carries the shared per-paper **provenance** (`repo` + `publication`) and nothing else:
  in particular **no `source`**.
- **`sections`** is a list with one entry per table; **each section owns its own `source`** (its own
  `local` path **and** its own `source.url` download link, plus `sheet`/`row_slice`/`delimiter` as
  needed) and its own `statement`. Different sections can therefore reference **different files with
  different download links**.

The final-answer gate (`validate_table_config`) validates **every** section, so a config is accepted
only when all of its sections are schema-valid. `map_coverage` measures each section and reports an
**aggregate** (`overall` = mean of section coverages, `min` = weakest section, `measured` = true iff
every section measured, plus the per-section breakdown under `sections`). The supervisor's
`propose_config_edit` edits
each section independently from its own coverage entry. A single-table paper is still one config with
one section. State and storage stay **per-paper**: one best config (`configs/<pmc_id>.yaml`) holding
all sections, with `section_coverages` recorded for visibility.

### Workspace layout, target graph, and checkpoint / rerun

`--configuration-file` is the caller-owned Graph YAML that the agent updates in place. Its `fullmap`,
`name`, `version`, complete `rig:`, and artifact metadata drive every one-table audit. The agent does
**not** create an aggregate graph under `state_dir`; `state_dir` remains only the checkpoint and working
artifact directory (default `.tablassert/agent`, override with `--state-dir`):

```text
project/graph.yaml                       # caller-owned aggregate graph, updated in place
.tablassert/agent/                       # checkpoint/artifact workspace
  state.json                             # supervisor checkpoint (atomic)
  configs/<pmc_id>.yaml                  # accepted generated table config (absolute source.local)
  configs/<pmc_id>.derived.yaml          # initial generated config
  downloads/<pmc_id>/<prefix>/...        # fetched PMC payload; stable across runs
  builds/<pmc_id>/table.yaml             # temporary one-table audit input
  builds/<pmc_id>/artifacts/             # <graph-name>_<graph-version>.{nodes,edges}.ndjson + RIG
  builds/<pmc_id>/.tablassert/store/      # temporary parquet cache
```

Only newly generated agent table configs are normalized: every section's `source.local` is written as
an absolute local/data-lake path, and the graph's new `tables` entry is an absolute path. Existing
user-authored table YAMLs and their source paths are not rewritten. The target graph's existing metadata
and unrelated table entries are preserved.

Before the accepted best config is persisted it is also **compacted deterministically**
(`compact_config`), after normalization: provably no-op entries (keys equal to the Pydantic model
defaults) are removed while semantics are preserved — the compacted config builds the identical KGX
and scores the identical `quality_score` (pinned by the offline accuracy-invariance test). Compaction
can only shrink a config or leave it alone, never corrupt it: any failure writes the normalized
uncompacted config and the status is unaffected. Each record tracks `config_chars` — the character
count of what was actually written to `configs/<pmc_id>.yaml` — in `state.json`, so size deltas are
auditable per article.

Each record also carries `error_code` — the stable kebab-case `code` of the exception that caused a
`SKIPPED` record, or `null` for a deterministic gate, an uncoded error, or a pre-field `state.json`.
It describes the **most recent attempt**: starting an attempt clears it, so a later `MAPPED` result
cannot retain a stale transient code. Consumers should inspect it only when `status == "SKIPPED"`.
`network-transient` and `llm-transient` mean requeue the article; the CLI emits the latter through one
bounded retry layer for the inner agent, reflexion, and judge. Smolagents' retryer and OpenAI's client
retryer are disabled; LiteLLM's internal retry setting is not exposed by `LiteLLMModel.__init__`.
The worst case is 29 logical calls per article (20 agent + 3 reflexion + 6 judge) × 45 seconds =
1,305 seconds (about 21.8 minutes), leaving the rest of the 90-minute timeout for real work. Other
codes are terminal for that payload. Switch on `error_code` rather than keyword-matching `notes`;
`status` remains `SKIPPED` and the `SKIPPED: <error>` prefix remains for older consumers. The
supervisor also writes one `ERROR` log line for every catch-all skip.

A result is appended to the target graph only when it is `MAPPED` or `BUILT_UNMEASURED`. `SKIPPED`
articles never append. If the same PMC is processed again, its old table entry is replaced and the new
absolute config path is appended. Requested PMCs are deliberately processed again even when `state.json`
contains a terminal record; this makes reruns effective while retaining attempts, coverage history, and
metrics. A failed rerun does not replace the prior successful config.

The agent audits each candidate with a one-table in-process graph, so it does not rebuild every table
already present in the target graph. The temporary audit inherits the target graph's semantic metadata
and graph identity but writes physical artifacts to an isolated per-article workspace. Build the complete
aggregate explicitly after the agent finishes:

```bash
tablassert agent PMC11708054 --configuration-file ./graph.yaml --state-dir .tablassert/agent
# inspect graph.yaml, then build every existing + generated table together
tablassert build-kg -f ./graph.yaml
```

!!! warning "Absolute paths are intentional"
    Generated `tables` entries and generated `source.local` values are absolute so the target graph can
    be built from any current working directory. Moving the data lake, downloaded payload, or workspace
    requires updating those generated paths or rerunning the agent.

### Concurrent agents targeting one graph

Several agent processes may target the same caller-owned graph. Each successful append takes an exclusive
`<graph>.lock` sidecar lock and atomically replaces the graph YAML, so distinct PMCs do not lose one
another's entries and a same-PMC rerun has deterministic last-writer-wins replacement. The checkpoint
`state.json` read-modify-write is still per-workspace and is not cross-process locked; use separate
`state_dir` values for concurrent processes unless they intentionally coordinate their article ids.

## The tools

Full mode registers **exactly four** LLM tools — `read_table`, `pmc_article_context`, `derive_config`,
`build_and_audit` — the derive → build → answer surface. `map_coverage` and `propose_config_edit` are
**not** in the full-mode agent's surface: they are pure helpers the deterministic supervisor calls
itself in its improve loop (coverage improvement is the supervisor's job, after the agent answers).
Two batch derive modes vary the surface: `derive_only` registers only the three
inspection/authoring tools (no fullmap tools, so derivations parallelize), and `derive_coverage`
swaps `build_and_audit` for `map_coverage` (coverage feedback without the KGX build, so the agent
can pick the best sheet/columns).

| Tool | Kind | Purpose |
| --- | --- | --- |
| `fetch_pmc_article` | function | PMC-AWS download of the useful latest-version payload (main text + metadata + tables), fail-fast |
| `pmc_article_context` | tool | parse the JATS main text into a **data-fenced** summary (title/abstract/sections/supplementary manifest); `.txt` renders a fenced excerpt |
| `read_table` | tool | render a table as **data-fenced, spotlighted** text; lists **all worksheets** of an Excel file (`sheet=`) |
| `derive_config` | tool | author a table config (`template` + one section per table); each section must satisfy `Section.model_json_schema()` |
| `build_and_audit` | tool | **one** deterministic validate→build→QC→coverage→**Biolink-validity** mega-tool; the LLM observation is the **compact** 12-key report (`unresolved` capped at 20 + `+N more`), while direct/supervisor callers of the pure function get the **full** report; the report's `predicate_advice` / `multivalued_suspects` fields make demotions and missed `explode_by`s directly actionable |
| `map_coverage` | tool (`derive_coverage` mode only) / supervisor pure helper | fullmap term-resolution coverage (per-column + overall); in full mode ONLY the deterministic supervisor calls the pure function (improve loop + per-section recording), never the LLM |
| `propose_config_edit` | supervisor pure helper | deterministic, constrained edits + rationale used ONLY by the supervisor's improve loop (never an LLM tool): `NodeEncoding` knobs, `explode_by` from separator-carrying unresolved terms, and (given the audit report) a demoted-predicate fix |

`build_and_audit` returns coded errors **verbatim** (each carries a docs URL) so the agent can
self-correct the exact offending field. `derive_config` does the same: a candidate config that fails
the Section schema comes back as its coded error instead of being forwarded, because the final-answer
gate can only answer true/false and would otherwise swallow the reason.

## Prompt engineering

The agent's `instructions` make the techniques explicit:

- **Detail-first goal ordering**: the goals are (1) BREADTH + DETAIL — every mappable sheet as its own
  section, every evidence slot captured, multi-valued cells exploded, direction/aspect columns
  qualified; (2) coverage; (3) Biolink validity / QC; (4) efficiency LAST — the prompt states plainly
  that a mappable sheet or evidence column is never sacrificed to save a tool call.
- **Digest-first `explode_by`/`split_by` detection**: every previewed table/worksheet ships its
  injected `column_digest` (separator fractions over the first 500 data rows), and the prompt directs
  the agent to read those statistics FIRST — an entity column with a dominant separator gets
  `explode_by` for exactly that separator; `read_table` is justified only for rows beyond the digest's
  scan window.
- **ReAct, planning off**: `CodeAgent` is a ReAct loop, but periodic re-planning is disabled
  (`planning_interval=None`): each planning turn is a whole extra LLM round trip carrying the full
  prompt, and the task already prescribes a fixed short workflow (derive → build → answer): on a
  coded build error the agent fixes exactly the named field and rebuilds (at most twice) and never
  loops on coverage — the supervisor's deterministic improve loop keeps raising coverage after the
  agent finishes.
- **Structured / constrained output**: `derive_config` injects the Section JSON schema; a
  `final_answer_checks=[validate_table_config]` gate means the agent can only terminate with a config
  whose **every section** is schema-valid (multi-section configs are validated section-by-section).
- **A regex cookbook**: the prompt teaches the actual semantics agents get wrong — Rust-regex
  substitutions (no backreferences, no lookarounds), single-quoted YAML so backslashes stay literal,
  `regex` vs `remove` vs `exclude_regex`, and that CURIEs come from resolution or `prefix`/`suffix`,
  never from capture groups.
- **Positive qualifier guidance**: direction/aspect columns map to `object_direction_qualifier` /
  `object_aspect_qualifier` (`method: column` + `nullable: true` for blanks); enum-ranged qualifiers
  take literal tokens; `qualified_predicate: biolink:causes` is the one CURIE-taking exception;
  `species_context_qualifier` stays banned.
- **Predicate specificity**: pick the most-specific predicate the derived association class permits,
  chosen from the generated legal-predicate table — never a generic default and never a predicate the
  class forbids; `predicate_advice` in the audit report names the exact fix when demotion happens.
- **Few-shot exemplars**: the tutorial gene~disease section, the ALAMV6 organism~chemical section, a
  multi-section config (one config, two tables, each section its own source/url), and a **rich
  exemplar** combining `explode_by: ";"`, a column qualifier, a regex strip, and the paired
  `effect_size`/`effect_type` annotations — every exemplar's predicate is a legal, specific choice for
  its category pair (guarded by tests).
- **Reflexion-style self-critique (supervisor-side)**: the deterministic `propose_config_edit` /
  `reflexion_improve` reflect on failing rows,
  error codes, and unresolved terms, then make a targeted, schema-valid edit — in the supervisor's
  improve loop, never inside the agent's tool surface.
- **Error-recovery prompting**: tools return rich coded errors; the prompt directs the agent to read the
  code + message and fix precisely that field, never repeating an unchanged config.
- **Context trimming**: a `step_callback` tallies tokens/steps and failed/wrong/redundant tool calls, and
  trims large old observations to save tokens.

### Prompt-injection defenses

PMC article text and tables are **untrusted data**. Defenses:

- **Data-fence + spotlighting**: `read_table` wraps content in `<<<PMC_DATA_BEGIN>>>` /
  `<<<PMC_DATA_END>>>` preceded by a guardrail; the instructions state that fenced content is DATA, never
  instructions, and any embedded commands are ignored.
- **Minimal authorized imports**: the executor allowlist is exactly `["yaml"]`, so a hijacked agent cannot
  `import os`/`subprocess`.

## Evaluation & optimization loop

The harness scores every run on three objectives and optimizes them as a black box.

**Deterministic metrics (gate the loop):**

- **Quality**: fullmap mapping coverage (0.40), **Biolink pass rate** (0.25), KG node/edge **F1** vs
  the reference graph (0.15), QC audit pass rate (0.10), and config schema validity (0.10, and a hard
  gate: an invalid config scores 0).
- **Cost**: `RunResult.token_usage` + step count (the API is free; tokens are the proxy).
- **Reliability**: failed / wrong / redundant tool-call counts from the `ActionStep` logs.

**LLM-as-judge (semantic dimensions only):** a pointwise **0–3** rubric over schema validity, coverage,
**Biolink validity**, QC pass, predicate/category appropriateness, provenance completeness, efficiency,
and tool-call cleanliness, with **position** (both orderings averaged) and **verbosity** bias mitigation. Deterministic
metrics gate the rest; the judge only scores what a metric cannot. Without a judge model, an offline
deterministic heuristic is used.

**Optimizers:**

- **Reflexion**: the simple first-increment retry (`reflexion_improve`).
- **GEPA**: `dspy.GEPA(metric=gepa_metric, candidate_selection_strategy="pareto", …)` optimizes the
  agent's `instructions` + tool `description`s + exemplars as a **black box** from textual feedback
  (`gepa_metric` returns `dspy.Prediction(score=weighted_quality, feedback="<failing rows + error codes +
  Biolink problems + demoted-edge fraction + the legal predicates from predicate_advice + missed
  explode_by suspects + wrong-call list>")`). It is system-agnostic, Pareto-native, and needs few rollouts.

**Reporting:** `pareto_frontier(runs)` returns the **non-dominated set** over (quality ↑, cost ↓,
wrong-calls ↓) and its **knee** (best quality per unit cost).

### Real-run prompt optimization (`--optimize`)

GEPA prompt optimization is a first-class CLI path. `tablassert agent --optimize` (`-o`) runs
`dspy.GEPA` and **persists the optimized instructions** instead of running the supervisor.

Following GEPA best practice, the optimizer splits the models: a **strong reflection LM** (`--model-id`)
proposes the few instruction edits, and an optional **fast task LM** (`--task-model`) runs the many
candidate program evaluations. Pointing `--task-model` at a cheap model (e.g. a flash model) keeps the
run fast while the strong model does the thinking; without `--task-model` the reflection LM is used for
both. GEPA runs its candidate evaluations with its library default parallelism; the coverage-scoring
builds stay serialized on the process-wide `_GEPA_BUILD_LOCK` (`agent.py`, since `os.chdir` is
process-global), so extra parallelism does not speed up the expensive build/coverage step.

```bash
# optimize the agent prompt over a dataset of examples, writing the result to a file
tablassert agent PMC11708054 --configuration-file ./graph.yaml --optimize \
  --dataset examples/gepa-dataset.yaml --task-model qwen-flash \
  --max-metric-calls 30 \
  --instructions-out .tablassert/agent/optimized_instructions.yaml

# later, run the supervisor with the optimized prompt
tablassert agent PMC11708054 --configuration-file ./graph.yaml \
  --instructions-file .tablassert/agent/optimized_instructions.yaml
```

`--dataset` is a YAML/JSON list of examples. Each example carries `table_summary` and
`coverage_feedback` (the program inputs); it MAY also carry:

- `fullmap`: a fullmap path. When present, the GEPA metric scores each proposed config with **real
  fullmap coverage** (via a `build_and_audit` head-sample), so GEPA optimizes the genuine objective
  rather than a validity-only proxy.
- `workdir`: the directory a proposed config's relative `source.local` resolves against (LLMs mimic the
  exemplar's `./downloads/...` paths), so coverage is measured on the actual table.
- `head`: defaults to `true`, which scores a fast 5-row preview; set `false` for full-fidelity coverage builds.

`--max-metric-calls` bounds the GEPA metric budget. `save_optimized_instructions` /
`load_optimized_instructions` persist and reload the prompt (a `{instructions, descriptions}` mapping).
Without `--instructions-file` the built-in `INSTRUCTIONS` prompt is used. The committed
`examples/agent/optimized_instructions.yaml` is a GEPA **artifact** from an older seed — do not hand-edit
it; rerun `--optimize` so GEPA starts from the current (detail-first) seed prompt instead. (A real
optimization run needs a live model; the offline suite exercises this path via an injectable `gepa_cls`
stub.)

### Golden fixture

`tests/agent_fixtures/PMC11708054/` is an offline replay pair: the ALAMV6 reference config, a small
**synthetic** source table, and a trimmed reference config (CC-BY attribution to PMC11708054; the
reference KGX is computed in-test against a tiny real redb; nothing large is committed). A second
fixture, `tests/agent_fixtures/GENE_DISEASE/`, is a gene~disease config in multi-section
(`{template, sections}`) shape with PMID provenance, used to keep the offline heuristic judge and the
W3 multi-section validation honest on a distinct config.

## Edge-count acceptance (agent vs reference)

An agent-produced config is only as good as the graph it emits. The acceptance gate is an **edge-count
fraction**: built over the SAME payload against the SAME fullmap, the agent config must emit at least
half the KGX edges of a richer hand-curated reference config: `agent_edges >= 0.5 * reference_edges`
(`REFERENCE_EDGE_FRACTION` in `tests/test_agent_edgecount.py`). A config that reads only one sheet, or
that skips `explode_by` on a multi-valued column, silently emits far fewer edges; this gate catches it.

**Offline harness (committed fixtures):** `tests/fixtures/edgecount/` ships a synthetic
PMC10766526-shaped disease x system workbook plus three configs: the improved-agent shape
(multi-section, correct `sheet` + `row_slice`, `explode_by`/`prioritize` breadth, paired
`effect_size` + `effect_type` annotations), a strictly richer reference (all three sheets), and an
intentionally-impoverished single-section no-`explode_by` negative control that MUST fail the gate:

```bash
uv run --extra qc --extra agent pytest tests/test_agent_edgecount.py -q
```

**Real runbook (PMC10766526):** run the agent over the downloaded payload, build one-table graphs
for the agent config and the already-converted v12 reference config, and compare the
`<name>_<version>.edges.ndjson` line counts from `rig.artifact_base_path`:

```bash
# 1. Run the agent OFFLINE against a local payload (no PMC-AWS fetch)
tablassert agent PMC10766526 --configuration-file ./graph.yaml \
  --local PMC10766526=./downloads/PMC10766526
# accepted config: .tablassert/agent/configs/PMC10766526.yaml

# 2. Build each as a one-table graph (same fullmap, same graph name/version conventions)
tablassert build-kg -f ./agent_graph.yaml       # tables: [.tablassert/agent/configs/PMC10766526.yaml]
tablassert build-kg -f ./reference_graph.yaml   # tables: [./legacy/PMC10766526.v12.yaml]

# 3. Count edges; acceptance: agent >= 0.5 * reference
wc -l <agent-graph-name>_<version>.edges.ndjson <reference-graph-name>_<version>.edges.ndjson
```

The same comparison is scriptable via the env-gated test: set `TABLASSERT_PMC_COMPARE` to a JSON
array of four paths: the agent config, the reference config, the payload, and the fullmap redb.
A JSON array (not a colon-separated string) keeps POSIX paths containing `:` and Windows
drive-letter paths working. The `<agent-config>` and `<reference-config>` may carry relative
`source.local` paths; both are rebuilt over `<payload>`:

```bash
TABLASSERT_PMC_COMPARE='[".tablassert/agent/configs/PMC10766526.yaml", "./legacy/PMC10766526.v12.yaml", "./downloads/PMC10766526/PMC10766526.1/table.xlsx", "data/fullmap.redb"]' \
  uv run --extra qc --extra agent pytest tests/test_agent_edgecount.py::test_real_pmc_comparison -q
```

Unset, that test skips with a printed reason; the offline fixture tests run regardless.

## Testing

The agent suite is **fully offline**: no live LLM or network. It uses a `FakeModel` smolagents stub,
mocked/snapshotted PMC data, a tiny real redb (`rs.build_fullmap_db`), and injectable GEPA stubs.

```bash
# Agent tests SKIP without the extra and PASS with it:
uv sync --extra agent
uv run pytest -q tests/test_agent_eval.py tests/test_agent_supervisor.py tests/test_agent_assembly.py
```

Without the extra, the base suite stays green and every agent test skips via `pytest.importorskip`.

!!! tip "Telemetry"
    A real `CodeAgent.run` emits HuggingFace telemetry that blocks on a network call. The agent tests set
    `HF_HUB_DISABLE_TELEMETRY=1` / `DO_NOT_TRACK=1` to stay hermetically offline; set the same when running
    fully air-gapped.
