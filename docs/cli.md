# CLI Reference

Tablassert extracts knowledge assertions from tabular data into KGX NDJSON. The `tablassert` app
exposes **six subcommands**: `agent`, `build-fullmap`, `build-kg`, `distill-export`, `validate`,
and `validate-kgx`, plus an app-level `--version` flag. Run `tablassert --help` (or `<command> --help`)
for the live surface.

## Command index

| Command | Use this to… |
| --- | --- |
| [`agent`](#agent) | Autonomously derive, build, audit, and improve KG configs from PMC articles |
| [`build-fullmap`](#build-fullmap) | Build the embedded fullmap redb used for entity resolution |
| [`build-kg`](#build-kg) | Build a KGX NDJSON knowledge graph from a YAML configuration |
| [`distill-export`](#distill-export) | Export a recorded distillation NDJSON dataset to an on-disk Hugging Face dataset |
| [`validate`](#validate) | Validate a graph or table configuration without executing it |
| [`validate-kgx`](#validate-kgx) | Validate built KGX NDJSON against the Biolink Model |

## App flags

These are flags on the root `tablassert` command, **not** subcommands.

| Flag | Description |
| --- | --- |
| `--version` | Print the installed package version as `tablassert <version>` (e.g. `tablassert 10.1.0`) and exit |
| `--help`, `-h` | Show help for the app or a subcommand |

!!! warning "Two different `--version`s"
    The app `--version` prints **Tablassert's package version**. The [`build-fullmap --version`](#build-fullmap)
    flag is unrelated: it selects a **RENCI BABEL snapshot date** (default `2026jul22`).

---

## agent

Use this to autonomously turn one or more PMC articles into audited, improved KG configs and graphs
(fetch → derive config → build + audit → improve until coverage maps). Requires the `[agent]` extra
(`pip install "tablassert[agent]"`); `--optimize` additionally needs the `[optimize]` extra
(`pip install "tablassert[optimize]"`, pulls `dspy`). Both are checked after flag validation and
before any model is built or article fetched, so a missing extra is reported with its install
command instead of surfacing mid-run; see [When an extra is missing](installation.md#when-an-extra-is-missing).

```bash
tablassert agent PMC-IDS... --configuration-file GRAPH.yaml [OPTIONS]
```

PMC ids are passed positionally (also accepted as `--pmc-ids`). The required graph target is accepted
as `--configuration-file` or `-f`; it is modified in place after successful article builds. The graph's
`fullmap`, name, version, RIG, and artifact metadata replace the old standalone fullmap argument. This
page lists the flags; see
[Agent](agent.md) for the full pipeline, workspace layout, checkpoint/resume, and tooling.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `PMC-IDS` (`--pmc-ids`) | list[str] | Yes | n/a | One or more PMC article ids (positional) |
| `--configuration-file`, `-f` | Path | Yes | n/a | Caller-owned Graph YAML; supplies build metadata/fullmap and receives successful absolute table entries |
| `--model-id`, `-m` | str | No | `None` | Model id (env `TABLASSERT_AGENT_MODEL_ID`) |
| `--api-base`, `-ab` | str | No | `None` | OpenAI-compatible base URL (env `TABLASSERT_AGENT_API_BASE`) |
| `--api-key`, `-ak` | str | No | `None` | API key secret (env `TABLASSERT_AGENT_API_KEY`) |
| `--max-steps`, `-ms` | int | No | `20` | Max inner-agent steps per article |
| `--min-rows`, `-mr` | int | No | `50` | Minimum non-empty data rows for a table/worksheet to reach the agent; `0` disables the small-table guard |
| `--map-threshold`, `-mt` | float | No | `0.25` | Coverage an article must reach to be MAPPED |
| `--max-improve-iters`, `-mi` | int | No | `3` | Max deterministic improve iterations per article |
| `--state-dir`, `-sd` | Path | No | `.tablassert/agent` | Checkpoint/resume workspace directory |
| `--backend`, `-b` | {openai, litellm} | No | `openai` | Model backend |
| `--reflexion` | bool | No | `False` | Enable the tier-2 LLM reflexion improver (same model config) when the deterministic proposer stalls |
| `--judge-model` | str | No | `None` | Model id for the semantic judge gate; MAPPED then also requires the score to clear `--judge-threshold` |
| `--judge-threshold` | float | No | `None` | Semantic judge normalized-score threshold for MAPPED (`0.5` when unset) |
| `--biolink-threshold` | float | No | `0.0` | Minimum Biolink pass rate of the built KGX for MAPPED; `0.0` reports the rate without gating |
| `--local`, `-l` | list[str] | No | `None` | Local payload: one DIR for all ids, or `PMCid=DIR` mappings; skips the PMC-AWS fetch (exit 2 on a missing DIR) |
| `--optimize`, `-o` | bool | No | `False` | Run GEPA prompt optimization and persist optimized instructions instead of running the supervisor |
| `--distill`, `-d`, `-dt` | bool | No | `False` | Record every LLM call (agent, judge, reflexion) as ChatML NDJSON under `<state-dir>/distill/records.ndjson` for fine-tuning; not supported with `--optimize` |
| `--instructions-file` | Path | No | `None` | Load GEPA-optimized instructions from a prior `--optimize` run |
| `--instructions-out` | Path | No | `None` | Where `--optimize` writes optimized instructions (default `<state-dir>/optimized_instructions.yaml`) |
| `--max-metric-calls` | int | No | `8` | GEPA metric-call budget for `--optimize` |
| `--dataset` | Path | No | `None` | YAML/JSON list of `{table_summary, coverage_feedback}` examples for `--optimize` (an example may also carry `fullmap`, `workdir`, and `head` to score each proposed config with real coverage) |
| `--task-model` | str | No | `None` | Fast model id for GEPA's many program evaluations (cheap task LM + strong reflection LM); `--model-id` is the reflection LM. Defaults to the reflection LM |

```bash
tablassert agent PMC11708054 --configuration-file ./graph.yaml
# equivalent short form:
tablassert agent PMC11708054 -f ./graph.yaml
```

!!! warning "Secrets"
    Model config comes from the flags above **or** the `TABLASSERT_AGENT_*` environment variables
    (explicit flags win). Secrets are **never** hardcoded or defaulted: a missing value fails loud
    (exit 2) **before** any model is built.

---

## distill-export

Use this to convert a distillation dataset recorded with
[`agent --distill`](#agent) into an on-disk Hugging Face dataset (`save_to_disk`). Requires the
`[distill]` extra (`pip install "tablassert[distill]"`, pulls `datasets`). The raw NDJSON already
loads directly in Unsloth Studio and via `datasets.load_dataset("json", ...)` — this export is
only needed for `datasets`-native workflows.

```bash
tablassert distill-export --distill-dir .tablassert/agent/distill --out ./hf-dataset
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--distill-dir`, `-dd` | Path | Yes | n/a | Directory holding the recorded `*.ndjson` files (exit 2 when empty) |
| `--out`, `-o` | Path | Yes | n/a | Destination directory for the `save_to_disk` dataset |

---

## build-fullmap

Use this to obtain the embedded `fullmap.redb` entity-resolution database. Every database this
command installs is filtered by the built-in **top-100 experimental-taxon allowlist** — there is no
flag for it. By default it first tries to **download a prebuilt database** published for this
Tablassert version; `--force` skips that and builds from RENCI BABEL exports instead (download class
+ synonym files, then build a single redb).

```bash
tablassert build-fullmap [ARGS]
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--output`, `-o` | Path | No | `./fullmap/data/fullmap.redb` | Path to write the redb file (prebuilt extraction or build output) |
| `--cache`, `-c` | Path | No | `./fullmap/downloads` | Directory for downloaded BABEL files when building from scratch (`classes/`, `synonyms/`) |
| `--version`, `-v` | str | No | `2026jul22` | BABEL snapshot date to fetch (a RENCI stamp, **not** Tablassert's version) |
| `--aria2c`, `-a` | Flag | No | `False` | Opt into the bundled `aria2c` binary from the `[aria2]` extra for resumable segmented downloads (the prebuilt archive **or** BABEL files); fails loud (exit 2, before any download starts) if the extra is missing or unsupported on the current platform, and on a non-zero aria2c exit |
| `--force`, `-f` | Flag | No | `False` | Skip the prebuilt download and always rebuild from BABEL outputs |

```bash
# Default: download the prebuilt fullmap.tar.zst for this version and extract it (fast)
tablassert build-fullmap --output /data/fullmap/fullmap.redb
# Force a from-scratch rebuild from BABEL outputs (e.g. after a BABEL snapshot bump)
tablassert build-fullmap --force --output /data/fullmap/fullmap.redb
# Accelerate either download with bundled aria2c (`pip install "tablassert[aria2]"`; the multi-GB prebuilt is the ideal aria2 use case)
tablassert build-fullmap --aria2c --output /data/fullmap/fullmap.redb
```

Every one of those invocations produces a database filtered by the checked-in
`src/tablassert/data/experimental_taxa.yaml` allowlist and records that filter as a
`META.taxon_allowlist` identity.

By default `build-fullmap` looks for a prebuilt `fullmap.tar.zst` at
`https://stars.renci.org/var/babel_outputs/<babel-version>/fullmap/<tablassert-version>/` (the version
directory is the **installed Tablassert package version**, never hardcoded), verifies it against the
published `sha256sum.txt`, and extracts it beside `--output` in the Rust extension, streaming zstd →
tar with the GIL released (the decompressed tar never touches disk), then validating the extracted
primary + shards against the force-build contract (exact `v5` schema, a recorded `build_id`, the exact
shard set, per-shard `build_id` equality, and a `META.taxon_allowlist` identity matching the built-in
allowlist) before atomically renaming them into place. If no prebuilt exists for this version (or the
download, extraction, or identity check fails), it falls back to a from-scratch BABEL build and logs a
warning — an archive published without the current filter is never installed.

A database already present at `--output` is reused only when it carries that same allowlist identity;
an unfiltered (or differently filtered) leftover from an older Tablassert is rebuilt, with a warning.
Pass `--force` to rebuild unconditionally.

A from-scratch build parallelizes automatically across all available CPU threads — on Linux the
worker count is capped by available memory (~2 GB per thread, read from `/proc/meminfo`) to avoid
OOMs. There is no flag to tune.

See [Fullmap](fullmap.md) for the data pipeline, output schema, and graph-config usage.

---

## build-kg

Use this to build a KGX NDJSON knowledge graph (nodes, edges, and a Resource Ingest Guide) from a
YAML configuration file.

```bash
tablassert build-kg GRAPH-CONFIGURATION-FILE [ARGS]
```

The positional `GRAPH-CONFIGURATION-FILE` (also `--configuration-file`, `-f`) is a **graph** YAML.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `GRAPH-CONFIGURATION-FILE` (`--configuration-file`, `-f`) | Path | Yes | n/a | Graph YAML |
| `--release`, `-r` | Flag | No | `False` | Emit a slim, significant-only graph (drops `not_significant` edges before resolution) |
| `--qc`, `-q` | Flag | No | `False` | Audit resolved mappings (exact → fuzzy → abbreviation → SapBERT) so low-confidence edges are flagged; requires the `[qc]` extra, checked before the build starts. Also runs a final study stage that asserts over the emitted NDJSON: no duplicate node ids, every node has a non-empty `id` and `name`, every edge has a non-empty `subject`, `predicate`, and `object`, no undeclared or isolated nodes, no malformed lines, no null or empty values in any field (checked recursively), no stray whitespace (verbatim `original_*` fields excepted from the whitespace check, since they are faithful source copies), and -- when any section declares a `category_override` -- no edge demoted to bare `biolink:Association` (a row escaped its pin and shipped without the class-specific slots `prune_to_class` nulled into `has_supporting_studies`; the assertion is graph-wide, so one pinned section also gates edges from unpinned sections) and fails the build (non-zero exit) on any violation |
| `--log`, `-l` | Flag | No | `False` | Enable verbose per-section logging |
| `--head`, `-hd` | Flag | No | `False` | Fast output-shape preview: ≤5 random rows/section, cached to `.head.parquet`, never clobbers a full build |

```bash
tablassert build-kg graph.yaml --qc --log
```

The parallel fullmap reads behind entity resolution are automatic: large lookup batches (≥ 1024
terms) fan out across the record-shard files on all available CPU threads (redb readers share-lock,
so they never contend), while smaller batches stay serial. There is no flag to tune, and results are
identical at any worker count.

Output is written to `rig.artifact_base_path` (created when missing) as `{name}_{version}.nodes.ndjson`,
`{name}_{version}.edges.ndjson`, and `{name}_{version}.RIG.yaml`; intermediate parquet lands in
`.tablassert/store/`. Build-time store keys incorporate the source-file content, so editing a source
file rebuilds exactly the sections that read the changed content. The first build after this key-format
upgrade rebuilds every section once because the keys have changed; orphaned old parquet files are not
automatically deleted. The RIG document is audited in memory before it is written: an invalid or
incomplete RIG fails the build with `[rig-validation-failed]` and nothing is emitted. See
[Graph Configuration](configuration/graph.md).

??? info "Build progress & stages"
    The build runs six parallel stages (Loading Tables → Extracting Sections → Building TCode →
    Collecting Instructions → Building Subgraphs → Compiling Graph) plus a seventh, Studying Graph,
    only when `--qc` is passed. They run under a three-row live progress block (stage header;
    section bar with count/elapsed/ETA; in-flight item detail). Each completed
    stage prints a green `✓ Stage N · NAME · elapsed` line above the live block. During Building
    Subgraphs the detail line also shows the per-section phase (`load`, `filter`, `clean`, `encode`,
    `resolve`, `qc`, `edge`, `provenance`, `significance`, `finalize`, `write`).

---

## validate

Use this to validate a configuration against a schema without running the build, ideal for CI and
pre-commit hooks. The required `--schema` flag selects which schema to validate against (the kind is
no longer sniffed from the YAML).

```bash
tablassert validate CONFIGURATION-FILE --schema graph
tablassert validate -f CONFIGURATION-FILE --schema table
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `CONFIGURATION-FILE` (`--configuration-file`, `-f`) | Path | Yes | n/a | Configuration file to validate |
| `--schema`, `-s` | `graph` \| `table` | Yes | n/a | Schema to validate against: `graph` validates the `Graph` model **and** every referenced table; `table` validates section syntax only |

Exits non-zero on any schema error. See [Table Configuration](configuration/table.md) and
[Graph Configuration](configuration/graph.md).

```bash
tablassert validate table-config.yaml --schema table
tablassert validate graph.yaml --schema graph
```

---

## validate-kgx

Use this to check that a completed build is actually Biolink-compliant. Where
[`validate`](#validate) checks your *configuration*, `validate-kgx` checks the *output*: every node
and edge is constructed as the Biolink Pydantic class named by its own `category`, the same classes
[`NCATSTranslator/translator-ingests`](https://github.com/NCATSTranslator/translator-ingests) builds
when it ingests your files.

```bash
tablassert validate-kgx --nodes MY_KG_1.0.0.nodes.ndjson --edges MY_KG_1.0.0.edges.ndjson
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--nodes`, `-n` | Path | Yes | n/a | Built `*.nodes.ndjson` file to validate |
| `--edges`, `-e` | Path | Yes | n/a | Built `*.edges.ndjson` file to validate |
| `--limit` | int | No | `20` | Maximum example failures to retain per file |

Failures are grouped by field and error type, so a systematic modelling problem shows up as one line
rather than a million:

```text
biolink-model 4.4.4
nodes: 424141/424141 valid (0 failures)
edges: 2000085/2000085 valid (0 failures)
KGX output is Biolink-compliant.
```

Exits non-zero when any record fails, so it can gate a release in CI. A missing or misspelled path is
reported as `file not found` and also exits non-zero: a file that was never read must never count as
a pass.

Some edges carry fields the installed model declares on no association: the KGX denormalized
carryovers (`synonym`, `xref`, `relation`, `provided_by`, ...). Tablassert emits them on purpose,
so they are counted separately as *pending* rather than treated as defects. The set is derived
from the installed package, so a field leaves it the moment a `biolink-model` release declares
it, as `effect_size` / `effect_type` did when 4.4.4 shipped
[#1774](https://github.com/biolink/biolink-model/pull/1774):

```text
edges: 1200000/2000085 valid (800085 failures; 800085 pending biolink-model support)
```

The strict count is what `ok` and the exit code use; the pending count is what
[`tablassert agent`](#agent) optimizes against, so a deliberate gap never reads as a modelling error.

Retrieval-source entries are the mirror case. Tablassert emits `resource_id` as each `sources` entry's
sole identifier, while the pinned model still requires the inherited `Entity.id` on `RetrievalSource`.
The validator supplies that `id` to its own in-memory copy of the record whenever the installed model
requires it, so compliant output validates without the duplicate identifier ever being written to disk.
The decoded record and the built NDJSON are untouched, and the alias stops being applied on its own
once a model release drops the requirement.

---

## Typical workflow

1. Author a table config, then a graph config that references it.
2. `tablassert validate graph.yaml --schema graph`: fail fast on schema errors.
3. `tablassert build-kg graph.yaml`: produce KGX NDJSON + RIG (add `--qc` to audit mappings).
4. `tablassert validate-kgx -n MY_KG_1.0.0.nodes.ndjson -e MY_KG_1.0.0.edges.ndjson`: confirm the
   output validates against the Biolink Model before shipping it downstream.

## Next Steps

- **[Tutorial](tutorial.md)**: complete example walkthrough
- **[Configuration Guide](configuration/graph.md)**: YAML configuration reference
- **[Fullmap](fullmap.md)**: entity-resolution database build and schema
- **[Agent](agent.md)**: autonomous PMC → KG pipeline depth
