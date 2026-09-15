# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Breaking Changes
- **Level-one normalization now defines fullmap keys by cleaned Unicode-lowercase, whitespace-tokenized, Porter2-normalized terms, with duplicate removal and byte-wise token ordering.** Existing fullmap databases are schema-v5 and are rejected by the current resolver; rebuild fullmaps before use. Preferred-name ranking now compares normalized forms under the same level-one semantics.

### Changed
- **BREAKING: `build-fullmap` always applies the built-in top-100 experimental-taxon allowlist, and `--taxon-allowlist` is removed.** The checked-in `src/tablassert/data/experimental_taxa.yaml` filter is no longer opt-in: every source build passes those 100 NCBI taxon IDs to Rust, and the resulting database records the same deterministic `META.taxon_allowlist` identity introduced in 18.1.0. The identity now also guards both reuse paths, so an unfiltered database can never satisfy an allowlisted `build-fullmap` invocation. A prebuilt archive whose recorded identity is absent or different fails extraction validation before anything is renamed, surfacing as the existing `PrebuiltFullmapUnavailable` fallback to a filtered BABEL build; an existing file at `--output` is reused only when its identity matches, and is otherwise rebuilt with a warning. Scripts passing `--taxon-allowlist` or `--no-taxon-allowlist` must drop the flag — parsing now fails with an unknown-option error. `build_fullmap_pipeline`, `fetch_prebuilt_fullmap`, and `_extract_prebuilt_fullmap` accept the IDs through their existing/extended `taxon_allowlist` parameter, and the Rust extension exposes `taxon_allowlist_identity` plus `fullmap_taxon_allowlist_identity` so Python can compare identities without duplicating the encoding.
- **Logging now requires the optional `log` extra and is completely disabled without it.** Removed the stdlib-backed loguru fallback, startup missing-extra warning, and base-install creation of `.tablassert/log/`. Installs without loguru emit no file, progress, or warning logs; `pip install "tablassert[log]"` remains the way to enable loguru-backed logging.

## 18.1.0 - 2026-09-14

### Added
- **`build-fullmap --taxon-allowlist` builds an opt-in, source-derived fullmap restricted to the top 100 experimental taxa, and InChIKey terms now survive ingestion.** The flag reads the checked-in `src/tablassert/data/experimental_taxa.yaml` allowlist (100 NCBI taxon IDs ranked by observed frequency across the `MultiomicsNext` and `MultiomicsHarness` corpora) and filters non-`OrganismTaxon` synonym rows carrying valid taxon metadata in `rust/src/fullmap.rs` before interning, yielding a smaller database while retaining taxonless rows (including `NCBITaxon:0` and malformed taxon values), rows with any allowlisted taxon, and every row whose categories include `OrganismTaxon`; the first taxon is still the one stored, so hydration and lookup behavior are unchanged. Filtered builds always come from BABEL sources, never reuse the unfiltered prebuilt archive, and record a deterministic `META.taxon_allowlist` identity so a filtered database cannot be mistaken for a full one. Separately, Rust token QC no longer rejects `inchikey` -- newline, tab, hypothetical, uncharacterized, and dead-token filtering are all preserved -- so InChIKey terms are indexed and resolvable in both raw normalized (`abc-def`) and CURIE (`inchikey:abc-def`) form. Default fullmap builds, the prebuilt download path, and existing Python callers are untouched. ([#149](https://github.com/SkyeAv/Tablassert/pull/149))

### Fixed
- **The `Categories` config vocabulary now names fullmap-emitted Biolink mixins via `CATEGORY_OVERRIDES`.** Biolink declares some node categories as mixins, and the pydantic model generates a mixin as a plain `ConfiguredBaseModel` that is not an `Entity` subclass -- invisible to the scan that builds `Categories`. The fullmap's BABEL vocabulary assigns one such mixin (`GenomicEntity`) to real terms, so an `avoid` allow-list could never name it: the category sailed through the guard and the edge fell back to bare `biolink:Association`, whose `prune_to_class` then nulled every class-specific slot into the inlined `has_supporting_studies` junk drawer (DAKP shipped 35 such edges). `GenomicEntity` is now a first-class member -- valid in `avoid`/`prioritize` and as a `category_override` key -- and a tripwire test drops the override the moment the installed model makes the mixin an `Entity` subclass (tests in `tests/test_biolink.py` and `tests/test_models.py`). ([#151](https://github.com/SkyeAv/Tablassert/pull/151))
- **`filter_and_rank` drops categories the `Categories` enum cannot name when `avoid` is set.** An `avoid` list is an allow-list by complement, and a complement can only name what the enum knows: a fullmap `CATEGORY_NAME` outside the enum could never appear in it, so without this guard the hard filter had a silent hole for any future vocabulary drift. Degradation is now to lost rows (surfaced in the resolution report) rather than mis-classed edges (regression tests in `tests/test_fullmap.py`). ([#151](https://github.com/SkyeAv/Tablassert/pull/151))
- **The `--qc` study stage now fails the build on edges demoted to bare `biolink:Association` when any section declares a `category_override`.** A pinned section promises every row lands on a pinned class, so a demotion means a row escaped the pin -- typically an object category the config vocabulary could not name -- and shipped without its class-specific evidence slots. Without a pin, bare `Association` remains the author's accepted default, so the assertion is gated. `study_kgx` gained the `category_override_declared` keyword; `study_final_ndjson` forwards it from the built sections (regression tests in `tests/test_study.py`). ([#151](https://github.com/SkyeAv/Tablassert/pull/151))

## 18.0.0 - 2026-09-10

### Changed
- **Original source values are always retained and collision-merged deterministically.** `original_*` edge fields now combine distinct values using sorted `A`, `A|B`, or `A|B|C` formatting; the deprecated `build-kg --no-original` option and API parameters were removed.

### Documentation
- **`README.md` no longer presents a scalar `source.url` where the live schema requires a list, and its documentation index now includes the shipped Fullmap, Development, and Changelog pages.** The quick-start configuration uses the valid list form, and the navigation links now cover the corresponding site surfaces.
- **`docs/installation.md` no longer omits the `distill` extra or blur the distinction between recording and exporting distillation data.** The extras table and preflight guidance now document `distill`, while `agent --distill` is identified as zero-additional-dependency recording and `distill-export` as the step requiring the extra.
- **`docs/configuration/table.md` no longer omits the `exclude_prefixes` and `exclude_regex` resolution filters.** Their exact, case-sensitive matching, regex constraints, and unresolved-row behavior are now documented for subject, object, and qualifiers.
- **`docs/api/fullmap.md` no longer omits the `exclude_prefixes` and `exclude_regex` parameters or their position in resolution.** The signatures, filtering semantics, and examples now match the live `resolve()` API.
- **`docs/api/qc.md` no longer omits `fullmap_audit()`'s phase callback.** The `on_phase` signature, emitted phase labels, and build-progress use are now documented.
- **`docs/api/lib.md` no longer implies that `resolve_many()` exposes the full resolution context.** Its comparison now names `exclude_prefixes` and `exclude_regex` among the parameters unavailable to the convenience API.
- **`docs/api/utils.md` no longer calls the whole log directory a loguru sink.** It now distinguishes `log.LOGASSERT` and `.tablassert/log/` from the loguru sink file stored within that directory.
- **`llms.txt` no longer carries stale extras, CLI, graph-field, module, workflow, or documentation indexes.** Its entries now enumerate the live commands and optional extras, current implementation surface, MkDocs pages, and the root `CHANGELOG.md` pointer.
- **`docs/agent.md` no longer describes lower-bound dependencies as exact pins or omits the latest agent flags.** The dependency tables now quote the live requirements, and the compact reminder includes `--distill` and `--task-model` while keeping `docs/cli.md#agent` authoritative.
- **`CONTRIBUTING.md` no longer describes stale test counts, CI sharding, hook-install behavior, or an incomplete project layout.** Its quality gates, pre-commit instructions, CI dependency set, and layout now match the live configuration and workflows.
- **`docs/index.md` no longer omits shipped navigation surfaces.** The landing-page list now includes Fullmap, Agent, Development, and Changelog alongside the existing sections.
- **`tests/test_docs_source_of_truth.py` now guards the corrected documentation against drift.** It derives checks from the live schemas, CLI, optional extras, source modules, workflows, MkDocs navigation, and contributor tooling instead of allowing the same mismatches to return silently.
- **The documentation pass deliberately leaves two follow-ups rather than claiming them fixed.** The dead `errors.DOCS_URL` target and the still-undocumented coded error messages require separate remediation.
## 17.0.1 - 2026-09-09

### Fixed
- **The fullmap resolver now retains every CURIE tied on a term's best ranking tier instead of keeping one arbitrary winner.** `distinct` sorted by `term` / `PR` / `NLP_LEVEL` (plus the `column_context` category-`FREQUENCY` tiebreaker when set) and kept only the first row per term, so distinct CURIEs tied across the entire ranking tuple were silently discarded before `resolve` ever saw them — a term matching `HGNC:1` and `HGNC:2` equally well shipped only whichever the sort happened to surface. The op now computes each term's best ranking tier, retains every row in it, and collapses duplicates only by `(term, CURIE)`; tied CURIEs propagate through `resolve`, `resolve_batch`, and `join_matches`, so a source row may expand to one output row per tied best CURIE (level-one still beats level-two, and `column_context` still prefers the more frequent category). Regression tests pin the tier retention, duplicate `(term, CURIE)` collapse, and lower-tier exclusion (`test_filter_and_rank_collapses_duplicate_curies_and_keeps_equal_best_curies`), plus end-to-end row expansion through `resolve` and `join_matches`. ([#145](https://github.com/SkyeAv/Tablassert/pull/145))

## 17.0.0 - 2026-09-08

### Breaking Changes
- **The `--threads` / `-t` CLI options (on `build-kg` and `build-fullmap`), the `threads` parameter on the Python API (`fullmap.resolve`, etc.), and the agent command's `--gepa-threads` option (`dspy.GEPA` evaluation pool) are gone.** All parallelism is now automatic: every parallel stage uses all available CPU threads, with the fullmap build still capped on Linux by available memory (~2 GB per thread, read from `/proc/meminfo`) to avoid OOMs, and entity-resolution lookups fanning out across the fullmap shards automatically only for batches of 1024+ terms (smaller batches stay serial). GEPA now uses its library default parallelism. Results are unchanged. ([#142](https://github.com/SkyeAv/Tablassert/pull/142))

  **Migration:** drop the `--threads` / `-t` and `--gepa-threads` flags from invocations and the `threads` / `num_threads` arguments from Python and Rust calls — they were optional, and the automatic behavior is exactly the previous default, so builds, lookups, and results need no other change; GEPA evaluation now runs at the library's default parallelism.

### Performance
- **The `uuid_on_collision: merge` edge pass is now near-linear in divergent rows per edge instead of quadratic, and its canonical serializer no longer clones per record.** The merge fold re-canonicalized and re-sorted every list item already stored on an edge on each incoming row — O(n²) in rows × `supporting_case_ids` — and `canonical_json_bytes` deep-cloned into a sorted-key `Value` tree before writing, a fixed cost paid once per record on every pass. Array fields now carry their items' canonical bytes, computed once when an item arrives, so each fold is O(1) per incoming item (hash-set membership, one deferred stable sort at write-out, and only for lists a real union touched), while the sorted-key form is written directly into the output buffer with scalars delegated to `serde_json`'s own writers. Measured on release builds (`uv run maturin develop --release`) over the seeded corpora (seed 42) of the committed harness — `TABLASSERT_BENCH=1 uv run pytest tests/bench_merge_bench.py -s -n 0 -q` — on an AMD Ryzen AI 9 HX 370 with Python 3.13: **37.45 s → 1.311 s (28.6x)** for 100,000 lines of 1k triples × 100 divergent rows at 50 case ids per row, **20.69 s → 2.155 s (9.6x)** for 100,000 lines of 5k triples × 20 rows at 100 case ids per row, **6.71 s → 5.184 s (1.29x)** for 600,000 lines of 200k triples × 3 rows at 10–20 case ids per row, and **0.59 s → 0.495 s (1.19x)** for 100,000 byte-identical rows, so the no-fold path does not pay for the new bookkeeping either. Merged-record and conflict counters are unchanged on every scenario (400,000 / 1,199,899; 95,000 / 284,979; 99,000 / 296,969), and the emitted bytes are identical to the previous fold — pinned by a seeded fuzz against a frozen verbatim copy of the old implementation (`cargo test merge_fold_matches_reference_on_fuzz`), an end-to-end pure-Python reference (`tests/test_rs.py::test_dedup_edges_merge_matches_python_reference`), and an in-process guard that fails below a 5x bound against that frozen quadratic reference (`cargo test fold_speedup`). The buffered merge state does carry per-list-item bookkeeping memory — one canonical-bytes copy per distinct list item, a single allocation shared between the membership set and the ordered write-out list — which is the price of the O(1) membership check and one more reason the mode stays opt-in: sharing that allocation rather than storing two copies drops peak RSS on the harness's 1k triples × 100 rows corpus (~5M stored list items) from 1.93 GB to 1.26 GB. ([#143](https://github.com/SkyeAv/Tablassert/pull/143))

## 16.7.0 - 2026-09-04

### Changed
- **The release-mode minimum `number_of_cases` threshold for `applied_to_treat` edges is now 10 instead of 25.** `drop_low_number_of_cases` keeps edges whose case count is >= 10 (nulls kept, as before); release builds now ship `applied_to_treat` edges backed by 10-24 cases that earlier releases dropped. Gating (`--release` *and* `applied_to_treat`), null handling, non-numeric tolerance, and `significance`-phase placement are unchanged. ([#141](https://github.com/SkyeAv/Tablassert/pull/141))

## 16.6.2 - 2026-09-04

### Fixed
- **`number_of_cases` is now emitted as a JSON integer instead of source text.** The slot is typed `int` on biolink-model's `EntityToDiseaseAssociation` / `EntityToPhenotypicFeatureAssociation`, but it was absent from `lib.numeric_columns`' exact set — since 15.1's `STUDY_SIZE_EXEMPT_PATTERN` (#119) stopped it being *renamed* onto `study_size`, nothing cast its *values*, so the raw TSV cell (`"1"`) shipped on the edge NDJSON. Pydantic's lax validation coerced the string back to int inside `validate_kgx`, so the record still validated, and the Rust `uuid_on_collision: merge` recompute already wrote a real int — leaving the shipped graph type-inconsistent edge to edge. ([#139](https://github.com/SkyeAv/Tablassert/pull/139)) `number_of_cases` now rides the same `clean_numeric` / `format_numeric` machinery as `study_size`: `numeric_slot_kind` reads the `int` typing off the installed model, fractional / negative / non-numeric counts become null, and the release-mode `drop_low_number_of_cases` filter is untouched (it already cast inline).
- **`--release` builds no longer reuse section parquets cached by other build modes.** The section-parquet cache key was `STORE / f"{mkhash(s)}.parquet"` with `release` injected into `Tcode` outside the hashed dict, and `Tcode.collect` quick-exits on any existing store file — so a parquet cached by a non-release build (or a pre-16.6.0 build, where the case-count filter silently no-op'd) was reused verbatim in `--release` builds, skipping the release filters and shipping `applied_to_treat` edges with `number_of_cases < 25`. A new `_section_store_path` helper generalizes the existing `.head.parquet` idiom: `--head`, `--release`, and `--qc` each get their own suffix, composing when combined (`<h>.head.release.qc.parquet`). `--qc` belongs because `fullmap_audit` drops rejects from the cached parquet too — the same cache-poisoning class. ([#140](https://github.com/SkyeAv/Tablassert/pull/140))
- **The release-mode case-count and effect-size filters tolerate non-numeric cells instead of crashing.** `drop_low_number_of_cases` and `drop_zero_effect_size` cast with strict `Float64`, but the `csv` op reads sources with `has_header=False`, so a TSV's header row flows through as data and the strict cast raised `InvalidOperationError` on the non-numeric header cell. Both ops now cast with `strict=False` (the `clean_numeric` idiom); non-numeric cells become null and are kept, dying later at entity resolution as before. ([#140](https://github.com/SkyeAv/Tablassert/pull/140))

## 16.6.1 - 2026-09-04

### Performance
- **The agent's LLM workflow now carries a far smaller tool surface, tighter context, and smaller persisted configs.** Full mode registers exactly four tools — `read_table`, `pmc_article_context`, `derive_config`, `build_and_audit` — because coverage improvement left the LLM's hands entirely: `map_coverage` and `propose_config_edit` are pure helpers the deterministic supervisor calls in its own improve loop, and the opt-in `--reflexion` path is the only place an LLM call is still spent on improvement. The task text now injects a bounded per-column `column_digest` for every previewed table/worksheet (separator fractions over the first 500 data rows, plus non-null/distinct counts, max cell length, and sample values), so the canonical path is a fixed derive → build → answer of three or fewer steps with zero inspection tool calls, and the agent rebuilds only on a coded build error — fixing exactly the field the error names, at most twice. The LLM sees a compact `build_and_audit` observation (twelve high-signal keys, with `unresolved` capped at 20 entries and a visible `+N more` marker) while the supervisor still receives the full report, and the accepted best config is **compacted deterministically** before persistence: `compact_config` strips keys equal to their Pydantic model defaults while preserving semantics — the pinned accuracy-invariance test proves the compacted config builds the identical KGX and scores the identical `quality_score`, and any compaction failure falls back to the exact original config. `state.json` records `config_chars` so the shrink is auditable per article. Builds, scores, and persisted configs remain behaviorally equivalent. ([#138](https://github.com/SkyeAv/Tablassert/pull/138))

## 16.6.0 - 2026-09-04

### Added
- **The agent can now record every LLM call as a ChatML fine-tuning dataset.** `tablassert agent --distill` (`-d` / `-dt`) appends one ChatML record per line to `<state-dir>/distill/records.ndjson` for every LLM call of the run — inner agent, judge, and reflexion alike. The file is append-only across invocations, so a corpus accumulates run over run and loads directly in Unsloth Studio or via `datasets.load_dataset("json")`, and every record is tagged with its `pmc_id` so `state.json` filtering can slice per article. Recording lives in the zero-dependency `tablassert.distill` (`DistillRecorder` plus ChatML serializers) and never raises into a batch; `make_distilling_model` wraps the model's `generate()` — the single capture seam — and `tablassert distill-export` (new `[distill]` extra) converts the NDJSON to an on-disk Hugging Face dataset. ([#136](https://github.com/SkyeAv/Tablassert/pull/136))
- **`uuid_on_collision: merge` now recomputes `number_of_cases` as the exact union of a new build-internal `supporting_case_ids` edge field.** Folding two divergent same-id edges used to keep the first record's `number_of_cases` (under-reporting the merged evidence) and count the divergence as a scalar conflict; summing the two counts would instead double-count every shared case. Edge frames may now carry a `supporting_case_ids` `list[str]` column — allow-listed via `TABLASERT_EDGE_EXTRAS` so it is never folded into `supporting_text`, and declared by no association class so `prune_to_class` leaves it untouched without a `CLASS_FIELD_OVERRIDES` grant. When a merged record carries the list and either side carried a count, the Rust merge sets `number_of_cases` to the union length (shared case IDs count once) and reports no scalar conflict; merges with no carrier keep first-wins exactly. The carrier is stripped from every edge record before write in both the merge and default streaming dedup paths, so it never ships in the final NDJSON and QC/`validate_kgx` never see it.

### Fixed
- **The release-mode case-count filter for `applied_to_treat` edges now reads the column the pipeline actually emits.** The filter shipped in 16.1.0 read a `case_count` column that never exists by the `significance` phase: a literal `case_count` source header is coerced onto `study_size` (unit `case` + count noun `count`, alias dropped), and the case counts that do survive travel under the exempt Biolink slot `number_of_cases` — so the filter was a silent no-op in every release build since, and small-cohort treatment edges still shipped. The op is renamed `drop_low_number_of_cases` and now reads `number_of_cases`; gating (`--release` *and* `applied_to_treat`), the 25-case threshold, null handling (nulls kept), and `significance`-phase placement are unchanged — but release builds now actually drop `applied_to_treat` edges backed by fewer than 25 cases, as documented since 16.1.0. ([#137](https://github.com/SkyeAv/Tablassert/pull/137))

## 16.5.0 - 2026-09-03

### Added
- **The agent now authors detail-first configs, backed by actionable audit feedback and deterministic `explode_by`/predicate fixes.** Generated configs used to ship shallow — generic `associated_with` predicates, missed `explode_by` on delimited cells, absent qualifiers, fragile regexes — so goal ordering is now breadth → coverage → Biolink/QC → efficiency and `INSTRUCTIONS` gained a regex cookbook (Rust-regex semantics, YAML single-quoting), positive qualifier guidance, and a most-specific-legal-predicate rule, with exemplars moved off generic `associated_with` (including a new rich exemplar combining `explode_by`, a qualifier, a regex, and a statistical pair) and the template converted to a raw string, fixing a real `\1` octal-escape corruption. `build_and_audit` now returns actionable signals — `predicate_advice` (legal predicates per demoted category pair), `multivalued_suspects` (missed-`explode_by` detection), and a `head` fidelity flag — that the deterministic proposer consumes to add `explode_by` from separator-carrying unresolved terms and fix demoted predicates, also correcting a pre-existing misfire where joined cells like `brca1;tp53` were misread as taxonomic. Tier-2 LLM reflexion can now touch `explode_by`/`split_by`/qualifiers/predicate, while `_is_improvement` rejects any edit that shrinks the full-build edge count by more than 25%; `docs/agent.md` documents the new behavior. ([#135](https://github.com/SkyeAv/Tablassert/pull/135))

### Fixed
- **Underscore-glued adjusted P value headers no longer ship as raw `p_value`.** `CONTEXTUAL_ADJUSTED_PATTERN` anchored on `\b`, but `_` is a word character, so the boundary never fired between `adjusted` and `_p_value` — the exact hazard `BARE_P_TOKEN_PATTERN` already documents and works around with alphanumeric lookarounds, a workaround that was never mirrored onto the adjustment pattern. Every underscore-glued spelling (`adjusted_p_value`, `adj_p_value`, `corrected_p_value`, `p_adjusted`) classified as a **raw** p-value, so a column literally named `adjusted_p_value` was renamed to `p_value` and `sig` banded an FDR-adjusted number as a raw one. The spaced and hyphenated spellings were always correct, which is why it went unnoticed: `test_sig_prefers_exact_p_value_column` supplies both columns, so canonical-wins masked the misclassification. The pattern now uses the same lookarounds as the bare-P token, and the adjustment vocabulary gained `adjust` / `adjustment` so the real DESeq2-family header `p_adjust` is recognized too. Adjusted values in existing graphs were being emitted in the raw `p_value` slot; rebuild to correct them. ([#134](https://github.com/SkyeAv/Tablassert/pull/134))

### Changed
- **Every coercion now drops its losing alias columns, not just the study-size and study-metadata ones.** The five coercions disagreed: `study_size` and `study_metadata` dropped their losers, while `p_value`, `effect_size` and `effect_type` left theirs on the frame for `fold_unknown_to_supporting_text` to pick up. A table with `effect size` and `odds ratio` therefore emitted `effect_size: 0.85` on the edge **and** `"odds ratio: 0.85"` inside `supporting_text` — one measurement twice, which is the duplication `coerce_study_size_columns` had always dropped aliases to prevent. The `-log10` case was worse than duplication: an un-logged `p_value` of 1e-8 rode beside a `"negative log10 p value: 8.0"` entry that reads as a contradiction. The policy is now uniform and lives as one `drop_aliases` field on each rule rather than in five divergent code paths. ([#134](https://github.com/SkyeAv/Tablassert/pull/134))

  **Migration:** if a table's headers are genuinely *different* statistics that all recognize onto one canonical slot — `beta` beside `odds ratio`, or a two-analysis header set like `p_value_analysis1` / `p_value_analysis2` — only the winner now survives; the others used to reach `supporting_text` and no longer do. Rename such columns to spellings the classifiers do not claim when every one of them must reach the edge. `docs/configuration/table.md` states the rule and this cost up front.

- **The five clean-phase coercion ops are one op, `coerce_columns`.** All five already carried the same `"clean"` phase label, so `compile_subgraph` emitted a single phase transition either way and the progress UI is unchanged. Fusing them means one schema resolution, one rename node and one `with_columns` batch per section, and lets the `effect_type` Biolink class rule read the renamed `effect_size` from the plan instead of re-resolving the frame's schema. The five per-slot functions remain exported and are thin slices of the same code path, so they stay the unit of testing and of documentation. ([#134](https://github.com/SkyeAv/Tablassert/pull/134))

- **Column coercion is a registry plus four pure helpers rather than five hand-written pipelines.** "An existing canonical column wins, otherwise the best `rapidfuzz` match" was copy-pasted four times, `sig` re-derived the p-value selection `coerce_pvalue_columns` had already computed, and `coerced_target` was a hand-maintained `or` chain whose docstring asked the reader to keep it in sync with the op order in `Tcode._source_ops`. Selection, planning, and application now exist once (`_select` / `_plan` / `_value_exprs` / `_apply`), each coercion is a `_Rule` record naming only what distinguishes it, and both `coerced_target` and `coerce_columns` read the same `_RULES` tuple — so the classification a config-time validator reports and the rename the build performs can no longer drift apart. A drift-guard test pins the classifier disjointness the single-pass planner depends on, and an equivalence test pins the fused op against the five applied in sequence. ([#134](https://github.com/SkyeAv/Tablassert/pull/134))

### Performance
- **`effect_type` value coercion is bounded by distinct values instead of by row count.** Each cell went through a `map_elements` Python UDF behind a process-global `lru_cache(maxsize=4096)`; past 4096 distinct values the cache thrashed and stopped helping entirely. The column is now resolved once over its distinct values and rewritten by a single native `replace_strict`. Measured on 200k rows: **56.8 ms → 9.7 ms** at 10 distinct values, **455.6 ms → 16.8 ms** at 5 000, and **1106.0 ms → 62.2 ms** at 20 000 — faster at every cardinality, with output verified identical over 4 037 probe values. The fuzzy fallback uses `rapidfuzz.process.extractOne` rather than `process.cdist`: `cdist` returns a numpy matrix, and numpy is not a Tablassert dependency, so it would raise inside the polars UDF. The global cache is gone with it — arbitrary source-cell values are not state this module should own. ([#134](https://github.com/SkyeAv/Tablassert/pull/134))
## 16.4.0 - 2026-09-03

### Added
- **The autonomous agent now excludes super-small tables and worksheets by default.** A new
  `MIN_TABLE_ROWS` threshold and `--min-rows`/`-mr` option require 50 non-empty data rows, excluding
  the header and all-null rows, before a CSV/TSV table or Excel worksheet reaches the agent. Counts
  use the production parsers and invalidate on file changes; unreadable inputs remain fail-open for
  the existing `read_table` fallback, while an article with no qualifying readable candidate is
  recorded as `SKIPPED` before an LLM is constructed. Set `--min-rows 0` to disable the guard; negative
  values fail before the agent starts. ([#131](https://github.com/SkyeAv/Tablassert/pull/131))
- **The README now shows Tablassert's PyPI monthly download count.** A Shields.io downloads badge
  links directly to the package's PyPI page beside the existing version, Python, CI, and license
  badges. ([#133](https://github.com/SkyeAv/Tablassert/pull/133))

### Performance
- **Math transformations now remain lazy native Polars expressions.** `math_op` handles the
  supported `pow` and `copysign` functions with Polars expressions instead of eagerly collecting a
  `LazyFrame` and applying per-element Python `math` calls through `map_elements`, preserving the
  numeric coercion while avoiding the materialization and Python callback overhead. ([#130](https://github.com/SkyeAv/Tablassert/pull/130))

## 16.3.1 - 2026-09-03

### Changed
- **PyPI metadata now reflects the current Tablassert feature set.** Keywords
  identify KGX, Biolink, biomedical and tabular data, quality control, and the
  optional LLM agent; classifiers identify the Python 3-only library, Pydantic
  2, and supported implementation targets. ([#132](https://github.com/SkyeAv/Tablassert/pull/132))

## 16.3.0 - 2026-09-03

### Added
- **`loguru` is now an optional `[log]` extra with a stdlib-backed fallback.** Removing it from core dependencies makes base installs lighter while preserving logging: `tablassert.log` keeps the same `log/tablassert.log` destination and brace-style category/sink API without loguru, and `pip install "tablassert[log]"` restores rotation and enqueue support; CI, setup instructions, and installation docs now install the extra when they need full logging. ([#129](https://github.com/SkyeAv/Tablassert/pull/129))

### Performance
- **PMC OA fetches no longer download redundant `.txt` main-text copies.** The agent reads JATS `.xml`/`.nxml` for article context, so `.txt` objects duplicated data and consumed bandwidth; `MAIN_TEXT_EXTENSIONS` now rejects them while local `.txt` payload rendering remains available. ([#127](https://github.com/SkyeAv/Tablassert/pull/127))

### Changed
- **`pdfminer.six` is no longer an `[agent]` dependency, and article PDFs are never downloaded.** The `pmc_article_context` `.pdf` branch was unreachable: the supervisor only ever selects `.xml`/`.nxml` main text, and every `pmc-oa-opendata` version ships JATS `.xml` (verified against the live bucket, including the scanned Historical OCR backfile), so a PDF-only main text cannot occur. `_extract_pdf_text` and the `.pdf` excerpt branch are gone; `.pdf` left `MAIN_TEXT_EXTENSIONS`, so fetches no longer download the article PDF (typically the largest file in a version payload). ([#128](https://github.com/SkyeAv/Tablassert/pull/128))

## 16.2.0 - 2026-09-03

### Added
- **`uuid_on_collision: merge` folds divergent same-id edges into one instead of aborting the build.** `uuid_fields` (16.0.0) narrows what feeds the derived edge id; when the declared set is the *resolved statement* — subject, predicate, object, qualifiers — two rows with different raw mention spellings that resolve to the same CURIE derive one id, and the build failed with `uuid-fields-not-a-key` even though the correct output is plainly a single edge with the combined evidence. `Graph` gained an optional `uuid_on_collision: Literal["error", "merge"]` (default `error`, behavior unchanged in every respect). Under `merge`, the Rust deduper buffers one full record per unique id, folds each divergent same-id record into the first — list fields (`publications`, `sources`, `source_record_urls`, `supporting_text`, `category`, `upstream_resource_ids`, `has_supporting_studies`, …) unioned, deduplicated by canonical content (object entries like `sources[]` that differ only in key order collapse), and **sorted**, so merged output is identical regardless of row order; scalar fields keep the first record's value, with each conflict counted into a build-log summary — and writes the merged edges in first-seen order at end-of-stream. That buffering is the memory trade-off the default streaming path avoids, which is why the mode is opt-in, and why it is rejected without `uuid_fields` (under the whole-record hash no two different records can share an id, so there would be nothing to merge) as `uuid-merge-without-fields`. ([#126](https://github.com/SkyeAv/Tablassert/pull/126))

## 16.1.1 - 2026-09-03

### Added
- **The README now exposes the repository's GitHub star count.** A Shields.io GitHub stars badge links beside the existing project badges, making repository support discoverable without changing package or documentation behavior. ([#125](https://github.com/SkyeAv/Tablassert/pull/125))

## 16.1.0 - 2026-09-03

### Added
- **Release mode drops `applied_to_treat` edges with a `number_of_cases` below 25.** The release pipeline already shed non-significant rows and zero effect sizes, but a treatment edge backed by a single-digit cohort still shipped beside them, because nothing checked how many cases stood behind the association. The new `drop_low_number_of_cases` filter closes that gap: it is wired into `Tcode` op construction gated on `--release` *and* `statement.predicate == "applied_to_treat"`, so other predicates never see it; null case counts are kept (no count was detected, not a small one); sections without a `number_of_cases` column are untouched; and like the other release filters it runs in the `significance` phase before `resolve_batch`, so dropped rows never pay for entity resolution. Non-release builds are unchanged. ([#123](https://github.com/SkyeAv/Tablassert/pull/123))


## 16.0.0 - 2026-08-25

### Breaking Changes
- **Edge ids are canonicalized before hashing, so every existing edge id changes once.** The derivation sorted only *top-level* keys: nested values reached the digest through `Value::to_string`, which under serde_json's `preserve_order` emits insertion order. Every edge carries a `sources` array, so a polars struct-field reordering — or a Biolink release that shifts a nested slot — silently re-minted ids across a whole graph while nothing about the assertion had changed. Canonicalization now recurses (object keys sorted at every depth; array order preserved, because it is semantic), which makes ids robust to that churn at the cost of one migration. Relatedly, `false` is now hashed as `"false"` instead of being dropped along with its key: `strip_nulls` deliberately keeps `false` (`negated: false` is a meaningful Biolink value), so dropping it made `{subject, negated: false}` and `{subject}` — two distinct records — derive the *same* id. No shipped graph is affected by that second fix (the 1.27M-edge MultiomicsKG 3.0.0 build contains zero `false` values); the canonicalization fix moves every id. ([#122](https://github.com/SkyeAv/Tablassert/pull/122))

  **Migration:** readers pinning Tablassert edge ids must re-key against the rebuilt graph. There is no mapping from old id to new — the old value was a hash of a serialization detail. This is a one-time event: adopt `uuid_fields` (below) in the same rebuild and ids stop moving for attribute changes thereafter.

### Added
- **`uuid_fields`: a graph config declares which edge fields constitute edge identity.** Edge ids were UUIDv3 hashes of the *entire* emitted record, so every field was an identity field — a corrected `p_value`, a bumped `subject_nlp_level` inside `supporting_text`, a changed `sources[].source_record_urls`, or a reordered source row (the `row:<N>` inside `has_supporting_studies`) each minted a brand-new id, and downstream Translator consumers saw a new edge rather than the same edge with updated attributes. `Graph` gained an optional `uuid_fields` list; when set, only those top-level edge keys feed the hash, and everything else is free to change. Entries canonicalize onto their allow-listed spelling exactly as annotations do, and a list that cannot be a key — empty, repeating, containing `id`, or naming a field no edge emits — is rejected at config time as `uuid-bad-fields`. Leaving `uuid_fields` unset preserves the whole-record hash, so the feature is strictly opt-in. Measured on MultiomicsKG 3.0.0 (1,265,355 edges): re-analysing `p_value` and `effect_size` on every row moved **930,081 ids (73%)** under the whole-record hash and **none at all** under a declared `uuid_fields`. ([#122](https://github.com/SkyeAv/Tablassert/pull/122))

- **The UUID namespace derives from the graph's infores when `uuid_fields` is declared.** Full-record hashing gave cross-graph uniqueness by accident: two graphs asserting the same triple from the same publication were separated by their differing `sources` and `supporting_text`. A narrow field set removes that accident, so the domain moves from the `TABLASSERT` constant onto `rig.source_info.infores_id`, making the separation structural instead. A new optional `uuid_domain` overrides it for the opposite case — graphs that must deliberately *share* an id space, such as a KG compiled in shards or renamed across versions while keeping its published ids. Both default to the historic constant when `uuid_fields` is unset. ([#122](https://github.com/SkyeAv/Tablassert/pull/122))

- **A `duplicate-edge-ids` assertion in `--qc` study output.** `study_kgx` checked `duplicate-node-ids` but had no edge equivalent, even though KGX requires edge ids to be unique. The symmetric check now streams the final edges file and reports duplicates independently of the writer, catching an id collision from any source. ([#122](https://github.com/SkyeAv/Tablassert/pull/122))

- **RIG `source_files` come from graph config, and RIG YAML output is indented.** `target_info.edge_type_info.source_files` was previously scraped from edge `source_record_urls`; an optional `rig.source_files` graph-config key now supplies the list, applied to every edge type, while edges keep emitting `source_record_urls` unchanged. Generated `.RIG.yaml` files are written with block sequences indented under their parent key, unicode allowed, and a 120-column wrap so long prose fields stay readable. ([#121](https://github.com/SkyeAv/Tablassert/pull/121))

### Fixed
- **Two edges deriving one id abort the build instead of shipping a duplicate.** Dedup keyed on the full canonical record bytes, which was safe only because the id was a pure function of those bytes. That equivalence breaks the moment the hash covers a subset, and it was already imperfect: `stable_json_bytes` never sorted despite its name, so two logically identical records arriving with different key order derived one id, produced different bytes, and *both* shipped. Edges now dedup on the derived id itself — an exact repeat collapses as before, while two genuinely different edges claiming one id raise `uuid-fields-not-a-key` with the id, the fields that differ (recovered by re-reading the partial output, on the failure path only), and the declared field list. The nodes path is unchanged: node ids are CURIEs, not derived hashes. ([#122](https://github.com/SkyeAv/Tablassert/pull/122))

### Performance
- **Edge dedup holds 24 bytes per edge instead of a full copy of every record: 2469 MB -> 100 MB peak on a 1.27M-edge graph.** `record_if_new` retained the complete bytes of every unique record to make suppression byte-exact — roughly 800 bytes per edge, and ~2.4 GB resident on MultiomicsKG 3.0.0 purely for the dedup set. The edge path now keys a `FxHashMap<[u8; 16], u64>` on the raw UUID bytes with an xxh64 of the id-free record as the value: a measured **25x** reduction that also removes the per-record heap allocation, and on the same graph took the dedup pass from 477s to 37s (the old path's allocation churn dominated; the margin will be smaller on machines under less memory pressure). The content hash is computed *before* the id is inserted, so no record is cloned to strip it back out, and a declared `uuid_fields` list additionally cuts the number of keys the hasher visits per edge. ([#122](https://github.com/SkyeAv/Tablassert/pull/122))

### Changed
- **`docs/api/utils.md` now describes the derivation that actually runs.** The page documented a `"\t".join(values)` encoding removed some releases ago (the join has been length-prefixed for collision-safety) and claimed edge ids covered only "subject, predicate, object, qualifiers, and publication" — which was never true, and is precisely what `uuid_fields` now makes achievable. Both are corrected, and the canonicalization, namespacing, and uniqueness rules are documented alongside. ([#122](https://github.com/SkyeAv/Tablassert/pull/122))

- **Golden UUID vectors pin the derivation.** No test asserted a concrete UUID value, so any change to the encoding could land silently. Three vectors now fix the full-record, declared-field, and raw-parts derivations; if one moves, edge ids in every published graph moved with it. ([#122](https://github.com/SkyeAv/Tablassert/pull/122))

## 15.1.0 - 2026-08-25

### Added
- **`disease_context_qualifier` is granted to `EntityToDiseaseAssociation` / `EntityToPhenotypicFeatureAssociation` edges as a class-scoped policy override.** Biolink declares the slot only on the `ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation` lineage, while `FDA_regulatory_approvals` lives only on the `EntityToDisease` / `EntityToPhenotypicFeature` classes DAKP pins via `category_override` — so a contraindication edge could natively carry one slot or the other, never both. The new `CLASS_FIELD_OVERRIDES` table in `biolink.py` grants the qualifier to the pinned classes: `prune_to_class` keeps the value on those rows instead of nulling and rescuing it, and record validation strips the granted field from its in-memory copy so the deliberate gap is not reported as `extra_forbidden`. The grant is deliberately ahead of the pinned model (pending an upstream Biolink widening); a tripwire test asserts every granted field stays absent from its class, so a future biolink-model release that attaches the slot fails the suite until the stale grant is removed ([#120](https://github.com/SkyeAv/Tablassert/pull/120)).

### Fixed
- **`number_of_cases` annotations are no longer coerced into `study_size`.** `number_of_cases` is a legitimate Biolink `Association` slot (cases carrying the phenotype/disease), but `STUDY_SIZE_PREFIX_PATTERN` matched the column via "number of cases" and `coerce_study_size_columns` renamed it to `study_size`, destroying the edge field. A new `STUDY_SIZE_EXEMPT_PATTERN` makes `study_size_target` return `None` for the exact slot, so the column reaches the edge as `number_of_cases` ([#119](https://github.com/SkyeAv/Tablassert/pull/119)).

## 15.0.0 - 2026-08-24

### Breaking Changes
- **The `approval_ids` biolink override is removed; the column is no longer a curated edge field.** `TABLASERT_EDGE_EXTRAS` no longer carries `approval_ids`, so the FDA-application-number pass-through no longer bypasses Biolink validation: `ALLOWED_EDGE_FIELDS` and `KNOWN_PENDING_EDGE_FIELDS` derive without it, an `approval_ids` annotation now emits a relocation warning and folds into `supporting_text` like any unknown edge field, per-record class pruning drops it, and strict KGX validation counts edges carrying it as real `extra_forbidden` failures (the pending exemption no longer forgives them). The DAKP real-world fixtures drop their `approval_ids` annotations accordingly ([#117](https://github.com/SkyeAv/Tablassert/pull/117)).

  **Migration:** annotate `FDA_regulatory_approvals` instead — it is a real Biolink slot declared on `EntityToDiseaseAssociation` / `EntityToPhenotypicFeatureAssociation`, so no curated override is needed. It is multivalued, so declare `split_by` for joined cells (e.g. `011111|022222`), and pin the subclass with the new `category_override` (below) when the section's derived pair class is `ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation`, which does not declare the slot and would prune it. Any casing of the name works; the annotation canonicalizes onto the exact `FDA_regulatory_approvals` spelling. Readers consuming a curated `approval_ids` edge field should read `FDA_regulatory_approvals`; strict `validate-kgx` consumers lose the pending exemption for the old name.

### Added
- **A per-section association-category override keyed on the resolved object category.** `Statement` gained `category_override`: a `{object category: association category}` map (validated against the `Categories` and `EdgeCategories` enums) that the edge-category derivation applies per row in place of the `(subject, object)` pair lookup, keyed on the raw object category *before* the role rollup that merges `Disease` and `PhenotypicFeature` into one pair key. Rows whose object category is absent from the map derive as before, and pinned classes still pass through `resolve_association_class` reconciliation against the section predicate — a pinned class whose `predicate` slot rejects it is walked up the association hierarchy, with a `BiolinkRelocationWarning` at config time. This is the DAKP split: drug→disease and drug→phenotype rows can now pin `EntityToDiseaseAssociation` / `EntityToPhenotypicFeatureAssociation` respectively, so the subclass-only slots those classes declare (`FDA_regulatory_approvals`, `number_of_cases`) survive `prune_to_class` on the edge instead of being nulled and rescued into the pruned column ([#118](https://github.com/SkyeAv/Tablassert/pull/118)).
- **Annotation casing now canonicalizes onto allow-listed spellings, preserving mixed-case slots like `FDA_regulatory_approvals`.** The `Annotation` validator no longer blind-lowercases: a declared name whose lowercase form matches an edge allow-list entry canonicalizes onto that entry's exact spelling. Model-slot annotations are unchanged (`P_Value` still becomes `p_value`), but Biolink's mixed-case `FDA_regulatory_approvals` slot (on `EntityToDiseaseAssociation` / `EntityToPhenotypicFeatureAssociation`) can now be authored in any casing and reaches the final edge verbatim as `FDA_regulatory_approvals` instead of being lowercased into an unknown name and folded into `supporting_text` ([#117](https://github.com/SkyeAv/Tablassert/pull/117)).

## 14.0.0 - 2026-08-24

### Breaking Changes
- **Retrieval-source entries now use `resource_id` as their sole identifier.** Generated edges no longer duplicate each nested `sources` entry's provenance identifier into `sources[].id`; readers should use `sources[].resource_id`. Tablassert's validator supplies the inherited `id` only to its in-memory compatibility copy while the pinned Biolink model still requires it, so compliant output still validates and nothing changes in the decoded record or the written NDJSON ([#115](https://github.com/SkyeAv/Tablassert/pull/115)).

  **Migration:** readers indexing `sources[].id` must read `sources[].resource_id` instead; the two carried the same value, so the change is a key rename at the reader.

### Added
- **An explicit retrieval-`sources` template, with `{edge_id}` record-URL interpolation.** `ManualProvenance` gained `sources`: an ordered list of Biolink `RetrievalSource` entry templates that replaces the derived "graph infores as primary, one `supporting_data_source` per upstream" emission entirely. Each entry declares `resource_id`, `resource_role` (validated against the three `ResourceRoleEnum` members), and optional `upstream_resource_ids` / `source_record_urls`; the entries are emitted verbatim, in the order given. The template is mutually exclusive with `upstream_resource_ids` and `upstream_source_record_urls` (it subsumes both), must be non-empty with unique `resource_id` values, and must carry at least one `primary_knowledge_source` or `aggregator_knowledge_source` entry; every violation reports as `override-bad-sources`. A `source_record_urls` value may embed the literal `{edge_id}` placeholder to build per-edge drill-down URLs: the edge `id` is a content hash assigned at the final dedup stage, *after* subgraphs are written, so the literal placeholder is what the subgraph carries and what the hash covers (edge ids stay deterministic), and a post-dedup sweep of the final `*.edges.ndjson` substitutes each record's own `id`. Files carrying no marker are left byte-identical. This lets an ingest such as DAKP reproduce legacy KG retrieval provenance declaratively from the table config (an aggregator primary entry with a per-edge viewer URL, the real primary knowledge source as a sibling entry) instead of post-processing the built edges ([#116](https://github.com/SkyeAv/Tablassert/pull/116)).

### Fixed
- **Negative-log p-value columns are un-logged instead of shipped verbatim.** `pvalue_target` already routed spellings like `negative log p value` / `-log10(p)` onto `p_value` / `adjusted_p_value`, but the score rode through untouched: a −log10(p)=8 enrichment column (the `mokg-v12` HOYER1 fixture ships exactly this shape) emitted `p_value` 8.0 — wildly out of range — and `sig` banded it `not_significant`, the exact inverse of the truth. The coercion now recognizes an explicit negation marker (`negative` / `negated` / `neg` / `-`) before a `log`/`log10` p-or-q token (`is_neglog10_column`) — requiring a complete p/q-value token (or a bare delimited `P`), so a word merely starting with p or q (`negative log protein`) no longer matches — and converts the score (`p = 10**-x`) when it lands on the numeric slot, in both `coerce_pvalue_columns` and `sig`. A raw p/q candidate now always beats a −log10 alias of the same statistic before fuzzy ranking runs, so a short raw name (`P`, `FDR`) can no longer lose to a long alias it would then be un-logged over. Float64 underflow floors extreme scores at `0.0` (indistinguishable from `p ~ 0`, and the band is identical); nulls stay null. A plain `log10` spelling without a negation marker is deliberately left verbatim — the sign convention is ambiguous there — and when a raw p-value column coexists with a −log10 alias the raw column still wins and the alias is left untouched ([#114](https://github.com/SkyeAv/Tablassert/pull/114)).

## 13.0.0 - 2026-08-24

### Breaking Changes
- **The inlined supporting study now carries the current Biolink Study metadata, with disjoint ids and names and no `#` composition.** Supporting studies were verbose and self-duplicating: a published Excel section keyed its `Study` by `"PMC11708054#all correlations"`, repeated the sheet as the `name`, and fabricated a per-edge `StudyResult` (`"…#row42"` / `"all correlations row 42"`) even when it carried no evidence; an unpublished section keyed the study by its own config filename (`my_table.yaml`). The contract is now: `Study.id` is a real identifier — the section's publication CURIE, or the config stem (YAML filename minus its extension) as the rescue fallback — and `Study.name` a *disjoint* human label (worksheet for spreadsheet sources, source filename otherwise; omitted when it would equal the id). Study-level metadata declared as annotations (`study_size`, `study_cohort`, `study_context`, `study_date_range`, `study_method_description`, `study_method_types` — the `Study` node properties that [biolink-model#1770](https://github.com/biolink/biolink-model/pull/1770) replaced the deprecated `supporting_study_*` association slots with) lands as real typed fields **on the Study**, never on the edge and never inside a description string. The `StudyResult` is identified only by the scoped CURIE `row:<N>` — its source row — and carries no name; its `description` is reserved for values with no structured home (routed unsatisfiable columns and class-pruned qualifiers). No `#` delimiter appears anywhere in the output. A published section always keeps the `{id, name}` wrapper; an unpublished one emits the struct only when it has metadata, routed unsatisfiable values, or pruned values to carry (the contentless-skip behavior is unchanged).

  **Migration:** readers keying `has_supporting_studies` by the composed `publication#sheet` string should key by the publication CURIE; consumers scraping `"row N"` result names or `supporting_study_size=…` description entries should read `StudyResult.id` (`row:<N>`) and the typed `Study.study_*` fields instead. Declaring the deprecated `supporting_study_size` / `sample_size` / `supporting_study_*` annotation names still builds — the coercion phase renames them onto the canonical `study_*` properties — but emits a `BiolinkRelocationWarning` naming the destination. If multiple study-size synonyms coexist, the canonical/winning value is retained and the losing synonyms are dropped as duplicate representations of the same Study property.

- **biolink-model 4.4.4: the statistical edge slots the model now declares are emitted as real model-typed values.** The dependency moved from 4.4.3 to 4.4.4 and every model-derived set follows it with no code change: `effect_size` (`float`) and `effect_type` (`EffectTypeEnum`) are real `Association` slots ([#1774](https://github.com/biolink/biolink-model/pull/1774)) and leave the pending-exemption set, so `effect_size` ships as a real JSON number instead of a `{:.4g}` string; `statistical_significance_qualifier` is attached to every association class and rides the edge as a bare `StatisticalSignificanceQualifierEnum` token (`"strongly_significant"`, no `biolink:` prefix) instead of being relocated into the study description; `EffectTypes` is now derived from the installed model's enum rather than a local tuple. `relationship_strength` is deliberately no longer listed among unsatisfiable study-routed names — it is a legacy alias the coercion renames to the edge slot `effect_size`.

  **Migration:** consumers parsing `effect_size` as a `{:.4g}` string must read a real JSON number now (`-0.85`, not `"-0.85"`), and `statistical_significance_qualifier` moved out of the study description onto the edge as a bare enum token. Configs naming a class 4.4.4 renamed no longer validate: `AffinityMeasurement` — which appears in generated `avoid:` / `prioritize:` lists, since those are emitted as the complement of the prioritized set — is now `ProteinLigandAssayResult`; rename it in place, or regenerate the config against the current model. P-value columns are deliberately unaffected: they stay controlled scientific-notation strings regardless of the model's `float` typing (12.1.0).

- **An unpaired `effect_size` / `effect_type` annotation is dropped with a warning instead of failing the section.** 11.0.0 made the pair mandatory ([#92](https://github.com/SkyeAv/Tablassert/pull/92)): a section declaring one half without the other raised `annotation-effect-size-without-type` / `annotation-effect-type-without-size` and took the rest of the table down with it. `Section.drop_unpaired_effect_annotations` replaces that hard fail — the unpaired half is dropped from the section with an `UnpairedEffectAnnotationWarning` naming what was dropped and from where, and the section and its edges are kept. The value was going to be discarded by the build anyway (`coerce_effect_type_columns` nulls an effect type that has no effect size), so failing the section traded a real table for a column the pipeline would never have emitted. Names are judged by their coerced target rather than their raw spelling, so the aliases the clean phase renames (`odds ratio`, the legacy `relationship_strength`) are seen exactly as the build sees them; the validator runs after `ingests.to_sections` expands `template`/`sections`, so an unpaired half declared once on a template is dropped from every expanded section. Declaring both together is still the recommendation — neither half carries evidence alone ([#105](https://github.com/SkyeAv/Tablassert/pull/105)).

  **Migration:** the two error codes are retired; callers asserting on `annotation-effect-size-without-type` / `annotation-effect-type-without-size` should assert on `UnpairedEffectAnnotationWarning` instead. Configs that failed validation under 11.x and 12.x now build, minus the unpaired column.

### Added
- **The final graph QC now asserts no field in the emitted NDJSON is null or empty, and that every node carries a name.** `build-kg --qc`'s stage-7 study pass (in the spirit of `studyKGtsvs.pl`) gained two assertions. `empty-or-null-values`: any field in the nodes or edges file whose value is JSON `null`, a string that strips to empty, or an empty container — checked recursively, so a null or blank nested inside an `attributes` list counts — fails the build. The check is deliberately stricter than the NDJSON writer's `strip_nulls` (`rust/src/json.rs`), which scrubs dict entries at every depth but passes array scalars (`["x", ""]`) and emptied nested objects (`[{}]`) through verbatim; the study stage now asserts the stronger contract — no null or empty value anywhere — so the first such shape to reach an emitted file fails the build loudly instead of shipping silently. Null-like *strings* (`NA`/`NaN`/`null`/`none`) are also dropped by the writer but are neither null nor empty, and are deliberately not flagged; the `original_*` whitespace exemption does not extend to emptiness. `unnamed-nodes`: a node record whose `name` key is missing, `null`, or strips to empty fails the build — the missing-key case is what pipeline output surfaces, since `strip_nulls` deletes empty and null-like names before the file is written. Both report offenders per field or per node id, capped at 10 examples like the other assertions.
- **The final graph QC now asserts every node has an `id` and every edge has `subject`, `predicate`, and `object`.** Two more stage-7 study assertions join the `unnamed-nodes` check (which already requires a non-empty node `name`). `unidentified-nodes`: a node record whose `id` key is missing, `null`, or strips to empty fails the build; since the id is exactly what is absent, examples key on the node's `name` (or `<no name>` when it has none). `incomplete-edges`: an edge record missing any of the three core slots — missing key, `null`, or strips-to-empty — fails, counted per slot (e.g. `predicate (2)`). The writer's `strip_nulls` deletes a null slot outright rather than emitting it, so on pipeline output a hit means the slot was null upstream and the record shipped broken — exactly the condition these assertions exist to catch loudly. Non-string, non-null ids and slots pass, mirroring the name convention (no writer emits them).

### Changed
- **`openpyxl` is an explicit test dependency.** It was only ever transitive via `linkml` 1.10; biolink-model 4.4.4's `linkml` 1.11 bump dropped it, and `tests/test_cover_lib.py` imports it directly.

### Performance
- **Entity resolution now materializes each section once instead of once per node column.** `resolve_batch` used to re-execute the entire upstream lazy plan — the source scan, every encoding/regex op, and both NLP normalization levels — once per resolved column: `distinct(...).collect()` per column, `join_matches`' collect per column, and `log_unmatched`'s anti-join collect when `--log` is on, so a section resolving subject + object + two qualifiers ran that pipeline roughly five times (up to nine with logging). The batch now collects the frame a single time and runs term extraction, unmatched logging, and the level-one/level-two join-backs against the in-memory frame; `join_matches` keeps its LazyFrame-in/LazyFrame-out contract over a new eager core, and output rows are unchanged. On a 50k-row synthetic section with four resolved columns the resolve phase runs about 1.75× faster, with the gain growing as the upstream plan gets heavier. Separately, `trim` — the op that drops the spent raw `column_<n>` columns — moved from the finalize ops to just before `resolve_batch`, so the materialized frame and every downstream join no longer carry columns nothing reads; its progress phase label falls back to `transform` instead of flashing `finalize` early.
- **The agent pre-renders every deterministic inspection payload into the task, stops re-planning, caps in-agent improve rounds, and memoizes identical builds.** `pmc_article_context` and `read_table` are pure functions of files the supervisor has already downloaded, yet each call cost a whole LLM step: fleet logs showed roughly 2,100 article-context and 2,500 `read_table` emissions, with 68% of articles exhausting the 20-step budget largely on that inspection overhead. `render_task_context` now ships the article summary and a head preview of every candidate table — and every Excel worksheet, capped at 10 with the remainder noted — inside the task text (truncated at 60k characters), so the agent can author a config with zero inspection tool calls; both tools stay registered as fallbacks for rows beyond a preview, and a table that cannot be previewed renders a visible in-band note rather than aborting the run. `build_agent`'s `planning_interval` default changed from `3` to `None` — each smolagents planning turn is an extra round trip carrying the full prompt, and the task already prescribes a fixed short workflow (derive → build → optional edit → answer). The instructions cap in-agent improve rounds at two, after which the supervisor's deterministic improve loop continues, and `build_and_audit` memoizes per tool instance so a model re-running an unchanged config no longer pays a full validate + build + coverage pass ([#113](https://github.com/SkyeAv/Tablassert/pull/113)).

## 12.1.0 - 2026-08-18

### Added
- **Per-upstream `source_record_urls` via a provenance override.** `ManualProvenance` gained `upstream_source_record_urls`: a mapping of upstream infores CURIE to source record URLs. When set, the section's `source.url` values serve the RIG only and are no longer emitted on the primary `sources` entry; each listed upstream `supporting_data_source` entry carries its own `source_record_urls` instead. Keys must be infores CURIEs declared in `upstream_resource_ids` (`override-bad-upstream-urls`). This lets transforming resources (e.g. DAKP's `infores:multiomics-drugapprovals`) keep their primary entry bare while the dataset download URLs live on the `infores:dailymed` / `infores:faers` entries they were actually retrieved from. Legacy placement — URLs on the primary entry — is unchanged when the mapping is absent (#104).

### Fixed
- **P-value columns are emitted in scientific notation again.** The KGX-compliance rework (#71) made `format_numeric()` emit `p_value` / `adjusted_p_value` as real JSON numbers because the `numeric_slot_kind` lookup reports the `float`-typed Biolink slots — which silently retired the `{:.4e}` scientific-notation branch for exactly the columns it existed for, so edges shipped shortest-repr numbers (`"p_value":0.0001`, `"p_value":0.05`) instead of the controlled notation the tutorial documents (`"p_value":"1.0000e-03"`). P-value-like columns are now always formatted as scientific-notation strings regardless of the slot's model type: Biolink validation here and downstream runs in Pydantic's lax mode, which coerces the numeric string back, so `validate-kgx` stays green (verified against the pinned `biolink-model` classes). Non-p-value numeric columns keep the model-typed behavior: a real JSON number once a future biolink model types the slot (`biolink/biolink-model#1770` / `#1774`), controlled `{:.4g}` strings until then.

## 12.0.0 - 2026-08-14

### Breaking Changes
- **`tablassert agent` now builds into a caller-owned graph config; the internal graph registry is gone.** Every successful agent build used to self-register into an agent-maintained aggregate at `<state-dir>/graph.yaml` — a dedicated registry module fabricated its metadata (hardcoded name `tablassert-agent`, version `1`, a synthetic PMC-flavored `rig:` block, first-wins `fullmap`), quarantined a corrupt registry to `graph.yaml.corrupt-<timestamp>`, and `tablassert rebuild-agent-graph` reconstructed it from `state.json`. All of that is gone. The agent now takes a required `--configuration-file`/`-f` naming a graph YAML *you* own: the graph's real `name`, `version`, `fullmap`, and `rig:` drive every per-article audit build — nothing is synthesized — and each successful result mutates that file in place, replacing any `tables` entry for the same PMC id and appending the new config's absolute path under an exclusive sidecar flock with an atomic rename. The target is validated against the `Graph` model up front, before any model is built or article fetched; a malformed target raises `graph-validation-failed` instead of being quarantined and silently rebuilt.

  **Migration:** replace `--fullmap <path>` with `--configuration-file <graph.yaml>` (same `-f` short flag, new meaning) and build the aggregate afterward with `tablassert build-kg -f ./graph.yaml`. Remove any `tablassert rebuild-agent-graph` invocations — the subcommand is gone, and an existing `<state-dir>/graph.yaml` registry is orphaned: carry old results forward by adding their `configs/<pmc_id>.yaml` paths to your graph's `tables:` list, or rerun the PMC ids (terminal records are now deliberately reprocessed every invocation, so a rerun replaces its target-graph entry). Per-article artifacts move from `builds/<pmc_id>/agent_0.0.1.*` to `builds/<pmc_id>/artifacts/<graph-name>_<graph-version>.{nodes,edges}.ndjson`. Concurrent agents may now share one target graph (flock-serialized appends, last-writer-wins per PMC) but need separate `--state-dir`s — the inverse of the old guidance. The `tablassert.graph_registry` module is deleted; direct API callers using `fullmap=` keep working, and `run_supervisor` / `build_and_audit` / `make_tools` now also accept a `graph=` (with `graph_path=` required on `run_supervisor`).
- **The QC embedding stage migrated from BioBERT to SapBERT, and the model cache moved with it.** The cascade's final stage now scores residual pairs with `cambridgeltl/SapBERT-from-PubMedBERT-fulltext` — PubMedBERT self-aligned on UMLS synonym pairs (NAACL 2021) — rather than `pritamdeka/BioBERT-mnli-snli-scinli-scitail-mednli-stsb`: SapBERT's training objective is exactly the relation the stage audits (whether two names denote the same concept), whereas the old model was trained for sentence entailment. Consequences of the swap: the cosine pass threshold stays at `0.5` — now exposed as `qc.SIMILARITY_THRESHOLD` — carried over from the BioBERT gate and *not* re-tuned for SapBERT's score distribution, so reject rates may shift until it is tuned; the `on_phase` sub-phase label renamed `qc:bert` → `qc:sapbert`; rejection logs now report the score as `sapbert=` (was `bert=`); and the model cache moved from `.tablassert/biobert/` to `.tablassert/sapbert/` (`qc.MODEL`) — existing `.tablassert/biobert/` directories are stale and can be deleted, and SapBERT (~440 MB) is downloaded and cached on the first QC run that reaches the embedding stage. No new dependencies: the same `[qc]` extra (`sentence-transformers`, `scikit-learn`) serves both models.

### Added
- **A QC abbreviation-expansion stage between fuzzy matching and the embedding stage.** Pairs that fail RapidFuzz now go through a Schwartz-Hearst (2003) matcher — the source text abbreviates the resolved preferred name, or vice versa — before any model runs, and pass deterministically at zero inference cost. This rescues the class both fuzzy matching and embedding cosine miss: `AML` ↔ `acute myeloid leukemia`, `EGFR` ↔ `epidermal growth factor receptor`. Matching is case-insensitive, requires a short form of length ≥ 2 that is strictly shorter than the long form, and demands the abbreviation's first character land on a long-form word boundary (so `na` is not an abbreviation of `banana`). The audit is now a four-stage cascade (exact → fuzzy → abbreviation → SapBERT), and a new `qc:abbrev` sub-phase fires through `on_phase` when the stage runs.
- **`approval_ids` is now a curated pass-through edge field.** FDA application numbers declared as an `approval_ids` annotation (the DAKP translator-ingest precedent) now reach the final KGX edges as their own top-level field instead of being folded into `supporting_text`, notwithstanding that no association class declares the slot: the allow-list keeps the column out of the fold sweep, annotation validation emits no `BiolinkRelocationWarning`, and strict KGX validation counts it as *pending* Biolink support exactly like `effect_size` / `effect_type`. The representation follows the ingest: a pipe-joined scalar (e.g. `011111|022222`) is emitted verbatim as a scalar string — Tablassert does not split it into a JSON array.
- **`build-kg` gained `--threads` (`-t`) to control parallel fullmap reads, and those reads now scale past the 16 record shards.** Entity resolution always looked fullmap terms up with the Rust default: batches of ≥ 1024 terms fan out one reader per RECORDS shard file (16), smaller batches stay serial, and nothing above the Rust layer ever supplied an explicit worker count. The flag overrides that count — pin or limit readers on a shared host (`--threads 4`), force the serial path for reproducibility (`--threads 1`), or force parallel reads for small batches. And because redb readers hold a SHARED lock and each reader opens its own read transaction, readers never contend with each other — the one-reader-per-shard ceiling was an implementation choice, not a redb limit. The shard fan-out now splits the busiest shards' term buckets across additional concurrent readers of the SAME shard file whenever `--threads` exceeds the non-empty shard count, and results stay content- and order-identical at any worker count. Unset keeps the prior auto behavior.
- **`build-kg --release` now drops edges whose `effect_size` is exactly zero.** When an `effect_size` column is present, release-mode builds filter out rows with a non-null zero effect size before fullmap resolution, matching the existing release-mode drop of `biolink:not_significant` edges. Rows with a null effect size are kept.

### Changed
- **`build-fullmap`'s prebuilt extraction now runs in the Rust extension, and extracted archives are validated against the force-build contract before install.** The prebuilt download (and its `sha256sum.txt` check) is unchanged, but the extraction is no longer Python-side (`tarfile` zstd with an installed-`zstd`-binary fallback): the extension streams the archive through zstd → tar — the multi-GB decompressed tar is never materialized on disk — with the GIL released, extracts into a temp directory on the output's filesystem, and validates the bundle BEFORE renaming anything into place: the primary's `meta` schema tag must be exactly `tablassert.fullmap.v5` (older schemas are rejected), a `build_id` must be present, the shard files must be exactly the set the primary advertises — no gaps, no extras — and every shard's `build_id` must equal the primary's. Only a passing bundle is atomically renamed beside `--output` (primary → `--output`, shard `i` → `<output stem>.s<i>.redb`); unsupported entry types (symlinks, devices) and PAX-sparse archives are rejected loudly, and path traversal is blocked. Any failure raises, so the command falls back to the from-scratch BABEL build exactly as a failed download does (the archive is kept for that retry). For end users the visible effect is faster extraction and the guarantee that an invalid prebuilt can never land a broken database at `--output`; the progress callback keeps firing its step-detail strings (`opening archive`, `extracting <entry>`, `validating`). The private Python helpers this replaces — `_extract_zst_tar` / `_stream_tar` — are gone; they were never public API, so the removal breaks only code that reached into those internals, and the external `zstd` executable (or Python 3.14 native zstd) is no longer needed at runtime.

### Fixed
- **`species_context_qualifier` is no longer auto-derived or relocated into supporting studies.** The field was derived from resolved node taxon metadata, then stored as a `StudyResult.description` key/value when the selected association class could not accept it. It is now disabled for both qualifiers and annotations; node taxon metadata remains available on node records.
- **A section with no publication and nothing to preserve no longer fabricates a supporting study.** `inline_supporting_study` emitted `has_supporting_studies` unconditionally, so a table declaring no `publications` got a `Study` keyed by its own config filename (`my_table.yaml`) whose single `StudyResult` was a row index into a file the pipeline regenerates. Biolink defines `has supporting studies` as "studies that produced information used as evidence", and nothing in `NCATSTranslator/translator-ingests` models evidence that way — there a `Study` is a real cohort (ICEES), dataset (COHD), trial (CTKP), or text-mining group (SemMedDB/TMKP) with typed `StudyResult` slots. The struct is now emitted only when it carries something: a section with a real publication keeps it unchanged (`PMID:123#Table_S7 row 12` is genuine provenance), as does any section with routed `UNSATISFIABLE_EDGE_FIELDS` statistics or class-pruned values to preserve. The sheet-name and row-number columns are consumed either way — they are never meant to reach the edge.

## 11.0.0 - 2026-08-13

### Breaking Changes
- **Edges no longer carry a flat `primary_knowledge_source` scalar; retrieval provenance lives only in the nested `sources` list.** Each edge emitted the graph infores three times — as the flat `primary_knowledge_source` column, as `sources[0].id`, and as `sources[0].resource_id`. The flat scalar is now gone, matching current `NCATSTranslator/translator-ingests` practice (no ingest sets it anymore; their KGX validation plugin reads only `sources`) and the Biolink Model's direction. The `sources` structure is unchanged: the primary entry (`resource_role: primary_knowledge_source`) carries `upstream_resource_ids` and `source_record_urls`, with one `supporting_data_source` entry per upstream. RIG generation is unaffected — it already read the nested `sources` first and still accepts the flat columns from legacy parquet inputs. Downstream consumers reading the flat column should read the primary `sources` entry's `resource_id` instead.

  `sources[].id` still mirrors `resource_id`: `RetrievalSource` inherits `id` from `entity`, and the LinkML-generated Biolink Pydantic classes (PyPI 4.4.3 and GitHub master 4.4.4 alike) require it, so omitting it fails KGX validation. The mirror disappears once biolink-model [#1706](https://github.com/biolink/biolink-model/issues/1706) / [#1731](https://github.com/biolink/biolink-model/pull/1731) land.
- **Graph configs now require a `rig:` section, and every generated RIG is audited before anything is written.** The generated Resource Ingest Guides were structurally weaker than upstream Translator RIGs — output KGX filenames stood in for source access locations, `relevant_files`/`included_content` were unstructured strings, and edge summaries read raw intermediate parquets — and nothing validated the document before it shipped. Every human-authored RIG fact now lives in a required `rig:` section mirroring the released `resource-ingest-guide-schema`: `source_info` (explicit `infores_id`, non-empty terms-of-use assessment, URL-bearing `data_access_locations`, enum `source_status`), `ingest_info` (required utility/scope, structured relevant/included/filtered content), `provenance_info` (required contributions), optional supporting-data/target-level extras, and `artifact_base_url`/`artifact_base_path`. Legacy top-level `description`/`contributions`/`ui_explanation`/`infores` keys are rejected with a `rig-legacy-keys` migration pointer. `compile_graph` writes artifacts under `rig.artifact_base_path`, composes `relevant_files`/`included_content` for the exact generated `.nodes.ndjson`/`.edges.ndjson` names (with observed record counts and fields), cross-checks configured upstream `relevant_files` against the table sources (stale entries fail the build), and streams edge/node type summaries from the FINAL deduplicated KGX — per-predicate edge types with resolved SPO categories, list-valued KL/AT, qualifier shapes, observed `edge_properties`, and role-separated knowledge sources. `compile_rig` audits the assembled document in memory and raises `rig-validation-failed` without writing anything on violation, so every emitted RIG is schema-shaped and internally consistent by construction.

  **Migration:** move your graph's human-authored RIG facts into a `rig:` section; the built-in graph registry and the agent's measurement configs emit honest built-in `rig` blocks to copy. Edge `primary_knowledge_source` now always derives from the explicit `rig.source_info.infores_id` — it is never implicitly derived from the graph name.

- **`effect_size` and `effect_type` are now mandatory as a pair.** A section declaring one without the other fails validation with `annotation-effect-size-without-type` / `annotation-effect-type-without-size`. Only half the rule existed before, and only as silent data nulling at build time: `coerce_effect_type_columns` nulls an `effect_type` on every row where `effect_size` is null, but a bare `effect_size` validated clean, warned nothing, and shipped on the edge. That is uninterpretable evidence — `"effect_size": "0.85"` says nothing without knowing whether 0.85 is an odds ratio, a Spearman rho, or a log2 fold change — and the only guard against it was prose advice in the agent prompt, which nothing enforced.

  The check lives on `Section`, so it covers `tablassert validate`, `build-kg`, and the agent's `validate_section` final-answer gate alike, and it runs after `template`/`sections` expansion — annotation lists are concatenated, so a constant `effect_type` declared once on the template pairs with each section's own `effect_size` column:

  ```yaml
  template:
    annotations:
      - {annotation: effect_type, method: value, encoding: correlation_coefficient}
  sections:
    - annotations: [{annotation: effect_size, method: column, encoding: B}]
  ```

  Names are judged by their **coerced** target, not their raw spelling, so alias forms are caught the same way the build sees them: `odds ratio` and the legacy `relationship_strength` both coerce to `effect_size` and both now require a sibling `effect_type`. The runtime coercion behavior is unchanged — this adds a config-time gate in front of it, nothing more.

  **Migration:** add an `effect_type` to any config declaring an effect size (`method: value` when every row shares one statistic, `method: column` when the table provides it); its value is coerced to the permissible `EffectTypes` set as before. Drop a lone `effect_type` that had no effect size — the build was already discarding it.

### Changed
- **The `build-kg --qc` study no longer flags `original_*` fields for leading/trailing whitespace.** Those slots are verbatim copies of the source-table cell (written by `Tcode.encoding` under an `original_` prefix before any regex/normalization runs), so retaining the cell's whitespace is faithful to the source, not a defect. The whitespace assertion now skips any key prefixed `original_`, while every other field is still checked exactly as before.

### Fixed
- **Duplicate qualifier declarations now fail at config time instead of crashing mid-build with a raw polars error.** A table config that declared the same qualifier key twice died deep into the build with `polars.exceptions.ColumnNotFoundError: unable to find column "<qualifier>_two"` — both declarations resolved the same column, and the fullmap join drops the `<col>_two` working column right after the first resolve pass, so the second pass hit an already-dropped column. The statement validator now rejects duplicate qualifier keys at config time with the `qualifier-duplicated` code, and `resolve_batch` gained a defense-in-depth guard: it validates its specs schema-only before any term extraction or database access, raising `resolve-bad-specs` on duplicate spec columns or a spec missing its `<col>` / `<col>+tag` normalization column, so the same failure class can never resurface as a raw polars crash from another call path.

## 10.1.0 - 2026-08-13

### Added
- **`build-kg --qc` now studies the final KGX NDJSON and fails the build on violations.** After the graph is compiled, a seventh stage (only with `--qc`) streams the emitted `{name}_{version}.nodes.ndjson` / `.edges.ndjson` and asserts, in the spirit of the legacy `studyKGtsvs.pl` QC script: no duplicate node ids (Rust dedup only removes byte-identical lines, so same-id/different-content nodes are caught here), no nodes referenced by edges but never declared, no declared nodes participating in no edge, no empty/malformed JSON lines, and no string values with leading/trailing whitespace. Violations print a one-line-per-assertion summary with examples to stderr and exit non-zero, so a build can be gated in CI; a clean study logs and continues. The stage is stdlib-only — it rides the existing `--qc` flag but needs nothing from the `[qc]` extra itself. A missing NDJSON file is itself a violation, so a typo'd path can never read as a pass.

### Fixed
- **`prune_to_class` no longer crashes a subgraph build on a scalar column bound to a multivalued qualifier slot.** Any section carrying a scalar column for a slot that Biolink declares multivalued *somewhere* (`anatomical_context_qualifier`, declared multivalued by 14 association classes, was the case in the wild) died at collect with `polars.exceptions.InvalidOperationError: cannot cast List type (inner: 'String', to: 'String')` — surfacing inside `format_numeric`, so the traceback pointed nowhere near the cause. The multivalued wrap was built per row (`when(class-is-multivalued).then(concat_list(...)).otherwise(scalar)`), which asks one column for two dtypes and makes Polars insert an impossible `strict_cast`. Worse, the scan that decided "is this slot multivalued?" counted classes that do not declare the slot at all (`is_multivalued` is `False` there), so every qualifier slot looked mixed. The scan now covers only *declaring* classes and the wrap applies unconditionally when all of them type the slot multivalued — uniform for every qualifier slot in the installed biolink-model — with nulls preserved (`concat_list` maps null → `[null]`). A hypothetically mixed slot keeps its scalar instead of crashing. Refused values are still nulled per row and rescued into the inlined `StudyResult` description, unchanged.
- **The "folded into `supporting_text`" annotation warning no longer fires on aliases that do reach the edge.** `Annotation.warn_when_the_slot_cannot_reach_the_edge` judged the raw alias against the edge allow-list, but the clean-phase coercions rename statistical aliases to their canonical slot before any relocation runs. `adjusted p value` — which reaches the edge as the real Biolink `float` slot `adjusted_p_value` — was reported as folded away, and so were `odds ratio` and `q_value`. The validator now resolves the name through the same classifiers the pipeline uses and judges the *coerced* target; aliases of slots that genuinely cannot reach the edge name that target instead of the alias (e.g. `sample size` → `supporting_study_size`).

## 10.0.0 - 2026-08-13

A major bump for one removal: `method: list` is gone, and a table config that still declares it no longer validates. A `~=9.1` pin cannot cross that silently. The other change in the release is purely additive.

### Breaking Changes
- **`method: list` is removed; use `split_by` instead.** The literal-list encoding — `method: list` with a list `encoding`, added in 9.0.0 as the successor of `Annotation.delimiter` — is gone. Every edge carried the *same* array (the list is fixed at config time), which is exactly the one shape `split_by` subsumes in practice: a multivalued annotation is declared on a column whose cells hold delimited text, and each row splits into its own JSON array. Multivalued annotations now have exactly one encoding:

  ```yaml
  annotations:
    - {annotation: has_evidence, method: column, encoding: D, split_by: "|"}
  ```

  Configs still declaring `method: list` fail at validation with the new `encoding-list-method-removed` code and a pointer to `split_by`, instead of a bare enum error — including under `tablassert agent`, whose error-recovery loop reads coded errors verbatim. The `encoding` field is scalar-only now (`str | int | float`); a list value is rejected by the schema.

  **Accepted caveat:** an array known at config time (identical on every edge) has no literal form anymore — materialize it as a source column (the same delimited value per row) and split it with `split_by`, or record it once as graph-level metadata. This trades a rare literal shape for one multivalued encoding instead of two that overlapped.

### Added
- **`nullable` qualifiers — optional per-edge qualifiers without edge loss.** A `method: column` qualifier may now declare `nullable: true` so that a blank or unresolvable cell **keeps the edge and omits the qualifier for that row**, instead of dropping the edge:

  ```yaml
  qualifiers:
    - {qualifier: disease_context_qualifier, method: column, encoding: F, nullable: true}
  ```

  A qualifier is a node encoding resolved through the fullmap alongside subject/object, and `join_matches` drops any row whose resolved node column is null — the right default for subject/object (an edge with a missing node is meaningless) but a hard constraint on qualifiers: a declared qualifier had to resolve on **every** row or the edge was lost. That forbade per-edge *optional* qualifiers, which downstream builders worked around by only declaring qualifiers backed by dense columns and dropping otherwise-usable context.

  `nullable: true` threads a `drop_unresolved=False` flag through `join_matches`/`resolve_batch` for that column alone: the row survives with a null qualifier that the existing null-stripper omits from the edge, subject/object stay strict, and the miss is still reported by `log_unmatched`. QC `fullmap_audit` skips nullable qualifier columns (their nulls are expected, not resolution errors to delete). `nullable` on a literal qualifier (`method: value`) is rejected at config time with `qualifier-nullable-literal`, since a config-time constant can never be null.

  Purely additive: existing configs are unaffected, and the default (`false`) is byte-for-byte the previous behavior.

## 9.1.0 - 2026-08-12

### Added
- **`tablassert build-fullmap` now downloads a prebuilt database by default, with `--force` / `-f` to rebuild from scratch.** Without `--force`, the command first fetches the prebuilt `fullmap.tar.zst` published for the INSTALLED Tablassert version at `https://stars.renci.org/var/babel_outputs/<babel-version>/fullmap/<tablassert-version>/` (the version directory is derived from installed-package metadata, never hardcoded), verifies it against the co-published `sha256sum.txt`, and stream-extracts it beside `--output` — far faster than building from BABEL. If no prebuilt exists for this version (or the download/extract fails), it falls back to the existing from-scratch BABEL build and logs a warning; `--force` / `-f` skips the prebuilt attempt entirely. A database already present at `--output` is reused. Extraction uses Python 3.14+ native `tarfile` zstd, falling back to the installed `zstd` binary on older interpreters. The `--aria2c` / `-a` flag accelerates the prebuilt download through the same shared downloader the BABEL build uses (so it benefits from the optional bundled `[aria2]` extra when that is installed).
- **Missing optional extras now name themselves and the command that installs them.** Reaching a feature whose extra was never installed used to surface as whatever the import happened to throw — most often a bare `ModuleNotFoundError: No module named 'sklearn'`, which does not tell anyone that `tablassert[qc]` is the fix, or that `sklearn` is installed as `scikit-learn`. Every one of those paths now reports both:

  ```text
  Missing optional dependencies 'scikit-learn', 'sentence-transformers' — required by the QC audit.
  Install the [qc] extra: pip install "tablassert[qc]" (uv: uv tool install "tablassert[qc]")
  ```

  Where the gap is knowable before the work starts, it is now reported before the work starts:

  | Command | Checked | When |
  |---|---|---|
  | `build-kg --qc` | `[qc]` | Before the build starts |
  | `tablassert agent` | `[agent]` | After flag validation, before any model is built or article fetched |
  | `tablassert agent --optimize` | `[agent]` + `[optimize]` | Same point; both reported at once |
  | `build-fullmap --aria2c` | `[aria2]` | Before the first download, rather than on it (BABEL URL discovery already hit the network by then). Keeps the platform-aware message: on macOS, where the `aria2` distribution publishes no wheels, it still says to drop the flag rather than to install a dead end |

  `build-kg --qc` is the one that mattered most. The QC audit is the pipeline's LAST stage, so a missing extra was discovered only after entity resolution had already finished — the whole build spent, then a traceback. The check now costs one `importlib.util.find_spec` probe (no import, so nothing is paid for having the extra) and happens before stage 1.

  A half-installed extra reports every package it is still missing rather than one per attempt. That was a real failure mode in `[qc]`: `scikit-learn` is imported at the start of the audit and `sentence-transformers` only if Stage 3 is reached, so installing the first would have run the audit again just to fail on the second.

  Flag and secret validation still comes first — `tablassert agent` reports a missing `--model-id` before a missing extra, since fixing an install only to be told the model id was never set is the worse loop.

  New `tablassert.extras` module holds the package → extra mapping as the single source of truth, and a test asserts it against `pyproject.toml`, so an extra cannot be added or a dependency moved without the hints following.

- **`split_by` on annotations — per-row multivalued slots.** A `method: column` annotation may declare a separator that splits each cell's own delimited text into a real JSON array:

  ```yaml
  annotations:
    - {annotation: has_evidence, method: column, encoding: D, split_by: "|"}
  ```

  This closes the gap 8.2.1 opened. Removing `Annotation.delimiter` left `method: list` as the only multivalued encoding, but a list `encoding` is a literal — it emits the same array on every row — so a column whose cells hold aggregated values had no migration at all. `split_by` is the per-row counterpart: `method: list` for an array known at config time, `split_by` for one that differs per row.

  The failure it prevents is silent rather than loud. `mask_illegal_edge_fields` wraps a scalar bound for a multivalued slot into a one-element list, so a joined cell emits `has_evidence: ["EFO:0001|EFO:0002"]` — structurally valid Biolink that passes `validate-kgx` while handing consumers one unusable blob instead of two ids.

  `split_by` requires `method: column` (`annotation-split-by-requires-column`) and rejects an empty separator (`annotation-split-by-empty`), which would split into individual characters. It is unrelated to the `source.delimiter` CSV/TSV field separator, and it is annotation-only — subject/object/qualifier nodes are single entities.

  Purely additive: existing configs are unaffected, and upgrading from 8.2.x with column-based `delimiter` configs is now a rename to `split_by`.

### Changed
- **`explode_by` and `split_by` are now one splitting primitive.** `explode_by` was already "split a delimited cell, then fan the items out into rows"; `split_by` is the same split without the fan-out. Both now route through a shared `split_expr`, so a delimited cell is parsed identically whether it feeds a node encoding or an annotation, and the parsing rules live in exactly one place:

  | | Destination | Use for |
  | --- | --- | --- |
  | `explode_by` | one **row** per item | node encodings — each item is its own entity, its own edge |
  | `split_by` | one **array** on the row | annotations — the items are one multivalued slot on a single edge |

  This tightens `explode_by`: items are now trimmed and blanks dropped, so a trailing or doubled separator (`"P1;P2;"`, `"P1;;P2"` — routine in hand-maintained spreadsheets) no longer fans out a row carrying `""`. Those rows only ever failed entity resolution and were discarded downstream, so no edge changes; the work is simply not done. Trimming is likewise not a behavior change for nodes — `level_one` already strips before resolution — but it is load-bearing for annotations, which are never resolved and previously would have carried `" b"` straight onto the edge.
- **`tablassert build-fullmap --aria2c` / `-a` now uses the optional `[aria2]` PyPI extra instead of a system `aria2c` install.** Install with `pip install "tablassert[aria2]"` to get the bundled static aria2c binary from the `aria2` package (`aria2==0.0.1b0`, imported as `aria2c`). The extra has Linux/Windows wheels only; on macOS, `--aria2c` fails loud and the default Python downloader remains available. `aria2` is a separate optional GPL-2.0 runtime dependency; Tablassert remains Apache-2.0, but redistributors who ship the optional extra should review GPL-2.0 obligations.

### Fixed
- **Suggested install commands now quote the extra: `pip install "tablassert[qc]"`.** Unquoted brackets are a glob pattern in zsh — the default shell on macOS — so every `pip install tablassert[agent]` this project printed or documented failed with `zsh: no matches found` before pip was ever reached. A suggestion the user's own shell rejects is worse than none. `agent.AGENT_EXTRA` and `agent.OPTIMIZE_EXTRA` carry the quoted form and are now derived from the registry rather than written out by hand.
- **The Excel error messages no longer recommend an extra that has never contained an Excel engine.** An unreadable workbook told users to `install tablassert[agent] or tablassert[rt]`. Neither extra ships an Excel engine, and the calamine engine (`fastexcel`) became a core dependency, so the advice both misdirected and described an install the user already had. The message now says what is actually true — calamine ships with the base install, so a failure is usually the workbook itself — and points at `pip install openpyxl` for the pure-Python fallback engine. `docs/installation.md` carried the same drift (`pip install python-calamine`) and is corrected.
- **`agent --backend litellm` names the `[agent]` extra when `litellm` is absent.** The path only required `smolagents`, leaving smolagents' own import error to explain a Tablassert extra.
- **A `polars` import failure now points at the `[rt]` extra.** polars is a core dependency, so it is never merely absent — the realistic failure is a wheel whose instruction set the CPU does not support, which is exactly what `polars[rtcompat]` (the `[rt]` extra) exists to fix. The extra cannot be detected by inspection (it imports as plain `polars`), so this hint is the only place a user learns it exists.

### Documentation
- **Issue and pull-request templates.** `.github/ISSUE_TEMPLATE/bug_report.md` asks for the parts of a Tablassert report that are otherwise missing on the first round-trip — the exact command, expected versus actual behavior, and the OS/Python/Rust environment — and `feature_request.md` asks a proposal to state its problem and success metrics before its implementation. `.github/pull_request_template.md` asks for the validation commands actually run and their results, rather than an unqualified "tests pass".
- `llms.txt` listed the optional extras as `rt` and `qc` only, predating `aria2`, `agent`, and `optimize`; all five are now named in both places the file lists them, and the new `tablassert.extras` registry is on the implementation map.
- The `--version` example in `docs/cli.md` had been left at `tablassert 8.2.0`.

## 9.0.0 - 2026-08-11

**Identical in content to 8.2.1 — re-released under a major version to correct the version signal.**

8.2.1 shipped as a patch release but carried two breaking configuration changes: `source.url` became a list (`url: list[HttpUrl]`, no longer accepting the scalar form) and `Annotation.delimiter` was removed in favor of `method: list`. A patch bump advertises a drop-in upgrade, so a downstream pinned to `~=8.2.0` or `>=8.2,<8.3` would have picked 8.2.1 up automatically and broken on its existing table configs.

9.0.0 contains no code, test, or documentation changes over 8.2.1 — only the version bump. It exists so the breaking changes are announced by the version number itself, and so a compatible-release pin cannot cross them silently. 8.2.1 remains on PyPI and is unaffected; upgrade from any 8.x directly to 9.0.0 and apply the migrations described under 8.2.1's **Breaking Changes** below.

## 8.2.1 - 2026-08-11

### Breaking Changes
- **`source.url` is now a list of URLs (`url: list[HttpUrl]`).** A table-config section may declare one or more remote source URLs, all recorded as provenance (emitted in the edge `source_record_urls` list and the RIG). The legacy scalar form `url: https://example.com/x.tsv` is no longer accepted — wrap it in a list. Update existing configs from `url: https://...` to a sequence:

  ```yaml
  source:
    url:
      - https://example.com/data.tsv
  ```

- **`Annotation.delimiter` is removed; use `method: list` instead.** Multivalued annotations no longer split an encoded scalar on a separator — declare the list directly with the new `method: list` (see Added). Update `annotations: [{annotation: has_evidence, method: value, encoding: "a|b", delimiter: "|"}]` to `annotations: [{annotation: has_evidence, method: list, encoding: ["a", "b"]}]`. The `source.delimiter` CSV/TSV separator is unrelated and unchanged.

  **`method: list` only replaces the literal (`method: value`) form of `delimiter`.** Because a list `encoding` is a literal, the same array is emitted on every row, so a *column-based* annotation that relied on `delimiter` to split each cell's own value (`{annotation: has_evidence, method: column, encoding: D, delimiter: "|"}`) has no direct migration. `explode_by` is not a substitute: it fans one row out into many rows rather than emitting a per-row JSON array. Such configs must either pre-split the column upstream (emit one already-delimited source column per value, or reshape the source so each row carries a single value) or stay on 8.2.x until per-column list support ships.

### Added
- **`tablassert build-fullmap --aria2c` / `-a`** opt-in downloader acceleration. When requested, the BABEL download stage uses the installed `aria2c` executable with segmented HTTP downloads plus resume/retry flags (`--continue=true`, `--max-tries`, `--retry-wait`) while keeping the existing Python downloader as the default. Missing or failing `aria2c` fails loud instead of silently falling back, and aria2 `.aria2` control files are preserved so interrupted downloads can resume on rerun.
- **`method: list` annotation encoding.** A new encoding method — the multivalued counterpart of `method: value` — lets an annotation carry a manually-defined list of values, emitted verbatim as a real JSON array for multivalued Biolink slots such as `has_evidence`. It replaces the removed `Annotation.delimiter` (which split an encoded scalar). `method: list` is annotation-only: subject/object/qualifier nodes are single entities and reject it at config time, and it is incompatible with the scalar string ops (`regex`/`remove`/`prefix`/`suffix`/`transformations`/`fill`/`explode_by`).
- **The autonomous agent now measures and optimizes the Biolink validity of its own output.** 8.2.0 rebuilt the emit path so KGX validates against the Biolink Model, but the agent — the component that authors configs unsupervised — was never retrofitted: `build_and_audit` wrote its NDJSON, counted the lines, and returned, so a run could converge on, persist, and register a config whose output validated at 0%. It now constructs every emitted record as its own Biolink class (the same check `validate-kgx` runs) and reports four new fields:
  - `biolink_valid_pct` — the pass rate excluding known-pending fields; the number the objective function scores.
  - `biolink_valid_pct_strict` — the unexempted rate, so the pending gap stays visible.
  - `biolink_problems` — the top `"field: error-type"` failures with counts, so the model can self-correct.
  - `demoted_edge_pct` — the fraction of edges that fell back to bare `biolink:Association`. This is the **predicate** signal: a predicate its association class forbids is never an error, it silently discards the class and every qualifier and evidence slot the class declared, and nothing else surfaces it.
- **`tablassert agent --biolink-threshold`** gates `MAPPED` on that pass rate. Defaults to `0.0` (report only), so terminal statuses are unchanged unless you opt in; `biolink_valid_pct` / `demoted_edge_pct` are recorded on every `state.json` record either way.
- **A generated legal-predicate cheat-sheet in the agent prompt.** The agent's only vocabulary channel was the ~30 KB `Section.model_json_schema()` enum dump — 247 predicates and 159 categories with nothing tying the two together. The prompt now carries a compact predicate↔class table for the pairs the agent meets in practice, rendered at import from the installed `biolink-model`, so it cannot drift from the model the build validates against.
- **`biolink.legal_predicates()` and `lib.predicate_options()`** — the missing authoring-time helpers. `predicate_options("Gene", "Disease")` returns `{affects, associated_with, contributes_to}`; there was previously no way to ask which predicates a subject/object pair may carry without composing three private functions.
- **`biolink.KNOWN_PENDING_EDGE_FIELDS`** — the curated extras Tablassert emits deliberately that the pinned model does not declare (`effect_size` / `effect_type` pending [biolink-model#1774](https://github.com/biolink/biolink-model/pull/1774), plus the KGX denormalized carryovers). `validate_kgx` now reports `valid_excluding_pending` / `ok_excluding_pending` alongside the strict counts, so a deliberate gap is not scored as a modelling error. Derived from the installed package, so it empties itself as the model catches up.
- **A `BiolinkRelocationWarning` on annotations whose values cannot reach the edge.** An annotation named `supporting_study_size` or `sample_size` is routed onto the inlined `StudyResult`; one like `q_value` is folded into `supporting_text`. Both were silent. This is a warning, not an error: nothing is lost and every existing config keeps building.
- New regression tests: three `sig()` rigor tests (raw-`p_value`-over-adjusted preference, canonical-column-wins-over-alias, and `pvalue_target`-based exclusion of a look-alike substring column) plus an end-to-end smoke proving the real pipeline normalizes raw statistical annotation names (`p value`, `sample size`, `odds ratio`, `effect type`) to canonical Biolink edge fields and routes `supporting_study_size` / `statistical_significance_qualifier` into the inlined Study.

### Fixed
- **`map_coverage` no longer resolves enum-ranged qualifiers the build deliberately skips.** `lib.Tcode._node_ops` excludes them (their vocabulary wants the token `increased`, not a CURIE), but the agent's coverage measurement sent every qualifier through the fullmap — counting terms the build never looks up, depressing `overall` for a column working exactly as designed, and potentially flipping a good config to `SKIPPED`.
- **The final-answer gates no longer swallow the coded error text.** `validate_section` / `validate_table_config` keep their boolean contract, but the reason is now available via the new `section_error()` / `table_config_error()`, and `derive_config` returns it to the agent instead of forwarding an invalid config — so the model finally sees the actionable messages (`qualifier-unsatisfiable`: use a concrete subtype; `qualifier-bad-value`: here is the permitted vocabulary) those errors were written to carry.
- **The improve loop no longer trades Biolink validity for coverage.** A candidate is accepted only when it regresses on neither axis and improves on at least one. Still monotonic.
- **`validate-kgx` no longer passes on a file it never read.** A missing or misspelled path yielded `total=0, valid=0`, kept `ok` true, and exited 0 reporting "KGX output is Biolink-compliant" — a false pass in CI.
- **`examples/agent/optimized_instructions.yaml` no longer recommends `gene_associated_with_condition`** for gene~disease tables. `GeneToDiseaseAssociation` forbids it, which is exactly the 723,595-edge failure 8.2.0's Biolink fix measured; post-fix it no longer errors, it silently demotes. The stats-annotation guidance was likewise half-updated and listed `sample_size` and other names that fold into `supporting_text`. The next `--optimize` run reseeds from the corrected built-in `INSTRUCTIONS`.
- **`examples/agent/qc/qc_report.py` no longer flags the correct predicate as wrong.** Its `GENERIC_PREDICATES` set marked `associated_with` a "generic fallback" — but that is one of only three predicates `GeneToDiseaseAssociation` permits. It now asks the model whether the predicate demotes the edge instead of matching on spelling.

### Changed
- **`quality_score` reweighted** to coverage 0.40, Biolink validity 0.25, node/edge F1 0.15, QC 0.10, schema validity 0.10 (still a hard gate). Most of the new weight came out of `w_qc`, which scores `build_and_audit`'s structurally-constant `qc_pass_rate`. GEPA's feedback string now carries `biolink_problems` and `demoted_edge_pct`, and the judge rubric gained a `biolink_validity` dimension.
- **`sig()` now applies the same fuzzy-matching rigor as the column coercions.** The `statistical_significance_qualifier` is derived from a p-value column chosen by `pvalue_target()` — the same delimiter-anchored classifier `coerce_pvalue_columns` uses — rather than a naive substring. A raw `p_value` column is preferred over `adjusted_p_value`, and an existing canonical column always wins over a higher-scoring spaced alias, mirroring the selection rule every other `coerce_*` step already uses. The five-band cascade and the Biolink class rule (qualifier omitted when no p-value column is present) are unchanged. No realistic build is affected: `coerce_pvalue_columns` canonicalizes every p-value column before `sig` runs, so the old and new selectors pick the same column; the lone divergence is a contrived column that merely contains a `p_value` substring but is not a real p-value column, which now correctly omits the qualifier instead of deriving a bogus one.

### Documentation
- `docs/agent.md` gains a **Biolink validity** section; its "NCATS Translator-compliant KGX" claim is now verified by the loop rather than asserted.
- `docs/configuration/table.md` was left stale by 8.2.0's Biolink fix: it recommended `supporting_study_size` without noting the reroute, and still said `extracted_from_row_number` folds into `supporting_text`. Both corrected.
- **Documented automatic column coercion** in the table-configuration reference: p-value / study-size / effect-size / effect-type columns are auto-normalized to canonical Biolink names before the edge allow-list sweep (with `effect_type` values mapped to the `EffectTypes` enum, unmatched values dropped to `null`, and the legacy `relationship_strength` renamed forward to `effect_size`), `statistical_significance_qualifier` is auto-derived into five significance bands, and the Biolink class rules that null `effect_type` where `effect_size` is absent and omit the qualifier without a p-value column.

## 8.2.0 - 2026-08-10

### Breaking Changes
- **`dspy` moved out of the `[agent]` extra into a new `[optimize]` extra.** `dspy` is used ONLY by the GEPA prompt-optimization path (`agent --optimize`); ordinary agent runs never import it. Installs that use `--optimize` must now install `pip install "tablassert[agent,optimize]"` (or add `tablassert[optimize]`); the missing-package error now points at `tablassert[optimize]` accordingly. Installs that never run `--optimize` get a lighter `[agent]` install (no `dspy`).
- **Fullmap databases built by older releases must be rebuilt.** The Rust extension upgraded its embedded database engine from redb 2.6 to redb 4.1, and redb ≥ 3 dropped the old v2 file format. Existing `fullmap.redb` (and sibling `fullmap.s*.redb`) files fail to open with `fullmap DB is outdated or needs repair; rebuild with 'tablassert build-fullmap'`. Run `tablassert build-fullmap` once after upgrading. BABEL downloads stay cached, but the command rebuilds the fullmap files. The on-disk fullmap schema is now `tablassert.fullmap.v5` (the table layout is unchanged; the bump makes the redb-4 rebuild explicit and lets an older extension reject new files loudly).

### Changed
- **Fullmap reads no longer serialize across processes.** The lookup path (`lookup_fullmap_terms` and the `hydrate_*` helpers) now opens the fullmap redb files READ-ONLY with a SHARED file lock (redb ≥ 3 `ReadOnlyDatabase`) instead of an exclusive lock: concurrent readers — the agent supervisor, its code-executor subprocesses, and parallel `agent run` processes — no longer contend on the fullmap lock ("Database already open"); only a running `build-fullmap` rebuild can briefly block readers. Read-only opens also never touch the file mtime, making the mtime-keyed Python lookup caches fully stable. The redb 4.1 upgrade additionally speeds up multi-threaded shard reads (~15% on upstream benchmarks) and the fullmap build's redb write phase (~1.5x on upstream write benchmarks).

### Performance
- **The fullmap build's redb engine advanced to the 4.2-to-be (pinned `cberner/redb` master, rev `a35e7cc`) and its Phase-4 RECORDS write now appends through an end-of-table cursor.** Two independent wins, both measured on this repo's build path:
  - **Ascending-key insert page optimization** (automatic in the new engine): an insert past a table's last key now starts a new leaf page instead of splitting the full one and leaving dead free space, so a key-order-loaded table occupies about half as many pages. Measured effect: the 16 RECORDS shard files shrink **~50%** on identical input (a 539 MB shard set rebuilds to 270 MB), which also makes cold lookups faster (half the bytes to page in) and halves the DB's disk footprint. The file format is unchanged (still redb v3), so this is a drop-in engine swap — existing `tablassert.fullmap.v5` databases keep opening and no rebuild is required; a redb-4.1 extension also reads files the new engine writes (and vice versa).
  - **Experimental cursor bulk inserts** (`experimental_cursor`): `write_shard_records` now opens one `upper_bound_mut(Unbounded)` cursor per shard batch and appends the hash-sorted merged groups via `insert_before`, which redb documents as ~3× faster than per-key `insert()` for ascending data. An internally-controlled microbenchmark (2 M ascending fixed-size records, both paths in one process) measured the cursor path at **~4× the insert throughput** of plain `insert()` (0.22 s vs 0.89 s) at identical file size. The cursor requires strictly ascending keys and never overwrites, so the rare duplicate xxh64 hash (two distinct terms colliding) falls back to a plain `insert()` for that one record — preserving the historical overwrite semantics — and reopens the cursor; covered by a dedicated regression test. When redb 4.2.0 publishes on crates.io the git pin swaps back to `redb = "4.2"`.
- **BABEL gzip decompression switched from flate2's default miniz_oxide backend to `zlib-rs`** (`flate2 = { features = ["zlib-rs"], default-features = false }`). The build's producer threads decompress ~30–40 GB of BABEL `.gz`; on a real 46 MB BABEL synonym stream zlib-rs decompressed at ~4.6 GB/s vs miniz_oxide's ~3.0 GB/s (**~1.5× faster**), and it is pure Rust with runtime SIMD multiversioning, so shipped wheels need no C toolchain.

## 8.1.0 - 2026-08-03

### Breaking Changes
- **Removed the `TABLASSERT_FULLMAP_SHARDS` environment variable.** The number of on-disk RECORDS shard files is now fixed at the compile-time cap (`16`) and can no longer be overridden at build time; the variable is silently ignored if set. Default builds are unaffected — the previous default was already `16`. The read path still honors the shard count recorded in an existing database's `meta` table (`shards`), so databases built with fewer shards under the old variable continue to open and resolve correctly. See `docs/fullmap.md`.
- **Removed the `build-kg --table-config`/`-tc` flag** (and the `build-kg --fullmap`/`-fm` flag, which existed solely to feed it). `build-kg` now always takes a graph YAML; the throwaway `TEMP_KG` wrapper for building a bare table (Section) config is gone. To build a single table config, wrap it in a graph config — `tablassert agent` already writes a ready `graph.yaml` alongside each build under `builds/<pmc_id>/`. (`validate --schema table` still checks a bare table config on its own.)
- **Renamed `build-kg`'s configuration-file parameter** from `configuration_file` to `graph_configuration_file` to reflect that it is always a graph config. The CLI flags are unchanged (`--configuration-file`/`-f` and positional); only the positional metavar (`GRAPH-CONFIGURATION-FILE`) and the Python / bound-argument name change.
- **`validate` no longer auto-detects the config kind.** The YAML-sniffing heuristic (a top-level `tables` key ⇒ graph, otherwise table) is removed; `validate` now requires an explicit `--schema {graph,table}`/`-s` flag selecting which schema to validate against. `tablassert validate foo.yaml` now fails without `--schema`; use `--schema graph` (validates the `Graph` model and every referenced table) or `--schema table` (validates section syntax only).

### Added
- **The autonomous `tablassert agent` pipeline grew from one-section-per-paper to a richer, still-deterministic supervisor:** multi-section configs, local (non-open-access) payloads, first-class GEPA prompt optimization, and opt-in reflexion/judge gates.
  - **Multi-section configs:** a derived config is now `{template, sections}` — `template` carries only shared provenance (`repo` + `publication`, no `source`); each entry in `sections` owns its own `source` (`local` path and `source.url`, plus `sheet`/`row_slice`/`delimiter`) and `statement`, so one paper maps every supplementary table/worksheet. `validate_table_config` (the final-answer gate) validates **every** section; `map_coverage` measures each section and reports an aggregate (`overall` mean, `min` weakest, `measured`) plus a per-section breakdown; `propose_config_edit` edits each section from its own coverage entry.
  - **Local payloads (`--local`/`-l`):** runs the same derive/build/improve pipeline on an article held locally (e.g. a non-open-access paper) and skips the PMC-AWS fetch. Accepts one `DIR` applied to every id or per-article `PMCid=DIR` mappings; a missing directory fails loud (exit 2).
  - **GEPA prompt optimization (`--optimize`/`-o`):** a first-class path that runs `dspy.GEPA` with a real reflection LM and **persists** the optimized instructions (`--instructions-out`, default `<state-dir>/optimized_instructions.yaml`; reload via `--instructions-file`) instead of running the supervisor. Budget via `--max-metric-calls` (default `8`); example datasets via `--dataset` (YAML/JSON `{table_summary, coverage_feedback}`).
  - **Reflexion improver (`--reflexion`):** an opt-in tier-2 LLM reflexion improver for edits that may change predicate/source when the deterministic proposer stalls.
  - **Semantic judge (`--judge-model`/`--judge-threshold`):** an opt-in judge scores the output; when set, `MAPPED` additionally requires the normalized score to clear `--judge-threshold` (default `0.5`). Without it, the coverage gate alone decides.
  - **PDF context:** `pdfminer.six` (new `[agent]` dependency) extracts a `.pdf` main text into data-fenced context, so PDF-only articles still give the agent main-text context.
  - **`BUILT_UNMEASURED`:** a new terminal **non-failure** outcome for a config that builds but whose fullmap coverage can't be measured (unreproducible source frame) — neither `MAPPED` nor `SKIPPED`; the best config is still written and reusable.
- **GEPA optimization now works end-to-end against real dspy and scores the genuine objective.** `gepa_metric` satisfies `dspy.GEPA`'s 5-arg metric contract (`gold, pred, trace, pred_name, pred_trace`) while keeping the legacy single-bundle call; when a dataset example carries a `fullmap` (+ optional `workdir`/`head`), the metric scores each proposed config with **real fullmap coverage** (a `build_and_audit` head-sample) instead of a validity-only proxy. New knobs: `--task-model` splits a fast task LM from the strong reflection LM, and `--gepa-threads` parallelizes LM forward passes (builds stay serialized on an internal build lock). `make_dspy_lm` adds reasoning-model-safe defaults (`max_tokens=16000`, `temperature=1.0`) and a 600 s request timeout so a truncated/stalled call can't hang the optimizer.
- **`derive_mode` (`full` | `derive_only` | `derive_coverage`) on `make_tools`/`run_supervisor`** for scalable config derivation — derive-and-validate, or derive-plus-coverage-measurement, without running the full loop.
- **Automated LLM-QC loop for derived agent configs** (example tooling under `examples/agent/qc/`): `qc_report.py` builds a deterministic per-PMC QC report (derived config + predicate/encodings/provenance + KG node/edge counts + sample edges + aggregate metrics), and `qc_reviewer.py` runs an LLM-as-judge that critiques each derived config + table + KG sample across `predicate_appropriateness` / `encoding_correctness` / `provenance` / `coverage` / `other_mistakes` and proposes one prompt improvement per PMC. Reports and reviews (`QC_REPORT.md`, `QC_REVIEW.md`) are regenerated together from the same state dir and each entry carries a shared config sha256 so report/review/config drift is detectable. Two rounds of this loop hardened the shipped derivation prompt (see Changed).
- **The test suite now runs parallel by default:** `pytest-xdist` joined the dev group and `-n auto` is in `addopts` (~7× faster; disable for one serial run with `pytest -n 0`). Coverage is identical.
- New regression tests: multi-section agent configs (`tests/test_agent_multisection.py`), example-QC guards (`tests/test_example_qc.py` — downloads-dir allowlist, column derivation, judge-JSON schema validation, path redaction, committed-artifact yaml-fence parseability + shared sha256), CLI parser locks proving the removed `build-kg --table-config`/`--fullmap` options stay gone and `validate` requires `--schema`, GEPA regression tests (5-arg contract, real-coverage metric, task-LM split, `num_threads`, `make_dspy_lm` defaults, no model built before `--gepa-threads` validation), and a public-build test proving `TABLASSERT_FULLMAP_SHARDS` is ignored end-to-end.

### Changed
- **The agent's derivation prompt was hardened through the iterative LLM-QC loop.** Added a Quality-principles section addressing the recurring mistakes the reviewer surfaced (over-interpretation, hard-coded objects, dropped statistical annotations, wrong object columns, generic predicates): pick the most-specific valid biolink predicate (`gene_associated_with_condition`, `correlated_with`, `expressed_in`, `biomarker_for`, `has_sequence_variant`, `affects`, …), falling back to `associated_with`/`related_to` only when nothing specific fits; add `prioritize` only when confident, otherwise omit it (a wrong prioritize is worse than none); verify each table/worksheet via `read_table` before authoring a section; use exact case/space-sensitive worksheet names and only candidate-table absolute paths; parse `build_and_audit`/`map_coverage` JSON output with `yaml.safe_load`, never `import json`. QC assay: 10/10 diverse PMCs `MAPPED` first-attempt (mean best coverage 0.974, 8/10 specific predicates); the two weakest PMCs improved poor→acceptable in round 2.
- **Pre-commit hooks trimmed to the fast, auto-fixing ones** (`ruff --fix`, `ruff-format`, `cargo-fmt`). The slow gates that used to run on every commit (pyright, the full pytest suite with a Rust-extension rebuild, cargo-clippy, cargo-test) are owned by CI on every PR; `pre-commit run --all-files` drops from ~2.5+ min to ~0.3 s.
- **QC reviewer/report hardening:** the judge's JSON is schema-validated (dimension mappings, score 0–3, quality enum) before rendering; failed entries carry the config sha256; the table summary derives `max_cols` from the configured subject/object/annotation columns and accumulates all sections; every review string is sanitized via `redact_paths` (generalized to any absolute local path, URL/ratio-safe) before JSON+markdown; `--rerender` rebuilds markdown from `qc_review.json` without re-querying the judge; the config fence cap rose 3000 → 8000 so no YAML block truncates mid-mapping; the reviewer runs with an untrusted-data system message, a downloads-dir allowlist for `source.local`, and a bounded litellm timeout.
- CI now caches uv dependencies on every `setup-uv` step, runs the test step with explicit `-n auto`, and publishes docs with `peaceiris/actions-gh-pages` v4 (Node 20; v3 ran on the deprecated Node 16 runtime).

### Fixed
- **Corrected the BioBERT Hugging Face repo id.** `pritamdeka/BioBERT-mnli-snli-scitail-mednli-stsb` no longer exists (HF API 401); the real sentence-transformers repo is `pritamdeka/BioBERT-mnli-snli-scinli-scitail-mednli-stsb`. With the old id, `get_biobert()`'s download path could never succeed for a fresh `.tablassert/biobert/` cache.
- **Agent KG builds are now reliable against real filesystems.** `build_and_audit` resolves its workdir to an absolute path (a relative workdir made `build_pipeline`'s `.tablassert/store` parquets resolve against the wrong base once the build chdir'd → build failure → false 0.0 coverage → every article `SKIPPED`); the supervisor presents absolute table paths so `source.local` resolves in the build workdir; a relative `source.local` is resolved against the build workdir as well as the cwd before a config is declared unmeasurable; the local executor timeout rose 30 s → 600 s so a large-table `build_and_audit` is not killed mid-build (which also stranded the fullmap redb lock); `build_and_audit` retries its coverage measurement on transient failure; fullmap lookups retry on transient redb lock contention (`Database already open`) without re-retrying exhausted contention.
- `agent --gepa-threads < 1` now fails loud (exit 2) **before** any model is built, and `--instructions-out` is resolved to an absolute path (GEPA's parallel builds chdir the process cwd).

### Performance
- **The test suite is ~7× faster:** 622 passed / 27 skipped at 90% coverage in ~22 s with `-n auto`, down from ~153 s serial (identical coverage; serial is one flag away via `pytest -n 0`). CI test runs reuse the uv package cache across runs.

### Documentation
- `docs/cli.md`: completed the SSOT `agent` flag table with all new flags (enforced by the `tests/test_docs_cli_coverage.py` guardrails) and replaced the shell-unsafe `--schema {graph,table}` brace expansion in the `validate` usage block with concrete commands.
- `docs/agent.md`: documented the multi-section config model, `--local`, `--optimize`, a new "Optional gates: reflexion improver & semantic judge" subsection, and the `pdfminer.six` pin.
- README: documented the QC state-dir / downloads allowlist relationship and the `examples/agent/qc/` scripts.
- CONTRIBUTING.md and `docs/installation.md`: synced to the parallel test suite and trimmed pre-commit hooks (correcting the stale serial-suite figures).
- `docs/fullmap.md` and CONTRIBUTING.md: removed the retired `TABLASSERT_FULLMAP_SHARDS` tunable.

## 8.0.1 - 2026-07-30

### Breaking Changes
- **Slimmed the `tablassert agent` CLI surface.** Removed the `--qc-threshold`/`-qt` flag (it was echoed into the run metrics but never gated any decision — the accept/`MAPPED` outcome is driven solely by `--map-threshold` coverage), the `--executor`/`-e` flag and its Docker sandbox option (model-written code now always runs on the in-process `local` executor), and the `--no-fetch`/`-nf` flag (the supervisor always fetches the PMC payload). The `--map-threshold` default was lowered from `0.8` to `0.25`, and the agent workspace default moved from the standalone `.tablassert-agent/` directory to `.tablassert/agent/` (under the shared, already-git-ignored `.tablassert/` parent).
- Renamed CLI commands to a consistent verb-noun scheme and retired the legacy `datassert` naming throughout: `build` → `build-kg` (`validate` and `build-fullmap` are unchanged — they already fit the scheme). The `Graph` config field `datassert` is renamed to `fullmap` (also renaming the `resolve_many()` keyword argument from `datassert` to `fullmap`); existing graph YAML configs must rename their `datassert:` key to `fullmap:`. This is a pure naming migration. `build-fullmap`'s default `--output`/`--cache` directories moved from `./datassert/` to `./fullmap/`. (Note: the embedded fullmap database format was independently changed — see the redb schema item below — so existing `fullmap.redb` files **do** need to be rebuilt with `tablassert build-fullmap`.)
- Removed the `syntax` key entirely from both configuration formats. Graph configs no longer accept `syntax: GC2`/`GC3` and table configs no longer accept `syntax: TC4`; any `syntax:` field is now rejected (`extra = "forbid"`).
- Removed the `status` template-metadata field from table configurations.
- Removed the `provenance.contributors` block from table configurations. `Provenance` now carries only `repo`, `publication`, `knowledge_level`, and `agent_type`; curation-style attribution moves to the graph-level RIG `contributions` field (below).
- Removed `qc` and `log` from the graph configuration. Both are now `build-kg` CLI flags (`--qc/-q` and `--log/-l`); graph configs that set `qc:` or `log:` are rejected.
- Removed `pubmed_db` and `pmc_db` from graph configuration and removed the downstream PubMed metadata and PMC caption enrichment steps from graph builds.
- **Entity resolution was rewritten onto an embedded [redb](https://github.com/cberner/redb) database.** The DuckDB-shard store built by the external `datassert` Go CLI is replaced by an embedded redb database — a primary file (`fullmap.redb`) plus sibling RECORDS shard files (`fullmap.s*.redb`) that must remain co-located for lookup — built in-process by Tablassert's own Rust extension (`tablassert.rs`); no external tool or install step is required. The on-disk fullmap schema is now `tablassert.fullmap.v4` (a primary file plus hash-sharded RECORDS files — see the sharded-fullmap entry under Changed below); v1/v2/v3 databases are rejected at lookup time and must be rebuilt with `tablassert build-fullmap`.
- **The QC runtime migrated from ONNX Runtime to PyTorch / sentence-transformers (BioBERT).** The `qc` extra now installs `torch`, `sentence-transformers`, `rapidfuzz`, `scikit-learn`, and `numpy`. The `qc-cuda` extra and all ONNX/CUDA provider selection were removed; `fullmap_audit()` no longer takes a `provider` argument and runs on the sentence-transformers backend. QC thresholds were tightened: the fuzzy stage now passes on `fuzz.ratio >= 70 OR partial_token_sort_ratio >= 80` (previously 20/30), and the BioBERT cosine-similarity stage on `>= 0.5` (previously 0.2). The cached model moved from `.tablassert/onnx/` to `.tablassert/biobert/`.
- **Removed automatic source-file downloading.** A table's `source.url` is no longer fetched into `source.local`; the input file must already exist at `source.local`. `source.url` is retained as provenance metadata — it is emitted as the edge `source_record_urls` column and recorded in the generated RIG. The parse-time URL-reachability check (`httpx.head`) and its `.cachassert/` disk cache were removed.
- Consolidated the four scattered hidden working directories (`.storassert/`, `.logassert/`, `.onnxassert/`, and the previously-documented-but-removed `.cachassert/`) into a single parent `.tablassert/` with three intuitively-named subdirectories: `.tablassert/store/` (intermediate parquet, `utils.STORE`), `.tablassert/log/` (loguru sink, `log.LOGASSERT`), and `.tablassert/biobert/` (cached BioBERT model, `qc.MODEL`). All three paths are now derived from a single `utils.BASE = Path("./.tablassert")` constant. The loguru sink file was renamed from `logassert.log` to `tablassert.log`. Any external tooling or log tailers pointed at the old paths need to be updated; the on-disk content is auto-regenerated on the next run (parquet is reproducible, the BioBERT model re-downloads if `.tablassert/biobert/` is not pre-populated).
- Removed runtime dependencies that are no longer used: `onnxruntime`, `diskcache`, `httpx`, `pyexcel`, `orjson`, `duckdb`, and `polars_hash`.
- **Knowledge-graph edge IDs are now canonicalized.** Edge IDs (assigned during Rust NDJSON deduplication/labeling) are derived from a key-order-independent, injective encoding: object keys are sorted before hashing and each key/value is fed as a separate length-prefixed part, so the same logical edge always yields the same UUID regardless of JSON field order (field order previously leaked into the ID) and distinct edges cannot collide on the encoding. This changes generated edge IDs relative to earlier builds — regenerate any stored/published graphs; downstream consumers that pin edge IDs must update.

### Added
- **Rust/PyO3 extension (`tablassert.rs`)** replacing the external `datassert` tool. Public functions include `build_fullmap_db`, `lookup_fullmap_terms`, `hydrate_prefixes`/`hydrate_categories`/`hydrate_sources`/`hydrate_curies`, `dedup_ndjson`, `namespace_uuid`, and `fullmap_source_version`. Published as maturin wheels (CPython 3.11–3.14, Linux + macOS) through the PyPI workflow.
- **`tablassert build-fullmap`** now downloads NCATS Translator BABEL class/synonym files from RENCI (resumable, range-request downloads) and builds the embedded fullmap redb database in-process, with a live progress display.
- **Resource Ingest Guide (RIG) generation.** `build-kg` now also emits `<name>_<version>.RIG.yaml` alongside the nodes/edges NDJSON. A new required graph field `description` and optional `contributions` and `ui_explanation` fields populate the RIG (with sensible defaults for `contributions`/`ui_explanation`).
- **`build-kg` flags:** `--release/-r` (emit a slim, significant-only graph by dropping `biolink:not_significant` edges before resolution), `--qc/-q` (run the QC audit stage), and `--log/-l` (verbose per-section logging).
- **`build-fullmap` shorthand flags:** every `build-fullmap` option now has a short alias — `--output/-o`, `--cache/-c`, `--version/-v`, and `--threads/-t` — matching the shorthand style already used by `build-kg`.
- **`build-kg` configuration-file flag:** `build-kg`'s configuration file now also accepts `--configuration-file`/`-f` (positional usage is unchanged), matching `validate`. To free `-f` for the configuration file, `build-kg`'s `--fullmap` short alias moved from `-f` to `-fm` (the `--fullmap` long form is unchanged).
- **Build-time CURIE prefix exclusion:** setting `TABLASSERT_FULLMAP_EXCLUDE_PREFIXES` (comma-separated prefixes, e.g. `INCHIKEY,Publication`) drops matching CURIE rows during the fullmap build. Excluding prefixes you never resolve can cut build time, peak memory, and database size several-fold. The build also skips terms the lookup path can never query (the `distinct()` dead-term filter: purely numeric or generic labels), and exposes memory/speed tunables (`TABLASSERT_FULLMAP_{LOCAL_SPILL,CURIE_SPILL,EQUIV_SPILL}_ENTRIES`, `_INSERT_BATCH`, `_REDB_CACHE_BYTES`, `_SPILL_DIR`); see `docs/fullmap.md`.
- **`TABLASSERT_FULLMAP_SHARDS`** environment variable (default `16`): the number of on-disk RECORDS shard files the fullmap term index is hash-partitioned across (and the number of concurrent redb writers in the write phase). Non-powers-of-two round down to the nearest power of two (6→4, 3→2) and the value is clamped to the compile-time cap (16). The resolved shard count is recorded in the primary file's `meta` table (`shards`) and the read path opens exactly that many shard files; see `docs/fullmap.md`.
- **Structured, coded errors** (`tablassert.errors`): `TablassertError`, `TablassertValidationError`, `GraphValidationError`, `SectionValidationError`, `QcRuntimeMissingError`, and `BabelDownloadError`, each carrying a stable error code that resolves to a documentation URL.
- Experimental Biolink feature support: per-section knowledge-level / agent-type provenance (`provenance.knowledge_level`, `provenance.agent_type`), auto-derived Biolink association edge categories (`EdgeCategories`), and a Biolink-compliant edge-column allow-list. Annotation columns not on the allow-list are folded into the edge `supporting_text` list (as `"name: value"` entries) rather than dropped.
- New edge provenance columns: `upstream_resource_ids` (from the Biolink information-resource mapping) and `source_record_urls` (list column).
- Google-style docstrings on all functions; `pytest-cov` coverage configuration; and an expanded end-to-end build-pipeline test suite.

### Changed
- **The fullmap database is now hash-sharded on disk.** The single redb file is split into a small primary file (`fullmap.redb` — dimensions, `curies`, and `meta`) plus 16 RECORDS shard files by default (`fullmap.s0.redb` … `fullmap.s15.redb`), with each normalized term routed to a shard by `xxh64(term) & 15`. The on-disk schema is now `tablassert.fullmap.v4`; existing `v3` databases are rejected at lookup time and must be rebuilt with `tablassert build-fullmap`. Two phases were parallelized as a result: the RECORDS write now runs `shard_count` independent k-way merges in parallel — the term spill runs are partitioned per-shard at write time (each `run_s{shard}_{id}.bin` holds only terms hashing to that shard), so one thread per shard merges only its own runs and inserts the merged groups inline into that shard's redb file (one database per shard — redb is single-writer per file) with no shared producer or per-shard channel, parallelizing both the merge and the B-tree insert (~N× write throughput); and fullmap lookup now releases the GIL and fans the query terms out across the shards in parallel (one reader per non-empty shard, re-merged into input order). The shard files are discovered as siblings of the primary path and must remain alongside it; see `docs/fullmap.md`.
- The fullmap build is now parallelized with rayon: class/synonym files are processed in parallel through an in-memory sharded pipeline (equivalents map → dimension/CURIE-id assignment → term aggregation → a single write transaction), replacing the old per-row database lookups and the stage-database/temp-file copy. `--threads` now defaults to ~90% of available CPUs when unset (previously effectively single-threaded). The redb stores six tables: `records`, `prefixes`, `categories`, `sources`, `curies`, and `meta`.
- **The fullmap build is now memory-bounded.** The two structures that previously grew with the number of unique CURIEs and drove the synonym phase into swap/OOM on a full BABEL build are no longer held in RAM: the CURIE dedup map is keyed by a 128-bit hash (`xxh3_128(curie) → id`) instead of the CURIE string, and the per-CURIE rows are spilled to bounded on-disk run files (streamed into the `curies` table at write time) instead of a global in-memory vector. Peak RSS now stays flat regardless of input size (measured synonym-phase RSS −45% on an 8 M-CURIE subset, scaling to ~70 GB avoided at full build), and the build is ~18–25% faster (the in-memory CURIE-row sort was removed and the redb write batch enlarged). This was a build-time-only change that did not itself bump the on-disk schema (the schema has since moved to `tablassert.fullmap.v4`; see the sharded-fullmap entry below).
- **The fullmap synonym phase now uses intra-file parallelism.** Previously each synonym file was processed by a single thread, so the few very large BABEL files (protein/smallmolecule/gene/drugchemicalconflated, ~290 GB of the ~300 GB input) ran single-threaded while other cores idled. The phase is now a producer–consumer pipeline: a small pool of producer threads decompresses/reads the files into byte-bounded line-chunks fed through a bounded channel, and all worker threads pull chunks and process rows in parallel — so a giant file is processed by every worker. This is intra-file parallelism only (at the time the output was still a single redb file; the sharded-fullmap entry below later split the RECORDS table across shard files). Measured ~19–25% faster builds on 1–6 GB subsets with identical output (table counts and lookup-equivalence unchanged). The crate also now uses the mimalloc global allocator, which keeps resident memory flat under the heavy multi-threaded allocation (glibc malloc's per-thread arenas otherwise retain freed memory and inflate peak RSS several-fold). New tunables: `TABLASSERT_FULLMAP_CHUNK_BYTES` (default 8 MiB) and `TABLASSERT_FULLMAP_PRODUCERS`.
- TCode operation ordering was optimized to accelerate graph build times.
- The CLI now uses a Rich-based progress interface across all pipeline stages.
- **Section content hashes widened from XXH32 to XXH64.** `utils.mkhash` now calls the extension's new `xxh64` primitive (16 hex chars) instead of `xxh32` (8) for the content-addressed section parquet store (`.tablassert/store/`) and section labels, removing the 32-bit birthday-collision risk at high section counts. User-facing labels still truncate to 8 chars. The first build after upgrading rebuilds the section cache; orphaned 8-char store files are safe to delete.

### Performance
- **The Phase-4 RECORDS write now runs per-shard independent k-way merges in parallel.** The term spill runs are partitioned per-shard *at write time* (one `run_s{shard}_{id}.bin` per non-empty shard, each holding only terms hashing to that shard), so the write phase runs `shard_count` independent k-way merges concurrently — one thread per shard, merging and inserting inline — instead of the prior design's single k-way-merge producer routing merged groups through bounded per-shard channels. This eliminates the single-threaded merge producer that previously capped the build: both the merge and the B-tree insert now scale across shards. Measured Phase-4 write wall on a slow disk (24 worker threads, mean of two runs): the medium subset dropped to ~`8.9s` at `TABLASSERT_FULLMAP_SHARDS=4` from ~`55.9s` at `=1` (~6.3×), and the stress subset to ~`20.0s` from ~`112.6s` (~5.6×). The `=1` single-merge control reproduces the old single-producer cap (~62s medium / ~104s stress), and the `SHARDS=4` wall is ~7× (medium) / ~5× (stress) below that baseline. No schema change (still `tablassert.fullmap.v4`) and no on-disk layout change (primary file plus `fullmap.s0..s<N-1>.redb`); databases built by the prior sharded design remain compatible.

### Fixed
- **Config YAML is parsed with `yaml.CSafeLoader` (safe construction).** Config loading previously used `yaml.CLoader`, which honors arbitrary construction tags (e.g. `!!python/object/apply`) and could execute code from an untrusted table/graph config; it now uses the safe loader with no speed loss (still libyaml-backed).
- `build-kg` now opens Polars NDJSON output streams with explicit UTF-8 encoding, avoiding `InvalidOperationError: file encoding is not UTF-8` during graph compilation on non-UTF-8 locales.
- **Base installs no longer crash at the `build-kg` clean phase with `ModuleNotFoundError: No module named 'rapidfuzz'`.** `rapidfuzz` was previously declared only in the optional `qc` extra, but it is imported by three operations that run on every build regardless of `--qc` — `coerce_pvalue_columns` and `coerce_study_size_columns` (clean phase) and `sig` (significance phase). `rapidfuzz` is now a core dependency. The `qc` extra now contains only `scikit-learn` and `sentence-transformers` (`torch`/`numpy` arrive transitively); installs that already use `tablassert[qc]` are unaffected.

### Removed
- The external `datassert` Go CLI dependency and the sharded DuckDB entity-resolution store.
- The `equivalents` and `term_records` redb staging tables (and the stage-database / temp-file build path).
- `namespace_uuid`, `basespace`, and `samphash` from `tablassert.utils` (`namespace_uuid` is now provided by the Rust extension; `utils` exposes `BASE`, `STORE`, and `mkhash`).
- The Playwright/httpx-based `downloader` module.
- The `.onnxassert/` and `.cachassert/` working directories.

### Documentation
- Comprehensive 8.0.0 documentation overhaul reconciling every surface (README, `llms.txt`, CONTRIBUTING, CITATION, and the full MkDocs site) against the codebase: removed the retired `syntax`/`status`/`contributors` config keys and the ONNX/CUDA QC documentation, documented the redb v4 schema and the new `build-kg` flags and RIG output, and corrected every shipped YAML example so it validates against the current schema.
- Upgraded the documentation site from the plain `readthedocs` theme to Material for MkDocs.
- Removed the Docker documentation (the project no longer publishes a container image).
- Added a CI test that validates the shipped example configurations against the Pydantic models and guards against reintroducing removed 8.0.0 config keys.

## 7.5.2 - 2026-07-01

### Changed
- `sig()` in `lib.py` now emits a third significance label, `"INCONCLUSIVE"`, for p-values that fall between the existing significance `cutoff` (default `0.05`, inclusive) and a new `threshold` parameter (default `0.10`, exclusive). A p-value `p` is now mapped as: null → `"UNSURE"`; `p <= cutoff` → `"YES"`; `cutoff < p < threshold` → `"INCONCLUSIVE"`; `p >= threshold` → `"NO"`. The upper bound is exclusive so `0.10` (and the `0.1` `NO` cases in the existing tests) continue to map to `"NO"`. `threshold` is added to `sig()`'s signature alongside `cutoff`; the function remains wired into `Tcode.collect()` at its default arguments, so builds are unaffected unless a caller overrides the new bound.

### Added
- One regression test in `test_lib.py` (`test_sig_marks_inconclusive_band`) asserting all four bands in a single frame: a value at/below cutoff (`YES`), a value in the inconclusive range (`INCONCLUSIVE`), and a value at the exclusive threshold (`NO`).

## 7.5.1 - 2026-07-01

### Changed
- Numeric annotation columns are now coerced and emitted as controlled-notation strings in NDJSON output instead of raw values. Two new pipeline steps wired into `Tcode.collect()` (`lib.py`): `clean_numeric()` lazily casts matching columns to `Float64` with `strict=False` (non-numeric entries drop to null), and `format_numeric()` renders them as strings — p-value columns (any name containing `"p value"`, case-insensitive) in scientific notation (`{:.4e}`), and `relationship strength` / `sample size` in decimal general format (`{:.4g}`, ≥4 significant figures). Non-matching columns are left untouched, and nulls are subsequently dropped by `strip_nulls()`. `math_op()` now also casts with `strict=False` so it tolerates residual junk in numeric annotation columns. `format_numeric()` formats via numpy-backed batch conversion rather than `map_elements` for throughput.
- Removed dead `pl.Config(set_fmt_float=...)` and `pl.Config(float_precision=...)` context managers from `compile_graph()` (`lib.py`); they were no-ops for NDJSON serialization (`write_ndjson` emits raw f64 via serde shortest-repr and ignores float display options), and the `fmt`/`precision` parameters of `compile_graph()` were removed alongside them.
- Fixed an off-by-one in the build/validate progress bar so each section loop now shows the configuration currently being processed instead of the last-completed one. `PipelineProgress.section_loop()` (`progress.py`) previously returned a single `advance(info)` callback that set the description and ticked the completed counter together, called after each item's work — so while section *K* ran the bar still displayed section *K−1*. It now returns a `(start, advance)` pair: `start(info)` updates the description to the in-flight item without incrementing, and `advance()` ticks the counter afterwards (so the counter never claims an in-flight item is complete). All five call sites in `cli.py` (TCode build, Collect, Subgraph, Graph, Validate) were updated to `start(...)` before the work and `advance()` after; the long-running Collect and Subgraph stages continue to show the full `format_section_oneline()` summary (including the `CONFIG` name) of the in-flight section.

### Added
- Fifteen regression tests in `test_lib.py` covering `numeric_columns()` detection (p-value substring, exact-name match, case-insensitivity), `clean_numeric()` (parse/coerce numeric and scientific notation, null out non-numeric junk, leave non-matching columns untouched, noop, idempotent on Float64), `format_numeric()` (scientific notation for p-value, decimal general format for relationship strength/sample size, null preservation, floating-point-noise cleaning, noop), null-stripped NDJSON rows, `compile_graph()` NDJSON emission, and `sig()` operating over a cleaned Float64 p-value column.
- Three regression tests in `tests/test_progress.py` pinning the new two-callback contract: `start` shows the in-flight item with the counter still at zero, `advance` ticks the counter without altering the description, and a `start`/`advance` cycle keeps the description synced to the current item rather than the previous one.

## 7.5.0 - 2026-07-01

### Changed
- Publication CURIEs in `compile_subgraph()` (`lib.py`) now use the `PMCID:` namespace prefix for PubMed Central sources. A `repo: PMC` section with `publication: PMC11708054` is emitted as `PMCID:PMC11708054` (previously `PMC:PMC11708054`); non-PMC repos such as `PMID` are unaffected and continue to emit `<repo>:<publication>` (e.g., `PMID:11708054`). The `repository` edge column is unchanged and still records the raw `repo` value. Extracted via a new `publication_curie()` helper.

### Added
- New `<col> table literal value` edge column for subject, object, and qualifier nodes encoded with `method: column`. Unlike the existing `original <col>` column (which snapshots the value *after* all `fill`/`explode_by`/`regex`/`remove`/`prefix`/`suffix`/`transformations`), `<col> table literal value` captures the pristine source-cell value *before* any transformation. Emitted only for column-encoded nodes; annotations and `method: value` nodes are unaffected. Implemented via a `table_literal` flag on `Tcode.encoding()`, enabled by `Tcode.node()`.
- Four regression tests in `test_lib.py`: `publication_curie()` for PMC and PMID namespaces, and two `Tcode` tcode-inspection tests covering presence/ordering of the table-literal column for column encodings and its absence for value encodings.

### Documentation
- Comprehensive accuracy pass across the API, configuration, and Docker documentation, reconciling every page against the current codebase. Highlights: corrected invalid examples that would not load (`syntax: TC2`; `publication` integers and missing `PMC` prefixes; a non-existent `Qualifiers` member; `reindex` placed at section level; a subject missing `method: column`), fixed wrong field types (`rows`/`row_slice`/`taxon` → `PositiveInt`, `remove` → regex patterns), corrected the QC fuzzy thresholds (`fuzz.ratio >= 20 OR partial_token_sort_ratio >= 30`), removed a non-existent `uuid:` prefix from `utils.md` return examples, fixed the `resolve_many()` parameter order and added the original-column-capture and optional QC-audit pipeline steps, corrected graph-config path resolution (CWD, not config-relative) and processing-flow ordering, documented the strict QC GPU no-fallback behavior and the `.cachassert/` working directory, and aligned `Categories` enum member names (`GENE`/`PROTEIN`) and Docker CI triggers with the source.

## 7.4.14 - 2026-06-30

### Changes
- Extended `sig()` in `lib.py` to select a p-value column by fuzzy matching rather than requiring an exact `"p value"` name. All schema columns whose names contain the substring `"p value"` are now considered candidates; `fuzz.ratio` (rapidfuzz) scores each against the literal `"p value"` and the highest-scoring column is used to compute the `"significant"` output. An exact `"p value"` column scores 100 and is always preferred; columns like `"adjusted p value"` or `"log p value"` are used only when no exact match is present. If no column contains the substring the function continues to emit `"UNSURE"` for all rows.
- Added five regression tests in `test_lib.py` covering: exact-match preference, non-exact fallback, closest-match selection among multiple non-exact candidates, no-p-value column (UNSURE), and null value handling.

## 7.4.13 - 2026-06-30

### Changes
- Removed the datassert prevalidation failure for unresolved `statement.subject` / `statement.object` literal encodings under `method: value`. Graph builds no longer abort during `Tcode.model_validate(...)` for cases like `"Incertae Sedis XI"`; unresolved literal values are now allowed through config validation so downstream runtime handling can decide whether they map or get filtered.
- Added a regression test at the `Tcode.model_validate(...)` layer covering an unresolved `method: value` subject encoding, matching the build-time validation path reported in the field.

## 7.4.12 - 2026-06-29

### Changes
- Expanded the placeholder-term filter regex in `distinct()` (`fullmap.py`) to drop additional non-informative terms during entity resolution. The `bad` pattern now also excludes `not applicable`, `p value`, `variable`, `result`, `exposure`, `expression`, and `symbol` alongside the existing `none`, `nan`, `na`, `null`, and `unknown`, preventing these generic column-header-like values from being sent through resolution and producing spurious CURIE mappings.

## 7.4.10 - 2026-05-29

### Changes
- Enforced explicit `biolink:` namespace prefix on predicates and qualifiers emitted by `compile_subgraph()` in `lib.py`. Both `self.statement.predicate` and `x.qualifier` (in the qualifier loop) are now prefixed via `add("biolink:", ...)`, ensuring all output edges carry fully-qualified Biolink CURIEs rather than bare predicate/qualifier names.

## 7.4.9 - 2026-05-26

### Bug Fixes
- Fixed `OSError: Too many open files` during subgraph build at large scales (700+ sections). `with_mesh()` and `with_captions()` in `lib.py` opened a `sqlite_utils.Database` per section but never closed the underlying SQLite connection, leaving FD release to GC. In the tight sequential `compile_subgraph` loop the leaked FDs accumulated past the OS soft limit, causing the next `to_store()` → `df.write_parquet()` (which polars 1.39 routes through `sink_parquet`) to fail opening its target `.storassert/*.parquet`. Both functions now wrap their query bodies in `try:` / `finally: db.conn.close()`.

## 7.4.8 - 2026-05-12

### Changes
- Expanded `fullmap_audit()` failure logging in `qc.py` to include the underlying score values that caused each rejection. Failed CURIE log lines now carry `FUZZ_RATIO`, `FUZZ_PARTIAL`, and (when the BERT stage ran) `BERT_SIMILARITY` alongside the existing `STORE`/`CONFIG`/`COL`/`ORIGINAL`/`PREFERRED`/`CURIE` fields, making it easier to diagnose why a term was dropped.
- Attached the per-row fuzzy and BERT scores as columns on the pending frame before masking, and switched the intermediate `pl.concat()` calls to `how="diagonal"` so the score columns survive concatenation with the already-passed rows.

## 7.4.7 - 2026-05-11

### Changes
- Added `Provenance.is_valid_pmc_id` model validator in `models.py` that enforces `publication` starts with `PMC` followed by digits when `repo` is `PMC` (`Repositories.PUBMED_CENTRAL`). The constraint was previously documented in 7.3.6 but only now enforced at parse time.

## 7.4.6 - 2026-05-11

### Changes
- Relaxed `BaseSource.is_real_url` validator in `models.py` to ignore `403 Forbidden` responses from `httpx.head()`. Some upstreams reject anonymous `HEAD` probes with 403 even though the URL itself is well-formed and reachable, so 403 no longer fails config validation.

## 7.4.5 - 2026-05-11

### Changes
- Cached `BaseSource.is_real_url` validator results to a `diskcache.Cache` at `.cachassert/` in `models.py`, so repeated config parses skip redundant `httpx.head()` round-trips against unchanged URLs.
- Increased `is_real_url` `httpx.head()` timeout from 5.0s to 15.0s to further reduce spurious validation failures against slow upstreams.
- Added `diskcache>=5.6.3` runtime dependency.

## 7.4.4 - 2026-05-11

### Changes
- Relaxed `BaseSource.is_real_url` validator in `models.py` to only raise on 4xx responses from `httpx.head()`. Servers that return 5xx or other non-2xx statuses to `HEAD` requests no longer fail config validation, since the URL itself is still well-formed and reachable.
- Increased `is_real_url` `httpx.head()` timeout from 3.0s to 5.0s to reduce spurious validation failures against slow upstreams.

## 7.4.3 - 2026-05-11

### Bug Fixes
- Fixed `compile_graph()` in `lib.py` stripping the final version segment from output paths. `Path(f"./{name}_{version}")` treated the trailing `.N` of a semver version (e.g. `.3` in `7.4.3`) as a suffix, so `with_suffix(".edges.ndjson.temp")` replaced the version segment instead of appending. Base path now carries a `.tmp` sentinel suffix (`Path(f"./{name}_{version}.tmp")`) that `with_suffix()` replaces, preserving the full version in emitted filenames. Temp suffixes also shortened from `.temp` to `.tmp` for consistency.

## 7.4.2 - 2026-05-07

### Changes
- Added Pydantic field and model validators to `models.py` that enforce configuration correctness at parse time: `url` fields are verified reachable via `httpx.head()`, `rows` and `row_slice` are mutually exclusive, `Reindex.comparator` type must match its `comparison` operator (`eq`/`ne` require `str`, numeric operators require `int`/`float`), `encoding` values under `method: column` must be Excel-style letters (`A`–`ZZZ`), `Regex` pattern/replacement strings are validated against the Polars regex engine, `remove` entries are validated as Polars-compatible regex, and `annotation` names have underscores replaced with spaces.
- Changed `rows` and `row_slice` element type from `NonNegativeInt` to `PositiveInt` in `BaseSource`.

## 7.4.1 - 2026-05-05

### Bug Fixes
- Fixed `AttributeError: 'str' object has no attribute 'value'` raised by `format_section_oneline()` in `progress.py` during the BUILDING TCODE stage. The `Section` model sets `use_enum_values=True`, so `Tcode.status` is already a plain string — removed the stale `.value` access.

## 7.4.0 - 2026-05-05

### Changes
- Renamed CLI commands for brevity: `build-knowledge-graph` → `build`, `verify-table-configuration-syntax` → `validate`. Version display moved from `tablassert version` subcommand to `tablassert --version` flag.
- Added `qc` parameter to `resolve_many()` for optional QC auditing during standalone batch resolution. ONNX Runtime provider is auto-detected via `get_qc_provider()`.
- Added `has_qc_runtime()` helper to `qc.py` for ONNX Runtime detection.
- Added `empty_matches()` helper to `fullmap.py` for empty result fallback.
- Added `DownloadReceipt` dataclass, `DownloadError`/`DownloadValidationError` exception classes, and `classify()`/`validate_download()`/`modernize_xls()` to `downloader.py`.
- Updated log format to include timestamps: `{time:YYYY-MM-DD HH:mm:ss}`.

### Bug Fixes
- Fixed tutorial table configuration using header names as `encoding` values instead of Excel column letters (`A`, `B`, `C`, `D`).

### Documentation
- Updated all documentation to reflect renamed CLI commands.
- Fixed tutorial and example YAML configurations to use Excel column letter references (`A`, `B`, `C`, `D`) for `method: column` encodings instead of header names, matching the headerless source reading behavior.
- Fixed `encoding` values in `docs/examples/` gallery configurations.
- Updated `resolve_many()` API reference with new `qc` parameter and auto-detected QC provider.
- Fixed CITATION.cff version (7.2.2 → 7.4.0).
- Fixed CONTRIBUTING.md lazy-loaded package list (`typer` → `cyclopts`, added missing packages).

## 7.3.6 - 2026-04-29

### Documentation
- Documented that `publication` must start with `PMC` followed by digits when `repo` is `"PMC"`.

## 7.3.5 - 2026-04-29

### Documentation
- Tightened the table-configuration reference so field requirements, defaults, accepted enum values, row indexing, and column-reference examples match the strict `Section` schema and section-merging behavior implemented in `models.py`, `ingests.py`, and the runtime loader.

## 7.3.4 - 2026-04-28

### Bug Fixes
- Fixed `downloader.from_url()` failing on URLs that trigger an immediate download. The Playwright session now opens a browser context with `accept_downloads=True`, wraps `page.goto()` inside `page.expect_download()`, and tolerates the expected `net::ERR_ABORTED` navigation error that fires when the response is a download rather than a page.

### Documentation
- Documented `miscellaneous notes` as a freetext catch-all annotation in the table configuration and advanced-example pages — used for assay caveats, non-standard units, and qualitative observations that don't map cleanly to a structured field. Supports both `method: value` (constant) and `method: column` (per-row).
- Documented Polars regex constraints for the `regex` and `remove` transforms: patterns are passed to Polars `str.replace_all()` (Rust `regex` crate), so capturing groups (`(...)` / `\1`) and lookarounds (`(?=...)`, `(?<=...)`, `(?!...)`, `(?<!...)`) are not supported and will raise at parse time. Chain simple substitutions instead, or capture residual context in a `miscellaneous notes` annotation.

## 7.3.3 - 2026-04-08

### Bug Fixes
- Changed datassert shard count to 10 (`SHARDS` constant in `fullmap.py`) to correspond to the current datassert database layout.

### Documentation
- Updated shard count references across documentation and examples to reflect the current 10-shard datassert layout.
- Corrected provenance examples so `repo` carries the namespace prefix and `publication` carries the repository-local identifier.

## 7.3.2 - 2026-04-03

### Maintenance
- Updated dependencies. No API changes.

## 7.3.1 - 2026-04-03

### Changes
- Changed `resolve_many()` return type from `dict[str, list[str]]` to `list[dict[str, Any]]` — each resolved entity is now a row dictionary, produced via `to_dicts()`.
- `resolve_many()` now preserves the original input text in an `original {col}` key on each result row.

### Documentation
- Updated `resolve_many()` API reference to match the current function signature, return type, and output format.

## 7.3.0 - 2026-04-03

### New Features
- Added `resolve_many()` to `lib` module — a standalone batch entity resolution function that resolves an iterable of text strings to CURIEs without requiring manual LazyFrame setup, NLP preprocessing, or DuckDB connection management.

### Documentation
- Added detailed API reference page for `resolve_many()` covering function signature, parameters, return value, usage examples, and integration notes.

## 7.2.2 - 2026-04-01

### Bug Fixes
- Fixed Docker publish workflow failing due to mixed-case repository owner in image tags. Hardcoded lowercase `ghcr.io/skyeav/tablassert` and switched trigger to run after autotag completion.

### Maintenance
- Updated PyPI short description.

## 7.2.1 - 2026-04-01

### Maintenance
- Improved PyPI trove classifiers. No API changes.

## 7.2.0 - 2026-03-31

### New Features
- Added `tablassert version` command to display current package version.
- Added autotag GitHub Action for automated version tagging on releases.
- Added PyPI publishing GitHub Action.
- Added Docker image publishing to GitHub Container Registry (ghcr.io).

### Changes
- Sharded datassert entity-resolution database into 16 DuckDB shards for parallel querying.
- Renamed dependency from DBssert to DATASSERT throughout.
- Separated CLI logic into dedicated `cli.py` module.
- Extracted NLP normalization into dedicated `nlp.py` module for cleaner separation of concerns.
- Implemented improved parallelization model for graph compilation.
- Annotated Pydantic model fields with `Field(...)` schema metadata.
- Renamed `fullmap.version4()` to `fullmap.resolve()` for clarity.
- Updated `fullmap` ranking to prioritize case-insensitive exact matches between normalized terms and preferred names.
- Updated `fullmap` term de-duplication to keep first occurrences, improving deterministic output ordering.
- Moved MkDocs to dev-only dependencies.

### Testing
- Added basic pytest suite covering core models, enums, ingests, lib, nlp, and utils.

### Maintenance
- Improved `.gitignore` to exclude common artifacts.

## 7.0.2 - 2026-03-23

### Changes
- Updated package metadata for the 7.0.2 release.
- Added optional `log` and `column_context` controls to `fullmap.resolve()` for more configurable entity-resolution behavior.

### Bug Fixes
- Reworked entity-resolution querying to register terms directly in DuckDB instead of writing temporary parquet files, removing tempfile lifecycle issues in `fullmap` query execution.
- Isolated unmatched-entity logging into a dedicated helper and gated it behind an explicit logging flag.

### Documentation
- Updated API reference docs to match the current `resolve()` function signature and behavior.
- Corrected QC documentation to reflect the implemented fuzzy/BERT validation pipeline.
- Fixed documentation path typos for cache/store artifact directories.

## 7.0.1 - 2026-03-17

### Documentation
- Updated installation docs to reflect `pyproject.toml` extras and added `tablassert[rt]` guidance for systems without required default Polars CPU instructions.

## 7.0.0 - 2026-03-17

### New Features
- Added pre-commit hooks for code quality (ruff linting, formatting, and pyright type checking).
- Enhanced development environment with improved VSCode settings and better gitignore including direnv support.

### Changes
- Migrated dependency management from Nix to UV for improved Python toolchain integration and simpler development workflow.
- Updated GitHub Actions workflows to use UV for deployment and documentation building.
- Removed Docker installation method from documentation to align with current supported usage.
- Removed Nix-specific installation methods and dependencies from the project.
- Removed Chromium dependency as it's no longer required for the core functionality.
- Removed random callable from codebase to simplify dependencies.
- Updated directory naming conventions for better consistency throughout the project.

### Breaking Changes
- Nix is no longer supported for development and installation. Use UV-based installation instead.
- Project now requires Python 3.11+ for compatibility with UV toolchain.

### Documentation
- Completely rewrote installation documentation to reflect UV-based development environment.
- Updated CLI and configuration documentation to remove Nix-specific sections.
- Updated project README with new installation instructions.

## 6.2.1 - 2026-03-12

### Features
- Improved QC auditing with clearer stage behavior and richer failure logging context for section/config/column tracing.
- Improved entity resolution and pipeline behavior for difficult mapping cases, including additional safeguards around nulls, strings, and column-context handling.
- Added optional `pubmed_db` and `pmc_db` graph-configuration support so enrichment can be enabled only when those databases are available.

### Bug Fixes
- Fixed multiple `fullmap` correctness issues, including handling for missing taxon values and unmatched-term edge cases.
- Fixed TCode and transform-path edge cases affecting reindex/math/null-strip behavior during section compilation.
- Fixed integration issues across lazy/eager collection boundaries to reduce incorrect intermediate outputs.

### Performance
- Optimized graph compilation by skipping empty node/edge artifacts and reducing unnecessary downstream work.

### Documentation
- Corrected stale or inaccurate docs from 6.2.0 and aligned CLI, configuration, and API references with current runtime behavior.

## 6.2.0 - 2026-02-27

### New Features
- Added `tablassert verify-table-configuration-syntax <table-config.yaml>` for fast TC3 schema validation without running a full graph build.
- Added rich progress bars across pipeline stages to improve runtime visibility during large graph builds.
- Added automated Docker publishing in CI so container images are built and distributed from the docs workflow.
- Added improved progress messaging and stage-level status updates for entity mapping and build orchestration.

### Changes
- Updated the CLI interface for graph builds from `tablassert -i <graph-config.yaml>` to `tablassert build-knowledge-graph <graph-config.yaml>`.
- Swapped hashing internals to xxHash to improve throughput in high-volume processing paths.
- Updated label-rebuild startup logic so label generation begins with clearer rebuild conditions.
- Refactored AGENTS.md hierarchy into root and scoped instruction files (`docs/`, `nix/`, `lib/tablassert/`) to reduce duplication and clarify ownership.
- Revised docs and installation guidance to align with the 6.2.0 command surface and Docker workflows.

### Breaking Changes
- Graph build invocation now requires the explicit `build-knowledge-graph` subcommand; legacy direct invocation with only `-i` is no longer the primary interface.

### Bug Fixes
- Resolved assignment and small runtime issues captured in recent maintenance commits.
- Applied lint and architecture-documentation cleanup updates to reduce drift and improve maintainability.

For full commit history, run `git log --oneline` in the repository.

## 6.1.0 - Date not tagged in repository metadata

### Notes
- Baseline release prior to the 6.2.0 CLI split and verification-command additions.
