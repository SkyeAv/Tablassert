# Quality Control (qc)

The `qc` module validates entity-resolution mappings through a four-stage pipeline (exact, fuzzy, abbreviation expansion, SapBERT semantic similarity); it runs behind `build-kg --qc` and `resolve_many(qc=True)` to keep only high-confidence assertions.

QC runtime support is optional. Install `tablassert[qc]` to enable it: the extra pulls `scikit-learn` and `sentence-transformers` (`torch` and `numpy` arrive transitively); `rapidfuzz` is a core dependency and is always available.

`fullmap_audit()` checks the whole extra before it does any work and raises `QcRuntimeMissingError` naming every absent package and the install command. Checking up front matters because the two packages are needed at different stages (`scikit-learn` from the start, `sentence-transformers` only if Stage 4 is reached), so a half-installed extra would otherwise fail after the audit had already run. `build-kg --qc` performs the same check before the build begins, so a missing extra fails immediately instead of after resolution has already cost time.

## fullmap_audit()

Primary quality control function that filters entity mappings based on confidence criteria.

### Function Signature

```python
def fullmap_audit(
  lf: pl.LazyFrame,
  col: str,
  section_hash: str,
  config_file: str,
  out: str = "passed",
  log: bool = True,
  on_phase: Optional[Callable[[str], None]] = None,
  similarity_threshold: float = 0.5,
  fuzzy_ratio_threshold: float = 70.0,
  fuzzy_partial_threshold: float = 80.0,
  aliases: Optional[dict[str, str]] = None,
) -> pl.LazyFrame
```

### Parameters

**`lf: pl.LazyFrame`**

Input LazyFrame containing entity resolution results.

Expected columns:
- `{col}_pre_resolution` - Original (pre-resolution) text string
- `{col}` - Resolved CURIE
- `{col}_name` - Preferred entity name

**`col: str`**

Base column name for entity resolution.

Example: If `col="subject"`, looks for:
- `"subject_pre_resolution"`
- `"subject"`
- `"subject_name"`

**`out: str` (default: `"passed"`)**

Name of the boolean column indicating validation status (used internally).

Rows with `out=True` passed QC, `out=False` failed.

**`log: bool` (default: `True`)**

Controls whether failed QC rows are logged.

**`on_phase: Optional[Callable[[str], None]]`**

Optional callback fired at the start of each QC cascade stage with exactly one of the phase labels `qc:exact`, `qc:fuzzy`, `qc:abbrev`, or `qc:sapbert` (`qc:abbrev` fires only when Stage 3 runs, and `qc:sapbert` only when Stage 4 does). `build-kg` passes one to drive per-section QC progress; `None` disables phase reporting.

**`section_hash: str` / `config_file: str`**

Context fields used in QC failure logs for traceability.

### Return Value

Returns a Polars LazyFrame containing only the rows whose `col` value (CURIE) has **at least one** passing pre-resolution/preferred-name pair.

QC scores unique `(CURIE, pre_resolution, preferred_name)` pairs, but the result is joined back to the input via a **semi-join on the CURIE column** (`df.join(passed.select(col), on=col, how="semi")`). The retention granularity is therefore the CURIE, not the individual pair: if *any* pair for a CURIE passes any stage, *every* input row sharing that CURIE is kept, including rows that were themselves part of a failed pair. A CURIE (and thus all of its rows) is dropped only when *none* of its pairs pass any stage. Failed pairs are logged with section/config/column context and their fuzzy/SapBERT scores.

### Four-Stage Pipeline

The function applies four validation stages in sequence. Each stage progressively filters out correct resolutions and leaves suspected errors for the next stage.

#### Stage 1: Exact Match & Rule-Based Pass-Through

**Fast path for high-confidence mappings.** A row passes if any of these hold:

```python
_qc_alias(original, aliases) == preferred_name
```

- The alias-resolved pre-resolution text equals the resolved preferred name. `aliases` maps a casefolded
  source text to its preferred spelling, so a configured alias can pass on exact match.
- The resolved CURIE matches an exempt prefix (`CHEBI`, `PR`, `UniProtKB`, `NCBIGene`, `UMLS`, `UNII`, `PUBCHEM`, `MONDO`).
- The original text contains `:` (already looks like a CURIE).
- The preferred name matches an exception prefix (`^LOC` or `^si:`).

#### Stage 2: Fuzzy Matching

**Medium confidence using RapidFuzz (batched via `rapidfuzz.process.cpdist`).**

Three scorers run over each unique pending pair, batched through `rapidfuzz.process.cpdist`:
1. **Ratio:** `fuzz.ratio`, overall string similarity
2. **Partial token sort ratio:** `fuzz.partial_token_sort_ratio`, subsequence matching
3. **Token set ratio:** `fuzz.token_set_ratio`, duplicate-tolerant token overlap

A pair passes if any scorer clears its threshold. `fuzz.ratio` is compared against
`fuzzy_ratio_threshold` (default 70.0); the two token scorers share `fuzzy_partial_threshold`
(default 80.0). Both thresholds are graph-configurable through `qc_fuzzy_ratio_threshold` and
`qc_fuzzy_partial_threshold`.

```python
fuzz.ratio(original, preferred) >= fuzzy_ratio_threshold
or fuzz.partial_token_sort_ratio(original, preferred) >= fuzzy_partial_threshold
or fuzz.token_set_ratio(original, preferred) >= fuzzy_partial_threshold
```

#### Stage 3: Abbreviation Expansion

**Deterministic pass for abbreviation/expansion pairs (Schwartz-Hearst).** A row passes when the pre-resolution text abbreviates the preferred name, or vice versa:

```python
_is_abbrev(original, preferred_name) or _is_abbrev(preferred_name, original)
```

The matcher scans the short form right-to-left against the long form (case-insensitively); the first character of the short form must land on a word boundary of the long form. This rescues the class both fuzzy matching and embedding similarity can miss, the `AML` / `acute myeloid leukemia` pair.

#### Stage 4: SapBERT Semantic Similarity

**High confidence using SapBERT embeddings.**

1. **Encode** original and preferred name with SapBERT (sentence-transformers)
2. **Compute** cosine similarity between embeddings (scikit-learn)
3. **Accept** if similarity >= 0.5

```python
embeddings = get_sapbert().encode(originals + preferreds)
similarity = cosine_similarity(embeddings[:n], embeddings[n:]).diagonal()
return similarity >= qc.SIMILARITY_THRESHOLD  # 0.5
```

The model is loaded once and cached for the lifetime of the process, so transformer inference is the
costly stage of the cascade.

### SapBERT Model

**Model:** `RenaissanceComputing/SapBERT-PubMedBERT-BabelSynonyms-v2025-01-23`

**Backend:** [sentence-transformers](https://www.sbert.net/) (PyTorch). Embeddings are compared with scikit-learn's `cosine_similarity`. SapBERT's self-alignment pretraining pulls synonym pairs together in embedding space; this checkpoint is RENCI's fine-tune of `cambridgeltl/SapBERT-from-PubMedBERT-fulltext` on synonym pairs sampled from NCATS Translator Babel identifier cliques, so its training signal matches this stage's task, deciding whether two names denote the same entity. The 0.5 threshold was carried over from the earlier BioBERT gate and has not been re-tuned for SapBERT's score distribution.

**Lazy-loaded** on the first `fullmap_audit()` call that reaches the embedding stage via `get_sapbert()`, then cached globally for the lifetime of the process.

### Model Caching

`get_sapbert()` loads the model from the local cache when present; otherwise it downloads `RenaissanceComputing/SapBERT-PubMedBERT-BabelSynonyms-v2025-01-23` and saves it for future runs.

**Cache location:** `.tablassert/sapbert-babel/` on disk (`qc.MODEL`); the loaded model object is also cached in memory for the lifetime of the process. Existing `.tablassert/sapbert/` directories hold the previous base checkpoint and are stale; they can be deleted.

### Example Usage

```python
from tablassert.qc import fullmap_audit
import polars as pl

# LazyFrame with entity resolution results
lf = pl.scan_parquet("resolved.parquet")

# Expected columns:
# - subject_pre_resolution
# - subject (CURIE)
# - subject_name

# Run QC
validated = fullmap_audit(
  lf,
  col="subject",
  section_hash="tutorial-section",
  config_file="tutorial-table.yaml",
)

# Only rows that passed QC remain
# Rows with low-confidence mappings removed
```

### Pipeline Flow

The cascade runs on unique `(original, preferred)` pairs, deduplicated before scoring, so the counts
below are pairs rather than source rows. Illustrative counts:

```
Input:  1000 unique pairs with entity mappings

Stage 1 (Exact): 700 pass, 300 pending
Stage 2 (Fuzzy): 250 pass, 50 pending
Stage 3 (Abbreviation): 10 pass, 40 pending
Stage 4 (SapBERT): 30 pass, 10 rejected

Output: 990 pairs pass (700 + 250 + 10 + 30)
```

### Confidence Levels

| Stage | Method | Confidence | Use Case |
|-------|--------|-----------|----------|
| 1 | Exact match / rule-based | Highest | Standardized IDs, acronyms, CURIE-like inputs |
| 2 | Fuzzy | Medium | Typos, word reordering |
| 3 | Abbreviation expansion | High | Abbreviation / full-name pairs |
| 4 | SapBERT | High | Synonyms, paraphrases |

### Rejection Logging

When `log=True`, each rejected CURIE is logged at INFO level with its context and the scores that caused the rejection: `curie`, `original`, `preferred`, `col`, `fuzz` (partial token sort ratio), `config`, `hash`, and, when the SapBERT stage ran, `sapbert` (cosine similarity).

### Integration with Pipeline

QC is applied after entity resolution when QC is enabled (the `build-kg --qc` flag, or `resolve_many(..., qc=True)`):

1. **Entity resolution** (`resolve()`) - Maps text to CURIEs
2. **Quality control** (`fullmap_audit()`) - Validates mappings
3. **Export** - Only validated mappings in final output

This ensures knowledge graphs contain only high-confidence assertions.

## Next Steps

- **[Entity Resolution](fullmap.md)** - How mappings are generated
- **[Configuration](../configuration/table.md)** - Prioritize/avoid to improve resolution
