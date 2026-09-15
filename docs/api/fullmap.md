# Entity Resolution (fullmap)

The `fullmap` module resolves free-text strings to standardized biological CURIEs against the embedded redb database: call `resolve()` for low-level, LazyFrame-based entity resolution inside a pipeline.

## resolve()

Primary entity resolution function, querying the embedded fullmap redb database.

### Function Signature

```python
def resolve(
  lf: pl.LazyFrame,
  col: str,
  db: Path,
  taxon: Optional[str] = None,
  prioritize: Optional[list[Categories]] = None,
  avoid: Optional[list[Categories]] = None,
  exclude_prefixes: Optional[list[str]] = None,
  exclude_regex: Optional[list[str]] = None,
  log: bool = True,
  section_hash: Optional[str] = None,
  config_file: Optional[str] = None,
  column_context: bool = True,
  tag: str = "_two",
) -> pl.LazyFrame
```

### Parameters

**`lf: pl.LazyFrame`**

Input LazyFrame containing the data to process. Internally collected at explicit collection points for redb lookups and joins.

**`col: str`**

Column name containing text strings to resolve.

**`db: Path`**

Path to the fullmap redb file (already resolved; see `fullmap_db_path()` and [Fullmap](../fullmap.md)).

**`taxon: Optional[str]`**

Optional NCBI Taxon ID for filtering results.

Example: `"9606"` filters to human-specific entities.

**`prioritize: Optional[list[Categories]]`**

Optional list of Biolink categories to prefer when multiple matches exist.

Example: `[Categories.GENE, Categories.PROTEIN]` prefers gene/protein mappings.

**`avoid: Optional[list[Categories]]`**

Optional list of Biolink categories to exclude from results. When set, the column is an
allow-list by complement: fullmap categories the `Categories` enum cannot name are
dropped as well.

Example: `[Categories.GENE]` prevents gene mappings.

**`exclude_prefixes: Optional[list[str]]`**

Optional list of CURIE namespace prefixes to drop from the results, mirroring the `NodeEncoding.exclude_prefixes` config field. A prefix is the text before the first `:` of a resolved CURIE (e.g., `"OMIM"` for `"OMIM:100100"`); every candidate whose prefix is listed is dropped. Matching is exact and case-sensitive.

Example: `["OMIM", "MONDO"]` drops all OMIM- and MONDO-namespaced candidates.

**`exclude_regex: Optional[list[str]]`**

Optional list of case-sensitive regex patterns (Polars/Rust dialect), mirroring the `NodeEncoding.exclude_regex` config field; any resolved CURIE matching one of the patterns is dropped. An empty or whitespace-only pattern is rejected at config-validation time because it would match every CURIE.

Example: `["^CHEBI:"]` drops all CHEBI candidates.

**`log: bool` (default: `True`)**

Controls unmatched-value logging. When enabled, unresolved terms are logged with section/config/column context.

**`section_hash: Optional[str]` / `config_file: Optional[str]`**

Optional context fields used for operational logging when unmatched values are encountered.

**`column_context: bool` (default: `True`)**

Controls category-frequency tie-breaking when multiple matches exist for a term. When `True`, the query result adds a category frequency score and prefers more frequent category hits; distinct CURIEs that remain tied are all returned.

**`tag: str` (default: `"_two"`)**

Suffix appended to `col` to locate the `level_two` output column.

`resolve()` expects the LazyFrame to already have two NLP columns applied upstream:
- `col`: the canonical `level_one` output (cleaned, Unicode-lowercased, Porter2-stemmed, deduplicated, and byte-sorted by token)
- `col + tag`: the `level_two` output (non-word characters removed via `\W+`)

The default `"_two"` matches `level_two`'s default tag.

### Return Value

Returns a Polars LazyFrame with these columns added:

| Column | Description | Example |
|--------|-------------|---------|
| `{col}` | CURIE identifier | `"HGNC:11998"` |
| `{col}_name` | Preferred entity name | `"TP53"` |
| `{col}_category` | Biolink category | `"biolink:Gene"` |
| `{col}_taxon` | NCBI Taxon ID | `"NCBITaxon:9606"` |
| `{col}_source` | Source database | `"HGNC"` |
| `{col}_source_version` | Database version | `"2025-01"` |
| `{col}_nlp_level` | NLP processing level | `1` or `2` |

### Lookup Pipeline

The function:

1. **Builds an in-memory term table** by collecting terms from both NLP levels and deduplicating by keeping first occurrences for deterministic ordering, then looks them up against the redb `records` table via the Rust `lookup_fullmap_terms()` extension function.

2. **Ranks matches** by:
   - Category priority (if `prioritize` specified)
   - Preferred-name exactness (raw exact match first, then normalized level-one match)
   - NLP level (level one preferred over level two)
   - Category frequency (if `column_context=True`)

3. **Filters by:**
   - Taxon ID (if `taxon` specified)
   - Category avoidance (if `avoid` specified; unnameable categories are dropped with it)
   - Excluded CURIE prefixes (if `exclude_prefixes` specified)
   - Excluded CURIE regex patterns (if `exclude_regex` specified)

4. **Retains the best ranking tier** per input string: duplicate rows for the same CURIE collapse, while distinct CURIEs tied across every ranking heuristic are returned as separate rows

### Example Usage

```python
from pathlib import Path
from tablassert.fullmap import resolve
from tablassert.biolink import Categories
import polars as pl

# Path to the fullmap redb file
db = Path("/path/to/fullmap/data/fullmap.redb")

# LazyFrame with data to resolve
lf = pl.scan_parquet("data.parquet")

# Resolve gene symbols to CURIEs
result = resolve(
  lf=lf,
  col="gene_symbol",
  db=db,
  taxon="9606",  # Human only
  prioritize=[Categories.GENE],
  avoid=[Categories.PROTEIN],
  log=True,
  section_hash="tutorial-section",
  config_file="tutorial-table.yaml",
  column_context=True,
)

# Result LazyFrame includes:
# - gene_symbol: "HGNC:11998"
# - gene_symbol_name: "TP53"
# - gene_symbol_category: "biolink:Gene"
# - etc.
```

### Mapping a Python List

Resolve a plain Python list by building a LazyFrame and applying the NLP levels before `resolve()`:

```python
import polars as pl
from pathlib import Path
from tablassert.fullmap import resolve
from tablassert.nlp import level_one, level_two
from tablassert.biolink import Categories

db = Path("/path/to/fullmap/data/fullmap.redb")
lf = pl.LazyFrame({"gene": ["TP53", "BRCA1", "EGFR", "KRAS"]})
lf = level_one(lf, "gene")   # canonical tokenize, stem, deduplicate, and sort
lf = level_two(lf, "gene")   # remove non-word chars → "gene_two" column

result = resolve(lf=lf, col="gene", db=db, taxon="9606",
                 prioritize=[Categories.GENE], log=False).collect()
print(result.select(["gene", "gene_name", "gene_category"]))
```

### NLP Processing Levels

`resolve()` requires that `level_one` and `level_two` have been applied to the LazyFrame before calling it. Level one is the canonical key used by the fullmap build and query paths:

**`level_one` output** (column: `col`):
- Cleans surrounding matching quotes and whitespace, then applies Unicode lowercase.
- Splits on whitespace, stems only tokens made entirely of ASCII letters with the English Porter2 stemmer, and passes digit-bearing, punctuation-bearing, and non-ASCII tokens through unchanged after lowercasing.
- Removes duplicate tokens, sorts the remaining tokens by their UTF-8 byte values, and joins them with one ASCII space.
- Preserves nulls in Python LazyFrame columns; empty and whitespace-only values become the empty string.
- Is queried first; preferred-name ranking compares these normalized forms, so a fullmap database built with different level-one keys cannot be reused.

Schema-v5 fullmap databases are rejected by the current resolver. Rebuild existing fullmaps to produce schema v6 before using them with this level-one contract.

**`level_two` output** (column: `col + "_two"`):
- All non-word characters removed (`\W+` → `""`) from the `level_one` result
- Used as fallback when `level_one` produces no match
- Preferred for disease names and free text

Rows without a valid CURIE are filtered from the returned frame.

### Provenance Tracking

Every resolved entity carries its source database, source version (snapshot date), and the matched synonym that triggered the match, enabling auditing and quality control. Case is handled by the NLP levels above: `level_one` matches any case variant, `level_two` further strips punctuation for hyphenated or slash-delimited names.

## Integration with QC

Entity resolution output is validated by `fullmap_audit()` from the `qc` module before being included in the knowledge graph.

See [Quality Control](qc.md) for details.

## Next Steps

- **[Quality Control](qc.md)** - Multi-stage validation
- **[Configuration](../configuration/table.md)** - How to specify prioritize/avoid in YAML
