# Batch Resolution (lib)

The `lib` module exposes `resolve_many()`, a high-level convenience function that batch-resolves an iterable of entity strings to CURIEs; use it in scripts and notebooks when you want results without building LazyFrames or running NLP preprocessing yourself. It wraps the lower-level [`resolve()`](fullmap.md) pipeline (normalization, fullmap lookup, resolution, optional QC audit when `qc=True`) and returns a plain Python list of row dictionaries.

## resolve_many()

Standalone batch entity resolution function. Accepts a column name, an iterable of text strings, and a path to the fullmap database, then returns resolved CURIEs and metadata as a list of row dictionaries.

### Function Signature

```python
def resolve_many(
    col: str,
    entities: Iterable[str],
    fullmap: Path,
    taxon: Optional[str] = None,
    prioritize: Optional[list[Categories]] = None,
    avoid: Optional[list[Categories]] = None,
    qc: bool = False,
    column_context: bool = True,
) -> list[dict[str, Any]]
```

### Parameters

**`col: str`**

Column name used internally to label the Polars Series and DataFrame columns during resolution. This name propagates through the NLP and resolution pipeline and determines the keys in each returned row dictionary.

For example, if `col="gene"`, each returned row dictionary will contain keys like `"gene"`, `"gene_name"`, `"gene_category"`, etc.

**`entities: Iterable[str]`**

An iterable of text strings to resolve. Each string is treated as a candidate entity name that will be normalized and matched against the fullmap synonym database. Accepts any iterable: lists, tuples, generators, sets, etc.

Examples: `["TP53", "BRCA1", "EGFR"]`, `("aspirin", "ibuprofen")`, or a generator expression.

**`fullmap: Path`**

Filesystem path to the fullmap redb file, or a base directory containing it (resolved via `fullmap_db_path()`; see [Fullmap](../fullmap.md)).

**`taxon: Optional[str]` (default: `None`)**

Optional NCBI Taxon ID for filtering results to a specific organism.

Example: `"9606"` restricts **gene** matches to human-specific entries; non-gene categories (e.g., diseases, chemicals) are returned regardless of taxon. When `None`, no taxon filtering is applied and matches from all organisms are returned.

**`prioritize: Optional[list[Categories]]` (default: `None`)**

Optional list of Biolink categories to prefer when multiple matches exist for the same input term. Categories listed here receive higher ranking scores during resolution.

Example: `[Categories.GENE, Categories.PROTEIN]` prefers gene and protein mappings over other categories like diseases or chemicals.

**`avoid: Optional[list[Categories]]` (default: `None`)**

Optional list of Biolink categories to exclude from results entirely. Any match belonging to an avoided category is filtered out before ranking.

Example: `[Categories.GENE]` prevents gene mappings from appearing in the output, even if they would otherwise be the best match.

**`column_context: bool` (default: `True`)**

Controls category-frequency tie-breaking when multiple matches exist for a term. When `True`, the deduplication stage adds a category-frequency score (computed in Polars after the SQL query) and prefers the category that appears most frequently across all matched terms in the batch. When `False`, frequency-based tie-breaking is disabled.

This is useful when resolving a column of related entities (e.g., all genes): the shared context helps disambiguate terms that map to multiple categories.

**`qc: bool` (default: `False`)**

When `True`, runs the QC audit stage after entity resolution. The QC pipeline validates mappings through a four-stage audit: exact match, fuzzy matching via rapidfuzz, abbreviation expansion (Schwartz-Hearst), and SapBERT sentence embeddings with cosine similarity. Mappings that fail all four stages are dropped from the returned list (in addition to the unresolved-entity filtering performed by `resolve()`). Requires the QC runtime to be installed (`tablassert[qc]`); see [Quality Control](qc.md) for the stage thresholds and backend.

### Return Value

Returns a `list[dict[str, Any]]`: one dictionary per resolved entity. The list is produced by calling `polars.DataFrame.to_dicts()` on the collected resolution output.

Each dictionary contains the following keys (where `{col}` is the value of the `col` parameter):

| Key | Description | Example Value |
|-----|-------------|---------------|
| `original_{col}` | Original input text before normalization | `"TP53"` |
| `{col}` | CURIE identifier | `"HGNC:11998"` |
| `{col}_name` | Preferred entity name | `"TP53"` |
| `{col}_category` | Biolink category (prefixed) | `"biolink:Gene"` |
| `{col}_taxon` | NCBI Taxon ID (prefixed) | `"NCBITaxon:9606"` |
| `{col}_source` | Source database | `"HGNC"` |
| `{col}_source_version` | Database version | `"2025-01"` |
| `{col}_nlp_level` | NLP processing level used for match | `1` or `2` |

**Important:** Only entities that successfully resolve to a CURIE are included in the output. Unresolved entities are filtered out by `resolve()`. The returned list may therefore be shorter than the input iterable.

### Pipeline Internals

Internally: wrap the iterable in a single-column LazyFrame; snapshot the raw input to `original_{col}` (returned) and `{col}_pre_resolution` (internal, dropped; mirrors edge output); apply `level_one`/`level_two`; resolve the fullmap path via `fullmap_db_path()`; delegate to `fullmap.resolve()`; optionally run `fullmap_audit()` when `qc=True`; collect and `to_dicts()`.

### Example Usage

#### Basic Gene Resolution

```python
from pathlib import Path
from typing import Any
from tablassert.lib import resolve_many
from tablassert.biolink import Categories

fullmap: Path = Path("/path/to/fullmap")

result: list[dict[str, Any]] = resolve_many(
    col="gene",
    entities=["TP53", "BRCA1", "EGFR", "KRAS"],
    fullmap=fullmap,
    taxon="9606",
    prioritize=[Categories.GENE],
)

# result[0] → {"original_gene": "TP53", "gene": "HGNC:11998", "gene_name": "TP53", ...}
# result[1] → {"original_gene": "BRCA1", "gene": "HGNC:1100", "gene_name": "BRCA1", ...}
```

#### Consuming Results

```python
import polars as pl
from pathlib import Path
from typing import Any
from tablassert.lib import resolve_many

fullmap: Path = Path("/path/to/fullmap")

result: list[dict[str, Any]] = resolve_many(
    col="gene",
    entities=["TP53", "BRCA1"],
    fullmap=fullmap,
    taxon="9606",
)

# Convert back to a Polars DataFrame
df: pl.DataFrame = pl.DataFrame(result)

# Or iterate over resolved rows
for row in result:
    print(f"{row['gene_name']} → {row['gene']}")
```

### Comparison With resolve()

| Aspect | `resolve_many()` | `resolve()` |
|--------|-------------------|-------------|
| **Module** | `tablassert.lib` | `tablassert.fullmap` |
| **Input** | Plain iterable of strings | Pre-normalized `pl.LazyFrame` |
| **NLP** | Applied automatically | Must be applied upstream |
| **Path resolution** | Resolved internally via `fullmap_db_path()` | Caller must pass the resolved redb path |
| **Output** | `list[dict[str, Any]]` | `pl.LazyFrame` |
| **Logging** | Uses default (`log=True`) | Configurable |
| **Context params** | `column_context` exposed; `section_hash`, `config_file`, `tag`, `exclude_prefixes`, `exclude_regex` not exposed | Fully configurable |
| **Use case** | Standalone batch lookups, scripting, notebooks | Internal pipeline integration |

`resolve_many()` is designed for ad-hoc and programmatic use: scripts, notebooks, and one-off lookups. For pipeline integration where you need full control over logging, context metadata, and lazy evaluation, use `resolve()` directly.

### NLP Processing

`resolve_many()` applies the same level-one normalizer used to build and query the fullmap, writing its result to `{col}`, then applies `level_two` to write `{col}_two`. Level one cleans surrounding quotes and whitespace, lowercases with Unicode rules, tokenizes on whitespace, stems only all-ASCII-alphabetic tokens with English Porter2, passes digit- and punctuation-bearing tokens through, removes duplicate tokens, sorts tokens by UTF-8 byte value, and joins them with one space. Nulls remain null in Python LazyFrame columns; empty values remain empty. Level one is preferred; level two is the fallback for terms with punctuation or special characters.

The resolver requires a schema-v6 fullmap. Existing schema-v5 databases are rejected and must be rebuilt. Preferred-name ranking compares normalized forms under the same level-one rules.

### Error Handling

- If the `fullmap` path does not resolve to a valid redb file, or the file's schema tag doesn't match the expected version, the Rust extension raises a `RuntimeError`.
- If `entities` is empty, the function returns `[]`.
- Unresolved entities are silently filtered from the output (logged at INFO level by default via `resolve()`).

## Integration

`resolve_many()` is a self-contained entry point. It does not require any prior setup beyond having a fullmap database available. For full pipeline builds, use the CLI (`tablassert build-kg`) which orchestrates resolution through the `Tcode` class.

## Next Steps

- **[Entity Resolution](fullmap.md)** - Lower-level `resolve()` function details
- **[Quality Control](qc.md)** - Multi-stage validation of resolved entities
- **[Configuration](../configuration/table.md)** - YAML-driven entity resolution settings
