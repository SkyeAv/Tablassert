# Advanced Example: Real-World Configuration

A fully-annotated real-world table configuration (ALAMV6.yaml) showing complex regex, taxonomic
filtering, and statistical annotations working together. **Source:** microbiome-chemical correlation
analysis from PMC11708054, an Excel file from which we extract correlations between gut microbiota
and tamoxifen metabolites.

## Full Configuration

```yaml
template:
  # Data source: Excel file from PubMed Central
  source:
    kind: excel
    local: ./DATALAKE/ALAM.XLSX
    url:
      - https://pmc.ncbi.nlm.nih.gov/articles/instance/11708054/bin/mbio.01679-24-s0006.xlsx
    row_slice:
      - 2  # Skip the first two rows (title + header)
      - auto  # Read to end
    sheet: all correlations

  # Triple definition: Microbe correlated_with Chemical
  statement:
    subject:
      method: column
      encoding: A  # Column A contains organism names

      # Prefer organism classifications over genes
      prioritize: [OrganismTaxon]
      avoid: [Gene]

      # Strip non-value text in place (rows are not dropped)
      remove: ["^NA "]  # Cells starting with "NA "

      # Clean taxonomic names with regex
      regex:
        - {pattern: ".*g__", replacement: ""}    # Remove genus prefix "g__"
        - {pattern: ";s__", replacement: " "}    # Replace species separator ";s__" with space
        - {pattern: "sp", replacement: "sp. "}   # Add space after "sp" abbreviation

    predicate: correlated_with

    object:
      method: value
      encoding: CHEBI:41774  # All rows: 13C-tamoxifen

  # Provenance: Publication and curation info
  provenance:
    repo: PMC
    publication: PMC11708054

  # Statistical metadata as edge annotations (method: value = constant,
  # method: column = per-row)
  annotations:
    - {annotation: study_size, method: value, encoding: 9}      # Study metadata -> inlined supporting Study
    - {annotation: p_value, method: column, encoding: C}
    - {annotation: multiple_testing_correction_method, method: value, encoding: Benjamini Hochberg}
    - {annotation: effect_size, method: column, encoding: B}   # Spearman rho value
    - {annotation: effect_type, method: value, encoding: spearmans_rho}
    - {annotation: assertion_method, method: value, encoding: Spearman correlation}

    # Freetext catch-all for context that doesn't map to a structured annotation
    - annotation: miscellaneous_notes
      method: value
      encoding: Correlation analysis between microbial composition and 13C-tamoxifen abundance after FDR correction
```

`miscellaneous_notes` is a freetext escape hatch: use `method: value` for a constant note across the
whole table or `method: column` to pull per-row notes from the source (see
[allow-list and auto-folding](table.md#allow-list-and-auto-folding)).

## Key Techniques

- **Excel column letters**: `encoding: A`/`B`/`C` reference the first/second/third columns of the
  headerless source (organism names, Spearman rho, p-value).
- **Regex pipeline**: the subject runs three substitutions in order: `.*g__` -> ``
  (`d__Bacteria;p__Firmicutes;g__Lactobacillus` -> `Lactobacillus`), then `;s__` -> ` `
  (`Lactobacillus;s__rhamnosus` -> `Lactobacillus rhamnosus`), then `sp` -> `sp. `.
- **Taxonomic filtering**: `prioritize: [OrganismTaxon]` + `avoid: [Gene]` stop "Lactobacillus"
  resolving to a similarly-named gene.
- **Mixed annotations**: `method: value` for constants (same every row), `method: column` for
  per-row values.
- **Subject-predicate-object**: subject varies per row (column), predicate `correlated_with` is
  fixed, object `CHEBI:41774` is fixed -> `Lactobacillus rhamnosus --[correlated_with]--> 13C-tamoxifen`.

??? note "Regex dialect constraint"
    Each `pattern` is handed to Polars `str.replace_all()` (Rust `regex` crate). **Backreferences
    (`\1`, `\2`, ...) and lookarounds (`(?=...)`, `(?<=...)`, `(?!...)`, `(?<!...)`) are not allowed**
    and fail validation; plain `(...)` and non-capturing `(?:...)` groups are supported. Express
    transformations as a chain of simple substitutions, or capture leftover context in a
    `miscellaneous_notes` annotation. See [Text Transformations](table.md#text-transformations).

## Output Example

**Nodes:**
```json
{"id":"NCBITaxon:47715","name":"Lactobacillus rhamnosus","category":["biolink:OrganismTaxon"]}
{"id":"CHEBI:41774","name":"13C-tamoxifen","category":["biolink:ChemicalEntity"]}
```

**Edges:** Allow-listed annotation columns (`p_value`, `effect_size`, `effect_type`) stay as
top-level edge fields (`p_value` in controlled scientific-notation string form, `effect_size` as a real
JSON number since biolink-model 4.4.4). Study metadata (`study_size`, here declared as a constant)
lands on the edge's inlined supporting `Study`; the `Study.id` is the publication CURIE, `Study.name`
the worksheet, and the single `StudyResult` anchors the source row as its `row:<N>` id. Any
non-Biolink-slot name such as `assertion_method`, `multiple_testing_correction_method`, and
`miscellaneous_notes` folds into the edge's `supporting_text` list as `"name: value"` entries
(sorted alphabetically):

```json
{
  "id": "2cfea591-0f8f-33af-a7df-03da531d3359",
  "subject": "NCBITaxon:47715",
  "predicate": "biolink:correlated_with",
  "object": "CHEBI:41774",
  "has_supporting_studies": {
    "PMCID:PMC11708054": {
      "id": "PMCID:PMC11708054",
      "name": "all correlations",
      "study_size": 9,
      "has_study_results": [{"id": "row:3"}]
    }
  },
  "p_value": "1.0000e-03",
  "effect_size": 0.85,
  "effect_type": "spearmans_rho",
  "supporting_text": [
    "assertion_method: Spearman correlation",
    "miscellaneous_notes: Correlation analysis between microbial composition and 13C-tamoxifen abundance after FDR correction",
    "multiple_testing_correction_method: Benjamini Hochberg"
  ]
}
```

## Template + Sections (multiple predicates)

Split one source into positive/negative correlations with two sections:

```yaml
template:
  source: {...}  # Same source
  provenance: {...}  # Same provenance

  statement:
    subject:
      method: column
      encoding: A
      prioritize: [OrganismTaxon]
      avoid: [Gene]
      regex: [...]  # Same transformations

    object:
      method: value
      encoding: CHEBI:41774

sections:
  # Section 1: Positive correlations
  - statement:
      predicate: positively_correlated_with
    source:
      reindex:
        - {column: B, comparison: gt, comparator: 0}  # Correlation coefficient

  # Section 2: Negative correlations
  - statement:
      predicate: negatively_correlated_with
    source:
      reindex:
        - {column: B, comparison: lt, comparator: 0}
```

Produces two edge sets from one table: rho > 0 and rho < 0.

---

## Dual-Column Mapping

Both subject and object come from columns, so both nodes undergo entity resolution. **Use case:**
correlation tables linking two biological entities per row (e.g., metabolite <-> microbe).

```yaml
template:
  source:
    kind: excel
    url:
      - https://pmc.ncbi.nlm.nih.gov/articles/instance/example/bin/data.xlsx
    local: ./DATALAKE/AVUTHU1.xlsx
    sheet: signif_metab_microb_corre
    row_slice: [2, auto]

  statement:
    subject:
      method: column
      encoding: A  # Column A: metabolite names
      remove: [".*_"]    # Strip trailing underscore artifacts
      prioritize: [SmallMolecule, ChemicalEntity]

    predicate: correlated_with

    object:
      method: column
      encoding: B  # Column B: microbe names
      prioritize: [OrganismTaxon]
      regex:
        - {pattern: _, replacement: ' '}   # "Lactobacillus_rhamnosus" -> "Lactobacillus rhamnosus"

  provenance:
    repo: PMC
    publication: PMC12345678

  annotations:
    - {annotation: p_value, method: column, encoding: E}
    - {annotation: effect_size, method: column, encoding: C}
    - {annotation: effect_type, method: value, encoding: correlation_coefficient}
```

Each column-mapped node gets its own `prioritize` list to guide disambiguation. `remove` strips each
listed pattern (replace with empty string); `regex` applies an ordered `pattern`->`replacement` list;
both transform cell text in place before resolution, and neither drops rows.

---

## Template + Sections (wide table)

Wide tables where each column encodes a different object (e.g., 24 metabolite columns for the same
microbe rows). Sections inherit the template's `source`, `provenance`, and `subject`, overriding only
the `object` (and optionally `row_slice`) per section: one section entry per metabolite column keeps a
24-metabolite config out of 24 separate files.

```yaml
template:
  source:
    kind: excel
    url:
      - https://pmc.ncbi.nlm.nih.gov/articles/instance/example/bin/data.xlsx
    local: ./DATALAKE/BLANTON1.xlsx
    sheet: Sheet1
    row_slice: [2, auto]

  statement:
    subject:
      method: column
      encoding: A  # Microbe names
      prioritize: [OrganismTaxon]
      avoid: [Gene]
      regex:
        - {pattern: "\\[|\\]", replacement: ""}    # Strip bracket annotations

    predicate: correlated_with

    object:
      method: value
      encoding: PLACEHOLDER  # Overridden per section

  provenance:
    repo: PMC
    publication: PMC87654321

  # Constant across every section. Template and section annotation lists are CONCATENATED,
  # so declaring the effect type once here pairs it with each section's own effect_size
  # column -- an unpaired half is dropped with an UnpairedEffectAnnotationWarning.
  annotations:
    - {annotation: effect_type, method: value, encoding: correlation_coefficient}

sections:
  # Each section targets one metabolite column

  - statement:
      object:
        method: value
        encoding: CHEBI:17196   # Glycine
    source:
      row_slice: [2, auto]
    annotations:
      - {annotation: effect_size, method: column, encoding: B}

  - statement:
      object:
        method: value
        encoding: CHEBI:16977   # Alanine
    source:
      row_slice: [2, auto]
    annotations:
      - {annotation: effect_size, method: column, encoding: C}

  # ... (one section per metabolite column; pattern repeats)
```

## Next Steps

- **[Table Configuration Reference](table.md)** - Full field documentation
- **[Tutorial](../tutorial.md)** - Start with a simpler example
