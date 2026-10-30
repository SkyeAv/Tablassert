---
title: 'Tablassert: Declarative Extraction of NCATS Translator Compliant Knowledge Graphs from Biomedical Tables'
tags:
  - knowledge graph
  - KGX
  - biolink
  - entity resolution
  - bioinformatics
authors:
  - name: Skye Lane Goetz
    affiliation: 1
    orcid: 0009-0007-8373-2352
  - name: Jared C. Roach
    affiliation: 1
    orcid: 0000-0003-4524-0107
  - name: Gwênlyn Glusman
    affiliation: 1
    orcid: 0000-0001-8060-5955
affiliations:
  - index: 1
    name: Institute for Systems Biology
date: 20 October 2026
lang: en-US
bibliography: paper.bib
---

# Summary

Tablassert turns biomedical spreadsheets (Excel, CSV, TSV) into knowledge graphs ready for NCATS
Translator [@translator]. Biomedical research routinely publishes findings as supplementary
tables: association statistics, assay readouts, curated gene lists. A user declares, in YAML,
how each table's columns map to qualified subject-predicate-object statements; Tablassert
resolves free text ("TP53", "lung cancer") to compact uniform resource identifiers (CURIEs),
attaches provenance and statistical annotations, and emits KGX-compliant [@kgx] node and edge
files leveraging the Biolink Model [@biolink-model]. You can inspect, reuse, and refine the same
recipe, so a graph starts small instead of being frozen inside a bespoke ingest script.

# Statement of need

Tablassert replaces repetitive spreadsheet-ingest code with declarative YAML recipes for
consistent formatting, resolution, and validation. Tablassert targets knowledge-graph engineers and bioinformaticians who hold tabular biomedical
sources and need rigorously structured output without maintaining a transform pipeline per
source. Extending beyond journal supplements, the same recipes apply to any relational source --
trial registries, drug approval records, or a researcher's own tables -- and each build can emit
the Resource Ingest Guide metadata required for integration with Translator.

# State of the field

Translator ingests commonly use source-specific Python with tools for exchange, schema modeling,
and transformation. KGX [@kgx] exchanges and validates graphs but provides no
column-to-statement mapping. Koza [@koza], the Monarch Initiative's [@monarch] ingest engine,
pairs YAML configuration with semi-declarative Python transforms and outputs KGX, with entity
resolution supplied through mappings or additional code. LinkML [@linkml] provides schema
modeling and validation rather than Tablassert's integrated resolution pipeline. BioCypher
[@biocypher] standardizes biomedical graph construction; OntoWeaver [@ontoweaver] provides
declarative tabular mappings for BioCypher. sPIRES [@spires] extracts knowledge from free text
by zero-shot prompting, while Tablassert's sources are already structured tables. Finally, the
hosted SRI name resolution service [@nameres] serves BABEL [@babel] identifier resources through
a network API. Hosted builds depend on service availability; reproducing historical results
requires preserving responses or pinning the underlying resources.

# Software design

Tablassert has three main trade-offs: fullmap storage enables local batched resolution without
service calls; declarative YAML favors accessibility and validation over procedural flexibility;
and the Maturin build system favors hot-path performance over implementation simplicity.

## Embedded fullmap database

The embedded fullmap database underpins the design. Built from BABEL [@babel] exports, it maps
normalized synonyms and cross-vocabulary meta-identifiers onto canonical identifiers with
category and taxon metadata. Level one trims, lowercases, Porter2-stems ASCII tokens, then
deduplicates, sorts, and joins them; level two removes non-word characters.

Synonyms and queries share normalization. Redb [@redb] offers crash-safe NoSQL point lookups
through one predictable, SQL-free query path. Graph builds never write to it, permitting
read-optimized, parallel, memory-bounded construction. The prebuilt archive for the current
BABEL 22.2.0 export is 44.5 GB compressed; the redb store used here, built from the 2026-07-22 export, occupies 92.9 GB
across one primary and 16 shard database files. Resolution is exact-match over normalized keys
rather than fuzzy search; approximate matches surface only in the audit stage, not in the graph.
To keep the database smaller and query times faster, the build applies a fixed allowlist of the
top 100 experimental taxa derived from Translator knowledge-graph node frequencies, alongside
all OrganismTaxon rows and rows without taxon metadata; this deliberate trade-off excludes some
rare species-specific identifiers outside the allowlist.

Resolution is column-aware rather than string-in/CURIE-out. Each node column declares an NCBI
taxon filter that keeps taxon-bearing matches for the declared species while retaining taxon-less rows, Biolink category prioritize and avoid lists (where avoid also
drops categories the installed model does not recognize), and CURIE prefix and regex exclusions.
Candidates are ranked by exact preferred-name agreement, normalized-name agreement, and Biolink
category priority; the best tier retains tied CURIEs. When column context is on, category frequency across the column helps break remaining
ties; this is a heuristic, not a
guarantee of semantic correctness.

## Declarative YAML configuration

Pydantic models validate recipes before execution with field-specific errors. Accepted recipes
yield correctly formatted KGX after successful builds, guaranteeing structure, not biological
correctness. Recipes require fewer changes than code, helping nontechnical contributors and AI
agents author graphs. The deliberately non-Turing-complete DSL provides common, fast
transformations rather than arbitrary computation; custom operations require extending the tool.

Recipes compile into TCode, an ordered sequence of (callable, arguments) operations executed
through Polars lazy frames. Independent transformations fuse until batched resolution, quality
control, or output writing requires materialization; sections sharing an instruction prefix reuse
its computed snapshot. Lazy optimization means logical recipe order need not dictate physical
execution order. This often improves performance but can be suboptimal for particular workloads
and prevents transformations requiring strict procedural ordering.

Edges carry recipe-declared provenance, including source URLs and publications when provided;
traceability depends on the source and configuration. Optional quality control audits each
resolved name with a four-stage cascade: exact or alias match, fuzzy string similarity,
Schwartz-Hearst abbreviation expansion, then SapBERT [@sapbert; @sapbert-renci] embedding
similarity using a checkpoint fine-tuned on BABEL synonyms
(`RenaissanceComputing/SapBERT-PubMedBERT-BabelSynonyms-v2025-01-23`), each stage handling only
what cheaper stages could not clear. Suspect mappings are dropped and logged with their scores
rather than silently retained.

For example, a DAKP FAERS audit rejected "factor ii" mapped to `HMDB:HMDB0000607`
(cyanocobalamin; SapBERT cosine 0.444): BABEL lists "Factor II" as its synonym, but the
coagulation drug context calls for prothrombin, not vitamin B12 [@prothrombin]. This is one
rejection, not measured QC precision or recall.

Output validation complements recipe validation. An optional study stage checks structural
invariants; `validate-kgx` checks category-specific Biolink records and Translator infores
identity offline, while `--prune` removes failing edges and gates the post-prune exit status.
These checks do not establish biological validity.

## Maturin build system

PyO3 Rust stems and normalizes terms, performs sharded fullmap lookups, streams database
construction, and serializes NDJSON. GIL-released Rayon workers parallelize these hot paths on
standard Python 3.11; Python retains recipe parsing, Pydantic validation, and orchestration.
Maturin packages performant native code in PyPI wheels for Linux/macOS, standard Unix-like
platforms for major bioinformatics ecosystems such as
[Bioconda](https://github.com/bioconda/bioconda-recipes). Supported users need no Rust toolchain;
Windows users need WSL or a source build. The performance gain justifies added maintenance and
readability costs.

## Comparisons

Tablassert resolved this sample faster than the separate hosted services. On a dual-socket Intel
Xeon Gold 6230 host (40 physical cores, 80 hardware threads, 187 GiB RAM, SSD RAID1, Ubuntu
24.04), 113 hand-selected entries comprised 104 unique terms. Median local time was 0.886 seconds
across three fresh-process passes, excluding startup, and 0.516 seconds across three in-process
passes; OS caches were not cleared. Local resolution returned complete identifier, name, and
category fields for 93 terms. One workflow using SRI NameResolution [@nameres], then the separate
SRI Node Normalization service [@nodenorm] returned complete top-ranked canonical records for 99
terms in 50.23 seconds: 104 lookups with four concurrent requests plus five batched normalization
requests, without errors. Hosted lookup used `limit=5` and `only_taxa=NCBITaxon:9606`;
NameResolution also supports bulk lookup, Biolink-type filters, and CURIE prefix
inclusion/exclusion. Matching policies and resource versions differ; this convenience sample
measures neither accuracy nor general service performance.

KGX's schema-driven `validate` [@kgx] likewise checks Biolink conformance; Tablassert adds
study-level structural invariants, offline infores checks, and in-place pruning.

The Tablassert tutorial uses 56 Tablassert YAML lines of code (LOC) versus 72 Koza [@koza]
YAML/Python LOC (1,152 versus 1,981 non-whitespace characters), excluding blanks, comments, and
docstrings. Koza calls Tablassert's API with identical resolution settings: both produce five
edge pairs and nine matching nodes. Tablassert additionally emits a Resource Ingest Guide,
deduplicated nodes, structured provenance, and supporting-study metadata. This compares
verbosity, not runtime or capability.

# Research impact statement

Tablassert and its declarative precursor underlie several production knowledge graphs.
MicrobiomeKG [@microbiomekg] used an early precursor; deployed Microbiome and Multiomics graphs
[@multiomics-kp] and their maintenance builds use Tablassert. Multiomics 3.1.3 contains 222,994
nodes and 18,206,965 edges from 2,136 PMC publications [@multiomics-kp]; DAKP 1.26.0 contains
16,257 nodes and 135,263 edges [@drug-approvals]. The Drug Approvals Knowledge Provider (DAKP)
[@dakp], developed in the glusman-team monorepo [@graph-monorepo], publishes validated,
versioned drug-approval graphs [@drug-approvals] and per-edge evidence linkouts. Translator
ingest integration is under review [@ingests], starting with a Multiomics passthrough
[@ingests-mokg]. KRAKEN [@kraken], Phenome Health's unified knowledge graph, integrates the
Microbiome, Multiomics, and Drug Approvals graphs as domain-specific sources. A rare-disease
multiomics knowledge graph built with Tablassert is in preparation.

# AI usage disclosure

Generative AI assisted with code generation, test writing, documentation, and drafting and
revising this manuscript, primarily through a customized Pi coding agent (version 0.81.1). AI
spelling and grammar checks covered documentation and the manuscript, including Grammarly for
the manuscript. Coding-agent use began on 27 January 2026 (`50f29e8`); co-author credits record
Claude Sonnet 4.5 and Opus 4.5/4.6 (context variants grouped). Initial versions used no coding
agents.

Models used included `claude-opus-5`, `claude-opus-5-5`, `claude-sonnet-5`, `claude-sonnet-5-5`,
`deepseek-v4-pro`, `deepseek-v4-flash`, `gemini-3.8-flash`, `glm-5.3`, `glm-5.3-flash`,
`gpt-5.6-luna`, `gpt-5.6-sol-review`, `gpt-5.6-terra`, `gpt-6-astra`, `gpt-6-luna`, `gpt-6-sol`,
`gpt-6.1-sol`, `kimi-k2.5`, `kimi-k2.6`, `kimi-k2.7-pro`, `kimi-k2.8-preview`, `kimi-k3`,
`kimi-k3.1-pro`, `muse-spark-1.3-contributor-free`, `nemotron-3-ultra-550b-a55b:free`,
`laguna-s-2.1`, `qwen3.8-flash`, `qwen3.8-max`, `qwen3.8-max-preview`, `mimo-v2.5`, and
`mimo-v2.6-pro`.

A bespoke LLM-as-judge implementation reviewed code before pull requests; CodeRabbit
occasionally reviewed pull requests before Goetz. Validation combined manual auditing, automated
tests, CI/CD, Goetz's custom linting rules, scripted inclusion checks, Git pre-commit hooks, and
language-server diagnostics. Detailed prompt engineering used promptfoo (0.121.17) and GEPA
(0.1.4) to optimize outputs, evaluated with LLM judges and inclusion checks. Prompt optimization
and AI evaluation supplemented human review without independently proving correctness. The
authors made all software architecture decisions and manually reviewed and audited all changes,
including AI-assisted outputs. The authors retain full responsibility for the accuracy,
originality, licensing, and ethical and legal compliance of the submitted software and
manuscript.

# Author contributions

Skye L. Goetz developed the software, led design, and performed analyses. Jared C. Roach and
Gwênlyn Glusman helped design the software and analyses and provided detailed project feedback.
All authors wrote, revised, and proofread the manuscript.

# Acknowledgements

This work was supported by the National Science Foundation (award 2150265) and by the National
Center for Advancing Translational Sciences, Biomedical Translator Program (Other Transaction
Awards OT2TR003443, OT2TR003428, and OT2TR005706). Any opinions expressed in this manuscript are
those of the Translator community at large and do not necessarily reflect the views of NCATS,
individual Translator team members, or affiliated organizations and institutions.

NCATS provided financial support but had no role in software design, analysis, or manuscript
preparation. Consortium authorship requirements were clarified through committee consultation.

# References
