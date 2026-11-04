---
title: 'Tablassert: Declarative Extraction of NCATS Translator-Compliant Knowledge Graphs from Biomedical Tables'
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

Most information in biomedical research papers is published in tables, particularly supplemental tables. These tables include association statistics, assay readouts, curated gene lists, omics data, and other information. Much of this information encodes relationships between biological entities (e.g., between row and column labels). Tablassert converts such spreadsheets (Excel, CSV, TSV) into knowledge graphs ready for the NCATS Biomedical Data Translator [@translator]. A user declares in YAML how each table's data map to subject-predicate-object statements; Tablassert resolves free text (e.g., "lung cancer") to standard identifiers, attaches provenance and statistics, and emits graph files conforming to community standards [@kgx; @biolink-model].  

# Statement of Need

Biomedical knowledge graphs classically rely on bespoke ingest scripts for each source. When these data sources lack standardization, such as across the millions of tables published in the biomedical literature, bespoke scripts impose three costs that limit scalability. First, mapping from each table's rows, columns, and cells to biological assertions is idiosyncratically encoded, so it cannot easily be inspected, reviewed, or reused. Second, entity resolution may depend on hosted network services, so graph builds may not be reproducible. Third, and most importantly, because each source requires a new pipeline, most published tables are never ingested. The process is not currently scalable.

Tablassert targets knowledge-graph engineers and bioinformaticians with tabular biomedical information sources who need Biolink-conformant output. Declarative YAML recipes make data-to-statement mapping scalable, inspectable, and reusable. Batched resolution against an embedded identifier database removes the runtime service dependency. The same recipes extend beyond journal supplemental tables to any relational source (trial registries, drug approval records, or a researcher's own tables), and each build can emit the Resource Ingest Guide metadata required for integration with Translator.

# State of the Field

Several tools address aspects of Translator data ingestion. KGX [@kgx] exchanges and validates graphs, including schema-driven Biolink conformance checks, but provides no column-to-statement mapping, study-level structural invariants, offline Translator infores checks, or in-place pruning. Additionally, LinkML [@linkml] models and validates schemas but does not perform entity resolution. Koza [@koza], the Monarch Initiative’s [@monarch] ingest engine, pairs YAML configuration with semi-declarative Python transforms, so complex sources often require repetitive procedural code and external mappings for resolution. Further, BioCypher [@biocypher] standardizes biomedical graph construction, to which OntoWeaver [@ontoweaver] adds declarative tabular mappings; however, Translator Biolink qualifier semantics and Resource Ingest Guide metadata fall outside their scope. SPIRES [@spires] extracts assertions from free text by zero-shot prompting instead of semi-structured tables. Finally, the hosted SRI NameResolution service [@nameres] and separate Node Normalization service [@nodenorm] provide identifier resolution and canonical records through network APIs, but hosted workflows depend on service availability, making it difficult to reproduce historical results without preserving responses or pinning complex underlying resources.

We built Tablassert rather than contributing to an existing project for three reasons. First, Tablassert’s recipe model is not backward-compatible with the translator-ingests transform interface [@translator-ingests]; migrating would discard the provenance, certain edge types, and several qualifiers attributed to each edge. Second, Tablassert targets many heterogeneous tables, so it was designed to make additions easy by editing simple recipes rather than crafting new code. Third, deterministic Tablassert builds run locally on demand, whereas shared ingests run as periodic batch jobs with unpinnable dependencies, slowing iteration and harming reproducibility.

# Software Design

Tablassert has three main trade-offs: an embedded NoSQL (fullmap) database enables local batched resolution without service calls; declarative YAML favors accessibility, validation, and speed over procedural flexibility; and a hybrid Python-Rust Maturin build system favors hot-path performance over implementation simplicity.

## Embedded fullmap database

Built from BABEL [@babel] exports, the embedded fullmap maps normalized synonyms and cross-vocabulary meta-identifiers to canonical identifiers with category and taxon metadata. Synonyms and queries share normalization. Redb [@redb] provides crash-safe NoSQL point lookups; graph builds treat the store as read-only, supporting parallel, memory-bounded access without SQL or runtime service calls.

Storage is the main cost: the current BABEL 22.2.0 archive is 44.5 GB compressed, while the 2026-07-22 redb store benchmarked here occupies 92.9 GB. Resolution uses exact matches over normalized keys. To bound storage and lookup cost, the build retains the top 100 experimental taxa by Translator knowledge-graph node frequency, all OrganismTaxon rows, and rows without taxon metadata, potentially excluding rare species-specific identifiers. Sixteen smaller shards accelerate lookups and support multithreaded reads. After the initial build, read-only access needs no writer coordination, and a bounded in-process cache reuses hits and misses with minimal memory and synchronization overhead.

Each node column declares an NCBI taxon filter that retains matches for the declared species and taxon-less rows, Biolink category priority and avoid lists, and CURIE-prefix and regex exclusions. Before lookup, raw strings are trimmed, stripped of surrounding quotes, and lowercased. ASCII alphabetic tokens are stemmed, duplicates are removed, and the remaining tokens are sorted so repeated words and word order do not affect matching. A second normalization uses a regular expression to remove runs of non-word characters (punctuation, whitespace, and other symbols) while retaining letters, digits, and underscores. Candidates are ranked by exact preferred-name agreement, normalized-name agreement, category priority, and the normalization level required for the match, with ties retained in the best tier. Finally, optional column-category frequencies provide a heuristic tie-breaker.

Optional quality control applies a four-stage cascade: exact or alias matching, fuzzy string similarity, Schwartz-Hearst abbreviation expansion, then SapBERT [@sapbert; @sapbert-renci] embedding similarity using a checkpoint fine-tuned on BABEL synonyms (`RenaissanceComputing/SapBERT-PubMedBERT-BabelSynonyms-v2025-01-23`). Each stage handles only what cheaper stages could not clear, and suspect mappings are dropped and logged with scores. For example, an audit rejected "factor ii" mapped to `HMDB:HMDB0000607` (cyanocobalamin; SapBERT cosine 0.444): BABEL lists "Factor II" as its synonym, but the coagulation drug context calls for prothrombin, not vitamin B12 [@prothrombin].

Using the 2026-07-22 BABEL fullmap on a dual-socket Intel Xeon Gold 6230 host, Tablassert resolved 104 hand-selected unique terms in a median of 0.886 seconds over three fresh-process runs and 0.516 seconds over three in-process runs, returning complete identifier, name, and category records for 93 terms.

## Declarative YAML configuration

Pydantic models validate recipes before execution with field-specific errors. The non-Turing-complete DSL favors common, fast transformations over arbitrary computation, reducing the code changes needed for new sources and helping nontechnical contributors and AI agents author graphs.

Recipes compile into TCode, an ordered sequence of (callable, arguments) operations executed through Polars lazy frames. Independent transformations fuse until batched resolution, quality control, or output writing requires materialization. Sections are statement mappings that inherit a template's shared source, provenance, metadata, and qualifier declarations; for example, a section can select another entity column and predicate while reusing everything else, avoiding a second nearly identical recipe. Sections that share the same instructions reuse snapshots from a cryptographic cache. Further, lazy optimization decouples recipe order from execution order: it often improves performance but can be suboptimal for some workloads and rules out transformations that require strict procedural ordering.

Builds emit deduplicated KGX nodes and can emit a Resource Ingest Guide. Edges carry recipe-declared structured provenance, supporting-study metadata, and statistical qualifiers, including source URLs and publications when provided; traceability depends on the source and configuration.

An optional study stage checks structural invariants; `validate-kgx` checks category-specific Biolink records and Translator infores identity offline, while `--prune` removes failing edges and gates the post-prune exit status. These validations establish structure, not biological validity.

## Maturin build system

PyO3 Rust handles normalization, sharded fullmap lookups, streamed database construction, and NDJSON serialization; GIL-released Rayon workers parallelize these hot paths on standard CPython, while Python handles recipe parsing, Pydantic validation, and orchestration. Maturin builds Linux/macOS PyPI wheels for Unix-like bioinformatics platforms such as Bioconda [@bioconda]. Wheel users need no Rust toolchain; Windows requires WSL or a source build. This design targets hot-path performance at the cost of added implementation complexity.

# Research Impact Statement

Tablassert and its predecessor underlie several production knowledge graphs. MicrobiomeKG [@microbiomekg] was built with an earlier version; the deployed Microbiome and Multiomics knowledge providers (MOKP) [@multiomics-kp] and their maintenance builds use Tablassert. The Drug Approvals Knowledge Provider (DAKP) [@dakp], developed in the glusman-team monorepo [@graph-monorepo], publishes validated, versioned drug-approval graphs with per-edge evidence linkouts [@drug-approvals]. As of release, Multiomics 3.1.3 contains 222,994 nodes and 18,206,965 edges derived from 2,136 PMC publications [@multiomics-kp], and DAKP 1.26.0 contains 16,257 nodes and 135,263 edges [@drug-approvals]. KRAKEN [@kraken], Phenome Health's unified knowledge graph, integrates the Microbiome, Multiomics, and Drug Approvals graphs as domain-specific sources. Translator ingest integration is in progress, beginning with a Multiomics passthrough [@ingests; @ingests-mokg]. Future directions include expanding MOKP to integrate a comprehensive subset of rare disease supplemental tables. 

# AI Usage Disclosure

Generative AI assisted with code generation, test writing, documentation, drafting, and revising. Coding-agent use began on 27 January 2026 (`50f29e8`). Models used included Grammarly, a customized Pi coding agent (version 0.81.1), `claude-opus-5`, `claude-sonnet-5`, `deepseek-v4`, `gemini-3.8-flash`, `glm-5.3`, `gpt-5.6`, `gpt-6`, `kimi-k2`, `kimi-k3`, `muse-spark-1.3-contributor`, `nemotron-3-ultra`, `laguna-s-2.1`, `qwen3.8`, `mimo-v2.5`, and `mimo-v2.6-pro`. Detailed prompt engineering used promptfoo (0.121.17) and GEPA (0.1.4). Prompt optimization and AI evaluation supplemented human review but did not independently prove correctness. Authors made all architecture decisions and reviewed all outputs. The authors retain full responsibility for all aspects of the software and manuscript.

# Author Contributions

Skye Goetz developed the software, led design, and performed analyses. Jared Roach and Gwênlyn Glusman helped design the software and analyses and provided feedback. All authors wrote the manuscript.

# Acknowledgements

This work was supported by the National Science Foundation (Award 2150265) and by the National Center for Advancing Translational Sciences, Biomedical Translator Program (Other Transaction Awards OT2TR003443 and OT2TR005706). Any opinions expressed in this manuscript are those of the Translator community at large and do not necessarily reflect the views of NCATS, individual Translator team members, or affiliated organizations and institutions.

NCATS provided financial support but had no role in software design, analysis, or manuscript preparation. Consortium authorship requirements were clarified through committee consultation.

# References
