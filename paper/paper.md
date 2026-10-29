---
title: 'Tablassert: Declarative Construction of NCATS Translator Knowledge Graphs from Biomedical Tables'
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
consistent formatting, resolution, and validation.

Tablassert targets knowledge-graph engineers and bioinformaticians who hold tabular biomedical
sources and need rigorously structured output without maintaining a transform pipeline per
source. Extending beyond journal supplements, the same recipes apply to any relational source --
trial registries, drug approval records, or a researcher's own tables -- and each build can emit
the Resource Ingest Guide metadata required for integration with Translator. Recipes are
validated before a build starts, so configuration errors fail early with field-specific
messages.

# State of the field

Translator ingests commonly use source-specific Python with tools for exchange, schema modeling,
and transformation. KGX [@kgx] exchanges and validates graphs but provides no
column-to-statement mapping. Koza [@koza], the Monarch Initiative's [@monarch] ingest engine,
pairs YAML configuration with semi-declarative Python transforms and outputs KGX, with entity
resolution supplied through mappings or additional code. LinkML [@linkml] provides schema
modeling and validation rather than Tablassert's integrated resolution pipeline. BioCypher
[@biocypher] standardizes biomedical graph construction; OntoWeaver [@ontoweaver] provides
declarative tabular mappings for BioCypher. These are related approaches, not inherently
unsuitable for research tables. SPIRES [@spires] extracts knowledge from free text by zero-shot
prompting, while Tablassert's sources are already structured tables. Finally, the hosted SRI
name resolution service [@nameres] serves BABEL [@babel] identifier resources through a network
API. Hosted builds depend on service availability; reproducing historical results requires
preserving responses or pinning the underlying resources.

Tablassert composes declarative mapping, local resolution, and mapping-quality checks into one
recipe-driven pipeline.

# Software design

Tablassert's core trade-off is up-front work for build-time ease: a large entity-resolution
database is built (or downloaded) once, and every subsequent build resolves millions of terms
locally in batched lookups without service calls. Recipes are compiled into TCode, an ordered
sequence of (callable, arguments) operations, executed through Polars lazy frames. Independent
transformations fuse until batched resolution, quality control, or output writing requires
materialization. Sections sharing an instruction prefix reuse its computed snapshot.

The embedded fullmap database underpins the design. Built from BABEL [@babel] exports, it maps
normalized synonyms, including the meta-identifiers that unify the same concept across source
vocabularies, onto canonical identifiers with category and taxon metadata. Level-one
normalization trims whitespace and quotes, Unicode-lowercases text, Porter2-stems ASCII
alphabetic tokens, then deduplicates, sorts, and joins tokens; level two additionally removes
non-word characters. Identical normalization is applied to synonyms and query text. The store
itself is a set of redb [@redb] key-value files -- an embedded, crash-safe Rust engine chosen
over analytical stores because resolution is point-lookup-shaped, not query-shaped -- built in a
parallel, memory-bounded streaming pass. The prebuilt archive for the current BABEL 22.2.0
export is 44.5 GB compressed; the redb store used here, built from the 2026-07-22 export,
occupies 92.9 GB across one primary and 16 shard files. What does not map is reported, not
guessed, and metadata arrives with the identifier. Resolution is exact-match over normalized
keys rather than fuzzy search -- approximate matches surface only in the audit stage, not in the
graph -- to avoid silently accepting plausible but incorrect approximate matches. To keep the
database smaller and query times faster, the build applies a fixed allowlist of the top 100
experimental taxa derived from Translator knowledge-graph node frequencies, alongside all
OrganismTaxon rows and rows without taxon metadata; this deliberate trade-off excludes some rare
species-specific identifiers outside the allowlist.

Resolution is column-aware rather than string-in/CURIE-out. Each node column carries its own
constraints: an NCBI taxon filter that keeps taxon-bearing matches for the declared species
while retaining taxon-less rows, Biolink category prioritize and avoid lists (where avoid also
drops categories the installed model does not recognize), and CURIE prefix and regex exclusions.
A priority score ranks surviving candidates -- exact preferred-name equality ranks 1,
level-one-normalized equality at NLP level 1 ranks 5, everything else 10, multiplied by a
category bonus of 1 for prioritized categories and 50 otherwise -- then deduplicates them to the
best tier, retaining distinct CURIEs that tie. When column context is on, category frequency
across the column being resolved helps break remaining ties; this is a heuristic, not a
guarantee of semantic correctness. Hosted NameResolution also supports bulk lookup, taxon and
Biolink-type filters, and CURIE prefix inclusion/exclusion. Fullmap additionally integrates
per-column category-ranking policies, regex exclusions, and column-context ranking. Hosted
candidates can be canonicalized through batched Node Normalization requests; two network stages
need not mean two requests per term. On a dual-socket Intel Xeon Gold 6230 host (40 physical
cores, 80 hardware threads, 187 GiB RAM, SSD RAID1, Ubuntu 24.04), 113 hand-selected ingest
entries comprised 104 unique terms. Three fresh-process local batch passes took a median 0.886
seconds, excluding interpreter startup; three in-process passes took a median 0.516 seconds. OS
caches were not cleared. Local resolution returned complete identifier, name, and category
fields for 93 terms. One hosted sweep returned complete top-ranked canonical records for 99
terms in 50.23 seconds: 104 concurrent-limited lookups plus five batched normalization requests,
with no request errors. Matching policies and resource versions differ; this convenience sample
measures neither accuracy nor general service performance.

Edges carry recipe-declared provenance, including source URLs and publications when provided;
traceability depends on the source and configuration. Optional quality control audits each
resolved name with a four-stage cascade: exact or alias match, fuzzy string similarity,
Schwartz-Hearst abbreviation expansion, then SapBERT [@sapbert] embedding similarity using a
checkpoint fine-tuned on BABEL synonyms
(`RenaissanceComputing/SapBERT-PubMedBERT-BabelSynonyms-v2025-01-23`), each stage handling only
what cheaper stages could not clear. Suspect mappings are dropped and logged with their scores
rather than silently retained. For example, a DAKP FAERS audit (2026-10-22) rejected "factor ii"
mapped to `HMDB:HMDB0000607` (cyanocobalamin; SapBERT cosine 0.444): BABEL lists "Factor II" as
its synonym, but the coagulation drug context calls for prothrombin, not vitamin B12
[@prothrombin]. This is one rejection, not measured QC precision or recall. Structural
validation is separate from scientific correctness: an optional study stage asserts structural
invariants over the emitted NDJSON (no duplicate or isolated nodes, no empty required fields, no
edges demoted to bare Association). Like KGX's schema-driven `validate` [@kgx], `validate-kgx`
checks graph records; it constructs records using category-specific Biolink classes,
knowledge-source identity is checked against the Translator infores registry offline, and
`--prune` removes edges failing its checks in place, with the exit code gating the post-prune
state. Passing these checks does not establish biological validity. Tablassert uses PyO3 Rust
for normalization, resolution, database construction, and NDJSON writing. GIL-released Rust
regions use Rayon workers for CPU-heavy normalization and sharded lookups, enabling parallel
execution even on GIL-enabled Python 3.11, without requiring free-threaded Python. This removes
per-term Python callbacks and serial lookup bottlenecks; recipes and validation remain Python.
The tutorial uses 29 recipe lines plus a 24-line graph wrapper; Koza's mapping-only version uses
25 YAML and 31 Python lines, excluding comments and docstrings. Koza supports custom hosted
resolution, avoiding local storage but adding code and service dependencies. Tablassert
integrates resolution but requires the fullmap. Its retained ties differ from the hosted
transform's top-hit policy, precluding identical-graph runtime claims.

# Research impact statement

Tablassert and its declarative precursor underlie several production knowledge graphs. The
published MicrobiomeKG [@microbiomekg], a Translator-deployed graph built from supplementary
tables of 40 publications, was produced with an early unnamed precursor; the deployed Microbiome
and Multiomics knowledge graphs [@multiomics-kp] and their maintenance builds use present-day
Tablassert. Multiomics 3.1.3 contains 222,994 nodes and 18,206,965 edges from 2,136 PMC
publications [@multiomics-kp]; DAKP 1.26.0 contains 16,257 nodes and 135,263 edges
[@drug-approvals]. DAKP [@dakp], the Drug Approvals Knowledge Provider (now developed in the
glusman-team graph monorepo [@graph-monorepo]), applies Tablassert to regulatory drug-approval
sources and publishes validated, versioned releases to a public dataset archive
[@drug-approvals]; a public linkouts interface exposes its per-edge evidence, and integration
into the Translator ingest infrastructure is under review [@ingests], beginning with a
Tablassert passthrough for the Multiomics graph [@ingests-mokg]. KRAKEN [@kraken], Phenome
Health's unified knowledge graph, integrates the Microbiome, Multiomics, and Drug Approvals
graphs as domain-specific sources. A rare-disease multiomics knowledge graph built with
Tablassert is in preparation.

# AI usage disclosure

Generative AI assisted with code generation, test writing, documentation, and drafting and
revising this manuscript, primarily through a customized Pi coding agent (version 0.81.1). AI
spelling and grammar checks covered documentation and manuscript, including Grammarly for the
manuscript. Coding-agent use began on 27 January 2026 (`50f29e8`); co-author credits record
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

Skye Lane Goetz developed the software, led design, and performed analyses. Jared C. Roach and
Gwênlyn Glusman helped design the software and analyses and provided detailed project feedback.
All authors will write, revise, and proofread.

# Acknowledgements

This work was supported by the National Science Foundation (award 2150265) and by the National
Center for Advancing Translational Sciences, Biomedical Translator Program (Other Transaction
Awards OT2TR003443, OT2TR003428, and OT2TR005706). Any opinions expressed in this manuscript are
those of the Translator community at large and do not necessarily reflect the views of NCATS,
individual Translator team members, or affiliated organizations and institutions.

The funders provided financial support but had no role in software design, analysis, or
manuscript preparation. Consortium authorship requirements were clarified through committee
consultation.

# References
