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
Translator [@translator]. Biomedical research routinely publishes its findings as supplementary
tables: association statistics, assay readouts, curated gene lists. A user declares, in YAML, how
each table's columns map to subject-predicate-object statements; Tablassert resolves free text
("TP53", "lung cancer") to compact uniform resource identifiers (CURIEs), attaches provenance and
statistical annotations, and emits KGX-compliant [@kgx] node and edge files carrying Biolink Model
categories [@biolink-model]. The same recipe can be inspected, reused, and refined, so a graph
starts small instead of being frozen inside a bespoke ingest script.

# Statement of need

Tablassert grew out of a recurring research task: converting one paper's supplementary spreadsheet
into a Translator knowledge graph, then another, then another. Each hand-written ingest repeated
the same structure -- read the table, map columns to Biolink statements, resolve names, attach
provenance -- until near-identical codebases outnumbered the tables they served. Tablassert
replaces that per-source code with a declarative recipe: a few lines of YAML per table describe
what each row asserts, and the package handles formatting, resolution, and validation the same way
every time.

Tablassert targets knowledge-graph engineers and bioinformaticians who hold tabular biomedical
sources and need rigorously structured output without maintaining a transform pipeline per source.
Beyond journal supplements, the same recipes apply to any relational source -- trial registries,
drug approval records, or a researcher's own tables -- and each build can emit the Resource Ingest
Guide metadata that Translator submissions require. Recipes are validated before a build
starts, so a typo fails immediately with an error naming the field, rather than surfacing as a
malformed graph hours later.

# State of the field

The prevailing practice for Translator ingests is bespoke Python per data source, with generic tools at each layer that none of them composes. KGX [@kgx] exchanges and validates graphs but provides no column-to-statement mapping. Koza [@koza], the Monarch Initiative's ingest engine, pairs YAML configuration with semi-declarative Python transforms and outputs KGX, but leaves entity resolution to separate services or mappings. LinkML [@linkml] models the schema of a finished knowledge graph generically, but a schema says nothing about how a spreadsheet column becomes a node. The hosted SRI name resolution service [@nameres] serves BABEL [@babel] identifier mappings as a network API, which makes build-time bulk lookups slow and builds themselves irreproducible without network access.
Extending those tools would not remove the cost Tablassert targets: repeated ingest code, the
hosted-resolution dependency, and no integrated mapping-quality checks. Tablassert integrates all
three into one recipe-driven pipeline; contributing resolution or audit stages back to Koza would
still not yield a single declarative mapping.

# Software design

Tablassert's core trade-off is buy-time work for build-time ease: a large entity-resolution
database is built (or downloaded) once, and every subsequent build resolves millions of terms
locally in batched lookups without service calls. Recipes are compiled into TCode, an ordered
sequence of (callable, arguments) operations, executed through Polars lazy frames so independent
transformations fuse and materialize only at explicit boundaries: batched resolution, quality
control, and output writing. Because operations are first-class values, sections that share a
prefix of instructions reuse its computed snapshot instead of recomputing it.

The embedded fullmap database is the design's foundation. Built from BABEL [@babel] exports, it
maps normalized synonyms, including the meta-identifiers that unify the same concept across source
vocabularies, onto canonical identifiers with category and taxon metadata. The store itself is a
set of redb [@redb] key-value files -- an embedded, crash-safe Rust engine chosen over
analytical stores because resolution is point-lookup-shaped, not query-shaped -- built in a
parallel, memory-bounded streaming pass, and partitioned into shards that serve concurrent batch
lookups. The prebuilt archive for the current BABEL 22.2.0 export is 44.5 GB compressed; the redb
store used here, built from the 2026-07-22 export, occupies 24.7 GB. What does not map is
reported, not guessed, and metadata arrives with the identifier. Resolution is exact-match over
normalized keys rather than fuzzy search -- approximate matches surface only in the audit stage,
not in the graph -- because fuzzy resolution returns plausible-but-wrong identifiers often enough
to corrupt a graph silently. The build applies a fixed allowlist of the top 100 experimental taxa
derived from Translator knowledge-graph node frequencies, alongside all OrganismTaxon rows and
rows without taxon metadata: a deliberate trade-off that shrinks the database but drops
species-specific identifiers outside the allowlist.

Resolution is column-aware rather than string-in/CURIE-out. Each node column carries its own
constraints: an NCBI taxon filter that keeps taxon-bearing matches for the declared species while
retaining taxon-less rows, Biolink category prioritize and avoid lists (where avoid also drops
categories the installed model does not recognize), and CURIE prefix and regex exclusions.
Surviving candidates are ranked by a priority score -- exact preferred-name equality ranks 1,
level-one-normalized equality at NLP level 1 ranks 5, everything else 10, multiplied by a category
bonus of 1 for prioritized categories and 50 otherwise -- then deduplicated to the best tier,
retaining distinct CURIEs that tie. When column context is on, category frequency across the
column being resolved breaks remaining ties, so a column of gene symbols resolves ambiguous short
names as genes. These are query shapes the hosted API does not offer: it takes a string and returns
ranked identifiers, with no taxon constraint, category policy, or per-column context, and the full
identifier-record workflow (string to identifier, then canonicalization for preferred name and
category) takes two HTTP calls per term where the fullmap returns all of it in one batched fetch
(see Benchmarks).

Every edge carries recipe-declared structured provenance -- source record URLs, supporting
publications, and the processing context that produced it -- so an assertion can be traced back to
the exact row and evidence that support it. Optional quality control audits each resolved name
with a four-stage cascade: exact or alias match, fuzzy string similarity, Schwartz-Hearst
abbreviation expansion, then SapBERT [@sapbert] embedding similarity using a checkpoint fine-tuned
on BABEL synonyms (`RenaissanceComputing/SapBERT-PubMedBERT-BabelSynonyms-v2025-01-23`), each
stage handling only what cheaper stages could not clear. Suspect mappings are dropped and logged
with their scores rather than silently retained; in the DAKP production builds this audit covers
the full graph -- 1,661,094 audited rows in the current release -- which in practice catches
resolution errors that upstream synonym sources are slow to fix. Output correctness is checked in
two passes: an optional study stage asserts structural invariants over the emitted NDJSON (no
duplicate or isolated nodes, no empty required fields, no edges demoted to bare Association). The
`validate-kgx` command goes beyond KGX's schema-driven `validate` [@kgx]: every node and edge is
constructed as the Biolink class its own category names, knowledge-source identity is checked
against the Translator infores registry offline, and `--prune` rewrites the edges file in place to
its strictly-compliant subset, with the exit code gating the post-prune state. Tablassert
is a Python package with a PyO3 Rust extension; the resolution engine, database build, and NDJSON
writer run in Rust while recipes and validation remain Python.

# Benchmarks

Raw results ship in `paper/benchmarks/`. On 113 real ingest terms, local
fullmap resolution sustained 171 unique terms per second through the build's batch path,
returning the full identifier record in one fetch; the hosted SRI service spent a median of 0.55
seconds per call, and its two-call identifier-record workflow 0.58 seconds per term. The Koza
contrast maps the tutorial example both ways: 79 lines of YAML plus Python with no entity
resolution, versus the 32-line Tablassert recipe (10,000 rows: 6.9 seconds including resolution
against the fullmap, 5.4 seconds without it).

# Research impact statement

Tablassert and its declarative precursor underlie several production knowledge graphs. The
published MicrobiomeKG [@microbiomekg], a Translator-deployed graph built from supplementary
tables of 40 publications, was produced with an early unnamed precursor; the deployed Microbiome
and Multiomics knowledge graphs [@multiomics-kp] and their maintenance builds use present-day
Tablassert. DAKP [@dakp], the Drug Approvals Knowledge Provider (now developed in the glusman-team
graph monorepo [@graph-monorepo]), applies Tablassert to regulatory drug-approval sources and
publishes validated, versioned releases to a public dataset archive [@drug-approvals]; a public
linkouts interface exposes its per-edge evidence, and integration into the Translator ingest
infrastructure is under review [@ingests], beginning with a Tablassert passthrough for the
Multiomics graph [@ingests-mokg].
KRAKEN [@kraken], Phenome Health's unified knowledge graph, integrates the Microbiome, Multiomics,
and Drug Approvals graphs as domain-specific sources. A rare-disease multiomics knowledge graph
built with Tablassert is in preparation.

# AI usage disclosure

Generative AI assisted with code generation, test writing, documentation, and drafting and
revising this manuscript, primarily through a customized pi coding agent (version 0.81.1). AI spelling and
grammar checks were used for the documentation and manuscript; Grammarly also assisted with
the manuscript. AI coding-agent use began on 27 January 2026, marked by commit `50f29e8`,
which credits Claude Sonnet 4.5; subsequent commits also credit Claude Opus 4.5. Initial
software versions were developed without AI coding agents.

Commit co-author credits additionally record Claude Sonnet 4.5, Claude Opus 4.5, and
Claude Opus 4.6, with context-window variants grouped under the same model version.
Models used included the following recorded identifiers:
`claude-opus-5`, `claude-opus-5-5`,
`claude-sonnet-5`, `claude-sonnet-5-5`, `deepseek-v4-pro`, `deepseek-v4-flash`,
`gemini-3.8-flash`, `glm-5.3`,
`glm-5.3-flash`, `gpt-5.6-luna`, `gpt-5.6-sol-review`, `gpt-5.6-terra`, `gpt-6-astra`,
`gpt-6-luna`, `gpt-6-sol`, `gpt-6.1-sol`, `kimi-k2.5`, `kimi-k2.6`, `kimi-k2.7-pro`,
`kimi-k2.8-preview`, `kimi-k3`, `kimi-k3.1-pro`,
`muse-spark-1.3-contributor-free`, `nemotron-3-ultra-550b-a55b:free`,
`laguna-s-2.1`, `qwen3.8-flash`, `qwen3.8-max`, `qwen3.8-max-preview`, `mimo-v2.5`, and
`mimo-v2.6-pro`.

A bespoke LLM-as-judge implementation reviewed code before pull requests, and CodeRabbit
reviewed pull requests before Goetz's own review. Validation combined manual review and
auditing with automated tests, CI/CD pipelines, custom linting rules written by Goetz, and
scripted inclusion checks for required output elements. Git pre-commit hooks provided earlier
checks to shorten CI feedback, and language servers provided development-time diagnostics.
Detailed prompt engineering, with promptfoo (version 0.121.17) and GEPA (version 0.1.4) used to
optimize prompts for desired
outputs, supported output control; LLM-as-judge evaluations and scripted inclusion checks
assessed outputs. Prompt optimization and AI-based evaluation were not treated as independent
proof of correctness. These checks supplemented, rather than replaced, human review.
Skye Lane Goetz made all software architecture decisions and manually reviewed and audited
all changes, including AI-assisted outputs. The authors retain full responsibility for the
accuracy, originality, licensing, and ethical and legal compliance of the submitted software
and manuscript.

# Acknowledgements

This work was supported by the National Science Foundation (award 2150265) and by the National
Center for Advancing Translational Sciences, Biomedical Translator Program (Other Transaction
Awards OT2TR003443, OT2TR003428, and OT2TR005706). Any opinions expressed in this manuscript are
those of the Translator community at large and do not necessarily reflect the views of NCATS,
individual Translator team members, or affiliated organizations and institutions.

TODO(collaborators): contributor acknowledgements and sponsor-involvement statement go here.

# References
