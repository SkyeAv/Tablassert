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
starts small and improves iteratively instead of being frozen inside a bespoke ingest script.

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
Guide metadata that Translator submissions require. Recipes are validated with Pydantic before a
build starts, so a typo or an impossible combination of options fails immediately with an error
naming the offending field, rather than surfacing as a malformed graph hours later.

# State of the field

The prevailing practice for Translator ingests is bespoke Python per data source. Generic KGX
tooling [@kgx] exchanges and validates graphs but provides no column-to-statement mapping. Koza
[@koza], the Monarch Initiative's ingest engine, pairs YAML configuration with semi-declarative
Python transforms and outputs KGX, but leaves entity resolution to separate services or mappings.
Hosted Translator name resolution [@babel] resolves identifiers as a network service, which makes
build-time bulk lookups slow and builds themselves irreproducible without network access.
Extending those tools would not remove the cost Tablassert targets: the repeated ingest code, the
hosted-resolution dependency, and the lack of integrated mapping-quality checks. Tablassert
integrates all three into one recipe-driven pipeline, and contributing resolution or audit stages
back to Koza would not produce a single declarative YAML mapping covering both.

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
lookups. Restricting resolution to BABEL-covered entities is deliberate: what does not map is
reported, not guessed, and metadata arrives with the identifier. A prebuilt fullmap can be
downloaded from RENCI alongside building one locally.

Every emitted edge carries structured provenance -- source record URLs, supporting publications,
and the processing context that produced it -- so an assertion can be traced back to the exact row
and evidence that support it. Optional quality control audits each resolved name with a four-stage
cascade: exact or alias match, fuzzy string similarity, Schwartz-Hearst abbreviation expansion,
then SapBERT [@sapbert] embedding similarity, each stage handling only what cheaper stages could
not clear. Suspect mappings are dropped and logged with their scores rather than silently
retained, which in practice catches resolution errors that upstream synonym sources are slow to
fix. Final output is validated against category-specific Biolink schema classes, so a passing
build means the emitted graph, not just its configuration, is well-formed. Tablassert is a Python
package with a PyO3 Rust extension; the resolution engine, database build, and NDJSON writer run
in Rust while recipes and validation remain Python.

# Research impact statement

Tablassert and its declarative precursor underlie several production knowledge graphs. The
published MicrobiomeKG [@microbiomekg], a Translator-deployed graph built from supplementary
tables of 40 publications, was produced with an early unnamed precursor; its current maintenance
builds use present-day Tablassert, as do the Microbiome and Multiomics knowledge graph
configurations of the MOKG project. DAKP [@dakp], the Drug Approvals Knowledge Provider, applies
Tablassert to regulatory drug-approval sources and publishes validated, versioned releases to a
public dataset archive [@drug-approvals]; a public linkouts interface exposes its per-edge
evidence, and integration into the Translator ingest infrastructure is under review [@ingests].
KRAKEN [@kraken], Phenome Health's unified knowledge graph, integrates the Microbiome, Multiomics,
and Drug Approvals graphs as domain-specific sources. A rare-disease multiomics knowledge graph
built with Tablassert is in preparation.

# AI usage disclosure

TODO(collaborators): state which generative AI tools (with versions) assisted the software,
tests, documentation, and this manuscript, and how outputs were verified by the authors.

# Acknowledgements

TODO(collaborators): funding awards and contributor acknowledgements go here.

# References
