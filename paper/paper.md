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
    affiliation: Institute for Systems Biology
    orcid: 0009-0007-8373-2352
  - name: Gwênlyn Glusman
    affiliation: Institute for Systems Biology
    orcid: 0000-0001-8060-5955
  - name: Jared C. Roach
    affiliation: Institute for Systems Biology
    orcid: 0000-0003-4524-0107
date: 16 October 2026
lang: en-US
bibliography: paper.bib
---

Biomedical knowledge lives in spreadsheets: association tables, assay results, curated gene-disease
lists. Tablassert is a Python package that turns such tables into knowledge graphs ready for NCATS
Translator [@translator]. The user declares how columns map to subject-predicate-object statements
in YAML; Tablassert resolves free text ("TP53", "lung cancer") to standard CURIEs, attaches
provenance and statistical annotations, and emits KGX-compliant [@kgx] node and edge files with
Biolink categories [@biolink-model].

## Statement of need

TODO(collaborators): expand from the README statement of need. Seed text follows.

Getting rows from a biomedical spreadsheet into an NCATS Translator knowledge graph normally means
writing a per-source Python ingest that maps columns to Biolink statements, resolves free text to
standard identifiers, and records provenance, terms of use, and statistical annotations. Entity
resolution usually means calling a hosted name-resolution service at build time, so builds are not
reproducible offline and cannot be audited without network access.

Tablassert replaces the per-source code with a declarative YAML mapping plus an embedded, offline
entity-resolution database built from RENCI BABEL [@babel] exports. A build is therefore
reproducible, auditable, and network-free. It targets Translator ingest authors and knowledge-graph
data engineers holding tabular biomedical sources, and bioinformatics groups that want validated
KGX output without writing a transform pipeline. Each build can also emit the Resource Ingest Guide
metadata that Translator submissions require.

## State of the field

TODO(collaborators): compare with the prevailing practice (hand-written per-source ingests in the
translator-ingests repository) and alternative tools. Seed text follows.

The prevailing practice for Translator ingests is bespoke Python per data source. Generic KGX
tooling [@kgx] handles exchange and validation but leaves the column-to-statement mapping to code.
Hosted name-resolution services resolve identifiers but require network access at build time and
rate-limit bulk lookups. Quality control of mappings is typically manual review of resolution
samples. Tablassert's four-stage audit (exact match, fuzzy match, abbreviation expansion, then
SapBERT embeddings [@sapbert]) flags low-confidence mappings automatically, and its use in
MicrobiomeKG [@microbiomekg] demonstrates the pipeline on a real Translator-scale knowledge graph.

## Figures

TODO(collaborators): add one figure, for example the YAML mapping to KGX nodes and edges flow.
Commented placeholder syntax:

<!-- ![Schematic: a table config maps CSV columns through entity resolution to KGX NDJSON.](figures/fig1.png){width=100%} -->

## Acknowledgements

TODO(collaborators): funding, contributor roles, and acknowledgements go here.

## References
