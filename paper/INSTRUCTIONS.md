# JOSS Paper Instructions

This directory holds the draft JOSS (Journal of Open Source Software) paper for Tablassert.
`paper.md` is the manuscript, `paper.bib` is its bibliography, and the compiled `paper.pdf`
is generated, never committed. See the [JOSS author guidelines](https://joss.readthedocs.io/en/latest/submitting.html)
before making major changes.

## What JOSS reviewers check

JOSS reviews the software as much as the text. Tablassert already satisfies the entry
requirements, so the review focuses on the paper itself:

- A clear **statement of need**: what problem Tablassert solves and for whom.
- **State of the field**: how the prevailing practice (hand-written per-source Translator
  ingests) falls short, what alternatives exist (Koza, KGX, hosted name resolution), and the
  build-versus-contribute justification for a new package.
- **Software design** and a **research impact statement**: the design trade-offs and the
  realized uses (MicrobiomeKG, DAKP, KRAKEN, pending Translator integration).
- An **AI usage disclosure** naming any generative tools used for the software,
  documentation, or manuscript, with versions and how outputs were verified.
- An **archive**: the tagged release gets a Zenodo DOI at acceptance; keep
  `CITATION.cff` and this paper's author list in sync.
- The paper's body stays between 750 and 1750 words excluding references; the abstract
  under 250. The `paper-check` gate enforces both locally.

JOSS rules change; re-check the
 [paper format page](https://joss.readthedocs.io/en/latest/paper.html) before submission.

## File map

- `paper.md`: manuscript in JOSS (pandoc) format. YAML front matter carries title,
  authors, ORCIDs, tags, and the `bibliography: paper.bib` key that citeproc loads;
  the body uses the sections JOSS expects (Summary, Statement of need, State of the
  field, Software design, Research impact statement, AI usage disclosure,
  Acknowledgements, References). Author `affiliation:` values are INDICES into the
  separate `affiliations:` list (the JOSS convention); putting an institution string
  there silently collapses the rendered author block to the last author.
- `paper.bib`: BibTeX entries. Every `[@key]` citation in `paper.md` must resolve to an
  entry here, and every entry must be cited; `paper-check` enforces both directions.
  Note: pandoc's bibliography parser treats `%` comment lines loosely, so keep `]`, `@`,
  and other entry-like punctuation out of comments.
- `figures/`: figure images referenced from `paper.md` (create when adding the figure).

## Workflow

1. Draft or edit `paper.md`. Cite with `[@key]`, for example KGX [@kgx] renders as
   "(Mungall et al. 2022)". Multi-cite as `[@a; @b]`.
2. Verify the matching `paper.bib` entry: title, authors, year, DOI. Entries seeded with
   a `% TODO: verify` comment are unverified; check them against the publisher page or
   <https://doi.org> before submission.
3. Add a figure by dropping the image into `figures/` and referencing it from its own
   paragraph with `![Caption text](figures/fig1.png)`; JOSS turns a captioned standalone
   image into a numbered figure. There is no figure in the current draft.
4. Run the local gates before committing:

```bash
make paper-check   # front matter, citations, word budgets (also a pre-commit hook)
make paper-pdf     # compile paper/paper.pdf with Docker (requires Docker)
make paper         # both
```

`make paper-pdf` builds the official JOSS paperdraft compiler image from the archived
openjournals/whedon resources (the prebuilt Docker Hub image is gone); the first build
takes a few minutes and later builds hit the Docker layer cache. The existing
markdownlint hooks cover this directory automatically; keep prose ASCII (no unicode
dashes or arrows) as everywhere else in the repository.

## CI

`.github/workflows/paper.yml` runs only when files under `paper/` change on a pull
request: it compiles the PDF from the JOSS paperdraft resources and uploads `paper.pdf`
as a workflow artifact so reviewers can preview the compiled paper without Docker. The
pytest gate runs in the regular CI suite on every pull request.

## Submitting

When the draft is ready:

1. Resolve every `TODO(collaborators)` marker in `paper.md` (AI usage disclosure,
   Acknowledgements) and verify every remaining `paper.bib` entry against its publisher
   page or <https://doi.org> before submission.
2. Confirm `make paper-check` and `make paper-pdf` both pass.
3. The submitting author creates an account at <https://joss.theoj.org>, then submits
   through the JOSS site, pointing at this repository and branch. The editor assigns
   two reviewers; JOSS compiles `paper.md` automatically during review (the bot stack
   formerly called Whedon, now Buffy, does the compilation).
4. Address review comments here, on this branch, until acceptance; JOSS archives the
   accepted version and mints the DOI.
