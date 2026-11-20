# Tablassert Remediation Guide (`SKILL.md`)

Quick remediation steps for common Tablassert CLI and configuration errors.

## `yaml-parse-error` {#yaml-parse-error}

The YAML file could not be parsed at the reported `file:line:column`.

1. Open the file at the reported line and column.
2. Check for unquoted colons (`:`), mismatched indentation, stray tabs, or unclosed quotes/brackets.
3. Re-run `tablassert validate <config> --schema graph` (or `--schema table`).

## `graph-validation-failed` {#graph-validation-failed}

One or more fields in the graph configuration failed schema validation.

1. Check the field path and reported `file:line:column` in the error message.
2. Verify required keys (`name`, `version`, `tables`, `fullmap`, `rig`) against [Graph Configuration](docs/configuration/graph.md).
3. Re-run `tablassert validate <graph.yaml> --schema graph`.

## `section-validation-failed` {#section-validation-failed}

One or more fields in a table configuration section failed schema validation.

1. Check the reported `file:line:column` (including inherited `template:` lines).
2. Verify `source`, `statement`, `provenance`, and `annotations` against [Table Configuration](docs/configuration/table.md).
3. Re-run `tablassert validate <table.yaml> --schema table`.

## `rig-legacy-keys` {#rig-legacy-keys}

Top-level `description`, `contributions`, `ui_explanation`, or `infores` keys moved under the `rig:` section.

1. Move `description` -> `rig.source_info.description`, `infores` -> `rig.source_info.infores_id`, `contributions` -> `rig.provenance_info.contributions`, and `ui_explanation` -> `rig.ui_explanation`.
2. Re-run `tablassert validate <graph.yaml> --schema graph`.

## `taxon-from-conflict` {#taxon-from-conflict}

`taxon_from` and an explicit `taxon` cannot both be set on the same node encoding.

1. Remove either `taxon` or `taxon_from` on the reported encoding.
2. Re-run `tablassert validate <table.yaml> --schema table`.

## `encoding-bad-excel-column` {#encoding-bad-excel-column}

`method: column` expects an Excel-style column letter (`A` through `ZZ`).

1. Replace the `encoding` value with the 1-based column letter (`A`, `B`, ..., `AA`).
2. Re-run `tablassert validate <table.yaml> --schema table`.

## `provenance-missing-publication` {#provenance-missing-publication}

Derived provenance requires `provenance.publication` unless `provenance.override` is set.

1. Provide `provenance.publication` (with `provenance.repo`), or configure `provenance.override`.
2. Re-run `tablassert validate <table.yaml> --schema table`.

## `qualifier-bad-value` {#qualifier-bad-value}

An enum-ranged qualifier literal is outside the allowed Biolink vocabulary.

1. Use one of the permitted Biolink enum values listed in the error message.
2. Re-run `tablassert validate <table.yaml> --schema table`.
