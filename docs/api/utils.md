# Utilities (utils)

The `tablassert.utils` module provides the shared working-directory constants and a compact hashing helper used throughout the CLI; import it for deterministic section hashes and the `.tablassert/` artifact layout. Deterministic UUID generation for KGX edge identifiers lives in the Rust extension (`tablassert.rs`) and is documented below as well.

## Constants

**`BASE: Path`**: `Path("./.tablassert")`

The single parent working directory. All runtime artifacts live beneath it.

**`STORE: Path`**: `BASE / "store"`

Intermediate parquet storage for compiled subgraphs (`.tablassert/store/`). Created on import.

The log directory (`log.LOGASSERT`, `.tablassert/log/`) holding the loguru sink file `.tablassert/log/tablassert.log` when the `log` extra is installed, and the cached SapBERT model (`.tablassert/sapbert/`, `qc.MODEL`) are likewise derived from `BASE`.

## mkhash()

Generates an **xxh64** digest (hex string, 16 characters) for arbitrary input, computed by the Rust extension (`tablassert.rs.xxh64`).

```python
def mkhash(x: Any) -> str
```

The input is converted to a string and UTF-8 encoded before hashing. `mkhash` is a general-purpose digest for arbitrary values, including section configuration; it is not itself a source-file hash. The build pipeline uses the section's `mkhash` value as one component of the build-time store key described below. Configuration-only validation uses this digest without reading source files; build-time progress and section-validation errors use the content-aware key, while source-file hashing errors retain the configuration digest as a fallback label. User-facing progress labels truncate the relevant key to 8 characters for display.

The full 64-bit digest is used deliberately: a 32-bit hash would invite birthday collisions (~50% at ~77k sections) that could silently reuse another section's cached subgraph.

```python
from tablassert.utils import mkhash

mkhash("hello")  # "26c7827d889f6da3"
```

**Deterministic:** the same input always produces the same digest.

## file_content_hash()

Generates an **xxh64** digest of a local source file's raw bytes. It resolves the path and raises `SourceFileError` if the path does not exist, is not a regular file, or cannot be read.

```python
def file_content_hash(
    path: Path,
    *,
    config: Path | None = None,
    section_label: str | None = None,
) -> str
```

This helper does not cache the digest. An edited source file therefore produces a different digest and, in a build, a different section store key. `config` and `section_label` are optional context included in a source-file error when available.

## section_store_key()

Returns the build-time content-aware store key for a section's cached parquet.

```python
def section_store_key(
    section: Any,
    local: Path | None = None,
    *,
    content_digest: str | None = None,
) -> str
```

For a section with a local source, the key combines the section configuration digest with the source content digest and hashes the result into one 16-character xxh64 value. Callers may provide a precomputed `content_digest` to avoid rereading the file; otherwise `local` is passed to `file_content_hash`. If neither is provided, the function returns the section's plain `mkhash` value. The build pipeline uses the content-aware form for local sources, so changing one source invalidates the cached parquet for each section that reads it without invalidating sections backed by unchanged sources.

The key formula is `mkhash(f"{mkhash(section)}:{content_digest}")`. Build-mode flags add their own filename suffixes (`.head`, `.release`, or `.qc`) after the key; they do not change the digest formula.

---

## namespace_uuid()  *(Rust extension: `tablassert.rs`)*

Generates deterministic UUIDs for KGX edge identifiers using UUID v3 (MD5-based namespacing). Provided by the Rust extension, not `tablassert.utils`.

### Function Signature

```python
def namespace_uuid(domain: str, values: list[str]) -> str
```

### Parameters

**`domain: str`**

Domain string used to create the namespace UUID. The default domain used internally for KGX edge IDs is `"TABLASSERT"`.

**`values: list[str]`**

The values to incorporate into the UUID. Empty entries are dropped; each surviving value is length-prefixed as `<byte-length>:<value>` and concatenated, then hashed within the domain namespace.

### Return Value

Returns a string representation of a UUID v3: `"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`.

### How It Works

**Step 1:** Create the domain namespace from the nil UUID:

```python
domain_uuid = uuid3(UUID("00000000-0000-0000-0000-000000000000"), domain)
```

**Step 2:** Length-prefix each value and hash the concatenation within that namespace:

```python
joined = "".join(f"{len(v.encode())}:{v}" for v in values)
return str(uuid3(domain_uuid, joined))
```

The length prefix is what makes the encoding **injective**. A plain separator join is ambiguous
whenever a value contains the separator: `["a", "x\tb", "y"]` and `["a", "x", "b", "y"]` both join
to `"a\tx\tb\ty"`, so two different inputs would derive the same UUID.

### Deterministic Behavior

Same inputs always produce the same UUID; different inputs (or different domains) produce different UUIDs.

## Edge IDs

Edge ids are **not** built by calling `namespace_uuid()` from Python. They are assigned inside the
Rust deduper (`dedup_ndjson`) as each edge is written, from the record itself.

### Which fields feed the id

By default, **every field** of the emitted edge. That makes the id maximally sensitive: a corrected
`p_value`, a new `supporting_text` entry, a reordered source row, or a Biolink release that renames
a slot all mint a brand-new id, and downstream consumers see a new edge rather than an updated one.

A graph config can instead declare which fields constitute edge *identity*:

```yaml
# graph.yaml
uuid_fields: [subject, predicate, object, publications, has_supporting_studies]
```

Only those fields then feed the hash, so attribute-only changes leave the id alone. See
[Graph Configuration](../configuration/graph.md#stable-edge-ids) for how to choose a field set.

### Canonicalization

Before hashing, the record is normalized so that only *meaning* reaches the digest:

- object keys are sorted, recursively, so insertion order never changes the id;
- array order is preserved, because it is semantic;
- each key and its value are fed as **separate** parts, so the key/value boundary cannot shift to
  create a collision (`{"a": "b=c"}` and `{"a=b": "c"}` stay distinct);
- `null` and empty values drop out, key included; `false` is hashed as `"false"`, because
  `negated: false` is a meaningful Biolink value.

### Namespace

The domain defaults to `"TABLASSERT"`. When `uuid_fields` is declared it becomes the graph's
`rig.source_info.infores_id`, so two graphs asserting the same triple can never mint the same id —
the uniqueness that full-record hashing provided by accident becomes structural. `uuid_domain`
overrides it for graphs that must deliberately share an id space.

### Uniqueness

Edges deduplicate on their derived id, so an output file can never carry the same id twice. An
exact repeat collapses; two genuinely different edges deriving one id abort the build with
`uuid-fields-not-a-key`, naming the fields that would disambiguate them.

A graph that *expects* such collisions — e.g. one whose `uuid_fields` are the resolved statement,
so two rows with different raw mention spellings resolve to the same CURIE — can instead set
`uuid_on_collision: merge` (requires `uuid_fields`). Divergent same-id records are then folded
into one edge: list fields are unioned, deduplicated by content, and sorted (so the merged edge is
identical regardless of row order), conflicting scalars keep the first record's value, and a
build-log summary reports how many records merged and how many scalar conflicts were arbitrated.
Merge mode buffers one full record per unique id until end-of-stream — the memory cost the default
streaming path avoids — which is why it is opt-in. See
[Merging collisions instead](../configuration/graph.md#merging-collisions-instead).

### KGX Compliance

NCATS Translator KGX requires edge IDs to be globally unique and, where possible, deterministic.
UUID v3 with domain namespacing satisfies both.

## Next Steps

- **[Entity Resolution](fullmap.md)** - Core entity mapping
- **[Tutorial](../tutorial.md)** - See UUIDs in action
