use crate::json::{canonical_json_bytes, emitted_json_bytes, strip_nulls};
use crate::uuid::uuid_for_json_object;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::Value;
use std::collections::hash_map::Entry;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use uuid::Uuid;
use xxhash_rust::xxh64::xxh64;

fn runtime_error(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

/// Collision-safe dedup decision for the NODES stream.  Records are bucketed by xxh64
/// but a record is suppressed ONLY when its canonical bytes exactly match an existing
/// entry in the bucket, so two DISTINCT records whose xxh64 collides both survive.
/// (The former `HashSet<u64>` keyed on the hash alone silently dropped the second record
/// on a collision — data loss at scale.)  Returns true when `bytes` is new and recorded.
///
/// Edges do NOT use this path: their `id` is a derived hash, so they dedup on the id
/// itself (see `EdgeIndex`), which both guarantees id uniqueness and keeps 16 bytes per
/// edge instead of the whole record.
fn record_if_new(seen: &mut FxHashMap<u64, Vec<Vec<u8>>>, bytes: &[u8]) -> bool {
    let bucket = seen.entry(xxh64(bytes, 0)).or_default();
    if bucket.iter().any(|existing| existing.as_slice() == bytes) {
        false
    } else {
        bucket.push(bytes.to_vec());
        true
    }
}

fn label_edge(mut value: Value, domain: &str, fields: Option<&[String]>) -> PyResult<Value> {
    let id: String = uuid_for_json_object(domain, &value, fields)
        .ok_or_else(|| runtime_error("expected JSON object"))?;
    value
        .as_object_mut()
        .ok_or_else(|| runtime_error("expected JSON object"))?
        .insert("id".to_string(), Value::String(id));
    Ok(value)
}

/// A record ready to write, plus (for edges) the content hash of the record as it stood
/// BEFORE its `id` was inserted.
///
/// Hashing pre-insertion matters twice over: the id is a pure function of the other
/// fields, so a post-insertion hash would agree whenever the ids agree -- exactly the
/// divergence this is meant to detect -- and it avoids cloning every record just to strip
/// one key back out.
struct Finalized {
    value: Value,
    content: u64,
}

fn finalize_record(
    value: Value,
    is_edges: bool,
    domain: &str,
    fields: Option<&[String]>,
) -> PyResult<Option<Finalized>> {
    let cleaned: Value = strip_nulls(&value);
    let is_empty_object: bool = matches!(&cleaned, Value::Object(map) if map.is_empty());
    if is_empty_object {
        return Ok(None);
    }
    if !is_edges {
        return Ok(Some(Finalized {
            value: cleaned,
            content: 0,
        }));
    }
    let content: u64 = xxh64(&canonical_json_bytes(&cleaned).map_err(runtime_error)?, 0);
    label_edge(cleaned, domain, fields).map(|value| Some(Finalized { value, content }))
}

/// Edge dedup state: derived id -> hash of that edge's canonical, id-free content.
///
/// Keying on the id (not on the whole record, as the nodes path does) is what makes
/// `uuid_fields` safe: once the hash covers only a subset of the record, two records can
/// share an id while differing in bytes, and byte-keyed dedup would happily emit both --
/// duplicate edge ids in the output. It is also ~20-25x cheaper: 24 bytes per edge
/// instead of a full copy of every record (~800 bytes each at the 10M-edge scale).
#[derive(Default)]
struct EdgeIndex {
    seen: FxHashMap<[u8; 16], u64>,
}

/// What to do with an edge whose id has been seen before.
enum EdgeVerdict {
    /// First sighting of this id -- write it.
    Fresh,
    /// Byte-identical to the record that already claimed this id -- suppress it.
    Duplicate,
    /// A DIFFERENT record claims this id: the declared `uuid_fields` are not a key.
    Divergent,
}

impl EdgeIndex {
    fn classify(&mut self, id: [u8; 16], content: u64) -> EdgeVerdict {
        match self.seen.get(&id) {
            None => {
                self.seen.insert(id, content);
                EdgeVerdict::Fresh
            }
            Some(existing) if *existing == content => EdgeVerdict::Duplicate,
            Some(_) => EdgeVerdict::Divergent,
        }
    }
}

fn edge_id_bytes(value: &Value) -> PyResult<[u8; 16]> {
    let id: &str = value
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| runtime_error("labeled edge is missing its id"))?;
    Uuid::parse_str(id)
        .map(Uuid::into_bytes)
        .map_err(|error| runtime_error(format!("edge id {id} is not a UUID: {error}")))
}

/// Recover the record that first claimed `id` so the error can name what actually
/// differs.  Runs ONLY on the failure path -- the build is about to abort, so re-reading
/// the partial output costs nothing in the happy case.
fn find_written_edge(output: &Path, id: &str) -> Option<Value> {
    let reader = BufReader::new(File::open(output).ok()?);
    reader
        .lines()
        .map_while(Result::ok)
        .filter(|line| line.contains(id))
        .find_map(|line| {
            let value: Value = serde_json::from_str(&line).ok()?;
            (value.get("id").and_then(Value::as_str) == Some(id)).then_some(value)
        })
}

/// Top-level keys whose values differ between two records.
fn differing_keys(left: &Value, right: &Value) -> Vec<String> {
    let (Some(left), Some(right)) = (left.as_object(), right.as_object()) else {
        return Vec::new();
    };
    let mut keys: Vec<String> = left
        .keys()
        .chain(right.keys())
        .filter(|key| key.as_str() != "id")
        .filter(|key| left.get(*key) != right.get(*key))
        .cloned()
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// Build the `uuid-fields-not-a-key` diagnostic.
///
/// The author's declared `uuid_fields` do not uniquely identify an edge in this graph, so
/// two genuinely different records derived the same id. Emitting both would ship duplicate
/// ids; silently dropping one would lose data. Fail, and name the fields that would fix it.
fn not_a_key_error(output: &Path, id: &str, incoming: &Value, fields: Option<&[String]>) -> PyErr {
    let declared: String = match fields {
        Some(fields) => fields.join(", "),
        None => "<all fields>".to_string(),
    };
    let differing: String = find_written_edge(output, id)
        .map(|existing| differing_keys(&existing, incoming))
        .filter(|keys| !keys.is_empty())
        .map_or_else(
            || "  (could not recover the first record to diff)".to_string(),
            |keys| format!("  they differ in: {}", keys.join(", ")),
        );
    let context: String = ["subject", "predicate", "object"]
        .iter()
        .filter_map(|key| {
            incoming
                .get(*key)
                .and_then(Value::as_str)
                .map(|value| format!("{key}={value}"))
        })
        .collect::<Vec<String>>()
        .join(" ");
    runtime_error(format!(
        "uuid-fields-not-a-key: declared uuid_fields are not a key for this graph.\n\
         \x20 id {id} is claimed by 2 different edges.\n\
         {differing}\n\
         \x20 declared uuid_fields: {declared}\n\
         \x20 offending edge: {context}\n\
         Add a discriminating field to `uuid_fields` (a qualifier, `has_supporting_studies` \
         for the source row, or the statistic that actually differs)."
    ))
}

/// Merge-mode dedup state: derived id -> the record that first claimed it plus its fold
/// bookkeeping, plus the first-seen id order so the buffered output is deterministic.
///
/// Unlike the default `EdgeIndex` (24 bytes per edge, streaming writes), this retains one
/// COMPLETE record per unique id and writes nothing until end-of-stream, because a
/// divergent record must be folded into the record that already claimed the id. That
/// memory cost is exactly why merge mode is opt-in (`uuid_on_collision: merge`).
#[derive(Default)]
struct MergeIndex {
    records: FxHashMap<[u8; 16], MergedRecord>,
    order: Vec<[u8; 16]>,
    merged: u64,
    scalar_conflicts: u64,
}

type OriginalValues = FxHashMap<String, FxHashSet<String>>;

fn collect_original_values(value: &Value) -> OriginalValues {
    let mut values: OriginalValues = FxHashMap::default();
    let Some(map) = value.as_object() else {
        return values;
    };
    for (key, value) in map {
        if key.starts_with("original_") {
            if let Value::String(text) = value {
                for part in text.split('|').filter(|part| !part.is_empty()) {
                    values
                        .entry(key.clone())
                        .or_default()
                        .insert(part.to_string());
                }
            }
        }
    }
    values
}

fn format_original_values(values: &FxHashSet<String>) -> String {
    let mut sorted: Vec<&str> = values.iter().map(String::as_str).collect();
    sorted.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    sorted.join("|")
}

/// The canonical bytes of one list item, shared by every structure that needs them.
///
/// ONE heap allocation per distinct item instead of one per consumer: `Rc<T>`'s `Hash`,
/// `Eq`, and `Ord` all delegate to `T`, so set membership still compares FULL canonical
/// bytes (never a bare hash -- a hash-only membership set would merge two distinct items
/// on a collision) and the deferred write-out sort still orders by those same bytes.
/// `Rc<[u8]>` rather than `Rc<Vec<u8>>` because the slice form stores the bytes inline in
/// the reference-count block: one exactly-sized allocation per item instead of a count
/// block plus a `Vec` buffer that keeps `canonical_json_bytes`' 128-byte starting
/// capacity. At scenario-C shapes (~5M stored list items, since every row contributes its
/// own `supporting_case_ids`) that is roughly 64 resident bytes per short item instead of
/// ~320 for two full copies -- measured on the committed harness, scenario C's peak RSS
/// falls from 1.93 GB to 1.26 GB.
type SharedBytes = Rc<[u8]>;

/// Union bookkeeping for one array field of one buffered record.
///
/// Canonical bytes are computed ONCE per item -- when the item first enters the record,
/// either with the record itself or appended by a fold -- and are reused for membership
/// and the final sort: each fold costs O(1) per incoming item instead of re-canonicalizing
/// every stored item on every fold (the pre-US-002 quadratic). The two structures below
/// SHARE that one allocation per item (`SharedBytes`), so the bookkeeping costs one
/// canonical-bytes copy per distinct item, not two.
struct ListState {
    /// Canonical bytes of every stored item. Membership oracle for INCOMING items only:
    /// duplicates inside the first-seen record stay in the list but still reject an equal
    /// incoming item, exactly like the former linear `seen` scan.
    seen: FxHashSet<SharedBytes>,
    /// Canonical bytes parallel to the live items, so the deferred write-out sort never
    /// re-canonicalizes anything. The SAME allocation `seen` holds -- a refcount, not a
    /// second copy.
    bytes: Vec<SharedBytes>,
    /// Set when a real union ran (BOTH sides carried an array). Only then does the
    /// write-out sort the list; a list merely copied from a later record keeps its source
    /// order, byte-for-byte with the pre-US-002 fold.
    unioned: bool,
}

impl ListState {
    /// Seed from an array that just entered the record (first-seen, or copied from a later
    /// record): keep EVERY item -- duplicates included -- and record each item's canonical
    /// bytes once.
    fn from_items(items: &[Value]) -> PyResult<Self> {
        let mut state = Self {
            seen: FxHashSet::with_capacity_and_hasher(items.len(), Default::default()),
            bytes: Vec::with_capacity(items.len()),
            unioned: false,
        };
        for item in items {
            let bytes: SharedBytes = canonical_json_bytes(item).map_err(runtime_error)?.into();
            state.seen.insert(Rc::clone(&bytes));
            state.bytes.push(bytes);
        }
        Ok(state)
    }
}

/// One buffered edge record plus the O(1) bookkeeping its folds need.
///
/// Replaces the pre-US-002 `(Vec<u64>, Value)` tuple: exact-repeat suppression and list
/// membership are hash sets, and the per-fold re-canonicalization + re-sort of every
/// stored list item is gone -- lists sort ONCE, at write-out, and only if a union ran.
struct MergedRecord {
    /// The live record: conflicting scalars keep the lexicographically smallest canonical
    /// value (see `merge_records`), lists in original/append order until the deferred
    /// write-out sort.
    value: Value,
    /// Content hashes of every record absorbed into this id: O(1) exact-repeat
    /// suppression. Bare hashes keep the pre-US-002 `Vec<u64>` semantics exactly (a false
    /// hit could only skip re-folding a record, matching the old `contains`); node dedup
    /// stays full-bytes because a collision there would drop a DISTINCT record.
    hashes: FxHashSet<u64>,
    /// Union state for every field whose current value is an array.
    lists: FxHashMap<String, ListState>,
    /// Distinct non-empty scalar source values for each `original_*` field.
    original_values: OriginalValues,
    /// Conflicting scalar field names, one entry per fold conflict (with multiplicity).
    /// Consumed at write-out by the per-predicate fold report.
    conflict_fields: Vec<String>,
}

impl MergedRecord {
    /// Buffer the record that first claimed its id, seeding union state for every array
    /// field (items kept verbatim, canonical bytes recorded once).
    fn new(value: Value, content: u64) -> PyResult<Self> {
        let mut lists: FxHashMap<String, ListState> = FxHashMap::default();
        if let Some(map) = value.as_object() {
            for (key, item) in map {
                if let Value::Array(items) = item {
                    lists.insert(key.clone(), ListState::from_items(items)?);
                }
            }
        }
        let mut hashes: FxHashSet<u64> = FxHashSet::default();
        hashes.insert(content);
        Ok(Self {
            original_values: collect_original_values(&value),
            value,
            hashes,
            lists,
            conflict_fields: Vec::new(),
        })
    }

    /// Apply the one deferred sort just before the write: every list that went through a
    /// real union is sorted by canonical bytes (stable, so equal-byte items keep arrival
    /// order -- byte-identical to the pre-US-002 per-fold sort). Lists that were only
    /// copied keep their source order. Returns the finished record.
    fn finish(mut self) -> PyResult<Value> {
        let Some(map) = self.value.as_object_mut() else {
            return Err(runtime_error("expected JSON object"));
        };
        for (key, values) in &self.original_values {
            if !values.is_empty() {
                map.insert(key.clone(), Value::String(format_original_values(values)));
            }
        }
        for (key, state) in &mut self.lists {
            if !state.unioned {
                continue;
            }
            // The `number_of_cases` recompute may have replaced a unioned array with a
            // number after the union; only arrays are sortable.
            let Some(Value::Array(items)) = map.get_mut(key) else {
                continue;
            };
            let bytes: Vec<SharedBytes> = std::mem::take(&mut state.bytes);
            // Fail loudly on a desync instead of silently corrupting the record: `zip`
            // truncates to the shorter side while `drain(..)` empties the WHOLE array, so
            // ANY length mismatch drops entries without a trace -- live items when `bytes`
            // is short, canonical-byte entries when `items` is short. The invariant
            // (`bytes` is parallel to the live items) holds by construction -- every push
            // into one is paired with a push into the other -- but this repo's standard is
            // fail-loudly: hash-only keying once silently dropped records the same way.
            // Deliberately a plain runtime check, not a `debug_assert_eq!`: a debug assert
            // would panic first in test/debug builds, so the structured error below could
            // never be observed or tested there. This fires in EVERY build profile.
            if bytes.len() != items.len() {
                return Err(runtime_error(format!(
                    "merge-state-desync: list field {key:?} carries {} canonical-byte entries \
                     for {} live items; refusing to write the record, because pairing them would \
                     silently truncate {} entry/entries",
                    bytes.len(),
                    items.len(),
                    items.len().abs_diff(bytes.len())
                )));
            }
            let mut keyed: Vec<(SharedBytes, Value)> =
                bytes.into_iter().zip(items.drain(..)).collect();
            keyed.sort_by(|left, right| left.0.cmp(&right.0));
            items.extend(keyed.into_iter().map(|(_, item)| item));
        }
        Ok(self.value)
    }
}

/// Fold `incoming` into `stored`, field-wise. Returns the conflicting scalar field names
/// (one entry per conflict, reported for the base record) so the caller can build the fold
/// report.
///
/// Near-linear implementation of the semantics frozen by `merge_records_reference` and
/// policed by `merge_fold_matches_reference_on_fuzz`:
///
/// - list fields: union, deduped by canonical JSON bytes (so two `sources` objects that
///   differ only in key order collapse). Each incoming item is canonicalized ONCE and
///   checked against the field's `ListState` set in O(1); stored items are never
///   re-canonicalized, and the sort by canonical bytes is deferred to write-out
///   (`MergedRecord::finish`), where it runs only for fields that saw a real union;
/// - scalar fields: a conflicting scalar keeps the LEXICOGRAPHICALLY SMALLEST canonical
///   value -- a commutative, associative winner, so the merged record is identical under
///   any arrival order -- and each conflict is reported by field name for the fold report,
///   except `original_*` fields;
/// - `original_*` fields collect distinct non-empty strings and render sorted values as
///   `A`, `A|B`, or `A|B|C`;
/// - fields only on `incoming`: copied over (only a conflict when both sides disagree);
/// - `id` is never touched: both sides carry the same one by construction.
///
/// `number_of_cases` has one hardcoded exception to the smallest-value rule: when the
/// MERGED record carries `supporting_case_ids` (a build-internal `list[str]` of the case
/// IDs behind the count -- allowed onto edge frames so it reaches this pass, then stripped
/// before write) and either side carried a count, the count is recomputed as the length of
/// the unioned ID list. A case ID shared by both records is one case, so smallest-value
/// and summing both over- and under-report; the union length is the exact count. The
/// superseded divergence is NOT reported as a scalar conflict. When neither side carries
/// the list, `number_of_cases` stays an ordinary smallest-value scalar.
fn merge_records(stored: &mut MergedRecord, incoming: &Value) -> PyResult<Vec<String>> {
    let Some(incoming_map) = incoming.as_object() else {
        return Err(runtime_error("expected JSON object"));
    };
    let mut conflict_fields: Vec<String> = Vec::new();
    // Winner overrides are collected during the loop and applied after it: the stored
    // value's mutable borrow is live while a conflict is detected, and the winner needs
    // the canonical bytes of BOTH sides compared.
    let mut winner_overrides: Vec<(String, Value)> = Vec::new();
    // Read both counts BEFORE the fold: the fold may copy incoming's over a stored side
    // that lacks it, and the recompute rule below needs to know each side contributed one.
    let stored_cases: Option<Value> = stored.value.get("number_of_cases").cloned();
    let incoming_cases: Option<Value> = incoming_map.get("number_of_cases").cloned();
    let Some(stored_map) = stored.value.as_object_mut() else {
        return Err(runtime_error("expected JSON object"));
    };
    for (key, incoming_value) in incoming_map {
        if key == "id" {
            continue;
        }
        match stored_map.get_mut(key) {
            None => {
                // First time this field appears on the stored record: copy it over. An
                // array gets union state seeded so LATER folds can union into it in O(1)
                // (this copy itself is NOT a union: it keeps its source order).
                if let Value::Array(items) = incoming_value {
                    stored
                        .lists
                        .insert(key.clone(), ListState::from_items(items)?);
                } else if key.starts_with("original_") {
                    if let Value::String(text) = incoming_value {
                        let values = stored.original_values.entry(key.clone()).or_default();
                        values.extend(
                            text.split('|')
                                .filter(|part| !part.is_empty())
                                .map(str::to_owned),
                        );
                    }
                }
                stored_map.insert(key.clone(), incoming_value.clone());
            }
            Some(stored_value) => {
                if let (Value::Array(stored_items), Value::Array(incoming_items)) =
                    (&mut *stored_value, incoming_value)
                {
                    let Some(state) = stored.lists.get_mut(key) else {
                        // Invariant: every stored array field carries union state.
                        return Err(runtime_error("list field without union state"));
                    };
                    // BOTH sides arrays -> a union happened: this field sorts at write-out
                    // even when no new item survives membership (the pre-US-002 fold
                    // sorted on every such fold).
                    state.unioned = true;
                    for item in incoming_items {
                        let bytes: SharedBytes =
                            canonical_json_bytes(item).map_err(runtime_error)?.into();
                        if state.seen.insert(Rc::clone(&bytes)) {
                            state.bytes.push(bytes);
                            stored_items.push(item.clone());
                        }
                    }
                } else if key.starts_with("original_") {
                    if let (Value::String(stored_text), Value::String(incoming_text)) =
                        (&*stored_value, incoming_value)
                    {
                        let values = stored.original_values.entry(key.clone()).or_default();
                        values.extend(
                            stored_text
                                .split('|')
                                .filter(|part| !part.is_empty())
                                .map(str::to_owned),
                        );
                        values.extend(
                            incoming_text
                                .split('|')
                                .filter(|part| !part.is_empty())
                                .map(str::to_owned),
                        );
                    } else if stored_value != incoming_value {
                        conflict_fields.push(key.clone());
                        let stored_wins = {
                            let stored_bytes =
                                canonical_json_bytes(stored_value).map_err(runtime_error)?;
                            let incoming_bytes =
                                canonical_json_bytes(incoming_value).map_err(runtime_error)?;
                            stored_bytes <= incoming_bytes
                        };
                        if !stored_wins {
                            winner_overrides.push((key.clone(), incoming_value.clone()));
                        }
                    }
                } else if stored_value != incoming_value {
                    conflict_fields.push(key.clone());
                    let stored_wins = {
                        let stored_bytes =
                            canonical_json_bytes(stored_value).map_err(runtime_error)?;
                        let incoming_bytes =
                            canonical_json_bytes(incoming_value).map_err(runtime_error)?;
                        stored_bytes <= incoming_bytes
                    };
                    if !stored_wins {
                        winner_overrides.push((key.clone(), incoming_value.clone()));
                    }
                }
            }
        }
    }
    for (key, value) in winner_overrides {
        stored_map.insert(key, value);
    }
    // WHY: exact-unique `number_of_cases` semantics (see the docstring). Guarded on the
    // merged record actually carrying the ID list: a one-sided carrier is fine (the union
    // is just that side's list), while a carrier-less merge never recomputes.
    if let Some(union_len) = stored
        .value
        .get("supporting_case_ids")
        .and_then(Value::as_array)
        .map(Vec::len)
    {
        if stored_cases.is_some() || incoming_cases.is_some() {
            // Undo the loop's conflict count when it fired on this very field: the
            // recompute supersedes first-wins, so the divergence is not a conflict.
            // Mirrors the loop's condition exactly -- both sides present, unequal, and
            // not both arrays (two arrays took the union path and were never counted).
            if let (Some(left), Some(right)) = (&stored_cases, &incoming_cases) {
                if left != right && !(left.is_array() && right.is_array()) {
                    // The superseded divergence was counted by an EARLIER fold when the
                    // counts first collided (not necessarily this one), so the pop is
                    // unconditional on this fold's pushes: at most one live
                    // number_of_cases conflict can exist per merged record, and the
                    // recompute retires it.
                    if let Some(pos) = conflict_fields
                        .iter()
                        .rposition(|field| field == "number_of_cases")
                    {
                        conflict_fields.remove(pos);
                    }
                }
            }
            let Some(map) = stored.value.as_object_mut() else {
                return Err(runtime_error("expected JSON object"));
            };
            map.insert(
                "number_of_cases".to_string(),
                Value::Number(serde_json::Number::from(union_len)),
            );
        }
    }
    Ok(conflict_fields)
}

/// Remove build-internal carrier fields from an edge record before it is written.
///
/// `supporting_case_ids` exists only so merge mode can recompute `number_of_cases`
/// (see `merge_records`); it must never ship in the final NDJSON, so EVERY edge write
/// path drops it -- the buffering merge pass and the default streaming path alike.
/// Nodes never carry it and are untouched.
fn strip_internal_edge_fields(value: &mut Value) {
    if let Some(map) = value.as_object_mut() {
        map.remove("supporting_case_ids");
    }
}

impl MergeIndex {
    fn absorb(&mut self, id: [u8; 16], value: Value, content: u64) -> PyResult<()> {
        match self.records.entry(id) {
            Entry::Vacant(slot) => {
                slot.insert(MergedRecord::new(value, content)?);
                self.order.push(id);
                Ok(())
            }
            Entry::Occupied(mut slot) => {
                let record: &mut MergedRecord = slot.get_mut();
                // Exact repeat of ANY record already folded into this id -- including a
                // divergent one -- is suppressed, so the conflict summary never
                // double-counts a re-seen row.
                if record.hashes.contains(&content) {
                    return Ok(());
                }
                self.merged += 1;
                let fields: Vec<String> = merge_records(record, &value)?;
                self.scalar_conflicts += fields.len() as u64;
                record.hashes.insert(content);
                record.conflict_fields.extend(fields);
                Ok(())
            }
        }
    }
}

/// FROZEN EQUIVALENCE ORACLE -- BYTE-VERBATIM copy of the pre-US-002 `merge_records`
/// fold, compiled ONLY under `cfg(test)`. US-002 may rewrite the production fold for
/// speed, but this copy stays the exact algorithm of record: `merge_fold_matches_
/// reference_on_fuzz` drives both and demands identical output and counters. Never edit
/// this copy to match an optimization -- edit the production code and let the fuzz test
/// arbitrate. See the module docs on `merge_fold_reference` below.
///
/// Original semantics doc:
///
/// Fold `incoming` into `stored`, field-wise. Returns the number of conflicting scalar
/// fields (the base record's value wins; see `merge_records`) so the caller can report
/// them.
///
/// - list fields: union, deduped by canonical JSON bytes (so two `sources` objects that
///   differ only in key order collapse), then sorted by canonical bytes so the merged
///   output is identical regardless of which record arrived first;
/// - scalar fields: first-wins on conflict, counted;
/// - fields only on `incoming`: copied over (only a conflict when both sides disagree);
/// - `id` is never touched: both sides carry the same one by construction.
///
/// `number_of_cases` has one hardcoded exception to base-wins: when the MERGED record
/// carries `supporting_case_ids` (a build-internal `list[str]` of the case IDs behind
/// the count -- allowed onto edge frames so it reaches this pass, then stripped before
/// write) and either side carried a count, the count is recomputed as the length of the
/// unioned ID list. A case ID shared by both records is one case, so base-wins and
/// summing both over- and under-report; the union length is the exact count. The
/// superseded divergence is NOT reported as a scalar conflict. When neither side
/// carries the list, `number_of_cases` stays an ordinary base-wins scalar.
#[cfg(test)]
fn merge_records_reference(stored: &mut Value, incoming: &Value) -> PyResult<u64> {
    let Some(incoming_map) = incoming.as_object() else {
        return Err(runtime_error("expected JSON object"));
    };
    let mut conflicts: u64 = 0;
    let mut winner_overrides: Vec<(String, Value)> = Vec::new();
    let mut original_values: OriginalValues = collect_original_values(stored);
    for (key, values) in collect_original_values(incoming) {
        original_values.entry(key).or_default().extend(values);
    }
    // Read both counts BEFORE the fold: the fold may copy incoming's over a stored side
    // that lacks it, and the recompute rule below needs to know each side contributed one.
    let stored_cases: Option<Value> = stored.get("number_of_cases").cloned();
    let incoming_cases: Option<Value> = incoming_map.get("number_of_cases").cloned();
    let Some(stored_map) = stored.as_object_mut() else {
        return Err(runtime_error("expected JSON object"));
    };
    for (key, incoming_value) in incoming_map {
        if key == "id" {
            continue;
        }
        match stored_map.get_mut(key) {
            None => {
                stored_map.insert(key.clone(), incoming_value.clone());
            }
            Some(stored_value) => {
                if let (Value::Array(stored_items), Value::Array(incoming_items)) =
                    (&mut *stored_value, incoming_value)
                {
                    let mut seen: Vec<Vec<u8>> = Vec::with_capacity(stored_items.len());
                    for item in stored_items.iter() {
                        seen.push(canonical_json_bytes(item).map_err(runtime_error)?);
                    }
                    for item in incoming_items {
                        let bytes: Vec<u8> = canonical_json_bytes(item).map_err(runtime_error)?;
                        if !seen.contains(&bytes) {
                            seen.push(bytes);
                            stored_items.push(item.clone());
                        }
                    }
                    let mut keyed: Vec<(Vec<u8>, Value)> = Vec::with_capacity(stored_items.len());
                    for item in stored_items.drain(..) {
                        keyed.push((canonical_json_bytes(&item).map_err(runtime_error)?, item));
                    }
                    keyed.sort_by(|left, right| left.0.cmp(&right.0));
                    stored_items.extend(keyed.into_iter().map(|(_, item)| item));
                } else if key.starts_with("original_") {
                    // Aggregated and rendered after this fold, once all values are known.
                } else if stored_value != incoming_value {
                    conflicts += 1;
                    let stored_bytes = canonical_json_bytes(stored_value).map_err(runtime_error)?;
                    let incoming_bytes =
                        canonical_json_bytes(incoming_value).map_err(runtime_error)?;
                    if incoming_bytes < stored_bytes {
                        winner_overrides.push((key.clone(), incoming_value.clone()));
                    }
                }
            }
        }
    }
    for (key, value) in winner_overrides {
        stored_map.insert(key, value);
    }
    if let Some(map) = stored.as_object_mut() {
        for (key, values) in &original_values {
            if !values.is_empty() {
                map.insert(key.clone(), Value::String(format_original_values(values)));
            }
        }
    }
    // WHY: exact-unique `number_of_cases` semantics (see the docstring). Guarded on the
    // merged record actually carrying the ID list: a one-sided carrier is fine (the union
    // is just that side's list), while a carrier-less merge never recomputes.
    if let Some(union_len) = stored
        .get("supporting_case_ids")
        .and_then(Value::as_array)
        .map(Vec::len)
    {
        if stored_cases.is_some() || incoming_cases.is_some() {
            // Undo the loop's conflict count when it fired on this very field: the
            // recompute supersedes first-wins, so the divergence is not a conflict.
            // Mirrors the loop's condition exactly -- both sides present, unequal, and
            // not both arrays (two arrays took the union path and were never counted).
            if let (Some(left), Some(right)) = (&stored_cases, &incoming_cases) {
                if left != right && !(left.is_array() && right.is_array()) {
                    conflicts -= 1;
                }
            }
            let Some(map) = stored.as_object_mut() else {
                return Err(runtime_error("expected JSON object"));
            };
            map.insert(
                "number_of_cases".to_string(),
                Value::Number(serde_json::Number::from(union_len)),
            );
        }
    }
    Ok(conflicts)
}

/// FROZEN EQUIVALENCE ORACLE -- BYTE-VERBATIM copy of the pre-US-002 `MergeIndex` absorb
/// path (state shape included), compiled ONLY under `cfg(test)`; drives
/// `merge_records_reference`. Treat as read-only -- see that fn's docs.
///
/// AMENDED once, deliberately (fold determinism): the oracle's base election now keeps the
/// record with the smallest canonical bytes as the fold base, exactly mirroring the
/// production `MergeIndex::absorb`, so both sides still demand byte-identical merged
/// output under any arrival order. This is a semantic amendment agreed for the fold
/// report work, not an optimization -- the oracle remains the algorithm of record.
#[cfg(test)]
#[derive(Default)]
struct MergeIndexReference {
    records: FxHashMap<[u8; 16], (Vec<u64>, Value)>,
    order: Vec<[u8; 16]>,
    merged: u64,
    scalar_conflicts: u64,
}

#[cfg(test)]
impl MergeIndexReference {
    fn absorb(&mut self, id: [u8; 16], value: Value, content: u64) -> PyResult<()> {
        match self.records.entry(id) {
            Entry::Vacant(slot) => {
                slot.insert((vec![content], value));
                self.order.push(id);
                Ok(())
            }
            Entry::Occupied(mut slot) => {
                let (hashes, stored_value) = slot.get_mut();
                // Exact repeat of ANY record already folded into this id -- including a
                // divergent one -- is suppressed, so the conflict summary never
                // double-counts a re-seen row.
                if hashes.contains(&content) {
                    return Ok(());
                }
                self.merged += 1;
                self.scalar_conflicts += merge_records_reference(stored_value, &value)?;
                hashes.push(content);
                Ok(())
            }
        }
    }
}

/// Merge-mode edge pass: buffer every unique edge, fold divergent same-id records into
/// their smallest-canonical-bytes base, then write in first-seen id order. Returns
/// (divergent records merged, conflicting scalar fields) for the summary log, and writes a
/// fold report (`<output>.foldreport.json`: per-predicate, per-field conflict counts) when
/// any record folded. Runs ONLY under `uuid_on_collision: merge`; the default path stays
/// streaming and never buffers a record. The build-internal `supporting_case_ids` carrier
/// is stripped from each record right before the write.
fn dedup_edges_merge(
    reader: BufReader<File>,
    mut writer: BufWriter<File>,
    domain: &str,
    fields: Option<&[String]>,
    output: &Path,
) -> PyResult<(u64, u64)> {
    let mut fold_report: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, u64>,
    > = std::collections::BTreeMap::new();
    let mut index: MergeIndex = MergeIndex::default();
    for line in reader.lines() {
        let line: String = line.map_err(runtime_error)?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line).map_err(runtime_error)?;
        let Some(Finalized { value, content }) = finalize_record(value, true, domain, fields)?
        else {
            continue;
        };
        index.absorb(edge_id_bytes(&value)?, value, content)?;
    }
    for id in std::mem::take(&mut index.order) {
        let Some(mut record) = index.records.remove(&id) else {
            continue;
        };
        // Fold report: every conflict this id accumulated is attributed to the base
        // record's predicate, so consumers can see WHERE divergent evidence merged.
        if !record.conflict_fields.is_empty() {
            let predicate = record
                .value
                .get("predicate")
                .and_then(Value::as_str)
                .unwrap_or("_unknown_predicate_")
                .to_string();
            let fields_report = fold_report.entry(predicate).or_default();
            for field in std::mem::take(&mut record.conflict_fields) {
                *fields_report.entry(field).or_insert(0) += 1;
            }
        }
        // The one deferred sort: unioned lists sort here, everything else ships as-is.
        let mut value: Value = record.finish()?;
        strip_internal_edge_fields(&mut value);
        writer
            .write_all(&emitted_json_bytes(&value).map_err(runtime_error)?)
            .map_err(runtime_error)?;
        writer.write_all(b"\n").map_err(runtime_error)?;
    }
    writer.flush().map_err(runtime_error)?;
    if !fold_report.is_empty() {
        let report_path = output.with_file_name(format!(
            "{}.foldreport.json",
            output
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .unwrap_or_default()
        ));
        fs::write(
            &report_path,
            serde_json::to_vec_pretty(&fold_report).map_err(runtime_error)?,
        )
        .map_err(runtime_error)?;
    }
    Ok((index.merged, index.scalar_conflicts))
}

#[pyfunction]
#[pyo3(signature = (input, output, is_edges, domain=None, uuid_fields=None, on_collision=None))]
pub fn dedup_ndjson(
    input: PathBuf,
    output: PathBuf,
    is_edges: bool,
    domain: Option<String>,
    uuid_fields: Option<Vec<String>>,
    on_collision: Option<String>,
) -> PyResult<(u64, u64)> {
    let merge: bool = match on_collision.as_deref() {
        None | Some("error") => false,
        Some("merge") => true,
        Some(other) => {
            return Err(runtime_error(format!(
                "unknown on_collision {other:?}: expected \"error\" or \"merge\""
            )));
        }
    };
    let domain: String = domain.unwrap_or_else(|| "TABLASSERT".to_string());
    let fields: Option<&[String]> = uuid_fields.as_deref();
    let reader: BufReader<File> = BufReader::new(File::open(input).map_err(runtime_error)?);
    let mut writer: BufWriter<File> = BufWriter::new(File::create(&output).map_err(runtime_error)?);
    if is_edges && merge {
        return dedup_edges_merge(reader, writer, &domain, fields, &output);
    }
    let mut nodes: FxHashMap<u64, Vec<Vec<u8>>> = FxHashMap::default();
    let mut edges: EdgeIndex = EdgeIndex::default();

    reader
        .lines()
        .map(|line| line.map_err(runtime_error))
        .filter(|line| line.as_ref().map_or(true, |line| !line.trim().is_empty()))
        .map(|line| {
            line.and_then(|line| serde_json::from_str::<Value>(&line).map_err(runtime_error))
        })
        .map(|value| value.and_then(|value| finalize_record(value, is_edges, &domain, fields)))
        .try_for_each(|record| -> PyResult<()> {
            let Some(Finalized { mut value, content }) = record? else {
                return Ok(());
            };
            let write: bool = if is_edges {
                // Edges dedup on their derived id, so the output can never carry the same
                // id twice: an exact repeat is suppressed and a genuine divergence aborts
                // the build rather than shipping a duplicate.
                match edges.classify(edge_id_bytes(&value)?, content) {
                    EdgeVerdict::Fresh => true,
                    EdgeVerdict::Duplicate => false,
                    EdgeVerdict::Divergent => {
                        writer.flush().map_err(runtime_error)?;
                        let id: &str = value["id"].as_str().unwrap_or_default();
                        return Err(not_a_key_error(&output, id, &value, fields));
                    }
                }
            } else {
                record_if_new(
                    &mut nodes,
                    &emitted_json_bytes(&value).map_err(runtime_error)?,
                )
            };
            if write {
                if is_edges {
                    strip_internal_edge_fields(&mut value);
                }
                writer
                    .write_all(&emitted_json_bytes(&value).map_err(runtime_error)?)
                    .map_err(runtime_error)?;
                writer.write_all(b"\n").map_err(runtime_error)?;
            }
            Ok(())
        })?;
    // Flush explicitly and propagate failure: relying on BufWriter's drop-time
    // flush would swallow a final write error and report success with truncated
    // output.
    writer.flush().map_err(runtime_error)?;
    Ok((0, 0))
}

#[cfg(test)]
mod tests {
    use super::{dedup_ndjson, differing_keys, format_original_values, record_if_new};
    use rustc_hash::FxHashMap;
    use rustc_hash::FxHashSet;
    use serde_json::Value;
    use std::fs;
    use tempfile::tempdir;
    use uuid::Uuid;

    #[test]
    fn record_if_new_suppresses_only_exact_byte_duplicates() {
        // WHY: dedup must be collision-safe. Keying on xxh64 alone (the old
        // HashSet<u64>) would drop a distinct record that hashes into an
        // occupied bucket; suppression must require an exact byte match.
        let mut seen: FxHashMap<u64, Vec<Vec<u8>>> = FxHashMap::default();
        // First sighting of a record is new.
        assert!(record_if_new(&mut seen, b"{\"id\":\"A\"}"));
        // A byte-identical record is suppressed.
        assert!(!record_if_new(&mut seen, b"{\"id\":\"A\"}"));
        // Distinct records survive even when they land in the same hash bucket
        // (a real xxh64 collision is impractical to force, so this exercises the
        // byte-exact equality path that decides suppression directly).
        assert!(record_if_new(&mut seen, b"{\"id\":\"B\"}"));
        assert!(!record_if_new(&mut seen, b"{\"id\":\"B\"}"));
    }

    #[test]
    fn dedup_ndjson_keeps_distinct_records() {
        // WHY: two different records must both survive dedup; only an identical
        // duplicate is collapsed.
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("nodes.ndjson.tmp");
        let output = dir.path().join("nodes.ndjson");
        fs::write(&input, "{\"id\":\"A\"}\n{\"id\":\"B\"}\n{\"id\":\"A\"}\n").expect("write input");

        dedup_ndjson(input, output.clone(), false, None, None, None).expect("dedup nodes");

        let lines: Vec<String> = fs::read_to_string(output)
            .expect("read output")
            .lines()
            .map(str::to_string)
            .collect();
        assert_eq!(
            lines,
            vec!["{\"id\":\"A\"}".to_string(), "{\"id\":\"B\"}".to_string()]
        );
    }

    #[test]
    fn dedup_ndjson_deduplicates_nodes() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("nodes.ndjson.tmp");
        let output = dir.path().join("nodes.ndjson");
        fs::write(
            &input,
            "{\"id\":\"A\",\"drop\":\"NA\"}\n{\"id\":\"A\",\"drop\":\"NA\"}\n{}",
        )
        .expect("write input");

        dedup_ndjson(input, output.clone(), false, None, None, None).expect("dedup nodes");

        let lines: Vec<String> = fs::read_to_string(output)
            .expect("read output")
            .lines()
            .map(str::to_string)
            .collect();
        assert_eq!(lines, vec!["{\"id\":\"A\"}".to_string()]);
    }

    #[test]
    fn dedup_ndjson_labels_edges() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("edges.ndjson.tmp");
        let output = dir.path().join("edges.ndjson");
        fs::write(
            &input,
            "{\"subject\":\"A\",\"object\":\"B\",\"predicate\":\"r\"}\n",
        )
        .expect("write input");

        dedup_ndjson(
            input,
            output.clone(),
            true,
            Some("TABLASSERT".to_string()),
            None,
            None,
        )
        .expect("dedup edges");

        let line = fs::read_to_string(output).expect("read output");
        let value: Value = serde_json::from_str(line.trim()).expect("json");
        let id = value.get("id").and_then(Value::as_str).expect("edge id");
        Uuid::parse_str(id).expect("valid UUID");
    }

    #[test]
    fn dedup_ndjson_duplicate_edges_get_stable_single_id() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("edges.ndjson.tmp");
        let output = dir.path().join("edges.ndjson");
        fs::write(
            &input,
            concat!(
                "{\"subject\":\"A\",\"object\":\"B\",\"predicate\":\"r\"}\n",
                "{\"subject\":\"A\",\"object\":\"B\",\"predicate\":\"r\"}\n"
            ),
        )
        .expect("write input");

        dedup_ndjson(
            input,
            output.clone(),
            true,
            Some("TABLASSERT".to_string()),
            None,
            None,
        )
        .expect("dedup edges");

        let lines: Vec<String> = fs::read_to_string(output)
            .expect("read output")
            .lines()
            .map(str::to_string)
            .collect();
        assert_eq!(lines.len(), 1);
        let value: Value = serde_json::from_str(&lines[0]).expect("json");
        let id = value.get("id").and_then(Value::as_str).expect("edge id");
        Uuid::parse_str(id).expect("valid UUID");
    }

    #[test]
    fn dedup_ndjson_strips_nested_null_like_values() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("nodes.ndjson.tmp");
        let output = dir.path().join("nodes.ndjson");
        fs::write(
            &input,
            "{\"id\":\"A\",\"meta\":{\"drop\":\"none\",\"keep\":\"yes\"},\"items\":[{\"x\":\"NA\",\"y\":\"z\"}]}\n",
        )
        .expect("write input");

        dedup_ndjson(input, output.clone(), false, None, None, None).expect("dedup nodes");

        let line = fs::read_to_string(output).expect("read output");
        let value: Value = serde_json::from_str(line.trim()).expect("json");
        assert_eq!(value["meta"], serde_json::json!({"keep":"yes"}));
        assert_eq!(value["items"], serde_json::json!([{"y":"z"}]));
    }

    #[test]
    fn dedup_ndjson_empty_object_only_writes_empty_output() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("nodes.ndjson.tmp");
        let output = dir.path().join("nodes.ndjson");
        fs::write(&input, "{}\n{\"drop\":\"NA\"}\n").expect("write input");

        dedup_ndjson(input, output.clone(), false, None, None, None).expect("dedup nodes");

        assert_eq!(fs::read_to_string(output).expect("read output"), "");
    }

    #[test]
    fn dedup_ndjson_skips_blank_lines() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("nodes.ndjson.tmp");
        let output = dir.path().join("nodes.ndjson");
        fs::write(&input, "\n  \n{\"id\":\"A\"}\n\n{\"id\":\"A\"}\n   \n").expect("write input");

        dedup_ndjson(input, output.clone(), false, None, None, None).expect("dedup nodes");

        assert_eq!(
            fs::read_to_string(output).expect("read output"),
            "{\"id\":\"A\"}\n"
        );
    }

    fn write_edges(dir: &std::path::Path, body: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let input = dir.join("edges.ndjson.tmp");
        let output = dir.join("edges.ndjson");
        fs::write(&input, body).expect("write input");
        (input, output)
    }

    fn edge_ids(output: &std::path::Path) -> Vec<String> {
        fs::read_to_string(output)
            .expect("read output")
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line).expect("json")["id"]
                    .as_str()
                    .expect("edge id")
                    .to_string()
            })
            .collect()
    }

    #[test]
    fn declared_uuid_fields_hold_the_id_still_across_attribute_edits() {
        // WHY: the whole point of `uuid_fields`. Two builds whose only difference is a
        // p_value must produce the SAME edge id, so downstream sees one edge updated
        // rather than one retired and one created.
        let dir = tempdir().expect("tempdir");
        let fields = Some(vec![
            "subject".to_string(),
            "predicate".to_string(),
            "object".to_string(),
        ]);

        let (before_in, before_out) = write_edges(
            dir.path(),
            "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.01\"}\n",
        );
        dedup_ndjson(
            before_in,
            before_out.clone(),
            true,
            None,
            fields.clone(),
            None,
        )
        .expect("dedup");

        let after_dir = tempdir().expect("tempdir");
        let (after_in, after_out) = write_edges(
            after_dir.path(),
            "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.99\",\"effect_size\":1.5}\n",
        );
        dedup_ndjson(after_in, after_out.clone(), true, None, fields, None).expect("dedup");

        assert_eq!(edge_ids(&before_out), edge_ids(&after_out));
    }

    #[test]
    fn undeclared_uuid_fields_let_the_id_drift() {
        // WHY: the contrast case -- with no `uuid_fields`, the same attribute edit moves
        // the id. This is the pre-16.0.0 behavior the default still preserves.
        let dir = tempdir().expect("tempdir");
        let (before_in, before_out) = write_edges(
            dir.path(),
            "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.01\"}\n",
        );
        dedup_ndjson(before_in, before_out.clone(), true, None, None, None).expect("dedup");

        let after_dir = tempdir().expect("tempdir");
        let (after_in, after_out) = write_edges(
            after_dir.path(),
            "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.99\"}\n",
        );
        dedup_ndjson(after_in, after_out.clone(), true, None, None, None).expect("dedup");

        assert_ne!(edge_ids(&before_out), edge_ids(&after_out));
    }

    #[test]
    fn uuid_fields_that_are_not_a_key_abort_the_build() {
        // WHY: narrowing the hash inputs can make two DIFFERENT edges share an id.
        // Shipping both would emit duplicate ids; dropping one would lose data. Fail,
        // and name the fields that would disambiguate.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.01\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.99\"}\n"
            ),
        );
        let fields = Some(vec![
            "subject".to_string(),
            "predicate".to_string(),
            "object".to_string(),
        ]);
        let error = dedup_ndjson(input, output, true, None, fields, None).expect_err("not a key");
        let message = error.to_string();
        assert!(message.contains("uuid-fields-not-a-key"), "{message}");
        assert!(message.contains("p_value"), "{message}");
        assert!(message.contains("subject=A"), "{message}");
    }

    #[test]
    fn identical_edges_still_collapse_under_declared_uuid_fields() {
        // WHY: a true repeat is not a key violation -- it is the duplicate the deduper
        // exists to collapse.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.01\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.01\"}\n"
            ),
        );
        let fields = Some(vec!["subject".to_string(), "object".to_string()]);
        dedup_ndjson(input, output.clone(), true, None, fields, None).expect("dedup edges");
        assert_eq!(edge_ids(&output).len(), 1);
    }

    #[test]
    fn key_order_alone_no_longer_ships_a_duplicate_id() {
        // WHY: `uuid_for_json_object` sorts keys but the emitted bytes preserve insertion
        // order, so byte-keyed dedup used to keep BOTH of these -- same id, two lines.
        // Keying on the id collapses them.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"object\":\"B\"}\n",
                "{\"object\":\"B\",\"subject\":\"A\"}\n"
            ),
        );
        dedup_ndjson(input, output.clone(), true, None, None, None).expect("dedup edges");
        assert_eq!(edge_ids(&output).len(), 1);
    }

    #[test]
    fn the_domain_separates_identical_edges_across_graphs() {
        // WHY: two graphs asserting the same triple must not mint the same id, which is
        // what makes a narrow `uuid_fields` safe across KGs.
        let left_dir = tempdir().expect("tempdir");
        let (left_in, left_out) =
            write_edges(left_dir.path(), "{\"subject\":\"A\",\"object\":\"B\"}\n");
        dedup_ndjson(
            left_in,
            left_out.clone(),
            true,
            Some("infores:left".to_string()),
            None,
            None,
        )
        .expect("dedup");

        let right_dir = tempdir().expect("tempdir");
        let (right_in, right_out) =
            write_edges(right_dir.path(), "{\"subject\":\"A\",\"object\":\"B\"}\n");
        dedup_ndjson(
            right_in,
            right_out.clone(),
            true,
            Some("infores:right".to_string()),
            None,
            None,
        )
        .expect("dedup");

        assert_ne!(edge_ids(&left_out), edge_ids(&right_out));
    }

    #[test]
    fn differing_keys_reports_only_real_differences_and_never_the_id() {
        let left = serde_json::json!({"id": "x", "subject": "A", "p_value": "0.01"});
        let right =
            serde_json::json!({"id": "y", "subject": "A", "p_value": "0.99", "effect_size": 1.0});
        assert_eq!(
            differing_keys(&left, &right),
            vec!["effect_size", "p_value"]
        );
    }

    fn merged_edge(output: &std::path::Path) -> Value {
        let lines: Vec<String> = fs::read_to_string(output)
            .expect("read output")
            .lines()
            .map(str::to_string)
            .collect();
        assert_eq!(lines.len(), 1, "expected one merged edge, got {lines:?}");
        serde_json::from_str(&lines[0]).expect("json")
    }

    const SPO: &[&str] = &["subject", "predicate", "object"];

    fn spo_fields() -> Option<Vec<String>> {
        Some(SPO.iter().map(ToString::to_string).collect())
    }

    #[test]
    fn merge_mode_folds_divergent_edges_into_one() {
        // WHY: `uuid_on_collision: merge`. Two rows whose raw mention spellings resolve to
        // the same CURIE derive one id; merge mode unions their evidence into a single
        // edge instead of aborting with `uuid-fields-not-a-key`.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.01\",\"publications\":[\"PMID:2\",\"PMID:1\"]}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.99\",\"publications\":[\"PMID:3\",\"PMID:1\"]}\n"
            ),
        );
        let (merged, conflicts) = dedup_ndjson(
            input,
            output.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        assert_eq!((merged, conflicts), (1, 1));
        let edge = merged_edge(&output);
        // List fields union, dedup, and sort; scalar conflicts keep the first value.
        assert_eq!(
            edge["publications"],
            serde_json::json!(["PMID:1", "PMID:2", "PMID:3"])
        );
        assert_eq!(edge["p_value"], serde_json::json!("0.01"));
    }

    #[test]
    fn merge_mode_suppresses_exact_repeats_without_remerging() {
        // WHY: an exact repeat is a Duplicate even when it repeats a DIVERGENT record
        // already folded into the merged edge -- re-merging it would leave the record
        // unchanged but double-count the scalar conflict in the summary.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.01\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.99\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.99\"}\n"
            ),
        );
        let (merged, conflicts) = dedup_ndjson(
            input,
            output.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        assert_eq!((merged, conflicts), (1, 1));
        assert_eq!(merged_edge(&output)["p_value"], serde_json::json!("0.01"));
    }

    #[test]
    fn merge_mode_output_is_order_independent() {
        // WHY: merged output must not depend on which row the source happened to emit
        // first, or two builds of one graph would diverge. List unions sort by canonical
        // bytes, so both arrival orders produce byte-identical output.
        let left_dir = tempdir().expect("tempdir");
        let (left_in, left_out) = write_edges(
            left_dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"publications\":[\"PMID:2\"]}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"publications\":[\"PMID:1\",\"PMID:3\"]}\n"
            ),
        );
        dedup_ndjson(
            left_in,
            left_out.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        let right_dir = tempdir().expect("tempdir");
        let (right_in, right_out) = write_edges(
            right_dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"publications\":[\"PMID:1\",\"PMID:3\"]}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"publications\":[\"PMID:2\"]}\n"
            ),
        );
        dedup_ndjson(
            right_in,
            right_out.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        assert_eq!(
            fs::read_to_string(left_out).expect("read left"),
            fs::read_to_string(right_out).expect("read right")
        );
    }

    #[test]
    fn merge_mode_recomputes_number_of_cases_from_case_id_union() {
        // WHY: the hardcoded merge rule. Two builds of one edge (e.g. different FAERS
        // quarters) each know their own case count and case IDs; first-wins would keep
        // the left count and summing would double-count the shared case. The merged
        // count is the size of the UNION of both `supporting_case_ids` lists -- exact.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"number_of_cases\":2,\"supporting_case_ids\":[\"case:1\",\"case:2\"]}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"number_of_cases\":5,\"supporting_case_ids\":[\"case:2\",\"case:3\"]}\n"
            ),
        );
        let (merged, conflicts) = dedup_ndjson(
            input,
            output.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        // The recomputed count supersedes the 2-vs-5 divergence: no scalar conflict.
        assert_eq!((merged, conflicts), (1, 0));
        let edge = merged_edge(&output);
        // |{case:1, case:2} u {case:2, case:3}| = 3 -- the shared ID counts once.
        assert_eq!(edge["number_of_cases"], serde_json::json!(3));
        // The carrier is build-internal and must never ship.
        assert!(edge.get("supporting_case_ids").is_none());
    }

    #[test]
    fn merge_mode_one_sided_case_ids_still_recompute_the_count() {
        // WHY: only one side carries `supporting_case_ids`. The union is that side's
        // list, but the other side still contributed a `number_of_cases`, so the count
        // is recomputed to the union length rather than kept from either record.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"number_of_cases\":7}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"supporting_case_ids\":[\"case:1\",\"case:2\"]}\n"
            ),
        );
        let (merged, conflicts) = dedup_ndjson(
            input,
            output.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        assert_eq!((merged, conflicts), (1, 0));
        let edge = merged_edge(&output);
        assert_eq!(edge["number_of_cases"], serde_json::json!(2));
        assert!(edge.get("supporting_case_ids").is_none());
    }

    #[test]
    fn merge_mode_without_case_ids_keeps_first_wins_number_of_cases() {
        // WHY: the rule only fires when the carrier is present. With no
        // `supporting_case_ids` on either record, `number_of_cases` is an ordinary
        // first-wins scalar and the divergence is still counted as a conflict.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"number_of_cases\":2}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"number_of_cases\":5}\n"
            ),
        );
        let (merged, conflicts) = dedup_ndjson(
            input,
            output.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        assert_eq!((merged, conflicts), (1, 1));
        assert_eq!(
            merged_edge(&output)["number_of_cases"],
            serde_json::json!(2)
        );
    }

    #[test]
    fn merge_mode_case_count_is_order_independent() {
        // WHY: the recomputed count derives from the sorted canonical-bytes list union,
        // so both arrival orders must produce byte-identical output, count included.
        let left_dir = tempdir().expect("tempdir");
        let (left_in, left_out) = write_edges(
            left_dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"number_of_cases\":1,\"supporting_case_ids\":[\"case:2\"]}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"number_of_cases\":2,\"supporting_case_ids\":[\"case:1\",\"case:3\"]}\n"
            ),
        );
        dedup_ndjson(
            left_in,
            left_out.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        let right_dir = tempdir().expect("tempdir");
        let (right_in, right_out) = write_edges(
            right_dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"number_of_cases\":2,\"supporting_case_ids\":[\"case:1\",\"case:3\"]}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"number_of_cases\":1,\"supporting_case_ids\":[\"case:2\"]}\n"
            ),
        );
        dedup_ndjson(
            right_in,
            right_out.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        let count = merged_edge(&right_out)["number_of_cases"].clone();
        assert_eq!(
            fs::read_to_string(left_out).expect("read left"),
            fs::read_to_string(right_out).expect("read right")
        );
        assert_eq!(count, serde_json::json!(3));
    }

    #[test]
    fn streaming_edges_never_ship_supporting_case_ids() {
        // WHY: the carrier is stripped on the default path too, so a graph that never
        // opts into merge mode still cannot leak the build-internal field.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"number_of_cases\":2,\"supporting_case_ids\":[\"case:1\",\"case:2\"]}\n",
        );
        dedup_ndjson(input, output.clone(), true, None, spo_fields(), None).expect("dedup");

        let edge = merged_edge(&output);
        assert_eq!(edge["number_of_cases"], serde_json::json!(2));
        assert!(edge.get("supporting_case_ids").is_none());
    }

    #[test]
    fn merge_mode_writes_edges_in_first_seen_order() {
        // WHY: buffering must not reorder the graph -- the first record to claim each id
        // fixes its position in the output.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"publications\":[\"PMID:1\"]}\n",
                "{\"subject\":\"X\",\"predicate\":\"r\",\"object\":\"Y\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"publications\":[\"PMID:2\"]}\n"
            ),
        );
        dedup_ndjson(
            input,
            output.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        let lines: Vec<Value> = fs::read_to_string(output)
            .expect("read output")
            .lines()
            .map(|line| serde_json::from_str(line).expect("json"))
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["subject"], serde_json::json!("A"));
        assert_eq!(
            lines[0]["publications"],
            serde_json::json!(["PMID:1", "PMID:2"])
        );
        assert_eq!(lines[1]["subject"], serde_json::json!("X"));
    }

    #[test]
    fn merge_mode_dedups_list_objects_by_canonical_bytes() {
        // WHY: `sources` entries are objects; two entries that differ only in key order
        // are the same source and must collapse, and the union must sort so merged output
        // is deterministic.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"sources\":[{\"resource_id\":\"infores:b\",\"resource_role\":\"primary_knowledge_source\"}]}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"sources\":[{\"resource_role\":\"primary_knowledge_source\",\"resource_id\":\"infores:b\"},{\"resource_id\":\"infores:a\",\"resource_role\":\"aggregator_knowledge_source\"}]}\n"
            ),
        );
        dedup_ndjson(
            input,
            output.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        let edge = merged_edge(&output);
        let sources = edge["sources"].as_array().expect("sources array");
        assert_eq!(
            sources.len(),
            2,
            "key order alone must not keep both entries"
        );
        assert_eq!(sources[0]["resource_id"], serde_json::json!("infores:a"));
        assert_eq!(sources[1]["resource_id"], serde_json::json!("infores:b"));
    }

    #[test]
    fn format_original_values_uses_sorted_pipe_joining() {
        let empty: FxHashSet<String> = FxHashSet::default();
        assert_eq!(format_original_values(&empty), "");

        let one = FxHashSet::from_iter([String::from("only")]);
        assert_eq!(format_original_values(&one), "only");

        let two = FxHashSet::from_iter([String::from("z"), String::from("a")]);
        assert_eq!(format_original_values(&two), "a|z");

        let three = FxHashSet::from_iter([String::from("z"), String::from("a"), String::from("m")]);
        assert_eq!(format_original_values(&three), "a|m|z");
    }

    #[test]
    fn merge_mode_aggregates_original_scalars_across_all_records() {
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"original_subject\":\"z\",\"original_object\":\"\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"original_subject\":\"a\",\"original_object\":\"x\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"original_subject\":\"m\",\"original_object\":\"y\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"original_subject\":\"a\",\"original_object\":\"x\"}\n"
            ),
        );
        let (merged, conflicts) = dedup_ndjson(
            input,
            output.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        assert_eq!((merged, conflicts), (2, 0));
        let edge = merged_edge(&output);
        assert_eq!(edge["original_subject"], serde_json::json!("a|m|z"));
        assert_eq!(edge["original_object"], serde_json::json!("x|y"));
    }

    #[test]
    fn merge_mode_ignores_empty_originals_and_is_order_independent() {
        let write = |dir: &std::path::Path, rows: &str| {
            let (input, output) = write_edges(dir, rows);
            dedup_ndjson(
                input,
                output.clone(),
                true,
                None,
                spo_fields(),
                Some("merge".to_string()),
            )
            .expect("merge dedup");
            merged_edge(&output)["original_subject"].clone()
        };
        let first = write(
            tempdir().expect("tempdir").path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"original_subject\":\"\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"original_subject\":\"only\"}\n"
            ),
        );
        let second = write(
            tempdir().expect("tempdir").path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"original_subject\":\"only\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"original_subject\":\"\"}\n"
            ),
        );
        assert_eq!(first, serde_json::json!("only"));
        assert_eq!(first, second);
    }

    #[test]
    fn merge_mode_keeps_fields_only_the_second_record_carries() {
        // WHY: first-wins arbitrates CONFLICTS; a field absent from the first record is
        // not a conflict, it is extra evidence, and dropping it would lose data.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"effect_size\":1.5}\n"
            ),
        );
        let (merged, conflicts) = dedup_ndjson(
            input,
            output.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        assert_eq!((merged, conflicts), (1, 0));
        assert_eq!(merged_edge(&output)["effect_size"], serde_json::json!(1.5));
    }

    #[test]
    fn explicit_error_mode_still_aborts_on_divergent_edges() {
        // WHY: `uuid_on_collision` defaults to `error`; naming it explicitly must behave
        // exactly like the default.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.01\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"p_value\":\"0.99\"}\n"
            ),
        );
        let error = dedup_ndjson(
            input,
            output,
            true,
            None,
            spo_fields(),
            Some("error".to_string()),
        )
        .expect_err("not a key");
        assert!(
            error.to_string().contains("uuid-fields-not-a-key"),
            "{error}"
        );
    }

    #[test]
    fn unknown_on_collision_is_rejected() {
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(dir.path(), "{\"subject\":\"A\",\"object\":\"B\"}\n");
        let error = dedup_ndjson(input, output, true, None, None, Some("bogus".to_string()))
            .expect_err("unknown mode");
        assert!(error.to_string().contains("bogus"), "{error}");
    }

    #[test]
    fn merge_mode_sorts_only_lists_that_went_through_a_union() {
        // WHY: the US-002 fold defers sorting to write-out and sorts ONLY fields that saw
        // a real union (both sides arrays). A list copied from a later record is evidence
        // in its source order and must NOT be sorted -- byte parity with the pre-US-002
        // fold, pinned here directly in addition to the fuzz oracle.
        let dir = tempdir().expect("tempdir");
        let (input, output) = write_edges(
            dir.path(),
            concat!(
                // Group 1 (subject A): first record has no `tags`; the second copies its
                // deliberately unsorted list in -- never unioned, so it stays unsorted.
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\"}\n",
                "{\"subject\":\"A\",\"predicate\":\"r\",\"object\":\"B\",\"tags\":[\"z\",\"m\"]}\n",
                // Group 2 (subject X): both records carry `tags`, so a real union runs
                // and the write-out sorts.
                "{\"subject\":\"X\",\"predicate\":\"r\",\"object\":\"Y\",\"tags\":[\"z\",\"m\"]}\n",
                "{\"subject\":\"X\",\"predicate\":\"r\",\"object\":\"Y\",\"tags\":[\"a\"]}\n"
            ),
        );
        let (merged, conflicts) = dedup_ndjson(
            input,
            output.clone(),
            true,
            None,
            spo_fields(),
            Some("merge".to_string()),
        )
        .expect("merge dedup");

        assert_eq!((merged, conflicts), (2, 0));
        let lines: Vec<Value> = fs::read_to_string(output)
            .expect("read output")
            .lines()
            .map(|line| serde_json::from_str(line).expect("json"))
            .collect();
        assert_eq!(lines.len(), 2);
        // Copied, never unioned: source order preserved.
        assert_eq!(lines[0]["tags"], serde_json::json!(["z", "m"]));
        // Unioned: sorted by canonical bytes.
        assert_eq!(lines[1]["tags"], serde_json::json!(["a", "m", "z"]));
    }
}

/// Seeded fuzz equivalence gate for the merge-mode fold.
///
/// Drives the CURRENT production path (through the real `dedup_ndjson` entry point) and
/// the frozen `merge_records_reference` / `MergeIndexReference` oracle with the SAME
/// reproducible randomized stream and demands byte-identical output, identical output
/// order, and identical `(merged, scalar_conflicts)` counters. Trivially green while the
/// reference IS the current algorithm; it becomes the tripwire for the US-002 rewrite.
#[cfg(test)]
mod merge_fold_reference {
    use super::{
        dedup_ndjson, edge_id_bytes, finalize_record, runtime_error, strip_internal_edge_fields,
        Finalized, MergeIndexReference,
    };
    use crate::json::{canonical_json_bytes, emitted_json_bytes};
    use pyo3::prelude::*;
    use serde_json::Value;
    use std::fs;
    use tempfile::tempdir;

    /// The same pass as `dedup_edges_merge`, but every fold runs through the frozen
    /// reference oracle instead of the production `MergeIndex`.
    fn reference_pipeline(
        lines: &[String],
        domain: &str,
        fields: &[String],
    ) -> PyResult<(Vec<u8>, u64, u64)> {
        let mut index: MergeIndexReference = MergeIndexReference::default();
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(line).map_err(runtime_error)?;
            let Some(Finalized { value, content }) =
                finalize_record(value, true, domain, Some(fields))?
            else {
                continue;
            };
            index.absorb(edge_id_bytes(&value)?, value, content)?;
        }
        let mut output: Vec<u8> = Vec::new();
        for id in std::mem::take(&mut index.order) {
            let Some((_, mut value)) = index.records.remove(&id) else {
                continue;
            };
            strip_internal_edge_fields(&mut value);
            output.extend_from_slice(&emitted_json_bytes(&value).map_err(runtime_error)?);
            output.push(b'\n');
        }
        Ok((output, index.merged, index.scalar_conflicts))
    }

    /// Tiny deterministic PRNG (splitmix64): the fuzz stream must be reproducible across
    /// machines without pulling in a `rand` dependency.
    struct Rng(u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z: u64 = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, bound: usize) -> usize {
            (self.next_u64() % u64::try_from(bound).expect("bound fits in u64")) as usize
        }

        fn one_in(&mut self, odds: usize) -> bool {
            self.below(odds) == 0
        }
    }

    fn shuffle<T>(rng: &mut Rng, items: &mut [T]) {
        for high in (1..items.len()).rev() {
            let low: usize = rng.below(high + 1);
            items.swap(high, low);
        }
    }

    const SUBJECTS: [&str; 4] = ["MONDO:1", "MONDO:2", "MONDO:3", "MONDO:4"];
    const PREDICATES: [&str; 2] = ["biolink:related_to", "biolink:associated_with"];
    const OBJECTS: [&str; 3] = ["NCBIGene:1", "NCBIGene:2", "NCBIGene:3"];
    const TAGS: [&str; 5] = ["tag:a", "tag:b", "tag:c", "tag:d", "tag:e"];
    const CASES: [&str; 6] = ["case:1", "case:2", "case:3", "case:4", "case:5", "case:6"];
    const P_VALUES: [&str; 3] = ["0.01", "0.05", "0.99"];

    /// One of two logical `sources` entries, emitted in a random key order: the fold must
    /// collapse key-order variants of the same object via canonical bytes.
    fn source_object(rng: &mut Rng) -> Value {
        let (id, role): (&str, &str) = if rng.one_in(2) {
            ("infores:one", "primary_knowledge_source")
        } else {
            ("infores:two", "aggregator_knowledge_source")
        };
        let mut map = serde_json::Map::new();
        if rng.one_in(2) {
            map.insert("resource_id".to_string(), Value::String(id.to_string()));
            map.insert("resource_role".to_string(), Value::String(role.to_string()));
        } else {
            map.insert("resource_role".to_string(), Value::String(role.to_string()));
            map.insert("resource_id".to_string(), Value::String(id.to_string()));
        }
        Value::Object(map)
    }

    fn fuzz_record(rng: &mut Rng, group: usize, record_index: usize) -> Value {
        let mut map = serde_json::Map::new();
        // The identity triple, inserted in a random key order: all records of a group
        // derive the same id while their bytes diverge, so the fold decides the outcome.
        let triple: [(&str, &str); 3] = [
            ("subject", SUBJECTS[rng.below(SUBJECTS.len())]),
            ("predicate", PREDICATES[rng.below(PREDICATES.len())]),
            ("object", OBJECTS[rng.below(OBJECTS.len())]),
        ];
        let mut order: [usize; 3] = [0, 1, 2];
        shuffle(rng, &mut order);
        for index in order {
            let (key, value) = triple[index];
            map.insert(key.to_string(), Value::String(value.to_string()));
        }
        // Scalar fields drawn from small pools -> first-wins conflicts.
        map.insert(
            "p_value".to_string(),
            Value::String(P_VALUES[rng.below(P_VALUES.len())].to_string()),
        );
        map.insert(
            "effect_size".to_string(),
            Value::Number(serde_json::Number::from(rng.below(4) as u64)),
        );
        if rng.one_in(4) {
            map.insert("negated".to_string(), Value::Bool(rng.one_in(2)));
        }
        // String list field drawn WITH replacement: a record may repeat an item inside its
        // own array (stored-side duplicates survive; incoming-side ones collapse).
        if rng.one_in(2) {
            let mut tags: Vec<Value> = Vec::new();
            for _ in 0..rng.below(4) {
                tags.push(Value::String(TAGS[rng.below(TAGS.len())].to_string()));
            }
            map.insert("tags".to_string(), Value::Array(tags));
        }
        // Object list field with key-order variants -> canonical-bytes union + sort.
        if rng.one_in(2) {
            let mut sources: Vec<Value> = Vec::new();
            for _ in 0..1 + rng.below(2) {
                sources.push(source_object(rng));
            }
            map.insert("sources".to_string(), Value::Array(sources));
        }
        // Scalar-vs-array conflict on one field.
        if rng.one_in(3) {
            let mode: Value = if rng.one_in(2) {
                Value::String("solo".to_string())
            } else {
                serde_json::json!(["solo", "extra"])
            };
            map.insert("mode".to_string(), mode);
        }
        // A field only LATER records of the group carry (fold must copy it, not conflict).
        if record_index > 0 && (group.is_multiple_of(3) || rng.one_in(2)) {
            map.insert("late".to_string(), Value::String(format!("late:{group}")));
        }
        // The same "only a later record carries it" shape, but LIST-valued and on exactly
        // ONE record per group: the field is COPIED into the stored record and never
        // unioned, so `unioned` stays false and the deferred write-out sort must leave it
        // in its SOURCE order. The items are drawn strictly DESCENDING by canonical bytes
        // (`TAGS` reversed), which is what makes a stray sort observable --
        // `merge_fold_matches_reference_on_fuzz` counts the descending survivors. Before
        // US-006 no fuzz dataset produced this shape at all, so the `unioned == false` gate
        // (the subtlest semantic in the fold) rested on one hand-written test.
        if group.is_multiple_of(2) && record_index == 1 {
            let descending: Vec<Value> = TAGS
                .iter()
                .rev()
                .take(2 + rng.below(3))
                .map(|tag| Value::String(tag.to_string()))
                .collect();
            map.insert("late_list".to_string(), Value::Array(descending));
        }
        // The `number_of_cases` carrier pair in its three shapes: count + list, list only,
        // or absent. The count is deliberately wrong sometimes -- the recompute supersedes
        // it and must also undo the scalar conflict it would otherwise have counted.
        if rng.one_in(2) {
            let mut case_ids: Vec<Value> = Vec::new();
            for _ in 0..1 + rng.below(4) {
                case_ids.push(Value::String(CASES[rng.below(CASES.len())].to_string()));
            }
            map.insert(
                "supporting_case_ids".to_string(),
                Value::Array(case_ids.clone()),
            );
            if !rng.one_in(4) {
                let count: u64 = if rng.one_in(3) {
                    case_ids.len() as u64
                } else {
                    (case_ids.len() + 1 + rng.below(3)) as u64
                };
                map.insert(
                    "number_of_cases".to_string(),
                    Value::Number(serde_json::Number::from(count)),
                );
            }
        }
        Value::Object(map)
    }

    fn fuzz_stream(rng: &mut Rng) -> Vec<String> {
        // 24 groups of 1-8 divergent same-id records, arrival order shuffled so first-seen
        // id order and fold order disagree.
        let mut records: Vec<Value> = Vec::new();
        for group in 0..24 {
            for record_index in 0..1 + rng.below(8) {
                records.push(fuzz_record(rng, group, record_index));
            }
        }
        shuffle(rng, &mut records);
        let mut lines: Vec<String> = records
            .iter()
            .map(|record| serde_json::to_string(record).expect("serialize fuzz record"))
            .collect();
        // Exact byte repeats -- including of records that already diverged -- which the
        // content-hash membership must suppress without merging or counting.
        let unique: usize = lines.len();
        for _ in 0..12 {
            lines.push(lines[rng.below(unique)].clone());
        }
        shuffle(rng, &mut lines);
        // Blank lines and empty objects are legal stream noise the pass must skip.
        let mut stream: Vec<String> = Vec::new();
        for line in lines {
            if rng.one_in(14) {
                stream.push(String::new());
            }
            if rng.one_in(20) {
                stream.push("   ".to_string());
            }
            if rng.one_in(16) {
                stream.push("{}".to_string());
            }
            stream.push(line);
        }
        stream
    }

    /// Count the emitted records that carry `late_list`, and how many of those kept it in
    /// strictly DESCENDING canonical order -- proof that a list merely COPIED from one
    /// record was never sorted at write-out (the `unioned == false` gate).
    fn late_list_order(output: &[u8]) -> PyResult<(usize, usize)> {
        let mut carriers: usize = 0;
        let mut preserved: usize = 0;
        for line in String::from_utf8_lossy(output).lines() {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(line).map_err(runtime_error)?;
            let Some(items) = value.get("late_list").and_then(Value::as_array) else {
                continue;
            };
            carriers += 1;
            let mut bytes: Vec<Vec<u8>> = Vec::with_capacity(items.len());
            for item in items {
                bytes.push(canonical_json_bytes(item).map_err(runtime_error)?);
            }
            if bytes.len() >= 2 && bytes.windows(2).all(|pair| pair[0] > pair[1]) {
                preserved += 1;
            }
        }
        Ok((carriers, preserved))
    }

    #[test]
    fn merge_fold_matches_reference_on_fuzz() {
        // WHY: US-002 will rewrite the merge fold for speed. This seeded fuzz drives the
        // CURRENT fold through the real `dedup_ndjson` entry point and the frozen
        // `merge_records_reference` oracle with the SAME randomized stream -- divergent
        // same-id groups of varying sizes, list unions over object and scalar items,
        // key-order variants, fields only later records carry, scalar-vs-array conflicts,
        // exact byte repeats, empty objects, and blank lines -- and demands byte-identical
        // records in identical order plus identical `(merged, scalar_conflicts)` counters.
        let mut rng = Rng(0x5EED_2024_0000_0001);
        let lines: Vec<String> = fuzz_stream(&mut rng);
        let domain: String = "infores:multiomicskg".to_string();
        let fields: Vec<String> = ["subject", "predicate", "object"]
            .iter()
            .map(ToString::to_string)
            .collect();

        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("fuzz.ndjson.tmp");
        let output = dir.path().join("fuzz.ndjson");
        fs::write(&input, lines.join("\n") + "\n").expect("write fuzz input");
        let current: (u64, u64) = dedup_ndjson(
            input,
            output.clone(),
            true,
            Some(domain.clone()),
            Some(fields.clone()),
            Some("merge".to_string()),
        )
        .expect("merge dedup");
        let current_bytes: Vec<u8> = fs::read(&output).expect("read merged output");

        let (reference_bytes, reference_merged, reference_conflicts): (Vec<u8>, u64, u64) =
            reference_pipeline(&lines, &domain, &fields).expect("reference pipeline");

        // Non-vacuity: the seeded stream must actually exercise the fold -- records merge,
        // scalars conflict, several ids survive, and the carrier never leaks -- or the
        // equivalence check could pass on a trivially degenerate input.
        let output_lines: usize = current_bytes.iter().filter(|byte| **byte == b'\n').count();
        assert!(
            current.0 >= 10,
            "expected real folding, merged={}",
            current.0
        );
        assert!(
            current.1 >= 1,
            "expected scalar conflicts, got {}",
            current.1
        );
        assert!(
            output_lines >= 8,
            "expected distinct ids, got {output_lines}"
        );
        assert!(
            !current_bytes
                .windows("supporting_case_ids".len())
                .any(|window| window == b"supporting_case_ids"),
            "build-internal carrier leaked into the merged output"
        );

        assert_eq!(
            current,
            (reference_merged, reference_conflicts),
            "counters diverged from the frozen reference"
        );
        assert_eq!(
            current_bytes, reference_bytes,
            "merged output diverged from the frozen reference"
        );

        // The copied-never-unioned gate, asserted on the shape `fuzz_record` was extended
        // to emit (US-006): `late_list` arrives strictly descending, so any write-out that
        // sorted a merely COPIED list would flip it ascending and drop this count. It is a
        // lower bound, not an exact count, because the identity triple is drawn per RECORD,
        // so two carriers can land on one derived id -- those legitimately union and sort
        // (and both implementations still agree, checked above). Zero would mean the
        // unsorted-copy shape never reached the output at all.
        let (carriers, preserved): (usize, usize) =
            late_list_order(&current_bytes).expect("parse merged output");
        println!("copied-never-unioned `late_list`: {carriers} emitted carriers, {preserved} kept their source (descending) order");
        assert!(
            preserved >= 3,
            "expected the copied, never-unioned `late_list` to keep its source (descending) \
             order on at least 3 emitted records, got {preserved} of {carriers}"
        );
    }
}

/// Performance bound gate for the merge-mode fold.
///
/// US-002 rewrote the merge fold from quadratic to near-linear. Equivalence with the
/// frozen pre-US-002 oracle is policed by `merge_fold_reference` above; this module
/// polices the SPEED: it drives the production `MergeIndex` and the frozen
/// `MergeIndexReference` through the SAME seeded quadratic-shaped workload in-process
/// and demands the production fold win by a fixed ratio. A ratio -- not an absolute
/// wall-clock limit -- is machine-independent, because both legs share the same core,
/// allocator, and input.
///
/// The module is deliberately self-contained (own splitmix64 `Rng`, own generators):
/// the frozen equivalence harness above must stay byte-verbatim.
#[cfg(test)]
mod merge_fold_speedup {
    use super::{runtime_error, strip_internal_edge_fields, MergeIndex, MergeIndexReference};
    use crate::json::{canonical_json_bytes, emitted_json_bytes};
    use pyo3::prelude::*;
    use serde_json::Value;
    use std::time::{Duration, Instant};
    use uuid::Uuid;
    use xxhash_rust::xxh64::xxh64;

    /// Workload shape: `GROUPS` ids, each accumulating `RECORDS_PER_GROUP` divergent
    /// records whose unioned lists grow into the hundreds of items. Sized so the frozen
    /// quadratic leg takes ~1-5s in debug builds and the near-linear leg milliseconds;
    /// the whole test stays in single-digit seconds.
    const GROUPS: usize = 16;
    const RECORDS_PER_GROUP: usize = 280;
    /// The near-linear fold must beat the frozen quadratic oracle by at least this
    /// factor. The observed margin is far larger (the reference re-canonicalizes and
    /// re-sorts EVERY stored list item on EVERY fold), so the bound leaves ample headroom
    /// for machine noise while still tripping on any regression back toward quadratic.
    const BOUND: f64 = 5.0;
    /// Timing attempts for the NEW-fold leg (the reference leg stays single-shot). The new
    /// fold finishes in milliseconds, where one shot measures mostly scheduler and
    /// allocator noise; the MINIMUM over three attempts is the stable estimate of the leg's
    /// own cost, because noise only ever ADDS time.
    const NEW_FOLD_ATTEMPTS: usize = 3;
    const WORKLOAD_SEED: u64 = 0x5EED_2024_0000_0003;
    const WARMUP_SEED: u64 = 0x5EED_2024_0000_0004;

    const SUBJECTS: [&str; 4] = ["MONDO:1", "MONDO:2", "MONDO:3", "MONDO:4"];
    const PREDICATES: [&str; 2] = ["biolink:related_to", "biolink:associated_with"];
    const OBJECTS: [&str; 3] = ["NCBIGene:1", "NCBIGene:2", "NCBIGene:3"];
    const P_VALUES: [&str; 3] = ["0.01", "0.05", "0.99"];
    const CASES: [&str; 6] = ["case:1", "case:2", "case:3", "case:4", "case:5", "case:6"];

    /// Tiny deterministic PRNG (splitmix64): a COPY of `merge_fold_reference::Rng` --
    /// sharing it would mean editing the frozen harness, so the copy is deliberate.
    struct Rng(u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z: u64 = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, bound: usize) -> usize {
            (self.next_u64() % u64::try_from(bound).expect("bound fits in u64")) as usize
        }

        fn one_in(&mut self, odds: usize) -> bool {
            self.below(odds) == 0
        }
    }

    fn shuffle<T>(rng: &mut Rng, items: &mut [T]) {
        for high in (1..items.len()).rev() {
            let low: usize = rng.below(high + 1);
            items.swap(high, low);
        }
    }

    /// One of many logical `sources` entries, emitted in a random key order: the union
    /// must collapse key-order variants of the same object via canonical bytes.
    fn source_variant(rng: &mut Rng, identity: usize) -> Value {
        let resource_id: String = format!("infores:bulk{identity}");
        let role: &str = if identity.is_multiple_of(2) {
            "primary_knowledge_source"
        } else {
            "aggregator_knowledge_source"
        };
        let mut map = serde_json::Map::new();
        if rng.one_in(2) {
            map.insert("resource_id".to_string(), Value::String(resource_id));
            map.insert("resource_role".to_string(), Value::String(role.to_string()));
        } else {
            map.insert("resource_role".to_string(), Value::String(role.to_string()));
            map.insert("resource_id".to_string(), Value::String(resource_id));
        }
        Value::Object(map)
    }

    /// One record ready for `absorb`: the derived id bytes, the labeled record, and the
    /// xxh64 of its canonical id-free content -- exactly what the production
    /// `dedup_edges_merge` path computes before each fold, minus the file IO and parsing
    /// that are identical for both legs and would only dilute the ratio.
    #[derive(Clone)]
    struct FoldCase {
        id: [u8; 16],
        record: Value,
        content: u64,
    }

    /// The quadratic-shaped workload: many divergent records per id, unioned list fields
    /// growing into the hundreds of items (string items, object items with key-order
    /// variants), scalar conflicts, scalar-vs-array conflicts, the `number_of_cases`
    /// carrier, exact byte repeats, and shuffled arrival order. Deterministic in `seed`,
    /// so two calls with the same arguments yield identical streams.
    fn fold_workload(
        seed: u64,
        groups: usize,
        records_per_group: usize,
    ) -> PyResult<Vec<FoldCase>> {
        let mut rng = Rng(seed);
        // A tag pool large enough that intra-record dedup is rare: each id's unioned
        // `tags` list grows near-linearly toward the hundreds of items.
        let tag_pool: Vec<String> = (0..records_per_group * 4)
            .map(|index| format!("tag:{index}"))
            .collect();
        let mut cases: Vec<FoldCase> = Vec::with_capacity(groups * records_per_group);
        for group in 0..groups {
            let mut id_bytes = [0u8; 16];
            for byte in &mut id_bytes {
                *byte = rng.below(256) as u8;
            }
            let subject: &str = SUBJECTS[rng.below(SUBJECTS.len())];
            let predicate: &str = PREDICATES[rng.below(PREDICATES.len())];
            let object: &str = OBJECTS[rng.below(OBJECTS.len())];
            for record_index in 0..records_per_group {
                let mut map = serde_json::Map::new();
                // The identity triple is constant within a group, so every record of the
                // group derives the same id and the fold decides the outcome.
                map.insert("subject".to_string(), Value::String(subject.to_string()));
                map.insert(
                    "predicate".to_string(),
                    Value::String(predicate.to_string()),
                );
                map.insert("object".to_string(), Value::String(object.to_string()));
                // Scalars from small pools -> first-wins conflicts on most folds.
                map.insert(
                    "p_value".to_string(),
                    Value::String(P_VALUES[rng.below(P_VALUES.len())].to_string()),
                );
                map.insert(
                    "effect_size".to_string(),
                    Value::Number(serde_json::Number::from(rng.below(4) as u64)),
                );
                if rng.one_in(4) {
                    map.insert("negated".to_string(), Value::Bool(rng.one_in(2)));
                }
                // The growing string list: 2-5 items from the large pool per record.
                let mut tags: Vec<Value> = Vec::new();
                for _ in 0..2 + rng.below(4) {
                    tags.push(Value::String(tag_pool[rng.below(tag_pool.len())].clone()));
                }
                map.insert("tags".to_string(), Value::Array(tags));
                // The growing object list with key-order variants: a mix of new items and
                // canonical-byte duplicates of earlier ones.
                if rng.one_in(2) {
                    let mut sources: Vec<Value> = Vec::new();
                    for _ in 0..1 + rng.below(3) {
                        let identity: usize = rng.below(records_per_group);
                        sources.push(source_variant(&mut rng, identity));
                    }
                    map.insert("sources".to_string(), Value::Array(sources));
                }
                // Scalar-vs-array conflict on one field.
                if rng.one_in(3) {
                    let mode: Value = if rng.one_in(2) {
                        Value::String("solo".to_string())
                    } else {
                        serde_json::json!(["solo", "extra"])
                    };
                    map.insert("mode".to_string(), mode);
                }
                // A field only LATER records carry (fold copies it, no conflict).
                if record_index > 0 && rng.one_in(2) {
                    map.insert("late".to_string(), Value::String(format!("late:{group}")));
                }
                // The `number_of_cases` carrier pair, count deliberately wrong sometimes.
                if rng.one_in(2) {
                    let mut case_ids: Vec<Value> = Vec::new();
                    for _ in 0..1 + rng.below(5) {
                        case_ids.push(Value::String(CASES[rng.below(CASES.len())].to_string()));
                    }
                    map.insert(
                        "supporting_case_ids".to_string(),
                        Value::Array(case_ids.clone()),
                    );
                    if !rng.one_in(4) {
                        let count: u64 = case_ids.len() as u64
                            + if rng.one_in(3) {
                                0
                            } else {
                                1 + rng.below(3) as u64
                            };
                        map.insert(
                            "number_of_cases".to_string(),
                            Value::Number(serde_json::Number::from(count)),
                        );
                    }
                }
                let mut record: Value = Value::Object(map);
                // Content hashes the canonical id-free record exactly like
                // `finalize_record`, so an exact byte repeat carries the same content.
                let content: u64 = xxh64(&canonical_json_bytes(&record).map_err(runtime_error)?, 0);
                record.as_object_mut().expect("record is an object").insert(
                    "id".to_string(),
                    Value::String(Uuid::from_bytes(id_bytes).to_string()),
                );
                cases.push(FoldCase {
                    id: id_bytes,
                    record,
                    content,
                });
            }
        }
        // Exact byte repeats (~10%): content-hash suppression must keep them out of BOTH
        // folds without counting a merge.
        let unique: usize = cases.len();
        for _ in 0..unique / 10 {
            cases.push(cases[rng.below(unique)].clone());
        }
        // Arrival order shuffled so first-seen id order and fold order disagree.
        shuffle(&mut rng, &mut cases);
        Ok(cases)
    }

    /// Drive the production fold over a prepared workload: absorb every record, then
    /// `finish` in first-seen order (the one deferred union sort) -- exactly
    /// `dedup_edges_merge` minus the file IO, parsing, and finalization that are
    /// identical for both legs.
    fn run_merge_index(cases: Vec<FoldCase>) -> PyResult<(Vec<u8>, u64, u64)> {
        let mut index: MergeIndex = MergeIndex::default();
        for case in cases {
            index.absorb(case.id, case.record, case.content)?;
        }
        let mut output: Vec<u8> = Vec::new();
        for id in std::mem::take(&mut index.order) {
            let Some(record) = index.records.remove(&id) else {
                continue;
            };
            let mut value: Value = record.finish()?;
            strip_internal_edge_fields(&mut value);
            output.extend_from_slice(&emitted_json_bytes(&value).map_err(runtime_error)?);
            output.push(b'\n');
        }
        Ok((output, index.merged, index.scalar_conflicts))
    }

    /// Drive the frozen quadratic oracle over a prepared workload: the absorb path of
    /// `reference_pipeline` minus the file IO, parsing, and finalization.
    fn run_merge_index_reference(cases: Vec<FoldCase>) -> PyResult<(Vec<u8>, u64, u64)> {
        let mut index: MergeIndexReference = MergeIndexReference::default();
        for case in cases {
            index.absorb(case.id, case.record, case.content)?;
        }
        let mut output: Vec<u8> = Vec::new();
        for id in std::mem::take(&mut index.order) {
            let Some((_, mut value)) = index.records.remove(&id) else {
                continue;
            };
            strip_internal_edge_fields(&mut value);
            output.extend_from_slice(&emitted_json_bytes(&value).map_err(runtime_error)?);
            output.push(b'\n');
        }
        Ok((output, index.merged, index.scalar_conflicts))
    }

    /// WHY: US-002 rewrote the merge fold from quadratic to near-linear, and equivalence
    /// with the frozen oracle is already policed by
    /// `merge_fold_matches_reference_on_fuzz` -- this test guards the SPEED half of that
    /// work. It drives the production `MergeIndex` and the frozen quadratic
    /// `MergeIndexReference` through the SAME seeded quadratic-shaped workload
    /// in-process and demands the new fold beat the reference by at least `BOUND`.
    /// If a future change silently regresses the fold back toward quadratic, this
    /// trips. The bound is a same-process RATIO, not an absolute wall-clock limit:
    /// both legs share the same core, allocator, and input, so the assertion is
    /// machine-independent and needs no per-CI-box tuning.
    #[test]
    fn merge_fold_speedup_bound_vs_reference() {
        // Warmup: lazy allocator/paging work must not bill to whichever leg runs first.
        run_merge_index(fold_workload(WARMUP_SEED, 2, 4).expect("warmup workload"))
            .expect("warmup new fold");
        run_merge_index_reference(fold_workload(WARMUP_SEED, 2, 4).expect("warmup workload"))
            .expect("warmup reference fold");

        // Build the same seeded workload per leg so each one consumes its own owned
        // records and the timed region clones nothing: the ratio measures the fold
        // algorithm alone, not input preparation.
        //
        // The NEW fold is timed FIRST (cold caches bill against it, so a bound that passes
        // anyway is conservative) and BEST-OF-`NEW_FOLD_ATTEMPTS` (US-006): a millisecond
        // leg timed once is dominated by noise, and noise in a ratio's denominator is how
        // a >=5x bound turns flaky. The minimum of the attempts is compared, and each
        // attempt rebuilds its workload so no timed region clones.
        let mut new_elapsed: Duration = Duration::MAX;
        let mut new_outcome: Option<(Vec<u8>, u64, u64)> = None;
        for _ in 0..NEW_FOLD_ATTEMPTS {
            let cases: Vec<FoldCase> =
                fold_workload(WORKLOAD_SEED, GROUPS, RECORDS_PER_GROUP).expect("workload");
            let started: Instant = Instant::now();
            let outcome: (Vec<u8>, u64, u64) = run_merge_index(cases).expect("new fold");
            let elapsed: Duration = started.elapsed();
            if elapsed < new_elapsed {
                new_elapsed = elapsed;
                new_outcome = Some(outcome);
            }
        }
        let (new_bytes, new_merged, new_conflicts): (Vec<u8>, u64, u64) =
            new_outcome.expect("at least one new-fold attempt ran");

        // The frozen quadratic leg stays SINGLE-shot: it already runs for seconds, so its
        // timing is stable, and repeating it would multiply this test's runtime without
        // reducing noise.
        let reference_cases: Vec<FoldCase> =
            fold_workload(WORKLOAD_SEED, GROUPS, RECORDS_PER_GROUP).expect("workload");
        let started: Instant = Instant::now();
        let (reference_bytes, reference_merged, reference_conflicts): (Vec<u8>, u64, u64) =
            run_merge_index_reference(reference_cases).expect("reference fold");
        let reference_elapsed = started.elapsed();

        // Identical input must yield identical outcomes: the ratio measures the
        // algorithm, not an input mismatch.
        assert_eq!(
            (new_merged, new_conflicts),
            (reference_merged, reference_conflicts),
            "fold counters diverged on the identical workload"
        );
        assert_eq!(
            new_bytes, reference_bytes,
            "fold outputs diverged on the identical workload"
        );

        // Non-vacuity: the workload must actually fold heavily, or a ratio on a
        // degenerate input would be meaningless.
        assert!(
            new_merged >= (GROUPS * RECORDS_PER_GROUP * 9 / 10) as u64,
            "expected heavy folding, merged={new_merged}"
        );
        assert!(
            new_conflicts >= 1,
            "expected scalar conflicts, got {new_conflicts}"
        );
        let output_lines: usize = new_bytes.iter().filter(|byte| **byte == b'\n').count();
        assert_eq!(output_lines, GROUPS, "expected one merged record per id");

        let speedup: f64 = reference_elapsed.as_secs_f64() / new_elapsed.as_secs_f64();
        println!(
            "merge fold speedup: new {new_elapsed:.3?} vs frozen quadratic reference \
             {reference_elapsed:.3?} -> {speedup:.1}x (bound {BOUND}x)"
        );
        assert!(
            speedup >= BOUND,
            "the near-linear merge fold lost its speed margin: only {speedup:.2}x faster \
             than the frozen quadratic reference (new fold {new_elapsed:.3?}, reference \
             {reference_elapsed:.3?}); expected >= {BOUND}x on the identical workload"
        );
    }
}

#[cfg(test)]
mod merge_state_desync {
    use super::MergedRecord;
    use serde_json::json;

    /// WHY this test exists: `MergedRecord::finish`'s desync guard is load-bearing
    /// fail-loudly precedent, not decoration. Hash-only edge keying once silently dropped
    /// DISTINCT records at scale (the collision class `record_if_new_suppresses_only_exact_
    /// byte_duplicates` polices); the same silent-data-loss class lurks here if the
    /// `bytes`-parallel-to-`items` invariant ever breaks, because `zip` truncates to the
    /// shorter side while `drain(..)` empties the whole array. This forces that desync and
    /// asserts `finish` REFUSES to write -- returning a structured `merge-state-desync`
    /// error -- proving the guard is a real runtime check observable in EVERY build
    /// profile, not a `debug_assert_eq!` that panics first in tests and can never surface
    /// the structured error path.
    #[test]
    fn merge_state_desync_is_a_structured_error_not_silent_truncation() {
        // Reading a `PyErr`'s message needs an initialized interpreter (pyo3 is built
        // without `auto-initialize`); idempotent, so this is safe alongside the full suite.
        pyo3::Python::initialize();

        // Seed a record whose `ids` array holds three live items; `new` records a matching
        // three-entry canonical-`bytes` list (unioned = false, so `finish` skips it as-is).
        let mut record =
            MergedRecord::new(json!({ "ids": [1, 2, 3] }), 0).expect("seed merged record");

        // Desync it the way a broken parallel-invariant would: drop ONE canonical-byte
        // entry (2 byte-entries left for 3 live items) and mark the field unioned so
        // `finish` routes it through the zip/drain path the guard protects.
        let state = record.lists.get_mut("ids").expect("ids list state");
        state.bytes.pop().expect("a canonical-byte entry to drop");
        state.unioned = true;

        // The guard must fire BEFORE any zip/drain mutates the record: `finish` returns a
        // structured error naming the desync, so nothing is silently truncated/written.
        let error = record
            .finish()
            .expect_err("a desynced list must fail loudly, not silently truncate");
        let message = error.to_string();
        assert!(
            message.contains("merge-state-desync"),
            "expected a structured merge-state-desync error, got: {message}"
        );
    }
}
