# Issue Proposal: SQ Shared Artifact for Distributed IVF_SQ / IVF_HNSW_SQ Builds

- **Target repo**: `lance-format/lance` (intended to be filed as a GitHub issue)
- **Status**: Draft
- **Filed against**: lance Rust core, with downstream impact on `lance-format/lance-spark`
- **Last updated**: 2026-06-25
- **Related**:
  - RFC #7319 — Expose IVF centroids / PQ codebook as public reader APIs
  - PR #7014 — IVF_RQ shared RaBitQ rotation (same failure mode, fixed for RQ)
  - lance-format/lance-spark PR #605 — distributed vector index creation
  - lance-format/lance-spark PR #605, LuciferYang re-review (Jun 24, 2026, comment on `VectorIndexJob.scala:227`)

## Summary

Lance's distributed index commit path silently produces **incorrect** `IVF_SQ` / `IVF_HNSW_SQ`
indexes when the source segments were trained on different subsets of the column. Each per-segment
worker trains its own `ScalarQuantizer` over only its fragments' rows, but
`commit_existing_index_segments` does **not** reconcile per-segment Scalar Quantization (SQ) bounds
across shards — it keeps the first shard's `ScalarQuantizationMetadata` and concatenates u8 codes
byte-for-byte. At query time those codes are decoded with a single global `bounds` value, which means
codes coming from segments trained on different `bounds` are systematically misinterpreted, breaking
distance comparisons and recall.

This is the same failure mode that PR #7014 fixed for `IVF_RQ` by introducing a driver-trained shared
RaBitQ rotation. `IVF_SQ` and `IVF_HNSW_SQ` need an equivalent shared artifact.

## Motivation

Distributed vector index builders (lance-spark, lance-ray) follow a 3-phase pattern modelled on
`lance-format/lance` issue #6309:

1. **Driver phase** — train one global artifact and broadcast it to executors.
2. **Executor phase** — each worker calls `dataset.createIndex(IndexOptions(...).withFragmentIds(batch))`,
   plugging in the shared artifact so all segments share the same quantization grid.
3. **Driver commit** — `dataset.commitExistingIndexSegments(name, column, segments)` atomically merges
   all uncommitted segments into one logical index.

For IVF centroids and PQ codebooks this works because lance-core exposes:

- `VectorTrainer::trainIvfCentroids` + `IvfBuildParams::setCentroids`
- `VectorTrainer::trainPqCodebook` + `PQBuildParams::setCodebook`

For SQ there is **no equivalent**:

- `SQBuildParams { num_bits, sample_rate }` has no `bounds` field
  (`rust/lance-index/src/vector/sq/builder.rs`)
- Java `org.lance.index.vector.SQBuildParams` exposes only `numBits` and `sampleRate`
  — no `setBounds`, unlike PQ's `setCodebook` and IVF's `setCentroids`
- Python `SupportedDistributedIndices` lists `IVF_SQ` but `prepare_global_*` only covers IVF and PQ;
  there is no `prepare_global_ivf_sq`
- Java JNI exposes `nativeTrainIvfCentroids` and `nativeTrainPqCodebook` — no `nativeTrainSq`
- `ScalarQuantizer::with_bounds(...)` exists in Rust but is internal-only (used by deserialization
  paths); it is not reachable from the public distributed indexing flow

As a result, every IVF_SQ build that goes through lance-spark's distributed path today is one of
two cases:

- **Single-segment build** (currently forced as a workaround in lance-spark PR #605): correct, but
  loses parallelism and cannot scale beyond one worker.
- **Multi-segment build**: silently produces an index where query distances are inconsistent across
  partitions whenever per-segment `bounds` diverge.

## Evidence

### 1. SQ merge in `commit_existing_index_segments` does not reconcile bounds

`rust/lance-index/src/vector/distributed/index_merger.rs` documents this explicitly:

> For PQ and SQ, this assumes all selected source segments share the same quantizer/codebook and
> distance type; it reuses the first encountered metadata.

The IVF_SQ branch (around lines 884–941 at the time of writing) only validates that `dim` matches
across shards:

```rust
let sq_meta_parsed: ScalarQuantizationMetadata = serde_json::from_str(&sq_json)?;
let d0 = sq_meta_parsed.dim;
dim.get_or_insert(d0);
if let Some(dprev) = dim && dprev != d0 {
    return Err(Error::index("Dimension mismatch across shards".to_string()));
}
if sq_meta.is_none() {
    sq_meta = Some(sq_meta_parsed.clone());          // first shard only
}
if v2w_opt.is_none() {
    let w = init_writer_for_sq(object_store, &aux_out, dt, &sq_meta_parsed, fv).await?;
    v2w_opt = Some(w);                               // SQ_METADATA_KEY is fixed here
}
```

Compare to the IVF_PQ branch in the same file (around lines 1079–1115), which actively rejects
mismatched codebooks:

```rust
if let Some(existing_pm) = pq_meta.as_ref() {
    if existing_pm.num_sub_vectors != pm.num_sub_vectors
        || existing_pm.nbits != pm.nbits
        || existing_pm.dimension != pm.dimension { return Err(...) }
    let existing_cb = existing_pm.codebook.as_ref()...;
    let current_cb = pm.codebook.as_ref()...;
    if !fixed_size_list_equal(existing_cb, current_cb) {
        const TOL: f32 = 1e-5;
        if !fixed_size_list_almost_equal(existing_cb, current_cb, TOL) {
            return Err(Error::index(
                "PQ codebook content mismatch across shards".to_string(),
            ));
        }
    }
}
```

There is no equivalent `if existing_sq.bounds != current_sq.bounds { ... }` in the SQ or HNSW_SQ
branches.

### 2. SQ codes are byte-copied, not re-quantized

For SQ-typed shards, `merge_partial_vector_auxiliary_files` falls through to `write_partition_rows`
(in the same file, ~lines 1473–1495 / 345–364), which streams each shard's `RecordBatch` byte-for-byte
into the merged file:

```rust
_ => {
    for (pid, total_part_len) in accumulated_lengths.iter().copied().enumerate().take(nlist) {
        for shard in shard_infos.iter() {
            ...
            write_partition_rows(shard.reader.as_ref(), w, offset..offset + part_len).await?;
```

Each shard's u8 codes were quantized using **that shard's** `bounds`, but the merged file's metadata
preserves only the first shard's `bounds`. There is no re-quantization step.

### 3. Query path uses one global `bounds` to decode every code

`rust/lance-index/src/vector/sq/storage.rs` constructs a single `ScalarQuantizer` for the entire
index:

```rust
let quantizer = ScalarQuantizer::with_bounds(num_bits, chunks[0].dim(), bounds);
```

`SQDistCalculator::new` then quantizes the query vector using that single `bounds` and compares it
byte-wise against codes that, post-merge, came from segments with potentially different `bounds`:

```rust
let query_sq_code = match query.data_type() {
    DataType::Float32 => scale_to_u8::<Float32Type>(...values(), &bounds),
    ...
};
let dist = match self.storage.distance_type {
    DistanceType::L2 | DistanceType::Cosine => l2_u8(sq_code, query_sq_code) as f32,
    ...
};
```

When `bounds` differ across source shards, `l2_u8(sq_code_from_shard_2, query_sq_code_decoded_with_shard_1_bounds)`
is mathematically meaningless: the same `u8` value (e.g. `128`) refers to different real-valued ranges
in different shards.

### 4. `ScalarQuantizationMetadata` is index-global, not per-IVF-partition

Defined in `vector/sq/storage.rs` as `{ dim, num_bits, bounds: Range<f64> }` — there is no
`partition_id` dimension. `QuantizerStorage::load_partition` reuses the same metadata for every
partition. So even the workaround "ensure each IVF partition lives in exactly one segment" does not
help: the on-disk model carries one `bounds` for the whole index.

### 5. Worker-local bounds is the documented behavior

`rust/lance/src/index/vector/builder.rs` (~lines 443–517) routes `Q::build(...)` to
`ScalarQuantizer::build` (`sq.rs:139–166`), which calls `update_bounds` on the worker's sampled rows.
The worker's `fragment_filter` is propagated to `maybe_sample_training_data`, which means each
worker's `bounds` are derived only from its assigned fragments. The lance test
`test_distributed_ivf_sq_worker_training_respects_fragment_filter`
(`rust/lance/src/index/vector/ivf/v2.rs`) asserts exactly this: `sq_meta.bounds.end < FRAGMENT_OFFSETS[1]`
for the worker that only saw fragment 0. Different workers, different bounds — by design and by test.

## Proposal

Add a public, driver-side SQ trainer + serialization, mirroring the IVF/PQ pattern.

### Rust

1. Extend `SQBuildParams`:

   ```rust
   #[derive(Default, Clone, Debug)]
   pub struct SQBuildParams {
       pub num_bits: u8,
       pub sample_rate: u32,
       pub bounds: Option<Range<f64>>,   // <-- new
   }
   ```

   When `bounds.is_some()`, `ScalarQuantizer::build` skips `update_bounds` and uses the supplied
   range verbatim.

2. Add a public training entry point analogous to `train_ivf_centroids` / `train_pq_codebook`:

   ```rust
   pub async fn train_sq_quantizer(
       dataset: &Dataset,
       column: &str,
       params: &SQBuildParams,
       distance_type: DistanceType,
   ) -> Result<Range<f64>>;
   ```

   Returns the per-dimension global `(min, max)` collapsed into a single `Range<f64>` (the same
   shape `ScalarQuantizationMetadata.bounds` already uses).

3. Add validation to `merge_partial_vector_auxiliary_files`'s SQ branch — at minimum, fail-fast when
   `bounds` differs across shards beyond a small tolerance, mirroring the PQ codebook check. This
   prevents silent corruption in older builders that pre-date the new shared-bounds API.

### Java JNI

Mirror the existing `nativeTrainIvfCentroids` / `nativeTrainPqCodebook` pattern:

```java
public static double[] nativeTrainSqQuantizer(
    long datasetHandle, String column, SQBuildParams params, String distanceType);

// SQBuildParams:
public SQBuildParams setBounds(double lower, double upper);
```

The two-element `double[]` matches `ScalarQuantizationMetadata.bounds`.

### Python (pyo3)

Mirror `prepare_global_ivf_pq`:

```python
def prepare_global_ivf_sq(
    dataset: lance.Dataset, column: str, params: SQBuildParams, distance_type: str
) -> tuple[float, float]: ...
```

Update `SupportedDistributedIndices` to wire IVF_SQ / IVF_HNSW_SQ through the new API.

### Reader API (companion to RFC #7319)

Once `bounds` is settable, also add a public reader so downstream tools can audit a committed index:

```rust
pub async fn read_sq_metadata(&self, index_name: &str) -> Result<ScalarQuantizationMetadata>;
```

Currently RFC #7319 returns `Error::NotSupported` for IVF_SQ / IVF_HNSW_SQ — this would close that
gap.

## Migration & Compatibility

- The `bounds` field on `SQBuildParams` is `Option`; existing callers continue to work.
- The merger's bounds-mismatch check should be a hard error in new code paths and a `WARN` log for
  reads of pre-existing indexes that may have been built with the old (silently broken) flow.
- No on-disk format changes: `ScalarQuantizationMetadata.bounds` already stores what we need.

## Downstream Use

`lance-format/lance-spark` (PR #605) currently force-clamps `IVF_SQ` / `IVF_HNSW_SQ` to a single
segment as a correctness workaround. Once the API above lands, lance-spark will:

1. In phase 1, call `VectorTrainer.trainSqQuantizer` and broadcast the resulting `(min, max)`.
2. In phase 2, each executor sets `SQBuildParams.setBounds(min, max)` before
   `dataset.createIndex(IndexOptions(...).withFragmentIds(batch))`.
3. Phase 3 commit is unchanged.

This restores parallel SQ index builds without correctness risk.

## Out of Scope

- Re-quantizing already-corrupt SQ indexes built by older lance-spark versions. Those will need to
  be rebuilt from source data once the new API is available.
- Per-IVF-partition `bounds` (one `bounds` per centroid). Plausible future work, but a single global
  `bounds` is sufficient to fix the immediate correctness issue and matches the existing on-disk
  format.

## Acknowledgements

This proposal depends on the public reader API discussed in
[lance-format/lance issue #7319](https://github.com/lance-format/lance/issues/7319) (read paths for
IVF centroids and PQ codebook), and is implemented downstream as part of
[lance-format/lance-spark PR #605](https://github.com/lance-format/lance-spark/pull/605).
