# RFC: Expose IVF Centroids and PQ Codebook as Public Reader APIs

- **Status**: Draft
- **Target repo**: `lance-format/lance` (intended to be filed as a GitHub issue / discussion)
- **Author**: external contributor (lance-spark integration)
- **Last updated**: 2026-06-17

## Summary

Add two read-only public methods on `lance::Dataset` (and matching wrappers
in the Java JNI and Python pyo3 bindings) that return, by index name:

1. The trained **IVF centroids** of any committed vector index.
2. The trained **PQ codebook** of any committed `IVF_PQ` / `IVF_HNSW_PQ` index.

Both are pure projections of state already loaded by `open_vector_index`;
no new on-disk format is introduced.

## Motivation

Today the IVF centroids and PQ codebook of a committed vector index are
**not reachable from any public API**. The only ways to obtain them are:

- Re-train via `lance::index::vector::ivf::train_ivf_centroids`
  (expensive; produces *new* centroids that don't match the committed
  index), or
- Reach into private fields of `IvfModel` / `ProductQuantizer` from
  inside the `lance` crate.

Several use cases need the **already-trained** artifacts:

1. **Incremental / distributed index extension.** A driver process wants
   to extend an existing IVF index to cover newly-arrived fragments,
   **without retraining** centroids or codebook (so all segments share
   one quantizer). The driver needs to read the existing artifacts and
   broadcast them to workers (Spark / Ray) that build new segments.
2. **Index inspection / debugging.** "What centroids does this index
   actually use? Are partitions balanced? Did training converge?" Today
   answering this requires a custom Rust tool linked against private
   API.
3. **Cross-store transfer.** Copy a vector index between datasets
   without re-running the (slow) training step.
4. **External quantizers.** Re-quantize new data with the existing PQ
   codebook before bulk-appending into a Lance dataset that has an
   `IVF_PQ` index, so the eventual index extension does not have to
   re-quantize.

The first use case — distributed *incremental* index build — is the
immediate driver. The lance-spark connector's distributed builder
(commit `a412ecc feat: distributed vector index creation`) currently
re-trains on every `CREATE INDEX`, even when the user only added new
data. A proper "extend" path needs the original centroids/codebook.

## Detailed design

### Rust core API

Add two methods to the existing public trait `DatasetIndexExt` (defined
in `rust/lance/src/index/api.rs`).

```rust
#[async_trait]
pub trait DatasetIndexExt {
    // ... existing methods ...

    /// Read the trained IVF centroids of a committed vector index by name.
    ///
    /// Returns the union of every segment's centroids, in segment commit
    /// order. Each list element is one centroid; the inner list size
    /// equals the indexed vector dimension. Element type matches the
    /// indexed column (`Float16` / `Float32` / `Float64` / `UInt8`).
    ///
    /// # Errors
    /// - [`Error::IndexNotFound`] — no logical index with this name.
    /// - [`Error::Index`] — the index is not vector-typed, has no IVF
    ///   model, or segments disagree on dimension.
    async fn read_ivf_centroids(
        &self,
        index_name: &str,
    ) -> Result<FixedSizeListArray>;

    /// Read the trained PQ codebook of a committed `IVF_PQ` /
    /// `IVF_HNSW_PQ` index.
    ///
    /// Length = `num_sub_vectors * 2.pow(num_bits)`; inner list length =
    /// `dimension / num_sub_vectors`; element type = `Float32` (PQ
    /// codebooks are always loaded as `f32` per
    /// `ProductQuantizer::from_metadata`).
    ///
    /// # Errors
    /// - [`Error::IndexNotFound`] — no index with this name.
    /// - [`Error::NotSupported`] — the index does not use product
    ///   quantization (`IVF_FLAT`, `IVF_SQ`, `IVF_HNSW_FLAT`,
    ///   `IVF_HNSW_SQ`, `IVF_RQ`).
    /// - [`Error::Index`] — segments disagree on PQ shape.
    async fn read_pq_codebook(
        &self,
        index_name: &str,
    ) -> Result<FixedSizeListArray>;
}
```

The implementation is a thin projection on top of existing private
state. Default impl in `rust/lance/src/index.rs`:

```rust
async fn read_ivf_centroids(&self, index_name: &str) -> Result<FixedSizeListArray> {
    let metadatas = self.load_indices_by_name(index_name).await?;
    if metadatas.is_empty() {
        return Err(Error::index_not_found(format!("name={index_name}")));
    }
    let column = self.schema().field_by_id(metadatas[0].fields[0])
        .ok_or_else(|| Error::index(format!(
            "Index '{index_name}': index column not in schema")))?;
    let logical = self.open_logical_vector_index(&column.name, index_name).await?;
    logical.as_ivf()?.read_centroids()
}
```

with two new inherent methods on `LogicalIvfView`
(`rust/lance/src/index/vector.rs`):

```rust
impl<'a> LogicalIvfView<'a> {
    pub fn read_centroids(&self) -> Result<FixedSizeListArray> {
        let mut parts = Vec::with_capacity(self.logical_index.num_segments());
        let mut dim: Option<i32> = None;
        for index in self.indices() {
            let arr = index.ivf_model().centroids_array()
                .ok_or_else(|| Error::index(format!(
                    "Logical index '{}': segment is missing IVF centroids",
                    self.logical_index.name())))?;
            if let Some(d) = dim {
                if d != arr.value_length() {
                    return Err(Error::index(format!(
                        "Logical index '{}': segments disagree on dimension ({} vs {})",
                        self.logical_index.name(), d, arr.value_length())));
                }
            } else {
                dim = Some(arr.value_length());
            }
            parts.push(arr.clone());
        }
        let refs: Vec<&dyn Array> = parts.iter().map(|a| a as &dyn Array).collect();
        Ok(arrow::compute::concat(&refs)?.as_fixed_size_list().clone())
    }

    pub fn read_pq_codebook(&self) -> Result<FixedSizeListArray> {
        let mut codebook: Option<FixedSizeListArray> = None;
        for index in self.indices() {
            let pq = match index.quantizer() {
                Quantizer::Product(pq) => pq,
                other => return Err(Error::NotSupported {
                    source: format!(
                        "Index '{}' uses {} quantization, not PQ",
                        self.logical_index.name(),
                        other.quantization_type()).into(),
                    location: location!(),
                }),
            };
            match codebook.as_ref() {
                None => codebook = Some(pq.codebook.clone()),
                Some(prev) => {
                    if prev.value_length() != pq.codebook.value_length()
                        || prev.len() != pq.codebook.len() {
                        return Err(Error::index(format!(
                            "Logical index '{}': segments disagree on PQ codebook shape",
                            self.logical_index.name())));
                    }
                }
            }
        }
        codebook.ok_or_else(|| Error::index(format!(
            "Logical index '{}': no segments", self.logical_index.name())))
    }
}
```

The work routes through `open_logical_vector_index` →
`open_vector_index`, hitting the existing index cache. First call
incurs:

- One `read_global_buffer(0)` per segment for IVF centroids (small).
- One `read_global_buffer(N)` per segment for PQ codebook (~`2^num_bits
  * dim * 4` bytes; e.g. `256 × 768 × 4 ≈ 768 KB` for typical text
  embeddings).

Subsequent calls are in-memory clones (`Arc` refcount bumps); no copy.

### Java JNI surface

Add to `org.lance.Dataset`:

```java
public Optional<IvfCentroids> readIvfCentroids(String indexName) {
  try (LockManager.LockGuard guard = lockManager.acquireReadLock()) {
    Preconditions.checkArgument(nativeDatasetHandle != 0, "Dataset is closed");
    return Optional.ofNullable(nativeReadIvfCentroids(indexName));
  }
}
private native IvfCentroids nativeReadIvfCentroids(String indexName);

public Optional<PqCodebook> readPqCodebook(String indexName) {
  try (LockManager.LockGuard guard = lockManager.acquireReadLock()) {
    Preconditions.checkArgument(nativeDatasetHandle != 0, "Dataset is closed");
    return Optional.ofNullable(nativeReadPqCodebook(indexName));
  }
}
private native PqCodebook nativeReadPqCodebook(String indexName);
```

with two new POJOs under `org.lance.index.vector`:

```java
public final class IvfCentroids {
  private final float[] flat;          // [numPartitions * dimension] row-major
  private final int numPartitions;
  private final int dimension;
  private final String elementType;    // FLOAT32 | FLOAT16 | FLOAT64 | UINT8
}
public final class PqCodebook {
  private final float[] flat;          // row-major
  private final int numSubVectors;
  private final int numBits;
  private final int subVectorDim;
}
```

**Why `float[]` rather than `FixedSizeListVector`?** Lance's Java jar pins
Arrow Java to a specific version (currently 18.x), but downstream consumers
(Spark 3.5 still on Arrow 15.x) cannot upgrade synchronously. Returning
Arrow Java types across this version edge yields `LinkageError` /
`NoSuchMethodError` at the BufferAllocator boundary. The existing
`VectorTrainer.trainIvfCentroids` and `trainPqCodebook` already return
`float[]` for exactly this reason — this API maintains symmetry.

**Why `Optional<>`?** Two distinct error modes: index not found →
`IllegalArgumentException` (matches `getIndexStatistics`); index exists
but is not the requested type (e.g. `readPqCodebook` on `IVF_FLAT`) →
`Optional.empty()`. Lets callers write `optional.ifPresent(...)` instead
of try-catch. Mirrors `Dataset.memWalIndexDetails(): Optional<MemWalIndexDetails>`.

### Python pyo3 surface

Add to `lance.LanceDataset`:

```python
def read_ivf_centroids(self, index_name: str) -> pa.FixedSizeListArray:
    """Read the trained IVF centroids of a vector index.

    Length = num_partitions; inner list size = vector dimension; element
    type matches the indexed column (typically ``float32``).

    Raises
    ------
    ValueError
        When no index with this name exists, or when the index is not
        vector-typed.
    """
    return self._ds.read_ivf_centroids(index_name)


def read_pq_codebook(self, index_name: str) -> pa.FixedSizeListArray:
    """Read the trained PQ codebook of an IVF_PQ / IVF_HNSW_PQ index.

    Length = num_sub_vectors * 2 ** num_bits; inner list size =
    dimension / num_sub_vectors; element type = float32.

    Raises
    ------
    NotImplementedError
        When the index does not use product quantization.
    ValueError
        When no index with this name exists.
    """
    return self._ds.read_pq_codebook(index_name)
```

The pyo3 bridge in `python/src/dataset.rs`:

```rust
fn read_ivf_centroids<'py>(
    &self, py: Python<'py>, index_name: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let centroids = rt().block_on(Some(py), self.ds.read_ivf_centroids(index_name))?
        .infer_error()?;
    centroids.into_data().to_pyarrow(py)
}

fn read_pq_codebook<'py>(
    &self, py: Python<'py>, index_name: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let codebook = rt().block_on(Some(py), self.ds.read_pq_codebook(index_name))?
        .infer_error()?;
    codebook.into_data().to_pyarrow(py)
}
```

Returning `pa.FixedSizeListArray` (via `ArrayData.to_pyarrow`) follows
the existing `train_ivf_model` / `train_pq_model` precedent in
`python/src/indices.rs`. Error mapping is the standard
`PythonErrorExt::infer_error()` — `IndexNotFound → PyValueError`,
`NotSupported → PyNotImplementedError`. No new error mapping needed.

## Compatibility

The new APIs are pure additions: no existing signature changes; no
on-disk format changes. They work identically on:

- **Legacy v1 IVF_PQ** (`(0,0)` / `(0,1)` files, `pb::Index`-embedded
  centroids).
- **v2 transitional** (`(0,2)` files, IVF in schema metadata; used by
  `IVF_HNSW_*`).
- **v3** (`(0,3)` / `(2,_)` files, IVF in global buffer).

Because the implementation goes through `open_vector_index`, which
already encapsulates this dispatch (see `rust/lance/src/index.rs:1791-2008`),
all three are covered for free.

Segments newer than the binary's supported `index_version` are filtered
out by `retain_supported_indices` (`rust/lance/src/index.rs:1569-1592`)
before they ever reach this code path, so no version-skew handling is
needed in the new methods.

## Drawbacks

1. **Cache pressure.** First call triggers full segment open; for
   indexes whose only use case is "read centroids and discard" this is
   wasteful. Mitigation: a lighter-weight code path that opens only
   `index.idx` and skips the auxiliary file is feasible but adds
   complexity. Defer to a follow-up if profiling shows real cost.
2. **Java POJO vs Arrow Java.** Choosing `float[]` over
   `FixedSizeListVector` couples the Java surface to a layout convention
   (`[numPartitions * dim]` row-major) that future consumers must learn.
   The convention is identical to existing
   `VectorTrainer.trainIvfCentroids`, so the cost is one-time
   documentation, but a future API uplift to typed Arrow vectors would
   be a breaking change.
3. **PQ codebook segment-equality assumption.** The `read_pq_codebook`
   API assumes all segments of one logical index share the same
   codebook — true today by construction in
   `prepare_vector_segment_build` with `require_precomputed_ivf=true`
   (see `rust/lance/src/index/vector.rs:530-535`). The implementation
   defensively errors on shape mismatch but does not check value
   equality. If a future build path produces multi-codebook segments,
   the API contract needs revisiting.

## Alternatives considered

1. **Public access via `IndexMetadata.index_details` decoding.** The
   serialized `pb::VectorIndexDetails` contains some training metadata.
   Pros: zero new APIs. Cons: requires every consumer to bundle the
   protobuf schema; centroid bytes themselves are *not* in
   `index_details` for the v3 format (they live in a global buffer);
   fragile to format evolution.
2. **Python-only `lance.lance.indices.get_pq_model` mirroring the
   existing `get_ivf_model`.** Pros: smallest Python diff. Cons:
   ignores Java consumers (the immediate driver of this RFC); and
   `get_ivf_model` is itself an asymmetric one-off — promoting both to
   first-class `Dataset` methods is cleaner long-term.
3. **Typed wrapper struct** `IvfArtifact { centroids, distance_type,
   loss }` instead of bare `FixedSizeListArray`. Pros: self-describing.
   Cons: commits to a public type's shape prematurely; the same
   metadata is reachable today via `index_statistics()`. Defer to v2 if
   real demand emerges.
4. **Wait for distributed index merge support.** The motivating
   incremental-build use case could in principle be solved by extending
   `optimize_indices` to be distributable. That is a much larger change
   and does not address use cases 2-4. The reader API is independently
   useful and unblocks all four.

## Test plan

- **Rust** (`rust/lance/src/index.rs` test module):
  - IVF_FLAT round-trip: build → read centroids → assert dim, count.
  - IVF_PQ round-trip: build → read codebook → assert
    `len == num_sub_vectors * 2^num_bits`.
  - `read_pq_codebook` on `IVF_FLAT` / `IVF_SQ` → `Error::NotSupported`.
  - Unknown index name → `Error::IndexNotFound`.
  - Multi-segment: simulate distributed build with N segments → assert
    centroid concat length = sum, codebook returned once
    (consistency check).
  - Legacy v1 IVF_PQ format → still readable.
- **Java**: equivalent assertions on `IvfCentroids` / `PqCodebook`
  POJOs; `Optional.empty()` on type mismatch; `IllegalArgumentException`
  on missing.
- **Python**: pytest equivalents using `pa.FixedSizeListArray` on the
  return path and `pytest.raises(NotImplementedError)` for non-PQ
  indices.

## Unresolved questions

1. **Multi-segment IVF concatenation order.** The proposed
   `read_ivf_centroids` concatenates per-segment centroids in commit
   order. For consumers that want per-segment views, should we also
   expose `read_ivf_centroids_per_segment(name) -> Vec<(Uuid,
   FixedSizeListArray)>`? Defer until requested.
2. **PQ codebook segment-equality verification.** Should the
   implementation perform a byte-equal check across segments, log a
   warning, or stay silent (current proposal)? Trade-off: cost vs.
   safety against future build-path drift.
3. **Centroid-only reads.** If the dominant use case is "read centroids,
   never search", a leaner `IvfModel::open_at(object_store, index_dir,
   uuid)` path that skips auxiliary file open could be added. Worth
   doing now, or defer until profiling demands it?
4. **Distance type and loss exposure.** `IvfCentroids` currently does
   not include `distance_type` or training `loss`. Both are accessible
   via `index_statistics()` but having them on the artifact itself
   would be more ergonomic. Add now, or defer?

## Acknowledgements

This RFC originates from the lance-spark connector's distributed
incremental index build effort. The existing distributed build (commit
`a412ecc feat: distributed vector index creation`) laid the
centroid/codebook broadcast pattern that this API formalizes.

