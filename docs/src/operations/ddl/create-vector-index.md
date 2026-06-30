# CREATE VECTOR INDEX

Creates a vector index on a Lance table to accelerate vector similarity search.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! note "Vector column type"
    Vector index methods (`IVF_*`) require a `FixedSizeList<Float32>` column. The column dimension is read from the Arrow schema on the driver before any work is dispatched.

## Overview

Vector indexes use the `ALTER TABLE ... CREATE INDEX` syntax with an `IVF_*` index method. Lance Spark trains vector artifacts on the driver, builds index segments in parallel on executors, and commits the finished segment set atomically.

For scalar filtering and full-text search indexes, see [CREATE SCALAR INDEX](create-scalar-index.md).

## Basic Usage

The command uses the `ALTER TABLE` syntax to add an index.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX vec_idx USING ivf_flat (embedding);
    ```

## Index Methods

The following vector index methods are supported:

| Method          | Description |
|-----------------|-------------|
| `ivf_flat`      | IVF partitioning, no quantization. Highest recall, largest index size. |
| `ivf_pq`        | IVF + Product Quantization. Strong compression with tunable recall via `nprobes` / `refine_factor`. |
| `ivf_sq`        | IVF + Scalar Quantization. Lower memory than `IVF_FLAT` with simpler tuning than PQ. |
| `ivf_hnsw_pq`   | IVF + HNSW graph + PQ. Lower-latency search than flat IVF on large datasets. |
| `ivf_hnsw_sq`   | IVF + HNSW graph + SQ. HNSW search latency without a PQ codebook. |

## Options

Vector index methods (`IVF_FLAT`, `IVF_PQ`, `IVF_SQ`, `IVF_HNSW_PQ`, `IVF_HNSW_SQ`) share a set of common IVF training options. Methods that include PQ, SQ, or HNSW components accept additional method-specific options.

Spark CREATE INDEX currently supports `FixedSizeList<Float32>` vector columns for distributed vector index creation. `FixedSizeList<Double>` and Hamming/UInt8 vector indexing are rejected early until the corresponding Lance Java training paths are wired through.

The numeric defaults below are Spark SQL defaults pinned for reproducible CREATE INDEX behavior rather than implicit lance-core builder defaults.

### Common Options

Available for every `IVF_*` method:

| Option           | Type    | Default                                       | Description |
|------------------|---------|-----------------------------------------------|-------------|
| `num_partitions` | Integer | `max(1, round(sqrt(num_rows)))`               | Number of IVF cells (centroids) trained on the driver. Must be `<= num_rows`. |
| `distance_type`  | String  | `l2`                                          | Distance metric used for both training and search. Accepts `l2` (alias `euclidean`), `cosine`, `dot`. Case-insensitive. |
| `sample_rate`    | Integer | `256`                                         | Number of training samples per centroid (and per PQ sub-vector during codebook training). Must be `>= 2`. |
| `max_iters`      | Integer | `50`                                          | Maximum k-means iterations during IVF centroid (and PQ codebook) training. Must be positive. |
| `num_segments`   | Integer | `min(fragment_count, spark.default.parallelism)` | Target number of executor-parallel segments. Each segment covers a batch of fragments. Must be positive; values larger than the fragment count are clamped down with a warning. |

### IVF_PQ / IVF_HNSW_PQ Options

Available only for the two PQ-quantized variants (in addition to common options):

| Option            | Type    | Default                               | Description |
|-------------------|---------|---------------------------------------|-------------|
| `num_sub_vectors` | Integer | `dim / 16`, falling back to `dim / 8` | Number of PQ sub-vectors. Must divide the vector dimension. If the vector dimension is divisible by neither 16 nor 8, this option is required. |
| `num_bits`        | Integer | `8`                                   | Number of bits per PQ code. Lance currently supports `4` or `8`; when set to `4`, `num_sub_vectors` must be even. |

### IVF_SQ / IVF_HNSW_SQ Options

Available only for the two SQ-quantized variants (in addition to common options):

| Option     | Type    | Default | Description |
|------------|---------|---------|-------------|
| `num_bits` | Integer | `8`     | Number of bits per scalar-quantized component. Spark CREATE INDEX currently supports Lance's 8-bit SQ path only, so this must be `8`. |

### IVF_HNSW_PQ / IVF_HNSW_SQ Options

Available only for the two HNSW variants (in addition to common options and the matching PQ or SQ options):

| Option              | Type    | Default | Description |
|---------------------|---------|---------|-------------|
| `m`                 | Integer | `20`    | Maximum number of neighbours per HNSW graph node. Must be positive. |
| `ef_construction`   | Integer | `150`   | Size of the dynamic candidate list during HNSW graph construction. Must be positive. |
| `max_level`         | Integer | `7`     | Maximum HNSW level. Must be in `[1, 65535]`. |
| `prefetch_distance` | Integer | `2`     | Prefetch distance hint for graph traversal. Must be `>= 0`. |

## Examples

### IVF_FLAT

Create a flat IVF index. All defaults are inferred from the dataset; in particular, `num_partitions` defaults to `round(sqrt(num_rows))`.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX vec_idx USING ivf_flat (embedding);
    ```

### IVF_PQ

Create an IVF + Product Quantization index. `num_sub_vectors` is inferred from the vector dimension, but you can override it; `num_partitions` defaults to `round(sqrt(num_rows))` and can be set explicitly when you want a specific number of IVF cells.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX vec_pq_idx USING ivf_pq (embedding) WITH (
        num_partitions = 64,
        num_sub_vectors = 8,
        num_bits = 8,
        distance_type = 'cosine'
    );
    ```

### IVF_SQ

Create an IVF + Scalar Quantization index for lower memory usage than `IVF_FLAT` without a PQ codebook:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX vec_sq_idx USING ivf_sq (embedding) WITH (
        num_partitions = 64,
        num_bits = 8
    );
    ```

### IVF_HNSW_PQ

Create an IVF + HNSW graph + PQ index for fast approximate nearest-neighbour search on large datasets:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX vec_hnsw_pq USING ivf_hnsw_pq (embedding) WITH (
        num_partitions = 128,
        num_sub_vectors = 8,
        m = 20,
        ef_construction = 150,
        max_level = 7
    );
    ```

### IVF_HNSW_SQ

Create an IVF + HNSW graph + SQ index when you want HNSW search latency without training a PQ codebook:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX vec_hnsw_sq USING ivf_hnsw_sq (embedding) WITH (
        num_partitions = 128,
        num_bits = 8,
        m = 20,
        ef_construction = 150
    );
    ```

### Controlling Executor Parallelism

`num_segments` caps the number of executor-parallel segments produced. Values larger than the fragment count are clamped down with a log warning:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX vec_idx USING ivf_flat (embedding) WITH (
        num_partitions = 64,
        num_segments = 8
    );
    ```

`IVF_SQ` and `IVF_HNSW_SQ` are an exception: they are forced to a single segment regardless of the
requested `num_segments`. lance-core does not currently expose a driver-side Scalar Quantizer trainer,
and `commit_existing_index_segments` does not reconcile per-segment SQ bounds across shards, so a
multi-segment SQ build would silently corrupt query distance computation. A larger `num_segments`
on `IVF_SQ` / `IVF_HNSW_SQ` is accepted but logged as a `WARN` and downgraded to `1`.

Query the indexed vector column with the [VECTOR_SEARCH](../dql/vector-search.md) table function.

## Output

The `CREATE INDEX` command returns the following information about the operation:

| Column              | Type   | Description                            |
|---------------------|--------|----------------------------------------|
| `fragments_indexed` | Long   | The number of fragments that were indexed. |
| `index_name`        | String | The name of the created index.         |

## When to Use a Vector Index

Consider creating a vector index when:

- You run vector similarity search via [`VECTOR_SEARCH`](../dql/vector-search.md) on a large embedding column.
- You need a lower-memory approximate nearest-neighbour index; use `IVF_PQ` for memory-bound recall.
- You need lower-latency approximate nearest-neighbour search; use `IVF_HNSW_PQ` or `IVF_HNSW_SQ`.
- You want high recall and can afford the index size; use `IVF_FLAT`.

## How It Works

The vector `CREATE INDEX` command uses a three-phase pipeline:

1. **Driver training**: Lance Spark opens the dataset, trains IVF centroids once with `VectorTrainer.trainIvfCentroids`, and, for the two PQ variants, trains a single global PQ codebook with `VectorTrainer.trainPqCodebook`. Both arrays are broadcast to executors.
2. **Executor-parallel segment build**: Fragments are batched according to `num_segments`; each Spark task quantizes its batch and produces an uncommitted index segment via `Dataset.createIndex(...)`.
3. **Atomic commit**: The driver collects all segments and commits them in a single transaction via `Dataset.commitExistingIndexSegments`. If an index of the same name already exists, the new segments replace the previous ones in the same transaction. Broadcasts are destroyed whether the build succeeds or fails.

## Notes and Limitations

- **Index Methods**: `IVF_FLAT`, `IVF_PQ`, `IVF_SQ`, `IVF_HNSW_PQ`, and `IVF_HNSW_SQ` are supported for vector index creation.
- **Column Type**: Vector index methods currently support `FixedSizeList<Float32>` vector columns for distributed vector index creation.
- **Vector Column Count**: Vector index methods accept exactly one vector column per index.
- **`IVF_HNSW_FLAT`**: Spark SQL accepts generic method identifiers, but `IVF_HNSW_FLAT` creation is rejected explicitly because `lance-core` requires a PQ or SQ quantizer for HNSW. Use `IVF_HNSW_PQ` or `IVF_HNSW_SQ` instead.
- **PQ Sub-Vector Constraint**: For `IVF_PQ` and `IVF_HNSW_PQ`, `num_sub_vectors` must divide the vector dimension. If the dimension is divisible by neither 16 nor 8, you must specify `num_sub_vectors` explicitly.
- **Index Replacement**: Re-running `CREATE INDEX` with the same name replaces the existing index in the same atomic commit, so concurrent readers see either the old or the new segment set, never a mixture.
- **Empty Tables**: Creating an index on a table with no fragments returns `fragments_indexed = 0` and commits no segments.
- **Deferred Training**: `train = false` is not supported for `IVF_*` vector methods because Lance does not currently expose a vector-aware empty-index commit path.
