# CREATE INDEX

Creates a scalar or vector index on a Lance table to accelerate queries.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `CREATE INDEX` command builds an index on one or more columns of a Lance table. Indexing can improve the performance of queries that filter on the indexed columns. Depending on the index method, Lance Spark either uses a fragment-parallel build path or a driver-coordinated commit flow after parallel executor builds.

## Basic Usage

The command uses the `ALTER TABLE` syntax to add an index.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX user_id_idx USING btree (id);
    ```

## Index Methods

The following index methods are supported:

| Category | Method  | Description                                                                 |
|----------|---------|-----------------------------------------------------------------------------|
| Scalar   | `zonemap` | Lightweight min/max index for fragment pruning on a scalar column. |
| Scalar   | `btree` | B-tree index for efficient range queries and point lookups on scalar columns. |
| Scalar   | `fts`   | Full-text search (inverted) index for text search on string columns.        |
| Vector   | `ivf_flat`     | IVF partitioning, no quantization. Highest recall, largest index size. |
| Vector   | `ivf_pq`       | IVF + Product Quantization. Strong compression with tunable recall via `nprobes` / `refine_factor`. |
| Vector   | `ivf_sq`       | IVF + Scalar Quantization. Lower memory than `IVF_FLAT` with simpler tuning than PQ. |
| Vector   | `ivf_hnsw_pq`  | IVF + HNSW graph + PQ. Lower-latency search than flat IVF on large datasets. |
| Vector   | `ivf_hnsw_sq`  | IVF + HNSW graph + SQ. HNSW search latency without a PQ codebook. |

!!! note "Vector column type"
    Vector index methods (`IVF_*`) require a `FixedSizeList<Float32>` column. The column dimension is read from the Arrow schema on the driver before any work is dispatched.

## Options

The `CREATE INDEX` command supports options via the `WITH` clause to control index creation. These options are specific to the chosen index method.

### Common Options

These options apply to all index methods:

| Option  | Type    | Description                                                                                                                                                                |
|---------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `train` | Boolean | When `false`, defer index training: register an empty index covering no rows without scanning any data, to be populated later. Default `true`. See [Deferred Index Creation](#deferred-index-creation). |

### ZoneMap Options

For the `zonemap` method, the following options are supported:

| Option          | Type | Description                                  |
|-----------------|------|----------------------------------------------|
| `rows_per_zone` | Long    | The approximate number of rows per zonemap zone. |
| `num_segments`  | Integer | Target number of index segments (upper bound; clamped to fragment count when larger). Each segment covers a batch of fragments. Defaults to `min(fragment_count, spark.default.parallelism)`. |

### BTree Options

For the `btree` method, the following options are supported:

| Option           | Type   | Description                                                                                                                                                                                              |
|------------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `zone_size`      | Long   | The number of rows per zone in the B-tree index.                                                                                                                                                         |
| `build_mode`     | String | Index building mode: 'fragment' builds indexes in parallel by fragment; 'range' sorts data by indexed columns first, then partitions and builds indexes in parallel by partition. Default is 'fragment'. |
| `rows_per_range` | Long   | The number of rows per range when built using range mode. Default is 1000000.                                                                                                                            |


### FTS Options

For the `fts` method, the following options are required:

| Option             | Type    | Description                                                    |
|--------------------|---------|----------------------------------------------------------------|
| `base_tokenizer`   | String  | Tokenizer type: "simple" (whitespace/punctuation) or "ngram".  |
| `language`         | String  | Language for text processing (e.g., "English").                |
| `max_token_length` | Integer | Maximum token length (e.g., 40).                               |
| `lower_case`       | Boolean | Convert text to lowercase.                                     |
| `stem`             | Boolean | Enable stemming to reduce words to root form.                  |
| `remove_stop_words`| Boolean | Remove common stop words from index.                           |
| `ascii_folding`    | Boolean | Normalize accented characters (e.g., 'é' to 'e').              |
| `with_position`    | Boolean | Enable phrase queries. Increases index size.                   |

For advanced tokenizer configuration, refer to the [Lance FTS documentation](https://lance.org/format/table/index/scalar/fts/#tokenizers).

### FTS Format Version

Lance FTS index format v2 is selected by the Lance runtime environment variable `LANCE_FTS_FORMAT_VERSION=2`. Configure it on both the Spark driver and executors before creating the index.

=== "spark-submit"
    ```bash
    LANCE_FTS_FORMAT_VERSION=2 spark-submit \
        --conf spark.executorEnv.LANCE_FTS_FORMAT_VERSION=2 \
        ...
    ```

Spark SQL currently does not expose a per-index `fts_version` option. Use `USING fts` with the normal FTS options shown above; Spark records the index details and version returned by Lance.

### Vector Index Options

Vector index methods (`IVF_FLAT`, `IVF_PQ`, `IVF_SQ`, `IVF_HNSW_PQ`, `IVF_HNSW_SQ`) share a set of common IVF training options. Methods that include PQ, SQ, or HNSW components accept additional method-specific options.

Spark CREATE INDEX currently supports `FixedSizeList<Float32>` vector columns for distributed vector index creation. `FixedSizeList<Double>` and Hamming/UInt8 vector indexing are rejected early until the corresponding Lance Java training paths are wired through.

The numeric defaults below are Spark SQL defaults pinned for reproducible CREATE INDEX behavior rather than implicit lance-core builder defaults.

#### Common Options

Available for every `IVF_*` method:

| Option          | Type    | Default                                       | Description |
|-----------------|---------|-----------------------------------------------|-------------|
| `num_partitions`| Integer | `max(1, round(sqrt(num_rows)))`               | Number of IVF cells (centroids) trained on the driver. Must be `<= num_rows`. |
| `distance_type` | String  | `l2`                                          | Distance metric used for both training and search. Accepts `l2` (alias `euclidean`), `cosine`, `dot`. Case-insensitive. |
| `sample_rate`   | Integer | `256`                                         | Number of training samples per centroid (and per PQ sub-vector during codebook training). Must be `>= 2`. |
| `max_iters`     | Integer | `50`                                          | Maximum k-means iterations during IVF centroid (and PQ codebook) training. Must be positive. |
| `num_segments`  | Integer | `min(fragment_count, spark.default.parallelism)` | Target number of executor-parallel segments. Each segment covers a batch of fragments. Must be positive; values larger than the fragment count are clamped down with a warning. |

#### IVF_PQ / IVF_HNSW_PQ Options

Available only for the two PQ-quantized variants (in addition to common options):

| Option            | Type    | Default                                      | Description |
|-------------------|---------|----------------------------------------------|-------------|
| `num_sub_vectors` | Integer | `dim / 16`, falling back to `dim / 8`        | Number of PQ sub-vectors. Must divide the vector dimension. If the vector dimension is divisible by neither 16 nor 8, this option is required. |
| `num_bits`        | Integer | `8`                                          | Number of bits per PQ code. Lance currently supports `4` or `8`; when set to `4`, `num_sub_vectors` must be even. |

#### IVF_SQ / IVF_HNSW_SQ Options

Available only for the two SQ-quantized variants (in addition to common options):

| Option     | Type    | Default | Description |
|------------|---------|---------|-------------|
| `num_bits` | Integer | `8`     | Number of bits per scalar-quantized component. Spark CREATE INDEX currently supports Lance's 8-bit SQ path only, so this must be `8`. |

#### IVF_HNSW_PQ / IVF_HNSW_SQ Options

Available only for the two HNSW variants (in addition to common options and the matching PQ or SQ options):

| Option              | Type    | Default | Description |
|---------------------|---------|---------|-------------|
| `m`                 | Integer | `20`    | Maximum number of neighbours per HNSW graph node. Must be positive. |
| `ef_construction`   | Integer | `150`   | Size of the dynamic candidate list during HNSW graph construction. Must be positive. |
| `max_level`         | Integer | `7`     | Maximum HNSW level. Must be in `[1, 65535]`. |
| `prefetch_distance` | Integer | `2`     | Prefetch distance hint for graph traversal. Must be `>= 0`. |

## Examples

### Basic Index Creation

Create a simple B-tree index on a single column:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id USING btree (id);
    ```

### Indexing Multiple Columns

Create a composite index on multiple columns.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.logs CREATE INDEX idx_ts_level USING btree (timestamp, level);
    ```

### Lightweight Fragment Pruning

Create a zonemap index when you want lightweight min/max-based fragment pruning:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id_zonemap USING zonemap (id);
    ```

### Indexing with Options

Create an index and specify the `zone_size` for the B-tree:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id_zoned USING btree (id) WITH (zone_size = 2048);
    ```

### Zonemap with Options

Create a zonemap index and specify the approximate number of rows per zone:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id_zonemap USING zonemap (id) WITH (rows_per_zone = 2048);
    ```

### Full-Text Search Index

Create an FTS index on a text column:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.documents CREATE INDEX doc_fts USING fts (content) WITH (
        base_tokenizer = 'simple',
        language = 'English',
        max_token_length = 40,
        lower_case = true,
        stem = false,
        remove_stop_words = false,
        ascii_folding = false,
        with_position = true
    );
    ```

Query the indexed column with the [SEARCH](../dql/search.md) table function.

### Vector Index — IVF_FLAT

Create a flat IVF index. All defaults are inferred from the dataset; in particular, `num_partitions` defaults to `round(sqrt(num_rows))`.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX vec_idx USING ivf_flat (embedding);
    ```

### Vector Index — IVF_PQ

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

### Vector Index — IVF_SQ

Create an IVF + Scalar Quantization index for lower memory usage than `IVF_FLAT` without a PQ codebook:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX vec_sq_idx USING ivf_sq (embedding) WITH (
        num_partitions = 64,
        num_bits = 8
    );
    ```

### Vector Index — IVF_HNSW_PQ

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

### Vector Index — IVF_HNSW_SQ

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

`num_segments` caps the number of executor-parallel segments produced. It applies to `zonemap` and every `IVF_*` method. Values larger than the fragment count are clamped down with a log warning:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX vec_idx USING ivf_flat (embedding) WITH (
        num_partitions = 64,
        num_segments = 8
    );
    ```

Query the indexed vector column with the [VECTOR_SEARCH](../dql/vector-search.md) table function.

### Deferred Index Creation

Register an index without scanning any data by setting `train = false`. This commits an empty index
(covering no rows) instantly on the driver — useful when you want the index to exist immediately and
fill it in later, or when you intend to build it incrementally:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id USING zonemap (id) WITH (train = false);
    ```

A deferred index returns `fragments_indexed = 0` and is treated as fully unindexed: queries fall back
to scanning the data until it is populated. There are two ways to populate it:

- **Full distributed build (recommended):** re-run `CREATE INDEX` with the same name. This uses the
  normal distributed build across Spark tasks and atomically replaces the empty index:

    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id USING zonemap (id);
    ```

- **Incremental build through the SDK:** when only some fragments are unindexed (for example after
  appending data to an already-built index), `Dataset.optimizeIndices` indexes just the unindexed
  fragments. This currently runs on a single node:

    ```java
    dataset.optimizeIndices(OptimizeOptions.builder().build());
    ```

`train = false` is supported for all index methods (`btree`, `fts`, `zonemap`, and `IVF_*` vector
methods). Because a deferred index performs no segmented build at creation time, `num_segments`
cannot be combined with `train = false` — pass it on the eager build that populates the index
instead.

## Output

The `CREATE INDEX` command returns the following information about the operation:

| Column              | Type   | Description                            |
|---------------------|--------|----------------------------------------|
| `fragments_indexed` | Long   | The number of fragments that were indexed. |
| `index_name`        | String | The name of the created index.         |

## When to Use an Index

Consider creating an index when:

- You frequently filter a large table on a specific column.
- You want lightweight fragment pruning based on per-zone min/max statistics.
- Your queries involve point lookups or small range scans.
- You run vector similarity search via [`VECTOR_SEARCH`](../dql/vector-search.md) on a large embedding column. Use `IVF_PQ` for memory-bound recall, `IVF_HNSW_PQ` / `IVF_HNSW_SQ` for low-latency search, and `IVF_FLAT` when recall is the priority and memory is not a concern.

## How It Works

The `CREATE INDEX` command operates as follows:

1.  **Index Build Execution**: Lance Spark chooses an execution path based on the index method.
    - `btree` (fragment mode), `fts`: build per-fragment index segments in parallel and merge their metadata.
    - `btree` in range mode: globally sort the data on the indexed columns via Spark repartitioning, then build per-partition segments.
    - `zonemap`: build per-fragment-batch segments in parallel and publish them as one logical index.
    - `IVF_FLAT`, `IVF_PQ`, `IVF_SQ`, `IVF_HNSW_PQ`, `IVF_HNSW_SQ`: a three-phase pipeline:
        1. **Driver training.** Open the dataset, train IVF centroids once with `VectorTrainer.trainIvfCentroids`, and (for the two PQ variants) train a single global PQ codebook with `VectorTrainer.trainPqCodebook`. Both arrays are broadcast to executors.
        2. **Executor-parallel segment build.** Fragments are batched (sized by `num_segments`); each Spark task quantizes its batch and produces an uncommitted index segment via `Dataset.createIndex(...)`.
        3. **Atomic commit.** The driver collects all segments and commits them in a single transaction via `Dataset.commitExistingIndexSegments`. If an index of the same name already exists, the new segments replace the previous ones in the same transaction. Broadcasts are destroyed whether the build succeeds or fails.
2.  **Metadata Finalization**: Lance Spark merges or commits the resulting index metadata on the driver so the new logical index becomes visible atomically.
3.  **Transactional Commit**: A new table version is committed with the new index information. The operation is atomic and ensures that concurrent reads are not affected.

## Notes and Limitations

- **Index Methods**: `zonemap`, `btree`, and `fts` are supported for scalar index creation. `IVF_FLAT`, `IVF_PQ`, `IVF_SQ`, `IVF_HNSW_PQ`, and `IVF_HNSW_SQ` are supported for vector index creation.
- **Zonemap Column Count**: Zonemap indexes currently support a single column only. The generic `CREATE INDEX` grammar accepts a column list, but Lance rejects multi-column zonemap creation.
- **Vector Column Count**: Vector index methods accept exactly one `FixedSizeList<Float32>` column per index.
- **`IVF_HNSW_FLAT`**: Spark SQL accepts generic method identifiers, but `IVF_HNSW_FLAT` creation is rejected explicitly because `lance-core` requires a PQ or SQ quantizer for HNSW. Use `IVF_HNSW_PQ` or `IVF_HNSW_SQ` instead.
- **PQ Sub-Vector Constraint**: For `IVF_PQ` and `IVF_HNSW_PQ`, `num_sub_vectors` must divide the vector dimension. If the dimension is divisible by neither 16 nor 8, you must specify `num_sub_vectors` explicitly.
- **Index Replacement**: Re-running `CREATE INDEX` with the same name replaces the existing index. For vector indexes the replacement happens in the same atomic commit, so concurrent readers see either the old or the new segment set, never a mixture.
- **Empty Tables**: Creating an index on a table with no fragments returns `fragments_indexed = 0` and commits no segments.
- **Deferred Training**: With `train = false` the index is registered empty and is populated later, either by re-running `CREATE INDEX` (a full distributed build that replaces the empty index) or, for incremental coverage of newly appended fragments, by `Dataset.optimizeIndices` in the SDK. The SQL `OPTIMIZE` command compacts fragments and does not train deferred indexes.
