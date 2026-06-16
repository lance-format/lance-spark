# CREATE INDEX

Creates a scalar index on a Lance table to accelerate queries.

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

| Method  | Description                                                                 |
|---------|-----------------------------------------------------------------------------|
| `zonemap` | Lightweight min/max index for fragment pruning on a scalar column. |
| `btree` | B-tree index for efficient range queries and point lookups on scalar columns. |
| `fts`   | Full-text search (inverted) index for text search on string columns.        |

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

To query the index once it's built, see [Full-Text Search](../dql/fts.md).

### FTS Format Version

Lance FTS index format v2 is selected by the Lance runtime environment variable `LANCE_FTS_FORMAT_VERSION=2`. Configure it on both the Spark driver and executors before creating the index.

=== "spark-submit"
    ```bash
    LANCE_FTS_FORMAT_VERSION=2 spark-submit \
        --conf spark.executorEnv.LANCE_FTS_FORMAT_VERSION=2 \
        ...
    ```

Spark SQL currently does not expose a per-index `fts_version` option. Use `USING fts` with the normal FTS options shown above; Spark records the index details and version returned by Lance.

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

`train = false` is supported for all index methods (`btree`, `fts`, `zonemap`). Because a deferred
index performs no segmented build at creation time, `num_segments` cannot be combined with
`train = false` — pass it on the eager build that populates the index instead.

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

## How It Works

The `CREATE INDEX` command operates as follows:

1.  **Index Build Execution**: Lance Spark chooses an execution path based on the index method. Methods such as `btree`, `fts`, and `zonemap` can build physical index segments in parallel across fragments. `zonemap` publishes those segments directly as one logical index. Range-mode `btree` uses Spark repartitioning and sorted preprocessed data.
2.  **Metadata Finalization**: Lance Spark merges or commits the resulting index metadata on the driver so the new logical index becomes visible atomically.
3.  **Transactional Commit**: A new table version is committed with the new index information. The operation is atomic and ensures that concurrent reads are not affected.

## Notes and Limitations

- **Index Methods**: The `zonemap`, `btree`, and `fts` methods are supported for scalar index creation.
- **Zonemap Column Count**: Zonemap indexes currently support a single column only. The generic `CREATE INDEX` grammar accepts a column list, but Lance rejects multi-column zonemap creation.
- **Index Replacement**: If you create an index with the same name as an existing one, the old index will be replaced by the new one.
- **Deferred Training**: With `train = false` the index is registered empty and is populated later, either by re-running `CREATE INDEX` (a full distributed build that replaces the empty index) or, for incremental coverage of newly appended fragments, by `Dataset.optimizeIndices` in the SDK. The SQL `OPTIMIZE` command compacts fragments and does not train deferred indexes.
