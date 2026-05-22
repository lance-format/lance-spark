# CREATE INDEX

Creates a scalar index on a Lance table to accelerate queries.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `CREATE INDEX` command builds an index on one or more columns of a Lance table. Indexing can improve the performance of queries that filter on the indexed columns. This operation is performed in a distributed manner, building indexes for each data fragment in parallel.

## Basic Usage

The command uses the `ALTER TABLE` syntax to add an index.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX user_id_idx USING btree (id);
    ```

## Index Methods

The following index methods are supported:

| Method    | Description                                                                 |
|-----------|-----------------------------------------------------------------------------|
| `btree`   | B-tree index for efficient range queries and point lookups on scalar columns. |
| `fts`     | Full-text search (inverted) index for text search on string columns.        |
| `zonemap` | Lightweight min/max statistics per zone for fragment pruning on a scalar column. |

## Options

The `CREATE INDEX` command supports options via the `WITH` clause to control index creation. These options are specific to the chosen index method.

### BTree Options

For the `btree` method, the following options are supported:

| Option           | Type   | Description                                                                                                                                                                                              |
|------------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `zone_size`      | Long   | The number of rows per zone in the B-tree index.                                                                                                                                                         |
| `build_mode`     | String | Index building mode: 'fragment' builds indexes in parallel by fragment; 'range' sorts data by indexed columns first, then partitions and builds indexes in parallel by partition. Default is 'fragment'. |
| `rows_per_range` | Long   | The number of rows per range when built using range mode. Default is 1000000.                                                                                                                            |


### ZoneMap Options

For the `zonemap` method, the following option is supported:

| Option          | Type | Description                                       |
|-----------------|------|---------------------------------------------------|
| `rows_per_zone` | Long | Approximate number of rows per zone. Default 8192. |

ZoneMap is single-column only — passing more than one column raises an `IllegalArgumentException` at plan time.

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

### Indexing with Options

Create an index and specify the `zone_size` for the B-tree:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id_zoned USING btree (id) WITH (zone_size = 2048);
    ```

### ZoneMap Index

Create a zonemap index for lightweight fragment pruning on a scalar column:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.events CREATE INDEX idx_ts_zm USING zonemap (event_ts);
    ```

With a custom zone size:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.events CREATE INDEX idx_ts_zm USING zonemap (event_ts) WITH (rows_per_zone = 2048);
    ```

The build path is configurable via SparkConf:

| SparkConf key                              | Default | Effect                                                                                                                                                       |
|--------------------------------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `spark.lance.zonemap.consolidate.enabled`  | `false` | When `false`, each fragment is indexed by its own Spark task and committed as a separate IndexMetadata segment under a shared name. When `true`, workers compute per-fragment zone batches in memory and the driver merges them into one consolidated segment covering every fragment. |

The default produces a manifest entry per fragment (one IndexMetadata per `<uuid>/zonemap.lance`). The opt-in consolidated mode produces a single entry covering every fragment — fewer manifest entries and lower Lance file-header overhead at the cost of routing per-fragment batches through the driver.

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

## Output

The `CREATE INDEX` command returns the following information about the operation:

| Column              | Type   | Description                            |
|---------------------|--------|----------------------------------------|
| `fragments_indexed` | Long   | The number of fragments that were indexed. |
| `index_name`        | String | The name of the created index.         |

## When to Use an Index

Consider creating an index when:

- You frequently filter a large table on a specific column.
- Your queries involve point lookups or small range scans.

## How It Works

The `CREATE INDEX` command operates as follows:

1.  **Distributed Index Building**: For each fragment in the Lance dataset, a separate task is launched to build an index on the specified column(s). For `zonemap` with `spark.lance.zonemap.consolidate.enabled=true`, the per-task work returns an Arrow batch to the driver instead of writing a file.
2.  **Metadata Merging / Commit**: For `btree` and `fts`, per-fragment indexes are collected and merged before commit. For `zonemap` default, each fragment's segment is committed as its own IndexMetadata entry under the shared name. For `zonemap` consolidated, the driver writes one merged segment and commits a single IndexMetadata entry.
3.  **Transactional Commit**: A new table version is committed with the new index information. The operation is atomic and ensures concurrent reads are not affected.

## Notes and Limitations

- **Index Methods**: `btree`, `fts`, and `zonemap` are supported.
- **ZoneMap is single-column**: Passing more than one column raises `IllegalArgumentException`.
- **Index Replacement**: If you create an index with the same name as an existing one, the old index is replaced by the new one. For `zonemap` default, the replacement removes every previous segment under that name in the same transaction.
