# CREATE INDEX

Creates a scalar or vector index on a Lance table to accelerate queries.

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
| `ivf_flat`| IVF (Inverted File) index without quantization for vector similarity search. |
| `ivf_sq`  | IVF index with scalar quantization for memory-efficient vector search.       |
| `ivf_pq`  | IVF index with product quantization for large-scale vector search.           |

## Options

The `CREATE INDEX` command supports options via the `WITH` clause to control index creation. These options are specific to the chosen index method.

### BTree Options

For the `btree` method, the following options are supported:

| Option      | Type | Description                                  |
|-------------|------|----------------------------------------------|
| `zone_size` | Long | The number of rows per zone in the B-tree index. |

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

### Vector Index Options

All vector index methods support the following options:

| Option          | Type    | Description                                                                 | Default |
|-----------------|---------|-----------------------------------------------------------------------------|---------|
| `numPartitions` | Integer | Number of partitions (clusters) for IVF. Higher values improve recall but increase search time. | 32      |
| `maxIters`      | Integer | Maximum iterations for k-means clustering during centroid training.         | 50      |
| `distanceType`  | String  | Distance metric: "L2", "cosine", or "dot".                                 | "L2"    |

#### Additional Options for `ivf_sq`

| Option      | Type    | Description                                      | Default |
|-------------|---------|--------------------------------------------------|---------|
| `numBits`   | Integer | Number of bits per scalar quantized value (4-8). | 8       |
| `sampleRate`| Integer | Sampling rate for training quantization.         | 256     |

#### Additional Options for `ivf_pq`

| Option          | Type    | Description                                      | Default |
|-----------------|---------|--------------------------------------------------|---------|
| `numSubVectors` | Integer | Number of sub-vectors for product quantization.  | 16      |
| `numBits`       | Integer | Number of bits per sub-vector (typically 8).     | 8       |
| `sampleRate`    | Integer | Sampling rate for training codebook.             | 256     |

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

### IVF-FLAT Index

Create an IVF-FLAT index on a vector column:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.embeddings CREATE INDEX my_ivf_flat_index USING ivf_flat (embedding) WITH (
        numPartitions = 128,
        maxIters = 50,
        distanceType = 'cosine'
    );
    ```

### IVF-SQ Index

Create an IVF-SQ index with scalar quantization:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.embeddings CREATE INDEX my_ivf_sq_index USING ivf_sq (embedding) WITH (
        numPartitions = 256,
        numBits = 8,
        sampleRate = 256,
        maxIters = 50,
        distanceType = 'L2'
    );
    ```

### IVF-PQ Index

Create an IVF-PQ index with product quantization for maximum memory efficiency:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.embeddings CREATE INDEX my_ivf_pq_index USING ivf_pq (embedding) WITH (
        numPartitions = 512,
        numSubVectors = 16,
        numBits = 8,
        sampleRate = 256,
        maxIters = 50,
        distanceType = 'cosine'
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
- You need to perform vector similarity search on high-dimensional data.

## How It Works

The `CREATE INDEX` command operates as follows:

1.  **Distributed Index Building**: For each fragment in the Lance dataset, a separate task is launched to build an index on the specified column(s).
2.  **Metadata Merging**: Once all per-fragment indexes are built, their metadata is collected and merged.
3.  **Transactional Commit**: A new table version is committed with the new index information. The operation is atomic and ensures that concurrent reads are not affected.

## Notes and Limitations

- **Index Methods**: The `btree` and `fts` methods are supported for scalar index creation. The `ivf_flat`, `ivf_sq`, and `ivf_pq` methods are supported for vector index creation.
- **Index Replacement**: If you create an index with the same name as an existing one, the old index will be replaced by the new one. This is because the underlying implementation uses `replace(true)`.
