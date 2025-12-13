# CREATE TABLE

Create new Lance tables with SQL DDL statements.

## Basic Table Creation

Create a simple table:

```sql
CREATE TABLE users (
    id BIGINT NOT NULL,
    name STRING,
    email STRING,
    created_at TIMESTAMP
);
```

Create table with complex data types:

```sql
CREATE TABLE events (
    event_id BIGINT NOT NULL,
    user_id BIGINT,
    event_type STRING,
    tags ARRAY<STRING>,
    metadata STRUCT<
        source: STRING,
        version: INT,
        processed_at: TIMESTAMP
    >,
    occurred_at TIMESTAMP
);
```

## Vector Columns

Lance supports vector (embedding) columns for AI workloads. These columns are stored internally as Arrow `FixedSizeList[n]` where `n` is the vector dimension. Since Spark SQL doesn't have a native fixed-size array type, you must use `ARRAY<FLOAT>` or `ARRAY<DOUBLE>` with table properties to specify the fixed dimension. The Lance-Spark connector will automatically convert these to the appropriate Arrow FixedSizeList format during write operations.

### Supported Types

- **Element Types**: `FLOAT` (float32), `DOUBLE` (float64)
- **Requirements**:
    - Vectors must be non-nullable
    - All vectors in a column must have the same dimension
    - Dimension is specified via table properties

### Creating Vector Columns

To create a table with vector columns, use the table property pattern `<column_name>.arrow.fixed-size-list.size` with the dimension as the value:

```sql
CREATE TABLE embeddings_table (
    id INT NOT NULL,
    text STRING,
    embeddings ARRAY<FLOAT> NOT NULL
) USING lance
TBLPROPERTIES (
    'embeddings.arrow.fixed-size-list.size' = '128'
);
```

Create table with multiple vector columns of different dimensions:

```sql
CREATE TABLE multi_vector_table (
    id INT NOT NULL,
    title STRING,
    text_embeddings ARRAY<FLOAT> NOT NULL,
    image_embeddings ARRAY<DOUBLE> NOT NULL
) USING lance
TBLPROPERTIES (
    'text_embeddings.arrow.fixed-size-list.size' = '384',
    'image_embeddings.arrow.fixed-size-list.size' = '512'
);
```

### Working with Vector Tables

Once created, you can insert data using SQL:

```sql
-- Insert vector data (example with small vectors for clarity)
INSERT INTO embeddings_table VALUES
    (1, 'first text', array(0.1, 0.2, 0.3, ...)), -- 128 float values
    (2, 'second text', array(0.4, 0.5, 0.6, ...));
```

Query vector tables:

```sql
-- Select vectors
SELECT id, text FROM embeddings_table WHERE id = 1;

-- Count rows
SELECT COUNT(*) FROM embeddings_table;
```
Note: When reading vector columns back, they are automatically converted to Spark's `ARRAY<FLOAT>` or `ARRAY<DOUBLE>` types for compatibility with Spark operations.

### Vector Indexing

After creating and populating vector columns, you can create vector indexes using Lance Python API for similarity search:

```python
import lance

# Open the dataset
ds = lance.dataset("/path/to/embeddings_table.lance")

# Create a vector index on the embeddings column
ds.create_index(
    "embeddings",
    index_type="IVF_PQ",
)

# Perform similarity search
import numpy as np
query_vector = np.random.rand(128).astype(np.float32)
results = ds.to_table(
    nearest={"column": "embeddings", "q": query_vector, "k": 10}
).to_pandas()
```

## Blob Columns

Lance supports blob encoding for large binary data. Blob columns store large binary values (typically > 64KB) out-of-line in a separate blob file, which improves query performance when not accessing the blob data directly.

### Supported Types

- **Column Type**: `BINARY`
- **Requirements**:
    - Column must be nullable (blob data is not materialized when read, so nullability is required)
    - Blob encoding is specified via table properties

### Creating Blob Columns

To create a table with blob columns, use the table property pattern `<column_name>.lance.encoding` with the value `'blob'`:

```sql
CREATE TABLE documents (\n    id INT NOT NULL,\n    title STRING,\n    content BINARY\n) USING lance\nTBLPROPERTIES (\n    'content.lance.encoding' = 'blob'\n);
```

Create table with multiple blob columns:

```sql
CREATE TABLE media_files (\n    id INT NOT NULL,\n    filename STRING,\n    image_data BINARY,\n    video_data BINARY\n) USING lance\nTBLPROPERTIES (\n    'image_data.lance.encoding' = 'blob',\n    'video_data.lance.encoding' = 'blob'\n);
```

### Working with Blob Tables

Insert blob data using SQL or DataFrames:

```sql
-- Insert blob data (example with binary literals)
INSERT INTO documents VALUES\n    (1, 'Document 1', X'48656C6C6F'),\n    (2, 'Document 2', X'576F726C64');
```

Query blob tables:

```sql
-- Select non-blob columns
SELECT id, title FROM documents WHERE id = 1;

-- Count rows
SELECT COUNT(*) FROM documents;
```

**Note**: When reading blob columns, they are returned as empty byte arrays since the data is not materialized by default. To access the actual blob data, you need to use the blob position and size virtual columns.

### Blob Virtual Columns

When you have a blob column, Lance automatically provides virtual columns that give you access to the blob's position and size in the blob file:

- `<column_name>__blob_pos` - The byte position of the blob in the blob file
- `<column_name>__blob_size` - The size of the blob in bytes

```sql
-- Select blob metadata
SELECT id, title, content__blob_pos, content__blob_size
FROM documents
WHERE id = 1;
```

These virtual columns can be used to:
- Verify that blob data was written successfully
- Implement custom blob retrieval logic
- Monitor blob storage statistics

## Large String Columns

Lance supports large string columns for storing very large text data. By default, Arrow uses `Utf8` (VarChar) type with 32-bit offsets, which limits total string data to 2GB per batch. For columns containing very large strings (e.g., document content, base64-encoded data), you can use `LargeUtf8` (LargeVarChar) with 64-bit offsets.

### When to Use Large Strings

Use large string columns when:

- Individual string values may exceed several MB
- Total string data per batch may exceed 2GB
- You encounter `OversizedAllocationException` errors during writes

### Creating Large String Columns

To create a table with large string columns, use the table property pattern `<column_name>.arrow.large_var_char` with the value `'true'`:

```sql
CREATE TABLE articles (
    id INT NOT NULL,
    title STRING,
    content STRING
) USING lance
TBLPROPERTIES (
    'content.arrow.large_var_char' = 'true'
);
```

Create table with multiple large string columns:

```sql
CREATE TABLE documents (
    id INT NOT NULL,
    title STRING,
    body STRING,
    raw_html STRING
) USING lance
TBLPROPERTIES (
    'body.arrow.large_var_char' = 'true',
    'raw_html.arrow.large_var_char' = 'true'
);
```

### Working with Large String Tables

Insert and query large string data using standard SQL:

```sql
-- Insert data
INSERT INTO articles VALUES
    (1, 'Article 1', 'Very long article content...'),
    (2, 'Article 2', 'Another long article...');

-- Query data
SELECT id, title, length(content) as content_length
FROM articles
WHERE id = 1;
```

**Note**: When reading large string columns, they are returned as standard Spark `STRING` type. The large string encoding is transparent to read operations.