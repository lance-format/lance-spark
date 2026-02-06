# SELECT

Query data from Lance tables using SQL or DataFrames.

## Basic Queries

=== "SQL"
    ```sql
    SELECT * FROM users;
    ```

=== "Python"
    ```python
    # Load table as DataFrame
    users_df = spark.table("users")

    # Use DataFrame operations
    filtered_users = users_df.filter("age > 25").select("name", "email")
    filtered_users.show()
    ```

=== "Scala"
    ```scala
    // Load table as DataFrame
    val usersDF = spark.table("users")

    // Use DataFrame operations
    val filteredUsers = usersDF.filter("age > 25").select("name", "email")
    filteredUsers.show()
    ```

=== "Java"
    ```java
    // Load table as DataFrame
    Dataset<Row> usersDF = spark.table("users");

    // Use DataFrame operations
    Dataset<Row> filteredUsers = usersDF.filter("age > 25").select("name", "email");
    filteredUsers.show();
    ```

## Select Specific Columns

=== "SQL"
    ```sql
    SELECT id, name, email FROM users;
    ```

## Query with WHERE Clause

=== "SQL"
    ```sql
    SELECT * FROM users WHERE age > 25;
    ```

## Aggregate Queries

=== "SQL"
    ```sql
    SELECT department, COUNT(*) as employee_count
    FROM users
    GROUP BY department;
    ```

## Join Queries

=== "SQL"
    ```sql
    SELECT u.name, p.title
    FROM users u
    JOIN projects p ON u.id = p.user_id;
    ```

## Querying Blob Columns

When querying tables with blob columns, the blob data itself is not materialized by default. 
Instead, you can access blob metadata through virtual columns.
For each blob column, Lance provides two virtual columns:

- `<column_name>__blob_pos` - The byte position of the blob in the blob file
- `<column_name>__blob_size` - The size of the blob in bytes

These virtual columns can be used for:

- Monitoring blob storage statistics
- Filtering rows by blob size
- Implementing custom blob retrieval logic
- Verifying successful blob writes


=== "SQL"
    ```sql
    SELECT id, title, content__blob_pos, content__blob_size
    FROM documents
    WHERE id = 1;
    ```

=== "Python"
    ```python
    # Read table with blob column
    documents_df = spark.table("documents")

    # Access blob metadata using virtual columns
    blob_metadata = documents_df.select(
        "id",
        "title",
        "content__blob_pos",
        "content__blob_size"
    )
    blob_metadata.show()

    # Filter by blob size
    large_blobs = documents_df.filter("content__blob_size > 1000000")
    large_blobs.select("id", "title", "content__blob_size").show()
    ```

=== "Scala"
    ```scala
    // Read table with blob column
    val documentsDF = spark.table("documents")

    // Access blob metadata using virtual columns
    val blobMetadata = documentsDF.select(
      "id",
      "title",
      "content__blob_pos",
      "content__blob_size"
    )
    blobMetadata.show()

    // Filter by blob size
    val largeBlobs = documentsDF.filter("content__blob_size > 1000000")
    largeBlobs.select("id", "title", "content__blob_size").show()
    ```

=== "Java"
    ```java
    // Read table with blob column
    Dataset<Row> documentsDF = spark.table("documents");

    // Access blob metadata using virtual columns
    Dataset<Row> blobMetadata = documentsDF.select(
        "id",
        "title",
        "content__blob_pos",
        "content__blob_size"
    );
    blobMetadata.show();

    // Filter by blob size
    Dataset<Row> largeBlobs = documentsDF.filter("content__blob_size > 1000000");
    largeBlobs.select("id", "title", "content__blob_size").show();
    ```

## Time Travel

Lance supports time travel queries, allowing you to query historical versions of your data using either a specific version number or a timestamp.

### Query by Version

Use `VERSION AS OF` to query a specific version of the table:

=== "SQL"
    ```sql
    -- Query version 5 of the table
    SELECT * FROM users VERSION AS OF 5;

    -- Query specific columns from a version
    SELECT id, name FROM users VERSION AS OF 3;
    ```

=== "Python"
    ```python
    # Query a specific version using DataFrame API
    df = spark.read \
        .format("lance") \
        .option("version", "5") \
        .load("/path/to/dataset.lance")
    df.show()
    ```

=== "Scala"
    ```scala
    // Query a specific version using DataFrame API
    val df = spark.read
        .format("lance")
        .option("version", "5")
        .load("/path/to/dataset.lance")
    df.show()
    ```

=== "Java"
    ```java
    // Query a specific version using DataFrame API
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("version", "5")
        .load("/path/to/dataset.lance");
    df.show();
    ```

### Query by Timestamp

Use `TIMESTAMP AS OF` to query the table as it existed at a specific point in time:

=== "SQL"
    ```sql
    -- Query the table as it was at a specific timestamp
    SELECT * FROM users TIMESTAMP AS OF '2024-01-15 10:30:00';

    -- Query with timestamp in epoch microseconds
    SELECT * FROM users TIMESTAMP AS OF 1705314600000000;
    ```

=== "Python"
    ```python
    # Query by timestamp using SQL
    spark.sql("SELECT * FROM users TIMESTAMP AS OF '2024-01-15 10:30:00'").show()
    ```

=== "Scala"
    ```scala
    // Query by timestamp using SQL
    spark.sql("SELECT * FROM users TIMESTAMP AS OF '2024-01-15 10:30:00'").show()
    ```

=== "Java"
    ```java
    // Query by timestamp using SQL
    spark.sql("SELECT * FROM users TIMESTAMP AS OF '2024-01-15 10:30:00'").show();
    ```

## Read Options

These options control how data is read from Lance datasets. They can be set using the `.option()` method when reading data.

| Option                | Type    | Default | Description                                                                                                       |
|-----------------------|---------|---------|-------------------------------------------------------------------------------------------------------------------|
| `batch_size`          | Integer | `512`   | Number of rows to read per batch during scanning. Larger values may improve throughput but increase memory usage. |
| `version`             | Integer | Latest  | Specific dataset version to read. If not specified, reads the latest version.                                     |
| `block_size`          | Integer | -       | Block size in bytes for reading data.                                                                             |
| `index_cache_size`    | Integer | -       | Size of the index cache in number of entries.                                                                     |
| `metadata_cache_size` | Integer | -       | Size of the metadata cache in number of entries.                                                                  |
| `pushDownFilters`     | Boolean | `true`  | Whether to push down filter predicates to the Lance reader for optimized scanning.                                |
| `topN_push_down`      | Boolean | `true`  | Whether to push down TopN (ORDER BY ... LIMIT) operations to Lance for optimized sorting.                         |

### Example: Reading with Options

=== "SQL"
    ```sql
    -- Read a specific version using table options
    SELECT * FROM lance.`/path/to/dataset.lance` VERSION AS OF 10;
    ```

=== "Python"
    ```python
    # Reading with options
    df = spark.read \
        .format("lance") \
        .option("batch_size", "1024") \
        .option("version", "5") \
        .load("/path/to/dataset.lance")
    ```

=== "Scala"
    ```scala
    // Reading with options
    val df = spark.read
        .format("lance")
        .option("batch_size", "1024")
        .option("version", "5")
        .load("/path/to/dataset.lance")
    ```

=== "Java"
    ```java
    // Reading with options
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("batch_size", "1024")
        .option("version", "5")
        .load("/path/to/dataset.lance");
    ```

### Example: Tuning Batch Size for Performance

=== "Python"
    ```python
    # Larger batch size for better throughput on large scans
    df = spark.read \
        .format("lance") \
        .option("batch_size", "4096") \
        .load("/path/to/dataset.lance")
    ```

=== "Scala"
    ```scala
    // Larger batch size for better throughput on large scans
    val df = spark.read
        .format("lance")
        .option("batch_size", "4096")
        .load("/path/to/dataset.lance")
    ```
