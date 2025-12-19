# INSERT INTO

Add data to existing Lance tables using SQL or DataFrames.

## Basic Insert

=== "SQL"
    ```sql
    INSERT INTO users VALUES
        (4, 'David', 'david@example.com', '2024-01-15 10:30:00'),
        (5, 'Eva', 'eva@example.com', '2024-01-15 11:45:00');
    ```

=== "Python"
    ```python
    # Create new data
    new_data = [
        (8, "Henry", "henry@example.com"),
        (9, "Ivy", "ivy@example.com")
    ]
    new_df = spark.createDataFrame(new_data, ["id", "name", "email"])

    # Append to existing table
    new_df.writeTo("users").append()

    # Alternative: use traditional write API with mode
    new_df.write.mode("append").saveAsTable("users")
    ```

=== "Scala"
    ```scala
    // Create new data
    val newData = Seq(
        (8, "Henry", "henry@example.com"),
        (9, "Ivy", "ivy@example.com")
    )
    val newDF = newData.toDF("id", "name", "email")

    // Append to existing table
    newDF.writeTo("users").append()

    // Alternative: use traditional write API with mode
    newDF.write.mode("append").saveAsTable("users")
    ```

=== "Java"
    ```java
    // Create new data
    List<Row> newData = Arrays.asList(
        RowFactory.create(8L, "Henry", "henry@example.com"),
        RowFactory.create(9L, "Ivy", "ivy@example.com")
    );
    Dataset<Row> newDF = spark.createDataFrame(newData, schema);

    // Append to existing table
    newDF.writeTo("users").append();

    // Alternative: use traditional write API with mode
    newDF.write().mode("append").saveAsTable("users");
    ```

## Insert with Column Specification

=== "SQL"
    ```sql
    INSERT INTO users (id, name, email) VALUES
        (6, 'Frank', 'frank@example.com'),
        (7, 'Grace', 'grace@example.com');
    ```

## Insert from SELECT

=== "SQL"
    ```sql
    INSERT INTO users
    SELECT user_id as id, username as name, email_address as email, signup_date as created_at
    FROM staging.user_signups
    WHERE signup_date >= '2024-01-01';
    ```

## Insert with Complex Data Types

=== "SQL"
    ```sql
    INSERT INTO events VALUES (
        1001,
        123,
        'page_view',
        array('web', 'desktop'),
        struct('web_app', 1, '2024-01-15 12:00:00'),
        '2024-01-15 12:00:00'
    );
    ```

## Writing Vector Data

If you created a table with the `arrow.fixed-size-list.size` property (see [CREATE TABLE](../ddl/create-table.md#vector-columns)), 
subsequent writes will automatically use `FixedSizeList`. No additional configuration is needed:

=== "SQL"
    ```sql
    -- Insert vector data (example with small vectors for clarity)
    INSERT INTO embeddings_table VALUES
        (10, 'new text', array(0.1, 0.2, 0.3, ...)); -- 128 float values
    ```

=== "Python"
    ```python
    # Table was created with: 'embeddings.arrow.fixed-size-list.size' = '128'
    # Subsequent writes automatically use FixedSizeList encoding

    # Plain schema WITHOUT metadata - it just works!
    data = [(i, [float(j) for j in range(128)]) for i in range(10, 20)]
    df = spark.createDataFrame(data, ["id", "embeddings"])

    df.writeTo("embeddings_table").append()
    ```

=== "Scala"
    ```scala
    // Table was created with: 'embeddings.arrow.fixed-size-list.size' = '128'
    // Subsequent writes automatically use FixedSizeList encoding

    val data = (10 until 20).map { i =>
      (i, Array.fill(128)(Random.nextFloat()))
    }
    val df = data.toDF("id", "embeddings")

    df.writeTo("embeddings_table").append()
    ```

=== "Java"
    ```java
    // Table was created with: 'embeddings.arrow.fixed-size-list.size' = '128'
    // Subsequent writes automatically use FixedSizeList encoding

    List<Row> rows = new ArrayList<>();
    for (int i = 10; i < 20; i++) {
        float[] vector = new float[128];
        for (int j = 0; j < 128; j++) {
            vector[j] = random.nextFloat();
        }
        rows.add(RowFactory.create(i, vector));
    }
    Dataset<Row> df = spark.createDataFrame(rows, schema);

    df.writeTo("embeddings_table").append();
    ```

## Writing Blob Data

If you created a table with the `lance.encoding` property (see [CREATE TABLE](../ddl/create-table.md#blob-columns)), 
subsequent writes will automatically use blob encoding. No additional configuration is needed:

=== "SQL"
    ```sql
    -- Insert blob data (example with binary literals)
    INSERT INTO documents VALUES
        (3, 'Document 3', X'48656C6C6F576F726C64');
    ```

=== "Python"
    ```python
    # Table was created with: 'content.lance.encoding' = 'blob'
    # Subsequent writes automatically use blob encoding

    # Plain schema WITHOUT metadata - it just works!
    data = [(i, f"Document {i}", bytearray(b"Large content..." * 10000)) for i in range(10, 20)]
    df = spark.createDataFrame(data, ["id", "title", "content"])

    df.writeTo("documents").append()
    ```

=== "Scala"
    ```scala
    // Table was created with: 'content.lance.encoding' = 'blob'
    // Subsequent writes automatically use blob encoding

    val data = (10 until 20).map { i =>
      (i, s"Document $i", Array.fill[Byte](100000)(0x42))
    }
    val df = data.toDF("id", "title", "content")

    df.writeTo("documents").append()
    ```

=== "Java"
    ```java
    // Table was created with: 'content.lance.encoding' = 'blob'
    // Subsequent writes automatically use blob encoding

    List<Row> data = new ArrayList<>();
    for (int i = 10; i < 20; i++) {
        byte[] content = new byte[100000];
        Arrays.fill(content, (byte) 0x42);
        data.add(RowFactory.create(i, "Document " + i, content));
    }
    Dataset<Row> df = spark.createDataFrame(data, schema);

    df.writeTo("documents").append();
    ```

## Writing Large String Data

If you created a table with the `arrow.large_var_char` property (see [CREATE TABLE](../ddl/create-table.md#large-string-columns)), 
subsequent writes will automatically use `LargeVarCharVector`. No additional configuration is needed:

=== "SQL"
    ```sql
    -- Insert large string data
    INSERT INTO articles VALUES
        (3, 'Article 3', 'Very long article content...');
    ```

=== "Python"
    ```python
    # Table was created with: 'content.arrow.large_var_char' = 'true'
    # Subsequent writes automatically use large string encoding

    data = [(i, f"Article {i}", "Long content..." * 100000) for i in range(10, 20)]
    df = spark.createDataFrame(data, ["id", "title", "content"])

    df.writeTo("articles").append()
    ```

=== "Scala"
    ```scala
    // Table was created with: 'content.arrow.large_var_char' = 'true'
    // Subsequent writes automatically use large string encoding

    val data = (10 until 20).map { i =>
      (i, s"Article $i", "Long content..." * 100000)
    }
    val df = data.toDF("id", "title", "content")

    df.writeTo("articles").append()
    ```

=== "Java"
    ```java
    // Table was created with: 'content.arrow.large_var_char' = 'true'
    // Subsequent writes automatically use large string encoding

    String longContent = String.join("", Collections.nCopies(100000, "Long content..."));
    List<Row> data = new ArrayList<>();
    for (int i = 10; i < 20; i++) {
        data.add(RowFactory.create(i, "Article " + i, longContent));
    }
    Dataset<Row> df = spark.createDataFrame(data, schema);

    df.writeTo("articles").append();
    ```

## Write Options

These options control how data is written to Lance datasets. They can be set using the `.option()` method when writing data.

| Option                   | Type    | Default  | Description                                                                          |
|--------------------------|---------|----------|--------------------------------------------------------------------------------------|
| `write_mode`             | String  | `append` | Write mode: `append` to add to existing data, `overwrite` to replace existing data. |
| `max_row_per_file`       | Integer | -        | Maximum number of rows per Lance file.                                               |
| `max_rows_per_group`     | Integer | -        | Maximum number of rows per row group within a file.                                  |
| `max_bytes_per_file`     | Long    | -        | Maximum size in bytes per Lance file.                                                |
| `data_storage_version`   | String  | -        | Lance file format version: `LEGACY` or `STABLE`.                                     |
| `batch_size`             | Integer | `512`    | Number of rows per batch during writing.                                             |
| `use_queued_write_buffer`| Boolean | `false`  | Use pipelined write buffer for improved throughput.                                  |
| `queue_depth`            | Integer | `8`      | Queue depth for pipelined writes (only used when `use_queued_write_buffer=true`).    |

### Example: Controlling File Size

=== "Python"
    ```python
    df.write \
        .format("lance") \
        .option("max_row_per_file", "100000") \
        .option("max_rows_per_group", "10000") \
        .save("/path/to/output.lance")
    ```

=== "Scala"
    ```scala
    df.write
        .format("lance")
        .option("max_row_per_file", "100000")
        .option("max_rows_per_group", "10000")
        .save("/path/to/output.lance")
    ```

=== "Java"
    ```java
    df.write()
        .format("lance")
        .option("max_row_per_file", "100000")
        .option("max_rows_per_group", "10000")
        .save("/path/to/output.lance");
    ```

### Example: Using Stable Storage Format

=== "Python"
    ```python
    df.write \
        .format("lance") \
        .option("data_storage_version", "STABLE") \
        .save("/path/to/output.lance")
    ```

=== "Scala"
    ```scala
    df.write
        .format("lance")
        .option("data_storage_version", "STABLE")
        .save("/path/to/output.lance")
    ```

### Example: Using Pipelined Writes

=== "Python"
    ```python
    # Enable pipelined writes for improved throughput
    df.write \
        .format("lance") \
        .option("use_queued_write_buffer", "true") \
        .option("queue_depth", "4") \
        .save("/path/to/output.lance")
    ```

=== "Scala"
    ```scala
    // Enable pipelined writes for improved throughput
    df.write
        .format("lance")
        .option("use_queued_write_buffer", "true")
        .option("queue_depth", "4")
        .save("/path/to/output.lance")
    ```
