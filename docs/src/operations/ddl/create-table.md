# CREATE TABLE

Create new Lance tables with SQL DDL statements or DataFrames.

## Basic Table Creation

=== "SQL"
    ```sql
    CREATE TABLE users (
        id BIGINT NOT NULL,
        name STRING,
        email STRING,
        created_at TIMESTAMP
    );
    ```

=== "Python"
    ```python
    # Create DataFrame
    data = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Charlie", "charlie@example.com")
    ]
    df = spark.createDataFrame(data, ["id", "name", "email"])

    # Write as new table using catalog
    df.writeTo("users").create()
    ```

=== "Scala"
    ```scala
    import spark.implicits._

    // Create DataFrame
    val data = Seq(
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
        (3, "Charlie", "charlie@example.com")
    )
    val df = data.toDF("id", "name", "email")

    // Write as new table using catalog
    df.writeTo("users").create()
    ```

=== "Java"
    ```java
    import org.apache.spark.sql.types.*;
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.RowFactory;

    // Create DataFrame
    List<Row> data = Arrays.asList(
        RowFactory.create(1L, "Alice", "alice@example.com"),
        RowFactory.create(2L, "Bob", "bob@example.com"),
        RowFactory.create(3L, "Charlie", "charlie@example.com")
    );

    StructType schema = new StructType(new StructField[]{
        new StructField("id", DataTypes.LongType, false, Metadata.empty()),
        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
        new StructField("email", DataTypes.StringType, true, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // Write as new table using catalog
    df.writeTo("users").create();
    ```

## Complex Data Types

=== "SQL"
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

=== "SQL"
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

=== "Python"
    ```python
    import numpy as np

    # Create DataFrame with vector data
    data = [(i, np.random.rand(128).astype(np.float32).tolist()) for i in range(100)]
    df = spark.createDataFrame(data, ["id", "embeddings"])

    # Write to Lance table with tableProperty
    df.writeTo("embeddings_table") \
        .tableProperty("embeddings.arrow.fixed-size-list.size", "128") \
        .createOrReplace()
    ```

=== "Scala"
    ```scala
    import scala.util.Random

    // Create DataFrame with vector data
    val data = (0 until 100).map { i =>
      (i, Array.fill(128)(Random.nextFloat()))
    }
    val df = data.toDF("id", "embeddings")

    // Write to Lance table with tableProperty
    df.writeTo("embeddings_table")
      .tableProperty("embeddings.arrow.fixed-size-list.size", "128")
      .createOrReplace()
    ```

=== "Java"
    ```java
    // Create DataFrame with vector data
    List<Row> rows = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
        float[] vector = new float[128];
        for (int j = 0; j < 128; j++) {
            vector[j] = random.nextFloat();
        }
        rows.add(RowFactory.create(i, vector));
    }

    StructType schema = new StructType(new StructField[] {
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("embeddings",
            DataTypes.createArrayType(DataTypes.FloatType, false), false)
    });

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    // Write to Lance table with tableProperty
    df.writeTo("embeddings_table")
        .tableProperty("embeddings.arrow.fixed-size-list.size", "128")
        .createOrReplace();
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

=== "SQL"
    ```sql
    CREATE TABLE documents (
        id INT NOT NULL,
        title STRING,
        content BINARY
    ) USING lance
    TBLPROPERTIES (
        'content.lance.encoding' = 'blob'
    );
    ```

=== "Python"
    ```python
    # Create DataFrame with binary data
    data = [
        (1, "Document 1", bytearray(b"Large binary content..." * 10000)),
        (2, "Document 2", bytearray(b"Another large file..." * 10000))
    ]
    df = spark.createDataFrame(data, ["id", "title", "content"])

    # Write to Lance table with blob encoding
    df.writeTo("documents") \
        .tableProperty("content.lance.encoding", "blob") \
        .createOrReplace()
    ```

=== "Scala"
    ```scala
    // Create DataFrame with binary data
    val data = Seq(
      (1, "Document 1", Array.fill[Byte](1000000)(0x42)),
      (2, "Document 2", Array.fill[Byte](1000000)(0x43))
    )
    val df = data.toDF("id", "title", "content")

    // Write to Lance table with blob encoding
    df.writeTo("documents")
      .tableProperty("content.lance.encoding", "blob")
      .createOrReplace()
    ```

=== "Java"
    ```java
    // Create DataFrame with binary data
    byte[] largeData1 = new byte[1000000];
    byte[] largeData2 = new byte[1000000];
    Arrays.fill(largeData1, (byte) 0x42);
    Arrays.fill(largeData2, (byte) 0x43);

    List<Row> data = Arrays.asList(
        RowFactory.create(1, "Document 1", largeData1),
        RowFactory.create(2, "Document 2", largeData2)
    );

    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("title", DataTypes.StringType, true),
        DataTypes.createStructField("content", DataTypes.BinaryType, true)
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // Write to Lance table with blob encoding
    df.writeTo("documents")
        .tableProperty("content.lance.encoding", "blob")
        .createOrReplace();
    ```

## Large String Columns

Lance supports large string columns for storing very large text data. By default, Arrow uses `Utf8` (VarChar) type with 32-bit offsets, which limits total string data to 2GB per batch. For columns containing very large strings (e.g., document content, base64-encoded data), you can use `LargeUtf8` (LargeVarChar) with 64-bit offsets.

### When to Use Large Strings

Use large string columns when:

- Individual string values may exceed several MB
- Total string data per batch may exceed 2GB
- You encounter `OversizedAllocationException` errors during writes

### Creating Large String Columns

To create a table with large string columns, use the table property pattern `<column_name>.arrow.large_var_char` with the value `'true'`:

=== "SQL"
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

=== "Python"
    ```python
    # Create DataFrame with large string content
    data = [
        (1, "Article 1", "Very long content..." * 100000),
        (2, "Article 2", "Another long article..." * 100000)
    ]
    df = spark.createDataFrame(data, ["id", "title", "content"])

    # Write to Lance table with large string support
    df.writeTo("articles") \
        .tableProperty("content.arrow.large_var_char", "true") \
        .createOrReplace()
    ```

=== "Scala"
    ```scala
    // Create DataFrame with large string content
    val data = Seq(
      (1, "Article 1", "Very long content..." * 100000),
      (2, "Article 2", "Another long article..." * 100000)
    )
    val df = data.toDF("id", "title", "content")

    // Write to Lance table with large string support
    df.writeTo("articles")
      .tableProperty("content.arrow.large_var_char", "true")
      .createOrReplace()
    ```

=== "Java"
    ```java
    // Create DataFrame with large string content
    String largeContent1 = String.join("", Collections.nCopies(100000, "Very long content..."));
    String largeContent2 = String.join("", Collections.nCopies(100000, "Another long article..."));

    List<Row> data = Arrays.asList(
        RowFactory.create(1, "Article 1", largeContent1),
        RowFactory.create(2, "Article 2", largeContent2)
    );

    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("title", DataTypes.StringType, true),
        DataTypes.createStructField("content", DataTypes.StringType, true)
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // Write to Lance table with large string support
    df.writeTo("articles")
        .tableProperty("content.arrow.large_var_char", "true")
        .createOrReplace();
    ```
