# DataFrame Write

Append data to existing Lance tables using DataFrames.

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

## Writing Blob Data

When writing binary data to blob columns, you need to add metadata to the DataFrame schema to indicate blob encoding.

=== "Python"
    ```python
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BinaryType

    # Create schema with blob metadata
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("content", BinaryType(), True,
                   metadata={"lance-encoding:blob": "true"})
    ])

    # Create data with large binary content
    data = [
        (1, "Document 1", bytearray(b"Large binary content..." * 10000)),
        (2, "Document 2", bytearray(b"Another large file..." * 10000))
    ]

    df = spark.createDataFrame(data, schema)

    # Write to blob table
    df.writeTo("documents").append()
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.types._

    // Create schema with blob metadata
    val metadata = new MetadataBuilder()
      .putString("lance-encoding:blob", "true")
      .build()

    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("title", StringType, nullable = true),
      StructField("content", BinaryType, nullable = true, metadata)
    ))

    // Create data with large binary content
    val data = Seq(
      (1, "Document 1", Array.fill[Byte](1000000)(0x42)),
      (2, "Document 2", Array.fill[Byte](1000000)(0x43))
    )

    val df = spark.createDataFrame(data).toDF("id", "title", "content")

    // Write to blob table
    df.writeTo("documents").append()
    ```

=== "Java"
    ```java
    import org.apache.spark.sql.types.*;
    import java.util.Arrays;
    import java.util.List;

    // Create schema with blob metadata
    Metadata blobMetadata = new MetadataBuilder()
        .putString("lance-encoding:blob", "true")
        .build();

    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("title", DataTypes.StringType, true),
        DataTypes.createStructField("content", DataTypes.BinaryType, true, blobMetadata)
    });

    // Create data with large binary content
    byte[] largeData1 = new byte[1000000];
    byte[] largeData2 = new byte[1000000];
    Arrays.fill(largeData1, (byte) 0x42);
    Arrays.fill(largeData2, (byte) 0x43);

    List<Row> data = Arrays.asList(
        RowFactory.create(1, "Document 1", largeData1),
        RowFactory.create(2, "Document 2", largeData2)
    );

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // Write to blob table
    df.writeTo("documents").append();
    ```

**Important Notes**:

- Blob metadata must be added to the DataFrame schema using the key `"lance-encoding:blob"` with value `"true"`
- The binary column must be nullable in the schema
- Blob encoding is most beneficial for large binary values (typically > 64KB)
- When writing to an existing blob table, ensure the schema metadata matches the table definition

## Writing Large String Data

When writing very large string values, you may encounter `OversizedAllocationException` errors due to Arrow's default 32-bit offset limitation (max 2GB per batch). Use large string encoding to enable 64-bit offsets.

### Appending to Existing Tables

If you created a table with the `arrow.large_var_char` property (see [CREATE TABLE](../ddl/create-table.md#large-string-columns)), subsequent DataFrame writes will automatically use `LargeVarCharVector`. No additional configuration is needed:

```python
# Table was created with: 'content.arrow.large_var_char' = 'true'
# Subsequent writes automatically use large string encoding
df.writeTo("articles").append()
```

### Creating New Tables with DataFrame

When using `createOrReplace()` to create a new table, use the `tableProperty()` method to specify large string columns:

=== "Python"
    ```python
    # Create data with large string content
    data = [
        (1, "Article 1", "Very long content..." * 100000),
        (2, "Article 2", "Another long article..." * 100000)
    ]

    df = spark.createDataFrame(data, ["id", "title", "content"])

    # Create new table with large string support using tableProperty
    df.writeTo("lance_catalog.default.articles") \
        .using("lance") \
        .tableProperty("content.arrow.large_var_char", "true") \
        .createOrReplace()
    ```

=== "Scala"
    ```scala
    // Create data with large string content
    val data = Seq(
      (1, "Article 1", "Very long content..." * 100000),
      (2, "Article 2", "Another long article..." * 100000)
    )

    val df = data.toDF("id", "title", "content")

    // Create new table with large string support using tableProperty
    df.writeTo("lance_catalog.default.articles")
      .using("lance")
      .tableProperty("content.arrow.large_var_char", "true")
      .createOrReplace()
    ```

=== "Java"
    ```java
    import java.util.Arrays;
    import java.util.Collections;

    // Create data with large string content
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

    // Create new table with large string support using tableProperty
    df.writeTo("lance_catalog.default.articles")
        .using("lance")
        .tableProperty("content.arrow.large_var_char", "true")
        .createOrReplace();
    ```

**Important Notes**:

- For existing tables created with `arrow.large_var_char` property, writes automatically use large strings
- For new tables via DataFrame, use `.tableProperty("column.arrow.large_var_char", "true")`
- Use large strings when individual values or total batch data may exceed 2GB

## Writing Vector Data

Lance supports vector (embedding) columns stored as Arrow `FixedSizeList`. When writing vector data to existing tables, the behavior depends on how the table was created.

### Appending to Existing Tables

If you created a table with the `arrow.fixed-size-list.size` property (see [CREATE TABLE](../ddl/create-table.md#vector-columns)), subsequent DataFrame writes will automatically use `FixedSizeList`. No additional configuration is needed:

```python
# Table was created with: 'embeddings.arrow.fixed-size-list.size' = '128'
# Subsequent writes automatically use FixedSizeList encoding

# Plain schema WITHOUT metadata - it just works!
data = [(i, [float(j) for j in range(128)]) for i in range(10, 20)]
df = spark.createDataFrame(data, ["id", "embeddings"])

df.writeTo("embeddings_table").append()
```

### Creating New Tables with DataFrame

When using `createOrReplace()` to create a new table, use the `tableProperty()` method to specify vector columns:

=== "Python"
    ```python
    import numpy as np

    # Create data with vector content
    data = [(i, np.random.rand(128).astype(np.float32).tolist()) for i in range(100)]
    df = spark.createDataFrame(data, ["id", "embeddings"])

    # Create new table with vector support using tableProperty
    df.writeTo("lance_catalog.default.vectors") \
        .using("lance") \
        .tableProperty("embeddings.arrow.fixed-size-list.size", "128") \
        .createOrReplace()
    ```

=== "Scala"
    ```scala
    import scala.util.Random

    // Create data with vector content
    val data = (0 until 100).map { i =>
      (i, Array.fill(128)(Random.nextFloat()))
    }
    val df = data.toDF("id", "embeddings")

    // Create new table with vector support using tableProperty
    df.writeTo("lance_catalog.default.vectors")
      .using("lance")
      .tableProperty("embeddings.arrow.fixed-size-list.size", "128")
      .createOrReplace()
    ```

=== "Java"
    ```java
    import java.util.Random;

    // Create data with vector content
    List<Row> data = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
        float[] vector = new float[128];
        for (int j = 0; j < 128; j++) {
            vector[j] = random.nextFloat();
        }
        data.add(RowFactory.create(i, vector));
    }

    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("embeddings",
            DataTypes.createArrayType(DataTypes.FloatType, false), false)
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // Create new table with vector support using tableProperty
    df.writeTo("lance_catalog.default.vectors")
        .using("lance")
        .tableProperty("embeddings.arrow.fixed-size-list.size", "128")
        .createOrReplace();
    ```

**Important Notes**:

- For existing tables created with `arrow.fixed-size-list.size` property, writes automatically use `FixedSizeList`
- For new tables via DataFrame, use `.tableProperty("column.arrow.fixed-size-list.size", "dimension")`
- All vectors in a column must have the same dimension
- Supported element types: `FLOAT` (float32), `DOUBLE` (float64)