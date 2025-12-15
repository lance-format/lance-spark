# DataFrame Create Table

Create Lance tables from DataFrames using the DataSource V2 API.

## Basic DataFrame Creation

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

## Creating Tables with Vector Columns

Lance supports vector (embedding) columns for AI workloads. These columns are stored internally as Arrow `FixedSizeList[n]` where `n` is the vector dimension. Since Spark DataFrames don't have a native fixed-size array type, you need to add metadata to your schema fields to indicate that an `ArrayType(FloatType)` or `ArrayType(DoubleType)` should be converted to Arrow FixedSizeList.

The metadata key `"arrow.fixed-size-list.size"` with a value like `128` tells the Lance-Spark connector to convert that array column to a `FixedSizeList[128]` during write operations.

### Supported Types

- **Element Types**: `FloatType` (float32), `DoubleType` (float64)
- **Array Requirements**:
  - Must have `containsNull = false`
  - Column must be non-nullable
  - All arrays must have exactly the specified dimension

### Examples

Use the `tableProperty()` API to specify vector column dimensions:

=== "Python"
    ```python
    import numpy as np

    # Create DataFrame with vector data (no special metadata needed)
    data = [(i, np.random.rand(128).astype(np.float32).tolist()) for i in range(100)]
    df = spark.createDataFrame(data, ["id", "embeddings"])

    # Write to Lance format with tableProperty
    df.writeTo("vectors_table") \
        .using("lance") \
        .tableProperty("embeddings.arrow.fixed-size-list.size", "128") \
        .createOrReplace()
    ```

=== "Scala"
    ```scala
    import scala.util.Random

    // Create DataFrame with vector data (no special metadata needed)
    val data = (0 until 100).map { i =>
      (i, Array.fill(128)(Random.nextFloat()))
    }
    val df = data.toDF("id", "embeddings")

    // Write to Lance format with tableProperty
    df.writeTo("vectors_table")
      .using("lance")
      .tableProperty("embeddings.arrow.fixed-size-list.size", "128")
      .createOrReplace()
    ```

=== "Java"
    ```java
    // Create DataFrame with vector data (no special metadata needed)
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

    // Write to Lance format with tableProperty
    df.writeTo("vectors_table")
        .using("lance")
        .tableProperty("embeddings.arrow.fixed-size-list.size", "128")
        .createOrReplace();
    ```

**Note**: After creating the table with `tableProperty()`, 
subsequent DataFrame writes will automatically use `FixedSizeList` encoding without requiring any metadata. 
See [DataFrame Write](../dml/dataframe-write.md#writing-vector-data) for details.

### Creating Multiple Vector Columns

You can create DataFrames with multiple vector columns, each with different dimensions:

=== "Python"
    ```python
    import numpy as np

    # Create DataFrame with multiple vector columns
    data = [
        (i,
         np.random.rand(384).astype(np.float32).tolist(),
         np.random.rand(512).tolist())
        for i in range(100)
    ]
    df = spark.createDataFrame(data, ["id", "text_embeddings", "image_embeddings"])

    # Write to Lance format with multiple tableProperty calls
    df.writeTo("multi_vectors_table") \
        .using("lance") \
        .tableProperty("text_embeddings.arrow.fixed-size-list.size", "384") \
        .tableProperty("image_embeddings.arrow.fixed-size-list.size", "512") \
        .createOrReplace()
    ```

=== "Scala"
    ```scala
    import scala.util.Random

    // Create DataFrame with multiple vector columns
    val data = (0 until 100).map { i =>
      (i, Array.fill(384)(Random.nextFloat()), Array.fill(512)(Random.nextDouble()))
    }
    val df = data.toDF("id", "text_embeddings", "image_embeddings")

    // Write to Lance format with multiple tableProperty calls
    df.writeTo("multi_vectors_table")
      .using("lance")
      .tableProperty("text_embeddings.arrow.fixed-size-list.size", "384")
      .tableProperty("image_embeddings.arrow.fixed-size-list.size", "512")
      .createOrReplace()
    ```

=== "Java"
    ```java
    // Create DataFrame with multiple vector columns
    List<Row> rows = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
        float[] textVec = new float[384];
        double[] imageVec = new double[512];
        for (int j = 0; j < 384; j++) textVec[j] = random.nextFloat();
        for (int j = 0; j < 512; j++) imageVec[j] = random.nextDouble();
        rows.add(RowFactory.create(i, textVec, imageVec));
    }

    StructType schema = new StructType(new StructField[] {
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("text_embeddings",
            DataTypes.createArrayType(DataTypes.FloatType, false), false),
        DataTypes.createStructField("image_embeddings",
            DataTypes.createArrayType(DataTypes.DoubleType, false), false)
    });

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    // Write to Lance format with multiple tableProperty calls
    df.writeTo("multi_vectors_table")
        .using("lance")
        .tableProperty("text_embeddings.arrow.fixed-size-list.size", "384")
        .tableProperty("image_embeddings.arrow.fixed-size-list.size", "512")
        .createOrReplace();
    ```