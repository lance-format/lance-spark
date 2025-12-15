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

Lance supports blob encoding for large binary data. When writing blob data to existing tables, the behavior depends on how the table was created.

### Appending to Existing Tables

If you created a table with the `lance.encoding` property (see [CREATE TABLE](../ddl/create-table.md#blob-columns)), subsequent DataFrame writes will automatically use blob encoding. No additional configuration is needed:

```python
# Table was created with: 'content.lance.encoding' = 'blob'
# Subsequent writes automatically use blob encoding

# Plain schema WITHOUT metadata - it just works!
data = [(i, f"Document {i}", bytearray(b"Large content..." * 10000)) for i in range(10, 20)]
df = spark.createDataFrame(data, ["id", "title", "content"])

df.writeTo("documents").append()
```

### Creating New Tables with DataFrame

When using `createOrReplace()` to create a new table, use the `tableProperty()` method to specify blob columns. See [DataFrame Create Table](../ddl/dataframe-create-table.md#creating-tables-with-blob-columns) for details.

**Important Notes**:

- For existing tables created with `lance.encoding` property, writes automatically use blob encoding
- The binary column must be nullable in the schema
- Blob encoding is most beneficial for large binary values (typically > 64KB)

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

When using `createOrReplace()` to create a new table, use the `tableProperty()` method to specify large string columns. See [DataFrame Create Table](../ddl/dataframe-create-table.md#creating-tables-with-large-string-columns) for details.

**Important Notes**:

- For existing tables created with `arrow.large_var_char` property, writes automatically use large strings
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

When using `createOrReplace()` to create a new table, use the `tableProperty()` method to specify vector columns. See [DataFrame Create Table](../ddl/dataframe-create-table.md#creating-tables-with-vector-columns) for details.

**Important Notes**:

- For existing tables created with `arrow.fixed-size-list.size` property, writes automatically use `FixedSizeList`
- All vectors in a column must have the same dimension
- Supported element types: `FLOAT` (float32), `DOUBLE` (float64)