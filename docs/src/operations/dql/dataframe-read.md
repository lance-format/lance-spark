# DataFrame Read

Load Lance tables as DataFrames for programmatic data access.

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

## Reading Blob Data

When reading from tables with blob columns, the blob data itself is not materialized. Instead, you can access blob metadata through virtual columns.

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

### Blob Virtual Columns

For each blob column, Lance provides two virtual columns:

- `<column_name>__blob_pos` - The byte position of the blob in the blob file
- `<column_name>__blob_size` - The size of the blob in bytes

These virtual columns can be used for:

- Monitoring blob storage statistics
- Filtering rows by blob size
- Implementing custom blob retrieval logic
- Verifying successful blob writes

**Note**: The blob column itself returns empty byte arrays when read. To access the actual blob data, you would need to use the position and size information to read from the blob file using external tools.
