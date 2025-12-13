# Spark Options

This page documents the available options that can be configured when reading from or writing to Lance datasets 
using Spark's DataSource API.

## Usage

Options can be set using the `.option()` method when reading or writing data:

=== "Scala"
    ```scala
    // Reading with options
    val df = spark.read
        .format("lance")
        .option("batch_size", "1024")
        .option("version", "5")
        .load("/path/to/dataset.lance")

    // Writing with options
    df.write
        .format("lance")
        .option("max_rows_per_group", "10000")
        .option("data_storage_version", "STABLE")
        .save("/path/to/output.lance")
    ```

=== "Python"
    ```python
    # Reading with options
    df = spark.read \
        .format("lance") \
        .option("batch_size", "1024") \
        .option("version", "5") \
        .load("/path/to/dataset.lance")

    # Writing with options
    df.write \
        .format("lance") \
        .option("max_rows_per_group", "10000") \
        .option("data_storage_version", "STABLE") \
        .save("/path/to/output.lance")
    ```

=== "Java"
    ```java
    // Reading with options
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("batch_size", "1024")
        .option("version", "5")
        .load("/path/to/dataset.lance");

    // Writing with options
    df.write()
        .format("lance")
        .option("max_rows_per_group", "10000")
        .option("data_storage_version", "STABLE")
        .save("/path/to/output.lance");
    ```

## Read Options

These options control how data is read from Lance datasets.

| Option                | Type    | Default | Description                                                                                                       |
|-----------------------|---------|---------|-------------------------------------------------------------------------------------------------------------------|
| `batch_size`          | Integer | `512`   | Number of rows to read per batch during scanning. Larger values may improve throughput but increase memory usage. |
| `version`             | Integer | Latest  | Specific dataset version to read. If not specified, reads the latest version.                                     |
| `block_size`          | Integer | -       | Block size in bytes for reading data.                                                                             |
| `index_cache_size`    | Integer | -       | Size of the index cache in number of entries.                                                                     |
| `metadata_cache_size` | Integer | -       | Size of the metadata cache in number of entries.                                                                  |
| `pushDownFilters`     | Boolean | `true`  | Whether to push down filter predicates to the Lance reader for optimized scanning.                                |
| `topN_push_down`      | Boolean | `true`  | Whether to push down TopN (ORDER BY ... LIMIT) operations to Lance for optimized sorting.                         |

### Example: Reading a Specific Version

=== "Scala"
    ```scala
    val df = spark.read
        .format("lance")
        .option("version", "10")
        .load("/path/to/dataset.lance")
    ```

=== "Python"
    ```python
    df = spark.read \
        .format("lance") \
        .option("version", "10") \
        .load("/path/to/dataset.lance")
    ```

### Example: Tuning Batch Size for Performance

=== "Scala"
    ```scala
    // Larger batch size for better throughput on large scans
    val df = spark.read
        .format("lance")
        .option("batch_size", "4096")
        .load("/path/to/dataset.lance")
    ```

=== "Python"
    ```python
    # Larger batch size for better throughput on large scans
    df = spark.read \
        .format("lance") \
        .option("batch_size", "4096") \
        .load("/path/to/dataset.lance")
    ```

## Write Options

These options control how data is written to Lance datasets.

| Option                 | Type    | Default  | Description                                                                         |
|------------------------|---------|----------|-------------------------------------------------------------------------------------|
| `write_mode`           | String  | `append` | Write mode: `append` to add to existing data, `overwrite` to replace existing data. |
| `max_row_per_file`     | Integer | -        | Maximum number of rows per Lance file.                                              |
| `max_rows_per_group`   | Integer | -        | Maximum number of rows per row group within a file.                                 |
| `max_bytes_per_file`   | Long    | -        | Maximum size in bytes per Lance file.                                               |
| `data_storage_version` | String  | -        | Lance file format version: `LEGACY` or `STABLE`.                                    |

### Example: Controlling File Size

=== "Scala"
    ```scala
    df.write
        .format("lance")
        .option("max_row_per_file", "100000")
        .option("max_rows_per_group", "10000")
        .save("/path/to/output.lance")
    ```

=== "Python"
    ```python
    df.write \
        .format("lance") \
        .option("max_row_per_file", "100000") \
        .option("max_rows_per_group", "10000") \
        .save("/path/to/output.lance")
    ```

### Example: Using Stable Storage Format

=== "Scala"
    ```scala
    df.write
        .format("lance")
        .option("data_storage_version", "STABLE")
        .save("/path/to/output.lance")
    ```

=== "Python"
    ```python
    df.write \
        .format("lance") \
        .option("data_storage_version", "STABLE") \
        .save("/path/to/output.lance")
    ```
