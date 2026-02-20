# Performance Tuning

This guide covers performance tuning for Lance Spark operations in large-scale ETL and batch processing scenarios.

## Understanding Lance's Default Optimization

Lance is **optimized by default for random access patterns** - fast point lookups, vector searches, and selective column reads.
These defaults work well for ML/AI workloads where you frequently access individual records or small batches.

For **large-scale batch ETL and scan-heavy OLAP operations** (writing millions of rows, full table scans, bulk exports),
you can tune Lance's environment variables and Spark options to better utilize available resources.

## General Recommendations

For optimal performance with Lance, we recommend:

1. **Use fixed-size lists for vector columns** - Store embedding vectors as `ARRAY<FLOAT>` with a fixed dimension. This enables efficient SIMD operations and better compression. See [Vector Columns](operations/ddl/create-table.md#vector-columns).

2. **Use blob encoding for multimodal data** - Store large binary data (images, audio, video) using Lance's unique blob encoding to enable efficient access. See [Blob Columns](operations/ddl/create-table.md#blob-columns).

3. **Use large varchar for large text** - When storing very large string values for use cases like full text search, use the large varchar type to avoid out of memory issues. See [Large String Columns](operations/ddl/create-table.md#large-string-columns).

## Write Performance

### Upload Concurrency

Set via environment variable `LANCE_UPLOAD_CONCURRENCY` (default: 10).

Controls the number of concurrent multipart upload streams to S3. 
Increasing this to match your CPU core count can improve throughput.

```bash
export LANCE_UPLOAD_CONCURRENCY=32
```

### Upload Part Size

Set via environment variable `LANCE_INITIAL_UPLOAD_SIZE` (default: 5MB).

Controls the initial part size for S3 multipart uploads. 
Larger part sizes reduce the number of API calls and can improve throughput for large writes. 
However, larger part sizes use more memory and may increase latency for small writes. 
Use the default for interactive workloads.

!!!note
      Lance automatically increments the multipart upload size by 5MB every 100 uploads,
      so large file writes progressively use increasingly large upload parts.
      There is no configuration for a fixed upload size.

```bash
export LANCE_INITIAL_UPLOAD_SIZE=33554432  # 32MB
```

### Queued Write Buffer (Experimental)

Set via Spark write option `use_queued_write_buffer` (default: false).

Enables a buffered write mode that improves throughput for large batch writes.
When enabled, Lance uses a queue-based buffer instead of the default semaphore-based buffer
that batches data more efficiently before writing to storage,
avoiding small lock waiting operations between Spark producer and Lance writer.

!!!note
      This feature is currently in experimental mode.
      It will be set to true by default after the community considers it mature. 

```python
df.write \
    .format("lance") \
    .option("use_queued_write_buffer", "true") \
    .mode("append") \
    .saveAsTable("my_table")
```

### Max Rows Per File

Set via Spark write option `max_row_per_file` (default: 1,000,000).

Controls the maximum number of rows per Lance fragment file.
There is no specific recommended value, but be aware the default is 1 million rows.
If you store many multimodal data columns (images, audio, embeddings)
without using [Lance blob encoding](operations/dml/dataframe-write.md#writing-blob-data),
or store a lot of long text columns, the file size might become very large.
From Lance's perspective, having very large files does not impact your read performance.
But you may want to reduce this value depending on the limits in your choice of object storage.

```python
df.write \
    .format("lance") \
    .option("max_row_per_file", "500000") \
    .mode("append") \
    .saveAsTable("my_table")
```

## Read Performance

### I/O Threads

Set via environment variable `LANCE_IO_THREADS` (default: 64).

Controls the number of I/O threads used for parallel reads from storage. 
For large scans, increasing this to match your CPU core count enables more concurrent S3 requests.

```bash
export LANCE_IO_THREADS=128
```
