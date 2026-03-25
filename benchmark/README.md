# TPC-DS Benchmark for Lance Spark

Runs the [TPC-DS](http://www.tpc.org/tpcds/) query suite against Lance and Parquet formats using Apache Spark, comparing query performance, correctness, and resource usage.

`parquet` here refers to Spark's built-in Parquet reader, used as a performance baseline.

## Architecture

Data generation uses the [Apache Kyuubi TPC-DS connector](https://kyuubi.readthedocs.io/en/master/connector/spark/tpcds.html), a pure-Spark data source that generates TPC-DS data in parallel across executors. No external `dsdgen` binary, CSV files, or intermediate formats are needed — data is written directly into the target format (Lance, Parquet, etc.) and can target any Spark-supported storage (local, S3, GCS, HDFS).

The pipeline is split into two independent Spark jobs:

1. **`TpcdsDataGenerator`** — Reads from the Kyuubi TPC-DS catalog and writes all 24 tables directly into each target format.
2. **`TpcdsBenchmarkRunner`** — Registers the generated tables and runs the 99 TPC-DS queries, comparing formats.

### TODO
- **Delta / Iceberg support** — not yet included because they require additional catalog/metastore tooling outside lance-spark's scope.

## Quick Start

```bash
# Build the jars first
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build

# End-to-end: generate data + run queries
make benchmark SF=10 FORMATS=parquet,lance ITERATIONS=3
```

Or run the two phases separately:

```bash
# Step 1: Generate TPC-DS data (parallel Spark job)
./benchmark/scripts/generate-data.sh [SCALE_FACTOR] [FORMATS] [SPARK_MASTER]

# Step 2: Run benchmark queries against generated data
./benchmark/scripts/run-benchmark.sh [FORMATS] [SPARK_MASTER] [ITERATIONS]
```

### Examples

```bash
# Generate SF=10 data in both formats
./benchmark/scripts/generate-data.sh 10 parquet,lance local[*]

# Run queries with profiling
EXPLAIN=true METRICS=true ./benchmark/scripts/run-benchmark.sh parquet,lance local[*] 3

# Run a subset of queries
QUERIES=q3,q14a,q55 ./benchmark/scripts/run-benchmark.sh lance local[*] 1

# Override Spark/Scala versions
SPARK_VERSION=3.5 SCALA_VERSION=2.12 ./benchmark/scripts/generate-data.sh 10
```

Set `SPARK_HOME` if `spark-submit` is not on your `PATH`.

### Using the Docker environment

If you don't have Spark installed locally, use the existing `spark-lance` Docker container. The benchmark jar, data, and results directories are volume-mounted automatically.

```bash
# Build jars on the host
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build

# Start the Spark container
make docker-up

# Generate data (inside the container)
docker exec spark-lance spark-submit \
  --class org.lance.spark.benchmark.TpcdsDataGenerator \
  --master local[*] \
  /home/lance/benchmark/lance-spark-benchmark-0.3.0-beta.1.jar \
  --data-dir /home/lance/data --scale-factor 1 --formats parquet,lance

# Run benchmark queries (inside the container)
docker exec spark-lance spark-submit \
  --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
  --master local[*] \
  /home/lance/benchmark/lance-spark-benchmark-0.3.0-beta.1.jar \
  --data-dir /home/lance/data --results-dir /home/lance/results \
  --formats parquet,lance --iterations 3

# Results appear on the host at benchmark/results/
```

Or shell in and run interactively:

```bash
make docker-shell
# Now inside the container, spark-submit as usual
```

### Profiling Features

**`EXPLAIN=true`** prints the Spark query plan before executing each query (first iteration only), useful for verifying filter/projection pushdown.

**`METRICS=true`** registers a `SparkListener` that captures per-task stats:
```
  [OK] q3 iter=1 time=822ms rows=89
       Metrics: tasks=12 cpu=680ms gc=15ms read=45MB shuffle_r=2MB shuffle_w=2MB
```

## Running on an External Cluster

To benchmark against a standalone or YARN cluster with data in an object store (S3, GCS, HDFS):

### 1. Build the jars

```bash
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build
```

### 2. Generate TPC-DS data directly to object store

Data generation is a Spark job — it writes directly to any Spark-supported storage:

```bash
spark-submit \
  --class org.lance.spark.benchmark.TpcdsDataGenerator \
  --master local[*] \
  --driver-memory 8g \
  --jars path/to/lance-spark-bundle-3.5_2.12-*.jar \
  --conf spark.sql.extensions=org.lance.spark.LanceSparkSessionExtension \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET \
  benchmark/target/lance-spark-benchmark-*.jar \
  --data-dir s3a://my-bucket/tpcds/sf10 \
  --scale-factor 10 \
  --formats parquet,lance
```

Or use the script with env vars:

```bash
DATA_DIR=s3a://my-bucket/tpcds/sf10 \
./benchmark/scripts/generate-data.sh 10 parquet,lance local[*]
```

### 3. Run benchmark queries on the cluster

```bash
spark-submit \
  --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
  --master spark://cluster-master:7077 \
  --deploy-mode client \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 8 \
  --jars path/to/lance-spark-bundle-3.5_2.12-*.jar \
  --conf spark.sql.extensions=org.lance.spark.LanceSparkSessionExtension \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET \
  benchmark/target/lance-spark-benchmark-*.jar \
  --data-dir s3a://my-bucket/tpcds/sf10 \
  --results-dir s3a://my-bucket/tpcds/sf10/results \
  --formats parquet,lance \
  --iterations 3 \
  --explain \
  --metrics
```

For YARN:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  ...
```

For GCS, use `--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/path/to/key.json` and `gs://` paths.

## Output

Results are written to a timestamped CSV file (e.g. `tpcds_20260318_062221.csv`) and a summary table is printed to stdout:

```
Query     parquet(ms)    lance(ms)      Ratio   Status
-------------------------------------------------------
q1               2505         1529       1.64x     PASS
q3                796          756       1.05x     PASS
q4               6426        14229       0.45x     PASS
...
Geometric mean ratio (parquet/lance): 0.87x
Queries passed: 75, partial/failed: 30
Row count validation: all matching
```

When `--metrics` is enabled, extra columns appear (CPU time, bytes read, shuffle).

## Project Structure

```
benchmark/
├── scripts/
│   ├── generate-data.sh          # Data generation via Spark + Kyuubi connector
│   └── run-benchmark.sh          # Query runner
├── src/main/java/org/lance/spark/benchmark/
│   ├── TpcdsDataGenerator.java   # Spark job: Kyuubi TPC-DS → Lance/Parquet
│   ├── TpcdsBenchmarkRunner.java # Main entry point for query benchmarking
│   ├── TpcdsDataLoader.java      # Register pre-generated tables as temp views
│   ├── TpcdsQueryRunner.java     # Query execution loop
│   ├── BenchmarkResult.java      # Per-query result record
│   ├── BenchmarkReporter.java    # CSV + summary output
│   ├── QueryMetrics.java         # Per-query task-level metrics
│   └── QueryMetricsListener.java # SparkListener for metrics collection
├── src/main/resources/tpcds-queries/
│   └── q1.sql ... q99.sql        # 103 TPC-DS query files
├── data/                         # Generated data (gitignored)
├── results/                      # Output CSVs (gitignored)
└── pom.xml
```
