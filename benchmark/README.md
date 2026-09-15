# Lance Spark Benchmarks

Runs the [TPC-DS](http://www.tpc.org/tpcds/) and [TPC-H](http://www.tpc.org/tpch/) query suites against Lance and Parquet formats using Apache Spark, comparing query performance, correctness, and resource usage.

`parquet` here refers to Spark's built-in Parquet reader, used as a performance baseline.

## Architecture

Data generation uses Apache Kyuubi's [TPC-DS](https://kyuubi.readthedocs.io/en/master/connector/spark/tpcds.html) and [TPC-H](https://kyuubi.readthedocs.io/en/master/connector/spark/tpch.html) connectors — pure-Spark data sources that generate test data in parallel across executors. No external `dsdgen`/`dbgen` binaries, CSV files, or intermediate formats are needed — data is written directly into the target format (Lance, Parquet, etc.) and can target any Spark-supported storage (local, S3, GCS, HDFS).

Each benchmark is split into two independent Spark jobs:

1. **Data generator** — Reads from the Kyuubi catalog and writes all tables directly into each target format (24 tables for TPC-DS, 8 tables for TPC-H).
2. **Benchmark runner** — Registers the generated tables and runs the full query suite, comparing formats (99 queries for TPC-DS, 22 for TPC-H).

Both benchmarks share the same reporting infrastructure (`BenchmarkResult`, `BenchmarkReporter`, `QueryMetrics`, `QueryMetricsListener`) and produce output in the same CSV/summary format, so results are directly comparable across workloads.

## FragmentSlice physical-read benchmark

`FragmentSliceBenchmark` is a focused local benchmark for ZoneMap-driven physical row-range
pruning. It creates one large, physically ordered Lance fragment, builds a ZoneMap, and compares
four Java Scanner modes: with and without a physical slice, each with scalar-index execution enabled
and disabled. The primary signal is native `ScanStats.bytesRead`; elapsed time is secondary and is
expected to be noisier on a local filesystem.

Run the default workload (4 million rows, approximately 1 GB of payload):

```bash
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
./benchmark/scripts/run-fragment-slice-benchmark.sh
```

Run a small smoke workload, rebuilding the dataset if it already exists:

```bash
ROWS=65536 PAYLOAD_BYTES=64 ROWS_PER_ZONE=4096 \
  WARMUPS=0 ITERATIONS=1 REGENERATE=true \
  ./benchmark/scripts/run-fragment-slice-benchmark.sh
```

Configuration is provided through environment variables:

| Variable | Default | Description |
|---|---:|---|
| `DATA_DIR` | `benchmark/data/fragment-slice` | Parent directory of the generated dataset |
| `RESULTS_DIR` | `benchmark/results` | Local directory for result CSV files |
| `ROWS` | `4000000` | Physical rows in the single fragment |
| `PAYLOAD_BYTES` | `256` | Deterministic payload width per row |
| `ROWS_PER_ZONE` | `8192` | ZoneMap rows per zone and selected slice size |
| `WARMUPS` | `1` | Warmup passes over all four modes |
| `ITERATIONS` | `5` | Recorded passes over all four modes |
| `REGENERATE` | `false` | Overwrite an existing benchmark dataset |
| `MAVEN_REPO_LOCAL` | unset | Maven repository containing a locally built Lance artifact |

The runner verifies that all modes return the same row count and reports the planned row ratio,
median elapsed time, bytes read, requests, I/O operations, and index loads. A byte reduction of at
least 80% with scalar indexes disabled is printed as the initial physical-pruning target.

### TODO
- **Delta / Iceberg support** — not yet included because they require additional catalog/metastore tooling outside lance-spark's scope.

## Data Layout

The default data directories are isolated between the two benchmarks:

```
benchmark/data/          # TPC-DS tables (default DATA_DIR for TPC-DS scripts)
benchmark/data/tpch/     # TPC-H tables (default DATA_DIR for TPC-H scripts)
benchmark/results/       # tpcds_*.csv AND tpch_*.csv (shared)
```

**Why the isolation matters**: TPC-DS and TPC-H both define a `customer` table with *different* schemas. If both benchmarks shared one data directory, `createOrReplaceTempView("customer", ...)` would silently overwrite one with the other between runs, causing cryptic query failures. If you override `DATA_DIR`, use distinct paths for the two benchmarks to preserve this isolation.

The shared `benchmark/results/` directory is fine — CSV files are prefixed `tpcds_*.csv` or `tpch_*.csv` so they never collide.

## Running TPC-DS

### Quick Start

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

## Running TPC-H

### Quick Start

```bash
# Build the jars first (shared with TPC-DS)
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build

# End-to-end: generate data + run queries
make benchmark-tpch SF=10 FORMATS=parquet,lance ITERATIONS=3
```

Or run the two phases separately:

```bash
# Step 1: Generate TPC-H data (parallel Spark job)
./benchmark/scripts/generate-tpch-data.sh [SCALE_FACTOR] [FORMATS] [SPARK_MASTER]

# Step 2: Run benchmark queries against generated data
./benchmark/scripts/run-tpch-benchmark.sh [FORMATS] [SPARK_MASTER] [ITERATIONS]
```

### Examples

```bash
# Generate SF=10 data in both formats
./benchmark/scripts/generate-tpch-data.sh 10 parquet,lance local[*]

# Run queries with profiling
EXPLAIN=true METRICS=true ./benchmark/scripts/run-tpch-benchmark.sh parquet,lance local[*] 3

# Run a subset of queries
QUERIES=q1,q6,q14 ./benchmark/scripts/run-tpch-benchmark.sh lance local[*] 1

# Override default data dir (e.g. to avoid the default benchmark/data/tpch location)
DATA_DIR=/tmp/tpch-sf10 ./benchmark/scripts/generate-tpch-data.sh 10

# Override Spark/Scala versions
SPARK_VERSION=4.0 SCALA_VERSION=2.13 ./benchmark/scripts/generate-tpch-data.sh 10
```

TPC-H query files (`benchmark/src/main/resources/tpch-queries/q1.sql` through `q22.sql`) are derived from Apache Kyuubi's `kyuubi-spark-connector-tpch` test resources (Apache 2.0 licensed).

### Using the Docker environment

If you don't have Spark installed locally, use the existing `spark-lance` Docker container. The benchmark jar, data, and results directories are volume-mounted automatically — the same container and mounts work for both TPC-DS and TPC-H.

```bash
# Build jars on the host
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build

# Start the Spark container
make docker-up

# Generate TPC-DS data (inside the container)
docker exec spark-lance spark-submit \
  --class org.lance.spark.benchmark.TpcdsDataGenerator \
  --master local[*] \
  /home/lance/benchmark/lance-spark-benchmark-0.3.0-beta.1.jar \
  --data-dir /home/lance/data --scale-factor 1 --formats parquet,lance

# Run TPC-DS benchmark queries (inside the container)
docker exec spark-lance spark-submit \
  --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
  --master local[*] \
  /home/lance/benchmark/lance-spark-benchmark-0.3.0-beta.1.jar \
  --data-dir /home/lance/data --results-dir /home/lance/results \
  --formats parquet,lance --iterations 3

# Generate TPC-H data (inside the container, note the /tpch subdir)
docker exec spark-lance spark-submit \
  --class org.lance.spark.benchmark.TpchDataGenerator \
  --master local[*] \
  /home/lance/benchmark/lance-spark-benchmark-0.3.0-beta.1.jar \
  --data-dir /home/lance/data/tpch --scale-factor 1 --formats parquet,lance

# Run TPC-H benchmark queries (inside the container)
docker exec spark-lance spark-submit \
  --class org.lance.spark.benchmark.TpchBenchmarkRunner \
  --master local[*] \
  /home/lance/benchmark/lance-spark-benchmark-0.3.0-beta.1.jar \
  --data-dir /home/lance/data/tpch --results-dir /home/lance/results \
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

Both flags work for TPC-DS and TPC-H runners.

## Running on an External Cluster

To benchmark against a standalone or YARN cluster with data in an object store (S3, GCS, HDFS), the same `spark-submit` patterns apply — only the runner class differs between TPC-DS and TPC-H.

### 1. Build the jars

```bash
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build
```

### 2. Generate data directly to object store

Data generation is a Spark job — it writes directly to any Spark-supported storage. Use `TpcdsDataGenerator` or `TpchDataGenerator` depending on the workload:

```bash
# TPC-DS (SF=10)
spark-submit \
  --class org.lance.spark.benchmark.TpcdsDataGenerator \
  --master local[*] \
  --driver-memory 8g \
  --jars path/to/lance-spark-bundle-3.5_2.12-*.jar \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET \
  benchmark/target/lance-spark-benchmark-*.jar \
  --data-dir s3a://my-bucket/tpcds/sf10 \
  --scale-factor 10 \
  --formats parquet,lance

# TPC-H (SF=10) — note distinct bucket path to avoid `customer` collision
spark-submit \
  --class org.lance.spark.benchmark.TpchDataGenerator \
  --master local[*] \
  --driver-memory 8g \
  --jars path/to/lance-spark-bundle-3.5_2.12-*.jar \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET \
  benchmark/target/lance-spark-benchmark-*.jar \
  --data-dir s3a://my-bucket/tpch/sf10 \
  --scale-factor 10 \
  --formats parquet,lance
```

Or use the wrapper scripts with env vars:

```bash
# TPC-DS
DATA_DIR=s3a://my-bucket/tpcds/sf10 \
./benchmark/scripts/generate-data.sh 10 parquet,lance local[*]

# TPC-H
DATA_DIR=s3a://my-bucket/tpch/sf10 \
./benchmark/scripts/generate-tpch-data.sh 10 parquet,lance local[*]
```

### 3. Run benchmark queries on the cluster

```bash
# TPC-DS
spark-submit \
  --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
  --master spark://cluster-master:7077 \
  --deploy-mode client \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 8 \
  --jars path/to/lance-spark-bundle-3.5_2.12-*.jar \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
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

# TPC-H — identical submit flags, just a different class and paths
spark-submit \
  --class org.lance.spark.benchmark.TpchBenchmarkRunner \
  --master spark://cluster-master:7077 \
  --deploy-mode client \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 8 \
  --jars path/to/lance-spark-bundle-3.5_2.12-*.jar \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET \
  benchmark/target/lance-spark-benchmark-*.jar \
  --data-dir s3a://my-bucket/tpch/sf10 \
  --results-dir s3a://my-bucket/tpch/sf10/results \
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

Results are written to a timestamped CSV file (e.g. `tpcds_20260318_062221.csv` or `tpch_20260318_073015.csv`) and a summary table is printed to stdout:

```
=== TPC-H Benchmark Summary ===

Query     parquet(ms)    lance(ms)      Ratio   Status
-------------------------------------------------------
q1               2105         1890       1.11x     PASS
q3                756          712       1.06x     PASS
q6                890          542       1.64x     PASS
...
Geometric mean ratio (parquet/lance): 0.91x
Queries passed: 22, partial/failed: 0
Row count validation: all matching
```

When `--metrics` is enabled, extra columns appear (CPU time, bytes read, shuffle).

## Project Structure

```
benchmark/
├── scripts/
│   ├── generate-data.sh             # TPC-DS data generation wrapper
│   ├── run-benchmark.sh             # TPC-DS query runner wrapper
│   ├── submit-datagen.sh            # TPC-DS cluster-ready data generation
│   ├── submit-benchmark.sh          # TPC-DS cluster-ready query runner
│   ├── generate-tpch-data.sh        # TPC-H data generation wrapper
│   ├── run-tpch-benchmark.sh        # TPC-H query runner wrapper
│   ├── submit-tpch-datagen.sh       # TPC-H cluster-ready data generation
│   └── submit-tpch-benchmark.sh     # TPC-H cluster-ready query runner
├── src/main/java/org/lance/spark/benchmark/
│   ├── TpcdsDataGenerator.java      # Spark job: Kyuubi TPC-DS → Lance/Parquet
│   ├── TpcdsBenchmarkRunner.java    # TPC-DS query benchmarking entry point
│   ├── TpcdsDataLoader.java         # Register pre-generated TPC-DS tables
│   ├── TpcdsQueryRunner.java        # TPC-DS query execution loop
│   ├── TpchDataGenerator.java       # Spark job: Kyuubi TPC-H → Lance/Parquet
│   ├── TpchBenchmarkRunner.java     # TPC-H query benchmarking entry point
│   ├── TpchDataLoader.java          # Register pre-generated TPC-H tables
│   ├── TpchQueryRunner.java         # TPC-H query execution loop
│   ├── BenchmarkResult.java         # Per-query result record (shared)
│   ├── BenchmarkReporter.java       # CSV + summary output (shared)
│   ├── QueryMetrics.java            # Per-query task-level metrics (shared)
│   └── QueryMetricsListener.java    # SparkListener for metrics (shared)
├── src/main/resources/
│   ├── tpcds-queries/
│   │   └── q1.sql ... q99.sql       # 103 TPC-DS query files
│   └── tpch-queries/
│       └── q1.sql ... q22.sql       # 22 TPC-H query files
├── data/                            # Generated data (gitignored)
│   └── tpch/                        # TPC-H default subdirectory (isolated)
├── results/                         # Output CSVs (gitignored)
└── pom.xml
```
