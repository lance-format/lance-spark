# TPC-DS Benchmark for Lance Spark

Runs the [TPC-DS](http://www.tpc.org/tpcds/) query suite against Lance and Parquet formats using Apache Spark, comparing query performance, correctness, and resource usage.

`parquet` here refers to Spark's built-in Parquet reader, used as a performance baseline.

### TODO
- **Delta / Iceberg support** — not yet included because they require additional catalog/metastore tooling outside lance-spark's scope.

## Quick Start (Docker)

The Docker-based runner is useful for local debugging (no host Spark or dsdgen installation required) and CI integration.

```bash
# Compare Lance vs Parquet (default), scale factor 1
make benchmark-docker

# Lance only, with profiling
make benchmark-docker FORMATS=lance

# Rebuild everything from scratch
make benchmark-docker SF=10 FORMATS=parquet,lance ITERATIONS=3
```

Or call the script directly for additional flags:

```bash
./benchmark/scripts/run-docker-benchmark.sh --formats lance --explain --metrics

# Run a single query with profiling
./benchmark/scripts/run-docker-benchmark.sh --formats parquet,lance --queries q3 --explain --metrics
```

### Docker Options

| Flag | Description | Default |
|------|-------------|---------|
| `--sf <n>` | TPC-DS scale factor | `1` |
| `--formats <list>` | Comma-separated formats to benchmark | `parquet,lance` |
| `--iterations <n>` | Iterations per query | `1` |
| `--queries <list>` | Comma-separated query names (e.g. `q3,q14a,q55`) | all |
| `--memory <size>` | Spark driver memory | `4g` |
| `--rebuild` | Force rebuild of jars and Docker image | off |
| `--explain` | Print `EXPLAIN EXTENDED` for each query (first iteration) | off |
| `--metrics` | Capture per-task metrics (CPU, GC, I/O, shuffle) | off |
| `--ui` | Enable Spark UI on `localhost:4040` | off |

### Profiling Features

**`--explain`** prints the Spark query plan before executing each query (first iteration only), useful for verifying filter/projection pushdown.

**`--metrics`** registers a `SparkListener` that captures per-task stats:
```
  [OK] q3 iter=1 time=822ms rows=89
       Metrics: tasks=12 cpu=680ms gc=15ms read=45MB shuffle_r=2MB shuffle_w=2MB
```

**`--ui`** exposes the Spark web UI at `http://localhost:4040` while the benchmark runs, with event logging enabled. Useful for inspecting stage DAGs, task timelines, and storage details.

## Running Without Docker

For running directly on a machine with Spark installed:

```bash
# Build the jars first
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build

# Run the benchmark
./benchmark/scripts/run-benchmark.sh [SCALE_FACTOR] [FORMATS] [SPARK_MASTER] [ITERATIONS]

# Examples
./benchmark/scripts/run-benchmark.sh 1 lance,parquet local[*] 3

# Override Spark/Scala versions
SPARK_VERSION=3.5 SCALA_VERSION=2.12 ./benchmark/scripts/run-benchmark.sh 1

# Enable profiling flags
EXPLAIN=true METRICS=true QUERIES=q3,q55 ./benchmark/scripts/run-benchmark.sh 1
```

Set `SPARK_HOME` if `spark-submit` is not on your `PATH`.

## Running on an External Cluster

To benchmark against a standalone or YARN cluster with data in an object store (S3, GCS, HDFS):

### 1. Build the jars

```bash
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build
```

### 2. Generate TPC-DS data and write directly to object store

Use `spark-submit` with object store paths so data is written directly — no local generation and upload step needed:

```bash
spark-submit \
  --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
  --master local[*] \
  --jars path/to/lance-spark-bundle-3.5_2.12-*.jar \
  --conf spark.sql.extensions=org.lance.spark.LanceSparkSessionExtension \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET \
  --conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
  benchmark/target/lance-spark-benchmark-*.jar \
  --raw-data /tmp/tpcds-raw \
  --data-dir s3a://my-bucket/tpcds/sf10 \
  --results-dir s3a://my-bucket/tpcds/sf10/results \
  --formats parquet,lance \
  --iterations 1
```

Alternatively, use the script with object store env vars:

```bash
DATA_DIR=s3a://my-bucket/tpcds/sf10 \
RESULTS_DIR=s3a://my-bucket/tpcds/sf10/results \
./benchmark/scripts/run-benchmark.sh 10 parquet,lance local[*] 1
```

### 3. Submit to the cluster

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
  --conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
  benchmark/target/lance-spark-benchmark-*.jar \
  --raw-data s3a://my-bucket/tpcds/sf10/raw \
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
│   ├── run-docker-benchmark.sh   # Docker-based runner (recommended)
│   ├── run-benchmark.sh          # Local runner (requires Spark installed)
│   ├── generate-data.sh          # TPC-DS data generation
│   └── install-dsdgen.sh         # dsdgen build script
├── src/main/java/org/lance/spark/benchmark/
│   ├── TpcdsBenchmarkRunner.java # Main entry point, arg parsing
│   ├── TpcdsQueryRunner.java     # Query execution loop
│   ├── TpcdsDataLoader.java      # CSV → Lance/Parquet conversion
│   ├── TpcdsSchemaDefinition.java# Table schemas (24 tables)
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
