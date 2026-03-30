#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Submit TPC-DS benchmark queries as a Spark job.
#
# Submits via spark-submit using whatever Spark configuration is already
# present (spark-defaults.conf, SPARK_HOME, etc.). Works with local mode,
# standalone clusters, YARN, and Kubernetes.
#
# Tables must already exist under <data-dir>/<format>/. Use submit-datagen.sh
# (or TpcdsDataGenerator) to create them first.
#
# The benchmark jar (shaded, includes Kyuubi TPC-DS connector) is built
# on-the-fly if not already present.
#
# Usage:
#   ./submit-benchmark.sh --data-dir <path> --results-dir <path> [OPTIONS]
#
# Examples:
#   # Run all 99 queries, 3 iterations, against both formats
#   ./submit-benchmark.sh -d /tmp/tpcds/sf1 -r /tmp/tpcds/results
#
#   # Lance only, with explain plans and metrics
#   ./submit-benchmark.sh -d /tmp/tpcds/sf1 -r /tmp/tpcds/results -f lance --explain --metrics
#
#   # Run specific queries
#   ./submit-benchmark.sh -d /tmp/tpcds/sf1 -r /tmp/tpcds/results --queries q3,q14a,q55
#
#   # Build for Spark 4.0 / Scala 2.13
#   SPARK_VERSION=4.0 SCALA_VERSION=2.13 ./submit-benchmark.sh -d /tmp/tpcds/sf1 -r /tmp/results

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
FORMATS="lance,parquet"
DATA_DIR=""
RESULTS_DIR=""
ITERATIONS=3
MAX_EXECUTORS=10
DRIVER_MEMORY=""
EXECUTOR_MEMORY=""
EXECUTOR_CORES=""
BENCHMARK_JAR=""
APP_NAME=""
EXPLAIN=""
METRICS=""
QUERIES=""
EXTRA_CONF=()

# Configurable Spark/Scala versions (override via environment)
SPARK_VERSION="${SPARK_VERSION:-3.5}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."
PROJECT_ROOT="${BENCHMARK_DIR}/.."

# Detect Spark home — check common locations
if [[ -z "${SPARK_HOME:-}" ]]; then
    for candidate in /opt/spark /usr/local/spark; do
        if [[ -x "${candidate}/bin/spark-submit" ]]; then
            SPARK_HOME="${candidate}"
            break
        fi
    done
    # Also search for versioned Spark dirs (e.g. /spark-4.0.1-bin-without-hadoop)
    if [[ -z "${SPARK_HOME:-}" ]]; then
        for candidate in /spark-*/bin/spark-submit; do
            if [[ -x "$candidate" ]]; then
                SPARK_HOME="$(dirname "$(dirname "$candidate")")"
                break
            fi
        done
    fi
fi
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
show_help() {
    cat <<'EOF'
Usage: submit-benchmark.sh --data-dir <path> --results-dir <path> [OPTIONS]

Run TPC-DS benchmark queries against pre-generated Lance and Parquet tables.

Options:
    --data-dir, -d DIR          Data directory with generated tables (REQUIRED)
    --results-dir, -r DIR       Output directory for CSV results (REQUIRED)
    --formats, -f FORMATS       Comma-separated formats (default: lance,parquet)
    --iterations, -i NUM        Iterations per query (default: 3)
    --max-executors NUM         Dynamic allocation max executors (default: 10)
    --driver-memory MEM         Driver memory, e.g. 8g (default: from Spark config)
    --executor-memory MEM       Executor memory, e.g. 16g (default: from Spark config)
    --executor-cores NUM        Executor cores (default: from Spark config)
    --benchmark-jar JAR         Path to pre-built benchmark jar (auto-built if absent)
    --app-name NAME             Spark application name
    --explain                   Print EXPLAIN plan for each query (first iteration)
    --metrics                   Collect per-query task-level metrics
    --queries QUERIES           Comma-separated query subset (e.g. q3,q14a,q55)
    --conf KEY=VALUE            Extra Spark conf (repeatable)
    -h, --help                  Show this help message

Environment variables:
    SPARK_VERSION               Spark major version for build (default: 3.5)
    SCALA_VERSION               Scala version for build (default: 2.12)
    SPARK_HOME                  Path to Spark installation

Examples:
    # Run all queries, 3 iterations, both formats
    ./submit-benchmark.sh -d /tmp/tpcds/sf1 -r /tmp/tpcds/results

    # Lance only, with profiling
    ./submit-benchmark.sh -d /tmp/tpcds/sf1 -r /tmp/results -f lance --explain --metrics

    # Specific queries, 1 iteration
    ./submit-benchmark.sh -d /tmp/tpcds/sf1 -r /tmp/results --queries q3,q55 -i 1

    # Build for Spark 4.0 / Scala 2.13
    SPARK_VERSION=4.0 SCALA_VERSION=2.13 ./submit-benchmark.sh -d /tmp/tpcds/sf1 -r /tmp/results
EOF
}

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-dir|-d)
            DATA_DIR="$2"; shift 2 ;;
        --results-dir|-r)
            RESULTS_DIR="$2"; shift 2 ;;
        --formats|-f)
            FORMATS="$2"; shift 2 ;;
        --iterations|-i)
            ITERATIONS="$2"; shift 2 ;;
        --max-executors)
            MAX_EXECUTORS="$2"; shift 2 ;;
        --driver-memory)
            DRIVER_MEMORY="$2"; shift 2 ;;
        --executor-memory)
            EXECUTOR_MEMORY="$2"; shift 2 ;;
        --executor-cores)
            EXECUTOR_CORES="$2"; shift 2 ;;
        --benchmark-jar)
            BENCHMARK_JAR="$2"; shift 2 ;;
        --app-name)
            APP_NAME="$2"; shift 2 ;;
        --explain)
            EXPLAIN="true"; shift ;;
        --metrics)
            METRICS="true"; shift ;;
        --queries)
            QUERIES="$2"; shift 2 ;;
        --conf)
            EXTRA_CONF+=("--conf" "$2"); shift 2 ;;
        -h|--help)
            show_help; exit 0 ;;
        *)
            echo "Error: unknown option $1" >&2
            show_help >&2
            exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Validate required arguments
# ---------------------------------------------------------------------------
if [[ -z "${DATA_DIR}" || -z "${RESULTS_DIR}" ]]; then
    echo "Error: --data-dir and --results-dir are both required." >&2
    echo "" >&2
    show_help >&2
    exit 1
fi

if [[ -z "${APP_NAME}" ]]; then
    APP_NAME="${USER:-tpcds}-benchmark"
fi

# ---------------------------------------------------------------------------
# Validate Spark
# ---------------------------------------------------------------------------
if [[ ! -x "${SPARK_SUBMIT}" ]]; then
    echo "Error: spark-submit not found at ${SPARK_SUBMIT}" >&2
    echo "Set SPARK_HOME or ensure Spark is installed." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Resolve Spark version for Maven build
# ---------------------------------------------------------------------------
case "${SPARK_VERSION}" in
    3.4) MVN_SPARK_VERSION="3.4.4" ;;
    3.5) MVN_SPARK_VERSION="3.5.5" ;;
    4.0) MVN_SPARK_VERSION="4.0.0" ;;
    4.1) MVN_SPARK_VERSION="4.0.0" ;;
    *)   MVN_SPARK_VERSION="${SPARK_VERSION}" ;;
esac

# ---------------------------------------------------------------------------
# Build benchmark jar if needed
# ---------------------------------------------------------------------------
if [[ -z "${BENCHMARK_JAR}" ]]; then
    BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark-0.3.0-beta.1.jar"
fi

if [[ ! -f "${BENCHMARK_JAR}" ]]; then
    echo "--- Building benchmark jar (Spark ${SPARK_VERSION}, Scala ${SCALA_VERSION}) ---"
    MVNW="${PROJECT_ROOT}/mvnw"
    if [[ ! -x "${MVNW}" ]]; then
        echo "Error: Maven wrapper not found at ${MVNW}" >&2
        echo "Run from the lance-spark project root, or pass --benchmark-jar." >&2
        exit 1
    fi
    (cd "${BENCHMARK_DIR}" && "${MVNW}" package -DskipTests -q \
        -Dspark.version="${MVN_SPARK_VERSION}" \
        -Dspark.compat.version="${SPARK_VERSION}" \
        -Dscala.compat.version="${SCALA_VERSION}")
    echo "Benchmark jar built: ${BENCHMARK_JAR}"
fi

if [[ ! -f "${BENCHMARK_JAR}" ]]; then
    echo "Error: benchmark jar not found at ${BENCHMARK_JAR}" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Find the lance-spark bundle jar
# ---------------------------------------------------------------------------
BUNDLE_JAR=""

for f in "${SPARK_HOME}"/jars/lance-spark-bundle-*.jar; do
    if [[ -f "$f" ]]; then
        BUNDLE_JAR="$f"
        break
    fi
done

if [[ -z "${BUNDLE_JAR}" ]]; then
    BUNDLE_JAR=$(find "${PROJECT_ROOT}" \
        -path "*/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}/target/lance-spark-bundle-*.jar" \
        -not -name "*sources*" -not -name "*javadoc*" 2>/dev/null | head -1)
fi

# ---------------------------------------------------------------------------
# Build spark-submit command
# ---------------------------------------------------------------------------
SUBMIT_ARGS=()

SUBMIT_ARGS+=("--class" "org.lance.spark.benchmark.TpcdsBenchmarkRunner")
SUBMIT_ARGS+=("--name" "${APP_NAME}")

if [[ -n "${DRIVER_MEMORY}" ]]; then
    SUBMIT_ARGS+=("--driver-memory" "${DRIVER_MEMORY}")
fi
if [[ -n "${EXECUTOR_MEMORY}" ]]; then
    SUBMIT_ARGS+=("--executor-memory" "${EXECUTOR_MEMORY}")
fi
if [[ -n "${EXECUTOR_CORES}" ]]; then
    SUBMIT_ARGS+=("--executor-cores" "${EXECUTOR_CORES}")
fi

SUBMIT_ARGS+=("--conf" "spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS}")
SUBMIT_ARGS+=("--conf" "spark.sql.extensions=org.lance.spark.LanceSparkSessionExtension")

if [[ -n "${BUNDLE_JAR}" ]] && [[ "${BUNDLE_JAR}" != "${SPARK_HOME}"/jars/* ]]; then
    SUBMIT_ARGS+=("--jars" "${BUNDLE_JAR}")
fi

if [[ ${#EXTRA_CONF[@]} -gt 0 ]]; then
    SUBMIT_ARGS+=("${EXTRA_CONF[@]}")
fi

# The benchmark jar (application jar)
SUBMIT_ARGS+=("${BENCHMARK_JAR}")

# Application arguments (passed to TpcdsBenchmarkRunner.main)
SUBMIT_ARGS+=("--data-dir" "${DATA_DIR}")
SUBMIT_ARGS+=("--results-dir" "${RESULTS_DIR}")
SUBMIT_ARGS+=("--formats" "${FORMATS}")
SUBMIT_ARGS+=("--iterations" "${ITERATIONS}")

if [[ -n "${EXPLAIN}" ]]; then
    SUBMIT_ARGS+=("--explain")
fi
if [[ -n "${METRICS}" ]]; then
    SUBMIT_ARGS+=("--metrics")
fi
if [[ -n "${QUERIES}" ]]; then
    SUBMIT_ARGS+=("--queries" "${QUERIES}")
fi

# ---------------------------------------------------------------------------
# Print summary and submit
# ---------------------------------------------------------------------------
echo "============================================="
echo "  TPC-DS Benchmark — Spark Submit"
echo "============================================="
echo "Formats:         ${FORMATS}"
echo "Iterations:      ${ITERATIONS}"
echo "Data dir:        ${DATA_DIR}"
echo "Results dir:     ${RESULTS_DIR}"
echo "Max executors:   ${MAX_EXECUTORS}"
echo "Spark version:   ${SPARK_VERSION}"
echo "Scala version:   ${SCALA_VERSION}"
echo "Spark home:      ${SPARK_HOME}"
echo "Benchmark jar:   ${BENCHMARK_JAR}"
if [[ -n "${BUNDLE_JAR}" ]]; then
    echo "Bundle jar:      ${BUNDLE_JAR}"
fi
if [[ -n "${QUERIES}" ]]; then
    echo "Queries:         ${QUERIES}"
fi
echo "Explain:         ${EXPLAIN:-false}"
echo "Metrics:         ${METRICS:-false}"
echo "App name:        ${APP_NAME}"
echo "============================================="
echo ""

exec "${SPARK_SUBMIT}" "${SUBMIT_ARGS[@]}"
