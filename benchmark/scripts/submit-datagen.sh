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

# Submit TPC-DS data generation as a Spark job.
#
# Submits via spark-submit using whatever Spark configuration is already
# present (spark-defaults.conf, SPARK_HOME, etc.). Works with local mode,
# standalone clusters, YARN, and Kubernetes.
#
# The benchmark jar (shaded, includes Kyuubi TPC-DS connector) is built
# on-the-fly if not already present.
#
# Usage:
#   ./submit-datagen.sh --data-dir <path> [OPTIONS]
#
# Examples:
#   # Generate SF=1 in both formats to a local directory
#   ./submit-datagen.sh -d /tmp/tpcds/sf1
#
#   # Generate SF=10 Lance-only to an object store, 20 executors
#   ./submit-datagen.sh -d s3a://my-bucket/tpcds/sf10 -s 10 -f lance --max-executors 20
#
#   # Generate SF=100 with custom resources
#   ./submit-datagen.sh -d /data/tpcds/sf100 -s 100 --max-executors 50 --driver-memory 16g
#
#   # Build for Spark 4.0 / Scala 2.13
#   SPARK_VERSION=4.0 SCALA_VERSION=2.13 ./submit-datagen.sh -d /tmp/tpcds/sf1
#
#   # Use pre-built benchmark jar
#   ./submit-datagen.sh -d /data/tpcds/sf1 --benchmark-jar /path/to/benchmark.jar

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SCALE_FACTOR=1
FORMATS="parquet,lance"
DATA_DIR=""
MAX_EXECUTORS=10
DRIVER_MEMORY=""
EXECUTOR_MEMORY=""
EXECUTOR_CORES=""
BENCHMARK_JAR=""
APP_NAME=""
USE_DOUBLE_FOR_DECIMAL=""
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
Usage: submit-datagen.sh --data-dir <path> [OPTIONS]

Generate TPC-DS data via a Spark job using the Kyuubi TPC-DS connector.

Options:
    --data-dir, -d DIR          Output directory (REQUIRED)
                                Supports local paths, s3a://, gs://, abfss://, etc.
    --scale-factor, -s SF       TPC-DS scale factor (default: 1)
    --formats, -f FORMATS       Comma-separated formats (default: parquet,lance)
    --max-executors NUM         Dynamic allocation max executors (default: 10)
    --driver-memory MEM         Driver memory, e.g. 8g (default: from Spark config)
    --executor-memory MEM       Executor memory, e.g. 16g (default: from Spark config)
    --executor-cores NUM        Executor cores (default: from Spark config)
    --benchmark-jar JAR         Path to pre-built benchmark jar (auto-built if absent)
    --app-name NAME             Spark application name
    --use-double-for-decimal    Convert TPC-DS decimal columns to double
    --conf KEY=VALUE            Extra Spark conf (repeatable)
    -h, --help                  Show this help message

Environment variables:
    SPARK_VERSION               Spark major version for build (default: 3.5)
    SCALA_VERSION               Scala version for build (default: 2.12)
    SPARK_HOME                  Path to Spark installation

Examples:
    # Local SF=1, both formats
    ./submit-datagen.sh -d /tmp/tpcds/sf1

    # SF=10 to S3, Lance only, 20 executors
    ./submit-datagen.sh -d s3a://my-bucket/tpcds/sf10 -s 10 -f lance --max-executors 20

    # SF=100 with custom resources
    ./submit-datagen.sh -d /data/tpcds/sf100 -s 100 --max-executors 50 --driver-memory 16g

    # Build for Spark 4.0 / Scala 2.13
    SPARK_VERSION=4.0 SCALA_VERSION=2.13 ./submit-datagen.sh -d /tmp/tpcds/sf1
EOF
}

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-dir|-d)
            DATA_DIR="$2"; shift 2 ;;
        --scale-factor|-s)
            SCALE_FACTOR="$2"; shift 2 ;;
        --formats|-f)
            FORMATS="$2"; shift 2 ;;
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
        --use-double-for-decimal)
            USE_DOUBLE_FOR_DECIMAL="true"; shift ;;
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
if [[ -z "${DATA_DIR}" ]]; then
    echo "Error: --data-dir is required." >&2
    echo "" >&2
    show_help >&2
    exit 1
fi

if [[ -z "${APP_NAME}" ]]; then
    APP_NAME="${USER:-tpcds}-datagen-sf${SCALE_FACTOR}"
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
# Map SPARK_VERSION (major) to the full version used by the parent POM
case "${SPARK_VERSION}" in
    3.4) MVN_SPARK_VERSION="3.4.4" ;;
    3.5) MVN_SPARK_VERSION="3.5.5" ;;
    4.0) MVN_SPARK_VERSION="4.0.0" ;;
    4.1) MVN_SPARK_VERSION="4.0.0" ;;  # TODO: update when 4.1 releases
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
# Prefer a jar already in Spark's jars/ directory.
# Fall back to the locally-built bundle from the project tree.
BUNDLE_JAR=""

# Check Spark jars dir first
for f in "${SPARK_HOME}"/jars/lance-spark-bundle-*.jar; do
    if [[ -f "$f" ]]; then
        BUNDLE_JAR="$f"
        break
    fi
done

# Fall back to project-built bundle (match current Spark/Scala version)
if [[ -z "${BUNDLE_JAR}" ]]; then
    BUNDLE_JAR=$(find "${PROJECT_ROOT}" \
        -path "*/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}/target/lance-spark-bundle-*.jar" \
        -not -name "*sources*" -not -name "*javadoc*" 2>/dev/null | head -1)
fi

# ---------------------------------------------------------------------------
# Build spark-submit command
# ---------------------------------------------------------------------------
SUBMIT_ARGS=()

SUBMIT_ARGS+=("--class" "org.lance.spark.benchmark.TpcdsDataGenerator")
SUBMIT_ARGS+=("--name" "${APP_NAME}")

# Resource overrides (only set if user specified; otherwise inherit spark-defaults.conf)
if [[ -n "${DRIVER_MEMORY}" ]]; then
    SUBMIT_ARGS+=("--driver-memory" "${DRIVER_MEMORY}")
fi
if [[ -n "${EXECUTOR_MEMORY}" ]]; then
    SUBMIT_ARGS+=("--executor-memory" "${EXECUTOR_MEMORY}")
fi
if [[ -n "${EXECUTOR_CORES}" ]]; then
    SUBMIT_ARGS+=("--executor-cores" "${EXECUTOR_CORES}")
fi

# Dynamic allocation — inherit defaults but allow overriding max executors
SUBMIT_ARGS+=("--conf" "spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS}")

# Lance extension
SUBMIT_ARGS+=("--conf" "spark.sql.extensions=org.lance.spark.LanceSparkSessionExtension")

# Kyuubi TPC-DS catalog
SUBMIT_ARGS+=("--conf" "spark.sql.catalog.tpcds=org.apache.kyuubi.spark.connector.tpcds.TPCDSCatalog")

# Add bundle jar if not already in Spark jars dir
if [[ -n "${BUNDLE_JAR}" ]] && [[ "${BUNDLE_JAR}" != "${SPARK_HOME}"/jars/* ]]; then
    SUBMIT_ARGS+=("--jars" "${BUNDLE_JAR}")
fi

# Extra user confs
if [[ ${#EXTRA_CONF[@]} -gt 0 ]]; then
    SUBMIT_ARGS+=("${EXTRA_CONF[@]}")
fi

# The benchmark jar (application jar)
SUBMIT_ARGS+=("${BENCHMARK_JAR}")

# Application arguments (passed to TpcdsDataGenerator.main)
SUBMIT_ARGS+=("--data-dir" "${DATA_DIR}")
SUBMIT_ARGS+=("--scale-factor" "${SCALE_FACTOR}")
SUBMIT_ARGS+=("--formats" "${FORMATS}")

if [[ -n "${USE_DOUBLE_FOR_DECIMAL}" ]]; then
    SUBMIT_ARGS+=("--use-double-for-decimal")
fi

# ---------------------------------------------------------------------------
# Print summary and submit
# ---------------------------------------------------------------------------
echo "============================================="
echo "  TPC-DS Data Generation — Spark Submit"
echo "============================================="
echo "Scale factor:    ${SCALE_FACTOR}"
echo "Formats:         ${FORMATS}"
echo "Data dir:        ${DATA_DIR}"
echo "Max executors:   ${MAX_EXECUTORS}"
echo "Spark version:   ${SPARK_VERSION}"
echo "Scala version:   ${SCALA_VERSION}"
echo "Spark home:      ${SPARK_HOME}"
echo "Benchmark jar:   ${BENCHMARK_JAR}"
if [[ -n "${BUNDLE_JAR}" ]]; then
    echo "Bundle jar:      ${BUNDLE_JAR}"
fi
echo "App name:        ${APP_NAME}"
echo "============================================="
echo ""

exec "${SPARK_SUBMIT}" "${SUBMIT_ARGS[@]}"
