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

# TPC-DS benchmark query runner.
#
# Tables must already exist under DATA_DIR/<format>/. Use generate-data.sh
# (or TpcdsDataGenerator) to create them first.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."

# Configurable Spark/Scala versions (override via environment)
SPARK_VERSION="${SPARK_VERSION:-3.5}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"

FORMATS="${1:-lance,parquet}"
SPARK_MASTER="${2:-local[*]}"
ITERATIONS="${3:-3}"

# Object store / external paths — default to local when unset
DATA_DIR="${DATA_DIR:-${BENCHMARK_DIR}/data}"
RESULTS_DIR="${RESULTS_DIR:-${BENCHMARK_DIR}/results}"

echo "=== TPC-DS Benchmark ==="
echo "Formats:         ${FORMATS}"
echo "Spark master:    ${SPARK_MASTER}"
echo "Iterations:      ${ITERATIONS}"
echo "Spark version:   ${SPARK_VERSION}"
echo "Scala version:   ${SCALA_VERSION}"
echo "Data dir:        ${DATA_DIR}"
echo "Results dir:     ${RESULTS_DIR}"
echo ""

# Step 1: Build benchmark jar if needed
echo "--- Step 1: Build benchmark jar ---"
BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark.jar"
if [ ! -f "${BENCHMARK_JAR}" ]; then
  cd "${BENCHMARK_DIR}"
  ../mvnw package -DskipTests -q \
    -Dspark.compat.version="${SPARK_VERSION}" \
    -Dscala.compat.version="${SCALA_VERSION}"
  cd "${SCRIPT_DIR}"
fi

# Step 2: Find the bundle jar
BUNDLE_JAR=$(find "${BENCHMARK_DIR}/.." -path "*/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}/target/lance-spark-bundle-*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)
if [ -z "${BUNDLE_JAR}" ]; then
  echo "WARNING: lance-spark bundle jar not found. Building it..."
  cd "${BENCHMARK_DIR}/.."
  make bundle SPARK_VERSION="${SPARK_VERSION}" SCALA_VERSION="${SCALA_VERSION}"
  BUNDLE_JAR=$(find "${BENCHMARK_DIR}/.." -path "*/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}/target/lance-spark-bundle-*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)
  cd "${SCRIPT_DIR}"
fi

# Step 3: Run benchmark via spark-submit
echo ""
echo "--- Step 3: Run benchmark ---"
mkdir -p "${RESULTS_DIR}"

SPARK_SUBMIT="spark-submit"
if [ -n "${SPARK_HOME:-}" ]; then
  SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
fi

# Build extra benchmark args from environment
BENCHMARK_EXTRA_ARGS=""
if [ "${EXPLAIN:-false}" = true ]; then
  BENCHMARK_EXTRA_ARGS="${BENCHMARK_EXTRA_ARGS} --explain"
fi
if [ "${METRICS:-false}" = true ]; then
  BENCHMARK_EXTRA_ARGS="${BENCHMARK_EXTRA_ARGS} --metrics"
fi
if [ -n "${QUERIES:-}" ]; then
  BENCHMARK_EXTRA_ARGS="${BENCHMARK_EXTRA_ARGS} --queries ${QUERIES}"
fi

${SPARK_SUBMIT} \
  --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
  --master "${SPARK_MASTER}" \
  --driver-memory "${DRIVER_MEMORY:-4g}" \
  --executor-memory "${EXECUTOR_MEMORY:-4g}" \
  --jars "${BUNDLE_JAR}" \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
  "${BENCHMARK_JAR}" \
  --data-dir "${DATA_DIR}" \
  --results-dir "${RESULTS_DIR}" \
  --formats "${FORMATS}" \
  --iterations "${ITERATIONS}"${BENCHMARK_EXTRA_ARGS}

echo ""
echo "=== Benchmark complete ==="
echo "Results saved to ${RESULTS_DIR}/"
