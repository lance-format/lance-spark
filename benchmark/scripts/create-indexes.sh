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

# Create btree indexes on TPC-DS dimension tables for zonemap-based
# fragment pruning and storage-partitioned join (SPJ) support.
#
# Run this after generate-data.sh and before run-benchmark.sh.
#
# Usage:
#   ./create-indexes.sh [SPARK_MASTER]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."

# Configurable Spark/Scala versions (override via environment)
SPARK_VERSION="${SPARK_VERSION:-3.5}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"

SPARK_MASTER="${1:-local[*]}"

# Object store / external paths — default to local when unset
DATA_DIR="${DATA_DIR:-${BENCHMARK_DIR}/data}"

echo "=== TPC-DS Index Creation ==="
echo "Spark master:    ${SPARK_MASTER}"
echo "Spark version:   ${SPARK_VERSION}"
echo "Scala version:   ${SCALA_VERSION}"
echo "Data dir:        ${DATA_DIR}"
echo ""

# Step 1: Build benchmark jar if needed
BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark.jar"
if [ ! -f "${BENCHMARK_JAR}" ]; then
  echo "--- Building benchmark jar (Spark ${SPARK_VERSION}, Scala ${SCALA_VERSION}) ---"
  cd "${BENCHMARK_DIR}"
  ../mvnw  package -DskipTests -q \
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

# Step 3: Create indexes via spark-submit
SPARK_SUBMIT="spark-submit"
if [ -n "${SPARK_HOME:-}" ]; then
  SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
fi
echo "BUNDLE_JAR ${BUNDLE_JAR}"
${SPARK_SUBMIT} \
  --class org.lance.spark.benchmark.TpcdsIndexBuilder \
  --master "${SPARK_MASTER}" \
  --driver-memory "${DRIVER_MEMORY:-4g}" \
  --executor-memory "${EXECUTOR_MEMORY:-4g}" \
  --jars "${BUNDLE_JAR}" \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
  "${BENCHMARK_JAR}" \
  --data-dir "${DATA_DIR}"

echo ""
echo "=== Index creation complete ==="
