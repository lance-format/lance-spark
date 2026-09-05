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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."
PROJECT_DIR="${BENCHMARK_DIR}/.."

SPARK_VERSION="${SPARK_VERSION:-3.5}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
DATA_DIR="${DATA_DIR:-${BENCHMARK_DIR}/data/fragment-slice}"
RESULTS_DIR="${RESULTS_DIR:-${BENCHMARK_DIR}/results}"
ROWS="${ROWS:-4000000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-256}"
ROWS_PER_ZONE="${ROWS_PER_ZONE:-8192}"
WARMUPS="${WARMUPS:-1}"
ITERATIONS="${ITERATIONS:-5}"

MAVEN_ARGS=()
if [ -n "${MAVEN_REPO_LOCAL:-}" ]; then
  MAVEN_ARGS+=("-Dmaven.repo.local=${MAVEN_REPO_LOCAL}")
fi

echo "=== FragmentSlice Benchmark ==="
echo "Spark master:    ${SPARK_MASTER}"
echo "Data dir:        ${DATA_DIR}"
echo "Results dir:     ${RESULTS_DIR}"
echo "Rows:            ${ROWS}"
echo "Payload bytes:   ${PAYLOAD_BYTES}"
echo "Rows per zone:   ${ROWS_PER_ZONE}"
echo "Warmups/runs:    ${WARMUPS}/${ITERATIONS}"
echo ""

BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark.jar"
if [ ! -f "${BENCHMARK_JAR}" ]; then
  echo "--- Building benchmark jar ---"
  (
    cd "${BENCHMARK_DIR}"
    ../mvnw package -DskipTests -q \
      -Dspark.compat.version="${SPARK_VERSION}" \
      -Dscala.compat.version="${SCALA_VERSION}" \
      "${MAVEN_ARGS[@]}"
  )
fi

BUNDLE_JAR=$(find "${PROJECT_DIR}" \
  -path "*/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}/target/lance-spark-bundle-*.jar" \
  -not -name "*sources*" -not -name "*javadoc*" -print -quit)
if [ -z "${BUNDLE_JAR}" ]; then
  echo "ERROR: lance-spark bundle jar not found." >&2
  echo "Build it first with:" >&2
  echo "  make bundle SPARK_VERSION=${SPARK_VERSION} SCALA_VERSION=${SCALA_VERSION}" >&2
  exit 1
fi

SPARK_SUBMIT="spark-submit"
if [ -n "${SPARK_HOME:-}" ]; then
  SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
fi

RUNNER_ARGS=(
  --data-dir "${DATA_DIR}"
  --results-dir "${RESULTS_DIR}"
  --rows "${ROWS}"
  --payload-bytes "${PAYLOAD_BYTES}"
  --rows-per-zone "${ROWS_PER_ZONE}"
  --warmups "${WARMUPS}"
  --iterations "${ITERATIONS}"
)
if [ "${REGENERATE:-false}" = true ]; then
  RUNNER_ARGS+=(--regenerate)
fi

mkdir -p "${RESULTS_DIR}"

"${SPARK_SUBMIT}" \
  --class org.lance.spark.benchmark.FragmentSliceBenchmark \
  --master "${SPARK_MASTER}" \
  --driver-memory "${DRIVER_MEMORY:-4g}" \
  --executor-memory "${EXECUTOR_MEMORY:-4g}" \
  --jars "${BUNDLE_JAR}" \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
  "${BENCHMARK_JAR}" \
  "${RUNNER_ARGS[@]}"
