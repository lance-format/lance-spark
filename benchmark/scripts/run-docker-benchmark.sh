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

# TPC-DS Benchmark runner for lance-spark (Docker-based).
#
# Runs benchmarks inside a Docker container, useful for local debugging
# (no host Spark or dsdgen installation required) and CI integration.
#
# Usage:
#   ./benchmark/scripts/run-docker-benchmark.sh [OPTIONS]
#
# Options:
#   --sf <n>           Scale factor (default: 1)
#   --formats <list>   Comma-separated formats (default: parquet,lance)
#   --iterations <n>   Iterations per query (default: 1)
#   --rebuild          Force rebuild of benchmark jar and Docker image
#   --explain          Print EXPLAIN EXTENDED for each query (first iteration)
#   --metrics          Capture per-stage task metrics via SparkListener
#   --queries <list>   Comma-separated query names to run (default: all)
#   --ui               Enable Spark UI on port 4040
#   --help             Show this help
#
# Examples:
#   ./benchmark/scripts/run-docker-benchmark.sh
#   ./benchmark/scripts/run-docker-benchmark.sh --sf 1 --formats parquet,lance --iterations 3
#   ./benchmark/scripts/run-docker-benchmark.sh --rebuild --sf 10
#   ./benchmark/scripts/run-docker-benchmark.sh --rebuild --formats parquet --explain --metrics
#   ./benchmark/scripts/run-docker-benchmark.sh --formats parquet --ui

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BENCHMARK_DIR="${PROJECT_ROOT}/benchmark"
DOCKER_DIR="${PROJECT_ROOT}/docker"

# Defaults
SF=1
FORMATS="parquet,lance"
ITERATIONS=1
REBUILD=false
SPARK_VERSION=3.5
SCALA_VERSION=2.12
DOCKER_IMAGE="lance-spark-benchmark:latest"
DRIVER_MEMORY="4g"
EXPLAIN=false
METRICS=false
QUERIES=""
UI=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sf)          SF="$2"; shift 2 ;;
    --formats)     FORMATS="$2"; shift 2 ;;
    --iterations)  ITERATIONS="$2"; shift 2 ;;
    --rebuild)     REBUILD=true; shift ;;
    --memory)      DRIVER_MEMORY="$2"; shift 2 ;;
    --explain)     EXPLAIN=true; shift ;;
    --metrics)     METRICS=true; shift ;;
    --queries)     QUERIES="$2"; shift 2 ;;
    --ui)          UI=true; shift ;;
    --help)
      head -35 "$0" | grep "^#" | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

echo "=== TPC-DS Benchmark ==="
echo "  Scale factor:  ${SF}"
echo "  Formats:       ${FORMATS}"
echo "  Iterations:    ${ITERATIONS}"
echo "  Driver memory: ${DRIVER_MEMORY}"
if [ "${EXPLAIN}" = true ]; then echo "  Explain:       enabled"; fi
if [ "${METRICS}" = true ]; then echo "  Metrics:       enabled"; fi
if [ "${UI}" = true ]; then echo "  Spark UI:      enabled (port 4040)"; fi
echo ""

# --- Step 1: Build bundle if needed ---
BUNDLE_DIR="${PROJECT_ROOT}/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}"
BUNDLE_JAR=$(find "${BUNDLE_DIR}/target" -name "lance-spark-bundle-*.jar" \
  -not -name "*sources*" -not -name "*javadoc*" 2>/dev/null | head -1 || true)

if [ -z "${BUNDLE_JAR}" ]; then
  echo ">>> Building lance-spark bundle..."
  cd "${PROJECT_ROOT}"
  make bundle SPARK_VERSION="${SPARK_VERSION}" SCALA_VERSION="${SCALA_VERSION}"
  BUNDLE_JAR=$(find "${BUNDLE_DIR}/target" -name "lance-spark-bundle-*.jar" \
    -not -name "*sources*" -not -name "*javadoc*" | head -1)
fi
echo ">>> Bundle: $(basename "${BUNDLE_JAR}")"

# --- Step 2: Build benchmark jar if needed ---
BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark-0.3.0-beta.1.jar"
if [ "${REBUILD}" = true ] || [ ! -f "${BENCHMARK_JAR}" ]; then
  echo ">>> Building benchmark jar..."
  cd "${BENCHMARK_DIR}"
  mvn package -DskipTests -q
fi
echo ">>> Benchmark jar: $(basename "${BENCHMARK_JAR}")"

# --- Step 3: Build Docker image if needed ---
IMAGE_EXISTS=$(docker images -q "${DOCKER_IMAGE}" 2>/dev/null)
if [ "${REBUILD}" = true ] || [ -z "${IMAGE_EXISTS}" ]; then
  echo ">>> Building Docker image..."
  cp "${BUNDLE_JAR}" "${DOCKER_DIR}/"
  cp "${BENCHMARK_JAR}" "${DOCKER_DIR}/"
  cd "${DOCKER_DIR}"

  docker build \
    --build-arg SPARK_MAJOR_VERSION="${SPARK_VERSION}" \
    --build-arg SCALA_VERSION="${SCALA_VERSION}" \
    -f Dockerfile.benchmark \
    -t "${DOCKER_IMAGE}" \
    . 2>&1 | tail -5
fi
echo ">>> Docker image: ${DOCKER_IMAGE}"
echo ""

# --- Step 4: Run benchmark in Docker ---
# Clean up any leftover container from a previous run
docker rm -f lance-benchmark 2>/dev/null || true

# Persistent data directory on the host
DATA_DIR="${BENCHMARK_DIR}/data/sf${SF}"
mkdir -p "${DATA_DIR}"

echo ">>> Data directory: ${DATA_DIR}"
echo ">>> Starting benchmark container..."
echo ""

JAVA_OPTS="-XX:+IgnoreUnrecognizedVMOptions"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/java.lang=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/java.io=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/java.net=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/java.nio=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/java.util=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/sun.security.action=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
JAVA_OPTS="${JAVA_OPTS} -Djdk.reflect.useDirectMethodHandle=false"
JAVA_OPTS="${JAVA_OPTS} -Dio.netty.tryReflectionSetAccessible=true"

# Build docker run options
DOCKER_OPTS="--rm --name lance-benchmark --memory=8g -v ${DATA_DIR}:/home/lance/data"
if [ "${UI}" = true ]; then
  DOCKER_OPTS="${DOCKER_OPTS} -p 4040:4040"
fi

# Build Spark conf for UI
SPARK_UI_CONF="spark.ui.enabled=false"
SPARK_EVENT_LOG_CONF=""
if [ "${UI}" = true ]; then
  SPARK_UI_CONF="spark.ui.enabled=true"
  SPARK_EVENT_LOG_CONF="--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/home/lance/data/spark-events"
fi

# Build extra benchmark args
BENCHMARK_EXTRA_ARGS=""
if [ "${EXPLAIN}" = true ]; then
  BENCHMARK_EXTRA_ARGS="${BENCHMARK_EXTRA_ARGS} --explain"
fi
if [ "${METRICS}" = true ]; then
  BENCHMARK_EXTRA_ARGS="${BENCHMARK_EXTRA_ARGS} --metrics"
fi
if [ -n "${QUERIES}" ]; then
  BENCHMARK_EXTRA_ARGS="${BENCHMARK_EXTRA_ARGS} --queries ${QUERIES}"
fi

docker run ${DOCKER_OPTS} \
  "${DOCKER_IMAGE}" \
  bash -c "
    mkdir -p /home/lance/data/raw /home/lance/results /home/lance/data/spark-events

    # Skip data generation if raw data already exists
    if ls /home/lance/data/raw/*.dat 1>/dev/null 2>&1; then
      echo '=== TPC-DS data (SF=${SF}) already exists, skipping generation ==='
      echo \"Data: \$(ls /home/lance/data/raw/*.dat | wc -l) files, \$(du -sh /home/lance/data/raw/ | cut -f1)\"
    else
      echo '=== Generating TPC-DS data (SF=${SF}) ==='
      cd /opt/tpcds-kit/tools
      ./dsdgen -DIR /home/lance/data/raw -SCALE ${SF} -TERMINATE N -FORCE Y -VERBOSE Y
      echo ''
      echo \"Data: \$(ls /home/lance/data/raw/*.dat | wc -l) files, \$(du -sh /home/lance/data/raw/ | cut -f1)\"
    fi
    echo ''

    spark-submit \\
      --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \\
      --master 'local[*]' \\
      --driver-memory ${DRIVER_MEMORY} \\
      --conf 'spark.driver.extraJavaOptions=${JAVA_OPTS}' \\
      --conf ${SPARK_UI_CONF} \\
      ${SPARK_EVENT_LOG_CONF} \\
      --conf spark.sql.adaptive.enabled=true \\
      --conf spark.log.level=ERROR \\
      /home/lance/benchmark/lance-spark-benchmark-0.3.0-beta.1.jar \\
      --raw-data /home/lance/data/raw \\
      --data-dir /home/lance/data \\
      --results-dir /home/lance/results \\
      --formats '${FORMATS}' \\
      --iterations ${ITERATIONS}${BENCHMARK_EXTRA_ARGS} 2>/dev/null
  "
