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

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

: "${SPARK_VERSION:?}"
: "${SCALA_VERSION:?}"
: "${SPARK_DOWNLOAD_VERSION:?}"
: "${BUNDLE_MODULE:?}"
: "${LANCE_NAMESPACE_IMPL_VERSION:?}"

PYTHON="${PYTHON:-python3}"
VENV="${INTEGRATION_VENV:-$ROOT/integration-tests/.venv}"
PYTEST_CMD="${INTEGRATION_PYTEST_CMD:-pytest integration-tests/ -v --timeout=180}"

if ! command -v azurite-blob >/dev/null; then
  echo "azurite-blob not on PATH (npm install -g azurite)" >&2
  exit 1
fi
if ! command -v minio >/dev/null; then
  echo "minio not on PATH" >&2
  exit 1
fi

"$PYTHON" -m venv "$VENV"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
python -m pip install --upgrade pip
python -m pip install -r integration-tests/requirements.txt "pyspark==${SPARK_DOWNLOAD_VERSION}"

PIP_SPARK_HOME="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"

# PyPI pyspark 3.x ships Scala 2.12 jars. Spark 3.x 2.13 bundles need the distro.
if [[ -n "${SPARK_SCALA_SUFFIX:-}" ]]; then
  : "${SPARK_DIST_TGZ:?}"
  make docker-fetch-spark SPARK_VERSION="$SPARK_VERSION" SCALA_VERSION="$SCALA_VERSION"
  SPARK_HOME_DIR="$ROOT/docker/.spark-home/${SPARK_DIST_TGZ%.tgz}"
  if [[ ! -x "$SPARK_HOME_DIR/bin/spark-submit" ]]; then
    rm -rf "$SPARK_HOME_DIR"
    mkdir -p "$SPARK_HOME_DIR"
    tar xzf "$ROOT/docker/.spark-cache/$SPARK_DIST_TGZ" -C "$SPARK_HOME_DIR" --strip-components 1
  fi
  export SPARK_HOME="$SPARK_HOME_DIR"
else
  export SPARK_HOME="$PIP_SPARK_HOME"
fi

if [[ ! -d "$SPARK_HOME/jars" ]]; then
  echo "SPARK_HOME=$SPARK_HOME has no jars/" >&2
  exit 1
fi

bundle_jar="$(ls "$ROOT/$BUNDLE_MODULE/target/${BUNDLE_MODULE}-"*.jar | grep -v -E 'original-|sources|javadoc' | head -n 1)"
cp -f "$bundle_jar" "$SPARK_HOME/jars/"

glue_jar="$SPARK_HOME/jars/lance-namespace-glue-${LANCE_NAMESPACE_IMPL_VERSION}-bundle.jar"
if [[ ! -f "$glue_jar" ]]; then
  curl -fL --retry 3 --retry-delay 5 -o "$glue_jar" \
    "https://repo1.maven.org/maven2/org/lance/lance-namespace-glue/${LANCE_NAMESPACE_IMPL_VERSION}/lance-namespace-glue-${LANCE_NAMESPACE_IMPL_VERSION}-bundle.jar"
fi

javac -cp "$SPARK_HOME/jars/*" "$ROOT/integration-tests/LanceRestDirNamespaceServer.java"

export LANCE_SPARK_DATA_ROOT="${LANCE_SPARK_DATA_ROOT:-$ROOT/integration-tests/.data}"
export LANCE_SPARK_REST_DIR_ROOT="${LANCE_SPARK_REST_DIR_ROOT:-$ROOT/integration-tests/.rest-data}"
mkdir -p "$LANCE_SPARK_DATA_ROOT" "$LANCE_SPARK_REST_DIR_ROOT"

export PYSPARK_PYTHON="$VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VENV/bin/python"

eval "$PYTEST_CMD"
