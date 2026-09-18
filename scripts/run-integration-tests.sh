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
EXTRA_JARS_DIR="$ROOT/integration-tests/.jars"

needs_emulator() {
  local name="$1"
  local backends="${TEST_BACKENDS:-}"
  if [[ -z "$backends" ]]; then
    return 0
  fi
  case ",${backends}," in
    *",${name},"*) return 0 ;;
    *) return 1 ;;
  esac
}

if needs_emulator azurite && ! command -v azurite-blob >/dev/null; then
  echo "azurite-blob not on PATH (npm install -g azurite)" >&2
  exit 1
fi
if needs_emulator minio && ! command -v minio >/dev/null; then
  echo "minio not on PATH" >&2
  exit 1
fi

need_glue_jar=0
if [[ -z "${TEST_BACKENDS:-}" && -n "${AWS_S3_BUCKET_NAME:-}" ]]; then
  need_glue_jar=1
elif [[ -n "${TEST_BACKENDS:-}" ]]; then
  case ",${TEST_BACKENDS}," in
    *,glue,*) need_glue_jar=1 ;;
  esac
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
  make fetch-spark-dist SPARK_VERSION="$SPARK_VERSION" SCALA_VERSION="$SCALA_VERSION"
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

# Keep connector jars out of the Spark distro this runner reuses.
rm -f "$SPARK_HOME/jars/"lance-spark-bundle-*.jar \
  "$SPARK_HOME/jars/"lance-namespace-glue-*.jar

mkdir -p "$EXTRA_JARS_DIR"
rm -f "$EXTRA_JARS_DIR"/lance-spark-bundle-*.jar \
  "$EXTRA_JARS_DIR"/lance-namespace-glue-*.jar

bundle_jar="$(ls "$ROOT/$BUNDLE_MODULE/target/${BUNDLE_MODULE}-"*.jar | grep -v -E 'original-|sources|javadoc' | head -n 1)"
cp -f "$bundle_jar" "$EXTRA_JARS_DIR/"

LANCE_SPARK_JARS="$EXTRA_JARS_DIR/$(basename "$bundle_jar")"

if [[ "$need_glue_jar" -eq 1 ]]; then
  glue_cache_dir="$ROOT/integration-tests/.cache"
  mkdir -p "$glue_cache_dir"
  glue_jar="$glue_cache_dir/lance-namespace-glue-${LANCE_NAMESPACE_IMPL_VERSION}-bundle.jar"
  if [[ ! -s "$glue_jar" ]]; then
    glue_tmp="$glue_jar.part"
    curl -fL --retry 3 --retry-delay 5 -o "$glue_tmp" \
      "https://repo1.maven.org/maven2/org/lance/lance-namespace-glue/${LANCE_NAMESPACE_IMPL_VERSION}/lance-namespace-glue-${LANCE_NAMESPACE_IMPL_VERSION}-bundle.jar"
    mv "$glue_tmp" "$glue_jar"
  fi
  LANCE_SPARK_JARS="$LANCE_SPARK_JARS,$glue_jar"
fi
export LANCE_SPARK_JARS
export LANCE_SPARK_REST_CLASSPATH="$ROOT/integration-tests:$EXTRA_JARS_DIR/*:$SPARK_HOME/jars/*"

javac -cp "$SPARK_HOME/jars/*:$EXTRA_JARS_DIR/*" "$ROOT/integration-tests/LanceRestDirNamespaceServer.java"

export LANCE_SPARK_DATA_ROOT="${LANCE_SPARK_DATA_ROOT:-$ROOT/integration-tests/.data}"
export LANCE_SPARK_REST_DIR_ROOT="${LANCE_SPARK_REST_DIR_ROOT:-$ROOT/integration-tests/.rest-data}"
mkdir -p "$LANCE_SPARK_DATA_ROOT" "$LANCE_SPARK_REST_DIR_ROOT"

export PYSPARK_PYTHON="$VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VENV/bin/python"

bash -c "$PYTEST_CMD"
