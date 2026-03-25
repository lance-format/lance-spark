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
INSTALL_DIR="${1:-${SCRIPT_DIR}/../tools/tpcds-kit}"

echo "=== Installing dsdgen from gregrahn/tpcds-kit ==="

if [ -x "${INSTALL_DIR}/tools/dsdgen" ]; then
  echo "dsdgen already installed at ${INSTALL_DIR}/tools/dsdgen"
  exit 0
fi

for cmd in git gcc make; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: $cmd is required but not found. Please install it."
    exit 1
  fi
done

mkdir -p "$(dirname "${INSTALL_DIR}")"

if [ ! -d "${INSTALL_DIR}" ]; then
  echo "Cloning tpcds-kit..."
  git clone --depth 1 https://github.com/gregrahn/tpcds-kit.git "${INSTALL_DIR}"
fi

echo "Building dsdgen..."
cd "${INSTALL_DIR}/tools"
make OS=LINUX

if [ -x "${INSTALL_DIR}/tools/dsdgen" ]; then
  echo "dsdgen built successfully at ${INSTALL_DIR}/tools/dsdgen"
else
  echo "ERROR: dsdgen build failed"
  exit 1
fi
