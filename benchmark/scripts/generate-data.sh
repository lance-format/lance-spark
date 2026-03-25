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
SCALE_FACTOR="${1:-1}"
OUTPUT_DIR="${2:-${SCRIPT_DIR}/../data/raw}"
DSDGEN_DIR="${3:-${SCRIPT_DIR}/../tools/tpcds-kit/tools}"

echo "=== Generating TPC-DS data at SF=${SCALE_FACTOR} ==="

if [ ! -x "${DSDGEN_DIR}/dsdgen" ]; then
  echo "ERROR: dsdgen not found at ${DSDGEN_DIR}/dsdgen"
  echo "Run install-dsdgen.sh first."
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"

cd "${DSDGEN_DIR}"
./dsdgen \
  -DIR "${OUTPUT_DIR}" \
  -SCALE "${SCALE_FACTOR}" \
  -TERMINATE N \
  -FORCE Y

FILE_COUNT=$(find "${OUTPUT_DIR}" -name "*.dat" | wc -l)
echo "Generated ${FILE_COUNT} data files in ${OUTPUT_DIR}"
echo "Scale factor: ${SCALE_FACTOR}"
du -sh "${OUTPUT_DIR}"
