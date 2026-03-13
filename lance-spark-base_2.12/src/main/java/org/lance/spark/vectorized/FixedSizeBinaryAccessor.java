/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.vectorized;

import org.apache.arrow.vector.FixedSizeBinaryVector;

/**
 * Accessor for FixedSizeBinary Arrow vectors. Maps to Spark BinaryType. FixedSizeBinary stores
 * fixed-length byte arrays (e.g., UUIDs, hashes).
 */
public class FixedSizeBinaryAccessor {
  private final FixedSizeBinaryVector accessor;

  FixedSizeBinaryAccessor(FixedSizeBinaryVector vector) {
    this.accessor = vector;
  }

  final byte[] getBinary(int rowId) {
    return accessor.get(rowId);
  }

  final boolean isNullAt(int rowId) {
    return accessor.isNull(rowId);
  }

  final int getNullCount() {
    return accessor.getNullCount();
  }

  final void close() {
    accessor.close();
  }
}
