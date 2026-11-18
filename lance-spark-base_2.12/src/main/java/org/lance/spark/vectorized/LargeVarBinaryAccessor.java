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

import org.apache.arrow.vector.LargeVarBinaryVector;

/**
 * Accessor for large binary values (LargeBinary). Maps to Spark BinaryType. This accessor supports
 * byte arrays with 64-bit offsets, allowing for larger individual values and total buffer sizes
 * than the standard VarBinaryVector.
 *
 * <p>Handling LargeBinary here rather than delegating to Spark's {@code ArrowColumnVector} is
 * required on Spark 3.4, whose {@code ArrowUtils.fromArrowType} rejects LargeBinary with
 * UNSUPPORTED_ARROWTYPE. This mirrors {@link LargeVarCharAccessor}, which exists for the same
 * reason on LargeUtf8.
 */
public class LargeVarBinaryAccessor {
  private final LargeVarBinaryVector accessor;

  LargeVarBinaryAccessor(LargeVarBinaryVector vector) {
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
