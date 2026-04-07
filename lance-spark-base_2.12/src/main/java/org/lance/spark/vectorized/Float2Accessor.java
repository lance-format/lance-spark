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

import org.lance.spark.utils.Float16Utils;

import org.apache.arrow.vector.ValueVector;

/**
 * Accessor for float16 (half-precision) vectors. Maps Arrow Float2Vector to Spark FloatType by
 * widening 16-bit half-precision floats to 32-bit single-precision floats.
 *
 * <p>Since Float2Vector is only available in Arrow 18+ (Spark 4.0+), this accessor uses the
 * ValueVector base class and accesses the underlying ArrowBuf directly to avoid compile-time
 * dependency.
 */
public class Float2Accessor {
  private final ValueVector accessor;

  Float2Accessor(ValueVector vector) {
    this.accessor = vector;
  }

  final float getFloat(int rowId) {
    // Float2Vector is a BaseFixedWidthVector with TYPE_WIDTH=2.
    // Element at rowId is at byte offset rowId * 2 in the data buffer.
    short halfBits = accessor.getDataBuffer().getShort((long) rowId * 2);
    return Float16Utils.halfToFloat(halfBits);
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
