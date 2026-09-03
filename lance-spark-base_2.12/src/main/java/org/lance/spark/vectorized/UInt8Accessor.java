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

import org.apache.arrow.vector.UInt8Vector;

/**
 * Accessor for unsigned 64-bit integers (UInt8). Maps to Spark LongType (may overflow for values
 * &gt; Long.MAX_VALUE, but no better option).
 */
public class UInt8Accessor {
  private final UInt8Vector accessor;

  UInt8Accessor(UInt8Vector vector) {
    this.accessor = vector;
  }

  final long getLong(int rowId) {
    // Read the raw two's-complement long rather than boxing through getObjectNoOverflow(), which
    // returns an unsigned BigInteger: longValueExact() then throws for every value at or above
    // 2^63 instead of wrapping as this accessor documents, and allocates per row. Null slots are
    // guarded because Arrow's get() rejects them when null checking is on, which is the default.
    return accessor.isNull(rowId) ? 0L : accessor.get(rowId);
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
