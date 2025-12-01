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
package org.apache.spark.sql.vectorized;

import org.apache.arrow.vector.UInt2Vector;

/**
 * Accessor for unsigned 16-bit integers (UInt2). Maps to Spark IntegerType (signed 32-bit can hold
 * all UInt16 values 0-65535).
 */
public class UInt2Accessor {
  private final UInt2Vector accessor;

  UInt2Accessor(UInt2Vector vector) {
    this.accessor = vector;
  }

  final int getInt(int rowId) {
    // UInt2Vector.get() returns char which is already unsigned 16-bit
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
