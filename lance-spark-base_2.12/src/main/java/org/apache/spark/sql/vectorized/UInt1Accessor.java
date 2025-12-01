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

import org.apache.arrow.vector.UInt1Vector;

/**
 * Accessor for unsigned 8-bit integers (UInt1). Maps to Spark ShortType (signed 16-bit can hold all
 * UInt8 values 0-255).
 */
public class UInt1Accessor {
  private final UInt1Vector accessor;

  UInt1Accessor(UInt1Vector vector) {
    this.accessor = vector;
  }

  final short getShort(int rowId) {
    return (short) Byte.toUnsignedInt(accessor.get(rowId));
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
