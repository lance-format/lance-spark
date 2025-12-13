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

import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Accessor for large UTF-8 strings (LargeUtf8). Maps to Spark StringType. This accessor supports
 * strings with 64-bit offsets, allowing for larger individual strings and total buffer sizes than
 * the standard VarCharVector.
 */
public class LargeVarCharAccessor {
  private final LargeVarCharVector accessor;

  LargeVarCharAccessor(LargeVarCharVector vector) {
    this.accessor = vector;
  }

  final UTF8String getUTF8String(int rowId) {
    byte[] bytes = accessor.get(rowId);
    if (bytes == null) {
      return null;
    }
    return UTF8String.fromBytes(bytes);
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
