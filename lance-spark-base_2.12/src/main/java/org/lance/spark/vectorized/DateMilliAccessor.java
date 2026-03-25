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

import org.apache.arrow.vector.DateMilliVector;

/** Accessor for Arrow Date(MILLISECOND), normalized to Spark DateType (days since epoch). */
public class DateMilliAccessor {
  private static final long MILLIS_PER_DAY = 86_400_000L;

  private final DateMilliVector accessor;

  DateMilliAccessor(DateMilliVector vector) {
    this.accessor = vector;
  }

  final int getInt(int rowId) {
    long millis = accessor.get(rowId);
    return (int) Math.floorDiv(millis, MILLIS_PER_DAY);
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
