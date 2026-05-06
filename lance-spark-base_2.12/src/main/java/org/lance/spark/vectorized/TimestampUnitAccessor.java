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

import org.apache.arrow.vector.TimeStampVector;

/**
 * Accessor for non-microsecond Arrow Timestamp vectors, normalized to microseconds for Spark.
 *
 * <p>Wraps any {@link TimeStampVector} subclass (Second, Milli, Nano — with or without timezone)
 * and converts raw values to microseconds via a multiplier or divisor.
 */
public class TimestampUnitAccessor {
  private final TimeStampVector accessor;
  private final long multiplier;
  private final long divisor;

  TimestampUnitAccessor(TimeStampVector vector, long multiplier, long divisor) {
    this.accessor = vector;
    this.multiplier = multiplier;
    this.divisor = divisor;
  }

  final long getLong(int rowId) {
    long raw = accessor.get(rowId);
    if (multiplier != 1L) {
      return raw * multiplier;
    }
    return Math.floorDiv(raw, divisor);
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
