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

import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.types.TimeUnit;

/**
 * Accessor for non-microsecond Arrow Duration vectors, normalized to the microseconds of Spark's
 * {@code DayTimeIntervalType}. Seconds and milliseconds are scaled up, failing on overflow;
 * nanoseconds are rounded down, as for timestamps.
 */
public class DurationUnitAccessor {
  private final DurationVector accessor;
  private final long multiplier;
  private final long divisor;

  DurationUnitAccessor(DurationVector vector) {
    this.accessor = vector;
    TimeUnit unit = vector.getUnit();
    this.multiplier =
        unit == TimeUnit.SECOND ? 1_000_000L : unit == TimeUnit.MILLISECOND ? 1_000L : 1L;
    this.divisor = unit == TimeUnit.NANOSECOND ? 1_000L : 1L;
  }

  final long getLong(int rowId) {
    long raw = DurationVector.get(accessor.getDataBuffer(), rowId);
    if (multiplier != 1L) {
      return Math.multiplyExact(raw, multiplier);
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
