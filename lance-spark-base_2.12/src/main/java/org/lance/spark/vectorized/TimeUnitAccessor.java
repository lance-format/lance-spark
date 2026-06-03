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

import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Accessor for Arrow Time vectors (TimeSecVector, TimeMilliVector, TimeMicroVector,
 * TimeNanoVector), normalized to nanoseconds for Spark TimeType.
 *
 * <p>Spark 4.1 TimeType stores time-of-day as nanoseconds since midnight internally (via {@code
 * LocalTime.getLong(NANO_OF_DAY)}). This accessor normalizes all four Arrow Time representations to
 * nanoseconds via a multiplier or divisor.
 *
 * <p>Unlike Timestamp vectors which share a common {@code TimeStampVector} base class, the four
 * Arrow Time vector types have no common typed base — Time32 vectors (Sec, Milli) store values as
 * {@code int} and Time64 vectors (Micro, Nano) store values as {@code long}. This accessor uses
 * typed {@code get(int)} methods on the concrete vector classes to avoid per-row boxing overhead,
 * then normalizes to nanoseconds via a multiplier or divisor.
 */
public class TimeUnitAccessor {
  private final ValueVector accessor;
  private final long multiplier;
  private final long divisor;
  private final TypedGetter getter;

  /** Functional interface for typed access to avoid per-row boxing. */
  @FunctionalInterface
  private interface TypedGetter {
    long get(int rowId);
  }

  TimeUnitAccessor(TimeNanoVector vector, long multiplier, long divisor) {
    this.accessor = vector;
    this.multiplier = multiplier;
    this.divisor = divisor;
    this.getter = vector::get;
  }

  TimeUnitAccessor(TimeMicroVector vector, long multiplier, long divisor) {
    this.accessor = vector;
    this.multiplier = multiplier;
    this.divisor = divisor;
    this.getter = vector::get;
  }

  TimeUnitAccessor(TimeMilliVector vector, long multiplier, long divisor) {
    this.accessor = vector;
    this.multiplier = multiplier;
    this.divisor = divisor;
    // TimeMilliVector.get() returns int; widen to long
    this.getter = rowId -> (long) vector.get(rowId);
  }

  TimeUnitAccessor(TimeSecVector vector, long multiplier, long divisor) {
    this.accessor = vector;
    this.multiplier = multiplier;
    this.divisor = divisor;
    // TimeSecVector.get() returns int; widen to long
    this.getter = rowId -> (long) vector.get(rowId);
  }

  final long getLong(int rowId) {
    long raw = getter.get(rowId);
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
