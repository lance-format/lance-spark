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
package org.lance.spark.substrait;

/**
 * Translates Spark V2 aggregations into Substrait {@code Plan} bytes (containing an {@code
 * AggregateRel}) for {@code ScanOptions.substraitAggregate}.
 *
 * <p>Phase 2 marker class — implementation lands in a follow-up PR that closes <a
 * href="https://github.com/lance-format/lance-spark/issues/231">lance-format/lance-spark#231</a>.
 * Lance executes pushdown aggregates completely per fragment and Spark merges across partitions, so
 * {@code SUM} / {@code COUNT} / {@code MIN} / {@code MAX} work but {@code AVG} does not (the Phase
 * 2 PR will reject it).
 */
public final class AggregateEncoder {
  private AggregateEncoder() {}
}
