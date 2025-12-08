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
package org.lance.spark.arrow

import org.apache.arrow.vector.ValueVector
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.util.LanceArrowUtils

/**
 * Base class for Arrow field writers.
 *
 * This class is copied from Apache Spark's ArrowWriter.scala
 * (https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowWriter.scala)
 * to support converting Spark DataFrame Array<Float/Double> columns to Arrow FixedSizeList
 * for vector embeddings and ML workloads in Lance.
 *
 * This base class defines the interface for writing individual fields from
 * Spark's InternalRow format to Arrow vectors.
 */
abstract private[arrow] class LanceArrowFieldWriter {
  def valueVector: ValueVector
  def name: String = valueVector.getField().getName()
  def dataType: DataType = LanceArrowUtils.fromArrowField(valueVector.getField())
  def nullable: Boolean = valueVector.getField().isNullable()

  def setNull(): Unit
  def setValue(input: SpecializedGetters, ordinal: Int): Unit

  private[arrow] var count: Int = 0

  def write(input: SpecializedGetters, ordinal: Int): Unit = {
    if (input.isNullAt(ordinal)) {
      setNull()
    } else {
      setValue(input, ordinal)
    }
    count += 1
  }

  def finish(): Unit = {
    valueVector.setValueCount(count)
  }

  def reset(): Unit = {
    count = 0
    valueVector.reset()
  }
}
