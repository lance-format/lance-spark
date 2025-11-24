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
package com.lancedb.lance.spark.arrow

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.StructVector
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.util.LanceArrowUtils
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId

class StructWriterSuite extends AnyFunSuite {

  test("struct null rows advance child writers") {
    val childType = new StructType().add("child", DataTypes.IntegerType, nullable = true)
    val schema = new StructType().add("struct_col", childType, nullable = true)

    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val arrowSchema = LanceArrowUtils.toArrowSchema(
        schema,
        ZoneId.systemDefault().getId,
        errorOnDuplicatedFieldNames = true)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      try {
        val writer = LanceArrowWriter.create(root, schema)
        val rows = Seq(
          new GenericInternalRow(Array[Any](new GenericInternalRow(Array[Any](1)))),
          new GenericInternalRow(Array[Any](null)),
          new GenericInternalRow(Array[Any](new GenericInternalRow(Array[Any](3)))))

        rows.foreach(writer.write)
        writer.finish()

        val structVector = root.getVector("struct_col").asInstanceOf[StructVector]
        val childVector = structVector.getChild("child").asInstanceOf[IntVector]

        assert(structVector.getValueCount === rows.length)
        assert(childVector.getValueCount === rows.length)
        assert(structVector.isNull(1))
        assert(childVector.get(0) === 1)
        assert(childVector.get(2) === 3)
      } finally {
        root.close()
      }
    } finally {
      allocator.close()
    }
  }
}
