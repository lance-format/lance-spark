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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LargeListSupportTest {

  @Test
  public void convertsLargeListSchemaToSparkArray() {
    Field element =
        new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList());
    Field largeList =
        new Field(
            "values",
            FieldType.nullable(ArrowType.LargeList.INSTANCE),
            Collections.singletonList(element));

    StructType sparkSchema =
        LanceArrowUtils.fromArrowSchema(new Schema(Collections.singletonList(largeList)));
    ArrayType arrayType = (ArrayType) sparkSchema.apply("values").dataType();

    assertEquals(DataTypes.IntegerType, arrayType.elementType());
    assertTrue(arrayType.containsNull());

    Field nested =
        new Field(
            "nested",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Collections.singletonList(largeList));
    StructType nestedSchema =
        LanceArrowUtils.fromArrowSchema(new Schema(Collections.singletonList(nested)));
    StructType nestedType = (StructType) nestedSchema.apply("nested").dataType();
    ArrayType nestedArrayType = (ArrayType) nestedType.apply("values").dataType();
    assertEquals(DataTypes.IntegerType, nestedArrayType.elementType());
    assertTrue(nestedArrayType.containsNull());
  }

  @Test
  public void readsLargeListVectorAsSparkArray() {
    try (RootAllocator allocator = new RootAllocator();
        LargeListVector vector = LargeListVector.empty("values", allocator)) {
      vector.addOrGetVector(FieldType.nullable(new ArrowType.Int(32, true)));
      vector.allocateNew();
      IntVector elements = (IntVector) vector.getDataVector();

      vector.startNewValue(0);
      elements.setSafe(0, 1);
      elements.setSafe(1, 2);
      vector.endValue(0, 2L);

      vector.startNewValue(1);
      elements.setSafe(2, 3);
      elements.setNull(3);
      elements.setSafe(4, 5);
      vector.endValue(1, 3L);

      vector.startNewValue(3);
      vector.endValue(3, 0L);
      elements.setValueCount(5);
      vector.setValueCount(4);

      LanceArrowColumnVector column = new LanceArrowColumnVector(vector, false);
      assertEquals(DataTypes.createArrayType(DataTypes.IntegerType), column.dataType());

      ColumnarArray first = column.getArray(0);
      assertEquals(2, first.numElements());
      assertEquals(1, first.getInt(0));
      assertEquals(2, first.getInt(1));

      ColumnarArray second = column.getArray(1);
      assertEquals(3, second.numElements());
      assertEquals(3, second.getInt(0));
      assertTrue(second.isNullAt(1));
      assertEquals(5, second.getInt(2));

      assertTrue(column.isNullAt(2));
      assertEquals(0, column.getArray(3).numElements());
      column.close();
    }
  }

  @Test
  public void rejectsOffsetsThatSparkCannotRepresent() {
    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> LanceLargeArrayAccessor.toSparkArrayInt((long) Integer.MAX_VALUE + 1, "offset"));

    assertTrue(error.getMessage().contains("32-bit array indexing"));
  }
}
