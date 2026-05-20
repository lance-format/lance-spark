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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LanceArrowColumnVectorTest {

  @Test
  public void nonOwningCloseKeepsArrowReaderOwnedStructVectorsReusable() {
    Field structField =
        new Field(
            "metadata",
            FieldType.nullable(new ArrowType.Struct()),
            Arrays.asList(
                Field.nullable("field0", new ArrowType.Int(32, true)),
                Field.nullable("field1", new ArrowType.Int(32, true)),
                Field.nullable("field2", new ArrowType.Int(32, true)),
                Field.nullable("field3", new ArrowType.Int(32, true)),
                Field.nullable("field4", new ArrowType.Int(32, true)),
                Field.nullable("field5", new ArrowType.Int(32, true))));

    try (BufferAllocator allocator = new RootAllocator();
        StructVector vector = (StructVector) structField.createVector(allocator)) {
      LanceArrowColumnVector columnVector = new LanceArrowColumnVector(vector, false);

      columnVector.close();

      assertEquals(6, vector.getChildrenFromFields().size());
    }
  }
}
