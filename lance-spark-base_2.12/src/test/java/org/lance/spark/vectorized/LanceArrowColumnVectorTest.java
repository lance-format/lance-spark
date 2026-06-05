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
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @Test
  public void decimalPrecision18FastPathReadsValuesAndNulls() {
    int precision = 18;
    int scale = 2;
    BigDecimal positive = new BigDecimal("9999999999999999.99");
    BigDecimal negative = new BigDecimal("-1234567890123456.78");

    try (BufferAllocator allocator = new RootAllocator();
        DecimalVector vector = new DecimalVector("decimal", allocator, precision, scale)) {
      vector.allocateNew();
      vector.setSafe(0, positive);
      vector.setSafe(1, negative);
      vector.setNull(2);
      vector.setValueCount(3);

      LanceArrowColumnVector columnVector = new LanceArrowColumnVector(vector, false);

      assertTrue(columnVector.hasNull());
      assertEquals(1, columnVector.numNulls());
      assertFalse(columnVector.isNullAt(0));
      assertTrue(columnVector.isNullAt(2));
      assertEquals(positive, columnVector.getDecimal(0, precision, scale).toJavaBigDecimal());
      assertEquals(negative, columnVector.getDecimal(1, precision, scale).toJavaBigDecimal());
      assertNull(columnVector.getDecimal(2, precision, scale));
    }
  }

  @Test
  public void decimalOwningCloseReleasesUnderlyingVector() {
    int precision = 18;
    int scale = 2;
    try (BufferAllocator allocator = new RootAllocator()) {
      // closeVectorOnClose=true: the column vector owns the decimal vector and must release it on
      // close(). Regression guard — the decimal accessor was initially missing from close(), so an
      // owned decimal vector would leak. Not wrapped in try-with-resources here so that
      // columnVector.close() is the sole owner of the underlying buffer.
      DecimalVector vector = new DecimalVector("decimal", allocator, precision, scale);
      vector.allocateNew();
      vector.setSafe(0, new BigDecimal("1.23"));
      vector.setValueCount(1);

      LanceArrowColumnVector columnVector = new LanceArrowColumnVector(vector, true);
      assertTrue(allocator.getAllocatedMemory() > 0);

      columnVector.close();

      assertEquals(
          0,
          allocator.getAllocatedMemory(),
          "close() must release the owned decimal vector's buffers");
    }
  }

  @Test
  public void decimalPrecision1FastPathRoundTrips() {
    int precision = 1;
    int scale = 0;
    try (BufferAllocator allocator = new RootAllocator();
        DecimalVector vector = new DecimalVector("decimal", allocator, precision, scale)) {
      vector.allocateNew();
      vector.setSafe(0, new BigDecimal("7"));
      vector.setSafe(1, new BigDecimal("-9"));
      vector.setValueCount(2);

      LanceArrowColumnVector columnVector = new LanceArrowColumnVector(vector, false);

      assertEquals(
          new BigDecimal("7"), columnVector.getDecimal(0, precision, scale).toJavaBigDecimal());
      assertEquals(
          new BigDecimal("-9"), columnVector.getDecimal(1, precision, scale).toJavaBigDecimal());
    }
  }

  @Test
  public void decimalPrecisionGreaterThan18FallsBackToArrowObjectPath() {
    int precision = 19;
    int scale = 2;
    BigDecimal value = new BigDecimal("99999999999999999.99");

    try (BufferAllocator allocator = new RootAllocator();
        DecimalVector vector = new DecimalVector("decimal", allocator, precision, scale)) {
      vector.allocateNew();
      vector.setSafe(0, value);
      vector.setValueCount(1);

      LanceArrowColumnVector columnVector = new LanceArrowColumnVector(vector, false);

      assertFalse(columnVector.hasNull());
      assertEquals(0, columnVector.numNulls());
      assertEquals(value, columnVector.getDecimal(0, precision, scale).toJavaBigDecimal());
    }
  }
}
