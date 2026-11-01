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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for the schema-aware {@code LanceArrowColumnVector(ValueVector, DataType)}
 * constructor introduced for <a href="https://github.com/lance-format/lance-spark/issues/499">issue
 * #499</a>. Lance's native scan does not push down nested struct projection, so the underlying
 * Arrow {@code StructVector} always carries all on-disk children in physical order. Spark's
 * generated projection accesses children by the pruned-schema ordinal; binding by physical Arrow
 * ordinal therefore causes a type mismatch and an {@code UnsupportedOperationException} in {@code
 * getLong} / {@code getInt} / etc.
 */
public class LanceArrowColumnVectorStructProjectionTest {

  /**
   * Mirrors the failing case in the bug report: a 4-child struct of differing types is read through
   * a 2-field pruned Spark schema that picks the two long children. Without the schema- aware
   * constructor, slot 0 would dispatch to the {@code external_path} VarChar accessor and {@code
   * getLong(0)} would throw.
   */
  @Test
  public void prunedStructProjection_issue499() {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        StructVector reference = createReferenceStruct(allocator)) {
      // Row 0: ("path-0", 100, 50, true)
      reference.setIndexDefined(0);
      writeRow(reference, 0, "path-0", 100L, 50L, true);
      // Row 1: ("path-1", 200, 75, false)
      reference.setIndexDefined(1);
      writeRow(reference, 1, "path-1", 200L, 75L, false);
      // Row 2: null struct
      reference.setNull(2);
      setRowCounts(reference, 3);

      StructType prunedSchema =
          new StructType().add("offset", DataTypes.LongType).add("length", DataTypes.LongType);

      try (LanceArrowColumnVector vector = new LanceArrowColumnVector(reference, prunedSchema)) {
        assertEquals(prunedSchema, vector.dataType());
        assertTrue(vector.hasNull());
        assertEquals(1, vector.numNulls());

        ColumnarRow row0 = vector.getStruct(0);
        assertEquals(100L, row0.getLong(0));
        assertEquals(50L, row0.getLong(1));

        ColumnarRow row1 = vector.getStruct(1);
        assertEquals(200L, row1.getLong(0));
        assertEquals(75L, row1.getLong(1));

        assertTrue(vector.isNullAt(2));
      }
    }
  }

  /**
   * The pruned schema reorders children relative to the Arrow on-disk order. Confirms that {@link
   * LanceStructAccessor} maps by name, not by ordinal.
   */
  @Test
  public void schemaAwareConstructor_reorderedChildren() {
    Field field =
        new Field(
            "s",
            FieldType.notNullable(ArrowType.Struct.INSTANCE),
            Arrays.asList(
                new Field("int_field", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("long_field", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("string_field", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        StructVector vector = (StructVector) field.createVector(allocator)) {
      vector.allocateNew();
      IntVector intVec = vector.getChild("int_field", IntVector.class);
      BigIntVector longVec = vector.getChild("long_field", BigIntVector.class);
      VarCharVector stringVec = vector.getChild("string_field", VarCharVector.class);

      vector.setIndexDefined(0);
      intVec.setSafe(0, 42);
      longVec.setSafe(0, 9000L);
      byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
      stringVec.setSafe(0, hello, 0, hello.length);

      intVec.setValueCount(1);
      longVec.setValueCount(1);
      stringVec.setValueCount(1);
      vector.setValueCount(1);

      // Pruned schema: string first, then int; long_field is dropped.
      StructType prunedSchema =
          new StructType()
              .add("string_field", DataTypes.StringType)
              .add("int_field", DataTypes.IntegerType);

      try (LanceArrowColumnVector columnVector = new LanceArrowColumnVector(vector, prunedSchema)) {
        assertEquals(prunedSchema, columnVector.dataType());

        ColumnarRow row0 = columnVector.getStruct(0);
        assertEquals(UTF8String.fromString("hello"), row0.getUTF8String(0));
        assertEquals(42, row0.getInt(1));
      }
    }
  }

  /**
   * A pruned outer struct contains an inner struct that is itself pruned and reordered relative to
   * the Arrow on-disk ordering. Confirms that schema-awareness recurses through nested struct
   * children.
   */
  @Test
  public void schemaAwareConstructor_recursesIntoNestedStructs() {
    Field innerField =
        new Field(
            "inner",
            FieldType.notNullable(ArrowType.Struct.INSTANCE),
            Arrays.asList(
                new Field("a", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                new Field("b", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("c", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("d", FieldType.nullable(ArrowType.Bool.INSTANCE), null)));
    Field outerField =
        new Field(
            "outer",
            FieldType.notNullable(ArrowType.Struct.INSTANCE),
            Arrays.asList(
                innerField,
                new Field("extra", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        StructVector outer = (StructVector) outerField.createVector(allocator)) {
      outer.allocateNew();
      StructVector inner = outer.getChild("inner", StructVector.class);
      IntVector extra = outer.getChild("extra", IntVector.class);
      VarCharVector aVec = inner.getChild("a", VarCharVector.class);
      BigIntVector bVec = inner.getChild("b", BigIntVector.class);
      BigIntVector cVec = inner.getChild("c", BigIntVector.class);
      BitVector dVec = inner.getChild("d", BitVector.class);

      outer.setIndexDefined(0);
      inner.setIndexDefined(0);
      byte[] alpha = "alpha".getBytes(StandardCharsets.UTF_8);
      aVec.setSafe(0, alpha, 0, alpha.length);
      bVec.setSafe(0, 11L);
      cVec.setSafe(0, 22L);
      dVec.setSafe(0, 1);
      extra.setSafe(0, 7);

      aVec.setValueCount(1);
      bVec.setValueCount(1);
      cVec.setValueCount(1);
      dVec.setValueCount(1);
      inner.setValueCount(1);
      extra.setValueCount(1);
      outer.setValueCount(1);

      StructType prunedInner =
          new StructType().add("b", DataTypes.LongType).add("c", DataTypes.LongType);
      StructType prunedOuter = new StructType().add("inner", prunedInner);

      try (LanceArrowColumnVector columnVector = new LanceArrowColumnVector(outer, prunedOuter)) {
        assertEquals(prunedOuter, columnVector.dataType());

        ColumnarRow outerRow = columnVector.getStruct(0);
        ColumnarRow innerRow = outerRow.getStruct(0, 2);
        assertEquals(11L, innerRow.getLong(0));
        assertEquals(22L, innerRow.getLong(1));
      }
    }
  }

  /**
   * When the pruned schema names a field that does not exist in the Arrow vector, construction
   * should fail with an actionable error rather than silently returning bogus data.
   */
  @Test
  public void schemaAwareConstructor_rejectsMissingField() {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        StructVector reference = createReferenceStruct(allocator)) {
      setRowCounts(reference, 0);

      StructType badSchema = new StructType().add("does_not_exist", DataTypes.LongType);

      IllegalArgumentException ex =
          org.junit.jupiter.api.Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> new LanceArrowColumnVector(reference, badSchema));
      assertTrue(ex.getMessage().contains("does_not_exist"));
    }
  }

  /** Builds the 4-child struct from the issue: external_path / offset / length / managed. */
  private static StructVector createReferenceStruct(BufferAllocator allocator) {
    Field field =
        new Field(
            "reference",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Arrays.asList(
                new Field("external_path", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                new Field("offset", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("length", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("managed", FieldType.nullable(ArrowType.Bool.INSTANCE), null)));
    StructVector vector = (StructVector) field.createVector(allocator);
    vector.allocateNew();
    return vector;
  }

  private static void writeRow(
      StructVector vector,
      int rowId,
      String externalPath,
      long offset,
      long length,
      boolean managed) {
    VarCharVector externalPathVec = vector.getChild("external_path", VarCharVector.class);
    BigIntVector offsetVec = vector.getChild("offset", BigIntVector.class);
    BigIntVector lengthVec = vector.getChild("length", BigIntVector.class);
    BitVector managedVec = vector.getChild("managed", BitVector.class);

    byte[] pathBytes = externalPath.getBytes(StandardCharsets.UTF_8);
    externalPathVec.setSafe(rowId, pathBytes, 0, pathBytes.length);
    offsetVec.setSafe(rowId, offset);
    lengthVec.setSafe(rowId, length);
    managedVec.setSafe(rowId, managed ? 1 : 0);
  }

  private static void setRowCounts(StructVector vector, int count) {
    for (int i = 0; i < vector.size(); i++) {
      vector.getChildByOrdinal(i).setValueCount(count);
    }
    vector.setValueCount(count);
  }

  @Test
  public void singleArgConstructor_unchangedForBlobAndPrimitives() {
    // Sanity check that adding the schema-aware path didn't break the existing legacy constructor.
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        BigIntVector longVec = new BigIntVector("v", allocator)) {
      longVec.allocateNew(2);
      longVec.setSafe(0, 7L);
      longVec.setSafe(1, 8L);
      longVec.setValueCount(2);

      try (LanceArrowColumnVector cv = new LanceArrowColumnVector(longVec)) {
        assertEquals(DataTypes.LongType, cv.dataType());
        assertEquals(7L, cv.getLong(0));
        assertEquals(8L, cv.getLong(1));
        assertFalse(cv.hasNull());
      }
    }
  }
}
