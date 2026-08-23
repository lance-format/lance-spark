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
package org.lance.spark.arrow;

import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.SchemaCompatibility;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FixedSizeListWriterTest {

  @Test
  public void testCompatibleFixedSizeListWithLargeStringElementsIsWritable() {
    Field item = new Field("item", FieldType.nullable(ArrowType.LargeUtf8.INSTANCE), null);
    Field valuesField =
        new Field(
            "values",
            FieldType.nullable(new ArrowType.FixedSizeList(3)),
            Collections.singletonList(item));
    Schema original = new Schema(Collections.singletonList(valuesField));
    Schema sparkArrow =
        new Schema(
            Collections.singletonList(
                new Field(
                    "values",
                    FieldType.nullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(item))));
    StructType sparkSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "values", DataTypes.createArrayType(DataTypes.StringType, true), true)
            });

    assertTrue(SchemaCompatibility.isCompatible(original, sparkArrow));
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(original, allocator)) {
      LanceArrowWriter writer = LanceArrowWriter.create(root, sparkSchema);
      writer.write(
          new GenericInternalRow(
              new Object[] {
                new GenericArrayData(
                    new Object[] {
                      UTF8String.fromString("alpha"),
                      UTF8String.fromString("beta"),
                      UTF8String.fromString("gamma")
                    })
              }));
      writer.write(new GenericInternalRow(new Object[] {null}));
      writer.write(
          new GenericInternalRow(
              new Object[] {
                new GenericArrayData(
                    new Object[] {
                      null, UTF8String.fromString("delta"), UTF8String.fromString("epsilon")
                    })
              }));
      writer.finish();

      FixedSizeListVector values = (FixedSizeListVector) root.getVector("values");
      LargeVarCharVector elements = (LargeVarCharVector) values.getDataVector();
      assertEquals(3, values.getValueCount());
      assertEquals(9, elements.getValueCount());
      assertEquals("alpha", elements.getObject(0).toString());
      assertEquals("gamma", elements.getObject(2).toString());
      assertTrue(values.isNull(1));
      assertNull(elements.getObject(6));
      assertEquals("delta", elements.getObject(7).toString());
      assertEquals("epsilon", elements.getObject(8).toString());
    }
  }

  @Test
  public void testNullListAdvancesBlobV2DataChild() {
    Map<String, String> blobMetadata = new HashMap<>();
    blobMetadata.put(BlobUtils.ARROW_EXTENSION_NAME_KEY, BlobUtils.ARROW_EXTENSION_BLOB_V2);
    Field blobField =
        new Field(
            "item",
            new FieldType(true, ArrowType.Struct.INSTANCE, null, blobMetadata),
            Arrays.asList(
                new Field("data", FieldType.nullable(ArrowType.LargeBinary.INSTANCE), null),
                new Field("uri", FieldType.nullable(ArrowType.LargeUtf8.INSTANCE), null),
                new Field("position", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("size", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    Field valuesField =
        new Field(
            "values",
            FieldType.nullable(new ArrowType.FixedSizeList(2)),
            Collections.singletonList(blobField));
    Schema schema = new Schema(Collections.singletonList(valuesField));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      LanceArrowWriter writer = LanceArrowWriter.create(root, sparkSchema);
      writer.write(
          new GenericInternalRow(
              new Object[] {
                new GenericArrayData(
                    new Object[] {
                      "a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8)
                    })
              }));
      writer.write(new GenericInternalRow(new Object[] {null}));
      writer.write(
          new GenericInternalRow(
              new Object[] {
                new GenericArrayData(new Object[] {null, "c".getBytes(StandardCharsets.UTF_8)})
              }));
      writer.finish();

      FixedSizeListVector values = (FixedSizeListVector) root.getVector("values");
      StructVector elements = (StructVector) values.getDataVector();
      LargeVarBinaryVector data = (LargeVarBinaryVector) elements.getChild("data");
      assertEquals(6, elements.getValueCount());
      assertEquals(6, data.getValueCount());
      assertTrue(values.isNull(1));
      assertTrue(elements.isNull(2));
      assertTrue(elements.isNull(3));
      assertTrue(elements.isNull(4));
      assertNull(data.getObject(4));
      assertArrayEquals("c".getBytes(StandardCharsets.UTF_8), data.getObject(5));
    }
  }

  @Test
  public void testNullListAdvancesStructChildren() {
    Field itemField =
        new Field(
            "item",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Arrays.asList(
                new Field("label", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                new Field("rank", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    Field valuesField =
        new Field(
            "values",
            FieldType.nullable(new ArrowType.FixedSizeList(2)),
            Collections.singletonList(itemField));
    Schema schema = new Schema(Collections.singletonList(valuesField));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      LanceArrowWriter writer = LanceArrowWriter.create(root, sparkSchema);
      writer.write(
          new GenericInternalRow(
              new Object[] {
                new GenericArrayData(
                    new Object[] {
                      new GenericInternalRow(new Object[] {UTF8String.fromString("a"), 1}),
                      new GenericInternalRow(new Object[] {UTF8String.fromString("b"), 2})
                    })
              }));
      writer.write(new GenericInternalRow(new Object[] {null}));
      writer.write(
          new GenericInternalRow(
              new Object[] {
                new GenericArrayData(
                    new Object[] {
                      null, new GenericInternalRow(new Object[] {UTF8String.fromString("c"), 3})
                    })
              }));
      writer.finish();

      FixedSizeListVector values = (FixedSizeListVector) root.getVector("values");
      StructVector elements = (StructVector) values.getDataVector();
      VarCharVector labels = (VarCharVector) elements.getChild("label");
      IntVector ranks = (IntVector) elements.getChild("rank");
      assertEquals(3, values.getValueCount());
      assertEquals(6, elements.getValueCount());
      assertTrue(values.isNull(1));
      assertTrue(elements.isNull(2));
      assertTrue(elements.isNull(3));
      assertTrue(elements.isNull(4));
      assertFalse(elements.isNull(5));
      assertEquals("c", labels.getObject(5).toString());
      assertEquals(3, ranks.getObject(5));
    }
  }
}
