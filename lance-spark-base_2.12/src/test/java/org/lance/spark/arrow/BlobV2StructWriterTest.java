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

import org.lance.spark.utils.SchemaConverter;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BlobV2StructWriterTest {

  @Test
  public void testWritesBinaryToDataChild() {
    StructType sparkSchema = blobV2Schema();
    Schema arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", true);
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      LanceArrowWriter writer = LanceArrowWriter.create(root, sparkSchema);

      byte[][] payloads = {
        "row0".getBytes(StandardCharsets.UTF_8),
        "row1".getBytes(StandardCharsets.UTF_8),
        "row2".getBytes(StandardCharsets.UTF_8),
      };

      for (int i = 0; i < payloads.length; i++) {
        writer.write(new GenericInternalRow(new Object[] {i, payloads[i]}));
      }

      writer.finish();
      StructVector content = (StructVector) root.getVector("content");
      assertEquals(payloads.length, content.getValueCount());
      LargeVarBinaryVector data = (LargeVarBinaryVector) content.getChild("data");
      assertEquals(payloads.length, data.getValueCount());
      for (int i = 0; i < payloads.length; i++) {
        assertFalse(content.isNull(i));
        assertArrayEquals(payloads[i], data.getObject(i));
      }

      for (String sibling : new String[] {"uri", "position", "size"}) {
        FieldVector child = content.getChild(sibling);
        for (int i = 0; i < payloads.length; i++) {
          assertTrue(child.isNull(i));
        }
      }
    }
  }

  @Test
  public void testNullRowNullsStruct() {
    StructType sparkSchema = blobV2Schema();
    Schema arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", true);
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      LanceArrowWriter writer = LanceArrowWriter.create(root, sparkSchema);

      writer.write(
          new GenericInternalRow(new Object[] {0, "first".getBytes(StandardCharsets.UTF_8)}));
      writer.write(new GenericInternalRow(new Object[] {1, null}));
      writer.write(
          new GenericInternalRow(new Object[] {2, "third".getBytes(StandardCharsets.UTF_8)}));
      writer.finish();

      StructVector content = (StructVector) root.getVector("content");
      assertEquals(3, content.getValueCount());
      assertFalse(content.isNull(0));
      assertTrue(content.isNull(1));
      assertFalse(content.isNull(2));

      LargeVarBinaryVector data = (LargeVarBinaryVector) content.getChild("data");
      assertEquals(3, data.getValueCount());
      assertArrayEquals("first".getBytes(StandardCharsets.UTF_8), data.getObject(0));
      assertTrue(data.isNull(1));
      assertArrayEquals("third".getBytes(StandardCharsets.UTF_8), data.getObject(2));
    }
  }

  @Test
  public void testResetClearsBatch() {
    StructType sparkSchema = blobV2Schema();
    Schema arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", true);
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      LanceArrowWriter writer = LanceArrowWriter.create(root, sparkSchema);
      writer.write(
          new GenericInternalRow(new Object[] {0, "discard".getBytes(StandardCharsets.UTF_8)}));
      writer.write(
          new GenericInternalRow(
              new Object[] {1, "also-discard".getBytes(StandardCharsets.UTF_8)}));
      writer.reset();

      writer.write(
          new GenericInternalRow(new Object[] {7, "keep".getBytes(StandardCharsets.UTF_8)}));
      writer.finish();

      StructVector content = (StructVector) root.getVector("content");
      assertEquals(1, content.getValueCount());
      LargeVarBinaryVector data = (LargeVarBinaryVector) content.getChild("data");
      assertEquals(1, data.getValueCount());
      assertArrayEquals("keep".getBytes(StandardCharsets.UTF_8), data.getObject(0));
    }
  }

  private static StructType blobV2Schema() {
    StructType raw =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("content", DataTypes.BinaryType, true),
            });
    Map<String, String> properties = ImmutableMap.of("content.lance.encoding", "blob");
    return SchemaConverter.processSchemaWithProperties(raw, properties, "2.2");
  }
}
