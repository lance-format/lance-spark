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

import org.lance.spark.utils.BlobUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BlobV2DescriptorColumnVectorTest {

  @Test
  public void testDescriptorChildrenRead() {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        StructVector struct = buildDescriptor(allocator, (short) 3, 12345L, 678L, 99L, "s3://b/o");
        LanceArrowColumnVector wrapper = new LanceArrowColumnVector(struct)) {

      assertEquals(1, struct.getValueCount());
      assertNotNull(wrapper.getChild(0));
      assertEquals((short) 3, wrapper.getChild(0).getShort(0));
      assertEquals(12345L, wrapper.getChild(1).getLong(0));
      assertEquals(678L, wrapper.getChild(2).getLong(0));
      assertEquals(99L, wrapper.getChild(3).getLong(0));
      assertEquals("s3://b/o", wrapper.getChild(4).getUTF8String(0).toString());
    }
  }

  @Test
  public void testDescriptorMaxUnsignedValues() {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        StructVector struct =
            buildDescriptor(allocator, (short) 255, 1L << 40, 1L << 30, 4294967295L, "x");
        LanceArrowColumnVector wrapper = new LanceArrowColumnVector(struct)) {

      assertEquals((short) 255, wrapper.getChild(0).getShort(0));
      assertEquals(1L << 40, wrapper.getChild(1).getLong(0));
      assertEquals(1L << 30, wrapper.getChild(2).getLong(0));
      assertEquals(4294967295L, wrapper.getChild(3).getLong(0));
    }
  }

  private static StructVector buildDescriptor(
      BufferAllocator allocator,
      short kind,
      long position,
      long size,
      long blobId,
      String blobUri) {
    Map<String, String> structMd = new HashMap<>();
    structMd.put(BlobUtils.ARROW_EXTENSION_NAME_KEY, BlobUtils.ARROW_EXTENSION_BLOB_V2);
    Field structField =
        new Field(
            "payload",
            new FieldType(true, ArrowType.Struct.INSTANCE, null, structMd),
            Arrays.asList(
                intChild("kind", 8),
                intChild("position", 64),
                intChild("size", 64),
                intChild("blob_id", 32),
                utf8Child("blob_uri")));
    StructVector struct = (StructVector) structField.createVector(allocator);
    struct.allocateNew();

    UInt1Vector kindV = (UInt1Vector) struct.getChild("kind");
    UInt8Vector posV = (UInt8Vector) struct.getChild("position");
    UInt8Vector sizeV = (UInt8Vector) struct.getChild("size");
    UInt4Vector idV = (UInt4Vector) struct.getChild("blob_id");
    VarCharVector uriV = (VarCharVector) struct.getChild("blob_uri");

    struct.setIndexDefined(0);
    kindV.setSafe(0, kind);
    posV.setSafe(0, position);
    sizeV.setSafe(0, size);
    idV.setSafe(0, (int) blobId);
    uriV.setSafe(0, blobUri.getBytes(StandardCharsets.UTF_8));

    struct.setValueCount(1);
    return struct;
  }

  private static Field intChild(String name, int bitWidth) {
    return new Field(
        name,
        new FieldType(true, new ArrowType.Int(bitWidth, false), null, Collections.emptyMap()),
        null);
  }

  private static Field utf8Child(String name) {
    return new Field(
        name, new FieldType(true, ArrowType.Utf8.INSTANCE, null, Collections.emptyMap()), null);
  }
}
