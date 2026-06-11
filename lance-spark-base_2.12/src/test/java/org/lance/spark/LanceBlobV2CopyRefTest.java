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
package org.lance.spark;

import org.lance.spark.utils.BlobReference;
import org.lance.spark.utils.BlobUtils;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.LanceBlobV2CopyRef;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LanceBlobV2CopyRefTest {

  private static final String DATASET_URI = "file:///tmp/source_dataset";
  private static final String COLUMN = "data";
  private static final long BLOB_SIZE = 42L;

  private static Literal descriptorLiteral() {
    Object[] descriptor = new Object[BlobUtils.BLOB_DESCRIPTOR_STRUCT.fields().length];
    descriptor[BlobUtils.BLOB_DESCRIPTOR_STRUCT.fieldIndex("kind")] = (short) 0;
    descriptor[BlobUtils.BLOB_DESCRIPTOR_STRUCT.fieldIndex("position")] = 0L;
    descriptor[BlobUtils.BLOB_DESCRIPTOR_SIZE_ORDINAL] = BLOB_SIZE;
    descriptor[BlobUtils.BLOB_DESCRIPTOR_STRUCT.fieldIndex("blob_id")] = 7L;
    return new Literal(new GenericInternalRow(descriptor), BlobUtils.BLOB_DESCRIPTOR_STRUCT);
  }

  private static Literal nullSentinelDescriptorLiteral() {
    Object[] descriptor = new Object[BlobUtils.BLOB_DESCRIPTOR_STRUCT.fields().length];
    descriptor[BlobUtils.BLOB_DESCRIPTOR_STRUCT.fieldIndex("kind")] = (short) 0;
    descriptor[BlobUtils.BLOB_DESCRIPTOR_STRUCT.fieldIndex("position")] = 0L;
    descriptor[BlobUtils.BLOB_DESCRIPTOR_SIZE_ORDINAL] = 0L;
    descriptor[BlobUtils.BLOB_DESCRIPTOR_STRUCT.fieldIndex("blob_id")] = 0L;
    descriptor[BlobUtils.BLOB_DESCRIPTOR_STRUCT.fieldIndex("blob_uri")] = UTF8String.fromString("");
    return new Literal(new GenericInternalRow(descriptor), BlobUtils.BLOB_DESCRIPTOR_STRUCT);
  }

  @Test
  public void encodesRowAddressAndSizeIntoBlobReferenceToken() {
    LanceBlobV2CopyRef ref =
        new LanceBlobV2CopyRef(
            descriptorLiteral(), new Literal(123L, DataTypes.LongType), DATASET_URI, COLUMN);
    byte[] expected =
        BlobReference.appendRowAddressAndSize(
            BlobReference.serializePrefix(DATASET_URI, COLUMN), 123L, BLOB_SIZE);
    assertArrayEquals(expected, (byte[]) ref.eval(null));
  }

  @Test
  public void nullDescriptorYieldsNull() {
    LanceBlobV2CopyRef ref =
        new LanceBlobV2CopyRef(
            new Literal(null, BlobUtils.BLOB_DESCRIPTOR_STRUCT),
            new Literal(123L, DataTypes.LongType),
            DATASET_URI,
            COLUMN);
    assertNull(ref.eval(null));
  }

  @Test
  public void nullSentinelDescriptorYieldsNull() {
    LanceBlobV2CopyRef ref =
        new LanceBlobV2CopyRef(
            nullSentinelDescriptorLiteral(),
            new Literal(123L, DataTypes.LongType),
            DATASET_URI,
            COLUMN);
    assertNull(ref.eval(null));
  }

  @Test
  public void nullRowAddressFailsInsteadOfCopyingRowZero() {
    LanceBlobV2CopyRef ref =
        new LanceBlobV2CopyRef(
            descriptorLiteral(), new Literal(null, DataTypes.LongType), DATASET_URI, COLUMN);
    assertThrows(IllegalStateException.class, () -> ref.eval(null));
  }
}
