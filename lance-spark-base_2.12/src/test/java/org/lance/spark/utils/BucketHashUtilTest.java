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
package org.lance.spark.utils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class BucketHashUtilTest {

  @Test
  public void testComputeBucketIdInt() {
    InternalRow row = new GenericInternalRow(new Object[] {42});
    int bucket =
        BucketHashUtil.computeBucketId(
            row, new int[] {0}, new DataType[] {DataTypes.IntegerType}, 10);
    assertTrue(bucket >= 0 && bucket < 10);
  }

  @Test
  public void testComputeBucketIdString() {
    InternalRow row = new GenericInternalRow(new Object[] {UTF8String.fromString("hello")});
    int bucket =
        BucketHashUtil.computeBucketId(
            row, new int[] {0}, new DataType[] {DataTypes.StringType}, 8);
    assertTrue(bucket >= 0 && bucket < 8);
  }

  @Test
  public void testComputeBucketIdFromValueConsistency() {
    InternalRow row = new GenericInternalRow(new Object[] {UTF8String.fromString("test")});
    int writeBucket =
        BucketHashUtil.computeBucketId(
            row, new int[] {0}, new DataType[] {DataTypes.StringType}, 16);
    int readBucket = BucketHashUtil.computeBucketIdFromValue("test", 16);
    assertEquals(writeBucket, readBucket, "Write and read bucket IDs must match");
  }

  @Test
  public void testComputeBucketIdFromUtf8StringValueConsistency() {
    UTF8String value = UTF8String.fromString("test");
    InternalRow row = new GenericInternalRow(new Object[] {value});
    int writeBucket =
        BucketHashUtil.computeBucketId(
            row, new int[] {0}, new DataType[] {DataTypes.StringType}, 16);
    int readBucket = BucketHashUtil.computeBucketIdFromValue(value, 16);
    assertEquals(writeBucket, readBucket, "Catalyst UTF8String values must hash consistently");
  }

  @Test
  public void testComputeBucketIdFromValueIntConsistency() {
    InternalRow row = new GenericInternalRow(new Object[] {123});
    int writeBucket =
        BucketHashUtil.computeBucketId(
            row, new int[] {0}, new DataType[] {DataTypes.IntegerType}, 32);
    int readBucket = BucketHashUtil.computeBucketIdFromValue(123, 32);
    assertEquals(writeBucket, readBucket);
  }

  @Test
  public void testComputeBucketIdFromValueNull() {
    int bucket = BucketHashUtil.computeBucketIdFromValue(null, 4);
    assertEquals(0, bucket);
  }

  @Test
  public void testUnsupportedTypeThrows() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> BucketHashUtil.computeBucketIdFromValue(new Object(), 4));
  }

  @Test
  public void testUnsupportedRowTypeThrows() {
    InternalRow row = new GenericInternalRow(new Object[] {new byte[] {1}});
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            BucketHashUtil.computeBucketId(
                row, new int[] {0}, new DataType[] {DataTypes.BinaryType}, 4));
  }
}
