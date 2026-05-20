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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class BlobReferenceTest {

  @Test
  public void testRoundTripSerialization() {
    BlobReference original = new BlobReference("/tmp/my-dataset", "image_col", 0x0003_0000_0042L);

    byte[] serialized = original.serialize();
    assertTrue(BlobReference.isBlobReference(serialized));

    BlobReference deserialized = BlobReference.deserialize(serialized);
    assertEquals(original.getDatasetUri(), deserialized.getDatasetUri());
    assertEquals(original.getColumnName(), deserialized.getColumnName());
    assertEquals(original.getRowAddress(), deserialized.getRowAddress());
  }

  @Test
  public void testRoundTripWithUnicodeUri() {
    BlobReference original = new BlobReference("s3://bucket/path/日本語", "データ", 123456789L);

    byte[] serialized = original.serialize();
    BlobReference deserialized = BlobReference.deserialize(serialized);

    assertEquals(original.getDatasetUri(), deserialized.getDatasetUri());
    assertEquals(original.getColumnName(), deserialized.getColumnName());
    assertEquals(original.getRowAddress(), deserialized.getRowAddress());
  }

  @Test
  public void testRoundTripWithEmptyStrings() {
    BlobReference original = new BlobReference("", "", 0L);

    byte[] serialized = original.serialize();
    BlobReference deserialized = BlobReference.deserialize(serialized);

    assertEquals("", deserialized.getDatasetUri());
    assertEquals("", deserialized.getColumnName());
    assertEquals(0L, deserialized.getRowAddress());
  }

  @Test
  public void testIsBlobReferenceRejectsNonReference() {
    assertFalse(BlobReference.isBlobReference(null));
    assertFalse(BlobReference.isBlobReference(new byte[0]));
    assertFalse(BlobReference.isBlobReference(new byte[] {1, 2, 3, 4}));
    assertFalse(BlobReference.isBlobReference("not a blob reference".getBytes()));
  }

  @Test
  public void testDeserializeRejectsInvalidInput() {
    assertThrows(
        IllegalArgumentException.class, () -> BlobReference.deserialize(new byte[] {1, 2, 3, 4}));
  }

  @Test
  public void testMagicHeader() {
    BlobReference ref = new BlobReference("uri", "col", 42L);
    byte[] serialized = ref.serialize();

    assertEquals('L', serialized[0]);
    assertEquals('A', serialized[1]);
    assertEquals('N', serialized[2]);
    assertEquals('C', serialized[3]);
    assertEquals('E', serialized[4]);
    assertEquals('R', serialized[5]);
    assertEquals('E', serialized[6]);
    assertEquals('F', serialized[7]);
  }

  @Test
  public void testRandomBytesNotMisidentified() {
    // Bytes that happen to start with LANCEREF but have invalid structure
    byte[] fake = "LANCEREFxgarbage-data-here-padding".getBytes();
    assertFalse(BlobReference.isBlobReference(fake));
  }
}
