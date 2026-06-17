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

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseBlobV2RowLevelCopyTest extends AbstractBlobV2CopyTest {

  @Test
  public void testUpdateScalarColumnPreservesCarriedForwardBlobs() throws Exception {
    String tgt = "v2_rl_upd_tgt_" + System.currentTimeMillis();
    String fqTgt = fq(tgt);
    byte[] blobA = deterministicBlob(150, 16);
    byte[] blobB = deterministicBlob(151, 16);
    createV2BlobTagTarget(fqTgt);
    insertTagRow(fqTgt, 1, blobA, "a");
    insertTagRow(fqTgt, 2, blobB, "b");
    try {
      spark.sql("UPDATE " + fqTgt + " SET tag = 'z' WHERE id = 1");

      List<Row> rows = spark.sql("SELECT id, tag FROM " + fqTgt + " ORDER BY id").collectAsList();
      assertEquals(2, rows.size());
      assertEquals("z", rows.get(0).getString(1));
      assertEquals("b", rows.get(1).getString(1));
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {blobA, blobB});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void testMergeMatchedUpdateCopiesSourceBlob() throws Exception {
    String src = "v2_rl_merge_src_" + System.currentTimeMillis();
    String tgt = "v2_rl_merge_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] sourceBlob = deterministicBlob(152, 16);
    byte[] targetBlob = deterministicBlob(153, 16);
    createV2BlobSource(fqSrc, row(1, sourceBlob));
    createV2BlobTagTarget(fqTgt);
    insertTagRow(fqTgt, 1, targetBlob, "a");
    try {
      spark.sql(
          "MERGE INTO "
              + fqTgt
              + " t USING "
              + fqSrc
              + " s ON t.id = s.id"
              + " WHEN MATCHED THEN UPDATE SET t.data = s.data");

      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {sourceBlob});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void testMergeNotMatchedInsertCopiesSourceBlob() throws Exception {
    String src = "v2_rl_ins_src_" + System.currentTimeMillis();
    String tgt = "v2_rl_ins_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] existingBlob = deterministicBlob(154, 16);
    byte[] insertedBlob = deterministicBlob(155, 16);
    createV2BlobSource(fqSrc, row(1, existingBlob), row(2, insertedBlob));
    createV2BlobTagTarget(fqTgt);
    insertTagRow(fqTgt, 1, existingBlob, "a");
    try {
      spark.sql(
          "MERGE INTO "
              + fqTgt
              + " t USING "
              + fqSrc
              + " s ON t.id = s.id"
              + " WHEN NOT MATCHED THEN INSERT (id, data, tag) VALUES (s.id, s.data, 'n')");

      assertEquals(2, spark.sql("SELECT * FROM " + fqTgt).count());
      assertTargetBlobBytes(
          datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {existingBlob, insertedBlob});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void testMergeMatchedAndNotMatchedCopiesBothBranches() throws Exception {
    String src = "v2_rl_both_src_" + System.currentTimeMillis();
    String tgt = "v2_rl_both_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] updatedBlob = deterministicBlob(156, 16);
    byte[] insertedBlob = deterministicBlob(157, 16);
    byte[] originalBlob = deterministicBlob(158, 16);
    createV2BlobSource(fqSrc, row(1, updatedBlob), row(2, insertedBlob));
    createV2BlobTagTarget(fqTgt);
    insertTagRow(fqTgt, 1, originalBlob, "a");
    try {
      spark.sql(
          "MERGE INTO "
              + fqTgt
              + " t USING "
              + fqSrc
              + " s ON t.id = s.id"
              + " WHEN MATCHED THEN UPDATE SET t.data = s.data"
              + " WHEN NOT MATCHED THEN INSERT (id, data, tag) VALUES (s.id, s.data, 'n')");

      assertTargetBlobBytes(
          datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {updatedBlob, insertedBlob});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void testMergeScalarOnlyUpdatePreservesTargetBlob() throws Exception {
    String src = "v2_rl_scalar_src_" + System.currentTimeMillis();
    String tgt = "v2_rl_scalar_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] sourceBlob = deterministicBlob(159, 16);
    byte[] targetBlob = deterministicBlob(160, 16);
    createV2BlobSource(fqSrc, row(1, sourceBlob));
    createV2BlobTagTarget(fqTgt);
    insertTagRow(fqTgt, 1, targetBlob, "a");
    try {
      spark.sql(
          "MERGE INTO "
              + fqTgt
              + " t USING "
              + fqSrc
              + " s ON t.id = s.id"
              + " WHEN MATCHED THEN UPDATE SET t.tag = 'm'");

      assertEquals("m", spark.sql("SELECT tag FROM " + fqTgt).first().getString(0));
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {targetBlob});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void testMergeMatchedUpdateCopiesNullBlobAsNull() throws Exception {
    String src = "v2_rl_null_src_" + System.currentTimeMillis();
    String tgt = "v2_rl_null_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] targetBlob = deterministicBlob(161, 16);
    createV2BlobSource(fqSrc, row(1, null));
    createV2BlobTagTarget(fqTgt);
    insertTagRow(fqTgt, 1, targetBlob, "a");
    try {
      spark.sql(
          "MERGE INTO "
              + fqTgt
              + " t USING "
              + fqSrc
              + " s ON t.id = s.id"
              + " WHEN MATCHED THEN UPDATE SET t.data = s.data");
      Row desc =
          spark
              .sql(
                  "SELECT data.kind, data.position, data.size, data.blob_id, data.blob_uri FROM "
                      + fqTgt
                      + " WHERE id = 1")
              .first();
      assertEquals((short) 0, desc.getShort(0));
      assertEquals(0L, desc.getLong(1));
      assertEquals(0L, desc.getLong(2));
      assertEquals(0L, desc.getLong(3));
      assertEquals("", desc.getString(4));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void testDeleteFromBlobV2TablePreservesRemainingBlobs() throws Exception {
    String tgt = "v2_rl_del_tgt_" + System.currentTimeMillis();
    String fqTgt = fq(tgt);
    byte[] blobA = deterministicBlob(162, 16);
    byte[] blobB = deterministicBlob(163, 16);
    createV2BlobTagTarget(fqTgt);
    insertTagRow(fqTgt, 1, blobA, "a");
    insertTagRow(fqTgt, 2, blobB, "b");
    try {
      spark.sql("DELETE FROM " + fqTgt + " WHERE id = 1");

      assertEquals(1, spark.sql("SELECT * FROM " + fqTgt).count());
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {blobB});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void testUpdateBlobWithBinaryLiteralFailsAtAnalysis() throws Exception {
    String tgt = "v2_rl_lit_tgt_" + System.currentTimeMillis();
    String fqTgt = fq(tgt);
    byte[] targetBlob = deterministicBlob(164, 16);
    createV2BlobTagTarget(fqTgt);
    insertTagRow(fqTgt, 1, targetBlob, "a");
    try {
      Exception ex =
          assertThrows(
              Exception.class,
              () -> spark.sql("UPDATE " + fqTgt + " SET data = X'00' WHERE id = 1"));
      assertTrue(
          ex.getMessage() != null && ex.getMessage().contains("Cannot safely cast"),
          "expected Spark's cast rejection, got: " + ex.getMessage());
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {targetBlob});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  private static String hex(byte[] bytes) {
    StringBuilder sb = new StringBuilder("X'");
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.append("'").toString();
  }

  private void insertTagRow(String fqTable, int id, byte[] blob, String tag) {
    String blobLiteral = blob == null ? "CAST(NULL AS BINARY)" : hex(blob);
    spark.sql(
        "INSERT INTO " + fqTable + " VALUES (" + id + ", " + blobLiteral + ", '" + tag + "')");
  }
}
