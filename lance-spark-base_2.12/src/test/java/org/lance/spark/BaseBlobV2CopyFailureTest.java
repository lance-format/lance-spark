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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseBlobV2CopyFailureTest extends AbstractBlobV2CopyTest {

  @Test
  public void dynamicOverwrite_rejectedAsUnsupported() throws Exception {
    String src = "v2_fail_dynover_src_" + System.currentTimeMillis();
    String tgt = "v2_fail_dynover_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(140, 16)));
    createV2BlobTable(fqTgt);
    try {
      Exception ex =
          assertThrows(
              Exception.class, () -> spark.table(fqSrc).writeTo(fqTgt).overwritePartitions());
      assertTrue(
          ex.getMessage().contains("dynamic overwrite"),
          "expected the connector's dynamic-overwrite rejection, got: " + ex.getMessage());
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void testUpdateBlobWithBinaryLiteralFailsAtAnalysis() throws Exception {
    String tgt = "v2_fail_update_tgt_" + System.currentTimeMillis();
    String fqTgt = fq(tgt);
    createV2BlobSource(fqTgt, row(1, deterministicBlob(143, 16)));
    try {
      Exception ex =
          assertThrows(
              Exception.class,
              () -> spark.sql("UPDATE " + fqTgt + " SET data = X'00' WHERE id = 1"));
      assertTrue(
          ex.getMessage() != null && !ex.getMessage().isEmpty(),
          "UPDATE must fail with a message, got: " + ex);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void unionAllInsert_failsAtWriteValidationNotSilently() throws Exception {
    String srcA = "v2_fail_union_a_" + System.currentTimeMillis();
    String srcB = "v2_fail_union_b_" + System.currentTimeMillis();
    String tgt = "v2_fail_union_tgt_" + System.currentTimeMillis();
    String fqA = fq(srcA);
    String fqB = fq(srcB);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqA, row(1, deterministicBlob(145, 16)));
    createV2BlobSource(fqB, row(2, deterministicBlob(146, 16)));
    createV2BlobTable(fqTgt);
    try {
      Exception ex =
          assertThrows(
              Exception.class,
              () ->
                  spark.sql(
                      "INSERT INTO "
                          + fqTgt
                          + " SELECT id, data FROM "
                          + fqA
                          + " UNION ALL SELECT id, data FROM "
                          + fqB));
      assertTrue(
          ex.getMessage().contains("binary"),
          "expected the write validator's descriptor rejection, got: " + ex.getMessage());
      assertEquals(0, spark.sql("SELECT * FROM " + fqTgt).count());
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqA);
      spark.sql("DROP TABLE IF EXISTS " + fqB);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }
}
