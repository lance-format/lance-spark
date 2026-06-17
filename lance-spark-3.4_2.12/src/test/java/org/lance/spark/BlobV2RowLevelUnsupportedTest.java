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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BlobV2RowLevelUnsupportedTest extends AbstractBlobV2CopyTest {

  @Test
  public void testMergeIntoBlobV2TableFailsLoudly() throws Exception {
    String src = "v2_rl34_merge_src_" + System.currentTimeMillis();
    String tgt = "v2_rl34_merge_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(141, 16)));
    createV2BlobSource(fqTgt, row(1, deterministicBlob(142, 16)));
    try {
      Exception ex =
          assertThrows(
              Exception.class,
              () ->
                  spark.sql(
                      "MERGE INTO "
                          + fqTgt
                          + " t USING "
                          + fqSrc
                          + " s ON t.id = s.id"
                          + " WHEN MATCHED THEN UPDATE SET t.data = s.data"));
      assertTrue(
          ex.getMessage() != null && !ex.getMessage().isEmpty(),
          "MERGE must fail with a message, got: " + ex);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void testUpdateBlobV2TableFailsLoudly() throws Exception {
    String tgt = "v2_rl34_update_tgt_" + System.currentTimeMillis();
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
}
