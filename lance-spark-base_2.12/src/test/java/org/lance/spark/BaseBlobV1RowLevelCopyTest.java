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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class BaseBlobV1RowLevelCopyTest extends AbstractBlobV2CopyTest {

  private static final StructType ID_DATA_TAG =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("data", DataTypes.BinaryType, true),
            DataTypes.createStructField("tag", DataTypes.StringType, true)
          });

  @Test
  public void testV1MergeMatchedUpdateCopiesSourceAndCarriesForward() throws Exception {
    String src = "v1_rl_merge_src_" + System.currentTimeMillis();
    String tgt = "v1_rl_merge_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] sourceBlob = deterministicBlob(310, 48);
    byte[] keptBlob = deterministicBlob(311, 48);
    createV1BlobSource(fqSrc, row(0, sourceBlob));
    createV1BlobSource(fqTgt, row(0, deterministicBlob(312, 48)), row(1, keptBlob));
    try {
      spark.sql(
          "MERGE INTO "
              + fqTgt
              + " t USING "
              + fqSrc
              + " s ON t.id = s.id"
              + " WHEN MATCHED THEN UPDATE SET t.data = s.data");

      assertTargetBlobBytes(
          datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {sourceBlob, keptBlob});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void testV1UpdateScalarCarriesForwardBlob() throws Exception {
    String tgt = "v1_rl_upd_tgt_" + System.currentTimeMillis();
    String fqTgt = fq(tgt);
    byte[] blob0 = deterministicBlob(320, 48);
    byte[] blob1 = deterministicBlob(321, 48);
    createV1TagTable(fqTgt);
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(0, blob0, "a"));
    rows.add(RowFactory.create(1, blob1, "b"));
    spark.createDataFrame(rows, ID_DATA_TAG).coalesce(1).writeTo(fqTgt).append();
    try {
      spark.sql("UPDATE " + fqTgt + " SET tag = 'z' WHERE id = 0");

      assertEquals(
          "z", spark.sql("SELECT tag FROM " + fqTgt + " WHERE id = 0").first().getString(0));
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {blob0, blob1});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  private void createV1TagTable(String fqTable) {
    spark.sql(
        "CREATE TABLE "
            + fqTable
            + " (id INT NOT NULL, data BINARY, tag STRING) USING lance TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob', 'file_format_version' = '2.0')");
  }
}
