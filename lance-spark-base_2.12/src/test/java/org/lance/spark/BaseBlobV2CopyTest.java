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

import org.lance.spark.utils.BlobUtils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseBlobV2CopyTest extends AbstractBlobV2CopyTest {

  /**
   * A null blob yields a null descriptor struct, so every projected field reads back null. Prior to
   * lance 9 the writer emitted a redundant rep/def validity layer, which made the descriptor decode
   * as a non-null zero-filled struct and silently dropped the null.
   */
  private static void assertNullBlobDescriptor(Row descriptor) {
    for (int i = 0; i < 5; i++) {
      assertTrue(
          descriptor.isNullAt(i), "expected null blob descriptor field " + i + " to be null");
    }
  }

  @Test
  public void insertSelect_copiesNullBlobAsNull() throws Exception {
    String src = "v2_e2e_null_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_null_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] blob = deterministicBlob(147, 16);
    createV2BlobSource(fqSrc, row(1, blob), row(2, null));
    createV2BlobTable(fqTgt);
    try {
      Row nullDesc =
          spark
              .sql(
                  "SELECT data.kind, data.position, data.size, data.blob_id, data.blob_uri "
                      + "FROM "
                      + fqSrc
                      + " WHERE id = 2")
              .first();
      assertNullBlobDescriptor(nullDesc);

      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc);

      List<Row> rows = spark.sql("SELECT id, data FROM " + fqTgt + " ORDER BY id").collectAsList();
      assertEquals(2, rows.size());
      assertFalse(rows.get(0).isNullAt(1));
      assertTrue(rows.get(1).isNullAt(1));

      Row tgtNullDesc =
          spark
              .sql(
                  "SELECT data.kind, data.position, data.size, data.blob_id, data.blob_uri "
                      + "FROM "
                      + fqTgt
                      + " WHERE id = 2")
              .first();
      assertNullBlobDescriptor(tgtNullDesc);

      List<Long> tgtAddrs = rowAddressesOf(fqTgt);
      assertEquals(2, tgtAddrs.size());
      try (org.lance.Dataset ds =
          org.lance.Dataset.open()
              .allocator(LanceRuntime.allocator())
              .uri(datasetUriOf(tgt))
              .build()) {
        assertEquals(1, ds.takeBlobs(tgtAddrs, "data").size(), "v2 takeBlobs skips null rows");
      }

      assertTargetBlobBytes(
          datasetUriOf(tgt),
          Collections.singletonList(rowAddressesOf(fqTgt).get(0)),
          new byte[][] {blob});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void insertSelect_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_insert_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_insert_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(11, 64), deterministicBlob(12, 4096)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void ctas_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_ctas_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_ctas_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(13, 64), deterministicBlob(14, 4096)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    try {
      spark.sql("CREATE TABLE " + fqTgt + " USING lance AS SELECT id, data FROM " + fqSrc);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void joinAndInsert_copiesBothSidesBlobBytes() throws Exception {
    String srcA = "v2_e2e_join_a_" + System.currentTimeMillis();
    String srcB = "v2_e2e_join_b_" + System.currentTimeMillis();
    String tgt = "v2_e2e_join_tgt_" + System.currentTimeMillis();
    String fqA = fq(srcA);
    String fqB = fq(srcB);
    String fqTgt = fq(tgt);
    byte[][] blobsA = {deterministicBlob(41, 1000), deterministicBlob(42, 1000)};
    byte[][] blobsB = {deterministicBlob(43, 2000), deterministicBlob(44, 2000)};
    createV2BlobSource(fqA, row(1, blobsA[0]), row(2, blobsA[1]));
    createV2BlobSource(fqB, row(1, blobsB[0]), row(2, blobsB[1]));
    createTwoBlobTarget(fqTgt);
    try {
      spark.sql(
          "INSERT INTO "
              + fqTgt
              + " SELECT a.id, a.data AS data_a, b.data AS data_b FROM "
              + fqA
              + " a JOIN "
              + fqB
              + " b ON a.id = b.id");
      List<Long> rowAddrs = rowAddressesOf(fqTgt);
      String tgtUri = datasetUriOf(tgt);
      assertTargetBlobBytes(tgtUri, rowAddrs, "data_a", blobsA);
      assertTargetBlobBytes(tgtUri, rowAddrs, "data_b", blobsB);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqA);
      spark.sql("DROP TABLE IF EXISTS " + fqB);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void oneToManyJoin_copiesBlobBytesForEveryFannedRow() throws Exception {
    String blobTbl = "v2_e2e_o2m_blob_" + System.currentTimeMillis();
    String tagTbl = "v2_e2e_o2m_tag_" + System.currentTimeMillis();
    String tgt = "v2_e2e_o2m_tgt_" + System.currentTimeMillis();
    String fqBlob = fq(blobTbl);
    String fqTag = fq(tagTbl);
    String fqTgt = fq(tgt);
    byte[] blob1 = deterministicBlob(51, 512);
    byte[] blob2 = deterministicBlob(52, 512);
    createV2BlobSource(fqBlob, row(1, blob1), row(2, blob2));
    spark.sql("CREATE TABLE " + fqTag + " (id INT NOT NULL, tag STRING) USING lance");
    List<Row> tagRows = new ArrayList<>();
    tagRows.add(RowFactory.create(1, "a"));
    tagRows.add(RowFactory.create(1, "b"));
    tagRows.add(RowFactory.create(1, "c"));
    tagRows.add(RowFactory.create(2, "d"));
    spark.createDataFrame(tagRows, tagSchema()).coalesce(1).writeTo(fqTag).append();
    createV2BlobTagTarget(fqTgt);
    try {
      spark.sql(
          "INSERT INTO "
              + fqTgt
              + " SELECT a.id, a.data, b.tag FROM "
              + fqBlob
              + " a JOIN "
              + fqTag
              + " b ON a.id = b.id");
      List<Row> ordered =
          spark
              .sql(
                  "SELECT "
                      + LanceConstant.ROW_ADDRESS
                      + ", id, tag FROM "
                      + fqTgt
                      + " ORDER BY id, tag")
              .collectAsList();
      assertEquals(4, ordered.size(), "one-to-many join should produce 4 rows");
      List<Long> rowAddrs = new ArrayList<>();
      byte[][] expected = new byte[ordered.size()][];
      for (int i = 0; i < ordered.size(); i++) {
        rowAddrs.add(ordered.get(i).getLong(0));
        expected[i] = ordered.get(i).getInt(1) == 1 ? blob1 : blob2;
      }
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddrs, "data", expected);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqBlob);
      spark.sql("DROP TABLE IF EXISTS " + fqTag);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void selfJoin_injectsCopyRefBoundToSelectedSide() throws Exception {
    String src = "v2_e2e_self_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_self_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(7, 128), deterministicBlob(8, 128)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT a.id, a.data FROM "
              + fqSrc
              + " a JOIN "
              + fqSrc
              + " b ON a.id = b.id";
      int copyRefs = countCopyRefs(analyzePlan(sql));
      assertEquals(1, copyRefs, "expected 1 CopyRef for self-join selecting one side");
      spark.sql(sql);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void insertSelectWithOrderBy_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_order_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_order_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(31, 64), deterministicBlob(32, 256)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc + " ORDER BY id DESC");
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void insertSelectWithOrderByLimit_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_limit_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_limit_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(33, 64), deterministicBlob(34, 256)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc + " ORDER BY id LIMIT 1");
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {blobs[0]});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void insertSelectWithOffset_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_offset_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_offset_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(93, 64), deterministicBlob(94, 256)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobTable(fqTgt);
    try {
      spark.sql(
          "INSERT INTO "
              + fqTgt
              + " SELECT id, data FROM "
              + fqSrc
              + " ORDER BY id LIMIT 1"
              + " OFFSET 1");
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {blobs[1]});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void insertOverwrite_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_overwrite_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_overwrite_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(68, 64), deterministicBlob(69, 4096)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobSource(fqTgt, row(99, deterministicBlob(70, 16)));
    try {
      spark.sql("INSERT OVERWRITE " + fqTgt + " SELECT id, data FROM " + fqSrc);
      assertEquals(
          2,
          spark.sql("SELECT * FROM " + fqTgt).count(),
          "overwrite must replace the previous row");
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void ctasFromJoin_copiesBothSidesBlobBytes() throws Exception {
    String srcA = "v2_e2e_ctasjoin_a_" + System.currentTimeMillis();
    String srcB = "v2_e2e_ctasjoin_b_" + System.currentTimeMillis();
    String tgt = "v2_e2e_ctasjoin_tgt_" + System.currentTimeMillis();
    String fqA = fq(srcA);
    String fqB = fq(srcB);
    String fqTgt = fq(tgt);
    byte[] blobA = deterministicBlob(73, 128);
    byte[] blobB = deterministicBlob(74, 128);
    createV2BlobSource(fqA, row(1, blobA));
    createV2BlobSource(fqB, row(1, blobB));
    try {
      spark.sql(
          "CREATE TABLE "
              + fqTgt
              + " USING lance AS SELECT a.id, a.data AS data_a, b.data AS data_b FROM "
              + fqA
              + " a JOIN "
              + fqB
              + " b ON a.id = b.id");
      List<Long> rowAddrs = rowAddressesOf(fqTgt);
      String tgtUri = datasetUriOf(tgt);
      assertTargetBlobBytes(tgtUri, rowAddrs, "data_a", new byte[][] {blobA});
      assertTargetBlobBytes(tgtUri, rowAddrs, "data_b", new byte[][] {blobB});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqA);
      spark.sql("DROP TABLE IF EXISTS " + fqB);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void ctasIntoCatalogWithPre22Default_upgradesToBlobV2() throws Exception {
    String oldCatalog = "lance_v20_default";
    spark
        .conf()
        .set("spark.sql.catalog." + oldCatalog, "org.lance.spark.LanceNamespaceSparkCatalog");
    spark.conf().set("spark.sql.catalog." + oldCatalog + ".impl", "dir");
    spark
        .conf()
        .set("spark.sql.catalog." + oldCatalog + ".root", tempDir.resolve("v20root").toString());
    spark.conf().set("spark.sql.catalog." + oldCatalog + ".file_format_version", "2.0");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + oldCatalog + ".default");

    String src = "v2_e2e_catdef_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_catdef_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = oldCatalog + ".default." + tgt;
    byte[][] blobs = {deterministicBlob(37, 64)};
    createV2BlobSource(fqSrc, row(1, blobs[0]));
    try {
      spark.sql("CREATE TABLE " + fqTgt + " USING lance AS SELECT id, data FROM " + fqSrc);
      LanceDataset target =
          (LanceDataset)
              ((TableCatalog) spark.sessionState().catalogManager().catalog(oldCatalog))
                  .loadTable(Identifier.of(new String[] {"default"}, tgt));
      assertTrue(
          BlobUtils.hasBlobV2Fields(target.schema()),
          "target should have been created with blob v2 columns, schema: " + target.schema());
      assertTargetBlobBytes(target.readOptions().getDatasetUri(), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void replaceTableAsSelect_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_rtas_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_rtas_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(65, 64), deterministicBlob(66, 4096)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobSource(fqTgt, row(99, deterministicBlob(67, 16)));
    try {
      spark.sql(
          "CREATE OR REPLACE TABLE " + fqTgt + " USING lance AS SELECT id, data FROM " + fqSrc);
      assertEquals(
          2, spark.sql("SELECT * FROM " + fqTgt).count(), "replace must drop the previous row");
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void appendIntoNonEmptyTarget_copiesBytesAndKeepsExistingRows() throws Exception {
    String src = "v2_e2e_append_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_append_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] existing = deterministicBlob(71, 64);
    byte[] appended = deterministicBlob(72, 256);
    createV2BlobSource(fqTgt, row(1, existing));
    createV2BlobSource(fqSrc, row(2, appended));
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc);
      assertEquals(2, spark.sql("SELECT * FROM " + fqTgt).count(), "append must add a row");
      assertTargetBlobBytes(
          datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {existing, appended});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void sameSourceBlobUnderTwoNames_copiesBytesToBothColumns() throws Exception {
    String src = "v2_e2e_dupname_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_dupname_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] blob = deterministicBlob(75, 512);
    createV2BlobSource(fqSrc, row(1, blob));
    createTwoBlobTarget(fqTgt);
    try {
      spark.sql(
          "INSERT INTO " + fqTgt + " SELECT id, data AS data_a, data AS data_b FROM " + fqSrc);
      List<Long> rowAddrs = rowAddressesOf(fqTgt);
      String tgtUri = datasetUriOf(tgt);
      assertTargetBlobBytes(tgtUri, rowAddrs, "data_a", new byte[][] {blob});
      assertTargetBlobBytes(tgtUri, rowAddrs, "data_b", new byte[][] {blob});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void timeTravelSingleSource_copiesPinnedVersionBytes() throws Exception {
    String src = "v2_e2e_ttsingle_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_ttsingle_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] oldBytes = deterministicBlob(85, 64);
    byte[] newBytes = deterministicBlob(86, 96);
    createV2BlobSource(fqSrc, row(1, oldBytes));
    long oldVersion = datasetVersionOf(src);
    spark
        .createDataFrame(Collections.singletonList(row(1, newBytes)), idDataBinarySchema())
        .coalesce(1)
        .writeTo(fqSrc)
        .overwrite(org.apache.spark.sql.functions.lit(true));
    createV2BlobTable(fqTgt);
    try {
      spark.sql(
          "INSERT INTO "
              + fqTgt
              + " SELECT id, data FROM "
              + fqSrc
              + " VERSION AS OF "
              + oldVersion);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {oldBytes});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void insertValuesLiteral_storesLiteralBytes() throws Exception {
    String tgt = "v2_e2e_values_tgt_" + System.currentTimeMillis();
    String fqTgt = fq(tgt);
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " VALUES (1, X'CAFEBABE')");
      assertTargetBlobBytes(
          datasetUriOf(tgt),
          rowAddressesOf(fqTgt),
          new byte[][] {{(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE}});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void dataframeAppend_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_dfappend_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_dfappend_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(111, 64), deterministicBlob(112, 256)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobTable(fqTgt);
    try {
      spark.table(fqSrc).writeTo(fqTgt).append();
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void dataframeOverwrite_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_dfover_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_dfover_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(113, 64)};
    createV2BlobSource(fqSrc, row(1, blobs[0]));
    createV2BlobSource(fqTgt, row(9, deterministicBlob(114, 16)));
    try {
      spark.table(fqSrc).writeTo(fqTgt).overwrite(org.apache.spark.sql.functions.lit(true));
      assertEquals(1, spark.sql("SELECT * FROM " + fqTgt).count());
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void dataframeCreateOrReplace_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_dfcor_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_dfcor_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(115, 64)};
    createV2BlobSource(fqSrc, row(1, blobs[0]));
    try {
      spark.table(fqSrc).writeTo(fqTgt).using("lance").createOrReplace();
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void selfJoinBothSides_copiesBothSidesBlobBytes() throws Exception {
    String src = "v2_e2e_selfboth_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_selfboth_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] blob = deterministicBlob(116, 128);
    createV2BlobSource(fqSrc, row(1, blob));
    createTwoBlobTarget(fqTgt);
    try {
      spark.sql(
          "INSERT INTO "
              + fqTgt
              + " SELECT a.id, a.data AS data_a, b.data AS data_b FROM "
              + fqSrc
              + " a JOIN "
              + fqSrc
              + " b ON a.id = b.id");
      List<Long> rowAddrs = rowAddressesOf(fqTgt);
      String tgtUri = datasetUriOf(tgt);
      assertTargetBlobBytes(tgtUri, rowAddrs, "data_a", new byte[][] {blob});
      assertTargetBlobBytes(tgtUri, rowAddrs, "data_b", new byte[][] {blob});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void whereFilter_copiesOnlyMatchingRows() throws Exception {
    String src = "v2_e2e_where_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_where_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(117, 64), deterministicBlob(118, 64)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc + " WHERE id = 2");
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {blobs[1]});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void largeBlob_copiesAllBytes() throws Exception {
    String src = "v2_e2e_large_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_large_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(119, 4 * 1024 * 1024)};
    createV2BlobSource(fqSrc, row(1, blobs[0]));
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void multiPartitionSource_copiesEveryRow() throws Exception {
    String src = "v2_e2e_multipart_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_multipart_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobTable(fqSrc);
    byte[][] blobs = new byte[8][];
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      blobs[i] = deterministicBlob(120 + i, 64 + i);
      rows.add(row(i + 1, blobs[i]));
    }
    spark.createDataFrame(rows, idDataBinarySchema()).repartition(3).writeTo(fqSrc).append();
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void duplicateBlobBytesAcrossRows_copiesEachRow() throws Exception {
    String src = "v2_e2e_dupbytes_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_dupbytes_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] same = deterministicBlob(130, 64);
    createV2BlobSource(fqSrc, row(1, same), row(2, same));
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), new byte[][] {same, same});
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void emptyBlob_roundTrips() throws Exception {
    String src = "v2_e2e_empty_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_empty_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(131, 64), new byte[0]};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void v1BlobSourceIntoV2Target_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_v1src_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_v1src_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(132, 64), deterministicBlob(133, 4096)};
    createV1BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void crossCatalogCopy_copiesBlobBytes() throws Exception {
    String otherCatalog = "lance_blob_v2_other";
    spark
        .conf()
        .set("spark.sql.catalog." + otherCatalog, "org.lance.spark.LanceNamespaceSparkCatalog");
    spark.conf().set("spark.sql.catalog." + otherCatalog + ".impl", "dir");
    spark
        .conf()
        .set("spark.sql.catalog." + otherCatalog + ".root", tempDir.resolve("othercat").toString());
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + otherCatalog + ".default");
    String src = "v2_e2e_xcat_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_xcat_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = otherCatalog + ".default." + tgt;
    byte[][] blobs = {deterministicBlob(134, 64)};
    createV2BlobSource(fqSrc, row(1, blobs[0]));
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTgt
            + " (id INT NOT NULL, data BINARY) USING lance TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob', 'file_format_version' = '2.2')");
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc);
      org.apache.spark.sql.connector.catalog.TableCatalog cat =
          (org.apache.spark.sql.connector.catalog.TableCatalog)
              spark.sessionState().catalogManager().catalog(otherCatalog);
      LanceDataset target =
          (LanceDataset)
              cat.loadTable(
                  org.apache.spark.sql.connector.catalog.Identifier.of(
                      new String[] {"default"}, tgt));
      List<Row> addrRows =
          spark
              .sql("SELECT " + LanceConstant.ROW_ADDRESS + " FROM " + fqTgt + " ORDER BY id")
              .collectAsList();
      List<Long> addrs = new ArrayList<>();
      for (Row r : addrRows) {
        addrs.add(r.getLong(0));
      }
      assertTargetBlobBytes(target.readOptions().getDatasetUri(), addrs, blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void cteInsert_copiesOnSpark3FailsLoudlyOnSpark4() throws Exception {
    String src = "v2_e2e_cte_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_cte_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(150, 64)};
    createV2BlobSource(fqSrc, row(1, blobs[0]));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "WITH x AS (SELECT id, data FROM "
              + fqSrc
              + ") INSERT INTO "
              + fqTgt
              + " SELECT id, data FROM x";
      if (spark.version().startsWith("4")) {
        Exception ex = assertThrows(IllegalArgumentException.class, () -> spark.sql(sql));
        assertTrue(ex.getMessage().contains("binary"), ex.getMessage());
        assertEquals(0, spark.sql("SELECT * FROM " + fqTgt).count());
      } else {
        spark.sql(sql);
        assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
      }
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void windowFunctionInQuery_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_window_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_window_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(151, 64), deterministicBlob(152, 256)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTgt
            + " (id INT NOT NULL, data BINARY, rn INT) USING lance TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob', 'file_format_version' = '2.2')");
    try {
      spark.sql(
          "INSERT INTO "
              + fqTgt
              + " SELECT id, data, CAST(row_number() OVER (ORDER BY id) AS INT) AS rn FROM "
              + fqSrc);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void directPathSave_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_dpath_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_dpath_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(160, 64)};
    createV2BlobSource(fqSrc, row(1, blobs[0]));
    createV2BlobTable(fqTgt);
    String tgtUri = datasetUriOf(tgt);
    try {
      spark
          .table(fqSrc)
          .write()
          .format("lance")
          .option(LanceSparkReadOptions.CONFIG_DATASET_URI, tgtUri)
          .mode("append")
          .save();
      assertTargetBlobBytes(tgtUri, rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void namedVersionSourceIntoV2Target_copiesBlobBytes() throws Exception {
    String src = "v2_e2e_namedver_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_namedver_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(161, 64)};
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqSrc
            + " (id INT NOT NULL, data BINARY) USING lance TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob', 'file_format_version' = 'stable')");
    spark
        .createDataFrame(java.util.Arrays.asList(row(1, blobs[0])), idDataBinarySchema())
        .coalesce(1)
        .writeTo(fqSrc)
        .append();
    createV2BlobTable(fqTgt);
    try {
      spark.sql("INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc);
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void tablesample_copiesSurvivingRowsBytes() throws Exception {
    String src = "v2_e2e_sample_src_" + System.currentTimeMillis();
    String tgt = "v2_e2e_sample_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[][] blobs = {deterministicBlob(171, 64), deterministicBlob(172, 256)};
    createV2BlobSource(fqSrc, row(1, blobs[0]), row(2, blobs[1]));
    createV2BlobTable(fqTgt);
    try {
      spark.sql(
          "INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc + " TABLESAMPLE (100 PERCENT)");
      assertTargetBlobBytes(datasetUriOf(tgt), rowAddressesOf(fqTgt), blobs);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }
}
