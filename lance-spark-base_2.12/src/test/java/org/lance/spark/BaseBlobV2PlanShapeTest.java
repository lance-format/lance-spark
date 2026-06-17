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

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseBlobV2PlanShapeTest extends AbstractBlobV2CopyTest {

  @Test
  public void directColumnRef_injectsCopyRef() throws Exception {
    String src = "v2_plan_direct_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_direct_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(1, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql = "INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc;
      LogicalPlan plan = analyzePlan(sql);
      int copyRefs = countCopyRefs(plan);
      List<String> rowAddrs = rowAddressColumnNames(plan);
      assertEquals(1, copyRefs, "expected 1 CopyRef, got " + copyRefs + "\nplan:\n" + plan);
      assertTrue(
          rowAddrs.contains("_rowaddr"),
          "row address should reference _rowaddr, got " + rowAddrs + "\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void aliasOfRef_injectsCopyRef() throws Exception {
    String src = "v2_plan_alias_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_alias_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(2, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql = "INSERT INTO " + fqTgt + " SELECT id, data AS data FROM " + fqSrc;
      LogicalPlan plan = analyzePlan(sql);
      int copyRefs = countCopyRefs(plan);
      List<String> rowAddrs = rowAddressColumnNames(plan);
      assertEquals(1, copyRefs, "expected 1 CopyRef, got " + copyRefs + "\nplan:\n" + plan);
      assertTrue(
          rowAddrs.contains("_rowaddr"),
          "row address should reference _rowaddr, got " + rowAddrs + "\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void ctas_injectsCopyRef() throws Exception {
    String src = "v2_plan_ctas_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_ctas_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(3, 16)));
    try {
      String sql = "CREATE TABLE " + fqTgt + " USING lance AS SELECT id, data FROM " + fqSrc;
      LogicalPlan plan = analyzePlan(sql);
      int copyRefs = countCopyRefs(plan);
      List<String> rowAddrs = rowAddressColumnNames(plan);
      assertEquals(1, copyRefs, "expected 1 CopyRef, got " + copyRefs + "\nplan:\n" + plan);
      assertTrue(
          rowAddrs.contains("_rowaddr"),
          "row address should reference _rowaddr, got " + rowAddrs + "\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
    }
  }

  @Test
  public void transformedDescriptor_noCopyRef() throws Exception {
    String src = "v2_plan_xform_src_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(4, 16)));
    try {
      String sql = "SELECT id, data.size AS sz FROM " + fqSrc;
      LogicalPlan plan = analyzePlan(sql);
      int copyRefs = countCopyRefs(plan);
      assertEquals(
          0,
          copyRefs,
          "expected 0 CopyRef for transformed descriptor, got " + copyRefs + "\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
    }
  }

  @Test
  public void singleSourceBlobInJoin_injectsOneCopyRef() throws Exception {
    String srcA = "v2_plan_multi_a_" + System.currentTimeMillis();
    String srcB = "v2_plan_multi_b_" + System.currentTimeMillis();
    String tgt = "v2_plan_multi_tgt_" + System.currentTimeMillis();
    String fqA = fq(srcA);
    String fqB = fq(srcB);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqA, row(1, deterministicBlob(5, 16)));
    spark.sql("CREATE TABLE " + fqB + " (id INT NOT NULL, tag STRING) USING lance");
    spark
        .createDataFrame(Collections.singletonList(RowFactory.create(1, "x")), tagSchema())
        .coalesce(1)
        .writeTo(fqB)
        .append();
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT a.id, a.data FROM "
              + fqA
              + " a JOIN "
              + fqB
              + " b ON a.id = b.id";
      LogicalPlan plan = analyzePlan(sql);
      int copyRefs = countCopyRefs(plan);
      assertEquals(
          1,
          copyRefs,
          "expected 1 CopyRef for blob from one join side, got " + copyRefs + "\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqA);
      spark.sql("DROP TABLE IF EXISTS " + fqB);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void twoBlobSourcesInJoin_injectsTwoCopyRefs() throws Exception {
    String srcA = "v2_plan_two_a_" + System.currentTimeMillis();
    String srcB = "v2_plan_two_b_" + System.currentTimeMillis();
    String tgt = "v2_plan_two_tgt_" + System.currentTimeMillis();
    String fqA = fq(srcA);
    String fqB = fq(srcB);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqA, row(1, deterministicBlob(5, 16)));
    createV2BlobSource(fqB, row(1, deterministicBlob(6, 16)));
    createTwoBlobTarget(fqTgt);
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT a.id, a.data AS data_a, b.data AS data_b FROM "
              + fqA
              + " a JOIN "
              + fqB
              + " b ON a.id = b.id";
      LogicalPlan plan = analyzePlan(sql);
      int copyRefs = countCopyRefs(plan);
      assertEquals(
          2,
          copyRefs,
          "expected 2 CopyRefs for two blob join sides, got " + copyRefs + "\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqA);
      spark.sql("DROP TABLE IF EXISTS " + fqB);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void orderByBlobColumn_noCopyRef() throws Exception {
    String src = "v2_plan_orderblob_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_orderblob_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(35, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql = "INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc + " ORDER BY data";
      int copyRefs = countCopyRefs(analyzePlan(sql));
      assertEquals(0, copyRefs, "expected 0 CopyRef when ordering by the blob column itself");
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void distinctSelect_noCopyRef() throws Exception {
    String src = "v2_plan_distinct_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_distinct_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(36, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql = "INSERT INTO " + fqTgt + " SELECT DISTINCT id, data FROM " + fqSrc;
      int copyRefs = countCopyRefs(analyzePlan(sql));
      assertEquals(0, copyRefs, "expected 0 CopyRef under DISTINCT");
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void nonLanceCatalogCtas_noCopyRef() throws Exception {
    String src = "v2_plan_noncat_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_noncat_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(8, 16)));
    try {
      String sql = "CREATE TABLE " + tgt + " USING parquet AS SELECT id, data FROM " + fqSrc;
      LogicalPlan plan = analyzePlan(sql);
      int copyRefs = countCopyRefs(plan);
      assertEquals(
          0,
          copyRefs,
          "expected 0 CopyRef for non-Lance catalog CTAS, got " + copyRefs + "\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
    }
  }

  @Test
  public void timeTravelJoinOneSideCopied_noCopyRef() throws Exception {
    String src = "v2_plan_ttjoin_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_ttjoin_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    byte[] oldBytes = deterministicBlob(81, 64);
    byte[] newBytes = deterministicBlob(82, 96);
    createV2BlobSource(fqSrc, row(1, oldBytes));
    long oldVersion = datasetVersionOf(src);
    spark
        .createDataFrame(Collections.singletonList(row(1, newBytes)), idDataBinarySchema())
        .coalesce(1)
        .writeTo(fqSrc)
        .overwrite(org.apache.spark.sql.functions.lit(true));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT o.id, o.data FROM "
              + fqSrc
              + " VERSION AS OF "
              + oldVersion
              + " o JOIN "
              + fqSrc
              + " c ON o.id = c.id";
      LogicalPlan plan = analyzePlan(sql);
      assertEquals(
          0,
          countCopyRefs(plan),
          "same URI at two versions is ambiguous; no CopyRef may be injected\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void timeTravelJoinBothSidesCopied_noCopyRef() throws Exception {
    String src = "v2_plan_ttjoin2_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_ttjoin2_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(83, 64)));
    long oldVersion = datasetVersionOf(src);
    spark
        .createDataFrame(
            Collections.singletonList(row(1, deterministicBlob(84, 96))), idDataBinarySchema())
        .coalesce(1)
        .writeTo(fqSrc)
        .overwrite(org.apache.spark.sql.functions.lit(true));
    createTwoBlobTarget(fqTgt);
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT o.id, o.data AS data_a, c.data AS data_b FROM "
              + fqSrc
              + " VERSION AS OF "
              + oldVersion
              + " o JOIN "
              + fqSrc
              + " c ON o.id = c.id";
      LogicalPlan plan = analyzePlan(sql);
      assertEquals(
          0,
          countCopyRefs(plan),
          "same URI at two versions is ambiguous; no CopyRef may be injected\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void timeTravelInSubqueryFilter_noCopyRef() throws Exception {
    String src = "v2_plan_ttsubq_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_ttsubq_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(87, 64)));
    long oldVersion = datasetVersionOf(src);
    spark
        .createDataFrame(
            Collections.singletonList(row(1, deterministicBlob(88, 96))), idDataBinarySchema())
        .coalesce(1)
        .writeTo(fqSrc)
        .overwrite(org.apache.spark.sql.functions.lit(true));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT id, data FROM "
              + fqSrc
              + " WHERE id IN (SELECT id FROM "
              + fqSrc
              + " VERSION AS OF "
              + oldVersion
              + ")";
      LogicalPlan plan = analyzePlan(sql);
      assertEquals(
          0,
          countCopyRefs(plan),
          "a subquery reading the same URI at another version is ambiguous; no CopyRef may be"
              + " injected\nplan:\n"
              + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void timeTravelInSelectListSubquery_noCopyRef() throws Exception {
    String src = "v2_plan_ttscalar_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_ttscalar_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(90, 64)));
    long oldVersion = datasetVersionOf(src);
    spark
        .createDataFrame(
            Collections.singletonList(row(1, deterministicBlob(91, 96))), idDataBinarySchema())
        .coalesce(1)
        .writeTo(fqSrc)
        .overwrite(org.apache.spark.sql.functions.lit(true));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT (SELECT MIN(id) FROM "
              + fqSrc
              + " VERSION AS OF "
              + oldVersion
              + ") AS id, data FROM "
              + fqSrc;
      LogicalPlan plan = analyzePlan(sql);
      assertEquals(
          0,
          countCopyRefs(plan),
          "a SELECT-list subquery reading the same URI at another version is ambiguous; no"
              + " CopyRef may be injected\nplan:\n"
              + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void blobIntoNonBlobTargetColumn_noCopyRefForThatColumn() throws Exception {
    String src = "v2_plan_gate_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_gate_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(61, 16)));
    createBlobAndPlainBinaryTarget(fqTgt);
    try {
      String sql = "INSERT INTO " + fqTgt + " SELECT id, data AS data, data AS note FROM " + fqSrc;
      LogicalPlan plan = analyzePlan(sql);
      int copyRefs = countCopyRefs(plan);
      assertEquals(
          1,
          copyRefs,
          "only the blob-column projection may be tokenized, got " + copyRefs + "\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void sameSourceBlobUnderTwoNames_injectsTwoCopyRefs() throws Exception {
    String src = "v2_plan_dupname_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_dupname_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(62, 16)));
    createTwoBlobTarget(fqTgt);
    try {
      String sql =
          "INSERT INTO " + fqTgt + " SELECT id, data AS data_a, data AS data_b FROM " + fqSrc;
      LogicalPlan plan = analyzePlan(sql);
      int copyRefs = countCopyRefs(plan);
      assertEquals(
          2,
          copyRefs,
          "the same blob under two names should yield two CopyRefs, got "
              + copyRefs
              + "\nplan:\n"
              + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void replaceTableAsSelect_injectsCopyRef() throws Exception {
    String src = "v2_plan_rtas_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_rtas_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(63, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "CREATE OR REPLACE TABLE " + fqTgt + " USING lance AS SELECT id, data FROM " + fqSrc;
      LogicalPlan plan = analyzePlan(sql);
      int copyRefs = countCopyRefs(plan);
      assertEquals(
          1, copyRefs, "expected 1 CopyRef for RTAS, got " + copyRefs + "\nplan:\n" + plan);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void blobSourceContext_pinsScanVersion() throws Exception {
    String src = "v2_plan_ctxpin_src_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(89, 64)));
    long version = datasetVersionOf(src);
    try {
      LogicalPlan query = analyzePlan("SELECT id, data FROM " + fqSrc);
      java.util.Map<String, Long> versions = BlobPlanProbe.blobSourceContextVersions(query);
      assertEquals(1, versions.size(), "expected one blob source context");
      assertEquals(
          version,
          versions.values().iterator().next(),
          "the context must pin the version the query reads, not float to latest");
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
    }
  }

  @Test
  public void caseOverDescriptor_noCopyRef() throws Exception {
    String src = "v2_plan_case_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_case_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(101, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT id, CASE WHEN id > 0 THEN data ELSE data END AS data FROM "
              + fqSrc;
      assertEquals(0, countCopyRefs(analyzePlan(sql)));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void unionAll_noCopyRef() throws Exception {
    String srcA = "v2_plan_union_a_" + System.currentTimeMillis();
    String srcB = "v2_plan_union_b_" + System.currentTimeMillis();
    String tgt = "v2_plan_union_tgt_" + System.currentTimeMillis();
    String fqA = fq(srcA);
    String fqB = fq(srcB);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqA, row(1, deterministicBlob(102, 16)));
    createV2BlobSource(fqB, row(2, deterministicBlob(103, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT id, data FROM "
              + fqA
              + " UNION ALL SELECT id, data FROM "
              + fqB;
      assertEquals(0, countCopyRefs(analyzePlan(sql)));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqA);
      spark.sql("DROP TABLE IF EXISTS " + fqB);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void groupByFirstBlob_noCopyRef() throws Exception {
    String src = "v2_plan_group_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_group_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(104, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "INSERT INTO " + fqTgt + " SELECT id, first(data) AS data FROM " + fqSrc + " GROUP BY id";
      assertEquals(0, countCopyRefs(analyzePlan(sql)));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void windowFunctionInQuery_injectsCopyRef() throws Exception {
    String src = "v2_plan_window_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_window_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(105, 16)));
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + fqTgt
            + " (id INT NOT NULL, data BINARY, rn INT) USING lance TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob', 'file_format_version' = '2.2')");
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT id, data, CAST(row_number() OVER (ORDER BY id) AS INT) AS rn FROM "
              + fqSrc;
      assertEquals(1, countCopyRefs(analyzePlan(sql)));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void cteInsert_injectsCopyRefOnSpark3() throws Exception {
    String src = "v2_plan_cte_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_cte_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(106, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "WITH x AS (SELECT id, data FROM "
              + fqSrc
              + ") INSERT INTO "
              + fqTgt
              + " SELECT id, data FROM x";
      int expected = spark.version().startsWith("4") ? 0 : 1;
      assertEquals(expected, countCopyRefs(analyzePlan(sql)));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void v1BlobSource_noCopyRef() throws Exception {
    String src = "v2_plan_v1src_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_v1src_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV1BlobSource(fqSrc, row(1, deterministicBlob(107, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql = "INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc;
      assertEquals(0, countCopyRefs(analyzePlan(sql)));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void selfJoinBothSides_injectsTwoCopyRefs() throws Exception {
    String src = "v2_plan_selfboth_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_selfboth_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(108, 16)));
    createTwoBlobTarget(fqTgt);
    try {
      String sql =
          "INSERT INTO "
              + fqTgt
              + " SELECT a.id, a.data AS data_a, b.data AS data_b FROM "
              + fqSrc
              + " a JOIN "
              + fqSrc
              + " b ON a.id = b.id";
      assertEquals(2, countCopyRefs(analyzePlan(sql)));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void whereFilter_injectsCopyRef() throws Exception {
    String src = "v2_plan_where_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_where_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(109, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql = "INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc + " WHERE id > 0";
      assertEquals(1, countCopyRefs(analyzePlan(sql)));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  @Test
  public void blobSourceContext_carriesNamespaceImpl() throws Exception {
    String src = "v2_plan_ctxns_src_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(110, 16)));
    try {
      java.util.Map<String, String> impls =
          BlobPlanProbe.blobSourceContextNamespaceImpls(
              analyzePlan("SELECT id, data FROM " + fqSrc));
      assertEquals(1, impls.size());
      assertEquals("dir", impls.values().iterator().next());
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
    }
  }

  @Test
  public void tablesample_injectsCopyRef() throws Exception {
    String src = "v2_plan_sample_src_" + System.currentTimeMillis();
    String tgt = "v2_plan_sample_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(170, 16)));
    createV2BlobTable(fqTgt);
    try {
      String sql =
          "INSERT INTO " + fqTgt + " SELECT id, data FROM " + fqSrc + " TABLESAMPLE (50 PERCENT)";
      assertEquals(1, countCopyRefs(analyzePlan(sql)));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }
}
