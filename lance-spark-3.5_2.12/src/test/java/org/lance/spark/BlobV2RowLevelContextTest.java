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

import org.lance.spark.utils.BlobSourceContext;

import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationTable;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.Test;
import scala.collection.Seq;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BlobV2RowLevelContextTest extends AbstractBlobV2CopyTest {

  @Test
  public void testMergeCopyingSourceBlobAttachesPinnedSourceContexts() throws Exception {
    String src = "v2_ctx_src_" + System.currentTimeMillis();
    String tgt = "v2_ctx_tgt_" + System.currentTimeMillis();
    String fqSrc = fq(src);
    String fqTgt = fq(tgt);
    createV2BlobSource(fqSrc, row(1, deterministicBlob(10, 16)));
    createV2BlobTagTarget(fqTgt);
    spark.sql("INSERT INTO " + fqTgt + " VALUES (1, X'00', 'a')");
    try {
      LancePositionDeltaOperation operation =
          deltaOperation(
              "MERGE INTO "
                  + fqTgt
                  + " t USING "
                  + fqSrc
                  + " s ON t.id = s.id"
                  + " WHEN MATCHED THEN UPDATE SET t.data = s.data");
      assertNotNull(operation);

      Map<String, BlobSourceContext> contexts = operation.blobSourceContexts();
      assertFalse(contexts.isEmpty());
      contexts.values().forEach(context -> assertNotNull(context.getReadOptions().getVersion()));
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fqSrc);
      spark.sql("DROP TABLE IF EXISTS " + fqTgt);
    }
  }

  private LancePositionDeltaOperation deltaOperation(String sql) throws ParseException {
    LogicalPlan parsed = spark.sessionState().sqlParser().parsePlan(sql);
    LogicalPlan analyzed = spark.sessionState().analyzer().execute(parsed);
    LogicalPlan optimized = spark.sessionState().optimizer().execute(analyzed);
    return findDeltaOperation(optimized);
  }

  private static LancePositionDeltaOperation findDeltaOperation(LogicalPlan plan) {
    if (plan instanceof WriteDelta) {
      NamedRelation target = ((WriteDelta) plan).table();
      if (target instanceof DataSourceV2Relation) {
        Table table = ((DataSourceV2Relation) target).table();
        if (table instanceof RowLevelOperationTable) {
          RowLevelOperation operation = ((RowLevelOperationTable) table).operation();
          if (operation instanceof LancePositionDeltaOperation) {
            return (LancePositionDeltaOperation) operation;
          }
        }
      }
    }
    Seq<LogicalPlan> children = plan.children();
    for (int i = 0; i < children.length(); i++) {
      LancePositionDeltaOperation found = findDeltaOperation(children.apply(i));
      if (found != null) {
        return found;
      }
    }
    return null;
  }
}
