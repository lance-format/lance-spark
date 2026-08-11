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
package org.lance.spark.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.optimizer.{LanceBlobSourceContextRule, LanceBlobV2CopyThroughRule, LanceBlobV2RowLevelCopyRule, LanceBlobV2RowLevelResolutionRule, LanceFragmentAwareJoinRule, LanceFtsPredicateRule, LanceInsertOnlyMergeUnwrapRule}
import org.apache.spark.sql.catalyst.parser.extensions.LanceSparkSqlExtensionsParser
import org.apache.spark.sql.execution.datasources.v2.LanceDataSourceV2Strategy
import org.lance.spark.search.LanceSearchTableFunctions

class LanceSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // parser extensions
    extensions.injectParser { case (_, parser) => new LanceSparkSqlExtensionsParser(parser) }

    // blob v2 copy-through for CTAS/INSERT (resolution rule)
    extensions.injectResolutionRule(_ => LanceBlobV2CopyThroughRule())

    extensions.injectResolutionRule(_ => LanceBlobV2RowLevelResolutionRule())

    // Spark 4.2 lowers insert-only MERGE to InsertOnlyMerge (not AppendData); unwrap it back to
    // AppendData for masked blob v2 targets so the copy rule below runs. Must precede it.
    extensions.injectOptimizerRule(_ => LanceInsertOnlyMergeUnwrapRule())

    extensions.injectOptimizerRule(_ => LanceBlobV2RowLevelCopyRule())

    // optimizer rules for fragment-aware joins
    extensions.injectOptimizerRule(_ => LanceFragmentAwareJoinRule())

    // propagate blob source credentials from read scans to the write side
    extensions.injectOptimizerRule(_ => LanceBlobSourceContextRule())

    // FTS predicate pushdown rule
    extensions.injectOptimizerRule(_ => new LanceFtsPredicateRule())

    extensions.injectTableFunction(
      (
        FunctionIdentifier("vector_search"),
        new ExpressionInfo(
          "org.lance.spark.search.LanceSearchTableFunctions",
          "vector_search"),
        LanceSearchTableFunctions.vectorSearch _))
    extensions.injectTableFunction(
      (
        FunctionIdentifier("search"),
        new ExpressionInfo(
          "org.lance.spark.search.LanceSearchTableFunctions",
          "search"),
        LanceSearchTableFunctions.search _))
    extensions.injectTableFunction(
      (
        FunctionIdentifier("hybrid_search"),
        new ExpressionInfo(
          "org.lance.spark.search.LanceSearchTableFunctions",
          "hybrid_search"),
        LanceSearchTableFunctions.hybridSearch _))

    extensions.injectPlannerStrategy(LanceDataSourceV2Strategy(_))
  }
}
