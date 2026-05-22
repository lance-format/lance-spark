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
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.util.LanceSerializeUtil
import org.lance.spark.{LanceConstant, LanceDataset}
import org.lance.spark.utils.{BlobSourceContext, BlobUtils}

/**
 * Optimizer rule that propagates blob source credentials to the write side.
 *
 * When a Lance table with blob columns is read and its blob columns flow through a shuffle into a
 * write (e.g. `INSERT INTO target SELECT ... [JOIN ...]`), the blob bytes are not materialized: a
 * compact reference carrying the source dataset URI travels instead. To resolve those references the
 * write executors must reopen the source dataset — but Spark's DSv2 write is never handed the read
 * plan, so it has no way to learn the source's credentials.
 *
 * This rule bridges that gap on the driver, where both the write command and its source query are
 * visible: it collects each blob source's [[BlobSourceContext]] (read options + namespace config for
 * credential refresh), encodes them keyed by source URI, and stashes the result in the write
 * command's options under [[LanceConstant.BLOB_SOURCE_CONTEXTS_KEY]]. `LanceDataset.newWriteBuilder`
 * decodes it and threads it down to the per-task blob resolver. This keeps the credential context
 * query-scoped (no global state) and rides the write's own options channel rather than bloating
 * every shuffled row.
 *
 * No-op when the target is not a Lance table or the source query has no Lance blob tables.
 */
case class LanceBlobSourceContextRule() extends Rule[LogicalPlan] {

  private val key = LanceConstant.BLOB_SOURCE_CONTEXTS_KEY

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case a: AppendData if shouldAnnotate(a.table, a.writeOptions) =>
      annotate(a.query) match {
        case Some(v) => a.copy(writeOptions = a.writeOptions + (key -> v))
        case None => a
      }
    case o: OverwriteByExpression if shouldAnnotate(o.table, o.writeOptions) =>
      annotate(o.query) match {
        case Some(v) => o.copy(writeOptions = o.writeOptions + (key -> v))
        case None => o
      }
    case o: OverwritePartitionsDynamic if shouldAnnotate(o.table, o.writeOptions) =>
      annotate(o.query) match {
        case Some(v) => o.copy(writeOptions = o.writeOptions + (key -> v))
        case None => o
      }
  }

  private def shouldAnnotate(table: NamedRelation, writeOptions: Map[String, String]): Boolean =
    !writeOptions.contains(key) && isLanceTarget(table)

  private def isLanceTarget(table: NamedRelation): Boolean = table match {
    case r: DataSourceV2Relation => r.table.isInstanceOf[LanceDataset]
    case _ => false
  }

  /** Encodes {sourceUri -> context} for the query's blob sources, or None if there are none. */
  private def annotate(query: LogicalPlan): Option[String] = {
    val contexts = new java.util.HashMap[String, BlobSourceContext]()
    query.foreach { node =>
      lanceTableWithBlobs(node).foreach { ds =>
        contexts.put(
          ds.readOptions().getDatasetUri,
          new BlobSourceContext(
            ds.readOptions(),
            ds.getInitialStorageOptions(),
            ds.getNamespaceImpl(),
            ds.getNamespaceProperties()))
      }
    }
    if (contexts.isEmpty) None else Some(LanceSerializeUtil.encode(contexts))
  }

  /** Returns the Lance table backing a relation iff it has blob columns (pre- or post-pushdown). */
  private def lanceTableWithBlobs(node: LogicalPlan): Option[LanceDataset] = {
    val table: Option[Table] = node match {
      case r: DataSourceV2Relation => Some(r.table)
      case sr: DataSourceV2ScanRelation => Some(sr.relation.table)
      case _ => None
    }
    table.collect { case ds: LanceDataset if hasBlobColumns(ds) => ds }
  }

  private def hasBlobColumns(ds: LanceDataset): Boolean =
    ds.schema().fields.exists((f: StructField) => BlobUtils.isBlobSparkField(f))
}
