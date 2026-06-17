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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.optimizer.BlobPlanUtils.{LanceRelation, V2BlobWrite}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, LanceSerializeUtil}
import org.lance.spark.{LanceConstant, LanceDataset, LanceSparkReadOptions}
import org.lance.spark.read.LanceScan
import org.lance.spark.utils.{BlobSourceContext, BlobUtils, Utils}

import scala.util.control.NonFatal

/**
 * Encodes blob source credentials into Lance write options so write tasks can reopen source
 * datasets for [[org.lance.spark.utils.BlobReference]] copy-through.
 */
case class LanceBlobSourceContextRule() extends Rule[LogicalPlan] {

  import LanceBlobSourceContextRule._

  // Context key lives on command writeOptions only. V2Writes.mergeOptions requires matching options
  // or one side empty; INSERT/OVERWRITE targets resolve with empty relation options.
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case V2BlobWrite(write) if shouldAnnotate(write.table, write.writeOptions) =>
      val (table, writeOptions) = annotate(write.query, write.table, write.writeOptions)
      write.withTableAndOptions(table, writeOptions)
  }

  private def annotate(
      query: LogicalPlan,
      table: NamedRelation,
      writeOptions: Map[String, String]): (NamedRelation, Map[String, String]) =
    encodeBlobSourceContexts(query) match {
      case None => (table, writeOptions)
      case Some(encoded) =>
        val relationCarriesOptions = table match {
          case relation: DataSourceV2Relation => !relation.options.isEmpty
          case _ => false
        }
        val annotatedTable = table match {
          case relation: DataSourceV2Relation if relationCarriesOptions =>
            val merged = new java.util.HashMap[String, String](relation.options)
            merged.put(key, encoded)
            relation.copy(options = new CaseInsensitiveStringMap(merged))
          case other => other
        }
        val annotatedWriteOptions =
          if (writeOptions.nonEmpty || !relationCarriesOptions) writeOptions + (key -> encoded)
          else writeOptions
        (annotatedTable, annotatedWriteOptions)
    }

  private def shouldAnnotate(table: NamedRelation, writeOptions: Map[String, String]): Boolean =
    !writeOptions.contains(key) && !annotatedOnRelation(table) && isLanceTarget(table)

  private def annotatedOnRelation(table: NamedRelation): Boolean = table match {
    case relation: DataSourceV2Relation => relation.options.containsKey(key)
    case _ => false
  }

  private def isLanceTarget(table: NamedRelation): Boolean = table match {
    case LanceRelation(_, _) => true
    case _ => false
  }
}

object LanceBlobSourceContextRule extends Logging {

  val key: String = LanceConstant.BLOB_SOURCE_CONTEXTS_KEY

  /** Adds the encoded blob source contexts for {@code query} to {@code writeOptions}, if any. */
  def annotateWriteOptions(
      query: LogicalPlan,
      writeOptions: Map[String, String]): Map[String, String] =
    encodeBlobSourceContexts(query) match {
      case Some(encoded) => writeOptions + (key -> encoded)
      case None => writeOptions
    }

  def encodeBlobSourceContexts(query: LogicalPlan): Option[String] = {
    // Last relation per URI wins; pinning runs after dedup.
    val sources =
      new java.util.LinkedHashMap[String, (LanceDataset, Option[LanceSparkReadOptions])]()
    (query +: query.subqueriesAll).foreach { plan =>
      plan.foreach { node =>
        blobSource(node).foreach { case source @ (ds, _) =>
          sources.put(ds.readOptions().getDatasetUri, source)
        }
      }
    }
    encodeContexts(sources)
  }

  /** Pinned source contexts for row-level copy (WriteDelta cannot carry write options). */
  def contextsForDatasets(datasets: Seq[LanceDataset]): java.util.Map[String, BlobSourceContext] = {
    val sources =
      new java.util.LinkedHashMap[String, (LanceDataset, Option[LanceSparkReadOptions])]()
    datasets.foreach(ds => sources.put(ds.readOptions().getDatasetUri, (ds, None)))
    buildContexts(sources)
  }

  private def blobSource(
      node: LogicalPlan): Option[(LanceDataset, Option[LanceSparkReadOptions])] = node match {
    case LanceRelation(_, ds) if hasBlobColumns(ds) =>
      Some((ds, None))
    case sr: DataSourceV2ScanRelation =>
      LanceRelation.unapply(sr.relation).collect {
        case (_, ds) if hasBlobColumns(ds) =>
          sr.scan match {
            case scan: LanceScan => (ds, Some(scan.readOptions()))
            case _ => (ds, None)
          }
      }
    case _ => None
  }

  // Pin to the driver-visible version when time travel is not set; fall back on open failure.
  private def pinToCurrentVersion(ds: LanceDataset): LanceSparkReadOptions = {
    val opts = ds.readOptions()
    if (opts.getVersion != null) {
      return opts
    }
    try {
      val dataset = Utils.openDatasetBuilder(opts).build()
      try opts.withVersion(dataset.version())
      finally dataset.close()
    } catch {
      case NonFatal(e) =>
        logWarning(
          s"Could not pin a dataset version for blob source '${opts.getDatasetUri}'; " +
            s"blob references will resolve at the latest version: ${e.getMessage}")
        opts
    }
  }

  private def hasBlobColumns(ds: LanceDataset): Boolean =
    ds.schema().fields.exists(f => BlobUtils.isBlobReadColumn(f))

  private def encodeContexts(
      sources: java.util.LinkedHashMap[String, (LanceDataset, Option[LanceSparkReadOptions])])
      : Option[String] = {
    val contexts = buildContexts(sources)
    if (contexts.isEmpty) None else Some(LanceSerializeUtil.encode(contexts))
  }

  private def buildContexts(
      sources: java.util.LinkedHashMap[String, (LanceDataset, Option[LanceSparkReadOptions])])
      : java.util.Map[String, BlobSourceContext] = {
    val contexts = new java.util.HashMap[String, BlobSourceContext]()
    sources.forEach { (uri, source) =>
      val (ds, scanPinned) = source
      contexts.put(
        uri,
        new BlobSourceContext(
          scanPinned.getOrElse(pinToCurrentVersion(ds)),
          ds.getInitialStorageOptions(),
          ds.getNamespaceImpl(),
          ds.getNamespaceProperties()))
    }
    contexts
  }
}
