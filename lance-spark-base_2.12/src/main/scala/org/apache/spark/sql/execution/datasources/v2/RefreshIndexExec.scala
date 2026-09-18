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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{LanceNamedArgument, RefreshIndexOutputType}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.Dataset
import org.lance.index.IndexType
import org.lance.spark.{LanceDataset, LanceSparkReadOptions}
import org.lance.spark.utils.{FieldPathUtils, Utils}

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Physical execution of distributed REFRESH INDEX (ALTER TABLE ... REFRESH INDEX ...).
 *
 * <p>Indexes only the fragments the named index does not cover, then adds the resulting segments
 * to it. Cost scales with new data, not the table.
 *
 * <p>Build options come from the {@code WITH} clause, not the existing index: Lance does not
 * expose a built index's parameters as readable metadata. For most types a mismatch only changes
 * performance; for {@code fts} it is rejected before commit — see
 * {@code IndexUtils.requiresUniformSegmentDetails}.
 */
case class RefreshIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    indexName: String,
    args: Seq[LanceNamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = RefreshIndexOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = LanceDataset.requireWritable(catalog.loadTable(ident), "RefreshIndex")
    val readOptions = lanceDataset.readOptions()

    val numSegments = validateArgs()

    val plan = {
      val ds = Utils.openDatasetBuilder(readOptions).build()
      try {
        planRefresh(ds, readOptions)
      } finally {
        ds.close()
      }
    }

    if (plan.unindexedFragments.isEmpty) {
      logInfo(s"Index '${plan.resolvedName}' already covers every fragment; nothing to refresh")
      return Seq(noOpResult(plan.resolvedName))
    }

    val (nsImpl, nsProps, tableId, initialStorageOpts) =
      IndexUtils.extractNamespaceInfo(catalog, lanceDataset, readOptions)

    val segments = new ScalarSegmentIndexJob(
      session.sparkContext,
      plan.resolvedName,
      plan.method,
      List(plan.column),
      IndexUtils.toJson(args),
      plan.buildReadOptions,
      plan.unindexedFragments,
      numSegments,
      nsImpl,
      nsProps,
      tableId,
      initialStorageOpts).run()

    val dataset = Utils.openDatasetBuilder(readOptions).build()
    val fragmentsIndexed =
      try {
        val liveFragmentIds = IndexUtils.liveFragmentIds(dataset)
        val retainedSegments =
          IndexUtils.resolveRetainedSegments(dataset, plan.resolvedName, liveFragmentIds)
        IndexUtils.requireUniformSegmentDetails(
          plan.indexType,
          plan.resolvedName,
          plan.method,
          retainedSegments,
          segments)
        IndexUtils.requireCommittableCoverage(liveFragmentIds, segments, plan.resolvedName)
        val committed =
          dataset.commitExistingIndexSegments(
            plan.resolvedName,
            plan.column,
            segments.toList.asJava)
        IndexUtils
          .establishedCoverage(
            segments,
            committed.asScala.toSeq,
            IndexUtils.liveFragmentIds(dataset),
            plan.resolvedName)
          .size
      } finally {
        dataset.close()
      }

    Seq(new GenericInternalRow(Array[Any](
      fragmentsIndexed.toLong,
      segments.size.toLong,
      UTF8String.fromString(plan.resolvedName))))
  }

  private def noOpResult(resolvedName: String): InternalRow =
    new GenericInternalRow(Array[Any](0L, 0L, UTF8String.fromString(resolvedName)))

  private def validateArgs(): Option[Int] = {
    args.find(_.name == "train").foreach { _ =>
      throw new IllegalArgumentException(
        "train is not supported for REFRESH INDEX: refreshing an index exists to populate it. " +
          "Use CREATE INDEX WITH (train = false) to register an index without building it.")
    }
    Seq("build_mode", "rows_per_range").foreach { option =>
      args.find(_.name == option).foreach { _ =>
        throw new IllegalArgumentException(
          s"$option is not supported for REFRESH INDEX: range mode redistributes and sorts the " +
            "whole table, which an incremental refresh does not do. Use CREATE INDEX to rebuild " +
            "with build_mode = 'range'.")
      }
    }
    args.find(_.name == "num_segments").map(IndexUtils.parseNumSegments)
  }

  private def planRefresh(ds: Dataset, readOptions: LanceSparkReadOptions): RefreshPlan = {
    if (IndexUtils.isSystemIndex(indexName)) {
      throw new IllegalArgumentException(
        s"'$indexName' is a Lance-maintained system index and cannot be refreshed")
    }

    val matched = ds.getIndexes.asScala.toSeq
      .filter(idx => indexName.equalsIgnoreCase(idx.name()))
      .groupBy(_.name())
    if (matched.isEmpty) {
      throw new IllegalArgumentException(
        s"Index '$indexName' does not exist on table ${ident.toString}. " +
          "Create it with ALTER TABLE ... CREATE INDEX first.")
    }
    if (matched.size > 1) {
      throw new IllegalArgumentException(
        s"'$indexName' matches ${matched.size} indexes differing only in case " +
          s"(${matched.keys.toSeq.sorted.mkString(", ")}). Drop or rebuild them so one remains.")
    }
    val segments = matched.head._2

    val indexType = segments.head.indexType()
    val method = Option(indexType).flatMap(IndexUtils.methodForIndexType).getOrElse {
      val described = Option(indexType).map(_.name()).getOrElse("unknown")
      throw new UnsupportedOperationException(
        s"Spark SQL cannot build index type $described, so '$indexName' cannot be refreshed here. " +
          "Maintain it through the Lance SDK, which is also where it was created.")
    }

    val fieldIds = segments.head.fields()
    if (fieldIds == null || fieldIds.isEmpty) {
      throw new IllegalStateException(
        s"Index '$indexName' declares no indexed field; rebuild it with CREATE INDEX")
    }
    if (fieldIds.size() != 1) {
      throw new UnsupportedOperationException(
        s"REFRESH INDEX does not support index '$indexName': it declares ${fieldIds.size()} " +
          "fields, and a refreshed segment would only cover the column it is keyed on. Rebuild it " +
          "with ALTER TABLE ... CREATE INDEX instead.")
    }
    val fieldId = fieldIds.get(0)
    val column = Option(FieldPathUtils.pathByFieldId(ds.getLanceSchema, fieldId)).getOrElse {
      throw new IllegalStateException(
        s"Index '$indexName' is keyed on field id $fieldId, which is not in the table's current " +
          "schema. Drop the index with ALTER TABLE ... DROP INDEX.")
    }

    val covered = segments
      .flatMap(_.fragments().orElse(Collections.emptyList[Integer]()).asScala)
      .map(_.intValue)
      .toSet

    val unindexed = IndexUtils
      .fragmentWorkloads(ds)
      .filterNot(fragment => covered.contains(fragment.fragmentId.intValue))

    RefreshPlan(
      segments.head.name(),
      indexType,
      method,
      column,
      unindexed,
      IndexUtils.pinVersion(readOptions, ds))
  }
}

final private[v2] case class RefreshPlan(
    resolvedName: String,
    indexType: IndexType,
    method: String,
    column: String,
    unindexedFragments: List[FragmentWorkload],
    buildReadOptions: LanceSparkReadOptions)
