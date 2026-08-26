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
 * <p>Where {@code CREATE INDEX} rebuilds every fragment, this command indexes only the fragments
 * the named index does not already cover, then adds the resulting segments to it. Cost is
 * proportional to the new data rather than to the table, which makes it the incremental
 * counterpart to a full rebuild for tables that are appended to continuously.
 *
 * <p>Execution mirrors {@code CREATE INDEX}: the driver resolves the index, diffs its fragment
 * coverage against the dataset, and balances the remainder into batches; executors build one
 * uncommitted segment per batch; the driver commits them as one logical index. Lance core keeps
 * existing segments whose fragments are disjoint from the incoming ones, so prior coverage
 * survives the commit.
 *
 * <p>Because a deferred index ({@code WITH (train=false)}) covers no fragments, refreshing one
 * indexes the whole table through the same distributed path.
 *
 * <p>Index build parameters are taken from the {@code WITH} clause, falling back to the index type's
 * defaults rather than to the existing index's configuration: Lance records a built index's
 * parameters inside the index rather than as table metadata a command can read back. For index types
 * whose segments are queried independently that only changes performance, so the caller is asked to
 * pass the original options. For a type whose segments must agree — see
 * {@code IndexUtils.requiresUniformSegmentDetails} — the mismatch is detected against the built
 * segments and rejected before anything is committed.
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

    // Lance core's commitExistingIndexSegments keeps existing segments that are disjoint from the
    // incoming fragments and removes the ones they supersede, all in one CreateIndex transaction.
    //
    // The index is re-resolved here rather than carried over from planning: the whole distributed
    // build sits between the two, so the state the commit has to fit is the state now, not the state
    // the plan saw.
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
        val covered = IndexUtils.committedCoverage(liveFragmentIds, segments, plan.resolvedName)
        dataset.commitExistingIndexSegments(plan.resolvedName, plan.column, segments.toList.asJava)
        covered.size
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

  /**
   * Validates the WITH clause and returns the requested segment count.
   *
   * Options that only make sense for a full build are rejected rather than ignored, so a
   * misapplied option fails instead of silently changing nothing.
   */
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

    // The parser lowercases the requested name, so matching has to ignore case to reach an index
    // created elsewhere with mixed case. Lance keys indexes by exact name, so that match can span
    // two distinct indexes; unioning their coverage would compute the wrong unindexed set and
    // commit it under one of the two names. Refuse instead of guessing.
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
    // An index type this command cannot rebuild is also one CREATE INDEX cannot build, since both go
    // through the same method mapping: a vector index, for instance, only exists because something
    // outside Spark SQL created it. Pointing such a user at CREATE INDEX would be a dead end.
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
    // Every method this command supports is keyed on exactly one column, so more fields than that
    // means the index carries extra columns or spans several. A rebuilt segment would declare only
    // the keyed column, and one logical index needs one field declaration across its segments, so
    // committing that mix would produce metadata `describe_indices` rejects. Lance core refuses to
    // optimize a covering index for the same reason.
    if (fieldIds.size() != 1) {
      throw new UnsupportedOperationException(
        s"REFRESH INDEX does not support index '$indexName': it declares ${fieldIds.size()} " +
          "fields, and a refreshed segment would only cover the column it is keyed on. Rebuild it " +
          "with ALTER TABLE ... CREATE INDEX instead.")
    }
    // Guaranteed by the check above to be the single field the index is keyed on. The path is
    // resolved from the field id rather than remembered, so a renamed column still resolves; a
    // field id absent from the current schema resolves to nothing and is reported as such.
    val fieldId = fieldIds.get(0)
    val column = Option(FieldPathUtils.pathByFieldId(ds.getLanceSchema, fieldId)).getOrElse {
      throw new IllegalStateException(
        s"Index '$indexName' is keyed on field id $fieldId, which is not in the table's current " +
          "schema. Drop the index with ALTER TABLE ... DROP INDEX.")
    }

    // A segment with no fragment bitmap predates coverage tracking, and one with an empty bitmap
    // is a deferred index. Both read as covering nothing here, which plans a full build. Lance
    // core rejects a partial-coverage commit against a segment it cannot place, so it stays the
    // authority on whether that build is a legal replacement.
    val covered = segments
      .flatMap(_.fragments().orElse(Collections.emptyList[Integer]()).asScala)
      .map(_.intValue)
      .toSet

    val unindexed = IndexUtils
      .fragmentWorkloads(ds)
      .filterNot(fragment => covered.contains(fragment.fragmentId.intValue))

    // Commit under the name the manifest actually stores. The parser lowercases the requested name,
    // so an index created elsewhere with mixed case is matched case-insensitively above; committing
    // under the lowercased spelling would fork a second logical index with overlapping coverage.
    RefreshPlan(
      segments.head.name(),
      indexType,
      method,
      column,
      unindexed,
      IndexUtils.pinVersion(readOptions, ds))
  }
}

/**
 * The driver-side plan for one refresh.
 *
 * @param resolvedName       index name as stored in the manifest, which the commit must reuse
 * @param indexType          type of the existing index, which the rebuilt segments must match
 * @param method             SQL index method resolved from the existing index's type
 * @param column             canonical path of the column the index is keyed on
 * @param unindexedFragments fragments the index does not cover, with their row counts
 * @param buildReadOptions   read options pinned to the version the plan was computed against
 */
final private[v2] case class RefreshPlan(
    resolvedName: String,
    indexType: IndexType,
    method: String,
    column: String,
    unindexedFragments: List[FragmentWorkload],
    buildReadOptions: LanceSparkReadOptions)
