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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{AddIndexOutputType, LanceNamedArgument}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.LanceArrowUtils
import org.apache.spark.sql.util.LanceSerializeUtil.{decode, encode}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.{CommitBuilder, Dataset, Transaction}
import org.lance.index.{Index, IndexOptions, IndexParams, IndexType}
import org.lance.index.scalar.{BTreeIndexParams, ScalarIndexParams}
import org.lance.operation.{CreateIndex => AddIndexOperation}
import org.lance.schema.{LanceField, LanceSchema}
import org.lance.spark.{BaseLanceNamespaceSparkCatalog, LanceDataset, LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.arrow.LanceArrowWriter
import org.lance.spark.utils.{CloseableUtil, FieldPathUtils, Utils}
import org.lance.spark.write.SingleBatchArrowReader

import java.util.{Collections, Locale, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Physical execution of distributed CREATE INDEX (ALTER TABLE ... CREATE INDEX ...) for Lance datasets.
 *
 * <p>Index creation behaviour is controlled by the WITH clause options:
 * <ul>
 * <li><b>BTREE</b>: uses the logical segment commit path in {@code build_mode='fragment'}
 *   (default). In {@code build_mode='range'}, Spark redistributes and sorts data across
 *   partitions, creates indexes for each partition in parallel, and commits the resulting
 *   segment set.
 * <li><b>ZONEMAP / BITMAP / LABEL_LIST / NGRAM / BLOOMFILTER / RTREE / FTS / INVERTED</b>:
 *   build uncommitted index segments in parallel across fragment batches and commit the logical
 *   index on the driver.
 * </ul>
 *
 * <p><b>Deferred training ({@code WITH (train=false)})</b>: commits an empty index on the driver
 * with an empty fragment bitmap (all rows appear unindexed), skipping data processing. Supported
 * for all supported scalar index methods. Empty tables use the same path even when
 * {@code train=true}, since there are no fragments to train. Populate the index later by re-running
 * {@code CREATE INDEX} with the same name (a full distributed build that replaces the empty index)
 * or, for incremental coverage of appended fragments, by {@code Dataset.optimizeIndices} (the SQL
 * {@code OPTIMIZE} only compacts fragments). {@code num_segments} is rejected with
 * {@code train=false}, since no segmented build occurs.
 *
 * <p>The following options are consumed at the Spark execution layer and are never forwarded
 * to the Lance index backend: {@code train}, {@code build_mode}, {@code rows_per_range},
 * {@code num_segments}.
 */
case class AddIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    indexName: String,
    method: String,
    columns: Seq[String],
    args: Seq[LanceNamedArgument]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = AddIndexOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = LanceDataset.requireWritable(catalog.loadTable(ident), "AddIndex")

    val readOptions = lanceDataset.readOptions()
    val indexType = IndexUtils.buildIndexType(method)
    val btreeBuildMode = IndexUtils.btreeBuildMode(indexType, args)
    val scalarSegmentIndexType = IndexUtils.scalarSegmentIndexType(method)

    // Plan and build at one pinned version; the commit opens the live dataset.
    val (fragmentWorkloads, canonicalColumns, buildReadOptions) = {
      val ds = Utils.openDatasetBuilder(readOptions).build()
      try {
        val canonical = columns.map { column =>
          val field = IndexUtils.resolveIndexField(ds.getLanceSchema, indexType, column)
          FieldPathUtils.pathByFieldId(ds.getLanceSchema, field.getId)
        }
        (
          IndexUtils.fragmentWorkloads(ds),
          canonical,
          IndexUtils.pinVersion(readOptions, ds))
      } finally {
        ds.close()
      }
    }
    val fragmentIds = fragmentWorkloads.map(_.fragmentId)

    val train = IndexUtils.extractTrain(args)

    if (canonicalColumns.size != 1) {
      throw new UnsupportedOperationException(
        s"${indexType.name()} indexes currently support a single column only")
    }

    val numSegmentsOpt = args.find(_.name == "num_segments")
    if (numSegmentsOpt.isDefined && btreeBuildMode.contains("range")) {
      throw new IllegalArgumentException(
        "num_segments is only supported for BTREE indexes with build_mode='fragment'")
    }
    if (numSegmentsOpt.isDefined && scalarSegmentIndexType.isEmpty) {
      throw new IllegalArgumentException(
        s"num_segments is not supported for ${indexType.name()} indexes")
    }
    if (numSegmentsOpt.isDefined && !train) {
      throw new IllegalArgumentException(
        "num_segments is not supported with train=false: a deferred index performs no segmented build")
    }
    val validatedNumSegments: Option[Int] = numSegmentsOpt.map { arg =>
      arg.value match {
        case null =>
          throw new IllegalArgumentException(
            "num_segments must be a positive integer, got: null")
        case n: Number =>
          val asLong = n.longValue()
          if (asLong < 1L || asLong > Int.MaxValue)
            throw new IllegalArgumentException(
              s"num_segments must be a positive integer that fits in Int, got: $asLong")
          asLong.toInt
        case other =>
          throw new IllegalArgumentException(
            s"num_segments must be a positive integer, got: $other")
      }
    }

    // train=false, or an empty table: commit an empty index on the driver and skip data
    // processing. Index and option validation above still applies to empty tables.
    if (!train || fragmentIds.isEmpty) {
      val uuid = UUID.randomUUID()
      val dataset = Utils.openDatasetBuilder(readOptions).build()
      try {
        return commitEmptyIndex(
          dataset,
          indexName,
          indexType,
          canonicalColumns,
          uuid)
      } finally {
        dataset.close()
      }
    }

    val (nsImpl, nsProps, tableId, initialStorageOpts) =
      extractNamespaceInfo(lanceDataset, readOptions)

    if (btreeBuildMode.contains("range")) {
      val segments = new RangeBasedBTreeIndexJob(
        this.copy(columns = canonicalColumns),
        buildReadOptions,
        fragmentIds.size,
        nsImpl,
        nsProps,
        tableId,
        initialStorageOpts).run()
      val indexed = commitIndexSegments(readOptions, canonicalColumns.head, segments)
      return Seq(new GenericInternalRow(Array[Any](
        indexed.toLong,
        UTF8String.fromString(indexName))))
    }

    // Scalar segment indexes use the logical segment commit path.
    if (scalarSegmentIndexType.isDefined) {
      val segmentJob = new ScalarSegmentIndexJob(
        this.copy(columns = canonicalColumns),
        buildReadOptions,
        fragmentWorkloads,
        validatedNumSegments,
        nsImpl,
        nsProps,
        tableId,
        initialStorageOpts)
      val segments = segmentJob.run()
      // Atomic add+remove via Lance core; see commitIndexSegments
      val indexed = commitIndexSegments(readOptions, canonicalColumns.head, segments)
      return Seq(new GenericInternalRow(Array[Any](
        indexed.toLong,
        UTF8String.fromString(indexName))))
    }

    throw new UnsupportedOperationException(s"Unsupported index type: $indexType")
  }

  /** Commits an empty (untrained) index on the driver, with an empty fragment bitmap. */
  private def commitEmptyIndex(
      dataset: Dataset,
      indexName: String,
      indexType: IndexType,
      canonicalColumns: Seq[String],
      uuid: UUID): Seq[InternalRow] = {
    val argsJson = IndexUtils.toJson(args)
    val params = IndexParams.builder()
      .setScalarIndexParams(
        ScalarIndexParams.create(IndexUtils.buildScalarIndexParamType(method), argsJson))
      .build()
    val opts = IndexOptions
      .builder(canonicalColumns.asJava, indexType, params)
      .replace(true)
      .withIndexName(indexName)
      .withIndexUUID(uuid.toString)
      .train(false)
      .build()

    // Without fragmentIds, Lance commits createIndex atomically. Do not manually commit the
    // returned metadata a second time.
    dataset.createIndex(opts)

    Seq(new GenericInternalRow(Array[Any](0L, UTF8String.fromString(indexName))))
  }

  // Lance core's commitExistingIndexSegments handles atomic replacement:
  // it finds existing segments whose fragments overlap with incoming ones
  // and removes them in the same CreateIndex transaction.
  //
  // Returns the number of fragments the commit actually covers, which is what the command reports.
  private def commitIndexSegments(
      readOptions: LanceSparkReadOptions,
      column: String,
      segments: Seq[Index]): Int = {
    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      IndexUtils.requireCommittableCoverage(
        IndexUtils.liveFragmentIds(dataset),
        segments,
        indexName)
      val committed = dataset.commitExistingIndexSegments(
        indexName,
        column,
        segments.toList.asJava)
      // The commit advances this handle to the manifest it wrote, so both the returned metadata and
      // the fragment list below describe the committed state rather than the one validated above.
      IndexUtils
        .establishedCoverage(
          segments,
          committed.asScala.toSeq,
          IndexUtils.liveFragmentIds(dataset),
          indexName)
        .size
    } finally {
      dataset.close()
    }
  }

  private def extractNamespaceInfo(
      lanceDataset: LanceDataset,
      readOptions: LanceSparkReadOptions): (
      Option[String],
      Option[Map[String, String]],
      Option[List[String]],
      Option[Map[String, String]]) = {
    catalog match {
      case nsCatalog: BaseLanceNamespaceSparkCatalog =>
        (
          Option(nsCatalog.getNamespaceImpl),
          Option(nsCatalog.getNamespaceProperties).map(_.asScala.toMap),
          Option(readOptions.getTableId).map(_.asScala.toList),
          Option(lanceDataset.getInitialStorageOptions).map(_.asScala.toMap))
      case _ => (None, None, None, None)
    }
  }

}

/**
 * A job implementation for creating range-based BTree indexes using preprocessed, globally sorted data.
 * This approach distributes data across multiple partitions based on ranges of values and creates
 * indexes on each range in parallel.
 *
 * The data is partitioned by fragment id so each Spark partition holds the rows of a disjoint
 * set of fragments, sorted by the indexed value. Each partition becomes one uncommitted segment
 * covering exactly those fragments, so the resulting segments have disjoint fragment coverage and
 * can be committed directly as a single logical index.
 *
 * @param addIndexExec       The AddIndexExec instance that initiated this job
 * @param readOptions        Configuration options for reading the Lance dataset
 * @param numFragments       Number of fragments in the dataset, used to bound shuffle partitions
 * @param nsImpl             Optional namespace implementation class for credential vending
 * @param nsProps            Optional namespace properties for credential vending
 * @param tableId            Optional table identifier for credential vending
 * @param initialStorageOpts Optional initial storage options for the dataset
 */
class RangeBasedBTreeIndexJob(
    addIndexExec: AddIndexExec,
    readOptions: LanceSparkReadOptions,
    numFragments: Int,
    nsImpl: Option[String],
    nsProps: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOpts: Option[Map[String, String]]) extends Serializable {

  private val VALUE_COLUMN_NAME = "value"

  /** Version `readOptions` is pinned to. Throws if the ref is not a version on main. */
  private def pinnedVersion: Long = {
    val ref = readOptions.getRef
    if (ref == null || !ref.isMain || !ref.getVersionNumber.isPresent) {
      throw new IllegalStateException(
        "Range-mode BTree builds need read options pinned to a version on main so the scan and the " +
          "segment stamp describe one snapshot; got a ref that names none")
    }
    ref.getVersionNumber.get.longValue()
  }

  def run(): Seq[Index] = {
    if (addIndexExec.columns.size != 1) {
      throw new UnsupportedOperationException(
        "Range-based BTree index currently supports a single column only")
    }

    val session = addIndexExec.session
    val catalog = addIndexExec.catalog
    val ident = addIndexExec.ident
    val columns = addIndexExec.columns.toList
    val zoneSize = addIndexExec.args.find(_.name == "zone_size").map(_.value.asInstanceOf[Long])

    // Build a fully qualified table name to read data back through Spark.
    val namespace = Option(ident.namespace()).map(_.toSeq).getOrElse(Seq.empty)
    val parts = if (namespace.isEmpty) {
      Seq(catalog.name(), ident.name())
    } else {
      catalog.name() +: namespace :+ ident.name()
    }
    val fullTableName = parts.mkString(".")

    val fragmentColumn = LanceDataset.FRAGMENT_ID_COLUMN.name
    val df = session.read
      .option(LanceSparkReadOptions.CONFIG_VERSION, pinnedVersion)
      .table(fullTableName)
    val selectDf = df.select(
      df.col(columns.head).as(VALUE_COLUMN_NAME),
      df.col(LanceDataset.ROW_ID_COLUMN.name),
      df.col(fragmentColumn))

    // Partition by fragment id so each partition holds whole fragments (disjoint coverage),
    // then sort within each partition by the indexed value to feed Lance pre-sorted data.
    // repartitionByRange keeps equal fragment ids together and is the shuffle the connector
    // already relies on elsewhere.
    val numPartitions = Math.max(1, numFragments)
    val rangeDf = selectDf
      .repartitionByRange(numPartitions, selectDf.col(fragmentColumn))
      .sortWithinPartitions(selectDf.col(VALUE_COLUMN_NAME).asc)

    val indexBuilder = RangeBTreeIndexBuilder(
      encode(readOptions),
      columns,
      zoneSize,
      nsImpl,
      nsProps,
      tableId,
      initialStorageOpts,
      rangeDf.schema)

    val results = rangeDf.queryExecution.toRdd.mapPartitionsWithIndex { case (_, rowsIter) =>
      indexBuilder.buildForFragmentGroup(rowsIter)
    }.collect()

    val segments = results.iterator
      .map(decode[Option[Index]])
      .collect { case Some(segment) => segment }
      .toSeq
    if (segments.isEmpty) {
      throw new IllegalStateException("Range-based BTree build produced no index segments")
    }
    segments
  }

}

/**
 * A helper class for building a range-based B-tree index.
 * This class is serialized and sent to executors to build the index for a specific range of data.
 *
 * @param encodedReadOptions      Serialized configuration for Lance dataset access.
 * @param columns                 The names of the columns to be indexed.
 * @param zoneSize                Optional size of zones within the B-tree index.
 * @param namespaceImpl           Optional implementation class for namespace operations, used for credential vending.
 * @param namespaceProperties     Optional properties of the namespace, used for credential vending.
 * @param tableId                 Optional identifier for the table within the namespace, used for credential vending.
 * @param initialStorageOptions   Optional initial storage configuration options for the dataset.
 * @param schema                  The schema of the input data rows.
 */
case class RangeBTreeIndexBuilder(
    encodedReadOptions: String,
    columns: List[String],
    zoneSize: Option[Long],
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]],
    schema: StructType) extends Serializable {

  def buildForFragmentGroup(rowsIter: Iterator[InternalRow]): Iterator[String] = {
    // The input rows carry (value, _rowid, _fragment_id). Only the first two
    // columns are written to the pre-sorted Arrow stream that Lance ingests; the
    // fragment ids are collected separately to declare the segment's coverage.
    val streamSchema = StructType(schema.fields.take(2))
    val fragmentIdOrdinal = 2

    val allocator = LanceRuntime.allocator()
    val data =
      VectorSchemaRoot.create(LanceArrowUtils.toArrowSchema(streamSchema, "UTC", false), allocator)
    val writer = LanceArrowWriter.create(data, streamSchema)

    val fragmentIds = scala.collection.mutable.LinkedHashSet[java.lang.Integer]()

    // Write the indexed value and row id of each row to the Arrow stream.
    try {
      while (rowsIter.hasNext) {
        val row = rowsIter.next()
        writer.field(0).write(row, 0)
        writer.field(1).write(row, 1)
        fragmentIds += java.lang.Integer.valueOf(row.getInt(fragmentIdOrdinal))
      }

      writer.finish()
    } catch {
      case e: Throwable =>
        CloseableUtil.closeQuietly(data)
        throw e
    }

    // No rows are written
    if (data.getRowCount == 0) {
      data.close()
      return Iterator(encode(None: Option[Index]))
    }

    var stream: ArrowArrayStream = null
    var reader: ArrowReader = null
    var dataset: Dataset = null

    try {
      stream = ArrowArrayStream.allocateNew(allocator)
      reader = new SingleBatchArrowReader(allocator, data)

      dataset = Utils.openDatasetBuilder(
        decode[LanceSparkReadOptions](encodedReadOptions))
        .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
        .runtimeNamespace(
          namespaceImpl.orNull,
          namespaceProperties.map(_.asJava).orNull,
          tableId.map(_.asJava).orNull)
        .build()

      Data.exportArrayStream(allocator, reader, stream)

      // Build an uncommitted BTree segment for this fragment group from the
      // pre-sorted data. No index name or UUID is set: Lance generates the
      // segment UUID, and the fragment ids declare the segment's coverage so
      // the per-partition segments stay disjoint.
      val btreeParamsBuilder = BTreeIndexParams.builder()
      if (zoneSize.isDefined) {
        btreeParamsBuilder.zoneSize(zoneSize.get)
      }

      val scalarParams = btreeParamsBuilder.build()
      val indexParams = IndexParams.builder().setScalarIndexParams(scalarParams).build()

      val indexOptions = IndexOptions
        .builder(columns.asJava, IndexType.BTREE, indexParams)
        .replace(false)
        .withFragmentIds(fragmentIds.toList.asJava)
        .withPreprocessedData(stream)
        .build()

      val createdIndex = dataset.createIndex(indexOptions)
      Iterator(encode(Some(createdIndex): Option[Index]))
    } finally {
      CloseableUtil.closeQuietly(stream)
      CloseableUtil.closeQuietly(reader)
      CloseableUtil.closeQuietly(data)
      CloseableUtil.closeQuietly(dataset)
    }
  }
}

/**
 * A job implementation for creating scalar segment indexes using logical segment commit.
 * Fragments are batched into segments, each built in parallel, and committed
 * as a logical index on the driver.
 */
class ScalarSegmentIndexJob(
    addIndexExec: AddIndexExec,
    readOptions: LanceSparkReadOptions,
    fragmentWorkloads: List[FragmentWorkload],
    numSegments: Option[Int],
    nsImpl: Option[String],
    nsProps: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOpts: Option[Map[String, String]]) {

  def run(): Seq[Index] = {
    val indexType = IndexUtils.scalarSegmentIndexType(addIndexExec.method).getOrElse {
      throw new UnsupportedOperationException(
        s"Unsupported Lance index method: ${addIndexExec.method}")
    }
    val encodedReadOptions = encode(readOptions)
    val columns = addIndexExec.columns.toList
    val argsJson = IndexUtils.toJson(addIndexExec.args)
    val fragmentBatches = IndexUtils.batchFragments(
      fragmentWorkloads,
      numSegments,
      addIndexExec.session.sparkContext.defaultParallelism)

    val tasks = fragmentBatches.map { batch =>
      ScalarSegmentIndexTask(
        encodedReadOptions,
        columns,
        addIndexExec.method,
        argsJson,
        batch,
        nsImpl,
        nsProps,
        tableId,
        initialStorageOpts)
    }.toSeq

    IndexUtils.runSegmentTasks(
      addIndexExec.session.sparkContext,
      tasks,
      s"${indexType.name()} index build failed. Uncommitted segments are not " +
        "visible to readers and will not affect query correctness.")(_.execute())
  }
}

final private[v2] case class FragmentWorkload(fragmentId: Integer, numRows: Long)

/**
 * A task to create a scalar index segment on a batch of fragments.
 */
case class ScalarSegmentIndexTask(
    encodedReadOptions: String,
    columns: List[String],
    method: String,
    argsJson: String,
    fragmentIds: List[Integer],
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]]) extends Serializable {

  def execute(): String = {
    val readOptions = decode[LanceSparkReadOptions](encodedReadOptions)
    val indexType = IndexUtils.scalarSegmentIndexType(method).getOrElse {
      throw new UnsupportedOperationException(
        s"Unsupported Lance index method: $method")
    }
    val params = IndexParams.builder()
      .setScalarIndexParams(
        ScalarIndexParams.create(IndexUtils.buildScalarIndexParamType(method), argsJson))
      .build()

    val indexOptions = IndexOptions
      .builder(java.util.Arrays.asList(columns: _*), indexType, params)
      .withFragmentIds(fragmentIds.asJava)
      .replace(false)
      .build()

    val dataset = Utils.openDatasetBuilder(readOptions)
      .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
      .runtimeNamespace(
        namespaceImpl.orNull,
        namespaceProperties.map(_.asJava).orNull,
        tableId.map(_.asJava).orNull)
      .build()

    try {
      encode(dataset.createIndex(indexOptions))
    } finally {
      dataset.close()
    }
  }
}

/**
 * Utility methods for working with index types.
 */
object IndexUtils extends Logging {

  private val jsonMapper = new ObjectMapper()

  private val methodToIndexTypes: Map[String, IndexType] = Map(
    "btree" -> IndexType.BTREE,
    "zonemap" -> IndexType.ZONEMAP,
    "bitmap" -> IndexType.BITMAP,
    "label_list" -> IndexType.LABEL_LIST,
    "ngram" -> IndexType.NGRAM,
    "bloomfilter" -> IndexType.BLOOM_FILTER,
    "rtree" -> IndexType.RTREE,
    "fts" -> IndexType.INVERTED,
    "inverted" -> IndexType.INVERTED)

  private val scalarSegmentIndexTypes: Set[IndexType] = Set(
    IndexType.BTREE,
    IndexType.ZONEMAP,
    IndexType.BITMAP,
    IndexType.LABEL_LIST,
    IndexType.NGRAM,
    IndexType.BLOOM_FILTER,
    IndexType.RTREE,
    IndexType.INVERTED)

  // Field-shape requirements are independent of the distributed execution path. These index
  // types reject container fields in Lance Core, so validate them before launching Spark tasks.
  private val leafFieldIndexTypes: Set[IndexType] = Set(
    IndexType.BTREE,
    IndexType.BITMAP,
    IndexType.NGRAM,
    IndexType.BLOOM_FILTER)

  // ScalarIndexParams uses Lance Core's scalar plugin names, which are a separate contract from
  // both Spark SQL method names and the Java IndexType enum names. Keep the mapping explicit instead
  // of deriving it from either naming convention.
  private val scalarParamTypesByIndexType: Map[IndexType, String] = Map(
    IndexType.BTREE -> "btree",
    IndexType.ZONEMAP -> "zonemap",
    IndexType.BITMAP -> "bitmap",
    IndexType.LABEL_LIST -> "labellist",
    IndexType.NGRAM -> "ngram",
    IndexType.BLOOM_FILTER -> "bloomfilter",
    IndexType.RTREE -> "rtree",
    IndexType.INVERTED -> "inverted")

  def scalarSegmentIndexType(method: String): Option[IndexType] =
    methodToIndexTypes
      .get(method.toLowerCase(Locale.ROOT))
      .filter(scalarSegmentIndexTypes.contains)

  /** Pins `readOptions` to the version `dataset` is open at. */
  def pinVersion(
      readOptions: LanceSparkReadOptions,
      dataset: Dataset): LanceSparkReadOptions =
    readOptions.withRef(Utils.pinOpenedRef(dataset, readOptions.getRef))

  /**
   * Fragment ids and live row counts of `dataset`, in manifest order.
   *
   * Reads the primitive fragment-statistics view rather than [[Dataset#getFragments]], which
   * materializes a Java object per fragment and per data file. Both commands enumerate fragments on
   * the driver, once to plan and once to check the commit, so on a large table that difference is
   * the bulk of planning cost.
   */
  def fragmentWorkloads(dataset: Dataset): List[FragmentWorkload] = {
    val stats = dataset.getFragmentStatistics
    val ids = stats.getIds
    val rowCounts = stats.getRowCounts
    List.tabulate(ids.length)(index =>
      FragmentWorkload(Integer.valueOf(ids(index)), rowCounts(index)))
  }

  /** Fragment ids live in `dataset`. See [[fragmentWorkloads]] for why this avoids getFragments. */
  def liveFragmentIds(dataset: Dataset): Set[Int] =
    dataset.getFragmentStatistics.getIds.toSet

  /** Fragment ids the given segments declare coverage of. */
  def declaredCoverage(segments: Seq[Index]): Set[Int] =
    segments.iterator
      .flatMap(_.fragments().orElse(Collections.emptyList[Integer]()).asScala)
      .map(_.intValue)
      .toSet

  /**
   * Refuses a segment set that declares coverage but intersects no live fragment. Partial loss is
   * accepted: fragments can be retired by compaction or deletion during the build, and the segments
   * covering what remains are still correct. [[establishedCoverage]] reports what was actually
   * committed.
   */
  def requireCommittableCoverage(
      liveFragmentIds: Set[Int],
      segments: Seq[Index],
      indexName: String): Unit = {
    val declared = declaredCoverage(segments)
    if (declared.nonEmpty && declared.intersect(liveFragmentIds).isEmpty) {
      throw new IllegalStateException(
        s"Index '$indexName' build raced a concurrent operation: every fragment it covers " +
          s"(${describeFragmentIds(declared)}) was retired while the build ran, so the segments " +
          "would cover nothing. No index change was committed; re-run the command.")
    }
  }

  /**
   * Committed fragment ids from the segments this build produced, intersected with what is still
   * live. Only segments whose UUID matches `builtSegments` are counted; pre-existing survivors are
   * not this command's coverage.
   */
  def establishedCoverage(
      builtSegments: Seq[Index],
      committedSegments: Seq[Index],
      liveFragmentIds: Set[Int],
      indexName: String): Set[Int] = {
    val builtUuids = builtSegments.map(_.uuid).toSet
    val established =
      declaredCoverage(committedSegments.filter(segment => builtUuids.contains(segment.uuid)))
        .intersect(liveFragmentIds)
    val uncovered = declaredCoverage(builtSegments).diff(established)
    if (uncovered.nonEmpty) {
      logWarning(
        s"Index '$indexName' build raced a concurrent operation: fragments " +
          s"${describeFragmentIds(uncovered)} are not covered by this commit, because they were " +
          "retired or because their indexed field was rewritten while the build ran. The segments " +
          "for the remaining fragments are committed; re-run the command to cover them.")
    }
    established
  }

  private def describeFragmentIds(ids: Set[Int]): String = {
    val ordered = ids.toSeq.sorted
    val shown = ordered.take(10).mkString(", ")
    if (ordered.size > 10) s"$shown, ... (${ordered.size} total)" else shown
  }

  def resolveIndexField(
      schema: LanceSchema,
      indexType: IndexType,
      column: String): LanceField = {
    if (leafFieldIndexTypes.contains(indexType)) {
      FieldPathUtils.resolveLeafField(schema, column)
    } else {
      FieldPathUtils.resolveField(schema, column)
    }
  }

  /**
   * Extracts the `train` option from named arguments, defaulting to `true`.
   *
   * When `train=false`, index creation registers an empty index without processing any data.
   * All existing rows will be unindexed and covered by a subsequent OPTIMIZE INDEX call.
   */
  def extractTrain(args: Seq[LanceNamedArgument]): Boolean =
    args.find(_.name == "train") match {
      case Some(LanceNamedArgument(_, b: java.lang.Boolean)) => b.booleanValue()
      case Some(LanceNamedArgument(_, other)) =>
        throw new IllegalArgumentException(
          s"'train' option must be a boolean literal (true/false), got: $other")
      case None => true
    }

  /**
   * Build an [[IndexType]] from the given index method string.
   *
   * @param method the index method name
   * @return the corresponding [[IndexType]]
   * @throws UnsupportedOperationException if the method is not supported
   */
  def buildIndexType(method: String): IndexType = {
    val normalized = method.toLowerCase(Locale.ROOT)
    methodToIndexTypes.getOrElse(
      normalized,
      throw new UnsupportedOperationException(s"Unsupported index method: $normalized"))
  }

  def buildScalarIndexParamType(method: String): String = {
    val normalized = method.toLowerCase(Locale.ROOT)
    val indexType = buildIndexType(normalized)
    scalarParamTypesByIndexType.getOrElse(
      indexType,
      throw new UnsupportedOperationException(s"Unsupported scalar index method: $normalized"))
  }

  def btreeBuildMode(indexType: IndexType, args: Seq[LanceNamedArgument]): Option[String] = {
    if (indexType != IndexType.BTREE) {
      None
    } else {
      val buildMode = args.find(_.name == "build_mode").map(_.value.asInstanceOf[String])
      buildMode match {
        case Some("fragment") | Some("range") | None =>
          buildMode
        case Some(unknown) =>
          throw new IllegalArgumentException(
            s"Unrecognized build_mode: '$unknown'. Supported values are 'fragment' and 'range'.")
      }
    }
  }

  // Options consumed at the Spark execution layer that must not be forwarded to the Lance
  // index backend as index parameters.
  private val SparkOnlyOptions: Set[String] =
    Set("train", "build_mode", "rows_per_range", "num_segments")

  def toJson(args: Seq[LanceNamedArgument]): String = {
    val indexArgs = args.filterNot(a => SparkOnlyOptions.contains(a.name))
    if (indexArgs.isEmpty) {
      "{}"
    } else {
      val node: ObjectNode = jsonMapper.createObjectNode()
      indexArgs.foreach { a =>
        a.value match {
          case null => node.putNull(a.name)
          case s: java.lang.String =>
            val trimmed = s.stripPrefix("\"").stripSuffix("\"").stripPrefix("'").stripSuffix("'")
            node.put(a.name, trimmed)
          case b: java.lang.Boolean => node.put(a.name, b.booleanValue())
          case c: java.lang.Character => node.put(a.name, String.valueOf(c))
          case by: java.lang.Byte => node.put(a.name, by.intValue())
          case sh: java.lang.Short => node.put(a.name, sh.intValue())
          case i: java.lang.Integer => node.put(a.name, i.intValue())
          case l: java.lang.Long => node.put(a.name, l.longValue())
          case f: java.lang.Float => node.put(a.name, f.doubleValue())
          case d: java.lang.Double => node.put(a.name, d.doubleValue())
          case other => node.put(a.name, String.valueOf(other))
        }
      }
      jsonMapper.writeValueAsString(node)
    }
  }

  def runSegmentTasks[T <: Serializable: ClassTag](
      sc: org.apache.spark.SparkContext,
      tasks: Seq[T],
      failureMessage: String)(execute: T => String): Seq[Index] = {
    if (tasks.isEmpty) {
      Seq.empty
    } else {
      try {
        sc.parallelize(tasks, tasks.size)
          .map(execute)
          .collect()
          .map(encoded => decode[Index](encoded))
          .toSeq
      } catch {
        case e: Exception => throw new RuntimeException(failureMessage, e)
      }
    }
  }

  /**
   * Splits `fragments` into `numSegments` batches, each a contiguous run of fragment ids, chosen so
   * that the heaviest batch is as light as any contiguous split allows.
   *
   * Contiguity is not cosmetic. Lance's compaction planner only groups fragments that are covered by
   * the identical set of index segments, so batches whose fragment ids interleave leave every
   * adjacent pair of fragments in a different group and make OPTIMIZE a no-op for the whole table.
   * It does cost some balance. `[10, 9, 8, 7]` into two batches is 19/15 here, where the previous
   * least-loaded-first assignment reached 17/17 by interleaving. Optimal among contiguous splits is
   * the guarantee, not optimal overall.
   *
   * Assignment is deterministic: the same fragments and segment count always produce the same
   * batches, whatever order `fragments` arrives in. Every batch holds at least one fragment, so the
   * result always has exactly `segmentCount` entries.
   */
  def batchFragments(
      fragments: List[FragmentWorkload],
      numSegments: Option[Int],
      defaultParallelism: Int): Seq[List[Integer]] = {
    val fragmentCount = fragments.size
    if (fragmentCount == 0) {
      return Seq.empty
    }

    val segmentCount = numSegments match {
      case Some(requested) =>
        val clamped = math.max(1, math.min(fragmentCount, requested))
        if (clamped != requested) {
          logWarning(
            s"num_segments=$requested clamped to $clamped " +
              s"(fragment count=$fragmentCount)")
        }
        clamped
      case None => math.max(1, math.min(fragmentCount, defaultParallelism))
    }

    val ordered = fragments.sortBy(_.fragmentId.intValue)
    // Prefix sums drive the search. addExact rejects a workload that cannot be summed rather than
    // balancing against a wrapped total.
    val rowsUpTo = new Array[Long](fragmentCount + 1)
    ordered.iterator.zipWithIndex.foreach { case (fragment, index) =>
      rowsUpTo(index + 1) = Math.addExact(rowsUpTo(index), fragment.numRows)
    }

    var offset = 0
    balancedRunLengths(rowsUpTo, segmentCount).map { length =>
      val batch = ordered.slice(offset, offset + length).map(_.fragmentId)
      offset += length
      batch
    }
  }

  /**
   * Lengths of exactly `segmentCount` contiguous runs over `rowsUpTo`, minimising the heaviest run.
   *
   * The smallest row budget a contiguous packing can respect is found by binary search, which is
   * exact rather than approximate: for a fixed budget, extending each run as far as it will go uses
   * the fewest runs, so the smallest feasible budget is the optimal maximum. The floor of the search
   * is the widest single fragment, below which no packing exists.
   *
   * Packing at that budget can use fewer runs than were asked for, which would cost parallelism, so
   * runs are divided until the count is reached. Which run gets divided does not matter: every run
   * already fits the budget, so both halves of any split fit it too, and the budget is minimal, so
   * the heaviest run cannot fall below it either.
   */
  private def balancedRunLengths(rowsUpTo: Array[Long], segmentCount: Int): Seq[Int] = {
    val fragmentCount = rowsUpTo.length - 1
    def rowsOf(start: Int, length: Int): Long = rowsUpTo(start + length) - rowsUpTo(start)

    var widestFragment = 0L
    var index = 0
    while (index < fragmentCount) {
      widestFragment = math.max(widestFragment, rowsOf(index, 1))
      index += 1
    }

    var low = widestFragment
    var high = rowsUpTo(fragmentCount)
    while (low < high) {
      val budget = low + (high - low) / 2
      if (runsWithin(rowsUpTo, budget) <= segmentCount) high = budget else low = budget + 1
    }

    // runLengthAt(start) is the length of the run beginning at `start`, and 0 elsewhere.
    val runLengthAt = new Array[Int](fragmentCount)
    var runCount = 0
    var start = 0
    while (start < fragmentCount) {
      var length = 1
      while (start + length < fragmentCount && rowsOf(start, length + 1) <= low) {
        length += 1
      }
      runLengthAt(start) = length
      runCount += 1
      start += length
    }

    // Divide from the left until the count is reached. Splitting the heaviest run first would be no
    // better, since the budget already bounds every run.
    var splitAt = 0
    while (runCount < segmentCount && splitAt < fragmentCount) {
      val length = runLengthAt(splitAt)
      if (length > 1) {
        val cut = balancePoint(rowsUpTo, splitAt, length)
        runLengthAt(splitAt) = cut
        runLengthAt(splitAt + cut) = length - cut
        runCount += 1
      } else {
        splitAt += length
      }
    }

    val runs = ArrayBuffer.empty[Int]
    var cursor = 0
    while (cursor < fragmentCount) {
      val length = runLengthAt(cursor)
      runs += length
      cursor += length
    }
    runs.toList
  }

  /**
   * Fewest contiguous runs that keep every run's row count within `budget`.
   *
   * A fragment wider than `budget` still forms a run of its own, so callers must not search below
   * the widest fragment or the count would understate what the budget can actually hold.
   */
  private def runsWithin(rowsUpTo: Array[Long], budget: Long): Int = {
    val fragmentCount = rowsUpTo.length - 1
    var runs = 0
    var start = 0
    while (start < fragmentCount) {
      var length = 1
      while (start + length < fragmentCount &&
        rowsUpTo(start + length + 1) - rowsUpTo(start) <= budget) {
        length += 1
      }
      runs += 1
      start += length
    }
    runs
  }

  /** Where to cut a run of `length` fragments so the two halves are as even as possible. */
  private def balancePoint(rowsUpTo: Array[Long], start: Int, length: Int): Int = {
    val total = rowsUpTo(start + length) - rowsUpTo(start)
    def leftOf(at: Int): Long = rowsUpTo(start + at) - rowsUpTo(start)
    def heavierHalf(at: Int): Long = math.max(leftOf(at), total - leftOf(at))

    var cut = 1
    while (cut < length - 1 && leftOf(cut) < total - leftOf(cut)) {
      cut += 1
    }
    if (cut > 1 && heavierHalf(cut - 1) < heavierHalf(cut)) cut - 1 else cut
  }

}
