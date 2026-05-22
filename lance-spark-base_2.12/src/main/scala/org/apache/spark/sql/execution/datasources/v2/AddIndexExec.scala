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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{AddIndexOutputType, LanceNamedArgument}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.LanceArrowUtils
import org.apache.spark.sql.util.LanceSerializeUtil.{decode, encode}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.{CommitBuilder, Dataset, Transaction}
import org.lance.index.{DistanceType, Index, IndexOptions, IndexParams, IndexType}
import org.lance.index.scalar.{BTreeIndexParams, ScalarIndexParams}
import org.lance.index.vector.{IvfBuildParams, PQBuildParams, SQBuildParams, VectorIndexParams, VectorTrainer}
import org.lance.operation.{CreateIndex => AddIndexOperation}
import org.lance.spark.{BaseLanceNamespaceSparkCatalog, LanceDataset, LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.arrow.LanceArrowWriter
import org.lance.spark.utils.{CloseableUtil, Utils}
import org.lance.spark.write.SingleBatchArrowReader

import java.time.Instant
import java.util.{Collections, Optional, UUID}

import scala.collection.JavaConverters._

/**
 * Physical execution of distributed CREATE INDEX (ALTER TABLE ... CREATE INDEX ...) for Lance datasets.
 *
 * <ul>
 * <li>For BTREE index, it uses a range-based approach that redistributes and sorts data across partitions, creates indexes for each range in parallel, and finally merges them into a global index structure.
 * <li>For other index types, it processes each fragment independently in parallel, merges index metadata
 * and commits an index-creation transaction.
 * </ul>
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
    val lanceDataset = catalog.loadTable(ident) match {
      case d: LanceDataset => d
      case _ => throw new UnsupportedOperationException("AddIndex only supports LanceDataset")
    }

    val readOptions = lanceDataset.readOptions()

    // Get all fragment id list from dataset
    val fragmentIds = {
      val ds = Utils.openDatasetBuilder(readOptions).build()
      try {
        ds.getFragments.asScala.map(_.getId).map(Integer.valueOf).toList
      } finally {
        ds.close()
      }
    }

    if (fragmentIds.isEmpty) {
      // No fragments to index
      return Seq(new GenericInternalRow(Array[Any](0L, UTF8String.fromString(indexName))))
    }

    val indexType = IndexUtils.buildIndexType(method)

    // Vector indexes (IVF_*) use the multi-segment commit API: each fragment produces a
    // self-contained uncommitted segment, and the driver publishes them atomically under
    // a single logical index name. This path is separate from the scalar (BTREE / INVERTED)
    // distributed build because scalar per-fragment createIndex still produces partial files
    // that require mergeIndexMetadata to finalize.
    if (IndexUtils.isVectorIndex(indexType)) {
      val (nsImpl, nsProps, tableId, initialStorageOpts) = credentialVending(lanceDataset)
      val spec = VectorIndexSpec.fromArgs(indexType, args)
      val committed = new VectorIndexJob(
        this,
        readOptions,
        indexType,
        spec,
        fragmentIds,
        nsImpl,
        nsProps,
        tableId,
        initialStorageOpts).runAndCommit()
      return Seq(new GenericInternalRow(Array[Any](
        committed,
        UTF8String.fromString(indexName))))
    }

    val uuid = UUID.randomUUID()

    val dataset = Utils.openDatasetBuilder(readOptions).build()

    val indexBuildResult =
      createIndexJob(dataset, lanceDataset, readOptions, uuid.toString, fragmentIds).run()

    try {
      // Merge index metadata after all fragments are indexed
      dataset.mergeIndexMetadata(uuid.toString, indexType, Optional.empty())

      val fieldIdByName = dataset.getLanceSchema.fields().asScala
        .map(f => f.getName -> f.getId)
        .toMap
      val fieldIds = columns.map { column =>
        fieldIdByName.getOrElse(
          column,
          throw new IllegalArgumentException(s"Cannot find index column in Lance schema: $column"))
      }.toList

      val datasetVersion = dataset.version()

      val indexBuilder = Index
        .builder()
        .uuid(uuid)
        .name(indexName)
        .fields(fieldIds.map(java.lang.Integer.valueOf).asJava)
        .datasetVersion(datasetVersion)
        .indexDetails(indexBuildResult.indexDetails)
        .indexVersion(indexBuildResult.indexVersion)
        .indexType(indexBuildResult.indexType)
        .fragments(fragmentIds.asJava)
      indexBuildResult.createdAt.foreach(indexBuilder.createdAt)
      val index = indexBuilder.build()

      // Find existing indices with the same name to mark as removed (for replace)
      val removedIndices = dataset.getIndexes.asScala
        .filter(_.name() == indexName)
        .toList.asJava

      val op = AddIndexOperation.builder()
        .withNewIndices(Collections.singletonList(index))
        .withRemovedIndices(removedIndices)
        .build()
      val txn = new Transaction.Builder()
        .readVersion(dataset.version())
        .operation(op)
        .build()
      try {
        val newDataset = new CommitBuilder(dataset)
          .writeParams(readOptions.getStorageOptions)
          .execute(txn)
        newDataset.close()
      } finally {
        txn.close()
      }
    } finally {
      dataset.close()
    }

    Seq(new GenericInternalRow(Array[Any](
      fragmentIds.size.toLong,
      UTF8String.fromString(indexName))))
  }

  // Get namespace info from catalog if available (for credential vending on workers)
  private def credentialVending(lanceDataset: LanceDataset): (
      Option[String],
      Option[Map[String, String]],
      Option[List[String]],
      Option[Map[String, String]]) = catalog match {
    case nsCatalog: BaseLanceNamespaceSparkCatalog =>
      (
        Option(nsCatalog.getNamespaceImpl),
        Option(nsCatalog.getNamespaceProperties).map(_.asScala.toMap),
        Option(lanceDataset.readOptions().getTableId).map(_.asScala.toList),
        Option(lanceDataset.getInitialStorageOptions).map(_.asScala.toMap))
    case _ => (None, None, None, None)
  }

  private def createIndexJob(
      dataset: Dataset,
      lanceDataset: LanceDataset,
      readOptions: LanceSparkReadOptions,
      uuid: String,
      fragmentIds: List[Integer]): IndexJob = {
    val (nsImpl, nsProps, tableId, initialStorageOpts) = credentialVending(lanceDataset)

    IndexUtils.buildIndexType(method) match {
      case IndexType.BTREE =>
        val mode = args.find(_.name == "build_mode").map(_.value.asInstanceOf[String])
        mode match {
          case Some("range") =>
            return new RangeBasedBTreeIndexJob(
              this,
              readOptions,
              uuid,
              nsImpl,
              nsProps,
              tableId,
              initialStorageOpts,
              dataset.getVersion.getManifestSummary.getTotalRows)

          case Some("fragment") | None =>
            new FragmentBasedIndexJob(
              this,
              readOptions,
              uuid,
              fragmentIds,
              nsImpl,
              nsProps,
              tableId,
              initialStorageOpts)

          case Some(unknown) =>
            throw new IllegalArgumentException(
              s"Unrecognized build_mode: '$unknown'. Supported values are 'fragment' and 'range'.")
        }

      case _ =>
        new FragmentBasedIndexJob(
          this,
          readOptions,
          uuid,
          fragmentIds,
          nsImpl,
          nsProps,
          tableId,
          initialStorageOpts)
    }
  }
}

/**
 * Interface for index job to implement different indexing strategies.
 */
trait IndexJob extends Serializable {

  /** @return index metadata returned by workers. */
  def run(): IndexBuildResult
}

case class IndexBuildResult(
    indexDetails: Array[Byte],
    indexVersion: Int,
    createdAt: Option[Instant],
    indexType: IndexType) extends Serializable

/**
 * A job implementation for creating indexes on fragments of a dataset in parallel.
 * Each fragment is processed independently to build its local index, which will later be
 * merged into a global index structure.
 *
 * @param addIndexExec         The AddIndexExec instance that initiated this job
 * @param readOptions          Configuration options for reading the Lance dataset
 * @param uuid                 Unique identifier for this index operation
 * @param fragmentIds          List of fragment IDs to process
 * @param nsImpl               Optional namespace implementation class for credential vending
 * @param nsProps              Optional namespace properties for credential vending
 * @param tableId              Optional table identifier for credential vending
 * @param initialStorageOpts   Optional initial storage options for the dataset
 */
class FragmentBasedIndexJob(
    addIndexExec: AddIndexExec,
    readOptions: LanceSparkReadOptions,
    uuid: String,
    fragmentIds: List[Integer],
    nsImpl: Option[String],
    nsProps: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOpts: Option[Map[String, String]]) extends IndexJob {

  override def run(): IndexBuildResult = {
    val encodedReadOptions = encode(readOptions)
    val columns = addIndexExec.columns.toList
    val argsJson = IndexUtils.toJson(addIndexExec.args)

    // Build per-fragment tasks
    val tasks = fragmentIds.zipWithIndex.map { case (fid, pos) =>
      FragmentIndexTask(
        encodedReadOptions,
        columns,
        addIndexExec.method,
        argsJson,
        addIndexExec.indexName,
        uuid,
        fid,
        nsImpl,
        nsProps,
        tableId,
        initialStorageOpts,
        returnBuildResult = pos == 0)
    }.toSeq

    val results = addIndexExec.session.sparkContext
      .parallelize(tasks, tasks.size)
      .map(t => t.execute())
      .collect()

    IndexUtils.collectIndexBuildResult(results, IndexUtils.buildIndexType(addIndexExec.method))
  }
}

/**
 * A task to create index on a single fragment of the dataset.
 * This is used in distributed index creation where each fragment is processed independently.
 *
 * @param encodedReadOptions    Configuration for Lance dataset access, serialized
 * @param columns               column names to index
 * @param method                Indexing method to use (e.g., "fts")
 * @param argsJson              JSON string containing index parameters
 * @param indexName             Name of the index being created
 * @param uuid                  Unique identifier for this index operation
 * @param fragmentId            ID of the fragment to create index on
 * @param namespaceImpl         Implementation class for namespace operations
 * @param namespaceProperties   Properties of the namespace
 * @param tableId               Identifier for the table within the namespace
 * @param initialStorageOptions Initial storage configuration options
 * @param returnBuildResult     Whether this task should return commit metadata to the driver
 */
case class FragmentIndexTask(
    encodedReadOptions: String,
    columns: List[String],
    method: String,
    argsJson: String,
    indexName: String,
    uuid: String,
    fragmentId: Int,
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]],
    returnBuildResult: Boolean) extends Serializable {

  def execute(): String = {
    val readOptions = decode[LanceSparkReadOptions](encodedReadOptions)
    val indexType = IndexUtils.buildIndexType(method)
    val params = IndexParams.builder()
      .setScalarIndexParams(ScalarIndexParams.create(
        IndexUtils.buildScalarIndexParamType(method),
        argsJson))
      .build()

    val indexOptions = IndexOptions
      .builder(java.util.Arrays.asList(columns: _*), indexType, params)
      .replace(true)
      .withIndexName(indexName)
      .withIndexUUID(uuid)
      .withFragmentIds(Collections.singletonList(fragmentId))
      .build()

    val dataset = Utils.openDatasetBuilder(readOptions)
      .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
      .runtimeNamespace(
        namespaceImpl.orNull,
        namespaceProperties.map(_.asJava).orNull,
        tableId.map(_.asJava).orNull)
      .build()

    try {
      val createdIndex = dataset.createIndex(indexOptions)
      if (returnBuildResult) {
        encode(Some(IndexUtils.extractIndexBuildResult(createdIndex)))
      } else {
        encode(None: Option[IndexBuildResult])
      }
    } finally {
      dataset.close()
    }
  }
}

/**
 * A job implementation for creating range-based BTree indexes using preprocessed, globally sorted data.
 * This approach distributes data across multiple partitions based on ranges of values and creates
 * indexes on each range in parallel.
 *
 * @param addIndexExec       The AddIndexExec instance that initiated this job
 * @param readOptions        Configuration options for reading the Lance dataset
 * @param uuid               Unique identifier for this index operation
 * @param nsImpl             Optional namespace implementation class for credential vending
 * @param nsProps            Optional namespace properties for credential vending
 * @param tableId            Optional table identifier for credential vending
 * @param initialStorageOpts Optional initial storage options for the dataset
 * @param totalRows          Total number of rows in the dataset
 */
class RangeBasedBTreeIndexJob(
    addIndexExec: AddIndexExec,
    readOptions: LanceSparkReadOptions,
    uuid: String,
    nsImpl: Option[String],
    nsProps: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOpts: Option[Map[String, String]],
    totalRows: Long) extends IndexJob {

  private val VALUE_COLUMN_NAME = "value"
  private val DEFAULT_ROWS_PER_RANGE = 1000000L

  override def run(): IndexBuildResult = {
    if (addIndexExec.columns.size != 1) {
      throw new UnsupportedOperationException(
        "Range-based BTree index currently supports a single column only")
    }

    val session = addIndexExec.session
    val catalog = addIndexExec.catalog
    val ident = addIndexExec.ident
    val indexName = addIndexExec.indexName
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

    // Read specific column and _rowid from dataset
    val df = session.table(fullTableName)
    val selectDf =
      df.select(df.col(columns.head).as(VALUE_COLUMN_NAME), df.col(LanceDataset.ROW_ID_COLUMN.name))

    // Repartition the data to numRanges and sort by indexed column
    val rowsPerRange = addIndexExec.args.find(_.name == "rows_per_range").map(
      _.value.asInstanceOf[Long]).getOrElse(DEFAULT_ROWS_PER_RANGE)
    val numRange = Math.max(1L, totalRows / rowsPerRange.longValue())

    val rangeDf = selectDf
      .repartitionByRange(
        numRange.intValue(),
        selectDf.col(VALUE_COLUMN_NAME).asc)
      .sortWithinPartitions(selectDf.col(VALUE_COLUMN_NAME).asc)

    val indexBuilder = RangeBTreeIndexBuilder(
      encode(readOptions),
      columns,
      zoneSize,
      indexName,
      uuid,
      nsImpl,
      nsProps,
      tableId,
      initialStorageOpts,
      rangeDf.schema)

    val results = rangeDf.queryExecution.toRdd.mapPartitionsWithIndex { case (rangeId, rowsIter) =>
      indexBuilder.buildForRange(rangeId, rowsIter)
    }.collect()

    IndexUtils.collectIndexBuildResult(results, IndexType.BTREE)
  }

}

/**
 * A helper class for building a range-based B-tree index.
 * This class is serialized and sent to executors to build the index for a specific range of data.
 *
 * @param encodedReadOptions      Serialized configuration for Lance dataset access.
 * @param columns                 The names of the columns to be indexed.
 * @param zoneSize                Optional size of zones within the B-tree index.
 * @param indexName               The name of the index to be created.
 * @param uuid                    The unique identifier for this index creation operation.
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
    indexName: String,
    uuid: String,
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]],
    schema: StructType) extends Serializable {

  def buildForRange(rangeId: Int, rowsIter: Iterator[InternalRow]): Iterator[String] = {
    // Initialize writer to write data to arrow stream
    val allocator = LanceRuntime.allocator()
    val data =
      VectorSchemaRoot.create(LanceArrowUtils.toArrowSchema(schema, "UTC", false), allocator)
    val writer = LanceArrowWriter.create(data, schema)

    val fieldsNum = schema.fields.length

    // Write the rows in the range partition to arrow stream
    try {
      while (rowsIter.hasNext) {
        val row = rowsIter.next()
        (0 until fieldsNum).foreach { ordinal =>
          writer.field(ordinal).write(row, ordinal)
        }
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
      return Iterator(encode(None: Option[IndexBuildResult]))
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

      // Build btree index for data in this range
      val btreeParamsBuilder = BTreeIndexParams.builder().rangeId(rangeId)
      if (zoneSize.isDefined) {
        btreeParamsBuilder.zoneSize(zoneSize.get)
      }

      val scalarParams = btreeParamsBuilder.build()
      val indexParams = IndexParams.builder().setScalarIndexParams(scalarParams).build()

      val indexOptions = IndexOptions
        .builder(columns.asJava, IndexType.BTREE, indexParams)
        .replace(true)
        .withIndexName(indexName)
        .withIndexUUID(uuid)
        .withPreprocessedData(stream)
        .build()

      val createdIndex = dataset.createIndex(indexOptions)
      Iterator(encode(Some(IndexUtils.extractIndexBuildResult(createdIndex))))
    } finally {
      CloseableUtil.closeQuietly(stream)
      CloseableUtil.closeQuietly(reader)
      CloseableUtil.closeQuietly(data)
      CloseableUtil.closeQuietly(dataset)
    }
  }
}

/**
 * A job implementation for creating IVF-family vector indexes on fragments in parallel.
 *
 * Each Spark task calls [[org.lance.Dataset#createIndex]] with `withFragmentIds(List(fid))`
 * and vector index params; lance-core returns an *uncommitted* index segment per call. The
 * driver collects these segments and publishes them atomically under one logical index name
 * via [[org.lance.Dataset#commitExistingIndexSegments]].
 *
 * IVF centroids and (for IVF_PQ) the PQ codebook are trained once on the driver and
 * broadcast to executors so every per-fragment segment shares the same artifacts. This is
 * required by lance-core's distributed build path, which rejects per-fragment-trained
 * centroids, and ensures all segments land in the same query-time compatibility group.
 *
 * Replace-on-recreate semantics are preserved by pre-calling [[org.lance.Dataset#dropIndex]]
 * when a same-name index already exists.
 *
 * @param addIndexExec       AddIndexExec instance that initiated this job
 * @param readOptions        Configuration options for reading the Lance dataset
 * @param indexType          One of IVF_FLAT, IVF_PQ, IVF_SQ
 * @param spec               Serializable carrier for IVF/PQ/SQ build params
 * @param fragmentIds        Fragment IDs to process (one segment per fragment)
 * @param nsImpl             Optional namespace implementation class for credential vending
 * @param nsProps            Optional namespace properties for credential vending
 * @param tableId            Optional table identifier for credential vending
 * @param initialStorageOpts Optional initial storage options for the dataset
 */
class VectorIndexJob(
    addIndexExec: AddIndexExec,
    readOptions: LanceSparkReadOptions,
    indexType: IndexType,
    spec: VectorIndexSpec,
    fragmentIds: List[Integer],
    nsImpl: Option[String],
    nsProps: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOpts: Option[Map[String, String]]) extends Serializable {

  def runAndCommit(): Long = {
    val columns = addIndexExec.columns.toList
    if (columns.size != 1) {
      throw new IllegalArgumentException(
        s"Vector index supports a single column only, got: $columns")
    }
    val column = columns.head
    val indexName = addIndexExec.indexName

    // Driver-side training: lance-core's distributed build (createIndex + withFragmentIds)
    // requires precomputed IVF centroids — and for IVF_PQ, a precomputed codebook. Train
    // once here, broadcast to executors, and every per-fragment segment shares the same
    // centroids/codebook so segments land in the same compatibility group at query time.
    val (centroids, codebook) = trainArtifactsOnDriver(column)

    val trainedSpec = spec.copy(centroids = centroids, codebook = codebook)
    val encodedReadOptions = encode(readOptions)

    val tasks = fragmentIds.map { fid =>
      VectorIndexTask(
        encodedReadOptions,
        columns,
        indexType.name(),
        trainedSpec,
        indexName,
        fid,
        nsImpl,
        nsProps,
        tableId,
        initialStorageOpts)
    }.toSeq

    val handles: Array[LanceIndexHandle] = addIndexExec.session.sparkContext
      .parallelize(tasks, tasks.size)
      .map(_.execute())
      .collect()

    val segments = handles.toList.map(_.toIndex).asJava

    // commit_existing_index_segments is additive — pre-drop to preserve
    // CREATE-INDEX replace-on-recreate semantics.
    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      val sameNameExists = dataset.getIndexes.asScala.exists(_.name() == indexName)
      if (sameNameExists) {
        dataset.dropIndex(indexName)
      }
      dataset.commitExistingIndexSegments(indexName, column, segments)
    } finally {
      dataset.close()
    }

    handles.length.toLong
  }

  private def trainArtifactsOnDriver(column: String): (Array[Float], Array[Float]) = {
    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      val ivfBuilder = new IvfBuildParams.Builder().setNumPartitions(spec.numPartitions)
      spec.sampleRate.foreach(ivfBuilder.setSampleRate)
      spec.maxIters.foreach(ivfBuilder.setMaxIters)
      val centroids = VectorTrainer.trainIvfCentroids(dataset, column, ivfBuilder.build())

      val codebook: Array[Float] = indexType match {
        case IndexType.IVF_PQ =>
          val pqBuilder = new PQBuildParams.Builder()
          spec.numSubVectors.foreach(pqBuilder.setNumSubVectors)
          spec.pqNumBits.foreach(pqBuilder.setNumBits)
          spec.pqMaxIters.foreach(pqBuilder.setMaxIters)
          spec.sampleRate.foreach(pqBuilder.setSampleRate)
          VectorTrainer.trainPqCodebook(dataset, column, pqBuilder.build())
        case _ => null
      }

      (centroids, codebook)
    } finally {
      dataset.close()
    }
  }
}

/**
 * Executor-side task that builds one uncommitted vector index segment covering one fragment.
 * Returns a Spark-serializable [[LanceIndexHandle]] that the driver reconstitutes into an
 * [[org.lance.index.Index]] for commit.
 */
case class VectorIndexTask(
    encodedReadOptions: String,
    columns: List[String],
    indexTypeName: String,
    spec: VectorIndexSpec,
    indexName: String,
    fragmentId: Int,
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]]) extends Serializable {

  def execute(): LanceIndexHandle = {
    val readOptions = decode[LanceSparkReadOptions](encodedReadOptions)
    val indexType = IndexType.valueOf(indexTypeName)
    val vectorParams = spec.toVectorIndexParams(indexType)
    val params = IndexParams.builder().setVectorIndexParams(vectorParams).build()

    val indexOptions = IndexOptions
      .builder(java.util.Arrays.asList(columns: _*), indexType, params)
      .replace(true)
      .withIndexName(indexName)
      .withFragmentIds(Collections.singletonList(fragmentId))
      .build()

    val dataset = Utils.openDatasetBuilder(readOptions)
      .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
      .build()

    try {
      LanceIndexHandle.from(dataset.createIndex(indexOptions))
    } finally {
      dataset.close()
    }
  }
}

/**
 * Serializable carrier for IVF-family build parameters. Parsed from user WITH-args on the
 * driver and shipped to executors, where [[toVectorIndexParams]] rebuilds the native
 * [[VectorIndexParams]] (whose nested builders are not serializable).
 */
case class VectorIndexSpec(
    metricType: String,
    numPartitions: Int,
    sampleRate: Option[Int],
    maxIters: Option[Int],
    useResidual: Option[Boolean],
    // IVF_PQ
    numSubVectors: Option[Int],
    pqNumBits: Option[Int],
    pqMaxIters: Option[Int],
    // IVF_SQ
    sqNumBits: Option[Short],
    // Driver-trained artifacts passed into per-fragment builds.
    // Populated by VectorIndexJob before task dispatch; null before training.
    centroids: Array[Float] = null,
    codebook: Array[Float] = null) extends Serializable {

  def toVectorIndexParams(indexType: IndexType): VectorIndexParams = {
    val ivfBuilder = new IvfBuildParams.Builder().setNumPartitions(numPartitions)
    sampleRate.foreach(ivfBuilder.setSampleRate)
    maxIters.foreach(ivfBuilder.setMaxIters)
    useResidual.foreach(v => ivfBuilder.setUseResidual(v))
    if (centroids != null) ivfBuilder.setCentroids(centroids)
    val ivfParams = ivfBuilder.build()

    val dt = VectorIndexSpec.parseDistanceType(metricType)
    val b = new VectorIndexParams.Builder(ivfParams).setDistanceType(dt)

    indexType match {
      case IndexType.IVF_FLAT => // no sub-quantizer
      case IndexType.IVF_PQ =>
        val pqBuilder = new PQBuildParams.Builder()
        numSubVectors.foreach(pqBuilder.setNumSubVectors)
        pqNumBits.foreach(pqBuilder.setNumBits)
        pqMaxIters.foreach(pqBuilder.setMaxIters)
        sampleRate.foreach(pqBuilder.setSampleRate)
        if (codebook != null) pqBuilder.setCodebook(codebook)
        b.setPqParams(pqBuilder.build())
      case IndexType.IVF_SQ =>
        val sqBuilder = new SQBuildParams.Builder()
        sqNumBits.foreach(v => sqBuilder.setNumBits(v))
        sampleRate.foreach(sqBuilder.setSampleRate)
        b.setSqParams(sqBuilder.build())
      case other =>
        throw new IllegalArgumentException(s"Unsupported vector index type: $other")
    }

    b.build()
  }
}

object VectorIndexSpec {

  def fromArgs(indexType: IndexType, args: Seq[LanceNamedArgument]): VectorIndexSpec = {
    def argInt(name: String): Option[Int] = args.find(_.name == name).map { a =>
      a.value match {
        case i: java.lang.Integer => i.intValue()
        case l: java.lang.Long => l.intValue()
        case other =>
          throw new IllegalArgumentException(
            s"Vector index arg '$name' must be an integer, got: $other")
      }
    }
    def argBool(name: String): Option[Boolean] = args.find(_.name == name).map { a =>
      a.value match {
        case b: java.lang.Boolean => b.booleanValue()
        case other =>
          throw new IllegalArgumentException(
            s"Vector index arg '$name' must be a boolean, got: $other")
      }
    }
    def argString(name: String): Option[String] = args.find(_.name == name).map { a =>
      a.value match {
        case s: java.lang.String => s
        case other => String.valueOf(other)
      }
    }

    val numPartitions = argInt("num_partitions").getOrElse(
      throw new IllegalArgumentException(
        "Vector index requires 'num_partitions' in WITH clause"))

    val metric = argString("metric_type").getOrElse("l2")

    val sqBits: Option[Short] = indexType match {
      case IndexType.IVF_SQ => argInt("num_bits").map(_.toShort).orElse(Some(8.toShort))
      case _ => None
    }
    val pqBits: Option[Int] = indexType match {
      case IndexType.IVF_PQ => argInt("num_bits").orElse(Some(8))
      case _ => None
    }

    VectorIndexSpec(
      metricType = metric,
      numPartitions = numPartitions,
      sampleRate = argInt("sample_rate"),
      maxIters = argInt("max_iters"),
      useResidual = argBool("use_residual"),
      numSubVectors = argInt("num_sub_vectors"),
      pqNumBits = pqBits,
      pqMaxIters = argInt("pq_max_iters"),
      sqNumBits = sqBits)
  }

  def parseDistanceType(s: String): DistanceType = s.toLowerCase match {
    case "l2" | "euclidean" => DistanceType.L2
    case "cosine" => DistanceType.Cosine
    case "dot" | "inner_product" | "ip" => DistanceType.Dot
    case "hamming" => DistanceType.Hamming
    case other => throw new IllegalArgumentException(
        s"Unsupported metric_type '$other'; expected one of: l2, cosine, dot, hamming")
  }
}

/**
 * Spark-serializable snapshot of [[org.lance.index.Index]], used to ship uncommitted
 * index-segment metadata from executors back to the driver. `org.lance.index.Index` is
 * not itself `Serializable`, so we carry its fields as primitives and reconstitute via
 * [[org.lance.index.Index.Builder]] on the driver.
 */
case class LanceIndexHandle(
    uuid: String,
    fieldIds: List[java.lang.Integer],
    name: String,
    datasetVersion: Long,
    fragmentIds: List[java.lang.Integer],
    indexDetails: Array[Byte],
    indexVersion: Int,
    createdAtMillis: Long,
    baseId: java.lang.Integer,
    indexTypeName: String) extends Serializable {

  def toIndex: Index = {
    val b = Index
      .builder()
      .uuid(UUID.fromString(uuid))
      .fields(fieldIds.asJava)
      .name(name)
      .datasetVersion(datasetVersion)
      .fragments(fragmentIds.asJava)
      .indexVersion(indexVersion)
      .indexType(IndexType.valueOf(indexTypeName))
    if (indexDetails != null && indexDetails.nonEmpty) b.indexDetails(indexDetails)
    if (createdAtMillis >= 0) b.createdAt(Instant.ofEpochMilli(createdAtMillis))
    if (baseId != null) b.baseId(baseId)
    b.build()
  }
}

object LanceIndexHandle {
  def from(index: Index): LanceIndexHandle = {
    val fragments = index.fragments().orElse(Collections.emptyList[java.lang.Integer]())
    val details: Array[Byte] =
      if (index.indexDetails().isPresent) index.indexDetails().get() else Array.emptyByteArray
    val createdAt: Long =
      if (index.createdAt().isPresent) index.createdAt().get().toEpochMilli else -1L
    val baseId: java.lang.Integer =
      if (index.baseId().isPresent) index.baseId().get() else null
    LanceIndexHandle(
      uuid = index.uuid().toString,
      fieldIds = index.fields().asScala.toList,
      name = index.name(),
      datasetVersion = index.datasetVersion(),
      fragmentIds = fragments.asScala.toList,
      indexDetails = details,
      indexVersion = index.indexVersion(),
      createdAtMillis = createdAt,
      baseId = baseId,
      indexTypeName = index.indexType().name())
  }
}

/**
 * Utility methods for working with index types.
 */
object IndexUtils {

  private val jsonMapper = new ObjectMapper()

  private val VectorIndexTypes: Set[IndexType] = Set(
    IndexType.IVF_FLAT,
    IndexType.IVF_PQ,
    IndexType.IVF_SQ)

  def isVectorIndex(indexType: IndexType): Boolean = VectorIndexTypes.contains(indexType)

  /**
   * Build an [[IndexType]] from the given index method string.
   *
   * @param method the index method name
   * @return the corresponding [[IndexType]]
   * @throws UnsupportedOperationException if the method is not supported
   */
  def buildIndexType(method: String): IndexType = {
    method.toLowerCase match {
      case "btree" => IndexType.BTREE
      case "fts" => IndexType.INVERTED
      case "ivf_flat" => IndexType.IVF_FLAT
      case "ivf_pq" => IndexType.IVF_PQ
      case "ivf_sq" => IndexType.IVF_SQ
      case other => throw new UnsupportedOperationException(s"Unsupported index method: $other")
    }
  }

  def buildScalarIndexParamType(method: String): String = {
    method.toLowerCase match {
      case "btree" => "btree"
      case "fts" => "inverted"
      case other => throw new UnsupportedOperationException(s"Unsupported index method: $other")
    }
  }

  /** Extracts the commit metadata from a newly created Index. */
  def extractIndexBuildResult(index: Index): IndexBuildResult = {
    val details = index.indexDetails()
    if (!details.isPresent || details.get().isEmpty) {
      throw new IllegalStateException(
        s"Index ${index.name()} was created without index details")
    }
    val indexType = Option(index.indexType()).getOrElse {
      throw new IllegalStateException(s"Index ${index.name()} was created without index type")
    }
    IndexBuildResult(
      details.get().clone(),
      index.indexVersion(),
      Option(index.createdAt().orElse(null)),
      indexType)
  }

  /** Returns the first index metadata from serialized worker results. */
  def collectIndexBuildResult(
      encodedResults: Array[String],
      expectedType: IndexType): IndexBuildResult = {
    val first = encodedResults.iterator
      .map(encoded => decode[Option[IndexBuildResult]](encoded))
      .collectFirst { case Some(result) => result }
      .getOrElse(throw new IllegalStateException("No per-task index metadata was returned"))

    if (first.indexType != expectedType) {
      throw new IllegalStateException(
        s"Expected index type $expectedType but worker returned ${first.indexType}")
    }
    if (first.indexDetails.isEmpty) {
      throw new IllegalStateException("Per-task index metadata is missing index details")
    }

    first
  }

  def toJson(args: Seq[LanceNamedArgument]): String = {
    if (args.isEmpty) {
      "{}"
    } else {
      val node: ObjectNode = jsonMapper.createObjectNode()
      args.foreach { a =>
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

}
