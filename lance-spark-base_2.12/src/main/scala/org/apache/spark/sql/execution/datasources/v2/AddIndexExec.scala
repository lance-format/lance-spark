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

import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{AddIndexOutputType, LanceNamedArgument}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.LanceArrowUtils
import org.apache.spark.sql.util.LanceSerializeUtil.{decode, encode}
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.{compact, render}
import org.lance.{CommitBuilder, Dataset, Transaction}
import org.lance.index.{Index, IndexOptions, IndexParams, IndexType}
import org.lance.index.scalar.{BTreeIndexParams, ScalarIndexParams}
import org.lance.operation.{CreateIndex => AddIndexOperation}
import org.lance.spark.{BaseLanceNamespaceSparkCatalog, LanceDataset, LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.arrow.LanceArrowWriter
import org.lance.spark.utils.{CloseableUtil, Utils}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
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

    val uuid = UUID.randomUUID()
    val indexType = IndexUtils.buildIndexType(method)

    // Create distributed index job and run it
    createIndexJob(lanceDataset, readOptions, uuid.toString, fragmentIds).run()

    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      // Merge index metadata after all fragments are indexed
      dataset.mergeIndexMetadata(uuid.toString, indexType, Optional.empty())

      val fieldIds = dataset.getLanceSchema.fields().asScala
        .filter(f => columns.contains(f.getName))
        .map(_.getId)
        .toList

      val datasetVersion = dataset.version()

      val index = Index
        .builder()
        .uuid(uuid)
        .name(indexName)
        .fields(fieldIds.map(java.lang.Integer.valueOf).asJava)
        .datasetVersion(datasetVersion)
        .indexVersion(0)
        .fragments(fragmentIds.asJava)
        .build()

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

  private def createIndexJob(
      lanceDataset: LanceDataset,
      readOptions: LanceSparkReadOptions,
      uuid: String,
      fragmentIds: List[Integer]): IndexJob = {
    // Get namespace info from catalog if available (for credential vending on workers)
    val (nsImpl, nsProps, tableId, initialStorageOpts): (
        Option[String],
        Option[Map[String, String]],
        Option[List[String]],
        Option[Map[String, String]]) = catalog match {
      case nsCatalog: BaseLanceNamespaceSparkCatalog =>
        (
          Option(nsCatalog.getNamespaceImpl),
          Option(nsCatalog.getNamespaceProperties).map(_.asScala.toMap),
          Option(readOptions.getTableId).map(_.asScala.toList),
          Option(lanceDataset.getInitialStorageOptions).map(_.asScala.toMap))
      case _ => (None, None, None, None)
    }

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
              initialStorageOpts)

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
  def run(): Unit
}

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

  override def run(): Unit = {
    val encodedReadOptions = encode(readOptions)
    val columns = addIndexExec.columns.toList
    val argsJson = IndexUtils.toJson(addIndexExec.args)

    // Build per-fragment tasks
    val tasks = fragmentIds.map { fid =>
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
        initialStorageOpts)
    }.toSeq

    addIndexExec.session.sparkContext
      .parallelize(tasks, tasks.size)
      .map(t => t.execute())
      .collect()
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
    initialStorageOptions: Option[Map[String, String]]) extends Serializable {

  def execute(): String = {
    val readOptions = decode[LanceSparkReadOptions](encodedReadOptions)
    val indexType = IndexUtils.buildIndexType(method)
    val params = IndexParams.builder()
      .setScalarIndexParams(ScalarIndexParams.create(method, argsJson))
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
      .build()

    try {
      dataset.createIndex(indexOptions)
    } finally {
      dataset.close()
    }

    encode("OK")
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
 */
class RangeBasedBTreeIndexJob(
    addIndexExec: AddIndexExec,
    readOptions: LanceSparkReadOptions,
    uuid: String,
    nsImpl: Option[String],
    nsProps: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOpts: Option[Map[String, String]]) extends IndexJob {

  private val VALUE_COLUMN_NAME = "value"

  override def run(): Unit = {
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
    val rangeDf = selectDf
      .repartitionByRange(
        session.sessionState.conf.numShufflePartitions,
        selectDf.col(VALUE_COLUMN_NAME).asc)
      .sortWithinPartitions(VALUE_COLUMN_NAME)

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

    rangeDf.queryExecution.toRdd.mapPartitionsWithIndex { case (rangeId, rowsIter) =>
      indexBuilder.buildForRange(rangeId, rowsIter)
    }.collect()
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

  def buildForRange(rangeId: Int, rowsIter: Iterator[InternalRow]): Iterator[Unit] = {
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
      return Iterator.empty
    }

    // Serialize the arrow stream to byte array
    val out = new ByteArrayOutputStream()
    val streamWriter = new ArrowStreamWriter(data, null, out)
    try {
      streamWriter.start()
      streamWriter.writeBatch()
      streamWriter.end()
    } finally {
      CloseableUtil.closeQuietly(streamWriter)
      CloseableUtil.closeQuietly(data)
    }

    val arrowData = out.toByteArray()
    val in = new ByteArrayInputStream(arrowData)

    // Export data to lance
    val reader = new ArrowStreamReader(in, allocator)
    val stream = ArrowArrayStream.allocateNew(allocator)

    var dataset: Dataset = null

    try {
      dataset = Utils.openDatasetBuilder(
        decode[LanceSparkReadOptions](encodedReadOptions))
        .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
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

      dataset.createIndex(indexOptions)
    } finally {
      CloseableUtil.closeQuietly(stream)
      CloseableUtil.closeQuietly(reader)

      if (dataset != null) {
        CloseableUtil.closeQuietly(dataset)
      }
    }

    Iterator.empty
  }
}

/**
 * Utility methods for working with index types.
 */
object IndexUtils {

  /**
   * Build an [[IndexType]] from the given index method string.
   *
   * @param method the index method name
   * @return the corresponding [[IndexType]]
   * @throws UnsupportedOperationException if the method is not supported
   */
  def buildIndexType(method: String): IndexType = {
    method match {
      case "btree" => IndexType.BTREE
      case "fts" => IndexType.INVERTED
      case other => throw new UnsupportedOperationException(s"Unsupported index method: $other")
    }
  }

  def toJson(args: Seq[LanceNamedArgument]): String = {
    if (args.isEmpty) {
      "{}"
    } else {
      val fields = args.map { a =>
        val jv = a.value match {
          case null => JNull
          case s: java.lang.String =>
            val trimmed = s.stripPrefix("\"").stripSuffix("\"").stripPrefix("'").stripSuffix("'")
            JString(trimmed)
          case b: java.lang.Boolean => JBool(b.booleanValue())
          case c: java.lang.Character => JString(String.valueOf(c))
          case by: java.lang.Byte => JInt(BigInt(by.intValue()))
          case sh: java.lang.Short => JInt(BigInt(sh.intValue()))
          case i: java.lang.Integer => JInt(BigInt(i.intValue()))
          case l: java.lang.Long => JInt(BigInt(l.longValue()))
          case f: java.lang.Float => JDouble(f.doubleValue())
          case d: java.lang.Double => JDouble(d.doubleValue())
          case other => JString(String.valueOf(other))
        }
        JField(a.name, jv)
      }
      compact(render(JObject(fields.toList)))
    }
  }

}
