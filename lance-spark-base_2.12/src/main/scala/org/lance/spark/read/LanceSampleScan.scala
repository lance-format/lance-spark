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
package org.lance.spark.read

import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.lance.Dataset
import org.lance.spark.{LanceConstant, LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.internal.ExecutorNamespace
import org.lance.spark.read.metric.LanceCustomMetrics
import org.lance.spark.utils.{BlobUtils, Utils}
import org.lance.spark.vectorized.LanceArrowColumnVector

import java.io.IOException
import java.util.{ArrayList, Map => JMap}

import scala.collection.JavaConverters._

/** An exact sample is a relation boundary: only column pruning may cross it. */
class LanceSampleScanBuilder(
    tableSchema: StructType,
    options: LanceSparkReadOptions,
    population: Long,
    initialStorageOptions: JMap[String, String],
    namespaceImpl: String,
    namespaceProperties: JMap[String, String])
  extends SupportsPushDownRequiredColumns {

  private val fullSchema = BlobUtils.applyBlobV2DescriptorSchema(tableSchema)
  private var schema = fullSchema
  private var scan: LanceSampleScan = _

  private lazy val selection =
    ExactKSampler.sample(population, options.getSampleSize.longValue(), options.getSampleSeed)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    ReadSchemaNestedStructWidening.validateRequiredSchema(requiredSchema, fullSchema)
    schema = ReadSchemaNestedStructWidening.widenRequiredSchema(requiredSchema, fullSchema)
    scan = null
  }

  override def build(): Scan = synchronized {
    if (scan == null) {
      schema.fields.foreach { field =>
        val stored = fullSchema.fields.find(_.name == field.name)
        val original = stored.getOrElse(field)
        val virtualBlob = stored.isEmpty &&
          BlobUtils.blobV1VirtualColumnBaseName(field.name) != null
        require(
          !containsBlob(original) && !virtualBlob,
          s"Exact sampling does not support projected blob column '${field.name}'")
        require(!Utils.isScoreMetadataColumn(field), "Exact sampling does not support _score")
      }
      val positions = selection
      val parallelism = math.max(1, SparkSession.active.sparkContext.defaultParallelism)
      val partitions = if (positions.isEmpty) Array.empty[InputPartition]
      else {
        val maxTasks = 1 + (positions.length - 1) / options.getSampleBatchSize
        val count = math.min(parallelism, maxTasks)
        (0 until count).map { index =>
          val start = (positions.length.toLong * index / count).toInt
          val end = (positions.length.toLong * (index + 1) / count).toInt
          LanceSampleInputPartition(
            schema,
            options,
            positions.slice(start, end),
            initialStorageOptions,
            namespaceImpl,
            namespaceProperties): InputPartition
        }.toArray
      }
      scan = new LanceSampleScan(schema, options, population, partitions)
    }
    scan
  }

  // Native take projects the entire top-level column, including nested blob children.
  private def containsBlob(field: StructField): Boolean =
    field.metadata.contains(BlobUtils.LANCE_ENCODING_BLOB_KEY) ||
      BlobUtils.isBlobV2SparkField(field) || containsBlob(field.dataType)

  private def containsBlob(dataType: DataType): Boolean = dataType match {
    case struct: StructType => struct.fields.exists(containsBlob)
    case array: ArrayType => containsBlob(array.elementType)
    case map: MapType => containsBlob(map.keyType) || containsBlob(map.valueType)
    case _ => false
  }
}

class LanceSampleScan(
    schema: StructType,
    options: LanceSparkReadOptions,
    population: Long,
    partitions: Array[InputPartition])
  extends Scan with Batch with SupportsMetadata {

  override def readSchema(): StructType = schema
  override def toBatch: Batch = this
  override def description(): String =
    s"LanceSampleScan [snapshot=${options.getRef}, population=$population, " +
      s"sample_size=${options.getSampleSize}, sample_seed=${options.getSampleSeed}, " +
      s"sampler=${ExactKSampler.Version}, sample_batch_size=${options.getSampleBatchSize}]"

  override def getMetaData(): Map[String, String] = Map(
    "snapshot" -> options.getRef.toString,
    "population" -> population.toString,
    "sample_size" -> options.getSampleSize.toString,
    "sample_seed" -> options.getSampleSeed.toString,
    "sampler" -> ExactKSampler.Version,
    "sample_batch_size" -> options.getSampleBatchSize.toString,
    "numPartitions" -> partitions.length.toString)

  override def planInputPartitions(): Array[InputPartition] = partitions.clone()
  override def createReaderFactory(): PartitionReaderFactory = new LanceSampleReaderFactory
  override def supportedCustomMetrics(): Array[CustomMetric] = LanceCustomMetrics.samplingMetrics()
}

@SerialVersionUID(1L)
case class LanceSampleInputPartition(
    schema: StructType,
    options: LanceSparkReadOptions,
    positions: Array[Long],
    initialStorageOptions: JMap[String, String],
    namespaceImpl: String,
    namespaceProperties: JMap[String, String]) extends InputPartition

class LanceSampleReaderFactory extends PartitionReaderFactory {
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    new LanceSamplePartitionReader(partition.asInstanceOf[LanceSampleInputPartition])

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new LanceRowPartitionReader(
      new LanceSamplePartitionReader(partition.asInstanceOf[LanceSampleInputPartition]))
}

class LanceSamplePartitionReader(partition: LanceSampleInputPartition)
  extends PartitionReader[ColumnarBatch] {

  private var namespace: ExecutorNamespace = _
  private var dataset: Dataset = _
  private var reader: ArrowReader = _
  private var current: ColumnarBatch = _
  private var offset = 0
  private var expectedRows = 0
  private var returnedRows = 0L
  private var closed = false
  private var takeCalls = 0L
  private var rowsRequested = 0L
  private var rowsReturned = 0L
  private var takeTimeNs = 0L
  private val columns = {
    val requested = partition.schema.fields.map(_.name).filterNot(_ == LanceConstant.FRAGMENT_ID)
    // Keep row counts for cardinality-only scans without relying on native empty-projection
    // defaults; deriving _fragid also requires each selected row's address.
    val withAddress =
      if (requested.isEmpty ||
        partition.schema.fieldNames.contains(LanceConstant.FRAGMENT_ID)) {
        requested :+ LanceConstant.ROW_ADDRESS
      } else requested
    withAddress.distinct.toList.asJava
  }

  private val metrics = Array[CustomTaskMetric](
    taskMetric(LanceCustomMetrics.NUM_TAKE_CALLS, () => takeCalls),
    taskMetric(LanceCustomMetrics.NUM_ROWS_REQUESTED, () => rowsRequested),
    taskMetric(LanceCustomMetrics.NUM_ROWS_RETURNED, () => rowsReturned),
    taskMetric(LanceCustomMetrics.TAKE_TIME_NS, () => takeTimeNs))

  private def taskMetric(metricName: String, readValue: () => Long): CustomTaskMetric =
    new CustomTaskMetric {
      override def name(): String = metricName
      override def value(): Long = readValue()
    }

  override def currentMetricsValues(): Array[CustomTaskMetric] = metrics
  override def get(): ColumnarBatch = current

  @throws[IOException]
  override def next(): Boolean = {
    if (closed) return false
    try nextBatch()
    catch {
      case error: Throwable =>
        throw LanceColumnarPartitionReader.asIOException(closeResources(error))
    }
  }

  private def nextBatch(): Boolean = {
    if (current != null) {
      current.close()
      current = null
    }
    while (true) {
      if (reader != null) {
        if (reader.loadNextBatch()) {
          val root = reader.getVectorSchemaRoot
          returnedRows += root.getRowCount
          if (returnedRows > expectedRows) {
            throw new IOException("Lance take returned more rows than requested")
          }
          current = toBatch(root)
          rowsReturned += root.getRowCount
          return true
        }
        val completed = reader
        reader = null
        completed.close()
        if (returnedRows != expectedRows) {
          throw new IOException(
            s"Lance take returned $returnedRows rows; expected $expectedRows at ${partition.options.getRef}")
        }
      }
      if (offset == partition.positions.length) {
        close()
        return false
      }
      if (dataset == null) {
        namespace = ExecutorNamespace.acquire(
          partition.options,
          partition.namespaceImpl,
          partition.namespaceProperties)
        dataset = Utils.openDatasetBuilder(partition.options)
          .initialStorageOptions(partition.initialStorageOptions).build()
        if (dataset.version() != partition.options.getRef.getVersionNumber.get()) {
          throw new IOException("Lance sampling opened an unexpected snapshot")
        }
      }
      expectedRows =
        math.min(partition.options.getSampleBatchSize, partition.positions.length - offset)
      val indices = new ArrayList[java.lang.Long](expectedRows)
      var i = 0
      while (i < expectedRows) {
        indices.add(partition.positions(offset + i))
        i += 1
      }
      val start = System.nanoTime()
      try reader = dataset.take(indices, columns)
      finally takeTimeNs += System.nanoTime() - start
      takeCalls += 1
      rowsRequested += expectedRows
      returnedRows = 0
      offset += expectedRows
    }
    false
  }

  private def toBatch(root: VectorSchemaRoot): ColumnarBatch = {
    val vectors = new Array[ColumnVector](partition.schema.length)
    try {
      partition.schema.fields.zipWithIndex.foreach { case (field, index) =>
        if (field.name == LanceConstant.FRAGMENT_ID) {
          val addresses =
            new LanceArrowColumnVector(root.getVector(LanceConstant.ROW_ADDRESS), false)
          val fragments = new IntVector(LanceConstant.FRAGMENT_ID, LanceRuntime.allocator())
          // Register the owned vector before allocation so error cleanup also releases it.
          vectors(index) = new LanceArrowColumnVector(fragments, true, field)
          fragments.allocateNew(root.getRowCount)
          var row = 0
          while (row < root.getRowCount) {
            fragments.setSafe(row, (addresses.getLong(row) >>> 32).toInt)
            row += 1
          }
          fragments.setValueCount(root.getRowCount)
        } else {
          val vector = root.getVector(field.name)
          if (vector == null) throw new IOException(s"Lance take did not return '${field.name}'")
          vectors(index) = new LanceArrowColumnVector(vector, false, field)
        }
      }
      new ColumnarBatch(vectors, root.getRowCount)
    } catch {
      case error: Throwable =>
        vectors.filter(_ != null).foreach { vector =>
          try vector.close()
          catch { case closeError: Throwable => error.addSuppressed(closeError) }
        }
        throw error
    }
  }

  @throws[IOException]
  override def close(): Unit = {
    val error = closeResources(null)
    if (error != null) throw LanceColumnarPartitionReader.asIOException(error)
  }

  private def closeResources(original: Throwable): Throwable = {
    if (closed) return original
    closed = true
    var error = original
    def release(action: => Unit): Unit = {
      try action
      catch {
        case closeError: Throwable =>
          if (error == null) error = closeError
          else if (error ne closeError) error.addSuppressed(closeError)
      }
    }
    if (current != null) release(current.close())
    if (reader != null) release(reader.close())
    if (dataset != null) release(dataset.close())
    if (namespace != null) release(namespace.close())
    current = null
    reader = null
    dataset = null
    namespace = null
    error
  }
}
