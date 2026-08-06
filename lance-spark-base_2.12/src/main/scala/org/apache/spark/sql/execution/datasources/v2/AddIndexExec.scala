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
import org.lance.index.{Index, IndexOptions, IndexParams, IndexType}
import org.lance.index.scalar.{BTreeIndexParams, ScalarIndexParams}
import org.lance.operation.{CreateIndex => AddIndexOperation}
import org.lance.spark.{BaseLanceNamespaceSparkCatalog, LanceDataset, LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.arrow.LanceArrowWriter
import org.lance.spark.utils.{CloseableUtil, FieldPathUtils, Utils}
import org.lance.spark.write.SingleBatchArrowReader

import java.time.Instant
import java.util.{Collections, Locale, Optional, UUID}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private case class AddIndexTableSnapshot(
    readOptions: LanceSparkReadOptions,
    fragmentIds: List[Integer],
    canonicalColumns: Seq[String],
    vectorPlan: Option[VectorIndexPlan])

/**
 * Physical execution of distributed CREATE INDEX (ALTER TABLE ... CREATE INDEX ...) for Lance datasets.
 *
 * <p>Index creation behaviour is controlled by the WITH clause options:
 * <ul>
 * <li><b>BTREE</b>: uses a range-based approach that redistributes and sorts data across
 *   partitions, creates indexes for each range in parallel, and merges them into a global
 *   index structure. Supports {@code build_mode='range'} (default) and
 *   {@code build_mode='fragment'}.
 * <li><b>FTS / INVERTED</b>: processes each fragment independently in parallel, merges index
 *   metadata, and commits an index-creation transaction.
 * <li><b>ZONEMAP / BITMAP / LABEL_LIST / NGRAM / BLOOMFILTER / RTREE</b>: build uncommitted
 *   index segments in parallel across fragment batches and commit the logical index on the driver.
 * </ul>
 *
 * <p><b>Deferred training ({@code WITH (train=false)})</b>: commits an empty index on the driver
 * with an empty fragment bitmap (all rows appear unindexed), skipping data processing. Supported
 * for all scalar index methods ({@code btree}, {@code fts}, and the scalar-segment methods —
 * {@code zonemap}, {@code bitmap}, {@code label_list}, {@code ngram}, {@code bloomfilter},
 * {@code rtree}). Rejected for {@code IVF_*} vector index types because Lance does not currently
 * expose a vector-aware empty-index commit path. Empty scalar tables use the same path even when
 * {@code train=true}; empty tables with an {@code IVF_*} method are rejected with a clear message
 * because there are no vectors to train the centroids on. Populate the index later by re-running
 * {@code CREATE INDEX} with the same name (a full distributed build that replaces the empty
 * index) or, for incremental coverage of appended fragments, by {@code Dataset.optimizeIndices}
 * (the SQL {@code OPTIMIZE} only compacts fragments). {@code num_segments} is rejected with
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
    args: Seq[LanceNamedArgument]) extends LeafV2CommandExec
  with org.apache.spark.internal.Logging {

  override def output: Seq[Attribute] = AddIndexOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case d: LanceDataset => d
      case _ => throw new UnsupportedOperationException("AddIndex only supports LanceDataset")
    }

    val baseReadOptions = lanceDataset.readOptions()
    val (nsImpl, nsProps, tableId, initialStorageOpts) =
      extractNamespaceInfo(lanceDataset, baseReadOptions)

    val train = IndexUtils.extractTrain(args)
    val indexType = IndexUtils.buildIndexType(method)
    val scalarSegmentIndexType = IndexUtils.scalarSegmentIndexType(method)
    val btreeBuildMode = IndexUtils.btreeBuildMode(indexType, args)

    // Deferred training (train=false) is only supported for scalar index methods today.
    // commitEmptyIndex(...) below builds ScalarIndexParams via buildScalarIndexParamType,
    // which has no IVF_* path. Until lance-core exposes an empty IndexOptions builder for
    // vector indices, reject the combination up front so the user sees a clear message instead
    // of "Unsupported index method: ivf_pq" coming out of the commit-empty path.
    if (!train && IndexUtils.isIvfIndexType(indexType)) {
      throw new IllegalArgumentException(
        s"train=false is not supported for $indexType. Run CREATE INDEX without train=false " +
          "(full distributed build) or use Dataset.optimizeIndices for incremental coverage of " +
          "appended fragments.")
    }

    val numSegmentsOpt = args.find(_.name == "num_segments")
    if (numSegmentsOpt.isDefined && !IndexUtils.useLogicalSegmentCommit(indexType)) {
      throw new IllegalArgumentException(
        s"num_segments is not supported for ${indexType.name()} indexes")
    }
    if (numSegmentsOpt.isDefined && !train) {
      throw new IllegalArgumentException(
        "num_segments is not supported with train=false: a deferred index performs no segmented build")
    }
    val parsedNumSegments: Option[Int] = numSegmentsOpt.map { arg =>
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

    // SQ-quantized IVF variants (IVF_SQ, IVF_HNSW_SQ) must be built as a single segment.
    // lance-core trains the ScalarQuantizer per createIndex call, and
    // commit_existing_index_segments does NOT reconcile per-shard SQ bounds — it keeps the
    // first shard's ScalarQuantizationMetadata and concatenates u8 codes byte-for-byte
    // (rust/lance-index/src/vector/distributed/index_merger.rs IVF_SQ branch). Different
    // bounds across segments => silently corrupt distance computation. Force-clamp to 1
    // until lance-core exposes a driver-side SQ trainer (tracked in the SQ-shared-artifact
    // follow-up issue against lance-format/lance).
    val validatedNumSegments: Option[Int] =
      if (IndexUtils.isSqIvfIndexType(indexType)) {
        parsedNumSegments match {
          case Some(req) if req > 1 =>
            logWarning(
              s"$indexType: num_segments=$req requested, but SQ-quantized IVF builds are " +
                "currently forced to a single segment because lance-core does not reconcile " +
                "per-segment ScalarQuantizer bounds across shards (see follow-up issue). " +
                "Downgrading to num_segments=1.")
          case _ =>
            logInfo(
              s"$indexType: forcing single-segment build (lance-core does not yet support " +
                "shared SQ bounds across segments).")
        }
        Some(1)
      } else {
        parsedNumSegments
      }

    // Resolve canonical columns + fragment ids + (optional) vector plan against a pinned
    // dataset version. Absorbs the upstream #740 change to use resolveField for scalar-segment
    // methods so container fields (label_list / rtree) resolve, while other methods keep the
    // existing resolveLeafField semantics.
    val snapshot: AddIndexTableSnapshot = {
      val ds = IndexUtils.openDataset(baseReadOptions, initialStorageOpts, nsImpl, nsProps, tableId)
      try {
        val canonical = columns.map { column =>
          val field = scalarSegmentIndexType match {
            case Some(_) => FieldPathUtils.resolveField(ds.getLanceSchema, column)
            case None => FieldPathUtils.resolveLeafField(ds.getLanceSchema, column)
          }
          FieldPathUtils.pathByFieldId(ds.getLanceSchema, field.getId)
        }
        // Single-column check: no index type currently supports more than one column.
        // Centralising here avoids per-type checks scattered further down (ZONEMAP, IVF, FTS).
        // The range-based BTree path re-validates on its own args-derived list; that separate
        // check stays local to that job.
        if (canonical.size != 1) {
          throw new UnsupportedOperationException(
            s"$indexType supports a single column only " +
              s"(got ${canonical.size}: ${canonical.mkString(", ")})")
        }
        val vectorPlan = if (IndexUtils.isIvfIndexType(indexType)) {
          val arrowSchema = ds.getLanceSchema.asArrowSchema()
          val arrowField =
            try {
              arrowSchema.findField(canonical.head)
            } catch {
              case _: IllegalArgumentException => null
            }
          if (arrowField == null) {
            val available = arrowSchema.getFields.asScala.map(_.getName).mkString(", ")
            throw new IllegalArgumentException(
              s"Column '${canonical.head}' not found. Available: $available")
          }
          val dim = VectorIndexParamsResolver.validateVectorFieldForIndex(
            canonical.head,
            arrowField)
          Some(VectorIndexParamsResolver.parseAndValidate(
            indexType,
            args,
            dim,
            ds.countRows()))
        } else None
        AddIndexTableSnapshot(
          readOptions = baseReadOptions.withVersion(ds.version()),
          fragmentIds = ds.getFragments.asScala.map(_.getId).map(Integer.valueOf).toList,
          canonicalColumns = canonical,
          vectorPlan = vectorPlan)
      } finally {
        ds.close()
      }
    }

    val readOptions = snapshot.readOptions
    val fragmentIds = snapshot.fragmentIds
    val canonicalColumns = snapshot.canonicalColumns

    // Empty-table CREATE INDEX: scalar methods fall through to commitEmptyIndex below and
    // register a zero-fragment index visible to SHOW INDEXES. IVF_* is rejected up front —
    // Lance cannot train IVF centroids without vectors, and commitEmptyIndex resolves
    // scalar params only.
    if (fragmentIds.isEmpty && IndexUtils.isIvfIndexType(indexType)) {
      throw new IllegalArgumentException(
        s"CREATE INDEX $indexType on an empty table is not supported: there are no vectors " +
          "to train IVF centroids. Insert data before running CREATE INDEX, or use " +
          "Dataset.optimizeIndices to build incrementally after appending fragments.")
    }

    // train=false, or an empty scalar table: commit an empty index on the driver and skip
    // data processing. Index and option validation above still applies to empty tables.
    if (!train || fragmentIds.isEmpty) {
      val uuid = UUID.randomUUID()
      val dataset =
        IndexUtils.openDataset(readOptions, initialStorageOpts, nsImpl, nsProps, tableId)
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

    // Logical segment commit path: scalar-segment methods (ZONEMAP / BITMAP / LABEL_LIST /
    // NGRAM / BLOOM_FILTER / RTREE) go through ScalarSegmentIndexJob; IVF_* goes through
    // VectorIndexJob.
    if (IndexUtils.useLogicalSegmentCommit(indexType)) {
      val segments: Seq[Index] = indexType match {
        case scalar if scalarSegmentIndexType.isDefined =>
          val _ = scalar
          val scalarSegmentJob = new ScalarSegmentIndexJob(
            this.copy(columns = canonicalColumns),
            readOptions,
            fragmentIds,
            validatedNumSegments,
            nsImpl,
            nsProps,
            tableId,
            initialStorageOpts)
          scalarSegmentJob.run()

        case vectorType if IndexUtils.isIvfIndexType(vectorType) =>
          val plan = snapshot.vectorPlan.getOrElse {
            throw new IllegalStateException(s"Vector plan was not resolved for $vectorType")
          }
          val vectorJob = new VectorIndexJob(
            this.copy(columns = canonicalColumns),
            readOptions,
            fragmentIds,
            plan,
            indexName,
            canonicalColumns,
            validatedNumSegments,
            nsImpl,
            nsProps,
            tableId,
            initialStorageOpts)
          vectorJob.runSegments()

        case other =>
          throw new IllegalStateException(s"Unexpected logical-segment type: $other")
      }
      // Atomic add+remove via Lance core; see commitIndexSegments
      // Atomic add+remove via Lance core; see commitIndexSegments
      commitIndexSegments(readOptions, canonicalColumns.head, segments)
      return Seq(new GenericInternalRow(Array[Any](
        fragmentIds.size.toLong,
        UTF8String.fromString(indexName))))
    }

    // BTree uses the Lance segment-commit path. Each worker builds an
    // uncommitted, per-fragment segment whose UUID is generated by Lance: a
    // caller-supplied index_uuid is rejected for distributed BTree builds, and
    // the driver-side mergeIndexMetadata path is no longer supported for BTree.
    // Both fragment mode (Lance sorts each fragment) and range mode (Spark
    // pre-sorts each fragment's data) produce segments with disjoint fragment
    // coverage, so they can be committed as a single logical index directly.
    if (indexType == IndexType.BTREE) {
      val segments: Seq[Index] =
        if (btreeBuildMode.contains("range")) {
          new RangeBasedBTreeIndexJob(
            this.copy(columns = canonicalColumns),
            readOptions,
            fragmentIds.size,
            nsImpl,
            nsProps,
            tableId,
            initialStorageOpts).run()
        } else {
          new BTreeFragmentIndexJob(
            this.copy(columns = canonicalColumns),
            readOptions,
            fragmentIds,
            nsImpl,
            nsProps,
            tableId,
            initialStorageOpts).run()
        }
      commitIndexSegments(readOptions, canonicalColumns.head, segments)
      return Seq(new GenericInternalRow(Array[Any](
        fragmentIds.size.toLong,
        UTF8String.fromString(indexName))))
    }

    // FTS/INVERTED still uses a caller-assigned UUID: each fragment writes
    // partial metadata under that UUID and the driver merges them into a single
    // index root before committing the resulting index transaction.
    val uuid = UUID.randomUUID()
    val dataset = Utils.openDatasetBuilder(readOptions).build()

    // The finally closes the dataset on every exit path. If the index-build job or the commit
    // below fails, partial index files written under this uuid (per-fragment partials and any
    // merged root) stay uncommitted: unreferenced by the manifest, so invisible to readers, and
    // eventually reclaimable by VACUUM with delete_unverified = true.
    try {
      val indexBuildResult =
        new FragmentBasedIndexJob(
          this.copy(columns = canonicalColumns),
          readOptions,
          uuid.toString,
          fragmentIds,
          nsImpl,
          nsProps,
          tableId,
          initialStorageOpts).run()

      // Merge index metadata after all fragments are indexed
      dataset.mergeIndexMetadata(uuid.toString, indexType, Optional.empty())

      val fieldIds = canonicalColumns.map { column =>
        FieldPathUtils.resolveLeafField(dataset.getLanceSchema, column).getId
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
  private def commitIndexSegments(
      readOptions: LanceSparkReadOptions,
      column: String,
      segments: Seq[Index]): Unit = {
    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      dataset.commitExistingIndexSegments(
        indexName,
        column,
        segments.toList.asJava)
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
 * A job implementation for creating a distributed BTree index, one uncommitted segment per
 * fragment. Segment UUIDs are generated by Lance (not the caller) and returned in the index
 * metadata; the driver commits the collected segments as a single logical index.
 *
 * @param addIndexExec       The AddIndexExec instance that initiated this job
 * @param readOptions        Configuration options for reading the Lance dataset
 * @param fragmentIds        List of fragment IDs to process
 * @param nsImpl             Optional namespace implementation class for credential vending
 * @param nsProps            Optional namespace properties for credential vending
 * @param tableId            Optional table identifier for credential vending
 * @param initialStorageOpts Optional initial storage options for the dataset
 */
class BTreeFragmentIndexJob(
    addIndexExec: AddIndexExec,
    readOptions: LanceSparkReadOptions,
    fragmentIds: List[Integer],
    nsImpl: Option[String],
    nsProps: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOpts: Option[Map[String, String]]) extends Serializable {

  def run(): Seq[Index] = {
    val encodedReadOptions = encode(readOptions)
    val columns = addIndexExec.columns.toList
    val argsJson = IndexUtils.toJson(addIndexExec.args)

    val tasks = fragmentIds.map { fid =>
      BTreeFragmentSegmentTask(
        encodedReadOptions,
        columns,
        addIndexExec.method,
        argsJson,
        fid,
        nsImpl,
        nsProps,
        tableId,
        initialStorageOpts)
    }

    addIndexExec.session.sparkContext
      .parallelize(tasks, tasks.size)
      .map(t => t.execute())
      .collect()
      .map(decode[Index])
      .toSeq
  }
}

/**
 * A task to build an uncommitted BTree index segment for a single fragment.
 *
 * @param encodedReadOptions    Configuration for Lance dataset access, serialized
 * @param columns               column names to index
 * @param method                Indexing method to use ("btree")
 * @param argsJson              JSON string containing index parameters
 * @param fragmentId            ID of the fragment to create the segment on
 * @param namespaceImpl         Implementation class for namespace operations
 * @param namespaceProperties   Properties of the namespace
 * @param tableId               Identifier for the table within the namespace
 * @param initialStorageOptions Initial storage configuration options
 */
case class BTreeFragmentSegmentTask(
    encodedReadOptions: String,
    columns: List[String],
    method: String,
    argsJson: String,
    fragmentId: Integer,
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]]) extends Serializable {

  def execute(): String = {
    val readOptions = decode[LanceSparkReadOptions](encodedReadOptions)
    val params = IndexParams.builder()
      .setScalarIndexParams(ScalarIndexParams.create(
        IndexUtils.buildScalarIndexParamType(method),
        argsJson))
      .build()

    // No index name or UUID: Lance generates the segment UUID, and the logical
    // index name is assigned when the segments are committed on the driver.
    val indexOptions = IndexOptions
      .builder(java.util.Arrays.asList(columns: _*), IndexType.BTREE, params)
      .replace(false)
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
      encode(dataset.createIndex(indexOptions))
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

    // Read the indexed column with the row id and fragment id metadata columns.
    val fragmentColumn = LanceDataset.FRAGMENT_ID_COLUMN.name
    val df = session.table(fullTableName)
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
    fragmentIds: List[Integer],
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
      fragmentIds,
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

    IndexUtils.createIndexSegment(
      readOptions,
      initialStorageOptions,
      namespaceImpl,
      namespaceProperties,
      tableId,
      indexOptions)
  }
}

/**
 * Utility methods for working with index types.
 */
object IndexUtils extends org.apache.spark.internal.Logging {

  private val jsonMapper = new ObjectMapper()

  private[datasources] val IvfIndexTypes: Set[IndexType] = Set(
    IndexType.IVF_FLAT,
    IndexType.IVF_PQ,
    IndexType.IVF_SQ,
    IndexType.IVF_HNSW_PQ,
    IndexType.IVF_HNSW_SQ)

  // SQ-quantized IVF variants. These are forced to a single segment build because lance-core
  // does not currently expose a driver-side ScalarQuantizer trainer; per-segment workers
  // would each train their own per-dimension SQ bounds against only that worker's fragments,
  // and `commit_existing_index_segments` does NOT reconcile bounds across segments — it
  // keeps the first shard's `ScalarQuantizationMetadata` (see lance Rust
  // `rust/lance-index/src/vector/distributed/index_merger.rs` IVF_SQ branch). Mixing
  // segments built with different bounds silently corrupts query distance computation.
  // Tracked as the SQ-shared-artifact follow-up; will be lifted once lance-core exposes
  // shared-bounds training similar to IVF centroids / PQ codebook.
  private[datasources] val SqIvfIndexTypes: Set[IndexType] = Set(
    IndexType.IVF_SQ,
    IndexType.IVF_HNSW_SQ)

  // Method → IndexType. Includes the six scalar-segment methods brought in by upstream #740
  // (zonemap / bitmap / label_list / ngram / bloomfilter / rtree) plus btree, fts, and the IVF
  // family. Keyed on lower-cased method names to normalise case-insensitive callers.
  private val methodToIndexTypes: Map[String, IndexType] = Map(
    "btree" -> IndexType.BTREE,
    "zonemap" -> IndexType.ZONEMAP,
    "bitmap" -> IndexType.BITMAP,
    "label_list" -> IndexType.LABEL_LIST,
    "ngram" -> IndexType.NGRAM,
    "bloomfilter" -> IndexType.BLOOM_FILTER,
    "rtree" -> IndexType.RTREE,
    "fts" -> IndexType.INVERTED,
    "ivf_flat" -> IndexType.IVF_FLAT,
    "ivf_pq" -> IndexType.IVF_PQ,
    "ivf_sq" -> IndexType.IVF_SQ,
    "ivf_hnsw_pq" -> IndexType.IVF_HNSW_PQ,
    "ivf_hnsw_sq" -> IndexType.IVF_HNSW_SQ)

  // Scalar-segment methods use the shared ScalarSegmentIndexJob path (uncommitted segments
  // built in parallel across fragment batches, then committed atomically on the driver).
  private val scalarSegmentIndexTypes: Set[IndexType] = Set(
    IndexType.ZONEMAP,
    IndexType.BITMAP,
    IndexType.LABEL_LIST,
    IndexType.NGRAM,
    IndexType.BLOOM_FILTER,
    IndexType.RTREE)

  // ScalarIndexParams uses Lance Core's scalar plugin names, which are a separate contract
  // from both Spark SQL method names and the Java IndexType enum names. Keep the mapping
  // explicit instead of deriving it from either naming convention.
  private val scalarParamTypesByIndexType: Map[IndexType, String] = Map(
    IndexType.BTREE -> "btree",
    IndexType.ZONEMAP -> "zonemap",
    IndexType.BITMAP -> "bitmap",
    IndexType.LABEL_LIST -> "labellist",
    IndexType.NGRAM -> "ngram",
    IndexType.BLOOM_FILTER -> "bloomfilter",
    IndexType.RTREE -> "rtree",
    IndexType.INVERTED -> "inverted")

  // All index types whose commit goes through the logical-segment path:
  // scalar-segment methods (zonemap / bitmap / label_list / ngram / bloomfilter / rtree)
  // plus the IVF_* vector family. Excludes BTree and FTS which use their own paths.
  private val logicalSegmentIndexTypes: Set[IndexType] =
    scalarSegmentIndexTypes ++ IvfIndexTypes

  def isIvfIndexType(indexType: IndexType): Boolean = IvfIndexTypes.contains(indexType)

  /**
   * True when `indexType` is an IVF variant whose quantizer (Scalar Quantizer) is currently
   * trained per-segment by lance-core. AddIndexExec downgrades these to a single-segment
   * build because lance-core's `commit_existing_index_segments` does not reconcile per-segment
   * SQ bounds across shards, which would otherwise silently corrupt query results. Tracked as
   * the SQ-shared-artifact follow-up against lance-format/lance.
   */
  def isSqIvfIndexType(indexType: IndexType): Boolean = SqIvfIndexTypes.contains(indexType)

  /**
   * Returns the [[IndexType]] for a scalar-segment method (zonemap / bitmap / label_list /
   * ngram / bloomfilter / rtree). Returns None for any other method — including BTree, FTS,
   * and IVF_*.
   */
  def scalarSegmentIndexType(method: String): Option[IndexType] =
    methodToIndexTypes
      .get(method.toLowerCase(Locale.ROOT))
      .filter(scalarSegmentIndexTypes.contains)

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
    normalized match {
      case "ivf_hnsw_flat" =>
        throw new UnsupportedOperationException(
          "IVF_HNSW_FLAT is not currently supported because Lance requires a PQ or SQ " +
            "quantizer for HNSW vector indexes. Use ivf_hnsw_pq or ivf_hnsw_sq instead.")
      case other =>
        methodToIndexTypes.getOrElse(
          other,
          throw new UnsupportedOperationException(s"Unsupported index method: $other"))
    }
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

  def useLogicalSegmentCommit(indexType: IndexType): Boolean =
    logicalSegmentIndexTypes.contains(indexType)

  def openDataset(
      readOptions: LanceSparkReadOptions,
      initialStorageOptions: Option[Map[String, String]],
      namespaceImpl: Option[String],
      namespaceProperties: Option[Map[String, String]],
      tableId: Option[List[String]]): Dataset = {
    Utils.openDatasetBuilder(readOptions)
      .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
      .runtimeNamespace(
        namespaceImpl.orNull,
        namespaceProperties.map(_.asJava).orNull,
        tableId.map(_.asJava).orNull)
      .build()
  }

  def createIndexSegment(
      readOptions: LanceSparkReadOptions,
      initialStorageOptions: Option[Map[String, String]],
      namespaceImpl: Option[String],
      namespaceProperties: Option[Map[String, String]],
      tableId: Option[List[String]],
      indexOptions: IndexOptions): String = {
    val dataset =
      openDataset(readOptions, initialStorageOptions, namespaceImpl, namespaceProperties, tableId)
    try {
      encode(dataset.createIndex(indexOptions))
    } finally {
      dataset.close()
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
        case e: Exception =>
          throw new RuntimeException(failureMessage, e)
      }
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

  /**
   * Split fragment ids into roughly equal batches for parallel index segment
   * builds. Used by the scalar-segment (zonemap / bitmap / label_list / ngram /
   * bloomfilter / rtree) and IVF_* logical-segment commit paths.
   *
   * @param fragmentIds       fragments to split (preserves input order)
   * @param numSegments       caller-supplied N; clamped to [1, fragmentIds.size]
   *                          and emits a warn when clamping happens
   * @param defaultParallelism fallback when numSegments is None
   * @return non-empty batches; sum of sizes equals fragmentIds.size
   */

  def batchFragments(
      fragmentIds: List[Integer],
      numSegments: Option[Int],
      defaultParallelism: Int): Seq[List[Integer]] = {
    val n = fragmentIds.size
    if (n == 0) return Seq.empty
    val k = numSegments match {
      case Some(req) =>
        val clamped = math.max(1, math.min(n, req))
        if (clamped != req) {
          logWarning(s"num_segments=$req clamped to $clamped (fragment count=$n)")
        }
        clamped
      case None =>
        math.max(1, math.min(n, defaultParallelism))
    }
    (0 until k).map { i =>
      fragmentIds.slice((i.toLong * n / k).toInt, ((i.toLong + 1) * n / k).toInt)
    }.filter(_.nonEmpty)
  }

}
