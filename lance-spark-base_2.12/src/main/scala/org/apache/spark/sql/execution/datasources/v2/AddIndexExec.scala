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
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader, ArrowStreamWriter}
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
import org.lance.spark.utils.{CloseableUtil, Utils}
import org.lance.spark.write.SingleBatchArrowReader

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels
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
    // Parse the consolidate flag BEFORE opening the dataset handle. If the parse threw
    // (currently can't, but defense-in-depth against future evolution of getOption/trim
    // behaviour), opening the dataset first would leak the JNI handle past the unwind.
    // SparkConf-only flag for now. Default false preserves the multi-segment distributed
    // shape that has been the only path for the entire history of this code path; flipping
    // the default would silently change the commit footprint of every existing pipeline.
    // Lenient parse: `_.toBoolean` rejects "yes"/"on"/"1" with IllegalArgumentException —
    // surfacing a plan-time crash for a config typo is worse than degrading to the safe
    // default, since the failure mode obscures the only corrective action (fix the value).
    // Accept the JVM's canonical "true" (case-insensitive); everything else routes to the
    // distributed path.
    val consolidateZonemap =
      indexType == IndexType.ZONEMAP &&
        session.conf.getOption("spark.lance.zonemap.consolidate.enabled")
          .exists(v => v != null && "true".equalsIgnoreCase(v.trim))

    val dataset = Utils.openDatasetBuilder(readOptions).build()

    if (indexType == IndexType.ZONEMAP) {
      if (consolidateZonemap) {
        return runZonemapConsolidated(dataset, lanceDataset, readOptions, fragmentIds)
      }
      return runZonemapDistributed(dataset, lanceDataset, readOptions, fragmentIds)
    }

    val uuid = UUID.randomUUID()
    // Wrap createIndexJob.run() in the same try-finally that closes `dataset`. Previously
    // run() was called BEFORE the try block — a throw from the distributed build path would
    // leak the dataset handle and its underlying JNI resources.
    //
    // Use NonFatal (not bare Throwable) so JVM-fatal errors (OOM, LinkageError),
    // InterruptedException, and ControlThrowable propagate untouched. Closing JNI handles
    // during a fatal error risks executor hang or masks the original cause. Symmetric with
    // the catch ladder in `ZonemapFragmentTask.execute` below — both paths should treat
    // fatal errors the same way.
    val indexBuildResult =
      try {
        createIndexJob(dataset, lanceDataset, readOptions, uuid.toString, fragmentIds).run()
      } catch {
        case scala.util.control.NonFatal(e) =>
          dataset.close()
          throw e
      }

    try {
      // Merge index metadata after all fragments are indexed.
      dataset.mergeIndexMetadata(uuid.toString, indexType, Optional.empty())

      val fieldIds = resolveFieldIdsOrThrow(dataset, columns)

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

  /**
   * Distributed ZONEMAP build. Each task indexes one fragment with a fresh per-task UUID (no
   * `withIndexUUID`), so each writes to its own `&lt;uuid&gt;/zonemap.lance` directory. The driver
   * commits N IndexMetadata entries under a shared name in one transaction; lance-core's read
   * path serves multi-segment indexes natively.
   *
   * <p>A shared UUID would race on the fixed `zonemap.lance` filename — ZoneMap, unlike BTree, has
   * no per-task file-name namespacing or merge step.
   *
   * @param dataset      open Lance dataset; this method closes it
   * @param lanceDataset V2 catalog view, for namespace credentials
   * @param readOptions  read config forwarded to executor tasks
   * @param fragmentIds  fragments to index, one task each
   */
  private def runZonemapDistributed(
      dataset: Dataset,
      lanceDataset: LanceDataset,
      readOptions: LanceSparkReadOptions,
      fragmentIds: List[Integer]): Seq[InternalRow] = {
    try {
      // Validate columns up-front (fail-fast before parallelize) and reuse at commit time —
      // schema cannot drift within one dataset handle.
      val fieldIds = resolveFieldIdsOrThrow(dataset, columns)

      val (nsImpl, nsProps, tableId, initialStorageOpts) =
        extractNamespaceInfo(lanceDataset, readOptions)
      val encodedReadOptions = encode(readOptions)
      val argsJson = IndexUtils.toJson(args)

      // Capture the read-version up front. `dataset.version()` returns the manifest version
      // bound at handle-open time and is constant for the lifetime of this Dataset object —
      // so this call is semantically the same whether we make it before or after parallelize.
      // Capturing here is a readability improvement (commits the version to a local before
      // the long-running task phase) rather than a fix for any real race. Conflict detection
      // for concurrent writers happens at commit time via Transaction.readVersion() against
      // the manifest, which Lance handles independently of when this method reads version().
      val datasetVersion = dataset.version()

      val tasks = fragmentIds.map { fid =>
        ZonemapFragmentTask(
          encodedReadOptions,
          columns.toList,
          method,
          argsJson,
          indexName,
          fid,
          nsImpl,
          nsProps,
          tableId,
          initialStorageOpts)
      }

      // Defensive: Spark rejects parallelize(_, numSlices=0). Upstream `fragmentIds.isEmpty`
      // already short-circuits before reaching here, but pin the invariant locally so a future
      // refactor that moves the empty check won't silently break with a cryptic Spark error.
      if (tasks.isEmpty) {
        throw new IllegalStateException(
          "runZonemapDistributed called with empty fragmentIds — upstream empty-check should " +
            "have short-circuited")
      }

      val encodedResults: Array[String] = session.sparkContext
        .parallelize(tasks, tasks.size)
        .map(_.execute())
        .collect()
      val perFragment: Array[ZonemapFragmentResult] = encodedResults.zipWithIndex.map {
        case (encoded, idx) =>
          try {
            decode[ZonemapFragmentResult](encoded)
          } catch {
            case e: Exception =>
              // Use the actual fragment id, not just the array index, so an operator reading
              // the log can trace the failure to the specific input fragment without first
              // mapping idx → tasks(idx).fragmentId.
              val fragId = tasks(idx).fragmentId
              throw new IllegalStateException(
                s"Failed to decode ZONEMAP build result for fragment $fragId " +
                  s"(task index $idx): ${e.getMessage}",
                e)
          }
      }

      // One Index entry per fragment, all sharing the index name; the read path groups by name.
      val newIndexes = perFragment.toList.map { r =>
        val builder = Index.builder()
          .uuid(UUID.fromString(r.uuid))
          .name(indexName)
          .fields(fieldIds.map(java.lang.Integer.valueOf).asJava)
          .datasetVersion(datasetVersion)
          .indexDetails(r.indexDetails)
          .indexVersion(r.indexVersion)
          .indexType(IndexType.ZONEMAP)
          .fragments(Collections.singletonList(java.lang.Integer.valueOf(r.fragmentId)))
        r.createdAt.foreach(builder.createdAt)
        builder.build()
      }.asJava

      val removedIndices = dataset.getIndexes.asScala
        .filter(_.name() == indexName)
        .toList.asJava

      val op = AddIndexOperation.builder()
        .withNewIndices(newIndexes)
        .withRemovedIndices(removedIndices)
        .build()
      val txn = new Transaction.Builder()
        .readVersion(datasetVersion)
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

  /**
   * Consolidated ZONEMAP build. Each executor task calls `computeZonemapBatch` on its assigned
   * fragment and returns an Arrow-IPC-encoded byte payload; the driver decodes all payloads and
   * calls `writeZonemapIndexFromBatches` once, producing a single `&lt;uuid&gt;/zonemap.lance`
   * segment covering the union of fragments. One IndexMetadata entry is committed under the
   * index name.
   *
   * <p>Trade-off vs. {@link runZonemapDistributed}: write I/O is centralized on the driver
   * (one consolidated file) instead of N parallel per-fragment files. Compute (the per-zone
   * min/max scan) stays distributed. Net effect: fewer manifest entries + lower segment-merge
   * cost at read time, at three costs the distributed path does not pay:
   * <ul>
   *   <li>Per-fragment Arrow IPC payloads traverse Spark's task-result/network path back to
   *       the driver via `.collect()`. The distributed path writes in-place on executors —
   *       consolidated adds serialization + transfer bandwidth proportional to N × per-zone
   *       payload size.
   *   <li>Driver heap and allocator pressure: every per-fragment {@code VectorSchemaRoot}
   *       coexists in the driver's {@code RootAllocator} from decode time until
   *       {@code writeZonemapIndexFromBatches} consumes them — footprint scales linearly with
   *       fragment count.
   *   <li>Driver-side write I/O for the single consolidated file.
   * </ul>
   * The kill-switch defaults consolidated to OFF precisely because the per-fragment cost
   * scales linearly with N; very-high-fragment datasets (sf=10000+) may regress wall-clock
   * even though manifest entry count + on-disk size both improve dramatically.
   *
   * @param dataset      open Lance dataset; this method closes it
   * @param lanceDataset V2 catalog view, for namespace credentials
   * @param readOptions  read config forwarded to executor tasks
   * @param fragmentIds  fragments to index, one task each
   */
  private def runZonemapConsolidated(
      dataset: Dataset,
      lanceDataset: LanceDataset,
      readOptions: LanceSparkReadOptions,
      fragmentIds: List[Integer]): Seq[InternalRow] = {
    // Operator-facing INFO line mirrors the pattern LanceScanBuilder uses when its CBO
    // fast-path engages. Lets operators confirm from the driver log which build path ran
    // (consolidated vs distributed) — without this, the only post-hoc signal is the manifest
    // entry count, which requires re-opening the dataset.
    logInfo(
      s"Using consolidated ZONEMAP build path for index '$indexName' on column " +
        s"'${columns.head}' over ${fragmentIds.size} fragments")
    try {
      val fieldIds = resolveFieldIdsOrThrow(dataset, columns)
      // ZONEMAP indexes exactly one column. The grammar enforces single-column at parse time;
      // assert here to make the invariant explicit at the consumer-side boundary.
      if (columns.size != 1) {
        throw new IllegalArgumentException(
          s"ZONEMAP index requires exactly one column; got ${columns.size}: $columns")
      }
      val columnName = columns.head

      val (nsImpl, nsProps, tableId, initialStorageOpts) =
        extractNamespaceInfo(lanceDataset, readOptions)
      val encodedReadOptions = encode(readOptions)
      val argsJson = IndexUtils.toJson(args)
      val datasetVersion = dataset.version()

      val tasks = fragmentIds.map { fid =>
        ZonemapConsolidatedFragmentTask(
          encodedReadOptions,
          columnName,
          argsJson,
          fid,
          nsImpl,
          nsProps,
          tableId,
          initialStorageOpts)
      }

      if (tasks.isEmpty) {
        throw new IllegalStateException(
          "runZonemapConsolidated called with empty fragmentIds — upstream empty-check should " +
            "have short-circuited")
      }

      val perFragmentBytes: Array[Array[Byte]] = session.sparkContext
        .parallelize(tasks, tasks.size)
        .map(_.execute())
        .collect()

      // Driver-side: decode Arrow IPC bytes back to VectorSchemaRoots, then write one
      // consolidated zonemap.lance. Each reader owns the VSR it produces, so closing the
      // reader (in finally) closes the VSR — we must not double-close.
      val driverAllocator = new RootAllocator()
      val readers = scala.collection.mutable.ListBuffer.empty[ArrowStreamReader]
      try {
        val decodedBatches = perFragmentBytes.zipWithIndex.map { case (bytes, idx) =>
          // Defensive: ArrowStreamReader.loadNextBatch() throws EOFException/IOException on
          // a zero-byte payload, which would surface as a stack trace without fragment-id
          // context. Pre-check produces the same diagnostic shape as the "no batch" path
          // below, keeping operator log analysis uniform.
          if (bytes.length == 0) {
            val fragId = tasks(idx).fragmentId
            throw new IllegalStateException(
              s"Empty Arrow IPC payload for fragment $fragId — executor returned a " +
                "zero-byte stream")
          }
          // The ArrowStreamReader constructor with a byte-array-backed channel does no I/O
          // and cannot throw on these inputs in Arrow Java 14–18 (the range covered by this
          // project's build matrix as of 2026). If a future Arrow upgrade changes that, the
          // constructor must be wrapped in try-catch and the partially-constructed reader
          // tracked, or a leak escapes between construction and the `readers += reader`
          // line below. Revisit this invariant on every Arrow major version bump.
          val reader = new ArrowStreamReader(
            new ByteArrayInputStream(bytes),
            driverAllocator)
          // Ordering invariant: tracking the reader in `readers` MUST precede `loadNextBatch()`.
          // A throw or false return from loadNextBatch leaves the reader's VSR in a partially-
          // loaded state that still owns allocator buffers; tracking the reader before the load
          // ensures the outer finally closes it and reclaims those buffers. A future refactor
          // that moves this insertion below the load check would leak on every failed batch.
          readers += reader
          val fragId = tasks(idx).fragmentId
          val hasBatch =
            try {
              reader.loadNextBatch()
            } catch {
              // Truncated / corrupted IPC streams surface as EOFException / IOException /
              // IllegalArgumentException from Arrow Java. Without this wrapper the stack trace
              // would carry no fragment-id context, making operator log analysis depend on
              // mapping the array index back to the task list. Mirror the diagnostic shape used
              // by the distributed path's per-fragment decode wrapper.
              case scala.util.control.NonFatal(e) =>
                throw new IllegalStateException(
                  s"Failed to decode Arrow IPC payload for fragment $fragId " +
                    s"(${e.getClass.getSimpleName}): ${e.getMessage}",
                  e)
            }
          if (!hasBatch) {
            throw new IllegalStateException(
              s"ArrowStreamReader returned no batch for fragment $fragId — executor produced " +
                "an empty zone-record stream")
          }
          reader.getVectorSchemaRoot
        }.toList

        val writtenIndex = dataset.writeZonemapIndexFromBatches(
          indexName,
          columnName,
          decodedBatches.asJava,
          argsJson,
          driverAllocator)

        // writeZonemapIndexFromBatches returns an uncommitted Index with uuid + indexType +
        // indexVersion + indexDetails populated from the file write. We rebuild on top with
        // name/fields/datasetVersion/fragments so the manifest entry carries the catalog-level
        // metadata the read path expects, mirroring runZonemapDistributed's Index.builder
        // pattern. The full fragment list is what the driver commissioned (not what the
        // returned Index's fragment_bitmap might compute from the batches) — a single source
        // of truth keeps the manifest consistent with the dispatcher's view.
        //
        // Pre-commit validation: lance-core computes the file's internal fragment_bitmap from
        // the actual batches it received. If `writeZonemapIndexFromBatches` silently elided a
        // batch (empty/all-null skip path, future evolution), the manifest entry built below
        // would CLAIM coverage the on-disk file does not have. Catch the divergence here and
        // fail the build BEFORE commit — converting a silent manifest lie into a fail-fast
        // error. We only validate when the returned Index actually has a fragment list (some
        // lance-core versions may leave it absent for uncommitted writes); absence is treated
        // as "no claim to check against" rather than a hard error.
        if (writtenIndex.fragments().isPresent) {
          val writtenFragments = writtenIndex.fragments().get().asScala.toSet
          val commissionedFragments = fragmentIds.toSet
          if (writtenFragments != commissionedFragments) {
            // Split the diff so operators reading a 5000-fragment build log can see at a
            // glance which direction the divergence went, instead of mentally diffing two
            // large sets in the message body.
            val missingInFile = commissionedFragments -- writtenFragments
            val extraInFile = writtenFragments -- commissionedFragments
            throw new IllegalStateException(
              s"Consolidated zonemap.lance file's fragment_bitmap diverges from the " +
                s"dispatched fragment set for $indexName. " +
                s"missing_in_file=$missingInFile, extra_in_file=$extraInFile " +
                s"(dispatched=$commissionedFragments, file=$writtenFragments) — committing " +
                "would record a manifest claim the file cannot back up. Failing the build " +
                "to keep the manifest honest.")
          }
        }
        val details = writtenIndex.indexDetails()
        if (!details.isPresent || details.get().length == 0) {
          throw new IllegalStateException(
            s"Consolidated zonemap write returned an Index without indexDetails for $indexName")
        }
        val builder = Index.builder()
          .uuid(writtenIndex.uuid())
          .name(indexName)
          .indexType(IndexType.ZONEMAP)
          .indexVersion(writtenIndex.indexVersion())
          .indexDetails(details.get())
          .fields(fieldIds.map(java.lang.Integer.valueOf).asJava)
          .fragments(fragmentIds.asJava)
          .datasetVersion(datasetVersion)
        if (writtenIndex.createdAt().isPresent) {
          builder.createdAt(writtenIndex.createdAt().get())
        }
        val finalIndex = builder.build()

        val removedIndices = dataset.getIndexes.asScala
          .filter(_.name() == indexName)
          .toList.asJava

        val op = AddIndexOperation.builder()
          .withNewIndices(Collections.singletonList(finalIndex))
          .withRemovedIndices(removedIndices)
          .build()
        val txn = new Transaction.Builder()
          .readVersion(datasetVersion)
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
        // Closing each reader closes its owned VectorSchemaRoot; allocator close validates
        // there are no live buffers.
        //
        // A silently-swallowed reader-close failure (e.g. an Arrow internal release error)
        // would surface only later as `driverAllocator.close()` throwing "Memory was leaked",
        // with no hint that a reader-close was the actual root cause. Log every close
        // exception with index context so operators can correlate. We do NOT rethrow — the
        // cleanup must run to completion across all readers regardless of individual
        // failures, and the allocator-close at the end will still surface the first
        // observable leak as its own exception.
        readers.zipWithIndex.foreach { case (r, idx) =>
          try r.close()
          catch {
            case scala.util.control.NonFatal(e) =>
              // Include fragment id to match the diagnostic shape of the decode-error path
              // above. `readers.length <= tasks.length` always — readers is populated only
              // after the bytes pre-check passes, and tracking precedes the load — so
              // `tasks(idx)` is always a valid lookup. If decode threw mid-loop, the
              // operator's log timeline already has the decode error tagged with the same
              // fragment id, making cross-correlation immediate.
              val fragId = tasks(idx).fragmentId
              logWarning(
                s"ArrowStreamReader close failed for consolidated zonemap fragment " +
                  s"$fragId (batch index $idx, ${e.getClass.getSimpleName}): " +
                  s"${e.getMessage}",
                e)
          }
        }
        driverAllocator.close()
      }
    } finally {
      dataset.close()
    }
    Seq(new GenericInternalRow(Array[Any](
      fragmentIds.size.toLong,
      UTF8String.fromString(indexName))))
  }

  /** Extract namespace credentials info from the catalog, mirroring `createIndexJob`. */
  private def extractNamespaceInfo(
      lanceDataset: LanceDataset,
      readOptions: LanceSparkReadOptions): (
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

  /**
   * Resolve column names to Lance field IDs. Throws IllegalArgumentException if any column is
   * absent; shared by both build paths so failure modes stay uniform.
   */
  private def resolveFieldIdsOrThrow(
      dataset: Dataset,
      columnsToResolve: Seq[String]): List[Int] = {
    val fieldIdByName = dataset.getLanceSchema.fields().asScala
      .map(f => f.getName -> f.getId)
      .toMap
    columnsToResolve.map { column =>
      fieldIdByName.getOrElse(
        column,
        throw new IllegalArgumentException(s"Cannot find index column in Lance schema: $column"))
    }.toList
  }

  private def createIndexJob(
      dataset: Dataset,
      lanceDataset: LanceDataset,
      readOptions: LanceSparkReadOptions,
      uuid: String,
      fragmentIds: List[Integer]): IndexJob = {
    // Get namespace info from catalog if available (for credential vending on workers)
    val (nsImpl, nsProps, tableId, initialStorageOpts) =
      extractNamespaceInfo(lanceDataset, readOptions)

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

/** Per-fragment build metadata returned by {@link ZonemapFragmentTask}. */
case class ZonemapFragmentResult(
    uuid: String,
    fragmentId: Int,
    indexDetails: Array[Byte],
    indexVersion: Int,
    createdAt: Option[Instant])
  extends Serializable

/**
 * Per-fragment ZONEMAP build task. Calls `dataset.createIndex` with `withFragmentIds=[id]` and no
 * `withIndexUUID`, so lance-core takes the uncommitted path with a fresh per-task UUID. The
 * returned metadata is collected by the driver into a multi-segment commit.
 */
case class ZonemapFragmentTask(
    encodedReadOptions: String,
    columns: List[String],
    method: String,
    argsJson: String,
    indexName: String,
    fragmentId: Int,
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]])
  extends Serializable {

  def execute(): String = {
    try {
      buildAndEncode()
    } catch {
      // Preserve the broad exception class (IAE vs ISE vs other) so callers matching on
      // type still match — only the message gains fragment-id context. Use
      // scala.util.control.NonFatal as the outer guard: it excludes VirtualMachineError
      // (OOM), ThreadDeath, InterruptedException, LinkageError, and ControlThrowable.
      // Letting those propagate untouched is the right policy:
      //   - JVM-fatal errors must crash the executor immediately rather than being wrapped
      //     into a recoverable-looking RuntimeException (the executor JVM is in undefined
      //     state after such an error).
      //   - InterruptedException is Spark's task-cancellation signal; wrapping it as
      //     RuntimeException would swallow the interrupt status and break cancellation.
      //   - LinkageError typically indicates an incompatible JNI/jar version; surfacing it
      //     as itself preserves the diagnostic.
      //
      // Note on subclass narrowing: a thrown `NumberFormatException` (which extends
      // IllegalArgumentException) is re-thrown as a plain IllegalArgumentException with the
      // NumberFormatException as the wrapped cause. Callers matching on the LEAF subclass
      // (`case e: NumberFormatException`) will no longer match, but the cause chain
      // (getCause / instanceof) preserves the original type. Trade-off accepted: the
      // fragment-id context is more valuable for log analysis than preserving the leaf
      // subclass identity through the rethrow.
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"ZONEMAP build failed for fragment $fragmentId: ${e.getMessage}",
          e)
      case e: IllegalStateException =>
        throw new IllegalStateException(
          s"ZONEMAP build failed for fragment $fragmentId: ${e.getMessage}",
          e)
      case scala.util.control.NonFatal(e) =>
        // Catches any other non-fatal exception (lance-core errors, IOException, custom
        // RuntimeException subclasses) and tags with fragment-id context. Re-thrown as
        // RuntimeException so Spark's TaskFailedReason serialises it cleanly back to the
        // driver, where the per-task index decoder will surface the wrapped cause.
        throw new RuntimeException(
          s"ZONEMAP build failed for fragment $fragmentId (${e.getClass.getSimpleName}): " +
            s"${e.getMessage}",
          e)
    }
  }

  private def buildAndEncode(): String = {
    val readOptions = decode[LanceSparkReadOptions](encodedReadOptions)
    val params = IndexParams.builder()
      .setScalarIndexParams(ScalarIndexParams.create(
        IndexUtils.buildScalarIndexParamType(method),
        argsJson))
      .build()

    val indexOptions = IndexOptions
      .builder(java.util.Arrays.asList(columns: _*), IndexType.ZONEMAP, params)
      .replace(true)
      .withIndexName(indexName)
      .withFragmentIds(Collections.singletonList(java.lang.Integer.valueOf(fragmentId)))
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
      val details = createdIndex.indexDetails()
      if (!details.isPresent || details.get().length == 0) {
        throw new IllegalStateException(
          s"Index ${createdIndex.name()} was created without index details")
      }
      val createdAt = if (createdIndex.createdAt().isPresent) {
        Some(createdIndex.createdAt().get())
      } else {
        None
      }
      encode(ZonemapFragmentResult(
        uuid = createdIndex.uuid().toString,
        fragmentId = fragmentId,
        indexDetails = details.get(),
        indexVersion = createdIndex.indexVersion(),
        createdAt = createdAt))
    } finally {
      dataset.close()
    }
  }
}

/**
 * Per-fragment task for the consolidated ZONEMAP build. Computes one Arrow batch of per-zone
 * stats via {@link Dataset#computeZonemapBatch} and returns it Arrow-IPC-encoded so it survives
 * the Spark task-result serialization round-trip back to the driver. No file is written here;
 * the driver merges every task's batch into one consolidated `zonemap.lance` file.
 */
case class ZonemapConsolidatedFragmentTask(
    encodedReadOptions: String,
    columnName: String,
    argsJson: String,
    fragmentId: Int,
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]])
  extends Serializable {

  def execute(): Array[Byte] = {
    try {
      computeAndEncode()
    } catch {
      // Mirror ZonemapFragmentTask.execute()'s catch ladder for diagnostic uniformity:
      // IAE/ISE preserve their type with fragment-id context; everything else NonFatal lands
      // as a RuntimeException carrying the original cause; VM-fatal and InterruptedException
      // propagate untouched.
      //
      // Subclass-narrowing trade-off (mirroring the R13 note on the distributed task):
      // a thrown NumberFormatException (extends IAE) is re-thrown as plain IAE with the
      // NumberFormatException as wrapped cause. Callers matching on the LEAF subclass
      // (case e: NumberFormatException) will no longer match, but the cause chain
      // (getCause / instanceof) preserves the original type. Trade-off accepted: the
      // fragment-id context is more valuable for log analysis than preserving the leaf
      // subclass identity through the rethrow.
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"ZONEMAP consolidated build failed for fragment $fragmentId: ${e.getMessage}",
          e)
      case e: IllegalStateException =>
        throw new IllegalStateException(
          s"ZONEMAP consolidated build failed for fragment $fragmentId: ${e.getMessage}",
          e)
      case scala.util.control.NonFatal(e) =>
        throw new RuntimeException(
          s"ZONEMAP consolidated build failed for fragment $fragmentId " +
            s"(${e.getClass.getSimpleName}): ${e.getMessage}",
          e)
    }
  }

  private def computeAndEncode(): Array[Byte] = {
    val readOptions = decode[LanceSparkReadOptions](encodedReadOptions)
    val allocator = new RootAllocator()
    try {
      val dataset = Utils.openDatasetBuilder(readOptions)
        .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
        .runtimeNamespace(
          namespaceImpl.orNull,
          namespaceProperties.map(_.asJava).orNull,
          tableId.map(_.asJava).orNull)
        .build()
      try {
        // computeZonemapBatch rejects an empty long[]; passing a one-element array is the
        // explicit "this fragment only" signal in the API contract.
        val vsr = dataset.computeZonemapBatch(
          columnName,
          Array(fragmentId.toLong),
          argsJson,
          allocator)
        try {
          val baos = new ByteArrayOutputStream()
          val writer = new ArrowStreamWriter(vsr, null, Channels.newChannel(baos))
          try {
            writer.start()
            writer.writeBatch()
            writer.end()
          } finally {
            writer.close()
          }
          baos.toByteArray
        } finally {
          vsr.close()
        }
      } finally {
        dataset.close()
      }
    } finally {
      allocator.close()
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
 * Utility methods for working with index types.
 */
object IndexUtils {

  private val jsonMapper = new ObjectMapper()

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
      case "zonemap" => IndexType.ZONEMAP
      case other => throw new UnsupportedOperationException(s"Unsupported index method: $other")
    }
  }

  def buildScalarIndexParamType(method: String): String = {
    method.toLowerCase match {
      case "btree" => "btree"
      case "fts" => "inverted"
      case "zonemap" => "zonemap"
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
