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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.util.LanceSerializeUtil.{decode, encode}
import org.lance.index.{DistanceType, Index, IndexOptions, IndexParams, IndexType}
import org.lance.index.vector.{HnswBuildParams, IvfBuildParams, PQBuildParams, SQBuildParams, VectorIndexParams, VectorTrainer}
import org.lance.spark.LanceSparkReadOptions

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Distributed builder for IVF_* family indices.
 *
 * Three phases:
 *   1. Driver: open dataset, train IVF centroids and (if needed) PQ codebook,
 *      broadcast both as flat float arrays.
 *   2. Executors: each Spark task takes a fragment batch, rebuilds
 *      IvfBuildParams.setCentroids(...) (and PQBuildParams.setCodebook(...) if
 *      this is a PQ variant), and calls dataset.createIndex(IndexOptions(...
 *      withFragmentIds(batch)). Returns an uncommitted Index segment.
 *   3. Driver: collects segments. The caller (AddIndexExec.run) is responsible
 *      for invoking commitExistingIndexSegments.
 *
 * Broadcasts are destroyed in a finally block whether the build succeeds or
 * fails so resources aren't leaked across SQL statements.
 */
class VectorIndexJob(
    addIndexExec: AddIndexExec,
    readOptions: LanceSparkReadOptions,
    fragmentIds: List[Integer],
    plan: VectorIndexPlan,
    indexName: String,
    columns: Seq[String],
    numSegments: Option[Int],
    nsImpl: Option[String],
    nsProps: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOpts: Option[Map[String, String]])
  extends Logging {

  /**
   * Returns the uncommitted Index segments. The caller commits them via
   * Dataset.commitExistingIndexSegments.
   *
   * NOTE: vector indices use the logical-segment commit path (like
   * ZonemapIndexJob) and return Seq[Index] instead of IndexBuildResult, so
   * VectorIndexJob deliberately does not extend the IndexJob trait.
   */
  def runSegments(): Seq[Index] = {
    val sc = addIndexExec.session.sparkContext
    val column = columns.head

    // ---- Phase 1: train + broadcast ----
    val (centroidsBC, codebookBC) = trainAndBroadcast(column, plan.distanceType)

    try {
      // ---- Phase 2: split fragments, parallelize tasks ----
      val batches =
        IndexUtils.batchFragments(fragmentIds, numSegments, sc.defaultParallelism)
      if (batches.isEmpty) {
        return Seq.empty
      }
      logInfo(
        s"VectorIndexJob phase 2: $indexName (${plan.indexType}) — ${batches.size} segment(s) " +
          s"covering ${fragmentIds.size} fragment(s) on ${batches.size} task(s)")

      val encodedReadOptions = encode(readOptions)
      val tasks = batches.map { batch =>
        VectorIndexTask(
          encodedReadOptions = encodedReadOptions,
          columns = columns.toList,
          indexType = plan.indexType,
          plan = plan,
          indexName = indexName,
          fragmentIds = batch,
          namespaceImpl = nsImpl,
          namespaceProperties = nsProps,
          tableId = tableId,
          initialStorageOptions = initialStorageOpts)
      }

      val localCentroidsBC = centroidsBC
      val localCodebookBC = codebookBC
      IndexUtils.runSegmentTasks(
        sc,
        tasks,
        "VectorIndexJob failed during distributed build " +
          "(uncommitted segments will be cleaned by vacuum)") { task =>
        task.execute(localCentroidsBC.value, localCodebookBC.value)
      }
    } finally {
      // Always destroy broadcasts so SQL doesn't accumulate broadcast memory
      destroyBroadcast("centroids", centroidsBC)
      destroyBroadcast("codebook", codebookBC)
    }
  }

  private def destroyBroadcast(name: String, broadcast: Broadcast[_]): Unit = {
    try {
      broadcast.destroy()
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        logWarning(s"Interrupted while destroying $name broadcast for vector index '$indexName'", e)
      case NonFatal(e) =>
        logWarning(s"Failed to destroy $name broadcast for vector index '$indexName'", e)
    }
  }

  // ----------------------------------------------------------------
  // Phase 1: driver-side training + broadcast
  // ----------------------------------------------------------------
  private def trainAndBroadcast(
      column: String,
      distanceType: DistanceType): (Broadcast[Array[Float]], Broadcast[Array[Float]]) = {
    val sc = addIndexExec.session.sparkContext

    val dataset =
      IndexUtils.openDataset(readOptions, initialStorageOpts, nsImpl, nsProps, tableId)
    try {
      logInfo(
        s"VectorIndexJob phase 1: training IVF centroids for '$indexName' " +
          s"(numPartitions=${plan.ivf.numPartitions}, distanceType=$distanceType)")
      val ivfTrainParams = new IvfBuildParams.Builder()
        .setNumPartitions(plan.ivf.numPartitions)
        .setSampleRate(plan.ivf.sampleRate)
        .setMaxIters(plan.ivf.maxIters)
        .build()
      val centroids: Array[Float] =
        VectorTrainer.trainIvfCentroids(dataset, column, ivfTrainParams, distanceType)
      val centroidsBC = sc.broadcast(centroids)

      // If codebook training (or its broadcast) throws, destroy centroidsBC here
      // since the caller never receives it and so cannot clean it up. Without
      // this guard, long-running SQL sessions accumulate orphan broadcasts.
      val codebookBC: Broadcast[Array[Float]] =
        try {
          plan.pq match {
            case Some(pqPlan) =>
              logInfo(
                s"VectorIndexJob phase 1: training PQ codebook for '$indexName' " +
                  s"(numSubVectors=${pqPlan.numSubVectors}, numBits=${pqPlan.numBits})")
              val pqTrainParams = new PQBuildParams.Builder()
                .setNumSubVectors(pqPlan.numSubVectors)
                .setNumBits(pqPlan.numBits)
                .setSampleRate(pqPlan.sampleRate)
                .setMaxIters(pqPlan.maxIters)
                .build()
              val codebook =
                VectorTrainer.trainPqCodebook(dataset, column, pqTrainParams, distanceType)
              sc.broadcast(codebook)
            case None =>
              // Empty array (not null) so Broadcast.value never returns null on executor.
              sc.broadcast(Array.empty[Float])
          }
        } catch {
          case t: Throwable =>
            destroyBroadcast("centroids", centroidsBC)
            throw t
        }

      (centroidsBC, codebookBC)
    } finally {
      dataset.close()
    }
  }
}

/**
 * Executor-side build of one segment.
 *
 * Closure invariants:
 *   - All fields are Serializable plain task metadata. Spark Broadcast handles
 *     are captured by the RDD closure and passed to execute as values.
 *   - We do NOT capture lance-core *BuildParams; they are rebuilt inside execute().
 */
case class VectorIndexTask(
    encodedReadOptions: String,
    columns: List[String],
    indexType: IndexType,
    plan: VectorIndexPlan,
    indexName: String,
    fragmentIds: List[Integer],
    namespaceImpl: Option[String],
    namespaceProperties: Option[Map[String, String]],
    tableId: Option[List[String]],
    initialStorageOptions: Option[Map[String, String]])
  extends Serializable {

  def execute(centroids: Array[Float], codebook: Array[Float]): String = {
    val readOptions = decode[LanceSparkReadOptions](encodedReadOptions)
    val distanceType = plan.distanceType

    // ---- rebuild BuildParams using broadcast artifacts ----
    val ivfParams = new IvfBuildParams.Builder()
      .setNumPartitions(plan.ivf.numPartitions)
      .setSampleRate(plan.ivf.sampleRate)
      .setMaxIters(plan.ivf.maxIters)
      .setCentroids(centroids)
      .build()

    val pqParams: Option[PQBuildParams] = plan.pq.map { p =>
      new PQBuildParams.Builder()
        .setNumSubVectors(p.numSubVectors)
        .setNumBits(p.numBits)
        .setSampleRate(p.sampleRate)
        .setMaxIters(p.maxIters)
        .setCodebook(codebook)
        .build()
    }

    val sqParams: Option[SQBuildParams] = plan.sq.map { s =>
      new SQBuildParams.Builder()
        .setNumBits(s.numBits.toShort)
        .setSampleRate(s.sampleRate)
        .build()
    }

    val hnswParams: Option[HnswBuildParams] = plan.hnsw.map { h =>
      val b = new HnswBuildParams.Builder()
        .setM(h.m)
        .setEfConstruction(h.efConstruction)
        .setMaxLevel(h.maxLevel.toShort)
      h.prefetchDistance.foreach(d => b.setPrefetchDistance(java.lang.Integer.valueOf(d)))
      b.build()
    }

    val builder = new VectorIndexParams.Builder(ivfParams).setDistanceType(distanceType)
    pqParams.foreach(builder.setPqParams)
    sqParams.foreach(builder.setSqParams)
    hnswParams.foreach(builder.setHnswParams)
    val vip = builder.build()

    val indexParams = IndexParams.builder().setVectorIndexParams(vip).build()
    // replace=true is required for IVF_*: per-task createIndex with withIndexName +
    // withFragmentIds consults the manifest for an existing same-named index and
    // rejects with "Index name X already exists" before commitExistingIndexSegments
    // can replace it. ZonemapIndexTask avoids this by omitting withIndexName, but
    // the IVF_* path sets it. Spec D9 documents replace=true as the default for
    // vector indices.
    val indexOptions = IndexOptions
      .builder(columns.asJava, indexType, indexParams)
      .replace(true)
      .withIndexName(indexName)
      .withFragmentIds(fragmentIds.asJava)
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
