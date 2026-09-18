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
package org.lance.spark.knn.internal

import org.lance.{Dataset, ReadOptions}
import org.lance.index.{IndexOptions, IndexParams, IndexType}
import org.lance.index.vector.VectorIndexParams
import org.lance.spark.LanceRuntime

import scala.collection.JavaConverters._

/**
 * Test-only helper to build an IVF-PQ vector index on a Lance dataset via
 * `Dataset.createIndex`. Exists so recall tests can construct the indexed scan path
 * without writing the Lance Java boilerplate inline.
 *
 * Lives in `src/test/scala` because the production code path doesn't need to build
 * indexes — users build them via Lance's Python / Rust / SQL DDL on their own datasets,
 * and we just probe whatever's there. The helper exists for closed-loop recall validation.
 */
object LanceVectorIndexBuilder {

  /**
   * Build an IVF-PQ index on `vectorColumn` of the dataset at `datasetUri`. Defaults are
   * tuned for tiny test datasets — production users would size these much larger.
   *
   * @param numPartitions IVF cluster count. Should divide cleanly into the dataset row count.
   *                      For a 4K-row dataset, 4-8 partitions is reasonable.
   * @param numSubVectors PQ sub-vector count. Must divide vector dim evenly.
   * @param numBits       PQ bits per sub-vector. 8 is the standard.
   * @param metric        distance type. Must match the metric used at probe time.
   * @param maxIters      KMeans iteration cap during IVF training. 50 is enough for tests.
   */
  def buildIvfPq(
      datasetUri: String,
      vectorColumn: String,
      numPartitions: Int = 4,
      numSubVectors: Int = 8,
      numBits: Int = 8,
      metric: Metric = Metric.L2,
      maxIters: Int = 50): Unit = {
    val dataset = openDataset(datasetUri)
    try {
      // Arg order in lance-core is (numPartitions, numBits, numSubVectors, distanceType, maxIters)
      // — numBits precedes numSubVectors. Both default to 8 here so a swap is silent; pin the
      // documented order explicitly.
      val vectorParams =
        VectorIndexParams.ivfPq(numPartitions, numBits, numSubVectors, metric.lanceType, maxIters)
      val indexParams = IndexParams.builder().setVectorIndexParams(vectorParams).build()
      val opts = IndexOptions
        .builder(java.util.Collections.singletonList(vectorColumn), IndexType.VECTOR, indexParams)
        .build()
      dataset.createIndex(opts)
    } finally dataset.close()
  }

  /**
   * Build an IVF_FLAT index — IVF clustering without PQ compression. Exact distances within
   * visited clusters (no PQ noise), so recall depends purely on `nprobes` coverage. Higher
   * memory/disk footprint than IVF-PQ (full vectors stored per cluster) but better recall on
   * high-dim or random workloads where PQ compression drops too much information.
   */
  def buildIvfFlat(
      datasetUri: String,
      vectorColumn: String,
      numPartitions: Int = 4,
      metric: Metric = Metric.L2): Unit = {
    val dataset = openDataset(datasetUri)
    try {
      val vectorParams = VectorIndexParams.ivfFlat(numPartitions, metric.lanceType)
      val indexParams = IndexParams.builder().setVectorIndexParams(vectorParams).build()
      val opts = IndexOptions
        .builder(java.util.Collections.singletonList(vectorColumn), IndexType.VECTOR, indexParams)
        .build()
      dataset.createIndex(opts)
    } finally dataset.close()
  }

  private def openDataset(uri: String): Dataset = {
    Dataset
      .open()
      .uri(uri)
      .allocator(LanceRuntime.allocator())
      .readOptions(new ReadOptions.Builder().build())
      .build()
  }

  /** Number of indexes on the dataset (sanity check after building). */
  def listIndexCount(datasetUri: String): Int = {
    val dataset = openDataset(datasetUri)
    try dataset.listIndexes.asScala.size
    finally dataset.close()
  }
}
