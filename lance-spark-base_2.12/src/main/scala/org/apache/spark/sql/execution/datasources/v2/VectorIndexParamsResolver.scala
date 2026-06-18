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

import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.sql.catalyst.plans.logical.LanceNamedArgument
import org.lance.index.IndexType
import org.lance.spark.LanceSparkReadOptions
import org.lance.spark.utils.{Utils, VectorUtils}

import scala.collection.JavaConverters._

/**
 * Parses and validates the WITH(...) named arguments attached to
 * `ALTER TABLE … CREATE INDEX … USING <IVF_*> ( col )` and infers defaults.
 *
 * The dataset-touching wrapper [[resolve]] (added in a later task) opens the
 * lance Dataset on the driver to read the column dimension and total row count,
 * then delegates to [[parseAndValidate]] which is pure and unit-tested.
 */
object VectorIndexParamsResolver {

  // ---- key whitelists ----
  // "mode" is consumed earlier in AddIndexExec.run (incremental dispatch).
  // Listing it here lets users pass mode='replace' / mode='incremental'
  // explicitly without the resolver rejecting it as an unknown parameter;
  // the resolver itself ignores the value.
  private val CommonKeys =
    Set("num_partitions", "distance_type", "sample_rate", "max_iters", "num_segments", "mode")
  private val PqKeys = Set("num_sub_vectors", "num_bits")
  private val SqKeys = Set("num_bits")
  private val HnswKeys = Set("m", "ef_construction", "max_level", "prefetch_distance")

  private val PqIndexTypes =
    Set(IndexType.IVF_PQ, IndexType.IVF_HNSW_PQ)
  private val SqIndexTypes =
    Set(IndexType.IVF_SQ, IndexType.IVF_HNSW_SQ)
  private val HnswIndexTypes =
    Set(IndexType.IVF_HNSW_SQ, IndexType.IVF_HNSW_PQ)

  /** Pure validation — no Dataset, no Spark. Tested by VectorIndexParamsResolverTest. */
  def parseAndValidate(
      indexType: IndexType,
      args: Seq[LanceNamedArgument],
      dim: Int,
      numRows: Long): VectorIndexPlan = {

    require(dim > 0, s"vector dimension must be positive (got $dim)")
    require(numRows >= 0, s"numRows must be non-negative (got $numRows)")

    // 1. Fold args, error on duplicate
    val map = scala.collection.mutable.LinkedHashMap[String, Any]()
    args.foreach { a =>
      if (map.contains(a.name)) {
        throw new IllegalArgumentException(
          s"Duplicate parameter '${a.name}' in WITH clause")
      }
      map.put(a.name, a.value)
    }

    // 2. Whitelist by index type
    val allowed =
      CommonKeys ++
        (if (PqIndexTypes.contains(indexType)) PqKeys else Set.empty) ++
        (if (SqIndexTypes.contains(indexType)) SqKeys else Set.empty) ++
        (if (HnswIndexTypes.contains(indexType)) HnswKeys else Set.empty)

    map.keys.foreach { k =>
      if (!allowed.contains(k)) {
        if (k == "num_sub_vectors" && !PqIndexTypes.contains(indexType)) {
          throw new IllegalArgumentException(
            s"num_sub_vectors is only supported for IVF_PQ / IVF_HNSW_PQ, not $indexType")
        }
        if (k == "num_bits"
          && !PqIndexTypes.contains(indexType)
          && !SqIndexTypes.contains(indexType)) {
          throw new IllegalArgumentException(
            s"num_bits is only supported for IVF_PQ / IVF_SQ / IVF_HNSW_PQ / IVF_HNSW_SQ, " +
              s"not $indexType")
        }
        if (HnswKeys.contains(k) && !HnswIndexTypes.contains(indexType)) {
          throw new IllegalArgumentException(
            s"$k is only supported for IVF_HNSW_* index types, not $indexType")
        }
        throw new IllegalArgumentException(
          s"$k is not a recognized parameter for $indexType. " +
            s"Allowed: ${allowed.toSeq.sorted.mkString(", ")}")
      }
    }

    // 3. Common: distance_type
    val distanceTypeName = parseDistanceTypeName(map.get("distance_type"))

    // 4. Common: sampleRate, maxIters, numPartitions
    val sampleRate = parseInt(map.get("sample_rate"), "sample_rate", default = 256)
    if (sampleRate < 2) {
      throw new IllegalArgumentException(s"sample_rate must be >= 2 (got $sampleRate)")
    }
    val maxIters = parseInt(map.get("max_iters"), "max_iters", default = 50)
    if (maxIters <= 0) {
      throw new IllegalArgumentException(
        s"max_iters must be a positive integer (got $maxIters)")
    }

    val numPartitionsOpt = parseIntOpt(map.get("num_partitions"), "num_partitions")
    val numPartitions = numPartitionsOpt.getOrElse {
      math.max(1, math.round(math.sqrt(numRows.toDouble)).toInt)
    }
    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
        s"num_partitions must be a positive integer (got $numPartitions)")
    }
    if (numRows > 0L && numPartitions > numRows) {
      throw new IllegalArgumentException(
        s"num_partitions ($numPartitions) cannot exceed total rows ($numRows)")
    }

    val ivf = IvfPlan(numPartitions = numPartitions, sampleRate = sampleRate, maxIters = maxIters)

    // 5. PQ
    val pq = if (PqIndexTypes.contains(indexType)) {
      val nsv = parseIntOpt(map.get("num_sub_vectors"), "num_sub_vectors") match {
        case Some(v) =>
          if (v < 1) throw new IllegalArgumentException(
            s"num_sub_vectors must be >= 1 (got $v)")
          if (v > dim) throw new IllegalArgumentException(
            s"num_sub_vectors ($v) cannot exceed vector dimension ($dim)")
          if (dim % v != 0) throw new IllegalArgumentException(
            s"$indexType requires num_sub_vectors to divide vector dimension $dim, " +
              s"but $v does not")
          v
        case None =>
          if (dim % 16 == 0) dim / 16
          else if (dim % 8 == 0) dim / 8
          else throw new IllegalArgumentException(
            s"vector dimension $dim is not divisible by 16 or 8; " +
              "please specify num_sub_vectors explicitly")
      }
      val numBits = parseInt(map.get("num_bits"), "num_bits", default = 8)
      if (numBits <= 0) throw new IllegalArgumentException(
        s"num_bits must be positive (got $numBits)")
      Some(PqPlan(
        numSubVectors = nsv,
        numBits = numBits,
        sampleRate = sampleRate,
        maxIters = maxIters))
    } else None

    // 6. SQ
    val sq = if (SqIndexTypes.contains(indexType)) {
      val numBits = parseInt(map.get("num_bits"), "num_bits", default = 8)
      if (numBits <= 0) throw new IllegalArgumentException(
        s"num_bits must be positive (got $numBits)")
      Some(SqPlan(numBits = numBits, sampleRate = sampleRate))
    } else None

    // 7. HNSW
    val hnsw = if (HnswIndexTypes.contains(indexType)) {
      val m = parseInt(map.get("m"), "m", default = 20)
      if (m <= 0) throw new IllegalArgumentException(s"m must be positive (got $m)")
      val ef = parseInt(map.get("ef_construction"), "ef_construction", default = 150)
      if (ef <= 0) throw new IllegalArgumentException(
        s"ef_construction must be positive (got $ef)")
      val maxLevel = parseInt(map.get("max_level"), "max_level", default = 7)
      if (maxLevel <= 0) throw new IllegalArgumentException(
        s"max_level must be positive (got $maxLevel)")
      val prefetch = parseIntOpt(map.get("prefetch_distance"), "prefetch_distance")
        .orElse(Some(2))
      prefetch.foreach { p =>
        if (p < 0) throw new IllegalArgumentException(
          s"prefetch_distance must be non-negative (got $p)")
      }
      Some(HnswPlan(m, ef, maxLevel, prefetch))
    } else None

    VectorIndexPlan(indexType, distanceTypeName, ivf, pq, sq, hnsw)
  }

  // ---------- helpers ----------

  private def parseDistanceTypeName(raw: Option[Any]): String = raw match {
    case None => "L2"
    case Some(s: String) =>
      s.trim.toLowerCase match {
        case "l2" => "L2"
        case "euclidean" => "L2"
        case "cosine" => "Cosine"
        case "dot" => "Dot"
        case "hamming" => "Hamming"
        case other =>
          throw new IllegalArgumentException(
            s"distance_type '$other' not supported. " +
              "Valid: l2, cosine, dot, hamming (alias: euclidean→l2)")
      }
    case Some(other) =>
      throw new IllegalArgumentException(
        s"distance_type must be a string, got: $other")
  }

  private def parseIntOpt(raw: Option[Any], key: String): Option[Int] = raw match {
    case None => None
    case Some(n: java.lang.Number) =>
      val asLong = n.longValue()
      if (asLong < Int.MinValue || asLong > Int.MaxValue) {
        throw new IllegalArgumentException(
          s"$key must be a positive integer that fits in Int (got $asLong)")
      }
      Some(asLong.toInt)
    case Some(other) =>
      throw new IllegalArgumentException(
        s"$key must be an integer, got: $other")
  }

  private def parseInt(raw: Option[Any], key: String, default: Int): Int =
    parseIntOpt(raw, key).getOrElse(default)

  /**
   * Driver-only entry point. Opens the lance Dataset to read column dimension
   * and total row count, then delegates to [[parseAndValidate]].
   */
  def resolve(
      indexType: IndexType,
      args: Seq[LanceNamedArgument],
      readOptions: LanceSparkReadOptions,
      initialStorageOptions: Option[Map[String, String]],
      namespaceImpl: Option[String],
      namespaceProperties: Option[Map[String, String]],
      tableId: Option[List[String]],
      column: String): VectorIndexPlan = {

    val dataset = Utils.openDatasetBuilder(readOptions)
      .initialStorageOptions(initialStorageOptions.map(_.asJava).orNull)
      .runtimeNamespace(
        namespaceImpl.orNull,
        namespaceProperties.map(_.asJava).orNull,
        tableId.map(_.asJava).orNull)
      .build()
    try {
      val arrowSchema = dataset.getLanceSchema.asArrowSchema()
      val arrowField: Field = Option(arrowSchema.findField(column)).getOrElse {
        val available = arrowSchema.getFields.asScala.map(_.getName).mkString(", ")
        throw new IllegalArgumentException(
          s"Column '$column' not found. Available: $available")
      }

      val dim = VectorUtils.getVectorArrowDimension(arrowField)
      if (dim < 0) {
        throw new IllegalArgumentException(
          s"Column '$column' is not a fixed-size vector column " +
            "(FixedSizeList<Float|Double>); cannot build vector index")
      }

      val numRows = dataset.countRows()
      parseAndValidate(indexType, args, dim, numRows)
    } finally {
      dataset.close()
    }
  }
}
