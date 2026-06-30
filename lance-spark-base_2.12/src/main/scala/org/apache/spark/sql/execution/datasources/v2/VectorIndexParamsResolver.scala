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

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.sql.catalyst.plans.logical.LanceNamedArgument
import org.lance.index.{DistanceType, IndexType}
import org.lance.spark.utils.VectorUtils

import java.util.Locale

import scala.collection.JavaConverters._

/**
 * Parses and validates the WITH(...) named arguments attached to
 * `ALTER TABLE … CREATE INDEX … USING <IVF_*> ( col )` and infers defaults.
 */
object VectorIndexParamsResolver {

  // ---- key whitelists ----
  // `train` is consumed at the Spark execution layer (handled in AddIndexExec) and stripped
  // before being forwarded to lance-core via SparkOnlyOptions. It is whitelisted here so that
  // explicitly writing `train=true` on an IVF_* CREATE INDEX is accepted instead of being
  // rejected as an unknown parameter; `train=false` for IVF_* is rejected up front in
  // AddIndexExec.run before reaching this resolver.
  private val CommonKeys =
    Set("num_partitions", "distance_type", "sample_rate", "max_iters", "num_segments", "train")
  private val PqKeys = Set("num_sub_vectors", "num_bits")
  private val SqKeys = Set("num_bits")
  private val HnswKeys = Set("m", "ef_construction", "max_level", "prefetch_distance")

  private val PqIndexTypes =
    Set(IndexType.IVF_PQ, IndexType.IVF_HNSW_PQ)
  private val SqIndexTypes =
    Set(IndexType.IVF_SQ, IndexType.IVF_HNSW_SQ)
  private val HnswIndexTypes =
    Set(IndexType.IVF_HNSW_SQ, IndexType.IVF_HNSW_PQ)

  // Spark SQL intentionally pins vector-index defaults instead of deferring to
  // lance-core builder defaults. This keeps SQL behavior reproducible across
  // lance-core retunes; docs expose these as Spark defaults.
  private val DefaultSampleRate = 256
  private val DefaultMaxIters = 50
  private val DefaultPqNumBits = 8
  private val DefaultSqNumBits = 8
  private val DefaultHnswM = 20
  private val DefaultHnswEfConstruction = 150
  private val DefaultHnswMaxLevel = 7
  private val DefaultHnswPrefetchDistance = 2

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
    val distanceType = parseDistanceType(map.get("distance_type"))

    // 4. Common: sampleRate, maxIters, numPartitions
    val sampleRate =
      parseInt(map.get("sample_rate"), "sample_rate", default = DefaultSampleRate)
    if (sampleRate < 2) {
      throw new IllegalArgumentException(s"sample_rate must be >= 2 (got $sampleRate)")
    }
    val maxIters = parseInt(map.get("max_iters"), "max_iters", default = DefaultMaxIters)
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
      val numBits = parseInt(map.get("num_bits"), "num_bits", default = DefaultPqNumBits)
      if (numBits != 4 && numBits != 8) {
        throw new IllegalArgumentException(
          s"PQ num_bits must be 4 or 8 (got $numBits)")
      }
      if (numBits == 4 && nsv % 2 != 0) {
        throw new IllegalArgumentException(
          s"PQ num_bits=4 requires num_sub_vectors to be even (got $nsv)")
      }
      Some(PqPlan(
        numSubVectors = nsv,
        numBits = numBits,
        sampleRate = sampleRate,
        maxIters = maxIters))
    } else None

    // 6. SQ
    val sq = if (SqIndexTypes.contains(indexType)) {
      val numBits = parseInt(map.get("num_bits"), "num_bits", default = DefaultSqNumBits)
      if (numBits != 8) {
        throw new IllegalArgumentException(
          s"SQ num_bits must be 8 because Lance SQ currently builds 8-bit quantizers " +
            s"(got $numBits)")
      }
      Some(SqPlan(numBits = numBits, sampleRate = sampleRate))
    } else None

    // 7. HNSW
    val hnsw = if (HnswIndexTypes.contains(indexType)) {
      val m = parseInt(map.get("m"), "m", default = DefaultHnswM)
      if (m <= 0) throw new IllegalArgumentException(s"m must be positive (got $m)")
      val ef = parseInt(
        map.get("ef_construction"),
        "ef_construction",
        default = DefaultHnswEfConstruction)
      if (ef <= 0) throw new IllegalArgumentException(
        s"ef_construction must be positive (got $ef)")
      val maxLevel =
        parseInt(map.get("max_level"), "max_level", default = DefaultHnswMaxLevel)
      if (maxLevel <= 0 || maxLevel > 65535) {
        throw new IllegalArgumentException(
          s"max_level must be in [1, 65535] because Lance stores it as u16 " +
            s"(got $maxLevel)")
      }
      val prefetch = parseIntOpt(map.get("prefetch_distance"), "prefetch_distance")
        .orElse(Some(DefaultHnswPrefetchDistance))
      prefetch.foreach { p =>
        if (p < 0) throw new IllegalArgumentException(
          s"prefetch_distance must be non-negative (got $p)")
      }
      Some(HnswPlan(m, ef, maxLevel, prefetch))
    } else None

    VectorIndexPlan(indexType, distanceType, ivf, pq, sq, hnsw)
  }

  // ---------- helpers ----------

  private def parseDistanceType(raw: Option[Any]): DistanceType = raw match {
    case None => DistanceType.L2
    case Some(s: String) =>
      s.trim.toLowerCase(Locale.ROOT) match {
        case "l2" => DistanceType.L2
        case "euclidean" => DistanceType.L2
        case "cosine" => DistanceType.Cosine
        case "dot" => DistanceType.Dot
        case "hamming" =>
          throw new IllegalArgumentException(
            "distance_type 'hamming' requires UInt8 vector support, which Spark " +
              "CREATE INDEX does not support yet")
        case other =>
          throw new IllegalArgumentException(
            s"distance_type '$other' not supported. " +
              "Valid: l2, cosine, dot (alias: euclidean->l2)")
      }
    case Some(other) =>
      throw new IllegalArgumentException(
        s"distance_type must be a string, got: $other")
  }

  private def parseIntOpt(raw: Option[Any], key: String): Option[Int] = raw match {
    case None => None
    case Some(null) =>
      throw new IllegalArgumentException(s"$key must be an integer, got: null")
    case Some(n: java.lang.Byte) => Some(validateIntRange(n.longValue(), key))
    case Some(n: java.lang.Short) => Some(validateIntRange(n.longValue(), key))
    case Some(n: java.lang.Integer) => Some(validateIntRange(n.longValue(), key))
    case Some(n: java.lang.Long) => Some(validateIntRange(n.longValue(), key))
    case Some(n: java.math.BigInteger) =>
      if (n.compareTo(java.math.BigInteger.valueOf(Int.MinValue.toLong)) < 0 ||
        n.compareTo(java.math.BigInteger.valueOf(Int.MaxValue.toLong)) > 0) {
        throw new IllegalArgumentException(s"$key must fit in Int (got $n)")
      }
      Some(n.intValue())
    case Some(n: java.math.BigDecimal) =>
      try {
        val exact = n.toBigIntegerExact
        if (exact.compareTo(java.math.BigInteger.valueOf(Int.MinValue.toLong)) < 0 ||
          exact.compareTo(java.math.BigInteger.valueOf(Int.MaxValue.toLong)) > 0) {
          throw new IllegalArgumentException(s"$key must fit in Int (got $n)")
        }
        Some(exact.intValue())
      } catch {
        case _: ArithmeticException =>
          throw new IllegalArgumentException(s"$key must be an integer, got: $n")
      }
    case Some(_: java.lang.Float) | Some(_: java.lang.Double) =>
      throw new IllegalArgumentException(s"$key must be an integer literal, got: ${raw.get}")
    case Some(other) =>
      throw new IllegalArgumentException(s"$key must be an integer, got: $other")
  }

  private def validateIntRange(value: Long, key: String): Int = {
    if (value < Int.MinValue || value > Int.MaxValue) {
      throw new IllegalArgumentException(s"$key must fit in Int (got $value)")
    }
    value.toInt
  }

  private def parseInt(raw: Option[Any], key: String, default: Int): Int =
    parseIntOpt(raw, key).getOrElse(default)

  def validateVectorFieldForIndex(column: String, field: Field): Int = {
    val dim = VectorUtils.getVectorArrowDimension(field)
    if (dim < 0) {
      throw new IllegalArgumentException(
        s"Column '$column' is not a fixed-size vector column " +
          "(requires FixedSizeList<Float32>); cannot build vector index")
    }

    val child = field.getChildren.asScala.headOption.getOrElse {
      throw new IllegalArgumentException(
        s"Column '$column' is not a fixed-size vector column " +
          "(requires FixedSizeList<Float32>); cannot build vector index")
    }
    child.getType match {
      case fp: ArrowType.FloatingPoint
          if fp.getPrecision == FloatingPointPrecision.SINGLE =>
        dim
      case fp: ArrowType.FloatingPoint =>
        throw new IllegalArgumentException(
          s"Column '$column' uses FixedSizeList<$fp>. Spark CREATE INDEX currently supports " +
            "only FixedSizeList<Float32> vector columns; Double vector index training is not " +
            "supported yet")
      case other =>
        throw new IllegalArgumentException(
          s"Column '$column' uses FixedSizeList<$other>. Spark CREATE INDEX currently " +
            "supports only FixedSizeList<Float32> vector columns")
    }
  }

}
