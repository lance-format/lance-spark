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
package org.lance.spark.knn.testutil

import java.util.Random

/**
 * Generate a clustered Gaussian-mixture embedding sample as a stand-in for real production
 * embeddings (SIFT / sentence-transformer / image features). Real embeddings are not uniform
 * over the unit hypercube — they cluster around a small number of topic centroids with each
 * cluster occupying a relatively narrow region of the space. Uniform-random vectors are the
 * worst case for IVF: there's no natural cluster structure for k-means to latch onto, so the
 * IVF partitions cover the space arbitrarily and the per-cluster recall is essentially random.
 *
 * Method:
 *   1. Pick `numClusters` cluster centers, each drawn uniformly from the unit hypercube.
 *   2. For each row, pick a cluster (round-robin so each cluster gets equal mass) and sample
 *      a Gaussian centered on it with standard deviation `sigma * cluster_separation`.
 *   3. L2-normalize so vectors live on the unit sphere — the natural geometry for cosine /
 *      inner-product retrieval, and what most production embedding models produce.
 *
 * The cluster-separation factor is the median pairwise distance between centers; scaling sigma
 * by it keeps the cluster radius proportional to inter-cluster spacing regardless of `dim` or
 * `numClusters`. With sigma ≈ 0.15 the clusters overlap a little but stay distinguishable —
 * a reasonable proxy for production embedding distributions.
 *
 * The generator is deterministic given the seed so test runs are reproducible.
 */
object ClusteredEmbeddings {

  /**
   * Build a clustered-Gaussian-mixture sample.
   *
   * @param n           number of vectors to generate
   * @param dim         vector dimension
   * @param numClusters number of cluster centers (small relative to `n` — typical 16-64)
   * @param sigma       per-cluster standard deviation, in units of inter-cluster distance.
   *                    0.05 = tight clusters (high recall floor); 0.5 = loose, near-uniform
   * @param seed        RNG seed for reproducibility
   * @return            an array of `n` float vectors of dimension `dim`, L2-normalized
   */
  def generate(
      n: Int,
      dim: Int,
      numClusters: Int,
      sigma: Double = 0.15,
      seed: Long = 0L): Array[Array[Float]] = {
    require(n > 0 && dim > 0 && numClusters > 0, "n, dim, numClusters must all be positive")
    require(numClusters <= n, "numClusters cannot exceed n")
    val rng = new Random(seed)

    // Step 1: cluster centers, uniform on [0, 1]^dim. Stored as Doubles so the noise pass keeps
    // numerical headroom — L2 normalization at the end folds back to Float precision.
    val centers = Array.fill(numClusters)(Array.fill(dim)(rng.nextDouble()))

    // Step 2: median pairwise distance between centers, used to scale sigma. We don't want sigma
    // expressed in absolute distance units — the right notion is "fraction of cluster spacing,"
    // which keeps clustering tightness behavior stable across (dim, numClusters) settings.
    val sep = medianPairwiseDistance(centers)
    val scaledSigma = sigma * sep

    // Step 3: sample each row from a Gaussian centered on a round-robin cluster. Round-robin
    // (rather than uniformly random cluster choice) gives every cluster the same mass — a more
    // controlled benchmark setup than letting some clusters get sparsely populated.
    val out = new Array[Array[Float]](n)
    var i = 0
    while (i < n) {
      val center = centers(i % numClusters)
      val v = new Array[Float](dim)
      var d = 0
      while (d < dim) {
        v(d) = (center(d) + rng.nextGaussian() * scaledSigma).toFloat
        d += 1
      }
      l2Normalize(v)
      out(i) = v
      i += 1
    }
    out
  }

  /**
   * Median pairwise L2 distance between centers. We sample up to 1024 random center pairs
   * rather than computing all `O(K^2)` of them — for `numClusters = 64` that's 2016 pairs,
   * trivial; for larger K we'd otherwise pay cost the rest of the test doesn't need.
   */
  private def medianPairwiseDistance(centers: Array[Array[Double]]): Double = {
    val k = centers.length
    if (k < 2) return 1.0
    val rng = new Random(0L)
    val numPairs = math.min(1024, k * (k - 1) / 2)
    val dists = new Array[Double](numPairs)
    var p = 0
    while (p < numPairs) {
      var i = rng.nextInt(k)
      var j = rng.nextInt(k)
      while (j == i) j = rng.nextInt(k)
      dists(p) = euclidean(centers(i), centers(j))
      p += 1
    }
    java.util.Arrays.sort(dists)
    dists(dists.length / 2)
  }

  private def euclidean(a: Array[Double], b: Array[Double]): Double = {
    var s = 0.0
    var i = 0
    while (i < a.length) {
      val d = a(i) - b(i)
      s += d * d
      i += 1
    }
    math.sqrt(s)
  }

  private def l2Normalize(v: Array[Float]): Unit = {
    var s = 0.0
    var i = 0
    while (i < v.length) { s += v(i) * v(i); i += 1 }
    val norm = math.sqrt(s).toFloat
    if (norm > 0f) {
      i = 0
      while (i < v.length) { v(i) = v(i) / norm; i += 1 }
    }
  }
}
