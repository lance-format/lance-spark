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

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.lance.spark.{LanceRef, LanceSparkReadOptions}
import org.lance.spark.utils.Utils

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URLClassLoader
import java.nio.file.{Files, Paths}
import java.util.Collections
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Own JVMs preserve retry-master and heap-limit semantics without altering a suite's context. */
object LanceSampleTestProcess {
  def run(mainClass: String, heap: Option[String] = None): String = {
    val classpath = mutable.LinkedHashSet.empty[String]
    var loader = Thread.currentThread().getContextClassLoader
    while (loader != null) {
      loader match {
        case urls: URLClassLoader =>
          urls.getURLs.filter(_.getProtocol == "file").foreach { url =>
            classpath += Paths.get(url.toURI).toString
          }
        case _ =>
      }
      loader = loader.getParent
    }
    classpath ++= System.getProperty("java.class.path").split(File.pathSeparator)
    val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val opens = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filter(_.startsWith("--add-opens="))
    val command = Seq(
      java,
      "-XX:+IgnoreUnrecognizedVMOptions",
      "-Dio.netty.tryReflectionSetAccessible=true") ++ heap.map("-Xmx" + _) ++
      opens ++ Seq("-cp", classpath.mkString(File.pathSeparator), mainClass)
    val log = Files.createTempFile("lance-sample-probe-", ".log")
    val process = new ProcessBuilder(command.asJava).redirectErrorStream(true)
      .redirectOutput(log.toFile).start()
    try {
      val exited = process.waitFor(60, TimeUnit.SECONDS)
      assertTrue(exited, s"$mainClass did not finish: ${Files.readString(log)}")
      val output = Files.readString(log)
      assertEquals(0, process.exitValue(), output)
      output
    } finally {
      if (process.isAlive) {
        process.destroyForcibly()
        process.waitFor(5, TimeUnit.SECONDS)
      }
      Files.deleteIfExists(log)
    }
  }

  def partition(
      schema: StructType,
      positions: Array[Long],
      batchSize: Int): LanceSampleInputPartition = {
    val uri = getClass.getResource("/example_db/test_dataset1.lance").toString
    val dataset = Utils.openDatasetBuilder(LanceSparkReadOptions.from(uri)).build()
    val version =
      try dataset.version()
      finally dataset.close()
    val options = LanceSparkReadOptions.builder().datasetUri(uri)
      .ref(LanceRef.ofMain(version)).sampleSize(positions.length.toLong).sampleSeed(42L)
      .sampleBatchSize(batchSize).build()
    LanceSampleInputPartition(
      schema,
      options,
      positions,
      Collections.emptyMap[String, String](),
      null,
      Collections.emptyMap[String, String]())
  }
}

object SampleRetryProbe {
  def main(args: Array[String]): Unit = {
    val input = LanceSampleTestProcess.partition(
      new StructType().add("x", DataTypes.LongType),
      Array[Long](1, 3),
      1)
    val context = new SparkContext(new SparkConf().setAppName("exact-sampling-retry-test")
      .setMaster("local[1,2]").set("spark.ui.enabled", "false"))
    try {
      val replayed = context.parallelize(Seq(input), 1).mapPartitions { inputs =>
        inputs.flatMap { partition =>
          val reader = new LanceSamplePartitionReader(partition)
          val values = mutable.ArrayBuffer.empty[Long]
          try {
            while (reader.next()) {
              val batch = reader.get()
              for (row <- 0 until batch.numRows()) values += batch.column(0).getLong(row)
            }
          } finally reader.close()
          val attempt = TaskContext.get().attemptNumber()
          if (attempt == 0)
            throw new IllegalStateException("Fail after consuming the first attempt")
          values.iterator.map(value => (value, attempt))
        }
      }.collect().toSeq
      require(replayed == Seq((1L, 1), (3L, 1)), s"Unexpected retries: $replayed")
      println("RETRY_OK")
    } finally context.stop()
  }
}

object SampleHeapProbe {
  def main(args: Array[String]): Unit = {
    val heap = Runtime.getRuntime.maxMemory()
    require(heap <= 64L * 1024 * 1024, s"Unexpected heap limit: $heap")
    val population = 1L << 40
    val positions = ExactKSampler.sample(population, LanceSparkReadOptions.MAX_SAMPLE_SIZE, 42)
    require(positions.length == 1000000)
    var index = 0
    while (index < positions.length) {
      require(positions(index) >= 0 && positions(index) < population)
      if (index > 0) require(positions(index) > positions(index - 1))
      index += 1
    }
    println(s"MAX_SAMPLE_OK rows=${positions.length} heap=$heap")
  }
}
