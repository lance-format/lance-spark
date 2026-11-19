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
package org.apache.spark.sql.catalyst

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Test

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

class CatalystCompatibilityValidationTest {

  @Test
  def auditsRuntimeLinkageAndExercisesWrites(): Unit = {
    val catalogRoot = Files.createTempDirectory("lance-catalyst-validation")
    try {
      val spark = SparkSession.builder()
        .appName("CatalystCompatibilityValidation")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.testing.memory", "2147480000")
        .config(
          "spark.sql.extensions",
          "org.lance.spark.extensions.LanceSparkSessionExtensions")
        .config(
          "spark.sql.catalog.lance",
          "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "dir")
        .config("spark.sql.catalog.lance.root", catalogRoot.toString)
        .getOrCreate()

      try CatalystCompatibilityValidation.run(spark)
      finally spark.stop()
    } finally {
      deleteRecursively(catalogRoot)
    }
  }

  private def deleteRecursively(root: Path): Unit = {
    val paths = Files.walk(root)
    try {
      paths.iterator().asScala.toSeq.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists(_))
    } finally {
      paths.close()
    }
  }
}
