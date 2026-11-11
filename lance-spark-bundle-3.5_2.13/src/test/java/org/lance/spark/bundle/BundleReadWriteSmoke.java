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
package org.lance.spark.bundle;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * Standalone smoke driver launched in a child JVM by {@link BundleReadWriteIT} with the shaded
 * bundle jar prepended to the classpath, so all lance/arrow classes (and the lance native library)
 * resolve from the bundle.
 *
 * <p>It writes a tiny table and reads it back through {@code spark.write/read.format("lance")},
 * which drives the real native read/write path. That path goes through {@code
 * org.lance.ipc.LanceScanner}, whose superinterface {@code
 * org.apache.arrow.dataset.scanner.Scanner} lives in the (native-stripped) arrow-dataset API kept
 * in the bundle -- exercising end to end the exact packaging the shade filter guards.
 *
 * <p>Prints {@code SMOKE_OK ...} and exits 0 on success; prints a stack trace and exits non-zero on
 * any failure. It intentionally has no JUnit dependency so it can run as a bare {@code main} in the
 * curated child classpath.
 */
public final class BundleReadWriteSmoke {

  private BundleReadWriteSmoke() {}

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("usage: BundleReadWriteSmoke <workDir>");
      System.exit(2);
    }
    String path = args[0] + "/smoke.lance";
    SparkSession spark =
        SparkSession.builder()
            .appName("bundle-read-write-smoke")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
    try {
      StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("id", DataTypes.LongType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true)
              });
      List<Row> rows =
          Arrays.asList(
              RowFactory.create(1L, "a"), RowFactory.create(2L, "b"), RowFactory.create(3L, "c"));

      spark
          .createDataFrame(rows, schema)
          .write()
          .format("lance")
          .mode(SaveMode.ErrorIfExists)
          .save(path);

      Dataset<Row> back = spark.read().format("lance").load(path);
      long count = back.count();
      if (count != rows.size()) {
        throw new IllegalStateException("expected " + rows.size() + " rows, got " + count);
      }
      long idSum =
          back.collectAsList().stream().mapToLong(r -> r.getLong(r.fieldIndex("id"))).sum();
      if (idSum != 6L) {
        throw new IllegalStateException("expected id sum 6, got " + idSum);
      }
      System.out.println("SMOKE_OK rows=" + count + " idSum=" + idSum);
    } catch (Throwable t) {
      t.printStackTrace();
      spark.stop();
      System.exit(1);
    }
    spark.stop();
    System.exit(0);
  }
}
