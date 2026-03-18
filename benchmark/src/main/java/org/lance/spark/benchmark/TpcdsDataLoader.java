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
package org.lance.spark.benchmark;

import java.io.File;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class TpcdsDataLoader {

  private final SparkSession spark;
  private final String rawDataDir;
  private final String dataDir;

  public TpcdsDataLoader(SparkSession spark, String rawDataDir, String dataDir) {
    this.spark = spark;
    this.rawDataDir = rawDataDir;
    this.dataDir = dataDir;
  }

  public void loadAllTables(String format) {
    Map<String, StructType> schemas = TpcdsSchemaDefinition.getAllSchemas();
    String outputDir = dataDir + "/" + format;

    System.out.println("Loading tables into " + format + " at " + outputDir);
    System.out.flush();

    for (Map.Entry<String, StructType> entry : schemas.entrySet()) {
      String tableName = entry.getKey();
      StructType schema = entry.getValue();
      loadTable(tableName, schema, format, outputDir);
    }
  }

  private void loadTable(String tableName, StructType schema, String format, String outputDir) {
    String rawFile = rawDataDir + "/" + tableName + ".dat";
    if (!new File(rawFile).exists()) {
      System.out.println("  SKIP " + tableName + " (no data file)");
      System.out.flush();
      return;
    }

    boolean isLance = "lance".equalsIgnoreCase(format);
    String tablePath = outputDir + "/" + tableName;
    if (isLance) {
      tablePath = tablePath + ".lance";
    }
    if (new File(tablePath).exists()) {
      System.out.println("  SKIP " + tableName + " (already loaded)");
      System.out.flush();
      return;
    }

    System.out.print("  LOAD " + tableName + "...");
    System.out.flush();
    long start = System.currentTimeMillis();

    Dataset<Row> df =
        spark
            .read()
            .option("delimiter", "|")
            .option("header", "false")
            .option("emptyValue", "")
            .schema(schema)
            .csv(rawFile);

    String writeFormat = isLance ? "lance" : format;
    SaveMode mode = isLance ? SaveMode.ErrorIfExists : SaveMode.Overwrite;

    df.write().mode(mode).format(writeFormat).save(tablePath);

    long elapsed = System.currentTimeMillis() - start;
    long count = spark.read().format(writeFormat).load(tablePath).count();
    System.out.println(" " + count + " rows (" + elapsed + "ms)");
    System.out.flush();
  }

  public void registerTables(String format) {
    String formatDir = dataDir + "/" + format;
    boolean isLance = "lance".equalsIgnoreCase(format);
    String readFormat = isLance ? "lance" : format;

    for (String tableName : TpcdsSchemaDefinition.getTableNames()) {
      String tablePath = formatDir + "/" + tableName;
      if (isLance) {
        tablePath = tablePath + ".lance";
      }
      if (!new File(tablePath).exists()) {
        continue;
      }
      spark.read().format(readFormat).load(tablePath).createOrReplaceTempView(tableName);
    }
  }
}
