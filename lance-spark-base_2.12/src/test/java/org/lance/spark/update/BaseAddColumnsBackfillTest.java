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
package org.lance.spark.update;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class BaseAddColumnsBackfillTest {
  protected String catalogName = "lance_test";
  protected String tableName = "add_column_backfill";
  protected String fullTable = catalogName + ".default." + tableName;

  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    spark =
        SparkSession.builder()
            .appName("dataframe-addcolumn-test")
            .master("local[12]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();
    // Create default namespace for multi-level namespace mode
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  protected void prepareDataset() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(0, 10)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
  }

  @Test
  public void testWithDataFrame() {
    prepareDataset();

    // Read back and verify
    Dataset<Row> result = spark.table(fullTable);
    assertEquals(10, result.count(), "Should have 10 rows");

    result = result.select("_rowaddr", "_fragid", "id");

    // Add new column
    Dataset<Row> df2 =
        result
            .withColumn("new_col1", functions.expr("id * 100"))
            .withColumn("new_col2", functions.expr("id * 2"));

    df2.createOrReplaceTempView("tmp_view");
    spark.sql(
        String.format("alter table %s add columns new_col1, new_col2 from tmp_view", fullTable));

    Assertions.assertEquals(
        "[[0,0,0,text_0], [1,100,2,text_1], [2,200,4,text_2], [3,300,6,text_3], [4,400,8,text_4], [5,500,10,text_5], [6,600,12,text_6], [7,700,14,text_7], [8,800,16,text_8], [9,900,18,text_9]]",
        spark
            .table(fullTable)
            .select("id", "new_col1", "new_col2", "text")
            .collectAsList()
            .toString());
  }

  @Test
  public void testWithSql() {
    prepareDataset();

    spark.sql(
        String.format(
            "create temporary view tmp_view as select _rowaddr, _fragid, id * 100 as new_col1, id * 2 as new_col2, id * 3 as new_col3 from %s;",
            fullTable));
    spark.sql(
        String.format("alter table %s add columns new_col1, new_col2 from tmp_view", fullTable));

    Assertions.assertEquals(
        "[[0,0,0,text_0], [1,100,2,text_1], [2,200,4,text_2], [3,300,6,text_3], [4,400,8,text_4], [5,500,10,text_5], [6,600,12,text_6], [7,700,14,text_7], [8,800,16,text_8], [9,900,18,text_9]]",
        spark
            .sql(String.format("select id, new_col1, new_col2, text from %s", fullTable))
            .collectAsList()
            .toString());
  }

  @Test
  public void testAddExistedColumns() {
    prepareDataset();

    spark.sql(
        String.format(
            "create temporary view tmp_view as select _rowaddr, _fragid, id * 100 as id, id * 2 as new_col2 from %s;",
            fullTable));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            spark.sql(
                String.format("alter table %s add columns id, new_col2 from tmp_view", fullTable)),
        "Can't add existed columns: id");
  }

  @Test
  public void testAddRowsNotAligned() {
    prepareDataset();

    // Add a new String column (which can be null)
    // New records are not aligned with existing records
    spark.sql(
        String.format(
            "create temporary view tmp_view as select _rowaddr, _fragid, concat('new_col_1_', id) as new_col1 from %s where id in (0, 1, 4, 8, 9);",
            fullTable));
    spark.sql(String.format("alter table %s add columns new_col1 from tmp_view", fullTable));

    Assertions.assertEquals(
        "[[0,new_col_1_0,text_0], [1,new_col_1_1,text_1], [2,null,text_2], [3,null,text_3], [4,new_col_1_4,text_4], [5,null,text_5], [6,null,text_6], [7,null,text_7], [8,new_col_1_8,text_8], [9,new_col_1_9,text_9]]",
        spark
            .sql(String.format("select id, new_col1, text from %s", fullTable))
            .collectAsList()
            .toString());
  }

  @Test
  public void testAddStructColumn() {
    prepareDataset();

    spark.sql(
        String.format(
            "create temporary view tmp_view as select _rowaddr, _fragid, named_struct('id', id, 'name', concat('name_', id)) as struct_col from %s;",
            fullTable));
    spark.sql(String.format("alter table %s add columns struct_col from tmp_view", fullTable));

    for (Row row :
        spark.sql(String.format("select id, struct_col from %s", fullTable)).collectAsList()) {
      int id = row.getInt(0);
      Row structCol = row.getStruct(1);
      Assertions.assertEquals(id, structCol.getInt(0));
      Assertions.assertEquals("name_" + id, structCol.getString(1));
    }
  }
}
