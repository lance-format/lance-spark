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
package org.lance.spark.cdf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Shared test utility for CDF (Change Data Feed) tests. Provides helper methods for table
 * operations and version tracking assertions.
 */
public class CdfTestHelper {
  private final SparkSession spark;
  private final String catalogName;
  private final String tableName;

  public CdfTestHelper(SparkSession spark, String catalogName) {
    this.spark = spark;
    this.catalogName = catalogName;
    this.tableName = "cdf_test_" + UUID.randomUUID().toString().replace("-", "");
  }

  public void create() {
    spark.sql(
        String.format(
            "CREATE TABLE %s.default.%s (id INT NOT NULL, name STRING, value INT) "
                + "TBLPROPERTIES ('enable_stable_row_ids' = 'true')",
            catalogName, tableName));
  }

  public void insert(List<CdfRow> rows) {
    String sql =
        String.format(
            "INSERT INTO %s.default.%s VALUES %s",
            catalogName,
            tableName,
            rows.stream().map(CdfRow::insertSql).collect(Collectors.joining(", ")));
    spark.sql(sql);
  }

  public void update(String setClause, String condition) {
    spark.sql(
        String.format(
            "UPDATE %s.default.%s SET %s WHERE %s", catalogName, tableName, setClause, condition));
  }

  public void delete(String condition) {
    spark.sql(
        String.format("DELETE FROM %s.default.%s WHERE %s", catalogName, tableName, condition));
  }

  public List<CdfRow> query(String sql) {
    return spark.sql(sql).collectAsList().stream()
        .map(
            row ->
                CdfRow.ofWithVersions(
                    row.getInt(0), row.getString(1), row.getInt(2), row.getLong(3), row.getLong(4)))
        .collect(Collectors.toList());
  }

  public List<CdfRow> getAllWithVersions() {
    String sql =
        String.format(
            "SELECT id, name, value, _row_created_at_version, _row_last_updated_at_version "
                + "FROM %s.default.%s ORDER BY id",
            catalogName, tableName);
    return query(sql);
  }

  public List<CdfRow> getChangesSince(long lastProcessedVersion) {
    String sql =
        String.format(
            "SELECT id, name, value, _row_created_at_version, _row_last_updated_at_version "
                + "FROM %s.default.%s "
                + "WHERE _row_created_at_version > %d OR _row_last_updated_at_version > %d "
                + "ORDER BY id",
            catalogName, tableName, lastProcessedVersion, lastProcessedVersion);
    return query(sql);
  }

  public void checkWithVersions(List<CdfRow> expected) {
    List<CdfRow> actual = getAllWithVersions();
    assertEquals(expected, actual);
  }

  public Dataset<Row> queryDataset(String sql) {
    return spark.sql(sql);
  }

  public String getTableName() {
    return catalogName + ".default." + tableName;
  }

  /** Data class representing a row with optional version tracking metadata. */
  public static class CdfRow {
    public Integer id;
    public String name;
    public Integer value;
    public Long createdAtVersion;
    public Long lastUpdatedAtVersion;

    public static CdfRow of(int id, String name, int value) {
      CdfRow row = new CdfRow();
      row.id = id;
      row.name = name;
      row.value = value;
      return row;
    }

    public static CdfRow ofWithVersions(
        int id, String name, int value, Long createdAtVersion, Long lastUpdatedAtVersion) {
      CdfRow row = new CdfRow();
      row.id = id;
      row.name = name;
      row.value = value;
      row.createdAtVersion = createdAtVersion;
      row.lastUpdatedAtVersion = lastUpdatedAtVersion;
      return row;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CdfRow cdfRow = (CdfRow) o;
      return Objects.equals(id, cdfRow.id)
          && Objects.equals(name, cdfRow.name)
          && Objects.equals(value, cdfRow.value)
          && Objects.equals(createdAtVersion, cdfRow.createdAtVersion)
          && Objects.equals(lastUpdatedAtVersion, cdfRow.lastUpdatedAtVersion);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, value, createdAtVersion, lastUpdatedAtVersion);
    }

    @Override
    public String toString() {
      return String.format(
          "CdfRow(id=%s, name=%s, value=%s, created=%s, updated=%s)",
          id, name, value, createdAtVersion, lastUpdatedAtVersion);
    }

    public String insertSql() {
      return String.format("(%d, '%s', %d)", id, name, value);
    }
  }
}
