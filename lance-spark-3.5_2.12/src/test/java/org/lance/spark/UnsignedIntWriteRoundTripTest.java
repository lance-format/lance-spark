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
package org.lance.spark;

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.memwal.InitializeMemWalParams;
import org.lance.memwal.ShardingEvaluator;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnsignedIntWriteRoundTripTest extends BaseUnsignedIntWriteRoundTripTest {

  @Test
  public void testUpdateKeepsUnsignedTypes() throws Exception {
    String datasetUri = tempDir.resolve("uint_update.lance").toString();
    createUnsignedDataset(datasetUri);

    spark.sql("UPDATE lance.`" + datasetUri + "` SET u8 = 254 WHERE id = 1");

    assertUnsignedColumns(datasetUri);
    List<Row> rows =
        spark
            .read()
            .format(LanceDataSource.name)
            .load(datasetUri)
            .selectExpr("id", "u8", "u16", "u32", "u64")
            .collectAsList();
    assertEquals(
        Collections.singletonList(
            RowFactory.create(1, (short) 254, 65535, 4294967295L, 9000000000L)),
        rows);
  }

  @Test
  public void testUpdateAndMergeKeepUint64Bits() throws Exception {
    long max = Long.parseUnsignedLong("18446744073709551615");
    long half = Long.parseUnsignedLong("9223372036854775808");
    String datasetUri = tempDir.resolve("uint64_row_level.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                unsignedField("u64", 64),
                new Field("tag", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, LanceRuntime.allocator())) {
      root.allocateNew();
      long[] values = {max, half};
      for (int i = 0; i < values.length; i++) {
        ((IntVector) root.getVector("id")).setSafe(i, i + 1);
        ((UInt8Vector) root.getVector("u64")).setSafe(i, values[i]);
        ((VarCharVector) root.getVector("tag")).setSafe(i, "old".getBytes(StandardCharsets.UTF_8));
      }
      root.setRowCount(values.length);
      writeDataset(datasetUri, root);
    }
    String table = "lance.`" + datasetUri + "`";

    spark.sql("UPDATE " + table + " SET tag = 'updated' WHERE id = 1");
    StructType sourceSchema =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("u64", DataTypes.LongType)
            .add("tag", DataTypes.StringType);
    spark
        .createDataFrame(
            Arrays.asList(RowFactory.create(2, null, "merged"), RowFactory.create(3, half, "new")),
            sourceSchema)
        .createOrReplaceTempView("uint64_merge_source");
    spark.sql(
        "MERGE INTO "
            + table
            + " t USING uint64_merge_source s ON t.id = s.id"
            + " WHEN MATCHED THEN UPDATE SET tag = s.tag"
            + " WHEN NOT MATCHED THEN INSERT (id, u64, tag) VALUES (s.id, s.u64, s.tag)");

    try (Dataset dataset = open(datasetUri)) {
      assertIntType(dataset.getSchema(), "u64", 64, false);
    }
    assertEquals(
        Arrays.asList(
            RowFactory.create(1, max, "updated"),
            RowFactory.create(2, half, "merged"),
            RowFactory.create(3, half, "new")),
        spark
            .read()
            .format(LanceDataSource.name)
            .load(datasetUri)
            .orderBy("id")
            .selectExpr("id", "u64", "tag")
            .collectAsList());
  }

  @Test
  public void testBucketShardingUsesUint32Keys() throws Exception {
    String datasetUri = tempDir.resolve("uint32_bucket.lance").toString();
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                unsignedField("u32", 32)));
    try (Dataset dataset =
        Dataset.write()
            .allocator(LanceRuntime.allocator())
            .uri(datasetUri)
            .schema(schema)
            .mode(WriteParams.WriteMode.CREATE)
            .execute()) {
      dataset.initializeMemWal(new InitializeMemWalParams().withBucketSharding("u32", 4));
    }
    StructType sparkSchema = spark.read().format(LanceDataSource.name).load(datasetUri).schema();
    List<Row> input = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      input.add(RowFactory.create(i, i * 107374182L));
    }
    spark
        .createDataFrame(input, sparkSchema)
        .write()
        .format(LanceDataSource.name)
        .mode(SaveMode.Append)
        .save(datasetUri);

    List<Row> stored =
        spark.sql("SELECT _fragid, u32 FROM lance.`" + datasetUri + "`").collectAsList();
    assertEquals(input.size(), stored.size());
    long[] values = stored.stream().mapToLong(row -> row.getLong(1)).toArray();
    int[] buckets = lanceBuckets(datasetUri, values);
    Map<Integer, Set<Integer>> bucketsByFragment = new HashMap<>();
    for (int i = 0; i < stored.size(); i++) {
      bucketsByFragment
          .computeIfAbsent(stored.get(i).getInt(0), id -> new HashSet<>())
          .add(buckets[i]);
    }
    bucketsByFragment.forEach(
        (fragment, fragmentBuckets) ->
            assertEquals(
                1, fragmentBuckets.size(), "fragment " + fragment + " " + fragmentBuckets));
    assertTrue(Arrays.stream(buckets).distinct().count() > 1, "rows should span buckets");
    // A one-row fragment matches any hash, so the check above needs fragments of several rows.
    assertTrue(bucketsByFragment.size() < stored.size(), "every fragment holds a single row");
  }

  /** Buckets Lance itself assigns to {@code values} of the table's uint32 sharding column. */
  private static int[] lanceBuckets(String datasetUri, long[] values) throws Exception {
    try (Dataset dataset = open(datasetUri);
        VectorSchemaRoot root =
            VectorSchemaRoot.create(dataset.getSchema(), LanceRuntime.allocator())) {
      root.allocateNew();
      UInt4Vector u32 = (UInt4Vector) root.getVector("u32");
      for (int i = 0; i < values.length; i++) {
        u32.setSafe(i, (int) values[i]);
      }
      root.setRowCount(values.length);
      try (ArrowReader reader =
          ShardingEvaluator.evaluate(
              LanceRuntime.allocator(),
              root,
              dataset.memWalIndexDetails().get().shardingSpecs().get(0),
              dataset.getLanceSchema())) {
        assertTrue(reader.loadNextBatch());
        IntVector bucketIds = (IntVector) reader.getVectorSchemaRoot().getVector(0);
        int[] buckets = new int[values.length];
        for (int i = 0; i < values.length; i++) {
          buckets[i] = bucketIds.get(i);
        }
        return buckets;
      }
    }
  }
}
