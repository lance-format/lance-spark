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
package org.lance.spark.write;

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Transaction;
import org.lance.WriteParams;
import org.lance.merge.MergeInsertParams;
import org.lance.operation.KeyExistenceFilter;
import org.lance.operation.Operation;
import org.lance.operation.Update;
import org.lance.operation.UpdateConfig;
import org.lance.operation.UpdateMap;
import org.lance.schema.LanceField;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Cross-implementation vector against the bundled lance-core: a native merge_insert on an
 * unenforced-PK table attaches a KeyExistenceFilter bloom, and this asserts the connector builds a
 * byte-identical bloom for the same keys. This pins the size, key encoding, hash and serialization
 * to lance-core's actual output, so a large Spark write and a native merge_insert produce
 * comparable filters (lance-core's intersects() rejects blooms of differing length).
 */
public class NativeMergeInsertFilterCompatibilityTest {
  @TempDir static Path tempDir;

  private static final Schema SCHEMA =
      new Schema(
          Collections.singletonList(
              new Field("id", FieldType.notNullable(new ArrowType.Utf8()), null)));

  private static final String[] KEYS = {"k0", "k1", "k2", "k3", "k4"};

  @Test
  public void connectorBloomMatchesNativeMergeInsertBloom() throws Exception {
    String uri = tempDir.resolve("compat").toString();
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.create(allocator, uri, SCHEMA, new WriteParams.Builder().build()).close();
      declareIdAsPrimaryKey(allocator, uri);
      nativeMergeInsert(allocator, uri);

      byte[] nativeBitmap;
      try (Dataset ds = Dataset.open(uri, allocator)) {
        Transaction txn = ds.readTransaction().orElseThrow(IllegalStateException::new);
        Operation op = txn.operation();
        Update update = assertInstanceOf(Update.class, op);
        KeyExistenceFilter filter =
            update.getInsertedRowsFilter().orElseThrow(IllegalStateException::new);
        assertEquals(KeyExistenceFilter.Type.BLOOM, filter.getType());
        // Native merge_insert pins these; the connector must declare the same or intersects()
        // refuses the comparison.
        assertEquals(SplitBlockBloomFilter.NUMBER_OF_ITEMS, filter.getBloomNumberOfItems());
        assertEquals(SplitBlockBloomFilter.PROBABILITY, filter.getBloomProbability());
        assertEquals(SplitBlockBloomFilter.NUM_BITS, filter.getBloomNumBits());
        assertEquals(SplitBlockBloomFilter.NUM_BYTES, filter.getBloomBitmap().length);
        nativeBitmap = filter.getBloomBitmap();
      }

      // Build the same filter through the connector: resolve the PK, hash the same keys, insert.
      byte[] connectorBitmap;
      try (Dataset ds = Dataset.open(uri, allocator)) {
        StructType sparkSchema = LanceArrowUtils.fromArrowSchema(SCHEMA);
        PrimaryKeyColumns pk = PrimaryKeyColumns.resolve(ds, sparkSchema);
        SplitBlockBloomFilter bloom = new SplitBlockBloomFilter();
        for (String key : KEYS) {
          InternalRow row = new GenericInternalRow(new Object[] {UTF8String.fromString(key)});
          bloom.insertHash(pk.hashKey(row));
        }
        connectorBitmap = bloom.toBytes();
      }

      assertArrayEquals(
          nativeBitmap,
          connectorBitmap,
          "connector bloom bitmap differs from native merge_insert bitmap");
    }
  }

  private void declareIdAsPrimaryKey(BufferAllocator allocator, String uri) {
    try (Dataset ds = Dataset.open(uri, allocator)) {
      LanceField id =
          ds.getLanceSchema().fields().stream()
              .filter(f -> f.getName().equals("id"))
              .findFirst()
              .orElseThrow(IllegalStateException::new);
      Map<String, String> md = new HashMap<>();
      md.put("lance-schema:unenforced-primary-key", "true");
      md.put("lance-schema:unenforced-primary-key:position", "1");
      Map<Integer, UpdateMap> updates = new HashMap<>();
      updates.put(id.getId(), UpdateMap.builder().updates(md).replace(false).build());
      UpdateConfig cfg = UpdateConfig.builder().fieldMetadataUpdates(updates).build();
      Transaction txn = new Transaction.Builder().readVersion(ds.version()).operation(cfg).build();
      try {
        new CommitBuilder(ds).execute(txn).close();
      } finally {
        txn.close();
      }
    }
  }

  private void nativeMergeInsert(BufferAllocator allocator, String uri) throws Exception {
    try (Dataset ds = Dataset.open(uri, allocator);
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      VarCharVector idVec = (VarCharVector) root.getVector("id");
      idVec.allocateNew();
      for (int i = 0; i < KEYS.length; i++) {
        idVec.setSafe(i, new Text(KEYS[i]));
      }
      idVec.setValueCount(KEYS.length);
      root.setRowCount(KEYS.length);
      try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
          SingleBatchArrowReader reader = new SingleBatchArrowReader(allocator, root)) {
        Data.exportArrayStream(allocator, reader, stream);
        MergeInsertParams params =
            new MergeInsertParams(Arrays.asList("id"))
                .withMatchedUpdateAll()
                .withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);
        ds.mergeInsert(params, stream);
      }
    }
  }
}
