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
package org.lance.spark.write.external;

import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.write.FragmentWriteSpec;
import org.lance.spark.write.LanceBatchWrite;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LanceBatchWriteTaskCommitPublicApiTest {
  @TempDir static Path tempDir;

  @Test
  public void testFragmentWriteSpecIsAccessibleAndSerializable() throws Exception {
    String datasetUri = tempDir.resolve("fragment_write_spec.lance").toString();
    Schema schema =
        new Schema(
            Collections.singletonList(
                new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.create(allocator, datasetUri, schema, new WriteParams.Builder().build()).close();
    }
    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            LanceArrowUtils.fromArrowSchema(schema),
            LanceSparkWriteOptions.from(datasetUri),
            false,
            null,
            null,
            null,
            null,
            false,
            null);

    FragmentWriteSpec spec = TestUtils.roundTrip(batchWrite.getFragmentWriteSpec());

    assertEquals(datasetUri, spec.getDatasetUri());
    assertEquals(Optional.of("APPEND"), spec.toWriteParams().getMode());
  }

  @Test
  public void testFactoryIsAccessibleAndSerializable() throws Exception {
    FragmentMetadata fragment = new FragmentMetadata(7, Collections.emptyList(), 0L, null, null);

    WriterCommitMessage message = LanceBatchWrite.taskCommit(Collections.singletonList(fragment));
    WriterCommitMessage deserialized = TestUtils.roundTrip(message);

    assertNotNull(deserialized);
    assertEquals(message.getClass(), deserialized.getClass());
  }
}
