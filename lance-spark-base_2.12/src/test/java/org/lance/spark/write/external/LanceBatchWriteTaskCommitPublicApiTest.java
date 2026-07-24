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

import org.lance.FragmentMetadata;
import org.lance.spark.write.LanceBatchWrite;

import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LanceBatchWriteTaskCommitPublicApiTest {
  @Test
  public void testFactoryIsAccessibleAndSerializable() throws Exception {
    FragmentMetadata fragment = new FragmentMetadata(7, Collections.emptyList(), 0L, null, null);

    WriterCommitMessage message = LanceBatchWrite.taskCommit(Collections.singletonList(fragment));
    WriterCommitMessage deserialized = roundTrip(message);

    assertNotNull(deserialized);
    assertEquals(message.getClass(), deserialized.getClass());
  }

  private static WriterCommitMessage roundTrip(WriterCommitMessage message) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(message);
    }
    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (WriterCommitMessage) input.readObject();
    }
  }
}
