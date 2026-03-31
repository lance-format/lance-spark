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

import org.lance.ipc.Query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class LanceSparkReadOptionsSerializationTest {

  @Test
  public void testJavaSerialization() throws IOException, ClassNotFoundException {
    String json = "{\"column\":\"vector_col\",\"k\":10,\"key\":[1.0,2.0,3.0]}";

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").nearest(json).build();

    Query originalQuery = options.getNearest();
    Assertions.assertNotNull(originalQuery);

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(options);
    oos.close();

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    LanceSparkReadOptions deserializedOptions = (LanceSparkReadOptions) ois.readObject();

    Query deserializedQuery = deserializedOptions.getNearest();

    Assertions.assertNotNull(
        deserializedQuery, "Nearest query should not be null after deserialization");
    Assertions.assertEquals(originalQuery.getK(), deserializedQuery.getK());
    Assertions.assertEquals(originalQuery.getColumn(), deserializedQuery.getColumn());
    Assertions.assertArrayEquals(originalQuery.getKey(), deserializedQuery.getKey());
  }

  @Test
  public void testUseIndexSerialization() throws IOException, ClassNotFoundException {
    // Case 1: useIndex is explicitly set to false
    String jsonFalse =
        "{\"column\":\"vector_col\",\"k\":10,\"key\":[1.0,2.0,3.0],\"useIndex\":false}";
    LanceSparkReadOptions optionsFalse =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").nearest(jsonFalse).build();

    Query queryFalse = optionsFalse.getNearest();
    Assertions.assertFalse(queryFalse.isUseIndex());

    // Serialize
    ByteArrayOutputStream baosFalse = new ByteArrayOutputStream();
    ObjectOutputStream oosFalse = new ObjectOutputStream(baosFalse);
    oosFalse.writeObject(optionsFalse);
    oosFalse.close();

    // Deserialize
    ByteArrayInputStream baisFalse = new ByteArrayInputStream(baosFalse.toByteArray());
    ObjectInputStream oisFalse = new ObjectInputStream(baisFalse);
    LanceSparkReadOptions deserializedOptionsFalse = (LanceSparkReadOptions) oisFalse.readObject();

    Assertions.assertFalse(
        deserializedOptionsFalse.getNearest().isUseIndex(),
        "useIndex should remain false after serialization/deserialization");

    // Case 2: useIndex is explicitly set to true
    String jsonTrue =
        "{\"column\":\"vector_col\",\"k\":10,\"key\":[1.0,2.0,3.0],\"useIndex\":true}";
    LanceSparkReadOptions optionsTrue =
        LanceSparkReadOptions.builder().datasetUri("s3://bucket/path").nearest(jsonTrue).build();

    Query queryTrue = optionsTrue.getNearest();
    Assertions.assertTrue(queryTrue.isUseIndex());

    // Serialize
    ByteArrayOutputStream baosTrue = new ByteArrayOutputStream();
    ObjectOutputStream oosTrue = new ObjectOutputStream(baosTrue);
    oosTrue.writeObject(optionsTrue);
    oosTrue.close();

    // Deserialize
    ByteArrayInputStream baisTrue = new ByteArrayInputStream(baosTrue.toByteArray());
    ObjectInputStream oisTrue = new ObjectInputStream(baisTrue);
    LanceSparkReadOptions deserializedOptionsTrue = (LanceSparkReadOptions) oisTrue.readObject();

    Assertions.assertTrue(
        deserializedOptionsTrue.getNearest().isUseIndex(),
        "useIndex should remain true after serialization/deserialization");
  }
}
