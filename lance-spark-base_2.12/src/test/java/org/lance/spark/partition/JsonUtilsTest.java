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
package org.lance.spark.partition;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonUtilsTest {
  @Test
  public void testPartitionDescriptors() throws Exception {
    List<PartitionDescriptor> original = new ArrayList<>();

    PartitionDescriptor root = PartitionDescriptor.root();
    original.add(root);

    Map<String, String> identityProps = new HashMap<>();
    original.add(PartitionDescriptor.of("country", "identity", identityProps));

    Map<String, String> bucketProps = new HashMap<>();
    bucketProps.put("num-buckets", "32");
    original.add(PartitionDescriptor.of("user_id", "bucket", bucketProps));

    String json = JsonUtils.encodePartitionDescriptors(original);

    // Validate that the JSON can be parsed back to partition descriptors with the same fields.
    List<PartitionDescriptor> decoded = JsonUtils.decodePartitionDescriptors(json);
    assertEquals(original.size(), decoded.size());
    for (int i = 0; i < original.size(); i++) {
      PartitionDescriptor expected = original.get(i);
      PartitionDescriptor actual = decoded.get(i);

      assertEquals(expected.getName(), actual.getName());
      assertEquals(expected.getFunction(), actual.getFunction());

      Map<String, String> expectedProps = expected.getProperties();
      Map<String, String> actualProps = actual.getProperties();
      if (expectedProps == null || expectedProps.isEmpty()) {
        assertTrue(actualProps == null || actualProps.isEmpty());
      } else {
        assertEquals(expectedProps, actualProps);
      }
    }

    // Round-trip through JsonUtils.decodePartitionDescriptors (without inserting root).
    List<PartitionDescriptor> roundTrip = JsonUtils.decodePartitionDescriptors(json);
    assertEquals(original.size(), roundTrip.size());
    for (int i = 0; i < original.size(); i++) {
      PartitionDescriptor expected = original.get(i);
      PartitionDescriptor actual = roundTrip.get(i);

      assertEquals(expected.getName(), actual.getName());
      assertEquals(expected.getFunction(), actual.getFunction());

      Map<String, String> expectedProps = expected.getProperties();
      Map<String, String> actualProps = actual.getProperties();
      if (expectedProps == null || expectedProps.isEmpty()) {
        assertTrue(actualProps == null || actualProps.isEmpty());
      } else {
        assertEquals(expectedProps, actualProps);
      }
    }
  }

  @Test
  public void testPartitionDescriptorsEmptyList() throws JsonProcessingException {
    String json = "[]";
    List<PartitionDescriptor> withoutRoot = JsonUtils.decodePartitionDescriptors(json);
    assertNotNull(withoutRoot);
    assertTrue(withoutRoot.isEmpty());
  }

  @Test
  public void testDecodePartitionDescriptorsInvalidJson() {
    String invalidJson = "not a json";

    assertThrows(
        JsonProcessingException.class, () -> JsonUtils.decodePartitionDescriptors(invalidJson));
  }

  @Test
  public void testEncodeDecodeArrowSchemaRoundTrip() throws JsonProcessingException {
    StructType sparkSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("score", DataTypes.DoubleType, true)
            });

    Schema arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false);

    String json = JsonUtils.encodeArrowSchema(arrowSchema);
    Schema decoded = JsonUtils.decodeJsonArrowSchema(json);

    assertEquals(arrowSchema, decoded);
    assertEquals(arrowSchema.getFields().size(), decoded.getFields().size());
    for (int i = 0; i < arrowSchema.getFields().size(); i++) {
      assertEquals(arrowSchema.getFields().get(i).getName(), decoded.getFields().get(i).getName());
      assertEquals(arrowSchema.getFields().get(i).getType(), decoded.getFields().get(i).getType());
    }
  }

  @Test
  public void testDecodeJsonArrowSchemaInvalidJson() {
    String invalidJson = "not a json";

    assertThrows(JsonProcessingException.class, () -> JsonUtils.decodeJsonArrowSchema(invalidJson));
  }
}
