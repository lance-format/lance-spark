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

import org.lance.namespace.model.JsonArrowSchema;
import org.lance.spark.utils.JsonArrowSchemaConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public class JsonUtils {
  private static ObjectMapper mapper = new ObjectMapper();

  public static List<PartitionDescriptor> decodePartitionDescriptors(String partitionColumnsJson)
      throws JsonProcessingException {
    List<PartitionDescriptor> partitionDescriptors =
        mapper.readValue(partitionColumnsJson, new TypeReference<>() {});
    return partitionDescriptors;
  }

  public static String encodePartitionDescriptors(List<PartitionDescriptor> partitionDescriptors)
      throws JsonProcessingException {
    String partitionColumnsJson = mapper.writeValueAsString(partitionDescriptors);
    return partitionColumnsJson;
  }

  public static Schema decodeJsonArrowSchema(String arrowSchemaJson)
      throws JsonProcessingException {
    JsonArrowSchema schema = mapper.readValue(arrowSchemaJson, JsonArrowSchema.class);
    Schema arrowSchema = JsonArrowSchemaConverter.convertToArrowSchema(schema);
    return arrowSchema;
  }

  public static String encodeArrowSchema(Schema arrowSchema) throws JsonProcessingException {
    JsonArrowSchema schema = JsonArrowSchemaConverter.convertToJsonArrowSchema(arrowSchema);
    return mapper.writeValueAsString(schema);
  }
}
