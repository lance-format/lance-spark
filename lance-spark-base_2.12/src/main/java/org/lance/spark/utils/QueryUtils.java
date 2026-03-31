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
package org.lance.spark.utils;

import org.lance.ipc.Query;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.util.Optional;

public class QueryUtils {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    SimpleModule module = new SimpleModule();
    module.addSerializer(Query.class, new QuerySerializer());
    MAPPER.registerModule(module);
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    MAPPER.addMixIn(Query.class, QueryMixin.class);
    MAPPER.addMixIn(Query.Builder.class, QueryBuilderMixin.class);
  }

  private QueryUtils() {}

  public static String queryToString(Query query) {
    if (query == null) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(query);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize query", e);
    }
  }

  public static Query stringToQuery(String json) {
    if (json == null) {
      return null;
    }
    try {
      return MAPPER.readValue(json, Query.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize query", e);
    }
  }

  private static class QuerySerializer extends JsonSerializer<Query> {
    @Override
    public void serialize(Query value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      if (value.getColumn() != null) {
        gen.writeStringField("column", value.getColumn());
      }
      gen.writeNumberField("k", value.getK());
      if (value.getKey() != null) {
        gen.writeFieldName("key");
        float[] key = value.getKey();
        gen.writeStartArray();
        for (float f : key) {
          gen.writeNumber(f);
        }
        gen.writeEndArray();
      }
      gen.writeNumberField("minimumNprobes", value.getMinimumNprobes());

      writeOptional(gen, "maximumNprobes", value.getMaximumNprobes());
      writeOptional(gen, "ef", value.getEf());
      writeOptional(gen, "refineFactor", value.getRefineFactor());

      gen.writeBooleanField("useIndex", value.isUseIndex());
      writeOptional(gen, "distanceType", value.getDistanceType());
      gen.writeEndObject();
    }

    private void writeOptional(JsonGenerator gen, String fieldName, Optional<?> opt)
        throws IOException {
      if (opt != null && opt.isPresent()) {
        gen.writeObjectField(fieldName, opt.get());
      }
    }
  }

  @JsonDeserialize(builder = Query.Builder.class)
  private abstract static class QueryMixin {}

  @JsonPOJOBuilder(withPrefix = "set")
  private abstract static class QueryBuilderMixin {}
}
