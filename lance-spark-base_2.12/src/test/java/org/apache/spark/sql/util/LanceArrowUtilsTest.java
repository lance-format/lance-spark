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
package org.apache.spark.sql.util;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LanceArrowUtilsTest {
  @Test
  public void testTimestampTimezoneSurvivesSchemaRoundtripThroughSparkMetadata() {
    Schema arrowSchema =
        new Schema(
            List.of(
                new Field(
                    "value",
                    FieldType.nullable(
                        new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Asia/Shanghai")),
                    Collections.emptyList())));

    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(arrowSchema);
    Schema converted = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false);
    ArrowType.Timestamp timestampType =
        (ArrowType.Timestamp) converted.findField("value").getType();

    assertEquals("Asia/Shanghai", timestampType.getTimezone());
  }
}
