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
package org.lance.spark.read;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Unit tests for {@link LanceStatsKeys#computeSchemaHash}. The hash is the sole guard against
 * schema drift between {@code ANALYZE TABLE} and a subsequent scan, so a regression in its
 * sensitivity to column changes silently feeds stale stats to Spark's CBO.
 */
class LanceAnalyzeTableSchemaHashTest {

  private static StructType schema(StructField... fields) {
    return new StructType(fields);
  }

  @Test
  @DisplayName("identical schemas produce identical hashes (deterministic)")
  void sameSchemaSameHash() {
    StructType a =
        schema(
            new StructField("id", DataTypes.LongType, false, null),
            new StructField("name", DataTypes.StringType, true, null));
    StructType b =
        schema(
            new StructField("id", DataTypes.LongType, false, null),
            new StructField("name", DataTypes.StringType, true, null));
    assertEquals(LanceStatsKeys.computeSchemaHash(a), LanceStatsKeys.computeSchemaHash(b));
  }

  @Test
  @DisplayName("hash is stable across repeated calls on same schema (no JVM-salted hashCode reuse)")
  void hashIsStableAcrossCalls() {
    StructType s = schema(new StructField("c", DataTypes.IntegerType, true, null));
    String h1 = LanceStatsKeys.computeSchemaHash(s);
    String h2 = LanceStatsKeys.computeSchemaHash(s);
    assertEquals(h1, h2);
  }

  @Test
  @DisplayName("different field order produces different hash")
  void fieldOrderChangeDifferentHash() {
    StructType ordered =
        schema(
            new StructField("a", DataTypes.LongType, true, null),
            new StructField("b", DataTypes.StringType, true, null));
    StructType reordered =
        schema(
            new StructField("b", DataTypes.StringType, true, null),
            new StructField("a", DataTypes.LongType, true, null));
    assertNotEquals(
        LanceStatsKeys.computeSchemaHash(ordered), LanceStatsKeys.computeSchemaHash(reordered));
  }

  @Test
  @DisplayName("renaming a column produces different hash")
  void renameDifferentHash() {
    StructType before = schema(new StructField("foo", DataTypes.IntegerType, true, null));
    StructType after = schema(new StructField("bar", DataTypes.IntegerType, true, null));
    assertNotEquals(
        LanceStatsKeys.computeSchemaHash(before), LanceStatsKeys.computeSchemaHash(after));
  }

  @Test
  @DisplayName("retyping a column produces different hash")
  void retypeDifferentHash() {
    StructType before = schema(new StructField("c", DataTypes.IntegerType, true, null));
    StructType after = schema(new StructField("c", DataTypes.LongType, true, null));
    assertNotEquals(
        LanceStatsKeys.computeSchemaHash(before), LanceStatsKeys.computeSchemaHash(after));
  }

  @Test
  @DisplayName("flipping nullable produces different hash")
  void nullabilityFlipDifferentHash() {
    StructType nullable = schema(new StructField("c", DataTypes.IntegerType, true, null));
    StructType notNull = schema(new StructField("c", DataTypes.IntegerType, false, null));
    assertNotEquals(
        LanceStatsKeys.computeSchemaHash(nullable), LanceStatsKeys.computeSchemaHash(notNull));
  }

  @Test
  @DisplayName("empty schema produces a stable, non-empty hash")
  void emptySchemaProducesHash() {
    String empty = LanceStatsKeys.computeSchemaHash(new StructType());
    // SHA-256 hex is 64 chars; an empty schema produces SHA-256(""), the well-known constant.
    assertEquals(64, empty.length());
    assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", empty);
  }

  @Test
  @DisplayName("hash is 64 hex characters (SHA-256)")
  void hashIsSha256Hex() {
    String h =
        LanceStatsKeys.computeSchemaHash(
            schema(new StructField("c", DataTypes.IntegerType, true, null)));
    assertEquals(64, h.length());
    for (char ch : h.toCharArray()) {
      boolean isHex = (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
      org.junit.jupiter.api.Assertions.assertTrue(isHex, "Expected hex char, got: " + ch);
    }
  }
}
