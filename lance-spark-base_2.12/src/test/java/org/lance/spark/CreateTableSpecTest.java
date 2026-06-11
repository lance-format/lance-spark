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

import org.lance.spark.utils.BlobUtils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CreateTableSpecTest {

  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("data", DataTypes.BinaryType, true, Metadata.empty()),
          });

  @Test
  public void noBlobColumnsPassesVersionThrough() {
    CreateTableSpec spec = CreateTableSpec.resolve(SCHEMA, Collections.emptyMap(), "2.0");
    assertEquals("2.0", spec.fileFormatVersion());
    assertFalse(BlobUtils.hasBlobV2Fields(spec.schema()));
  }

  @Test
  public void noBlobColumnsAndNoVersionStaysUnset() {
    CreateTableSpec spec = CreateTableSpec.resolve(SCHEMA, Collections.emptyMap(), null);
    assertNull(spec.fileFormatVersion());
  }

  @Test
  public void tablePropertyVersionOverridesCatalogDefault() {
    CreateTableSpec spec =
        CreateTableSpec.resolve(SCHEMA, props("file_format_version", "stable"), "legacy");
    assertEquals("stable", spec.fileFormatVersion());
  }

  @Test
  public void blobV2SchemaWithNoVersionUsesMinimumBlobV2Version() {
    CreateTableSpec spec =
        CreateTableSpec.resolve(blobV2QuerySchema(), Collections.emptyMap(), null);
    assertEquals(BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION, spec.fileFormatVersion());
    assertTrue(BlobUtils.hasBlobV2Fields(spec.schema()));
  }

  @Test
  public void blobV2SchemaHonorsTableVersionThatSupportsIt() {
    CreateTableSpec spec =
        CreateTableSpec.resolve(blobV2QuerySchema(), props("file_format_version", "2.3"), null);
    assertEquals("2.3", spec.fileFormatVersion());
  }

  @Test
  public void blobV2SchemaRejectsExplicitTableVersionBelowMinimum() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CreateTableSpec.resolve(
                    blobV2QuerySchema(), props("file_format_version", "2.0"), null));
    assertTrue(ex.getMessage().contains("2.0"), ex.getMessage());
  }

  @Test
  public void blobV2SchemaUpgradesCatalogDefaultBelowMinimum() {
    CreateTableSpec spec =
        CreateTableSpec.resolve(blobV2QuerySchema(), Collections.emptyMap(), "2.0");
    assertEquals(BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION, spec.fileFormatVersion());
  }

  @Test
  public void blobV2SchemaUpgradesNamedCatalogDefault() {
    CreateTableSpec spec =
        CreateTableSpec.resolve(blobV2QuerySchema(), Collections.emptyMap(), "stable");
    assertEquals(BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION, spec.fileFormatVersion());
  }

  @Test
  public void blobV2SchemaKeepsCatalogDefaultThatSupportsIt() {
    CreateTableSpec spec =
        CreateTableSpec.resolve(blobV2QuerySchema(), Collections.emptyMap(), "2.4");
    assertEquals("2.4", spec.fileFormatVersion());
  }

  @Test
  public void blobPropertyAtSupportedVersionResolvesToBlobV2Metadata() {
    CreateTableSpec spec =
        CreateTableSpec.resolve(
            SCHEMA, props("data.lance.encoding", "blob", "file_format_version", "2.2"), null);
    assertEquals("2.2", spec.fileFormatVersion());
    assertTrue(BlobUtils.isBlobV2SparkField(spec.schema().apply("data")));
  }

  @Test
  public void blobPropertyAtOlderVersionStaysBlobV1() {
    CreateTableSpec spec =
        CreateTableSpec.resolve(
            SCHEMA, props("data.lance.encoding", "blob", "file_format_version", "2.0"), null);
    assertEquals("2.0", spec.fileFormatVersion());
    assertTrue(BlobUtils.isBlobSparkField(spec.schema().apply("data")));
    assertFalse(BlobUtils.isBlobV2SparkField(spec.schema().apply("data")));
  }

  @Test
  public void blobPropertyAtNamedCatalogDefaultStaysBlobV1() {
    CreateTableSpec spec =
        CreateTableSpec.resolve(SCHEMA, props("data.lance.encoding", "blob"), "stable");
    assertEquals("stable", spec.fileFormatVersion());
    assertTrue(BlobUtils.isBlobSparkField(spec.schema().apply("data")));
    assertFalse(BlobUtils.isBlobV2SparkField(spec.schema().apply("data")));
  }

  @Test
  public void blobV2SchemaRejectsExplicitNamedTableVersion() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                CreateTableSpec.resolve(
                    blobV2QuerySchema(), props("file_format_version", "stable"), null));
    assertTrue(ex.getMessage().contains("stable"), ex.getMessage());
  }

  @Test
  public void versionUpgradeReResolvesPropertyDrivenBlobColumns() {
    Metadata v2 =
        new MetadataBuilder()
            .putString(BlobUtils.ARROW_EXTENSION_NAME_KEY, BlobUtils.ARROW_EXTENSION_BLOB_V2)
            .build();
    StructType mixed =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("copied", DataTypes.BinaryType, true, v2),
              new StructField("fresh", DataTypes.BinaryType, true, Metadata.empty()),
            });
    CreateTableSpec spec =
        CreateTableSpec.resolve(mixed, props("fresh.lance.encoding", "blob"), "2.0");
    assertEquals(BlobUtils.MIN_BLOB_V2_FILE_FORMAT_VERSION, spec.fileFormatVersion());
    assertTrue(BlobUtils.isBlobV2SparkField(spec.schema().apply("fresh")));
    assertFalse(BlobUtils.isBlobSparkField(spec.schema().apply("fresh")));
  }

  private static StructType blobV2QuerySchema() {
    Metadata v2 =
        new MetadataBuilder()
            .putString(BlobUtils.ARROW_EXTENSION_NAME_KEY, BlobUtils.ARROW_EXTENSION_BLOB_V2)
            .build();
    return new StructType(
        new StructField[] {
          new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("data", DataTypes.BinaryType, true, v2),
        });
  }

  private static Map<String, String> props(String... keyValues) {
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put(keyValues[i], keyValues[i + 1]);
    }
    return map;
  }
}
