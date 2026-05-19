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

import org.lance.Dataset;
import org.lance.memwal.InitializeMemWalParams;
import org.lance.memwal.MemWalIndexDetails;
import org.lance.memwal.ShardingSpec;
import org.lance.schema.LanceField;
import org.lance.spark.LanceConstant;
import org.lance.spark.partition.PartitionTransform;

import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Converts between Spark partition transforms and Lance MemWAL sharding metadata. */
public final class PartitionTransformUtil {
  private PartitionTransformUtil() {}

  /** Converts Spark partition transforms into the internal partition transform model. */
  public static List<PartitionTransform> toSpec(Transform[] transforms) {
    if (transforms == null || transforms.length == 0) {
      return Collections.emptyList();
    }

    List<PartitionTransform> spec = new ArrayList<>();

    for (Transform t : transforms) {
      if (t instanceof BucketTransform) {
        BucketTransform bt = (BucketTransform) t;
        int numBuckets = (int) bt.numBuckets().value();
        if (numBuckets <= 0) {
          throw new IllegalArgumentException(
              "Number of buckets must be positive, got: " + numBuckets);
        }
        List<NamedReference> cols = JavaConverters.seqAsJavaList(bt.columns());
        if (cols.size() > 1) {
          throw new UnsupportedOperationException(
              "Lance only supports bucketing on a single" + " column, got: " + cols);
        }
        String colName = String.join(".", cols.get(0).fieldNames());
        spec.add(new PartitionTransform.Bucket(colName, numBuckets));
      } else if (t instanceof IdentityTransform) {
        IdentityTransform it = (IdentityTransform) t;
        String colName = String.join(".", it.ref().fieldNames());
        spec.add(new PartitionTransform.Identity(colName));
      } else {
        throw new UnsupportedOperationException(
            "Unsupported partition transform: "
                + t.describe()
                + ". Lance supports bucket(N, col)"
                + " and identity(col).");
      }
    }

    return spec;
  }

  /**
   * Parses partition spec from MemWAL index metadata, falling back to older partition properties
   * for backward compatibility.
   */
  public static List<PartitionTransform> parseSpec(
      Dataset dataset, Map<String, String> tableProperties) {
    Optional<MemWalIndexDetails> details = dataset.memWalIndexDetails();
    if (details.isPresent() && !details.get().shardingSpecs().isEmpty()) {
      return fromMemWalIndexDetails(dataset, details.get());
    }
    return parseSpec(tableProperties);
  }

  /** Parses legacy partition spec table properties for backward compatibility. */
  public static List<PartitionTransform> parseSpec(Map<String, String> tableProperties) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return Collections.emptyList();
    }

    String shardingSpecJson = tableProperties.get(LanceConstant.TABLE_OPT_SHARDING_SPEC);
    if (shardingSpecJson != null && !shardingSpecJson.trim().isEmpty()) {
      return PartitionTransform.fromShardingSpecJson(shardingSpecJson);
    }

    // Fall back to the previous custom unified spec.
    String specJson = tableProperties.get(LanceConstant.TABLE_OPT_PARTITION_SPEC);
    if (specJson != null && !specJson.trim().isEmpty()) {
      return PartitionTransform.fromJsonString(specJson);
    }

    // Fall back to legacy properties
    List<PartitionTransform> spec = new ArrayList<>();

    String bucketCol = tableProperties.get(LanceConstant.TABLE_OPT_BUCKET_COLUMNS);
    String bucketNumStr = tableProperties.get(LanceConstant.TABLE_OPT_BUCKET_NUM_BUCKETS);
    if (bucketCol != null
        && !bucketCol.trim().isEmpty()
        && bucketNumStr != null
        && !bucketNumStr.trim().isEmpty()) {
      int numBuckets = Integer.parseInt(bucketNumStr.trim());
      if (numBuckets > 0) {
        spec.add(new PartitionTransform.Bucket(bucketCol.trim(), numBuckets));
        return spec;
      }
    }

    String partCol = tableProperties.get(LanceConstant.TABLE_OPT_PARTITION_COLUMNS);
    if (partCol != null && !partCol.trim().isEmpty()) {
      spec.add(new PartitionTransform.Identity(partCol.trim()));
    }

    return spec;
  }

  public static void initializeMemWal(Dataset dataset, List<PartitionTransform> spec) {
    if (spec == null || spec.isEmpty() || dataset.memWalIndexDetails().isPresent()) {
      return;
    }
    if (spec.size() > 1) {
      throw new UnsupportedOperationException(
          "Lance MemWAL sharding supports one partition transform, got: " + spec.size());
    }

    PartitionTransform transform = spec.get(0);
    InitializeMemWalParams params = new InitializeMemWalParams();
    if (transform instanceof PartitionTransform.Bucket) {
      PartitionTransform.Bucket bucket = (PartitionTransform.Bucket) transform;
      params.withBucketSharding(bucket.getCol(), bucket.getNumBuckets());
    } else if (transform instanceof PartitionTransform.Identity) {
      params.withIdentitySharding(transform.getCol());
    } else {
      throw new UnsupportedOperationException(
          "Unsupported MemWAL sharding transform: " + transform.getTransform());
    }
    dataset.initializeMemWal(params);
  }

  private static List<PartitionTransform> fromMemWalIndexDetails(
      Dataset dataset, MemWalIndexDetails details) {
    List<PartitionTransform> spec = new ArrayList<>();
    for (ShardingSpec shardingSpec : details.shardingSpecs()) {
      spec.addAll(
          PartitionTransform.fromShardingSpec(
              shardingSpec,
              sourceId -> columnNameByFieldId(dataset.getLanceSchema().fields(), sourceId)));
    }
    return spec;
  }

  private static String columnNameByFieldId(List<LanceField> fields, int fieldId) {
    for (LanceField field : fields) {
      String name = columnNameByFieldId(field, fieldId, "");
      if (name != null) {
        return name;
      }
    }
    return null;
  }

  private static String columnNameByFieldId(LanceField field, int fieldId, String prefix) {
    String fullName = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
    if (field.getId() == fieldId) {
      return fullName;
    }
    for (LanceField child : field.getChildren()) {
      String name = columnNameByFieldId(child, fieldId, fullName);
      if (name != null) {
        return name;
      }
    }
    return null;
  }
}
