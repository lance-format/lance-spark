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
package org.lance.spark.sharding;

import org.lance.Dataset;
import org.lance.index.scalar.ZoneStats;
import org.lance.memwal.InitializeMemWalParams;
import org.lance.memwal.MemWalIndexDetails;
import org.lance.memwal.ShardingField;
import org.lance.memwal.ShardingSpec;
import org.lance.schema.LanceField;
import org.lance.spark.utils.BucketHashUtil;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Spark-facing adapter for one Lance MemWAL sharding field. */
public abstract class SparkLanceShardingAdapter implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String transform;
  private final String col;

  protected SparkLanceShardingAdapter(String transform, String col) {
    this.transform = transform;
    this.col = col;
  }

  public String getTransform() {
    return transform;
  }

  public String getCol() {
    return col;
  }

  public abstract Expression toSparkExpression();

  public abstract SortOrder toSortOrder();

  public abstract NamedReference toClusteringRef();

  /**
   * Computes the partition key for a fragment from zonemap stats. Returns empty if the fragment is
   * not partition-compatible (e.g. min != max for identity).
   */
  public abstract Optional<Object> fragmentKeyFromZones(List<ZoneStats> zones, int fragmentId);

  /** Builds an {@link InternalRow} partition key from a computed partition value. */
  public InternalRow partitionKeyRow(Object value) {
    Object sparkValue = toSparkValue(value);
    return new GenericInternalRow(new Object[] {sparkValue});
  }

  /**
   * Returns the number of distinct partition values across all fragments, given a fragment→key map.
   */
  public int partitionCount(Map<Integer, Object> fragmentKeys) {
    return fragmentKeys.size();
  }

  private static Object toSparkValue(Object value) {
    if (value instanceof String) {
      return UTF8String.fromString((String) value);
    }
    return value;
  }

  /** Identity sharding adapter: data grouped by raw column value. */
  public static final class Identity extends SparkLanceShardingAdapter {
    private static final long serialVersionUID = 1L;

    public Identity(String col) {
      super("identity", col);
    }

    @Override
    public Expression toSparkExpression() {
      return FieldReference.apply(getCol());
    }

    @Override
    public SortOrder toSortOrder() {
      return Expressions.sort(
          Expressions.column(getCol()), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    }

    @Override
    public NamedReference toClusteringRef() {
      return Expressions.column(getCol());
    }

    @Override
    public Optional<Object> fragmentKeyFromZones(List<ZoneStats> zones, int fragmentId) {
      Comparable<?> value = null;
      for (ZoneStats zone : zones) {
        if (zone.getFragmentId() != fragmentId) {
          continue;
        }
        Comparable<?> min = zone.getMin();
        Comparable<?> max = zone.getMax();
        if (min == null || max == null || !min.equals(max)) {
          return Optional.empty();
        }
        if (value != null && !value.equals(min)) {
          return Optional.empty();
        }
        value = min;
      }
      return Optional.ofNullable(value);
    }
  }

  /** Bucket sharding adapter: data grouped by hash(col) % N. */
  public static final class Bucket extends SparkLanceShardingAdapter {
    private static final long serialVersionUID = 1L;

    private final int numBuckets;

    public Bucket(String col, int numBuckets) {
      super("bucket", col);
      this.numBuckets = numBuckets;
    }

    public int getNumBuckets() {
      return numBuckets;
    }

    @Override
    public Expression toSparkExpression() {
      return Expressions.bucket(numBuckets, getCol());
    }

    @Override
    public SortOrder toSortOrder() {
      // Sort by raw column value; rows with same bucket ID
      // but different values may not be contiguous.
      // TODO: sort by computed bucket ID for optimal
      // fragment sizing.
      return Expressions.sort(
          Expressions.column(getCol()), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    }

    @Override
    public NamedReference toClusteringRef() {
      return Expressions.column(getCol());
    }

    @Override
    public Optional<Object> fragmentKeyFromZones(List<ZoneStats> zones, int fragmentId) {
      Comparable<?> value = null;
      for (ZoneStats zone : zones) {
        if (zone.getFragmentId() != fragmentId) {
          continue;
        }
        Comparable<?> min = zone.getMin();
        Comparable<?> max = zone.getMax();
        if (min == null || max == null || !min.equals(max)) {
          return Optional.empty();
        }
        if (value != null && !value.equals(min)) {
          return Optional.empty();
        }
        value = min;
      }
      if (value == null) {
        return Optional.empty();
      }
      int bucketId = BucketHashUtil.computeBucketIdFromValue(value, numBuckets);
      return Optional.of(bucketId);
    }
  }

  public static List<SparkLanceShardingAdapter> fromShardingSpec(
      ShardingSpec shardingSpec, Function<Integer, String> sourceIdToColumn) {
    List<SparkLanceShardingAdapter> spec = new ArrayList<>();
    for (ShardingField field : shardingSpec.fields()) {
      spec.add(fromShardingField(field, sourceIdToColumn));
    }
    return spec;
  }

  /** Converts Spark catalog transforms into Spark-facing Lance sharding adapters. */
  public static List<SparkLanceShardingAdapter> toSpec(Transform[] transforms) {
    if (transforms == null || transforms.length == 0) {
      return Collections.emptyList();
    }

    List<SparkLanceShardingAdapter> spec = new ArrayList<>();

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
              "Lance only supports bucketing on a single column, got: " + cols);
        }
        String colName = String.join(".", cols.get(0).fieldNames());
        spec.add(new Bucket(colName, numBuckets));
      } else if (t instanceof IdentityTransform) {
        IdentityTransform it = (IdentityTransform) t;
        String colName = String.join(".", it.ref().fieldNames());
        spec.add(new Identity(colName));
      } else {
        throw new UnsupportedOperationException(
            "Unsupported Spark sharding adapter input: "
                + t.describe()
                + ". Lance supports bucket(N, col) and identity(col).");
      }
    }

    return spec;
  }

  /** Parses sharding adapters from MemWAL index metadata. */
  public static List<SparkLanceShardingAdapter> parseSpec(Dataset dataset) {
    Optional<MemWalIndexDetails> details = dataset.memWalIndexDetails();
    if (details.isPresent() && !details.get().shardingSpecs().isEmpty()) {
      return fromMemWalIndexDetails(dataset, details.get());
    }
    return Collections.emptyList();
  }

  public static void initializeMemWal(Dataset dataset, List<SparkLanceShardingAdapter> spec) {
    if (spec == null || spec.isEmpty() || dataset.memWalIndexDetails().isPresent()) {
      return;
    }
    if (spec.size() > 1) {
      throw new UnsupportedOperationException(
          "Lance MemWAL sharding supports one Spark sharding adapter, got: " + spec.size());
    }

    SparkLanceShardingAdapter adapter = spec.get(0);
    InitializeMemWalParams params = new InitializeMemWalParams();
    if (adapter instanceof Bucket) {
      Bucket bucket = (Bucket) adapter;
      params.withBucketSharding(bucket.getCol(), bucket.getNumBuckets());
    } else if (adapter instanceof Identity) {
      params.withIdentitySharding(adapter.getCol());
    } else {
      throw new UnsupportedOperationException(
          "Unsupported MemWAL sharding transform: " + adapter.getTransform());
    }
    dataset.initializeMemWal(params);
  }

  private static List<SparkLanceShardingAdapter> fromMemWalIndexDetails(
      Dataset dataset, MemWalIndexDetails details) {
    List<SparkLanceShardingAdapter> spec = new ArrayList<>();
    for (ShardingSpec shardingSpec : details.shardingSpecs()) {
      spec.addAll(
          fromShardingSpec(
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

  private static SparkLanceShardingAdapter fromShardingField(
      ShardingField field, Function<Integer, String> sourceIdToColumn) {
    String transform = field.transform().orElse(null);
    if ("bucket".equals(transform)) {
      String column = columnName(field, sourceIdToColumn);
      String numBuckets = requiredParameter(field, "num_buckets");
      return new Bucket(column, Integer.parseInt(numBuckets));
    } else if ("identity".equals(transform)) {
      return new Identity(columnName(field, sourceIdToColumn));
    }
    throw new UnsupportedOperationException("Unsupported sharding transform: " + transform);
  }

  private static String columnName(
      ShardingField field, Function<Integer, String> sourceIdToColumn) {
    String column = field.parameters().get("column");
    if (column != null && !column.trim().isEmpty()) {
      return column;
    }
    if (!field.sourceIds().isEmpty()) {
      String resolved = sourceIdToColumn.apply(field.sourceIds().get(0));
      if (resolved != null && !resolved.trim().isEmpty()) {
        return resolved;
      }
    }
    throw new IllegalArgumentException(
        "MemWAL sharding field " + field.fieldId() + " missing source column");
  }

  private static String requiredParameter(ShardingField field, String key) {
    String value = field.parameters().get(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "MemWAL sharding field " + field.fieldId() + " missing parameter " + key);
    }
    return value;
  }

  /**
   * Detects partition keys for all fragments from zonemap stats. Returns a map from fragment ID to
   * partition key, or empty if the transform is not compatible with the data.
   */
  public Optional<Map<Integer, Object>> detectFragmentKeys(List<ZoneStats> zones) {
    Map<Integer, Object> result = new HashMap<>();
    // Collect all fragment IDs
    for (ZoneStats zone : zones) {
      result.putIfAbsent(zone.getFragmentId(), null);
    }
    // Compute key per fragment
    for (int fragId : new java.util.ArrayList<>(result.keySet())) {
      Optional<Object> key = fragmentKeyFromZones(zones, fragId);
      if (!key.isPresent()) {
        return Optional.empty();
      }
      result.put(fragId, key.get());
    }
    return Optional.of(result);
  }
}
