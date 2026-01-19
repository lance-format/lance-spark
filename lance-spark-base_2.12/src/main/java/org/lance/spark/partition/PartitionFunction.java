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

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;

public interface PartitionFunction {
  /** Parse raw value into the partitioned value. */
  String parse(String value);
  /** Get partition function type. */
  Type getType();
  /** Is partition prune available. */
  boolean isPruneAvailable(Filter filter);

  static PartitionFunction of(PartitionDescriptor partitionDescriptor) {
    String func = partitionDescriptor.getFunction();
    Type type = Type.of(func);
    switch (type) {
      case IDENTITY:
        return new IdentityPartition();
      case BUCKET:
        int bucketSize = Integer.parseInt(partitionDescriptor.getProperties().get("num-buckets"));
        return new BucketPartition(bucketSize);
      default:
        throw new IllegalArgumentException("Unknown partition type: " + func);
    }
  }

  enum Type {
    IDENTITY("identity"),
    BUCKET("bucket");

    private final String value;

    Type(String value) {
      this.value = value;
    }

    public static Type of(String value) {
      for (Type t : Type.values()) {
        if (t.value.equalsIgnoreCase(value)) {
          return t;
        }
      }
      throw new IllegalArgumentException("Unknown partition function type: " + value);
    }
  }

  class IdentityPartition implements PartitionFunction {
    @Override
    public String parse(String value) {
      return value;
    }

    @Override
    public Type getType() {
      return Type.IDENTITY;
    }

    @Override
    public boolean isPruneAvailable(Filter f) {
      return f instanceof EqualTo || f instanceof In;
    }
  }

  class BucketPartition implements PartitionFunction {
    private int bucketSize;

    public BucketPartition(int bucketSize) {
      this.bucketSize = bucketSize;
    }

    @Override
    public String parse(String value) {
      return "" + (value.hashCode() % bucketSize);
    }

    @Override
    public Type getType() {
      return Type.BUCKET;
    }

    @Override
    public boolean isPruneAvailable(Filter f) {
      return f instanceof EqualTo || f instanceof In;
    }
  }
}
