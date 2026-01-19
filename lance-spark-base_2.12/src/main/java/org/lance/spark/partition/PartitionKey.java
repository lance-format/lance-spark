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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static org.lance.spark.partition.PartitionUtils.partitionValueWithCol;

/** Composite key for partition raw values in fixed column order. */
public class PartitionKey {
  private final ImmutableList<PartitionDescriptor> partitionDescriptors;
  private final ImmutableList<String> rawValues;

  private PartitionKey(List<PartitionDescriptor> partitionDescriptors, List<String> rawValues) {
    Preconditions.checkArgument(partitionDescriptors.size() == rawValues.size());
    this.partitionDescriptors = ImmutableList.copyOf(partitionDescriptors);
    this.rawValues = ImmutableList.copyOf(rawValues);
  }

  public static PartitionKey of(
      List<PartitionDescriptor> partitionDescriptors, List<String> values) {
    return new PartitionKey(partitionDescriptors, values);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PartitionKey other = (PartitionKey) obj;
    return rawValues.equals(other.rawValues);
  }

  @Override
  public int hashCode() {
    return rawValues.hashCode();
  }

  /**
   * Get partition id with input size.
   *
   * <p>Partition id is a namespace identifier in format 'col1=value1.col2=value2...coln=valueN'.
   * The column is the corresponding column name, the partition value is computed from raw value by
   * partition function.
   */
  public List<String> getPartitionId(int size) {
    Preconditions.checkArgument(
        size >= 0 && size <= size(), "size must in range [0, " + size + "]");
    List<String> newId = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      PartitionDescriptor pd = partitionDescriptors.get(i);
      String col = pd.getName();
      String rawValue = rawValues.get(i);
      String value = pd.getPartitionFunction().parse(rawValue);
      newId.add(partitionValueWithCol(col, value));
    }
    return newId;
  }

  public int size() {
    return rawValues.size();
  }

  public String value(int index) {
    Preconditions.checkArgument(
        index >= 0 && index < size(), "index must in range [0, " + size() + ")");
    return rawValues.get(index);
  }

  public String lastValue() {
    return rawValues.get(rawValues.size() - 1);
  }

  public String partitionValue(int index) {
    Preconditions.checkArgument(
        index >= 0 && index < size(), "index must in range [0, " + size() + ")");
    PartitionFunction function = partitionDescriptors.get(index).getPartitionFunction();
    return function.parse(value(index));
  }

  public PartitionDescriptor partitionDescriptor(int index) {
    Preconditions.checkArgument(
        index >= 0 && index < size(), "index must in range [0, " + size() + ")");
    return partitionDescriptors.get(index);
  }
}
