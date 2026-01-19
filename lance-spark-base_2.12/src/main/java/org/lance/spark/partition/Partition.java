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

import org.lance.spark.LanceDataset;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.arrow.util.Preconditions;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Leveled Physical Partition */
public class Partition {
  private final String col; /* partition column name */
  private final String value; /* partition value */
  private final PartitionFunction partitionFunction; /* partition function */
  private final Map<String, Partition> children; /* children partitions for inner partition */
  private final LanceDataset dataset; /* dataset for leaf partition */

  private Partition(
      String col,
      String value,
      PartitionFunction partitionFunction,
      Map<String, Partition> partitions,
      LanceDataset dataset) {
    // Either children partitions or dataset exists
    Preconditions.checkArgument(
        (partitions == null && dataset != null) || (partitions != null && dataset == null));
    this.col = col;
    this.value = value;
    this.partitionFunction = partitionFunction;
    this.children = partitions;
    this.dataset = dataset;
  }

  // Currently we only support value type as string.
  public String value() {
    return value;
  }

  public String columnName() {
    return col;
  }

  /** Transform raw value to partition value. */
  public String partitionValue(String value) {
    return partitionFunction.parse(value);
  }

  public boolean isLeaf() {
    return dataset != null;
  }

  public LanceDataset dataset() {
    return dataset;
  }

  public List<Partition> childrenOrEmpty() {
    return children == null ? Lists.newArrayList() : Lists.newArrayList(children.values());
  }

  /** Collect all partition column names in the tree. */
  public Set<String> columnNames() {
    Set<String> cols = new HashSet<>();
    collectColumnNamesRec(this, cols);
    // Exclude artificial root
    cols.remove("root");
    return cols;
  }

  private static void collectColumnNamesRec(Partition p, Set<String> cols) {
    if (p == null) return;
    cols.add(p.columnName());
    for (Partition c : p.childrenOrEmpty()) {
      collectColumnNamesRec(c, cols);
    }
  }

  /** Return all leaf datasets under this partition. */
  public List<LanceDataset> leaves() {
    List<LanceDataset> out = Lists.newArrayList();
    collectLeavesRec(this, out);
    return out;
  }

  private static void collectLeavesRec(Partition p, List<LanceDataset> out) {
    if (p == null) return;
    if (p.isLeaf()) {
      out.add(p.dataset());
      return;
    }
    for (Partition c : p.childrenOrEmpty()) {
      collectLeavesRec(c, out);
    }
  }

  /** Resolve physical partition from input key, create if not exists */
  public Partition resolveOrCreate(PartitionKey key, PartitionCreator partitionCreator) {
    return innerResolveOrCreate(key, 0, partitionCreator);
  }

  private Partition innerResolveOrCreate(
      PartitionKey key, int index, PartitionCreator partitionCreator) {
    // Resolve leaf partition.
    if (index == key.size()) {
      String rawValue = key.lastValue();
      Preconditions.checkArgument(
          partitionValue(rawValue).equals(value),
          "Partition key value {}/{} doesn't equals to partition value {}",
          rawValue,
          partitionValue(rawValue),
          value);
      return this;
    }

    String rawValue = key.value(index);
    Preconditions.checkArgument(
        !isLeaf(),
        "Try resolve child partition raw_value={}, but current partition {}={} has no children",
        rawValue,
        col,
        value);

    // Resolve partition.
    PartitionDescriptor pd = key.partitionDescriptor(index);
    PartitionFunction pFunc = pd.getPartitionFunction();
    String partitionValue = key.partitionValue(index);
    if (children.containsKey(partitionValue)) {
      return children.get(partitionValue).innerResolveOrCreate(key, index + 1, partitionCreator);
    }

    // Partition not found, create new partition.
    String colName = pd.getName();

    Partition newPartition;
    if (index == key.size() - 1) {
      // leaf partition
      List<String> id = key.getPartitionId(index + 1);
      LanceDataset dataset = partitionCreator.createLeafDataset(id);
      newPartition = Partition.leaf(colName, partitionValue, pFunc, dataset);
    } else {
      // inner partition
      List<String> id = key.getPartitionId(index + 1);
      partitionCreator.createInnerNamespace(id);
      newPartition = Partition.inner(colName, partitionValue, pFunc, Lists.newArrayList());
    }
    children.put(newPartition.value, newPartition);

    return newPartition.innerResolveOrCreate(key, index + 1, partitionCreator);
  }

  /** Get all physical sub-tables that needs to be scanned and filtered. */
  public List<LanceDataset> filter(Filter filter) {
    if (filter instanceof EqualTo) {
      EqualTo eq = (EqualTo) filter;
      String col = eq.attribute();
      String val = partitionValue(String.valueOf(eq.value()));

      if (col.equals(columnName())) {
        if (!val.equals(value)) {
          return Lists.newArrayList();
        } else {
          if (dataset != null) {
            return Lists.asList(dataset, new LanceDataset[0]);
          } else {
            return leaves();
          }
        }
      }
    } else if (filter instanceof In) {
      In in = (In) filter;
      String col = in.attribute();
      List<String> vals =
          Arrays.stream(in.values())
              .map(String::valueOf)
              .map((v) -> partitionValue(v))
              .collect(Collectors.toList());

      if (col.equals(columnName())) {
        if (!vals.contains(value)) {
          return Lists.newArrayList();
        } else {
          if (dataset != null) {
            return Lists.asList(dataset, new LanceDataset[0]);
          } else {
            return leaves();
          }
        }
      }
    } else {
      // TODO: support more filter in partition prune.
    }

    List<LanceDataset> list = Lists.newArrayList();
    if (dataset != null) {
      list.add(dataset);
    } else {
      for (Partition p : childrenOrEmpty()) {
        list.addAll(p.filter(filter));
      }
    }
    return list;
  }

  public static Partition leaf(
      String columnName, String value, PartitionFunction func, LanceDataset dataset) {
    return new Partition(columnName, value, func, null, dataset);
  }

  public static Partition inner(
      String columnName, String value, PartitionFunction func, List<Partition> partitions) {
    Map<String, Partition> partitionMap = Maps.newHashMap();
    for (Partition p : partitions) {
      partitionMap.put(p.value(), p);
    }
    return new Partition(columnName, value, func, partitionMap, null);
  }
}
