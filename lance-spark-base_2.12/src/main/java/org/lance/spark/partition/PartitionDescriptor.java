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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;

import java.util.Map;
import java.util.Objects;

/** Descriptor of a partition identity, including column name, partition function and properties. */
public class PartitionDescriptor {
  @JsonProperty("name")
  private String name;

  @JsonProperty("function")
  private String function;

  @JsonProperty("properties")
  private Map<String, String> properties;

  private PartitionDescriptor() {}

  public static PartitionDescriptor of(
      String name, String function, Map<String, String> properties) {
    PartitionDescriptor descriptor = new PartitionDescriptor();
    descriptor.name = name;
    descriptor.function = function;
    descriptor.properties = properties;
    return descriptor;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getFunction() {
    return function;
  }

  public void setFunction(String function) {
    this.function = function;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  @JsonIgnore
  public PartitionFunction getPartitionFunction() {
    return PartitionFunction.of(this);
  }

  public boolean isPruneAvailable(Filter f) {
    String col;
    if (f instanceof EqualTo) {
      col = ((EqualTo) f).attribute();
    } else if (f instanceof In) {
      col = ((In) f).attribute();
    } else {
      return false;
    }

    if (name.equals(col) && getPartitionFunction().isPruneAvailable(f)) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, function, properties);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o instanceof PartitionDescriptor) {
      PartitionDescriptor other = (PartitionDescriptor) o;
      return this.name.equals(other.name)
          && this.function.equals(other.function)
          && this.properties.equals(other.properties);
    }
    return false;
  }

  public static PartitionDescriptor root() {
    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
    partitionDescriptor.setName("root");
    partitionDescriptor.setFunction("identity");
    partitionDescriptor.setProperties(Maps.newHashMap());
    return partitionDescriptor;
  }
}
