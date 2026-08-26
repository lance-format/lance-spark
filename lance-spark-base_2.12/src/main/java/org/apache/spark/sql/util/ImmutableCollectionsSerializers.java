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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kryo serializers for the JDK immutable collections ({@code List.of}/{@code copyOf}, {@code
 * Set.of}, {@code Map.of}), backported from Kryo 5's {@code ImmutableCollectionsSerializers}
 * (absent from the Kryo 4.0.2 that Spark bundles).
 *
 * <p>These collections have no no-arg constructor and keep their elements in a private, {@code
 * java.base}-internal field, so the Objenesis-based instantiator {@link LanceSerializeUtil}
 * configures produces an instance whose backing storage is never populated -- a decoded collection
 * then throws (e.g. {@code UnsupportedOperationException} / NPE) the first time it is used.
 *
 * <p>These serializers deliberately do <b>not</b> delegate to Kryo's {@code JavaSerializer}: that
 * would route every nested element through {@code ObjectInputStream.readObject}, a Java
 * deserialization gadget surface that is dangerous here because {@code LanceSerializeUtil.decode}
 * is reachable from caller-controlled write options (a crafted payload's {@code readObject} would
 * run before the decoded value is even cast). Instead they keep every element on the Kryo path and
 * rebuild the collection through its public factory ({@code List.copyOf}/{@code Set.copyOf}/ {@code
 * Map.copyOf}), preserving immutability. No {@code java.base}-internal reflection is used, so no
 * {@code --add-opens} is required, and immutable sub-lists are handled too because the collection
 * is rebuilt by iteration rather than reconstructed in place.
 */
public final class ImmutableCollectionsSerializers {

  private ImmutableCollectionsSerializers() {}

  /** Registers the immutable-collection serializers on {@code kryo}. */
  public static void register(Kryo kryo) {
    // Reach the package-private abstract bases via getSuperclass so no java.base-internal class is
    // named in source. List/Set/Map are distinct hierarchies, each rebuilt through its own factory;
    // registering on the base covers every size variant (List12/ListN, Set12/SetN, Map1/MapN) and
    // sub-lists. copyOf(empty*) sidesteps the overloaded of() factories.
    Class<?> immutableList = List.copyOf(Collections.emptyList()).getClass().getSuperclass();
    Class<?> immutableSet = Set.copyOf(Collections.emptySet()).getClass().getSuperclass();
    Class<?> immutableMap = Map.of().getClass().getSuperclass();
    kryo.addDefaultSerializer(immutableList, new ImmutableListSerializer());
    kryo.addDefaultSerializer(immutableSet, new ImmutableSetSerializer());
    kryo.addDefaultSerializer(immutableMap, new ImmutableMapSerializer());
  }

  /** Reads elements onto a mutable buffer through Kryo, then rebuilds an immutable {@code List}. */
  private static final class ImmutableListSerializer extends CollectionSerializer {
    ImmutableListSerializer() {
      setElementsCanBeNull(false);
    }

    @Override
    protected Collection create(Kryo kryo, Input input, Class<Collection> type) {
      return new ArrayList<>();
    }

    @Override
    public Collection read(Kryo kryo, Input input, Class<Collection> type) {
      Collection buffer = super.read(kryo, input, type);
      return buffer == null ? null : List.copyOf(buffer);
    }
  }

  /** Reads elements onto a mutable buffer through Kryo, then rebuilds an immutable {@code Set}. */
  private static final class ImmutableSetSerializer extends CollectionSerializer {
    ImmutableSetSerializer() {
      setElementsCanBeNull(false);
    }

    @Override
    protected Collection create(Kryo kryo, Input input, Class<Collection> type) {
      return new ArrayList<>();
    }

    @Override
    public Collection read(Kryo kryo, Input input, Class<Collection> type) {
      Collection buffer = super.read(kryo, input, type);
      return buffer == null ? null : Set.copyOf(buffer);
    }
  }

  /** Reads entries onto a mutable buffer through Kryo, then rebuilds an immutable {@code Map}. */
  private static final class ImmutableMapSerializer extends MapSerializer {
    ImmutableMapSerializer() {
      setKeysCanBeNull(false);
      setValuesCanBeNull(false);
    }

    @Override
    protected Map create(Kryo kryo, Input input, Class<Map> type) {
      return new HashMap<>();
    }

    @Override
    public Map read(Kryo kryo, Input input, Class<Map> type) {
      Map buffer = super.read(kryo, input, type);
      return buffer == null ? null : Map.copyOf(buffer);
    }
  }
}
