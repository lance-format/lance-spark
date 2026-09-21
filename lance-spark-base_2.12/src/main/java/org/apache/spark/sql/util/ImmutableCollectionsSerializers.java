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
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.esotericsoftware.kryo.util.MapReferenceResolver;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
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
 * rebuild the collection through its public factory ({@code List.copyOf}/{@code Set.copyOf}/{@code
 * Map.copyOf}), preserving immutability. No {@code java.base}-internal reflection is used, so no
 * {@code --add-opens} is required, and immutable sub-lists are handled too because the collection
 * is rebuilt by iteration rather than reconstructed in place. A valid immutable collection never
 * holds {@code null}, and a {@code null} smuggled in by a malformed payload is rejected by the
 * {@code ArrayDeque} staging buffer and by {@code copyOf}, so no Kryo element-null flag is set (it
 * would be inert here anyway: with no per-element serializer, Kryo takes its {@code
 * writeClassAndObject}/{@code readClassAndObject} path, which never consults those flags).
 *
 * <p><b>Reference identity.</b> Rebuilding a collection means the value returned to callers (the
 * immutable copy) is not the object Kryo registered while reading (a mutable staging buffer). Left
 * alone -- as in Kryo 5's own port -- a back-reference to an aliased collection would resolve to
 * that throwaway buffer, so a decoded alias could be a mutable staging collection (or the wrong
 * type for a set/map). {@link ImmutableAwareReferenceResolver} closes that gap: each serializer
 * arms the resolver immediately before delegating to {@code super.read}, which lets it capture the
 * id Kryo assigns to the collection and, once the immutable copy exists, swap it in so every later
 * back-reference resolves to the immutable value. Aliased collections therefore round-trip to the
 * same immutable instance.
 *
 * <p><b>Cyclic graphs.</b> A pure-immutable cycle is impossible (an immutable collection cannot
 * contain itself -- it does not exist yet when its elements are chosen). A cycle can only form
 * through a mutable object in the loop. When the immutable collection is <em>not</em> the object
 * the cycle re-enters, it is fully supported (the mutable member is registered before its fields
 * are read, so the back-reference resolves correctly). The one unsupportable shape is an immutable
 * collection that a back-reference re-enters <em>while it is still being read</em> -- its immutable
 * copy does not exist yet, so only the mutable buffer could be handed out. That case is detected
 * and rejected with a clear {@link KryoException} rather than silently leaking the buffer.
 */
public final class ImmutableCollectionsSerializers {

  private ImmutableCollectionsSerializers() {}

  /** Registers the immutable-collection serializers and the identity-preserving resolver. */
  public static void register(Kryo kryo) {
    // Reach the package-private abstract bases via getSuperclass so no java.base-internal class is
    // named in source. List/Set/Map are distinct hierarchies, each rebuilt through its own factory;
    // registering on the base covers every size variant (List12/ListN, Set12/SetN, Map1/MapN) and
    // sub-lists. copyOf(empty*) sidesteps the overloaded of() factories.
    Class<?> immutableList = List.copyOf(Collections.emptyList()).getClass().getSuperclass();
    Class<?> immutableSet = Set.copyOf(Collections.emptySet()).getClass().getSuperclass();
    Class<?> immutableMap = Map.of().getClass().getSuperclass();
    // MapReferenceResolver is Kryo's default resolver, so subclassing it keeps all standard
    // reference behavior for every other type; only the immutable-collection ids are rewritten.
    ImmutableAwareReferenceResolver resolver = new ImmutableAwareReferenceResolver();
    kryo.setReferenceResolver(resolver);
    kryo.addDefaultSerializer(immutableList, new ImmutableListSerializer(resolver));
    kryo.addDefaultSerializer(immutableSet, new ImmutableSetSerializer(resolver));
    kryo.addDefaultSerializer(immutableMap, new ImmutableMapSerializer(resolver));
  }

  /** Reads elements onto a mutable buffer through Kryo, then rebuilds an immutable {@code List}. */
  private static final class ImmutableListSerializer extends CollectionSerializer {
    private final ImmutableAwareReferenceResolver resolver;

    ImmutableListSerializer(ImmutableAwareReferenceResolver resolver) {
      this.resolver = resolver;
    }

    @Override
    protected Collection create(Kryo kryo, Input input, Class<Collection> type) {
      // ArrayDeque, not ArrayList: CollectionSerializer.read calls ArrayList.ensureCapacity
      // with the wire-supplied length before any element is read, so a malformed huge length
      // would eagerly allocate a giant array. decode() is reachable from caller-controlled write
      // options, so a non-ArrayList buffer avoids that pre-allocation; ArrayDeque keeps order.
      return new ArrayDeque<>();
    }

    @Override
    public Collection read(Kryo kryo, Input input, Class<Collection> type) {
      resolver.arm();
      Collection buffer = super.read(kryo, input, type);
      Collection immutable = buffer == null ? null : List.copyOf(buffer);
      resolver.finish(immutable);
      return immutable;
    }
  }

  /** Reads elements onto a mutable buffer through Kryo, then rebuilds an immutable {@code Set}. */
  private static final class ImmutableSetSerializer extends CollectionSerializer {
    private final ImmutableAwareReferenceResolver resolver;

    ImmutableSetSerializer(ImmutableAwareReferenceResolver resolver) {
      this.resolver = resolver;
    }

    @Override
    protected Collection create(Kryo kryo, Input input, Class<Collection> type) {
      // ArrayDeque for the same reason as the list serializer: avoid ArrayList.ensureCapacity
      // pre-allocating from a hostile wire length. Element order is irrelevant to a set.
      return new ArrayDeque<>();
    }

    @Override
    public Collection read(Kryo kryo, Input input, Class<Collection> type) {
      resolver.arm();
      Collection buffer = super.read(kryo, input, type);
      Collection immutable = buffer == null ? null : Set.copyOf(buffer);
      resolver.finish(immutable);
      return immutable;
    }
  }

  /** Reads entries onto a mutable buffer through Kryo, then rebuilds an immutable {@code Map}. */
  private static final class ImmutableMapSerializer extends MapSerializer {
    private final ImmutableAwareReferenceResolver resolver;

    ImmutableMapSerializer(ImmutableAwareReferenceResolver resolver) {
      this.resolver = resolver;
    }

    @Override
    protected Map create(Kryo kryo, Input input, Class<Map> type) {
      return new HashMap<>();
    }

    @Override
    public Map read(Kryo kryo, Input input, Class<Map> type) {
      resolver.arm();
      Map buffer = super.read(kryo, input, type);
      Map immutable = buffer == null ? null : Map.copyOf(buffer);
      resolver.finish(immutable);
      return immutable;
    }
  }

  /**
   * A {@link MapReferenceResolver} that lets the immutable-collection serializers swap the mutable
   * staging buffer Kryo registered while reading for the final immutable value, so back-references
   * resolve to the immutable value returned to callers rather than the throwaway buffer.
   *
   * <p>Protocol: a serializer calls {@link #arm()} just before {@code super.read}. Kryo's {@code
   * CollectionSerializer} / {@code MapSerializer} register the collection (via {@code
   * kryo.reference}) right after {@code create()} and before reading elements, so the first {@link
   * #setReadObject} that follows {@code arm()} is the collection's own registration; its id is
   * captured. Nested reads re-arm and capture their own ids, forming a LIFO stack that matches read
   * nesting. After the immutable copy is built the serializer calls {@link #finish(Object)} to
   * rewrite that id. Until then the id is "pending"; a back-reference that resolves a pending id
   * means the collection is being re-entered mid-read (an unsupportable cycle), which {@link
   * #getReadObject} rejects.
   */
  static final class ImmutableAwareReferenceResolver extends MapReferenceResolver {
    private final Deque<Integer> pendingStack = new ArrayDeque<>();
    private final Set<Integer> pendingIds = new HashSet<>();
    private boolean armed;

    /** Arm the resolver so the next registered object's id is captured as a pending collection. */
    void arm() {
      armed = true;
    }

    /** Rewrite the just-read collection's reference id to point at its immutable copy. */
    void finish(Object immutable) {
      if (armed) {
        // super.read registered no reference for this collection (e.g. references disabled), so
        // there is nothing to rewrite; just clear the arm.
        armed = false;
        return;
      }
      if (pendingStack.isEmpty()) {
        return;
      }
      int id = pendingStack.pop();
      pendingIds.remove(id);
      if (immutable != null) {
        setReadObject(id, immutable);
      }
    }

    @Override
    public void setReadObject(int id, Object object) {
      super.setReadObject(id, object);
      if (armed) {
        pendingStack.push(id);
        pendingIds.add(id);
        armed = false;
      }
    }

    @Override
    public Object getReadObject(Class type, int id) {
      if (pendingIds.contains(id)) {
        throw new KryoException(
            "cyclic reference through an immutable collection is not supported: the collection is "
                + "referenced again while it is still being deserialized, before its immutable "
                + "value exists");
      }
      return super.getReadObject(type, id);
    }

    @Override
    public void reset() {
      super.reset();
      pendingStack.clear();
      pendingIds.clear();
      armed = false;
    }
  }
}
