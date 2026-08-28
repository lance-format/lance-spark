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

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests that {@link LanceSerializeUtil}'s Kryo codec roundtrips JDK immutable collections. */
public class LanceSerializeUtilTest {

  private static <T> T roundtrip(T obj) {
    return LanceSerializeUtil.decode(LanceSerializeUtil.encode(obj));
  }

  // The JDK immutable collections (List.of/copyOf, Set.of, Map.of) have no no-arg constructor and
  // keep their storage in a java.base-internal field, so the codec's Objenesis instantiator could
  // not reconstruct them. The assertions below pass once dedicated Kryo serializers rebuild the
  // collections through their public copyOf factories without invoking Java serialization.
  @Test
  public void roundtripsImmutableListOfEveryBackingShape() {
    List<List<Long>> cases =
        Arrays.asList(
            List.<Long>copyOf(Collections.emptyList()),
            List.copyOf(Arrays.asList(7L)),
            List.copyOf(Arrays.asList(1L, 2L, 3L)));
    for (List<Long> src : cases) {
      List<Long> decoded = roundtrip(src);
      // size() is the call that dereferenced the (unpopulated) backing array before the fix.
      assertEquals(src.size(), decoded.size());
      assertEquals(src, decoded);
    }
  }

  // Set.of(1-2 elems)/Map.of(1 entry) select the Set12/Map1 backing classes; the empty and larger
  // variants select SetN/MapN. All extend the abstract base the serializers register on, so both
  // shapes are exercised here rather than assumed.
  @Test
  public void roundtripsImmutableSetAndMap() {
    assertTrue(roundtrip(java.util.Set.of()).isEmpty());
    assertEquals(java.util.Set.of("x"), roundtrip(java.util.Set.of("x")));
    assertEquals(java.util.Set.of("x", "y"), roundtrip(java.util.Set.of("x", "y")));
    assertEquals(java.util.Set.of("a", "b", "c"), roundtrip(java.util.Set.of("a", "b", "c")));
    assertTrue(roundtrip(java.util.Map.of()).isEmpty());
    assertEquals(java.util.Map.of("k", "v"), roundtrip(java.util.Map.of("k", "v")));
    assertEquals(
        java.util.Map.of("a", "b", "c", "d"), roundtrip(java.util.Map.of("a", "b", "c", "d")));
  }

  // Mirrors the failing path in practice: a Serializable value object (e.g. a compaction task's
  // options) that holds an immutable list built with List.copyOf(...) as a field. The codec used to
  // throw "KryoException: UnsupportedOperationException" on that field when shipping the object to
  // an executor.
  @Test
  public void roundtripsSerializableObjectHoldingAnImmutableListField() {
    Holder src = new Holder(List.copyOf(Arrays.asList(10L, 20L, 30L)), "compact");
    Holder decoded = roundtrip(src);
    assertEquals(src.ids, decoded.ids);
    assertEquals(src.name, decoded.name);
  }

  /** A minimal Serializable object carrying an immutable-list field, like CompactionOptions. */
  public static final class Holder implements Serializable {
    private final List<Long> ids;
    private final String name;

    Holder(List<Long> ids, String name) {
      this.ids = ids;
      this.name = name;
    }
  }

  // Security regression: LanceSerializeUtil.decode() is reachable from caller-controlled write
  // options, so element (de)serialization must stay on the Kryo path and must never invoke Java's
  // readObject hook. Delegating immutable collections to JavaSerializer (an earlier candidate fix)
  // ran every nested element through ObjectInputStream.readObject -- a deserialization gadget
  // surface. Decoding an immutable collection carrying such an element must NOT run its readObject.
  @Test
  public void doesNotInvokeReadObjectOfNestedElements() {
    GadgetProbe.executed = false;
    List<GadgetProbe> decoded = roundtrip(List.of(new GadgetProbe()));
    assertEquals(1, decoded.size());
    assertFalse(
        GadgetProbe.executed, "nested element readObject must not run during a Kryo decode");
  }

  /**
   * A Serializable element whose readObject records execution -- a stand-in for a deserialization
   * gadget. Kryo never calls readObject, so a Kryo decode must leave {@link #executed} false.
   */
  public static final class GadgetProbe implements Serializable {
    static boolean executed;

    private void readObject(java.io.ObjectInputStream in)
        throws java.io.IOException, ClassNotFoundException {
      in.defaultReadObject();
      executed = true;
    }
  }

  // The immutable collections are rebuilt via their public factory during decode, so the value
  // returned to callers is not the mutable buffer Kryo registered while reading. Aliased
  // collections (the same instance referenced twice) must still round-trip to a single immutable
  // instance -- otherwise a back-reference could hand back the throwaway mutable ArrayList (or the
  // wrong type for a set/map), which is exactly what an unfixed rebuild-on-read serializer does.
  @Test
  public void preservesReferenceIdentityOfAliasedImmutableCollections() {
    List<Long> sharedList = List.copyOf(Arrays.asList(1L, 2L, 3L));
    List<List<Long>> decodedList = roundtrip(List.of(sharedList, sharedList));
    assertSame(decodedList.get(0), decodedList.get(1));
    assertEquals(sharedList, decodedList.get(0));
    assertThrows(UnsupportedOperationException.class, () -> decodedList.get(0).add(4L));

    java.util.Set<Long> sharedSet = java.util.Set.of(9L, 8L, 7L);
    List<java.util.Set<Long>> decodedSet = roundtrip(List.of(sharedSet, sharedSet));
    assertSame(decodedSet.get(0), decodedSet.get(1));
    assertEquals(sharedSet, decodedSet.get(0));
    assertThrows(UnsupportedOperationException.class, () -> decodedSet.get(0).add(6L));

    java.util.Map<String, Long> sharedMap = java.util.Map.of("a", 1L, "b", 2L);
    List<java.util.Map<String, Long>> decodedMap = roundtrip(List.of(sharedMap, sharedMap));
    assertSame(decodedMap.get(0), decodedMap.get(1));
    assertEquals(sharedMap, decodedMap.get(0));
    assertThrows(UnsupportedOperationException.class, () -> decodedMap.get(0).put("c", 3L));
  }

  // A cycle can only form through a mutable object in the loop (an immutable collection cannot
  // contain itself). When the immutable collection is not the object the cycle re-enters, the
  // mutable member is registered before its back-reference is read, so the graph decodes correctly
  // and the collection is still immutable.
  @Test
  public void supportsCycleThroughAMutableObjectHoldingAnImmutableCollection() {
    CycleHolder holder = new CycleHolder();
    holder.tag = 42;
    holder.ref = List.of(holder); // immutable list contains holder; holder points back at the list

    CycleHolder decoded = roundtrip(holder);
    assertEquals(42, decoded.tag);
    assertTrue(decoded.ref instanceof List);
    List<?> list = (List<?>) decoded.ref;
    assertEquals(1, list.size());
    assertSame(decoded, list.get(0));
    assertThrows(UnsupportedOperationException.class, () -> ((List<Object>) decoded.ref).add(0));
  }

  // The one unsupportable shape: an immutable collection that a back-reference re-enters while it
  // is still being read (here it is the graph root). Its immutable value does not exist yet, so
  // rather than leak the mutable buffer the decode fails fast with a clear message.
  @Test
  public void rejectsCycleThatReentersAnImmutableCollectionStillBeingRead() {
    CycleHolder holder = new CycleHolder();
    holder.tag = 7;
    List<CycleHolder> root = List.of(holder); // root is the immutable list itself
    holder.ref = root;

    RuntimeException thrown = assertThrows(RuntimeException.class, () -> roundtrip(root));
    assertTrue(
        messageChain(thrown).contains("cyclic reference through an immutable collection"),
        messageChain(thrown));
  }

  private static String messageChain(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      sb.append(cur.getMessage()).append('\n');
    }
    return sb.toString();
  }

  /** A mutable object that can hold a back-reference, used to build cyclic object graphs. */
  public static final class CycleHolder {
    public int tag;
    public Object ref;
  }
}
