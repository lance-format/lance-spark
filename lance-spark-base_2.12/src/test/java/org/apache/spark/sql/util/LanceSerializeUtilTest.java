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
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests that {@link LanceSerializeUtil}'s Kryo codec roundtrips JDK immutable collections. */
public class LanceSerializeUtilTest {

  private static <T> T roundtrip(T obj) {
    return LanceSerializeUtil.decode(LanceSerializeUtil.encode(obj));
  }

  // The JDK immutable collections (List.of/copyOf, Set.of, Map.of) have no no-arg constructor and
  // keep their storage in a java.base-internal field, so the codec's Objenesis instantiator could
  // not reconstruct them: a decoded collection threw on first use. The size()/contents assertions
  // below fail on the pre-fix codec and pass once the immutable classes delegate to JavaSerializer.
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

  @Test
  public void roundtripsImmutableSetAndMap() {
    assertTrue(roundtrip(java.util.Set.of()).isEmpty());
    assertEquals(java.util.Set.of("a", "b", "c"), roundtrip(java.util.Set.of("a", "b", "c")));
    assertTrue(roundtrip(java.util.Map.of()).isEmpty());
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
}
