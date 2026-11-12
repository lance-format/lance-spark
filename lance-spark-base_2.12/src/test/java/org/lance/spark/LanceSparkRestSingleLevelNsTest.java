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
package org.lance.spark;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.spark.utils.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for REST single-level namespace auto-detection. */
public class LanceSparkRestSingleLevelNsTest {

  private static final List<String> PARENT = Collections.singletonList("lance_catalog");

  @Test
  public void testProbeWithParentListsUnderParent() {
    assertFalse(probe(Optional.of(PARENT), PARENT::equals));
  }

  @Test
  public void testProbeWithoutParentListsRoot() {
    assertFalse(probe(Optional.empty(), List::isEmpty));
  }

  @Test
  public void testProbeFailureFallsBackToSingleLevel() {
    assertTrue(probe(Optional.of(PARENT), id -> false));
  }

  /** Runs the probe against a stub namespace that only accepts the listed ids. */
  private boolean probe(Optional<List<String>> parent, Predicate<List<String>> accepts) {
    LanceNamespace namespace =
        new LanceNamespace() {
          @Override
          public void initialize(Map<String, String> config, BufferAllocator allocator) {}

          @Override
          public String namespaceId() {
            return "stub";
          }

          @Override
          public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
            if (!accepts.test(request.getId())) {
              throw new IllegalStateException("rejected: " + request.getId());
            }
            return new ListNamespacesResponse();
          }
        };
    return BaseLanceNamespaceSparkCatalog.determineSingleLevelNsForRest(namespace, parent);
  }
}
