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
import org.lance.namespace.model.*;
import org.lance.spark.internal.LanceDatasetAdapter;

import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.arrow.memory.BufferAllocator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MemoryLanceNamespace implements LanceNamespace {
  public static final String SCHEME = "mem";
  public static final String ROOT = "root";
  private static Map<String, MemoryLanceNamespace> CACHE = new HashMap<>();

  private MemoryLanceNamespace() {}

  public static MemoryLanceNamespace create(String location, Map<String, String> options) {
    if (!location.startsWith(SCHEME)) {
      throw new IllegalArgumentException("Invalid location: " + location);
    }

    MemoryLanceNamespace namespace = CACHE.get(location);
    if (namespace == null) {
      namespace = new MemoryLanceNamespace();
      options.put(ROOT, location);
      namespace.initialize(options, LanceDatasetAdapter.allocator);
      CACHE.put(location, namespace);
    }
    return namespace;
  }

  static class NamespaceNode {
    Map<String, String> properties = Maps.newConcurrentMap();
    Map<String, Object> children = Maps.newConcurrentMap();

    public List<String> namespaces() {
      List<String> namespaces = new ArrayList<>();
      for (Map.Entry<String, Object> entry : children.entrySet()) {
        if (entry.getValue() instanceof NamespaceNode) {
          namespaces.add(entry.getKey());
        }
      }
      return namespaces;
    }

    public List<String> tables() {
      List<String> tables = new ArrayList<>();
      for (Map.Entry<String, Object> entry : children.entrySet()) {
        if (entry.getValue() instanceof TableNode) {
          tables.add(entry.getKey());
        }
      }
      return tables;
    }

    public void add(String name, Object node) {
      if (children.containsKey(name)) {
        throw new RuntimeException("Duplicate key: " + name);
      }
      children.put(name, node);
    }

    public Map<String, String> remove(String name) {
      Object obj = children.remove(name);
      if (obj == null) {
        return null;
      } else if (obj instanceof NamespaceNode) {
        return ((NamespaceNode) obj).properties;
      } else if (obj instanceof TableNode) {
        return ((TableNode) obj).properties;
      } else {
        throw new RuntimeException("Invalid object: " + obj);
      }
    }
  }

  static class TableNode {
    Map<String, String> properties;
  }

  private NamespaceNode root = new NamespaceNode();
  private final Map<String, Boolean> errorFunctionRegistration = new HashMap<>();
  private Path basePath;

  public void registerErrorFunction(String functionName, boolean error) {
    errorFunctionRegistration.put(functionName, error);
  }

  private void checkError(String functionName) {
    Boolean shouldError = errorFunctionRegistration.get(functionName);
    if (shouldError != null && shouldError) {
      throw new RuntimeException("Function " + functionName + " error!");
    }
  }

  private NamespaceNode findNamespaceOrNull(List<String> id) {
    NamespaceNode next = root;
    for (int i = 0; i < id.size(); i++) {
      String name = id.get(i);
      Object child = next.children.get(name);

      if (child == null) {
        return null;
      } else if (child instanceof TableNode) {
        return null;
      } else {
        next = (NamespaceNode) child;
      }
    }
    return next;
  }

  private TableNode findTableOrNull(List<String> id) {
    if (id.isEmpty()) {
      return null;
    }

    Object parent;
    if (id.size() == 1) {
      parent = root;
    } else {
      parent = findNamespaceOrNull(id.subList(0, id.size() - 1));
    }

    if (parent == null || parent instanceof TableNode) {
      return null;
    }
    NamespaceNode parentNode = (NamespaceNode) parent;
    Object child = parentNode.children.get(id.get(id.size() - 1));
    if (child == null || child instanceof NamespaceNode) {
      return null;
    } else {
      return (TableNode) child;
    }
  }

  public void setProperties(List<String> id, Map<String, String> properties) {
    NamespaceNode ns = findNamespaceOrNull(id);
    if (ns != null) {
      ns.properties.clear();
      ns.properties.putAll(properties);
    }

    TableNode tn = findTableOrNull(id);
    if (tn != null) {
      tn.properties.clear();
      tn.properties.putAll(properties);
    }
  }

  @Override
  public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
    checkError("initialize");

    String root = configProperties.get(ROOT);
    Preconditions.checkArgument(
        root != null && root.startsWith(SCHEME + "://"), "Invalid root: " + root);

    try {
      basePath = Files.createTempDirectory(SCHEME);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create base directory for namespace", e);
    }
  }

  @Override
  public String namespaceId() {
    return basePath.toString();
  }

  // Namespace related methods
  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    checkError("listNamespaces");
    List<String> id = request.getId();
    NamespaceNode ns = id.isEmpty() ? root : findNamespaceOrNull(id);

    ListNamespacesResponse resp = new ListNamespacesResponse();
    if (ns != null) {
      resp.setNamespaces(new HashSet<>(ns.namespaces()));
    } else {
      resp.setNamespaces(Sets.newHashSet());
    }
    return resp;
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    checkError("describeNamespace");
    List<String> id = request.getId();
    NamespaceNode ns = findNamespaceOrNull(id);
    if (ns == null) {
      throw new RuntimeException("Namespace not found: " + id);
    }
    DescribeNamespaceResponse resp = new DescribeNamespaceResponse();
    resp.setProperties(ns.properties);
    return resp;
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    checkError("createNamespace");
    List<String> id = request.getId();
    if (findNamespaceOrNull(id) != null) {
      throw new RuntimeException("Namespace already exists: " + id);
    }

    NamespaceNode parent =
        id.size() == 1 ? root : findNamespaceOrNull(id.subList(0, id.size() - 1));
    if (parent == null) {
      throw new RuntimeException("Namespace doesn't exist: " + id.subList(0, id.size() - 1));
    }

    NamespaceNode ns = new NamespaceNode();
    if (request.getProperties() != null) {
      ns.properties.putAll(request.getProperties());
    }
    parent.add(id.get(id.size() - 1), ns);

    CreateNamespaceResponse resp = new CreateNamespaceResponse();
    resp.setProperties(ns.properties);
    return resp;
  }

  @Override
  public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
    checkError("dropNamespace");
    List<String> id = request.getId();

    NamespaceNode node = findNamespaceOrNull(id);
    if (node == null) {
      throw new RuntimeException("Namespace not found: " + id);
    }
    if (!node.children.isEmpty() && !request.getBehavior().equals("Cascade")) {
      throw new RuntimeException("Couldn't drop namespace because namespace is not empty: " + id);
    }

    NamespaceNode parent = findNamespaceOrNull(id.subList(0, id.size() - 1));
    if (parent == null) {
      throw new RuntimeException("Namespace not found: " + id.subList(0, id.size() - 1));
    }

    Map<String, String> properties = parent.remove(id.get(id.size() - 1));

    DropNamespaceResponse resp = new DropNamespaceResponse();
    resp.setProperties(properties == null ? new HashMap<>() : properties);
    return resp;
  }

  @Override
  public void namespaceExists(NamespaceExistsRequest request) {
    checkError("namespaceExists");
    List<String> id = request.getId();
    if (findNamespaceOrNull(id) == null) {
      throw new RuntimeException("Namespace not found: " + id);
    }
  }

  // Table related methods
  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    checkError("listTables");
    List<String> id = request.getId();
    NamespaceNode ns = id.isEmpty() ? root : findNamespaceOrNull(id);

    ListTablesResponse resp = new ListTablesResponse();
    if (ns != null) {
      resp.setTables(new HashSet<>(ns.tables()));
    } else {
      resp.setTables(new HashSet<>());
    }
    return resp;
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    checkError("describeTable");
    List<String> id = request.getId();
    TableNode tn = findTableOrNull(id);

    if (tn == null) {
      throw new RuntimeException("Table not found: " + id);
    }
    DescribeTableResponse resp = new DescribeTableResponse();
    resp.setStorageOptions(tn.properties);
    String location = tn.properties.get(ROOT);
    Preconditions.checkArgument(location != null, "Location should not be null.");
    resp.setLocation(location);
    return resp;
  }

  @Override
  public CreateEmptyTableResponse createEmptyTable(CreateEmptyTableRequest request) {
    checkError("createEmptyTable");
    List<String> id = request.getId();

    if (findTableOrNull(id) != null) {
      throw new RuntimeException("Table already exists: " + id);
    }

    // Find or create parent namespace
    NamespaceNode parent =
        id.size() == 1 ? root : findNamespaceOrNull(id.subList(0, id.size() - 1));
    if (parent == null) {
      throw new RuntimeException("Parent namespace not found: " + id.subList(0, id.size() - 1));
    }

    // Create location if needed
    String location;
    if (request.getLocation() != null) {
      location = request.getLocation();
    } else {
      StringBuilder builder = new StringBuilder(basePath.toString());
      for (int i = 0; i < id.size(); i++) {
        if (i > 0) {
          builder.append("/");
        }
        builder.append(id.get(i));
      }
      builder.append(".lance");
      location = builder.toString();
    }

    // Create the table node
    TableNode tn = new TableNode();
    tn.properties = new HashMap<>();
    if (request.getProperties() != null) {
      tn.properties.putAll(request.getProperties());
    }
    tn.properties.put(ROOT, location);

    parent.add(id.get(id.size() - 1), tn);

    CreateEmptyTableResponse resp = new CreateEmptyTableResponse();
    resp.setLocation(location.toString());
    resp.setStorageOptions(new HashMap<>());
    return resp;
  }

  @Override
  public void tableExists(TableExistsRequest request) {
    checkError("tableExists");
    List<String> id = request.getId();

    if (findTableOrNull(id) == null) {
      throw new RuntimeException("Table not found: " + id);
    }
  }

  @Override
  public DropTableResponse dropTable(DropTableRequest request) {
    checkError("dropTable");
    List<String> id = request.getId();

    // Find the table
    TableNode tn = findTableOrNull(id);
    if (tn == null) {
      throw new RuntimeException("Table not found: " + id);
    }

    // Remove the table from its parent
    NamespaceNode parent =
        id.size() == 1 ? root : findNamespaceOrNull(id.subList(0, id.size() - 1));

    Map<String, String> properties = null;
    if (parent != null) {
      properties = parent.remove(id.get(id.size() - 1));
    }

    // Construct response
    DropTableResponse response = new DropTableResponse();
    response.setId(request.getId());
    response.setProperties(properties == null ? new HashMap<>() : properties);
    response.setLocation(properties == null ? null : properties.get(ROOT));
    return response;
  }

  public void cleanup() throws IOException {
    List<Path> files =
        Files.walk(basePath).sorted(Comparator.reverseOrder()).collect(Collectors.toList());
    for (Path f : files) {
      Files.delete(f);
    }
  }

  public String base() {
    return basePath.toString();
  }
}
