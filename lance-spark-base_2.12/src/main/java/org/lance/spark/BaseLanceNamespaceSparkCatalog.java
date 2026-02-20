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

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.spark.function.LanceFragmentIdWithDefaultFunction;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.SchemaConverter;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.lance.spark.utils.Utils.createReadOptions;
import static org.lance.spark.utils.Utils.getSchema;
import static org.lance.spark.utils.Utils.openDataset;

public abstract class BaseLanceNamespaceSparkCatalog
    implements TableCatalog, SupportsNamespaces, FunctionCatalog {

  private static final Logger logger =
      LoggerFactory.getLogger(BaseLanceNamespaceSparkCatalog.class);

  /** Used to specify the namespace implementation to use */
  private static final String CONFIG_IMPL = "impl";

  /**
   * Enable single-level namespace mode with a virtual "default" namespace.
   *
   * <p>When true: Tables are accessed as catalog.default.table, where "default" is a virtual
   * namespace that maps to the root level. CREATE NAMESPACE is not allowed.
   *
   * <p>When false (default): Multi-level namespace mode. Namespaces must be created explicitly with
   * CREATE NAMESPACE before creating tables. Tables use manifest-based storage with hash-prefixed
   * paths for better scalability.
   *
   * <p>For REST implementations: if ListNamespaces fails, single_level_ns is automatically enabled
   * for backward compatibility with flat namespace backends.
   */
  private static final String CONFIG_SINGLE_LEVEL_NS = "single_level_ns";

  /** Supply in CREATE TABLE options to supply a different location to use for the table */
  private static final String CREATE_TABLE_PROPERTY_LOCATION = "location";

  /** Parent prefix configuration for multi-level namespaces like Hive3 */
  private static final String CONFIG_PARENT = "parent";

  private static final String CONFIG_PARENT_DELIMITER = "parent_delimiter";
  private static final String CONFIG_PARENT_DELIMITER_DEFAULT = ".";

  private LanceNamespace namespace;
  private String name;
  private boolean singleLevelNs;
  private Optional<List<String>> parentPrefix;
  private LanceSparkCatalogConfig catalogConfig;
  private Map<String, String> storageOptions;

  /**
   * The namespace implementation type (e.g., "rest", "dir"). Saved for creating storage options
   * providers on workers.
   */
  private String namespaceImpl;

  /**
   * The namespace properties for connection. Saved for creating storage options providers on
   * workers.
   */
  private Map<String, String> namespaceProperties;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
    this.storageOptions = new HashMap<>(options.asCaseSensitiveMap());

    // Parse catalog configuration
    this.catalogConfig = LanceSparkCatalogConfig.from(this.storageOptions);

    if (!options.containsKey(CONFIG_IMPL)) {
      throw new IllegalArgumentException("Missing required configuration: " + CONFIG_IMPL);
    }
    String impl = options.get(CONFIG_IMPL);

    // Handle parent prefix configuration
    if (options.containsKey(CONFIG_PARENT)) {
      String parent = options.get(CONFIG_PARENT);
      String delimiter =
          options.getOrDefault(CONFIG_PARENT_DELIMITER, CONFIG_PARENT_DELIMITER_DEFAULT);
      List<String> parentParts = Arrays.asList(parent.split(Pattern.quote(delimiter)));
      this.parentPrefix = Optional.of(parentParts);
    } else {
      this.parentPrefix = Optional.empty();
    }

    // Initialize the namespace with proper configuration
    Map<String, String> namespaceOptions = new HashMap<>(options);

    // Save namespace impl and properties for serialization to workers
    this.namespaceImpl = impl;
    this.namespaceProperties = new HashMap<>(namespaceOptions);

    // Use the global buffer allocator
    this.namespace = LanceNamespace.connect(impl, namespaceOptions, LanceRuntime.allocator());

    // Handle single-level namespace configuration
    if (options.containsKey(CONFIG_SINGLE_LEVEL_NS)) {
      this.singleLevelNs = Boolean.parseBoolean(options.get(CONFIG_SINGLE_LEVEL_NS));
    } else if ("rest".equals(impl)) {
      // For REST: auto-detect based on whether ListNamespaces works
      this.singleLevelNs = determineSingleLevelNsForRest();
    } else {
      // Default: multi-level namespace mode (manifest mode)
      this.singleLevelNs = false;
    }
  }

  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the namespace implementation type (e.g., "rest", "dir").
   *
   * @return the namespace implementation type
   */
  public String getNamespaceImpl() {
    return namespaceImpl;
  }

  /**
   * Returns the namespace properties for connection.
   *
   * @return the namespace properties map
   */
  public Map<String, String> getNamespaceProperties() {
    return namespaceProperties;
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    if (namespace != null && namespace.length > 0 && !namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace);
    }
    String[] targetNamespace = namespace == null ? new String[0] : namespace;
    return new Identifier[] {
      Identifier.of(targetNamespace, LanceFragmentIdWithDefaultFunction.NAME)
    };
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    if (ident.namespace().length != 0
        || !LanceFragmentIdWithDefaultFunction.NAME.equalsIgnoreCase(ident.name())) {
      throw new NoSuchFunctionException(ident);
    }
    return new LanceFragmentIdWithDefaultFunction();
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    // Namespace alteration is not supported in the current API
    throw new UnsupportedOperationException("Namespace alteration is not supported");
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    // List root level namespaces
    org.lance.namespace.model.ListNamespacesRequest request =
        new org.lance.namespace.model.ListNamespacesRequest();

    // Add parent prefix to empty namespace (root)
    if (parentPrefix.isPresent()) {
      request.setId(parentPrefix.get());
    }

    try {
      org.lance.namespace.model.ListNamespacesResponse response = namespace.listNamespaces(request);

      List<String[]> result = new ArrayList<>();
      for (String ns : response.getNamespaces()) {
        // For single-level namespace names, create array
        String[] nsArray = new String[] {ns};

        // Note: parent prefix removal is not needed here as the response
        // only contains the namespace name, not the full path

        // In single-level mode, prepend virtual "default" namespace
        if (singleLevelNs) {
          String[] withDefault = new String[2];
          withDefault[0] = "default";
          withDefault[1] = ns;
          nsArray = withDefault;
        }

        result.add(nsArray);
      }

      return result.toArray(new String[0][]);
    } catch (Exception e) {
      throw new NoSuchNamespaceException(new String[0]);
    }
  }

  @Override
  public String[][] listNamespaces(String[] parent) throws NoSuchNamespaceException {
    // Remove single-level prefix and add parent prefix
    String[] actualParent = removeSingleLevelPrefixFromNamespace(parent);
    actualParent = addParentPrefix(actualParent);

    org.lance.namespace.model.ListNamespacesRequest request =
        new org.lance.namespace.model.ListNamespacesRequest();
    request.setId(Arrays.asList(actualParent));

    try {
      org.lance.namespace.model.ListNamespacesResponse response = namespace.listNamespaces(request);

      List<String[]> result = new ArrayList<>();
      for (String ns : response.getNamespaces()) {
        // For single-level namespace names, create full path
        String[] nsArray = new String[parent.length + 1];
        System.arraycopy(parent, 0, nsArray, 0, parent.length);
        nsArray[parent.length] = ns;

        result.add(nsArray);
      }

      return result.toArray(new String[0][]);
    } catch (Exception e) {
      throw new NoSuchNamespaceException(parent);
    }
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    // In single-level mode, the virtual "default" namespace always exists
    if (singleLevelNs && namespace.length == 1 && "default".equals(namespace[0])) {
      return true;
    }

    // Remove single-level prefix and add parent prefix
    String[] actualNamespace = removeSingleLevelPrefixFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

    org.lance.namespace.model.NamespaceExistsRequest request =
        new org.lance.namespace.model.NamespaceExistsRequest();
    request.setId(Arrays.asList(actualNamespace));

    try {
      this.namespace.namespaceExists(request);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    // Remove single-level prefix and add parent prefix
    String[] actualNamespace = removeSingleLevelPrefixFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

    org.lance.namespace.model.DescribeNamespaceRequest request =
        new org.lance.namespace.model.DescribeNamespaceRequest();
    request.setId(Arrays.asList(actualNamespace));

    try {
      org.lance.namespace.model.DescribeNamespaceResponse response =
          this.namespace.describeNamespace(request);

      Map<String, String> properties = response.getProperties();
      return properties != null ? properties : Collections.emptyMap();
    } catch (Exception e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> properties)
      throws NamespaceAlreadyExistsException {
    // Remove single-level prefix and add parent prefix
    String[] actualNamespace = removeSingleLevelPrefixFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

    org.lance.namespace.model.CreateNamespaceRequest request =
        new org.lance.namespace.model.CreateNamespaceRequest();
    request.setId(Arrays.asList(actualNamespace));

    if (properties != null && !properties.isEmpty()) {
      request.setProperties(properties);
    }

    try {
      this.namespace.createNamespace(request);
    } catch (Exception e) {
      if (e.getMessage() != null && e.getMessage().contains("already exists")) {
        throw new NamespaceAlreadyExistsException(namespace);
      }
      throw new RuntimeException("Failed to create namespace", e);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException {
    // Remove single-level prefix and add parent prefix
    String[] actualNamespace = removeSingleLevelPrefixFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

    DropNamespaceRequest request = new DropNamespaceRequest();
    request.setId(Arrays.asList(actualNamespace));

    // Set behavior based on cascade flag - let the Lance namespace API handle the logic
    if (cascade) {
      request.setBehavior("Cascade");
    } else {
      request.setBehavior("Restrict");
    }

    this.namespace.dropNamespace(request);
    return true;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    String[] actualNamespace = removeSingleLevelPrefixFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

    ListTablesRequest request = new ListTablesRequest();
    request.setId(Arrays.stream(actualNamespace).collect(Collectors.toList()));

    List<Identifier> identifiers = new ArrayList<>();
    String pageToken = null;

    do {
      if (pageToken != null) {
        request.setPageToken(pageToken);
      }
      ListTablesResponse response = this.namespace.listTables(request);
      for (String table : response.getTables()) {
        identifiers.add(Identifier.of(namespace, table));
      }
      pageToken = response.getPageToken();
    } while (pageToken != null && !pageToken.isEmpty());

    return identifiers.toArray(new Identifier[0]);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    // Transform identifier for API call
    Identifier actualIdent = transformIdentifierForApi(ident);

    org.lance.namespace.model.TableExistsRequest request =
        new org.lance.namespace.model.TableExistsRequest();
    for (String part : actualIdent.namespace()) {
      request.addIdItem(part);
    }
    request.addIdItem(actualIdent.name());

    try {
      this.namespace.tableExists(request);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return loadTableInternal(ident, Optional.empty(), Optional.empty());
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    return loadTableInternal(ident, Optional.empty(), Optional.of(version));
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    return loadTableInternal(ident, Optional.of(timestamp), Optional.empty());
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    Identifier actualIdent = transformIdentifierForApi(ident);

    // Build the table ID for credential vending
    List<String> tableIdList = buildTableId(actualIdent);

    StructType processedSchema = SchemaConverter.processSchemaWithProperties(schema, properties);

    // Create dataset using namespace - WriteDatasetBuilder handles declareTable internally
    // and properly leverages namespace client for credential vending
    String location;
    try (Dataset dataset =
        Dataset.write()
            .allocator(LanceRuntime.allocator())
            .namespace(namespace)
            .tableId(tableIdList)
            .schema(LanceArrowUtils.toArrowSchema(processedSchema, "UTC", true, false))
            .mode(WriteParams.WriteMode.CREATE)
            .storageOptions(catalogConfig.getStorageOptions())
            .execute()) {
      location = dataset.uri();
    }

    // Call describeTable to get initial storage options for Spark dataset wrapper
    DescribeTableRequest describeRequest = new DescribeTableRequest();
    tableIdList.forEach(describeRequest::addIdItem);
    DescribeTableResponse describeResponse = namespace.describeTable(describeRequest);
    Map<String, String> initialStorageOptions = describeResponse.getStorageOptions();

    // Create read options with namespace settings
    LanceSparkReadOptions readOptions =
        createReadOptions(
            location,
            catalogConfig,
            Optional.empty(),
            Optional.of(namespace),
            Optional.of(tableIdList));
    return createDataset(
        readOptions, processedSchema, initialStorageOptions, namespaceImpl, namespaceProperties);
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException("Table alteration is not supported");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    try {
      Identifier tableId = transformIdentifierForApi(ident);
      DeregisterTableRequest deregisterRequest = new DeregisterTableRequest();
      for (String part : tableId.namespace()) {
        deregisterRequest.addIdItem(part);
      }
      deregisterRequest.addIdItem(tableId.name());
      namespace.deregisterTable(deregisterRequest);

      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    try {
      Identifier tableId = transformIdentifierForApi(ident);
      DropTableRequest dropRequest = new DropTableRequest();
      for (String part : tableId.namespace()) {
        dropRequest.addIdItem(part);
      }
      dropRequest.addIdItem(tableId.name());
      namespace.dropTable(dropRequest);

      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Table renaming is not supported");
  }

  /**
   * Removes the virtual "default" prefix from a Spark identifier in single-level mode. For example:
   * - ["default", "table"] -> ["table"] - ["default"] -> [] (root namespace) - ["other", "table"]
   * -> ["other", "table"] (unchanged)
   */
  private Identifier removeSingleLevelPrefixFromId(Identifier identifier) {
    if (!singleLevelNs) {
      return identifier;
    }

    String[] newNamespace = removeSingleLevelPrefixFromNamespace(identifier.namespace());
    return Identifier.of(newNamespace, identifier.name());
  }

  /**
   * Transforms an identifier for API calls by removing single-level prefix and adding parent
   * prefix.
   */
  private Identifier transformIdentifierForApi(Identifier identifier) {
    Identifier transformed = removeSingleLevelPrefixFromId(identifier);
    String[] namespace = addParentPrefix(transformed.namespace());
    return Identifier.of(namespace, transformed.name());
  }

  /**
   * Removes the virtual "default" prefix from a namespace array in single-level mode. For example:
   * - ["default"] -> [] - ["default", "subnamespace"] -> ["subnamespace"] - ["other"] -> ["other"]
   * (unchanged)
   */
  private String[] removeSingleLevelPrefixFromNamespace(String[] namespace) {
    if (!singleLevelNs) {
      return namespace;
    }

    // Check if the first namespace part matches "default"
    if (namespace.length > 0 && "default".equals(namespace[0])) {
      // Remove the "default" prefix from namespace
      String[] newNamespace = new String[namespace.length - 1];
      System.arraycopy(namespace, 1, newNamespace, 0, namespace.length - 1);
      return newNamespace;
    }

    return namespace;
  }

  /**
   * Determines whether to use single-level mode for REST implementations by testing ListNamespaces.
   * If ListNamespaces fails, assumes flat namespace structure and enables single-level mode.
   *
   * @return true if ListNamespaces fails (single-level mode), false otherwise
   */
  private boolean determineSingleLevelNsForRest() {
    try {
      org.lance.namespace.model.ListNamespacesRequest request =
          new org.lance.namespace.model.ListNamespacesRequest();
      namespace.listNamespaces(request);
      return false;
    } catch (Exception e) {
      logger.info(
          "REST namespace ListNamespaces failed, "
              + "falling back to flat table structure with single_level_ns=true");
      return true;
    }
  }

  /**
   * Adds parent prefix to namespace array for API calls. For example, with
   * parentPrefix=["catalog1"], ["ns1"] becomes ["catalog1", "ns1"]
   */
  private String[] addParentPrefix(String[] namespace) {
    if (parentPrefix.isEmpty()) {
      return namespace;
    }

    List<String> result = new ArrayList<>(parentPrefix.get());
    result.addAll(Arrays.asList(namespace));
    return result.toArray(new String[0]);
  }

  /**
   * Removes parent prefix from namespace array for Spark. For example, with
   * parentPrefix=["catalog1"], ["catalog1", "ns1"] becomes ["ns1"]
   */
  private String[] removeParentPrefix(String[] namespace) {
    if (parentPrefix.isEmpty()) {
      return namespace;
    }

    List<String> prefix = parentPrefix.get();
    if (namespace.length >= prefix.size()) {
      // Check if namespace starts with the parent prefix
      boolean hasPrefix = true;
      for (int i = 0; i < prefix.size(); i++) {
        if (!prefix.get(i).equals(namespace[i])) {
          hasPrefix = false;
          break;
        }
      }

      if (hasPrefix) {
        // Remove the prefix
        String[] result = new String[namespace.length - prefix.size()];
        System.arraycopy(namespace, prefix.size(), result, 0, result.length);
        return result;
      }
    }

    return namespace;
  }

  /**
   * Builds the table ID list from an identifier for use with namespace operations. The table ID
   * includes both the namespace parts and the table name.
   */
  private List<String> buildTableId(Identifier ident) {
    return Stream.concat(Arrays.stream(ident.namespace()), Stream.of(ident.name()))
        .collect(Collectors.toList());
  }

  private Table loadTableInternal(
      Identifier ident, Optional<Long> timestamp, Optional<String> version)
      throws NoSuchTableException {

    // Transform identifier for API call
    Identifier actualIdent = transformIdentifierForApi(ident);

    // Build the table ID for credential vending
    List<String> tableId = buildTableId(actualIdent);

    // Call describeTable to get location and initial storage options
    DescribeTableRequest describeRequest = new DescribeTableRequest();
    tableId.forEach(describeRequest::addIdItem);
    DescribeTableResponse describeResponse;
    try {
      describeResponse = namespace.describeTable(describeRequest);
    } catch (TableNotFoundException e) {
      throw new NoSuchTableException(ident);
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed to describe table: " + ident, e);
    }
    String location = describeResponse.getLocation();
    Map<String, String> initialStorageOptions = describeResponse.getStorageOptions();

    Optional<Long> versionId = Optional.empty();
    if (timestamp.isPresent()) {
      LanceSparkReadOptions readOptions =
          createReadOptions(
              location,
              catalogConfig,
              Optional.empty(),
              Optional.of(namespace),
              Optional.of(tableId));
      try (Dataset dataset = openDataset(readOptions)) {
        versionId = Optional.of(Utils.findVersion(dataset.listVersions(), timestamp.get()));
      } catch (TableNotFoundException e) {
        throw new NoSuchTableException(ident);
      }
    } else if (version.isPresent()) {
      versionId = Optional.of(Utils.parseVersion(version.get()));
    }

    LanceSparkReadOptions readOptions =
        createReadOptions(
            location, catalogConfig, versionId, Optional.of(namespace), Optional.of(tableId));
    StructType schema = getSchema(ident, readOptions);

    // Create read options with namespace support
    return createDataset(
        readOptions, schema, initialStorageOptions, namespaceImpl, namespaceProperties);
  }

  public abstract LanceDataset createDataset(
      LanceSparkReadOptions readOptions,
      StructType sparkSchema,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties);
}
