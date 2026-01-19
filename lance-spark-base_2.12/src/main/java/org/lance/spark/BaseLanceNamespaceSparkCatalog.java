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
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.spark.partition.LancePartitionConfig;
import org.lance.spark.partition.PartitionUtils;
import org.lance.spark.partition.PartitionedLanceDataset;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.SchemaConverter;

import com.clearspring.analytics.util.Preconditions;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
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

import static org.lance.spark.partition.PartitionUtils.LANCE_PARTITION_TABLE_ROOT;
import static org.lance.spark.partition.PartitionUtils.identToId;
import static org.lance.spark.partition.PartitionUtils.namespaceProperties;
import static org.lance.spark.partition.PartitionUtils.tableProperties;

public abstract class BaseLanceNamespaceSparkCatalog implements TableCatalog, SupportsNamespaces {

  private static final Logger logger =
      LoggerFactory.getLogger(BaseLanceNamespaceSparkCatalog.class);

  /** Used to specify the namespace implementation to use */
  private static final String CONFIG_IMPL = "impl";

  /**
   * Spark requires at least 3 levels of catalog -> namespaces -> table (most uses exactly 3 levels)
   * For namespaces that are only 2 levels (e.g. dir), this puts an extra dummy level namespace with
   * the given name, so that a namespace mounted as ns1 in Spark will have ns1 -> dummy_name ->
   * table structure.
   *
   * <p>For native implementations, we perform the following handling automatically:
   *
   * <ul>
   *   <li>dir: directly configure extra_level=default
   *   <li>rest: if ListNamespaces returns error, configure extra_level=default
   * </ul>
   */
  private static final String CONFIG_EXTRA_LEVEL = "extra_level";

  /** Supply in CREATE TABLE options to supply a different location to use for the table */
  private static final String CREATE_TABLE_PROPERTY_LOCATION = "location";

  /** Parent prefix configuration for multi-level namespaces like Hive3 */
  private static final String CONFIG_PARENT = "parent";

  private static final String CONFIG_PARENT_DELIMITER = "parent_delimiter";
  private static final String CONFIG_PARENT_DELIMITER_DEFAULT = ".";

  private LanceNamespace namespace;
  private String impl;
  private String name;
  private Optional<String> extraLevel;
  private Optional<List<String>> parentPrefix;
  private LanceSparkCatalogConfig catalogConfig;
  private Map<String, String> storageOptions;

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

    this.namespace = PartitionUtils.createNamespace(namespaceOptions);
    this.impl = impl;

    // Handle extra level name configuration
    if (options.containsKey(CONFIG_EXTRA_LEVEL)) {
      this.extraLevel = Optional.of(options.get(CONFIG_EXTRA_LEVEL));
    } else if ("dir".equals(impl)) {
      this.extraLevel = Optional.of("default");
    } else if ("rest".equals(impl)) {
      this.extraLevel = determineExtraLevelForRest();
    } else {
      this.extraLevel = Optional.empty();
    }
  }

  @Override
  public String name() {
    return name;
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

        // Add extra level if configured
        if (extraLevel.isPresent()) {
          String[] withExtra = new String[2];
          withExtra[0] = extraLevel.get();
          withExtra[1] = ns;
          nsArray = withExtra;
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
    // Remove extra level and add parent prefix
    String[] actualParent = removeExtraLevelsFromNamespace(parent);
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
    // If the namespace is exactly the extra level, it exists as a virtual namespace
    if (extraLevel.isPresent() && namespace.length == 1 && extraLevel.get().equals(namespace[0])) {
      return true;
    }

    // Remove extra levels and add parent prefix
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);
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
    // Remove extra levels and add parent prefix
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);
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
    // Remove extra levels and add parent prefix
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);
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
    // Remove extra levels and add parent prefix
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);
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
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

    // List non-partitioned tables.
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

    // List partitioned tables.
    ListNamespacesRequest req = new ListNamespacesRequest();
    req.setId(Arrays.stream(actualNamespace).collect(Collectors.toList()));
    pageToken = null;

    do {
      if (pageToken != null) {
        req.setPageToken(pageToken);
      }
      ListNamespacesResponse response = this.namespace.listNamespaces(req);
      for (String ns : response.getNamespaces()) {
        Identifier nsId = Identifier.of(namespace, ns);
        if (PartitionUtils.isPartitionTable(this.namespace, impl, nsId)) {
          identifiers.add(nsId);
        }
      }
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
      logger.debug("Normal table {} not exists, fall back to partition table.", ident, e);
    }

    try {
      loadPartitionedTable(ident);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      return loadNonPartitionedTable(ident);
    } catch (RuntimeException e) {
      return loadPartitionedTable(ident);
    }
  }

  private Table loadNonPartitionedTable(Identifier ident) throws NoSuchTableException {
    // Transform identifier for API call
    Identifier actualIdent = transformIdentifierForApi(ident);

    // Build the table ID for credential vending
    List<String> tableId = buildTableId(actualIdent);

    // Open dataset using namespace for credential vending
    StructType schema;
    String location;
    try (Dataset dataset =
        Dataset.open()
            .allocator(LanceRuntime.allocator())
            .namespace(namespace)
            .tableId(tableId)
            .build()) {
      schema = LanceArrowUtils.fromArrowSchema(dataset.getSchema());
      location = dataset.uri();
    } catch (IllegalArgumentException e) {
      throw new NoSuchTableException(ident);
    }

    // Create read options with namespace support
    LanceSparkReadOptions readOptions = createReadOptions(location, tableId);
    return createDataset(readOptions, schema);
  }

  private Table loadPartitionedTable(Identifier ident) throws NoSuchTableException {
    // Transform identifier for API call
    Identifier actualIdent = transformIdentifierForApi(ident);

    Map<String, String> tableProps;
    if (impl.equals("dir") || impl.equals(MemoryLanceNamespace.SCHEME)) {
      tableProps = namespaceProperties(namespace, actualIdent);
    } else if (impl.equals("hive3") || impl.equals("hive2")) {
      tableProps = tableProperties(namespace, actualIdent);
    } else {
      throw new IllegalArgumentException(
          "Unsupported namespace type for partition table: " + namespace.getClass());
    }

    String location = tableProps.get(LANCE_PARTITION_TABLE_ROOT);
    if (location == null) {
      throw new NoSuchTableException(actualIdent);
    }

    LancePartitionConfig config =
        LancePartitionConfig.from(tableProps, actualIdent.name(), location);
    return new PartitionedLanceDataset(config);
  }

  /**
   * Creates LanceSparkReadOptions with namespace settings for this catalog.
   *
   * @param location the dataset location URI
   * @param tableId the table identifier within the namespace
   * @return a new LanceSparkReadOptions with all catalog settings
   */
  private LanceSparkReadOptions createReadOptions(String location, List<String> tableId) {
    return LanceSparkReadOptions.builder()
        .datasetUri(location)
        .withCatalogDefaults(catalogConfig)
        .namespace(namespace)
        .tableId(tableId)
        .build();
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    // Check if this is a partitioned table
    if (partitions == null || partitions.length == 0) {
      return createNonPartitionedTable(ident, schema, properties);
    } else {
      return createPartitionedTable(ident, schema, partitions, properties);
    }
  }

  private Table createNonPartitionedTable(
      Identifier ident, StructType schema, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    Identifier actualIdent = transformIdentifierForApi(ident);

    // Build the table ID for credential vending
    List<String> tableIdList = buildTableId(actualIdent);

    StructType processedSchema = SchemaConverter.processSchemaWithProperties(schema, properties);

    // Create dataset using namespace - WriteDatasetBuilder handles createEmptyTable internally
    Dataset.write()
        .allocator(LanceRuntime.allocator())
        .namespace(namespace)
        .tableId(tableIdList)
        .schema(LanceArrowUtils.toArrowSchema(processedSchema, "UTC", true, false))
        .mode(WriteParams.WriteMode.CREATE)
        .execute()
        .close();

    // Open the created dataset to get location for config
    String location;
    try (Dataset dataset =
        Dataset.open()
            .allocator(LanceRuntime.allocator())
            .namespace(namespace)
            .tableId(tableIdList)
            .build()) {
      location = dataset.uri();
    }

    // Create read options with namespace settings
    LanceSparkReadOptions readOptions = createReadOptions(location, tableIdList);
    return createDataset(readOptions, processedSchema);
  }

  private Table createPartitionedTable(
      Identifier ident,
      StructType sparkSchema,
      Transform[] partitions,
      Map<String, String> properties) {
    Identifier tableId = transformIdentifierForApi(ident);

    // Set location.
    // For partitioned table, user must specify the table location. This is a big different from
    // unpartitioned table, which accepts both table location and none table location.
    String location = properties.get(CREATE_TABLE_PROPERTY_LOCATION);
    Preconditions.checkArgument(
        location != null && !location.isEmpty(), "Table location must be specified");

    // Create partitioned table.
    Table table;
    try {
      table =
          PartitionUtils.createEmptyPartitionTable(
              "table", location, sparkSchema, partitions, properties);
    } catch (NoSuchTableException e) {
      throw new RuntimeException("Failed to create partitioned table", e);
    }

    // Register partitioned table to namespace.
    Map<String, String> options = new HashMap<>(properties);
    options.put(LANCE_PARTITION_TABLE_ROOT, location);

    if (impl.equals("dir") || impl.equals(MemoryLanceNamespace.SCHEME)) {
      CreateNamespaceRequest request = new CreateNamespaceRequest();
      request.id(identToId(tableId));
      request.setProperties(options);
      namespace.createNamespace(request);
    } else if (impl.equals("hive3") || impl.equals("hive2")) {
      CreateEmptyTableRequest request = new CreateEmptyTableRequest();
      request.id(identToId(tableId));
      request.setProperties(options);
      namespace.createEmptyTable(request);
    } else {
      throw new IllegalArgumentException(
          "Unsupported namespace type for partition table: " + namespace.getClass());
    }

    return table;
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException("Table alteration is not supported");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    Identifier tableId = transformIdentifierForApi(ident);
    if (dropNonPartitionedTable(tableId)) {
      return true;
    }
    return dropPartitionedTable(tableId);
  }

  private boolean dropNonPartitionedTable(Identifier tableId) {
    try {
      DropTableRequest dropRequest = new DropTableRequest();
      dropRequest.id(identToId(tableId));
      namespace.dropTable(dropRequest);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean dropPartitionedTable(Identifier tableId) {
    try {
      if (PartitionUtils.isPartitionTable(namespace, impl, tableId)) {
        if (impl.equals("dir") || impl.equals(MemoryLanceNamespace.SCHEME)) {
          dropNamespace(identToId(tableId).toArray(new String[0]), true);
        } else if (impl.equals("hive3") || impl.equals("hive2")) {
          dropTable(tableId);
        } else {
          throw new IllegalArgumentException(
              "Unsupported namespace type for partition table: " + namespace.getClass());
        }
      }
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Table renaming is not supported");
  }

  /**
   * Removes the extra level from a Spark identifier if it matches the configured extra level name.
   * For example, with extraLevelName="default": - ["default", "table"] -> ["table"] - ["default"]
   * -> [] (root namespace) - ["other", "table"] -> ["other", "table"] (unchanged)
   */
  private Identifier removeExtraLevelsFromId(Identifier identifier) {
    if (extraLevel.isEmpty()) {
      return identifier;
    }

    String[] newNamespace = removeExtraLevelsFromNamespace(identifier.namespace());
    return Identifier.of(newNamespace, identifier.name());
  }

  /** Transforms an identifier for API calls by removing extra levels and adding parent prefix. */
  private Identifier transformIdentifierForApi(Identifier identifier) {
    Identifier transformed = removeExtraLevelsFromId(identifier);
    String[] namespace = addParentPrefix(transformed.namespace());
    return Identifier.of(namespace, transformed.name());
  }

  /**
   * Removes the extra level from a namespace array if it matches the configured extra level name.
   * For example, with extraLevel="default": - ["default"] -> [] - ["default", "subnamespace"] ->
   * ["subnamespace"] - ["other"] -> ["other"] (unchanged)
   */
  private String[] removeExtraLevelsFromNamespace(String[] namespace) {
    if (extraLevel.isEmpty()) {
      return namespace;
    }

    String extraLevelName = this.extraLevel.get();

    // Check if the first namespace part matches the extra level
    if (namespace.length > 0 && extraLevelName.equals(namespace[0])) {
      // Remove the extra level from namespace
      String[] newNamespace = new String[namespace.length - 1];
      System.arraycopy(namespace, 1, newNamespace, 0, namespace.length - 1);
      return newNamespace;
    }

    return namespace;
  }

  /**
   * Determines whether to use extra level for REST implementations by testing ListNamespaces. If
   * ListNamespaces fails, assumes flat namespace structure and returns "default".
   *
   * @return Optional containing "default" if ListNamespaces fails, empty otherwise
   */
  private Optional<String> determineExtraLevelForRest() {
    try {
      org.lance.namespace.model.ListNamespacesRequest request =
          new org.lance.namespace.model.ListNamespacesRequest();
      namespace.listNamespaces(request);
      return Optional.empty();
    } catch (Exception e) {
      logger.info(
          "REST namespace ListNamespaces failed, "
              + "falling back to flat table structure with extra_level=default");
      return Optional.of("default");
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

  public abstract LanceDataset createDataset(
      LanceSparkReadOptions readOptions, StructType sparkSchema);
}
