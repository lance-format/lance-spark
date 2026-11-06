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

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Base class for Lance's session catalog implementation. Wraps an internal {@link
 * BaseLanceNamespaceSparkCatalog} (the standalone Lance catalog) and a delegate session catalog,
 * routing operations between them based on the table provider.
 *
 * <p>When the provider is "lance", operations are routed to the Lance catalog. When the provider is
 * something else (e.g., "parquet", "orc"), operations go to the delegate. When no provider is
 * specified, behavior is controlled by the {@code default-provider} configuration.
 *
 * <p>Subclasses must implement {@link #buildLanceCatalog()} to provide the version-specific Lance
 * catalog instance.
 */
public abstract class BaseLanceNamespaceSparkSessionCatalog
    implements CatalogExtension, StagingTableCatalog {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseLanceNamespaceSparkSessionCatalog.class);

  private static final Set<String> VALID_DEFAULT_PROVIDERS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("delegate", "lance", "error")));

  private String catalogName;
  private String defaultProvider;
  private BaseLanceNamespaceSparkCatalog lanceCatalog;
  private StagingTableCatalog asStagingLanceCatalog;
  private TableCatalog sessionCatalog;
  private FunctionCatalog sessionFunctionCatalog;
  private SupportsNamespaces sessionNamespaceCatalog;

  /**
   * Builds the version-specific Lance catalog instance. Called during {@link #initialize} — the
   * returned catalog will have {@code initialize(name + "$lance", options)} called on it
   * immediately after construction.
   */
  protected abstract BaseLanceNamespaceSparkCatalog buildLanceCatalog();

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;

    // Validate and store default-provider (normalized to lowercase)
    String dp = options.getOrDefault("default-provider", "delegate").toLowerCase();
    if (!VALID_DEFAULT_PROVIDERS.contains(dp)) {
      throw new IllegalArgumentException(
          "Invalid default-provider '" + dp + "'. Valid values: " + VALID_DEFAULT_PROVIDERS);
    }
    this.defaultProvider = dp;

    if (!options.containsKey("default-provider")) {
      LOG.info(
          "No explicit default-provider set for catalog '{}'. "
              + "Defaulting to 'delegate' (tables without USING clause go to the session catalog).",
          name);
    }

    // Build and initialize the internal Lance catalog with a derived name
    this.lanceCatalog = buildLanceCatalog();
    this.lanceCatalog.initialize(name + "$lance", options);

    if (lanceCatalog instanceof StagingTableCatalog) {
      this.asStagingLanceCatalog = (StagingTableCatalog) lanceCatalog;
    }
  }

  @Override
  public void setDelegateCatalog(CatalogPlugin delegate) {
    if (!(delegate instanceof TableCatalog)) {
      throw new IllegalArgumentException(
          "Delegate catalog must implement TableCatalog, got: " + delegate.getClass().getName());
    }
    if (!(delegate instanceof SupportsNamespaces)) {
      throw new IllegalArgumentException(
          "Delegate catalog must implement SupportsNamespaces, got: "
              + delegate.getClass().getName());
    }

    this.sessionCatalog = (TableCatalog) delegate;
    this.sessionNamespaceCatalog = (SupportsNamespaces) delegate;

    if (delegate instanceof FunctionCatalog) {
      this.sessionFunctionCatalog = (FunctionCatalog) delegate;
      detectFunctionCollisions(this.sessionFunctionCatalog);
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public String[] defaultNamespace() {
    return new String[] {"default"};
  }

  // ---------------------------------------------------------------------------
  // Routing helpers
  // ---------------------------------------------------------------------------

  /**
   * Determines whether an operation with the given provider should be routed to the Lance catalog.
   *
   * @param provider the table provider (e.g., "lance", "parquet"), or {@code null} if not specified
   * @return {@code true} if the operation should go to the Lance catalog
   */
  private boolean useLance(String provider) {
    if ("lance".equalsIgnoreCase(provider)) {
      return true;
    }
    if (provider != null) {
      return false;
    }
    // provider is null — use default-provider to decide
    if ("error".equals(defaultProvider)) {
      throw new RuntimeException(
          "No table provider specified. Set spark.sql.catalog."
              + catalogName
              + ".default-provider or add USING lance/parquet to your statement.");
    }
    boolean result = "lance".equals(defaultProvider);
    if (result) {
      LOG.warn("Creating table as Lance (no explicit USING clause; default-provider=lance).");
    }
    return result;
  }

  private TableCatalog getSessionCatalog() {
    if (sessionCatalog == null) {
      throw new IllegalStateException(
          "Delegated session catalog is missing. "
              + "Please make sure you are replacing Spark's default catalog, "
              + "named 'spark_catalog'.");
    }
    return sessionCatalog;
  }

  private SupportsNamespaces getSessionNamespaceCatalog() {
    if (sessionNamespaceCatalog == null) {
      throw new IllegalStateException("Delegated session catalog is missing.");
    }
    return sessionNamespaceCatalog;
  }

  // ---------------------------------------------------------------------------
  // TableCatalog — table operations
  // ---------------------------------------------------------------------------

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      return lanceCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      return getSessionCatalog().loadTable(ident);
    }
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    try {
      return lanceCatalog.loadTable(ident, timestamp);
    } catch (NoSuchTableException e) {
      return getSessionCatalog().loadTable(ident, timestamp);
    }
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    try {
      return lanceCatalog.loadTable(ident, version);
    } catch (NoSuchTableException e) {
      return getSessionCatalog().loadTable(ident, version);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get("provider");
    if (useLance(provider)) {
      return lanceCatalog.createTable(ident, schema, partitions, properties);
    }
    return getSessionCatalog().createTable(ident, schema, partitions, properties);
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    // Routes based on table existence. Cross-provider alter (e.g., converting a Parquet table
    // to Lance) is not supported — use CREATE TABLE ... AS SELECT to migrate between providers.
    if (lanceCatalog.tableExists(ident)) {
      return lanceCatalog.alterTable(ident, changes);
    }
    return getSessionCatalog().alterTable(ident, changes);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    boolean droppedFromLance = lanceCatalog.dropTable(ident);
    if (droppedFromLance) {
      if (getSessionCatalog().tableExists(ident)) {
        LOG.warn(
            "Table '{}' exists in both Lance and delegate catalog. "
                + "Dropped from Lance only. To drop the delegate copy, access the "
                + "underlying catalog directly (e.g., Hive metastore CLI or "
                + "spark.sessionState.catalog).",
            ident);
      }
      return true;
    }
    return getSessionCatalog().dropTable(ident);
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    boolean purgedFromLance = lanceCatalog.purgeTable(ident);
    if (purgedFromLance) {
      if (getSessionCatalog().tableExists(ident)) {
        LOG.error(
            "Table '{}' exists in both Lance and delegate catalog. "
                + "Purged from Lance only (irrecoverable). Delegate catalog copy remains. "
                + "Manual cleanup required via the underlying catalog.",
            ident);
      }
      return true;
    }
    return getSessionCatalog().purgeTable(ident);
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    if (lanceCatalog.tableExists(oldIdent)) {
      lanceCatalog.renameTable(oldIdent, newIdent);
      return;
    }
    getSessionCatalog().renameTable(oldIdent, newIdent);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    return lanceCatalog.tableExists(ident) || getSessionCatalog().tableExists(ident);
  }

  @Override
  public void invalidateTable(Identifier ident) {
    lanceCatalog.invalidateTable(ident);
    getSessionCatalog().invalidateTable(ident);
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    // Always delegate listing to the session catalog. Lance-only tables will not appear
    // in this listing since they are stored in the Lance namespace backend, not the
    // delegate's metastore. This matches Iceberg's session catalog behavior.
    return getSessionCatalog().listTables(namespace);
  }

  // ---------------------------------------------------------------------------
  // StagingTableCatalog — staging operations
  // ---------------------------------------------------------------------------

  @Override
  public StagedTable stageCreate(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get("provider");
    TableCatalog catalog;
    if (useLance(provider)) {
      if (asStagingLanceCatalog != null) {
        return asStagingLanceCatalog.stageCreate(ident, schema, partitions, properties);
      }
      catalog = lanceCatalog;
    } else {
      TableCatalog delegate = getSessionCatalog();
      if (delegate instanceof StagingTableCatalog) {
        return ((StagingTableCatalog) delegate).stageCreate(ident, schema, partitions, properties);
      }
      catalog = delegate;
    }

    Table table = catalog.createTable(ident, schema, partitions, properties);
    return new RollbackStagedTable(catalog, ident, table);
  }

  @Override
  public StagedTable stageReplace(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchTableException {
    String provider = properties.get("provider");
    TableCatalog catalog;
    if (useLance(provider)) {
      if (asStagingLanceCatalog != null) {
        return asStagingLanceCatalog.stageReplace(ident, schema, partitions, properties);
      }
      catalog = lanceCatalog;
    } else {
      TableCatalog delegate = getSessionCatalog();
      if (delegate instanceof StagingTableCatalog) {
        return ((StagingTableCatalog) delegate).stageReplace(ident, schema, partitions, properties);
      }
      catalog = delegate;
    }

    if (!catalog.dropTable(ident)) {
      throw new NoSuchTableException(ident);
    }

    try {
      Table table = catalog.createTable(ident, schema, partitions, properties);
      return new RollbackStagedTable(catalog, ident, table);
    } catch (TableAlreadyExistsException e) {
      throw new RuntimeException("Race condition: table recreated after drop", e);
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException {
    String provider = properties.get("provider");
    TableCatalog catalog;
    if (useLance(provider)) {
      if (asStagingLanceCatalog != null) {
        return asStagingLanceCatalog.stageCreateOrReplace(ident, schema, partitions, properties);
      }
      catalog = lanceCatalog;
    } else {
      TableCatalog delegate = getSessionCatalog();
      if (delegate instanceof StagingTableCatalog) {
        return ((StagingTableCatalog) delegate)
            .stageCreateOrReplace(ident, schema, partitions, properties);
      }
      catalog = delegate;
    }

    catalog.dropTable(ident);

    try {
      Table table = catalog.createTable(ident, schema, partitions, properties);
      return new RollbackStagedTable(catalog, ident, table);
    } catch (TableAlreadyExistsException e) {
      throw new RuntimeException("Race condition: table recreated after drop", e);
    }
  }

  // ---------------------------------------------------------------------------
  // SupportsNamespaces — all delegated to session catalog
  // ---------------------------------------------------------------------------

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return getSessionNamespaceCatalog().listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return getSessionNamespaceCatalog().listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    return getSessionNamespaceCatalog().loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    getSessionNamespaceCatalog().createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    getSessionNamespaceCatalog().alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    return getSessionNamespaceCatalog().dropNamespace(namespace, cascade);
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    return getSessionNamespaceCatalog().namespaceExists(namespace);
  }

  // ---------------------------------------------------------------------------
  // FunctionCatalog — combine Lance + delegate
  // ---------------------------------------------------------------------------

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    Set<Identifier> functions = new LinkedHashSet<>();

    // Add Lance functions — catch exception since the namespace may only exist in the delegate
    try {
      Identifier[] lanceFunctions = lanceCatalog.listFunctions(namespace);
      for (Identifier fn : lanceFunctions) {
        functions.add(fn);
      }
    } catch (NoSuchNamespaceException e) {
      // Namespace may not exist in Lance; continue to collect delegate functions
    }

    // Add delegate functions if available
    if (sessionFunctionCatalog != null) {
      try {
        Identifier[] delegateFunctions = sessionFunctionCatalog.listFunctions(namespace);
        for (Identifier fn : delegateFunctions) {
          functions.add(fn);
        }
      } catch (NoSuchNamespaceException e) {
        // Delegate may not recognize the namespace; ignore
      }
    }

    // If no functions were found, verify the namespace actually exists
    if (functions.isEmpty()) {
      if (!namespaceExists(namespace)) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    return functions.toArray(new Identifier[0]);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    // Try Lance first
    try {
      return lanceCatalog.loadFunction(ident);
    } catch (NoSuchFunctionException e) {
      // Fall through to delegate
    }

    if (sessionFunctionCatalog != null) {
      return sessionFunctionCatalog.loadFunction(ident);
    }

    throw new NoSuchFunctionException(ident);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Detects function name collisions between the Lance catalog and the delegate function catalog.
   * Logs a warning for each collision so users are aware that the Lance function will take
   * precedence.
   */
  private void detectFunctionCollisions(FunctionCatalog delegate) {
    try {
      // Only checks root-namespace functions. Lance currently registers all functions at root
      // level.
      Identifier[] lanceFunctions = lanceCatalog.listFunctions(new String[0]);
      for (Identifier fn : lanceFunctions) {
        try {
          delegate.loadFunction(fn);
          LOG.warn(
              "Function '{}' exists in both Lance and delegate catalogs. "
                  + "The Lance version will take precedence.",
              fn);
        } catch (NoSuchFunctionException e) {
          // No collision — expected
        } catch (Exception e) {
          // Delegate may not support the function identifier format (e.g., V2SessionCatalog
          // requires single-part namespace for function lookups). Skip this function.
          LOG.debug("Skipping collision check for function '{}': {}", fn, e.getMessage());
        }
      }
    } catch (Exception e) {
      // Cannot enumerate Lance functions — skip collision detection
      LOG.debug("Skipping function collision detection: {}", e.getMessage());
    }
  }

  // ---------------------------------------------------------------------------
  // RollbackStagedTable — wraps a committed table for non-staging catalogs
  // ---------------------------------------------------------------------------

  /**
   * A {@link StagedTable} wrapper for catalogs that do not natively support staging. The table has
   * already been committed (created), so {@link #commitStagedChanges()} is a no-op and {@link
   * #abortStagedChanges()} drops the table to roll back.
   */
  private static class RollbackStagedTable implements StagedTable {
    private final TableCatalog catalog;
    private final Identifier ident;
    private final Table table;

    RollbackStagedTable(TableCatalog catalog, Identifier ident, Table table) {
      this.catalog = catalog;
      this.ident = ident;
      this.table = table;
    }

    @Override
    public void commitStagedChanges() {
      // Table was already created — nothing to do.
    }

    @Override
    public void abortStagedChanges() {
      catalog.dropTable(ident);
    }

    @Override
    public String name() {
      return table.name();
    }

    @Override
    public StructType schema() {
      return table.schema();
    }

    @Override
    public Map<String, String> properties() {
      return table.properties();
    }

    @Override
    public Transform[] partitioning() {
      return table.partitioning();
    }

    @Override
    public Set<TableCapability> capabilities() {
      return table.capabilities();
    }
  }
}
