"""
Automated integration tests for Lance-Spark.

These tests run inside the Docker container against a real Spark + MinIO environment.

Test organization follows the Lance documentation structure:
- DDL (Data Definition Language): Namespace, Table, Index, Optimize, Vacuum operations
- DQL (Data Query Language): SELECT queries and data retrieval
- DML (Data Manipulation Language): INSERT, UPDATE, DELETE, MERGE, ADD COLUMN operations
"""

import os
import time
import pytest
from packaging.version import Version
from pyspark.sql import SparkSession

SPARK_VERSION = Version(os.environ.get("SPARK_VERSION", "3.5"))


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session configured with Lance catalog."""
    session = (
        SparkSession.builder
        .appName("LanceSparkTests")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "dir")
        .config("spark.sql.catalog.lance.root", "/home/lance/data")
        .config("spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
        .getOrCreate()
    )
    session.sql("SET spark.sql.defaultCatalog=lance")
    yield session
    session.stop()


def drop_table(spark, table_name):
    """Drop a table, using PURGE only on Spark >= 3.5."""
    purge = " PURGE" if SPARK_VERSION >= Version("3.5") else ""
    spark.sql(f"DROP TABLE IF EXISTS {table_name}{purge}")


@pytest.fixture(autouse=True)
def cleanup_tables(spark):
    """Clean up test tables before and after each test."""
    drop_table(spark, "default.test_table")
    drop_table(spark, "default.employees")
    # TODO - reenable once `tableExists` works on Spark 4.0
    #spark.catalog.dropTempView("source") if spark.catalog.tableExists("source") else None
    #spark.catalog.dropTempView("tmp_view") if spark.catalog.tableExists("tmp_view") else None
    spark.catalog.dropTempView("source")
    spark.catalog.dropTempView("tmp_view")
    yield
    drop_table(spark, "default.test_table")
    drop_table(spark, "default.employees")
    # TODO - reenable once `tableExists` works on Spark 4.0
    #spark.catalog.dropTempView("source") if spark.catalog.tableExists("source") else None
    #spark.catalog.dropTempView("tmp_view") if spark.catalog.tableExists("tmp_view") else None
    spark.catalog.dropTempView("source")
    spark.catalog.dropTempView("tmp_view")


# =============================================================================
# DDL (Data Definition Language) Tests
# =============================================================================

class TestDDLNamespace:
    """Test DDL namespace operations: CREATE, DROP, SHOW, DESCRIBE NAMESPACE."""

    def test_show_catalogs(self, spark):
        """Verify Lance catalog is registered."""
        catalogs = spark.sql("SHOW CATALOGS").collect()
        catalog_names = [row[0] for row in catalogs]
        assert "lance" in catalog_names

    def test_create_namespace(self, spark):
        """Test CREATE NAMESPACE."""
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        namespaces = spark.sql("SHOW NAMESPACES").collect()
        namespace_names = [row[0] for row in namespaces]
        assert "default" in namespace_names

    def test_show_namespaces(self, spark):
        """Test SHOW NAMESPACES."""
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        namespaces = spark.sql("SHOW NAMESPACES").collect()
        assert len(namespaces) >= 1
        namespace_names = [row[0] for row in namespaces]
        assert "default" in namespace_names


class TestDDLTable:
    """Test DDL table operations: CREATE, SHOW, DESCRIBE, DROP TABLE."""

    def test_create_table(self, spark):
        """Test CREATE TABLE."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        tables = spark.sql("SHOW TABLES IN default").collect()
        table_names = [row.tableName for row in tables]
        assert "test_table" in table_names

    def test_show_tables(self, spark):
        """Test SHOW TABLES."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING
            )
        """)

        tables = spark.sql("SHOW TABLES IN default").collect()
        assert len(tables) >= 1
        table_names = [row.tableName for row in tables]
        assert "test_table" in table_names

    def test_describe_table(self, spark):
        """Test DESCRIBE TABLE."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        schema = spark.sql("DESCRIBE TABLE default.test_table").collect()
        col_names = [row.col_name for row in schema if row.col_name and not row.col_name.startswith("#")]
        assert "id" in col_names
        assert "name" in col_names
        assert "value" in col_names

    def test_drop_table(self, spark):
        """Test DROP TABLE."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING
            )
        """)

        drop_table(spark, "default.test_table")

        tables = spark.sql("SHOW TABLES IN default").collect()
        table_names = [row.tableName for row in tables]
        assert "test_table" not in table_names


class TestDDLIndex:
    """Test DDL index operations: CREATE INDEX (BTree, FTS)."""

    def test_create_btree_index_on_int(self, spark):
        """Test CREATE INDEX with BTree on integer column."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        # Insert data first (index requires data)
        data = [(i, f"Name{i}", float(i * 10)) for i in range(100)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.writeTo("default.test_table").append()

        # Create BTree index on id column
        result = spark.sql("""
            ALTER TABLE default.test_table
            CREATE INDEX idx_id USING btree (id)
        """).collect()

        # Verify index was created (returns fragment count and index name)
        assert len(result) == 1
        assert result[0][1] == "idx_id"

        # Verify queries still work after indexing
        query_result = spark.sql("""
            SELECT * FROM default.test_table WHERE id = 50
        """).collect()
        assert len(query_result) == 1
        assert query_result[0].id == 50

    def test_create_btree_index_on_string(self, spark):
        """Test CREATE INDEX with BTree on string column."""
        spark.sql("""
            CREATE TABLE default.employees (
                id INT,
                name STRING,
                department STRING,
                salary INT
            )
        """)

        data = [
            (1, "Alice", "Engineering", 75000),
            (2, "Bob", "Marketing", 65000),
            (3, "Charlie", "Engineering", 70000),
            (4, "Diana", "Sales", 80000),
            (5, "Eve", "Engineering", 60000),
        ]
        df = spark.createDataFrame(data, ["id", "name", "department", "salary"])
        df.writeTo("default.employees").append()

        # Create BTree index on department column
        result = spark.sql("""
            ALTER TABLE default.employees
            CREATE INDEX idx_dept USING btree (department)
        """).collect()

        assert len(result) == 1
        assert result[0][1] == "idx_dept"

        # Query using the indexed column
        query_result = spark.sql("""
            SELECT * FROM default.employees WHERE department = 'Engineering'
        """).collect()
        assert len(query_result) == 3

    def test_create_fts_index(self, spark):
        """Test CREATE INDEX with full-text search (FTS)."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                title STRING,
                content STRING
            )
        """)

        data = [
            (1, "Introduction to Python", "Python is a programming language"),
            (2, "Java Basics", "Java is an object-oriented language"),
            (3, "Python Advanced", "Advanced Python topics like decorators"),
            (4, "Database Design", "SQL and database normalization"),
            (5, "Web Development", "Building web applications with Python"),
        ]
        df = spark.createDataFrame(data, ["id", "title", "content"])
        df.writeTo("default.test_table").append()

        # Create FTS index on content column
        result = spark.sql("""
            ALTER TABLE default.test_table
            CREATE INDEX idx_content_fts USING fts (content)
            WITH ( base_tokenizer = 'simple', language = 'English' )
        """).collect()

        assert len(result) == 1
        assert result[0][1] == "idx_content_fts"

    def test_create_index_empty_table(self, spark):
        """Test CREATE INDEX on empty table."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING
            )
        """)

        # Creating index on empty table should return 0 fragments indexed
        result = spark.sql("""
            ALTER TABLE default.test_table
            CREATE INDEX idx_id USING btree (id)
        """).collect()

        # Should return with 0 fragments indexed
        assert result[0][0] == 0


class TestDDLOptimize:
    """Test DDL OPTIMIZE operations for compacting table fragments."""

    def test_optimize_without_args(self, spark):
        """Test OPTIMIZE without options."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        # Insert data in multiple batches to create multiple fragments
        for batch in range(5):
            data = [(batch * 10 + i, f"Name{batch * 10 + i}", batch * 10 + i) for i in range(10)]
            df = spark.createDataFrame(data, ["id", "name", "value"])
            df.writeTo("default.test_table").append()

        # Run OPTIMIZE
        result = spark.sql("OPTIMIZE default.test_table").collect()

        # Verify output schema and that compaction occurred
        assert len(result) == 1
        row = result[0]
        # OPTIMIZE returns: fragments_removed, fragments_added, files_removed, files_added
        assert row.fragments_removed >= 0
        assert row.fragments_added >= 0
        assert row.files_removed >= 0
        assert row.files_added >= 0

        # Verify data integrity after optimization
        count = spark.table("default.test_table").count()
        assert count == 50  # 5 batches * 10 rows

    def test_optimize_with_target_rows(self, spark):
        """Test OPTIMIZE with target_rows_per_fragment option."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        # Insert data in multiple small batches
        for batch in range(5):
            data = [(batch * 10 + i, f"Name{batch * 10 + i}", batch * 10 + i) for i in range(10)]
            df = spark.createDataFrame(data, ["id", "name", "value"])
            df.writeTo("default.test_table").append()

        # Optimize with target rows per fragment
        result = spark.sql("""
            OPTIMIZE default.test_table WITH (target_rows_per_fragment = 100)
        """).collect()

        assert len(result) == 1
        row = result[0]
        assert row.fragments_removed >= 0
        assert row.fragments_added >= 0

        # Verify data integrity
        count = spark.table("default.test_table").count()
        assert count == 50

    def test_optimize_with_multiple_options(self, spark):
        """Test OPTIMIZE with multiple options."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        # Insert data
        for batch in range(3):
            data = [(batch * 20 + i, f"Name{batch * 20 + i}", batch * 20 + i) for i in range(20)]
            df = spark.createDataFrame(data, ["id", "name", "value"])
            df.writeTo("default.test_table").append()

        # Optimize with multiple options
        result = spark.sql("""
            OPTIMIZE default.test_table WITH (
                target_rows_per_fragment = 100,
                num_threads = 2,
                materialize_deletions = true
            )
        """).collect()

        assert len(result) == 1
        row = result[0]
        assert row.fragments_removed >= 0
        assert row.fragments_added >= 0

        # Verify data integrity
        count = spark.table("default.test_table").count()
        assert count == 60  # 3 batches * 20 rows

    def test_optimize_after_deletes(self, spark):
        """Test OPTIMIZE after DELETE to materialize soft deletes."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        # Insert data
        data = [(i, f"Name{i}", i) for i in range(100)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.writeTo("default.test_table").append()

        # Delete some rows
        spark.sql("DELETE FROM default.test_table WHERE id < 20")

        # Optimize to materialize deletions
        result = spark.sql("""
            OPTIMIZE default.test_table WITH (materialize_deletions = true)
        """).collect()

        assert len(result) == 1

        # Verify correct row count after optimization
        count = spark.table("default.test_table").count()
        assert count == 80  # 100 - 20 deleted

    def test_optimize_empty_table(self, spark):
        """Test OPTIMIZE on empty table."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING
            )
        """)

        # Optimize empty table should succeed without error
        result = spark.sql("OPTIMIZE default.test_table").collect()

        assert len(result) == 1
        row = result[0]
        # Empty table should have 0 fragments to compact
        assert row.fragments_removed == 0
        assert row.fragments_added == 0


class TestDDLVacuum:
    """Test DDL VACUUM operations for removing old versions."""

    def test_vacuum_without_args(self, spark):
        """Test VACUUM without options."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        # Insert data to create a version
        data = [(i, f"Name{i}", i) for i in range(10)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.writeTo("default.test_table").append()

        # Run VACUUM
        result = spark.sql("VACUUM default.test_table").collect()

        # Verify output schema
        assert len(result) == 1
        row = result[0]
        # VACUUM returns: bytes_removed, old_versions
        assert row.bytes_removed >= 0
        assert row.old_versions >= 0

    def test_vacuum_with_before_version(self, spark):
        """Test VACUUM with before_version option."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        # Create multiple versions by inserting data multiple times
        for version in range(5):
            data = [(version * 10 + i, f"Name{version * 10 + i}", version * 10 + i) for i in range(10)]
            df = spark.createDataFrame(data, ["id", "name", "value"])
            df.writeTo("default.test_table").append()

        # Vacuum with before_version to remove old versions
        result = spark.sql("""
            VACUUM default.test_table WITH (before_version = 1000000)
        """).collect()

        assert len(result) == 1
        row = result[0]
        # With multiple versions, we expect some cleanup
        assert row.bytes_removed >= 0
        assert row.old_versions >= 0

        # Verify data is still accessible
        count = spark.table("default.test_table").count()
        assert count == 50  # 5 versions * 10 rows

    def test_vacuum_with_timestamp(self, spark):
        """Test VACUUM with before_timestamp_millis option."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        # Insert initial data
        data = [(i, f"Name{i}", i) for i in range(10)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.writeTo("default.test_table").append()

        # Small delay to separate versions
        time.sleep(0.1)
        before_ts = int(time.time() * 1000)

        # Insert more data to create newer versions
        for version in range(3):
            data = [(100 + version * 10 + i, f"NewName{version * 10 + i}", version * 10 + i) for i in range(10)]
            df = spark.createDataFrame(data, ["id", "name", "value"])
            df.writeTo("default.test_table").append()

        # Vacuum versions older than the timestamp
        result = spark.sql(f"""
            VACUUM default.test_table WITH (before_timestamp_millis = {before_ts})
        """).collect()

        assert len(result) == 1
        row = result[0]
        assert row.bytes_removed >= 0
        assert row.old_versions >= 0

        # Verify current data is accessible
        count = spark.table("default.test_table").count()
        assert count == 40  # 10 initial + 3 versions * 10 rows

    def test_vacuum_after_optimize(self, spark):
        """Test VACUUM after OPTIMIZE to clean up removed fragments."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        # Create fragmented data
        for batch in range(5):
            data = [(batch * 10 + i, f"Name{batch * 10 + i}", batch * 10 + i) for i in range(10)]
            df = spark.createDataFrame(data, ["id", "name", "value"])
            df.writeTo("default.test_table").append()

        # Optimize to compact fragments
        spark.sql("""
            OPTIMIZE default.test_table WITH (target_rows_per_fragment = 100)
        """)

        # Vacuum to remove old fragment files
        result = spark.sql("""
            VACUUM default.test_table WITH (before_version = 1000000)
        """).collect()

        assert len(result) == 1
        row = result[0]
        # After optimize, vacuum should clean up the old fragments
        assert row.bytes_removed >= 0
        assert row.old_versions >= 0

        # Verify data integrity
        count = spark.table("default.test_table").count()
        assert count == 50

    def test_vacuum_preserves_current_data(self, spark):
        """Test VACUUM preserves all current data."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        # Insert and update data multiple times
        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 100),
            (2, 'Bob', 200),
            (3, 'Charlie', 300)
        """)

        # Update to create new versions
        spark.sql("UPDATE default.test_table SET value = 150 WHERE id = 1")
        spark.sql("UPDATE default.test_table SET value = 250 WHERE id = 2")

        # Delete and re-insert
        spark.sql("DELETE FROM default.test_table WHERE id = 3")
        spark.sql("INSERT INTO default.test_table VALUES (3, 'Charlie', 350)")

        # Vacuum
        result = spark.sql("""
            VACUUM default.test_table WITH (before_version = 1000000)
        """).collect()

        assert len(result) == 1

        # Verify all current data is preserved
        rows = spark.table("default.test_table").orderBy("id").collect()
        assert len(rows) == 3
        assert rows[0].id == 1 and rows[0].value == 150
        assert rows[1].id == 2 and rows[1].value == 250
        assert rows[2].id == 3 and rows[2].value == 350


# =============================================================================
# DQL (Data Query Language) Tests
# =============================================================================

class TestDQLSelect:
    """Test DQL SELECT operations."""

    def test_select_all(self, spark):
        """Test SELECT * query."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        data = [(1, "Alice", 10.5), (2, "Bob", 20.3), (3, "Charlie", 30.1)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.writeTo("default.test_table").append()

        result = spark.sql("SELECT * FROM default.test_table").collect()
        assert len(result) == 3

        ids = sorted([row.id for row in result])
        assert ids == [1, 2, 3]

    def test_select_with_where(self, spark):
        """Test SELECT with WHERE clause."""
        spark.sql("""
            CREATE TABLE default.employees (
                id INT,
                name STRING,
                age INT,
                department STRING,
                salary INT
            )
        """)

        data = [
            (1, "Alice", 25, "Engineering", 75000),
            (2, "Bob", 30, "Marketing", 65000),
            (3, "Charlie", 35, "Sales", 70000),
            (4, "Diana", 28, "Engineering", 80000),
            (5, "Eve", 32, "HR", 60000),
        ]
        df = spark.createDataFrame(data, ["id", "name", "age", "department", "salary"])
        df.writeTo("default.employees").append()

        result = spark.sql("""
            SELECT * FROM default.employees
            WHERE department = 'Engineering'
        """).collect()

        assert len(result) == 2
        names = sorted([row.name for row in result])
        assert names == ["Alice", "Diana"]

    def test_select_with_group_by(self, spark):
        """Test SELECT with GROUP BY aggregation."""
        spark.sql("""
            CREATE TABLE default.employees (
                id INT,
                name STRING,
                age INT,
                department STRING,
                salary INT
            )
        """)

        data = [
            (1, "Alice", 25, "Engineering", 75000),
            (2, "Bob", 30, "Marketing", 65000),
            (3, "Charlie", 35, "Sales", 70000),
            (4, "Diana", 28, "Engineering", 80000),
            (5, "Eve", 32, "HR", 60000),
        ]
        df = spark.createDataFrame(data, ["id", "name", "age", "department", "salary"])
        df.writeTo("default.employees").append()

        result = spark.sql("""
            SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
            FROM default.employees
            GROUP BY department
            ORDER BY count DESC
        """).collect()

        eng_row = [row for row in result if row.department == "Engineering"][0]
        assert eng_row["count"] == 2
        assert eng_row.avg_salary == 77500.0

    def test_select_with_order_by(self, spark):
        """Test SELECT with ORDER BY clause."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        data = [(3, "Charlie", 30.1), (1, "Alice", 10.5), (2, "Bob", 20.3)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.writeTo("default.test_table").append()

        result = spark.sql("""
            SELECT * FROM default.test_table ORDER BY id ASC
        """).collect()

        ids = [row.id for row in result]
        assert ids == [1, 2, 3]

    def test_select_with_limit(self, spark):
        """Test SELECT with LIMIT clause."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        data = [(i, f"Name{i}", float(i)) for i in range(10)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.writeTo("default.test_table").append()

        result = spark.sql("""
            SELECT * FROM default.test_table LIMIT 5
        """).collect()

        assert len(result) == 5

    def test_select_data_types(self, spark):
        """Test SELECT with various data types: INT, BIGINT, FLOAT, DOUBLE, STRING, BOOLEAN."""
        spark.sql("""
            CREATE TABLE default.test_table (
                int_col INT,
                long_col BIGINT,
                float_col FLOAT,
                double_col DOUBLE,
                string_col STRING,
                bool_col BOOLEAN
            )
        """)

        data = [(1, 100000000000, 1.5, 2.5, "test", True)]
        df = spark.createDataFrame(
            data,
            ["int_col", "long_col", "float_col", "double_col", "string_col", "bool_col"]
        )
        df.writeTo("default.test_table").append()

        result = spark.table("default.test_table").collect()[0]
        assert result.int_col == 1
        assert result.long_col == 100000000000
        assert abs(result.float_col - 1.5) < 0.01
        assert result.double_col == 2.5
        assert result.string_col == "test"
        assert result.bool_col is True


# =============================================================================
# DML (Data Manipulation Language) Tests
# =============================================================================

class TestDMLInsert:
    """Test DML INSERT INTO operations."""

    def test_insert_into_values(self, spark):
        """Test INSERT INTO with VALUES clause."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        # Insert using VALUES
        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 10.5),
            (2, 'Bob', 20.3),
            (3, 'Charlie', 30.1)
        """)

        result = spark.table("default.test_table").collect()
        assert len(result) == 3

        ids = sorted([row.id for row in result])
        assert ids == [1, 2, 3]

    def test_insert_into_select(self, spark):
        """Test INSERT INTO with SELECT clause."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        # Insert initial data
        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 10.5),
            (2, 'Bob', 20.3)
        """)

        # Insert from select (duplicate the data with modified ids)
        spark.sql("""
            INSERT INTO default.test_table
            SELECT id + 10, name, value * 2
            FROM default.test_table
        """)

        result = spark.table("default.test_table").collect()
        assert len(result) == 4

        ids = sorted([row.id for row in result])
        assert ids == [1, 2, 11, 12]

    def test_insert_append_data(self, spark):
        """Test INSERT by appending data with DataFrame API."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        data1 = [(1, "Alice", 10.5), (2, "Bob", 20.3)]
        df1 = spark.createDataFrame(data1, ["id", "name", "value"])
        df1.writeTo("default.test_table").append()

        data2 = [(3, "Charlie", 30.1), (4, "Diana", 40.2)]
        df2 = spark.createDataFrame(data2, ["id", "name", "value"])
        df2.writeTo("default.test_table").append()

        count = spark.table("default.test_table").count()
        assert count == 4


class TestDMLUpdate:
    """Test DML UPDATE SET operations."""

    def test_update_single_column(self, spark):
        """Test UPDATE SET single column."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 10),
            (2, 'Bob', 20),
            (3, 'Charlie', 30)
        """)

        # Update value for specific id
        spark.sql("""
            UPDATE default.test_table SET value = 100 WHERE id = 2
        """)

        result = spark.sql("""
            SELECT * FROM default.test_table WHERE id = 2
        """).collect()

        assert len(result) == 1
        assert result[0].value == 100

    def test_update_multiple_rows(self, spark):
        """Test UPDATE SET affecting multiple rows."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 10),
            (2, 'Bob', 20),
            (3, 'Charlie', 30),
            (4, 'Diana', 40)
        """)

        # Update all rows where value < 30
        spark.sql("""
            UPDATE default.test_table SET value = value + 100 WHERE value < 30
        """)

        result = spark.table("default.test_table").orderBy("id").collect()

        assert result[0].value == 110  # id=1: 10 + 100
        assert result[1].value == 120  # id=2: 20 + 100
        assert result[2].value == 30   # id=3: unchanged
        assert result[3].value == 40   # id=4: unchanged


class TestDMLDelete:
    """Test DML DELETE FROM operations."""

    def test_delete_with_condition(self, spark):
        """Test DELETE FROM with WHERE clause."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 10.5),
            (2, 'Bob', 20.3),
            (3, 'Charlie', 30.1),
            (4, 'Diana', 40.2),
            (5, 'Eve', 50.0)
        """)

        # Delete rows where id > 3
        spark.sql("""
            DELETE FROM default.test_table WHERE id > 3
        """)

        result = spark.table("default.test_table").collect()
        assert len(result) == 3

        ids = sorted([row.id for row in result])
        assert ids == [1, 2, 3]

    def test_delete_with_string_condition(self, spark):
        """Test DELETE FROM with string column condition."""
        spark.sql("""
            CREATE TABLE default.employees (
                id INT,
                name STRING,
                department STRING,
                salary INT
            )
        """)

        spark.sql("""
            INSERT INTO default.employees VALUES
            (1, 'Alice', 'Engineering', 75000),
            (2, 'Bob', 'Marketing', 65000),
            (3, 'Charlie', 'Engineering', 70000),
            (4, 'Diana', 'Sales', 80000)
        """)

        # Delete all Marketing employees
        spark.sql("""
            DELETE FROM default.employees WHERE department = 'Marketing'
        """)

        result = spark.table("default.employees").collect()
        assert len(result) == 3

        departments = [row.department for row in result]
        assert "Marketing" not in departments


class TestDMLMerge:
    """Test DML MERGE INTO operations."""

    def test_merge_into(self, spark):
        """Test MERGE INTO with WHEN MATCHED and WHEN NOT MATCHED."""
        # Create target table
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 10),
            (2, 'Bob', 20),
            (3, 'Charlie', 30)
        """)

        # Create source data as temp view
        source_data = [(2, "Bob_Updated", 200), (4, "Diana", 40)]
        source_df = spark.createDataFrame(source_data, ["id", "name", "value"])
        source_df.createOrReplaceTempView("source")

        # Merge: update matching rows, insert new rows
        spark.sql("""
            MERGE INTO default.test_table t
            USING source s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value
            WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)
        """)

        result = spark.table("default.test_table").orderBy("id").collect()

        assert len(result) == 4

        # Check updated row
        bob_row = [r for r in result if r.id == 2][0]
        assert bob_row.name == "Bob_Updated"
        assert bob_row.value == 200

        # Check inserted row
        diana_row = [r for r in result if r.id == 4][0]
        assert diana_row.name == "Diana"
        assert diana_row.value == 40


class TestDMLAddColumn:
    """Test DML ADD COLUMN FROM operations for schema evolution with backfill."""

    def test_add_column_from_view(self, spark):
        """Test ALTER TABLE ADD COLUMNS FROM with single column."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 100),
            (2, 'Bob', 200),
            (3, 'Charlie', 300)
        """)

        # Create temp view with _rowaddr, _fragid, and new column
        spark.sql("""
            CREATE TEMPORARY VIEW tmp_view AS
            SELECT _rowaddr, _fragid, value * 2 as doubled_value
            FROM default.test_table
        """)

        # Add column from the view
        spark.sql("""
            ALTER TABLE default.test_table ADD COLUMNS doubled_value FROM tmp_view
        """)

        # Verify the new column was added with correct values
        result = spark.sql("""
            SELECT id, name, value, doubled_value
            FROM default.test_table
            ORDER BY id
        """).collect()

        assert len(result) == 3
        assert result[0].doubled_value == 200  # 100 * 2
        assert result[1].doubled_value == 400  # 200 * 2
        assert result[2].doubled_value == 600  # 300 * 2

    def test_add_multiple_columns(self, spark):
        """Test ALTER TABLE ADD COLUMNS FROM with multiple columns."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 100),
            (2, 'Bob', 200),
            (3, 'Charlie', 300)
        """)

        # Create temp view with multiple new columns
        spark.sql("""
            CREATE TEMPORARY VIEW tmp_view AS
            SELECT _rowaddr, _fragid,
                   value * 2 as doubled,
                   value + 50 as plus_fifty,
                   CONCAT(name, '_suffix') as name_with_suffix
            FROM default.test_table
        """)

        # Add multiple columns
        spark.sql("""
            ALTER TABLE default.test_table ADD COLUMNS doubled, plus_fifty, name_with_suffix FROM tmp_view
        """)

        # Verify all columns were added
        result = spark.sql("""
            SELECT id, doubled, plus_fifty, name_with_suffix
            FROM default.test_table
            ORDER BY id
        """).collect()

        assert len(result) == 3
        assert result[0].doubled == 200
        assert result[0].plus_fifty == 150
        assert result[0].name_with_suffix == "Alice_suffix"
        assert result[1].doubled == 400
        assert result[1].plus_fifty == 250
        assert result[1].name_with_suffix == "Bob_suffix"

    def test_add_column_partial_rows(self, spark):
        """Test ADD COLUMNS FROM with data for only some rows (others get null)."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value INT
            )
        """)

        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 100),
            (2, 'Bob', 200),
            (3, 'Charlie', 300),
            (4, 'Diana', 400),
            (5, 'Eve', 500)
        """)

        # Create temp view with data for only some rows
        spark.sql("""
            CREATE TEMPORARY VIEW tmp_view AS
            SELECT _rowaddr, _fragid, CONCAT('special_', name) as special_name
            FROM default.test_table
            WHERE id IN (1, 3, 5)
        """)

        # Add column - rows not in view should get null
        spark.sql("""
            ALTER TABLE default.test_table ADD COLUMNS special_name FROM tmp_view
        """)

        result = spark.sql("""
            SELECT id, special_name
            FROM default.test_table
            ORDER BY id
        """).collect()

        assert len(result) == 5
        assert result[0].special_name == "special_Alice"
        assert result[1].special_name is None  # id=2 not in view
        assert result[2].special_name == "special_Charlie"
        assert result[3].special_name is None  # id=4 not in view
        assert result[4].special_name == "special_Eve"

    def test_add_column_computed_values(self, spark):
        """Test ADD COLUMNS FROM with computed/derived values."""
        spark.sql("""
            CREATE TABLE default.employees (
                id INT,
                name STRING,
                salary INT,
                bonus_percent INT
            )
        """)

        spark.sql("""
            INSERT INTO default.employees VALUES
            (1, 'Alice', 50000, 10),
            (2, 'Bob', 60000, 15),
            (3, 'Charlie', 70000, 20)
        """)

        # Compute total compensation as a new column
        spark.sql("""
            CREATE TEMPORARY VIEW tmp_view AS
            SELECT _rowaddr, _fragid,
                   salary + (salary * bonus_percent / 100) as total_compensation
            FROM default.employees
        """)

        spark.sql("""
            ALTER TABLE default.employees ADD COLUMNS total_compensation FROM tmp_view
        """)

        result = spark.sql("""
            SELECT id, name, salary, bonus_percent, total_compensation
            FROM default.employees
            ORDER BY id
        """).collect()

        assert len(result) == 3
        assert result[0].total_compensation == 55000   # 50000 + 5000
        assert result[1].total_compensation == 69000   # 60000 + 9000
        assert result[2].total_compensation == 84000   # 70000 + 14000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
