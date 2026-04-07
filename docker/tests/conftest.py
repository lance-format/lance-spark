"""
Shared fixtures for Lance-Spark integration tests.

Fixtures defined here are available to all test modules under docker/tests/.

The ``spark`` fixture is parameterized over storage backends (local filesystem,
Azurite, MinIO, and optionally LanceDB Cloud) so that every test is
automatically exercised against all available backends.
"""

import os
import subprocess
import time
import urllib.request
import urllib.error

import pytest
from pyspark.sql import SparkSession


def pytest_configure(config):
    config.addinivalue_line("markers", "requires_rest: test only runs on REST-based backends")


# ---------------------------------------------------------------------------
# Azurite (Azure Blob Storage emulator) configuration
# ---------------------------------------------------------------------------
AZURITE_BLOB_PORT = 10100  # Avoid conflict with Thrift server on 10000
AZURITE_ACCOUNT_NAME = "devstoreaccount1"
AZURITE_ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
AZURITE_CONTAINER = "lance-test"


@pytest.fixture(scope="session")
def azurite():
    """Start Azurite blob service, create the test container, and yield config.

    This fixture is **not** autouse — it only runs when a test explicitly
    depends on it (directly or transitively via the ``spark`` fixture).
    """
    proc = subprocess.Popen(
        [
            "azurite-blob",
            "--blobHost", "0.0.0.0",
            "--blobPort", str(AZURITE_BLOB_PORT),
            "--skipApiVersionCheck",
            "--silent",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Poll until the blob service is healthy (up to 30 s).
    # Azurite returns HTTP 400 on the root URL, which is fine — any HTTP
    # response means the server is up.
    blob_url = f"http://127.0.0.1:{AZURITE_BLOB_PORT}"
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(blob_url, timeout=1)
            break
        except urllib.error.HTTPError:
            break  # Server is responding (400 from Azurite is expected)
        except (urllib.error.URLError, OSError):
            if proc.poll() is not None:
                raise RuntimeError("azurite-blob exited unexpectedly")
            time.sleep(0.5)
    else:
        _stop_process(proc)
        raise RuntimeError("azurite-blob did not become healthy within 30 s")

    # Create the blob container using the Azure SDK.
    from azure.storage.blob import BlobServiceClient

    conn_str = (
        f"DefaultEndpointsProtocol=http;"
        f"AccountName={AZURITE_ACCOUNT_NAME};"
        f"AccountKey={AZURITE_ACCOUNT_KEY};"
        f"BlobEndpoint=http://127.0.0.1:{AZURITE_BLOB_PORT}/{AZURITE_ACCOUNT_NAME};"
    )
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_service.create_container(AZURITE_CONTAINER)

    yield {
        "account_name": AZURITE_ACCOUNT_NAME,
        "account_key": AZURITE_ACCOUNT_KEY,
        "container": AZURITE_CONTAINER,
        "port": AZURITE_BLOB_PORT,
        "endpoint": f"http://127.0.0.1:{AZURITE_BLOB_PORT}/{AZURITE_ACCOUNT_NAME}",
    }

    _stop_process(proc)


# ---------------------------------------------------------------------------
# MinIO (S3-compatible storage) configuration
# ---------------------------------------------------------------------------
MINIO_PORT = 9000
MINIO_CONSOLE_PORT = 9001
MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin"
MINIO_BUCKET = "lance-test"


@pytest.fixture(scope="session")
def minio():
    """Start MinIO server, create the test bucket, and yield config.

    This fixture is **not** autouse — it only runs when a test explicitly
    depends on it (directly or transitively via the ``spark`` fixture).
    """
    import os

    env = os.environ.copy()
    env["MINIO_ROOT_USER"] = MINIO_ROOT_USER
    env["MINIO_ROOT_PASSWORD"] = MINIO_ROOT_PASSWORD

    proc = subprocess.Popen(
        [
            "minio", "server", "/tmp/minio-data",
            "--address", f":{MINIO_PORT}",
            "--console-address", f":{MINIO_CONSOLE_PORT}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )

    # Poll until the MinIO health endpoint responds (up to 30 s).
    health_url = f"http://127.0.0.1:{MINIO_PORT}/minio/health/live"
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(health_url, timeout=1)
            break
        except (urllib.error.URLError, OSError):
            if proc.poll() is not None:
                raise RuntimeError("minio exited unexpectedly")
            time.sleep(0.5)
    else:
        _stop_process(proc)
        raise RuntimeError("minio did not become healthy within 30 s")

    # Create the test bucket using boto3.
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{MINIO_PORT}",
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
    )
    s3.create_bucket(Bucket=MINIO_BUCKET)

    yield {
        "endpoint": f"http://127.0.0.1:{MINIO_PORT}",
        "access_key": MINIO_ROOT_USER,
        "secret_key": MINIO_ROOT_PASSWORD,
        "bucket": MINIO_BUCKET,
        "port": MINIO_PORT,
    }

    _stop_process(proc)


# ---------------------------------------------------------------------------
# Parameterized Spark session (runs every test against each backend)
# ---------------------------------------------------------------------------
CATALOG = "lance"

# LanceDB Cloud configuration (optional – set env vars to enable)
LANCEDB_DB = os.environ.get("LANCEDB_DB")
LANCEDB_API_KEY = os.environ.get("LANCEDB_API_KEY")
LANCEDB_HOST_OVERRIDE = os.environ.get("LANCEDB_HOST_OVERRIDE")
LANCEDB_REGION = os.environ.get("LANCEDB_REGION", "us-east-1")

_all_backends = ["local", "azurite", "minio"]
if LANCEDB_DB and LANCEDB_API_KEY:
    _all_backends.append("lancedb")
_backends = os.environ.get("TEST_BACKENDS", ",".join(_all_backends)).split(",")


@pytest.fixture(scope="module", params=_backends)
def spark(request):
    """Create a Spark session configured with Lance catalog.

    Parameterized across storage backends so the full test suite runs against
    each one:

    - **local** – local filesystem at ``/home/lance/data``
    - **azurite** – Azure Blob Storage via the Azurite emulator
    - **minio** – S3-compatible storage via the MinIO emulator
    - **lancedb** – LanceDB Cloud via REST API (requires ``LANCEDB_DB`` and
      ``LANCEDB_API_KEY`` env vars; skipped otherwise)
    """
    backend = request.param

    builder = (
        SparkSession.builder
        .appName("LanceSparkTests")
        .config(
            f"spark.sql.catalog.{CATALOG}",
            "org.lance.spark.LanceNamespaceSparkCatalog",
        )
        .config(
            "spark.sql.extensions",
            "org.lance.spark.extensions.LanceSparkSessionExtensions",
        )
    )

    if backend == "lancedb":
        uri = LANCEDB_HOST_OVERRIDE or (
            f"https://{LANCEDB_DB}.{LANCEDB_REGION}.api.lancedb.com"
        )
        builder = (
            builder
            .config(f"spark.sql.catalog.{CATALOG}.impl", "rest")
            .config(f"spark.sql.catalog.{CATALOG}.uri", uri)
            .config(
                f"spark.sql.catalog.{CATALOG}.headers.x-api-key", LANCEDB_API_KEY,
            )
            .config(
                f"spark.sql.catalog.{CATALOG}.headers.x-lancedb-database", LANCEDB_DB,
            )
        )
    else:
        builder = builder.config(f"spark.sql.catalog.{CATALOG}.impl", "dir")

        if backend == "local":
            builder = builder.config(
                f"spark.sql.catalog.{CATALOG}.root", "/home/lance/data",
            )
        elif backend == "azurite":
            az = request.getfixturevalue("azurite")
            builder = (
                builder
                .config(f"spark.sql.catalog.{CATALOG}.root", f"az://{az['container']}")
                .config(f"spark.sql.catalog.{CATALOG}.storage.account_name", az["account_name"])
                .config(f"spark.sql.catalog.{CATALOG}.storage.account_key", az["account_key"])
                .config(
                    f"spark.sql.catalog.{CATALOG}.storage.azure_storage_endpoint", az["endpoint"],
                )
                .config(f"spark.sql.catalog.{CATALOG}.storage.allow_http", "true")
            )
        elif backend == "minio":
            s3 = request.getfixturevalue("minio")
            builder = (
                builder
                .config(f"spark.sql.catalog.{CATALOG}.root", f"s3://{s3['bucket']}")
                .config(f"spark.sql.catalog.{CATALOG}.storage.endpoint", s3["endpoint"])
                .config(f"spark.sql.catalog.{CATALOG}.storage.aws_allow_http", "true")
                .config(f"spark.sql.catalog.{CATALOG}.storage.access_key_id", s3["access_key"])
                .config(f"spark.sql.catalog.{CATALOG}.storage.secret_access_key", s3["secret_key"])
                .config(f"spark.sql.catalog.{CATALOG}.storage.region", "us-east-1")
            )

    session = builder.getOrCreate()
    session.sql(f"SET spark.sql.defaultCatalog={CATALOG}")
    # Create default namespace for multi-level namespace mode
    session.sql("CREATE NAMESPACE IF NOT EXISTS default")
    # Store backend name for marker-based test skipping
    session._lance_backend = backend
    yield session
    session.stop()


@pytest.fixture
def test_table(request, spark):
    """Provide a unique table name for each test to avoid isolation issues.

    Usage: def test_foo(spark, test_table):
               spark.sql(f"CREATE TABLE {test_table} ...")
    """
    # Create unique table name from test name (sanitize special chars)
    test_name = request.node.name.replace("[", "_").replace("]", "_").replace("-", "_")
    table_name = f"default.test_{test_name}"

    # Cleanup before test
    spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")

    yield table_name

    # Cleanup after test
    spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")


@pytest.fixture(autouse=True)
def _skip_by_backend(request, spark):
    """Auto-skip tests marked ``requires_rest`` on non-REST backends."""
    if request.node.get_closest_marker("requires_rest"):
        if getattr(spark, "_lance_backend", None) != "lancedb":
            pytest.skip("requires REST-based backend")


@pytest.fixture(autouse=True)
def cleanup_tables(spark):
    """Clean up test tables before and after each test."""
    spark.sql("DROP TABLE IF EXISTS default.test_table PURGE")
    spark.sql("DROP TABLE IF EXISTS default.test_table_renamed PURGE")
    spark.sql("DROP TABLE IF EXISTS default.test_table_new PURGE")
    spark.sql("DROP TABLE IF EXISTS default.employees PURGE")
    # TODO - reenable once `tableExists` works on Spark 4.0
    #spark.catalog.dropTempView("source") if spark.catalog.tableExists("source") else None
    #spark.catalog.dropTempView("tmp_view") if spark.catalog.tableExists("tmp_view") else None
    spark.catalog.dropTempView("source")
    spark.catalog.dropTempView("tmp_view")
    yield
    spark.sql("DROP TABLE IF EXISTS default.test_table PURGE")
    spark.sql("DROP TABLE IF EXISTS default.test_table_renamed PURGE")
    spark.sql("DROP TABLE IF EXISTS default.test_table_new PURGE")
    spark.sql("DROP TABLE IF EXISTS default.employees PURGE")
    # TODO - reenable once `tableExists` works on Spark 4.0
    #spark.catalog.dropTempView("source") if spark.catalog.tableExists("source") else None
    #spark.catalog.dropTempView("tmp_view") if spark.catalog.tableExists("tmp_view") else None
    spark.catalog.dropTempView("source")
    spark.catalog.dropTempView("tmp_view")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _stop_process(proc, timeout=10):
    """Terminate a subprocess gracefully, falling back to SIGKILL on timeout."""
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
