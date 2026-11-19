# Catalyst compatibility validation

Run the test suite against a supported Spark module:

```bash
./mvnw test -pl <spark-module> -Dtest=CatalystCompatibilityValidationTest
```

To validate a packaged bundle, start a Spark shell with the bundle and matching test JAR and
configure the Lance session extension and catalog:

```bash
./mvnw package -pl <bundle-module> -DskipTests
./mvnw test-compile jar:test-jar -pl <matching-spark-module> -DskipTests

spark-shell \
  --jars <lance-spark-bundle.jar>,<matching-tests.jar> \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog \
  --conf spark.sql.catalog.lance.impl=dir \
  --conf spark.sql.catalog.lance.root=<disposable-catalog-path>
```

Then invoke the validator in the shell:

```scala
org.apache.spark.sql.catalyst.CatalystCompatibilityValidation.run(spark)
```

The validator audits the loaded connector classes against the runtime Catalyst API, then exercises
append, overwrite, ordered reads, null Float32 vector backfill through `ADD COLUMNS FROM`, and
subsequent row-level DML (Spark 3.5+) in a temporary
namespace. It drops the table and namespace after the smoke test; use a disposable catalog.
