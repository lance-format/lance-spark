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
package org.lance.spark.benchmark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.lance.Fragment;
import org.lance.index.scalar.ZoneStats;
import org.lance.ipc.FragmentSlice;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.ipc.ScanStats;

/**
 * Measures physical read reduction from applying a {@link FragmentSlice} to a selective ZoneMap
 * query.
 *
 * <p>The generated dataset deliberately contains one large, physically ordered fragment. A query
 * selects exactly one ZoneMap zone while projecting a wide, deterministic payload. Four scanner
 * modes isolate physical slicing from scalar-index execution:
 *
 * <ol>
 *   <li>full fragment, scalar index disabled
 *   <li>physical slice, scalar index disabled
 *   <li>full fragment, scalar index enabled
 *   <li>physical slice, scalar index enabled
 * </ol>
 */
public final class FragmentSliceBenchmark {
  private static final String DATASET_NAME = "fragment_slice.lance";
  private static final String INDEX_NAME = "fragment_slice_zonemap";

  private FragmentSliceBenchmark() {}

  public static void main(String[] args) throws Exception {
    Config config = Config.parse(args);
    SparkSession spark = createSparkSession(config);
    try {
      String datasetUri = TpcdsDataGenerator.toLancePath(config.dataDir + "/" + DATASET_NAME);
      prepareDataset(spark, datasetUri, config);
      runBenchmark(datasetUri, config);
    } finally {
      spark.stop();
    }
  }

  private static SparkSession createSparkSession(Config config) {
    return SparkSession.builder()
        .appName("Lance FragmentSlice Benchmark")
        .config("spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
        .config("spark.sql.catalog.lance_default", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance_default.impl", "dir")
        .config("spark.sql.catalog.lance_default.root", config.dataDir)
        .getOrCreate();
  }

  private static void prepareDataset(SparkSession spark, String datasetUri, Config config)
      throws Exception {
    Path datasetPath = new Path(datasetUri);
    FileSystem fs = datasetPath.getFileSystem(spark.sparkContext().hadoopConfiguration());
    boolean exists = fs.exists(datasetPath);

    if (!exists || config.regenerate) {
      System.out.printf(
          Locale.ROOT,
          "Generating %,d rows with a %d-byte payload at %s%n",
          config.rows,
          config.payloadBytes,
          datasetUri);
      Dataset<Row> data =
          spark
              .range(0, config.rows, 1, 1)
              .selectExpr("id", payloadExpression(config.payloadBytes));
      data.write()
          .format("lance")
          .mode(exists ? SaveMode.Overwrite : SaveMode.ErrorIfExists)
          .option("max_row_per_file", String.valueOf(config.rows))
          .option("file_format_version", "2.0")
          .save(datasetUri);
      createZonemap(spark, datasetUri, config.rowsPerZone);
      return;
    }

    try (org.lance.Dataset dataset = org.lance.Dataset.open().uri(datasetUri).build()) {
      validateDataset(dataset, config);
      List<ZoneStats> zones = dataset.getZonemapStats("id");
      if (zones.isEmpty()) {
        createZonemap(spark, datasetUri, config.rowsPerZone);
      } else {
        validateZoneSize(zones, config.rowsPerZone);
      }
    }
  }

  private static String payloadExpression(int payloadBytes) {
    int hashes = (payloadBytes + 63) / 64;
    StringBuilder expression = new StringBuilder("substring(concat(");
    for (int i = 0; i < hashes; i++) {
      if (i > 0) {
        expression.append(',');
      }
      expression
          .append("sha2(concat(cast(id as string), ':fragment-slice:")
          .append(i)
          .append("'), 256)");
    }
    return expression.append("), 1, ").append(payloadBytes).append(") AS payload").toString();
  }

  private static void createZonemap(SparkSession spark, String datasetUri, long rowsPerZone) {
    String escapedUri = datasetUri.replace("`", "``");
    spark
        .sql(
            String.format(
                Locale.ROOT,
                "ALTER TABLE lance_default.`%s` CREATE INDEX %s USING zonemap (id) "
                    + "WITH (rows_per_zone=%d)",
                escapedUri,
                INDEX_NAME,
                rowsPerZone))
        .collectAsList();
  }

  private static void validateDataset(org.lance.Dataset dataset, Config config) {
    long actualRows = dataset.countRows();
    if (actualRows != config.rows) {
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Existing benchmark dataset has %,d rows, expected %,d; rerun with --regenerate",
              actualRows,
              config.rows));
    }
    List<Fragment> fragments = dataset.getFragments();
    if (fragments.size() != 1) {
      throw new IllegalStateException(
          "FragmentSlice benchmark requires exactly one fragment, found " + fragments.size());
    }
  }

  private static void validateZoneSize(List<ZoneStats> zones, long expectedRowsPerZone) {
    long largestZoneLength = 0;
    for (ZoneStats zone : zones) {
      largestZoneLength = Math.max(largestZoneLength, zone.getZoneLength());
    }
    if (largestZoneLength != expectedRowsPerZone) {
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Existing ZoneMap uses %,d rows per zone, expected %,d; rerun with --regenerate",
              largestZoneLength,
              expectedRowsPerZone));
    }
  }

  private static void runBenchmark(String datasetUri, Config config) throws Exception {
    try (org.lance.Dataset dataset = org.lance.Dataset.open().uri(datasetUri).build()) {
      validateDataset(dataset, config);
      List<ZoneStats> zones = dataset.getZonemapStats("id");
      if (zones.isEmpty()) {
        throw new IllegalStateException("ZoneMap index is missing after dataset preparation");
      }
      validateZoneSize(zones, config.rowsPerZone);

      Fragment fragment = dataset.getFragments().get(0);
      int fragmentId = fragment.getId();
      long physicalRows = fragment.metadata().getPhysicalRows();
      long sliceStart = alignedSliceStart(physicalRows, config.rowsPerZone);
      long sliceRows = Math.min(config.rowsPerZone, physicalRows - sliceStart);
      FragmentSlice slice = new FragmentSlice(fragmentId, sliceStart, sliceRows);
      double pruningRatio = (double) sliceRows / physicalRows;

      System.out.println("=== FragmentSlice Benchmark ===");
      System.out.printf(Locale.ROOT, "Dataset:          %s%n", datasetUri);
      System.out.printf(Locale.ROOT, "Physical rows:    %,d%n", physicalRows);
      System.out.printf(
          Locale.ROOT,
          "Selected slice:   fragment=%d [%d, %d)%n",
          fragmentId,
          sliceStart,
          sliceStart + sliceRows);
      System.out.printf(Locale.ROOT, "Planned row ratio: %.4f%%%n", pruningRatio * 100.0);
      System.out.printf(
          Locale.ROOT, "Warmups/runs:      %d/%d%n%n", config.warmups, config.iterations);

      List<Mode> forward = Arrays.asList(Mode.values());
      List<Mode> reverse = new ArrayList<>(forward);
      Collections.reverse(reverse);

      for (int warmup = 0; warmup < config.warmups; warmup++) {
        for (Mode mode : warmup % 2 == 0 ? forward : reverse) {
          runScan(dataset, mode, fragmentId, slice, sliceStart, sliceRows, pruningRatio, 0);
        }
      }

      List<Measurement> measurements = new ArrayList<>();
      for (int iteration = 1; iteration <= config.iterations; iteration++) {
        List<Mode> order = iteration % 2 == 1 ? forward : reverse;
        for (Mode mode : order) {
          Measurement measurement =
              runScan(
                  dataset, mode, fragmentId, slice, sliceStart, sliceRows, pruningRatio, iteration);
          measurements.add(measurement);
          System.out.println(measurement.toConsoleLine());
        }
      }

      java.nio.file.Path csvPath = writeResults(measurements, config.resultsDir);
      printSummary(measurements, pruningRatio, csvPath);
    }
  }

  private static long alignedSliceStart(long physicalRows, long rowsPerZone) {
    long midpoint = physicalRows / 2;
    long start = (midpoint / rowsPerZone) * rowsPerZone;
    if (start + rowsPerZone > physicalRows) {
      start = Math.max(0, physicalRows - rowsPerZone);
    }
    return start;
  }

  private static Measurement runScan(
      org.lance.Dataset dataset,
      Mode mode,
      int fragmentId,
      FragmentSlice slice,
      long filterStart,
      long expectedRows,
      double pruningRatio,
      int iteration)
      throws Exception {
    ScanOptions.Builder options =
        new ScanOptions.Builder()
            .columns(Collections.singletonList("payload"))
            .filter(
                String.format(
                    Locale.ROOT, "id >= %d AND id < %d", filterStart, filterStart + expectedRows))
            .fragmentIds(Collections.singletonList(fragmentId))
            .useScalarIndex(mode.useScalarIndex)
            .collectStats(true);
    if (mode.useSlice) {
      options.fragmentSlices(Collections.singletonList(slice));
    }

    long rowsRead = 0;
    ScanStats stats;
    long start = System.nanoTime();
    try (LanceScanner scanner = dataset.newScan(options.build())) {
      try (ArrowReader reader = scanner.scanBatches()) {
        while (reader.loadNextBatch()) {
          rowsRead += reader.getVectorSchemaRoot().getRowCount();
        }
      }
      stats =
          scanner
              .getStats()
              .orElseThrow(() -> new IllegalStateException("ScanStats unavailable after scan"));
    }
    long elapsedNs = System.nanoTime() - start;

    if (rowsRead != expectedRows) {
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "%s returned %,d rows, expected %,d",
              mode.label,
              rowsRead,
              expectedRows));
    }
    return new Measurement(mode, iteration, rowsRead, pruningRatio, elapsedNs, stats);
  }

  private static java.nio.file.Path writeResults(List<Measurement> measurements, String resultsDir)
      throws IOException {
    java.nio.file.Path directory = Paths.get(resultsDir);
    Files.createDirectories(directory);
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
    java.nio.file.Path csvPath = directory.resolve("fragment_slice_" + timestamp + ".csv");
    try (BufferedWriter writer = Files.newBufferedWriter(csvPath, StandardCharsets.UTF_8)) {
      writer.write(Measurement.csvHeader());
      writer.newLine();
      for (Measurement measurement : measurements) {
        writer.write(measurement.toCsv());
        writer.newLine();
      }
    }
    return csvPath;
  }

  private static void printSummary(
      List<Measurement> measurements, double pruningRatio, java.nio.file.Path csvPath) {
    System.out.println();
    System.out.println("=== Median results ===");
    for (Mode mode : Mode.values()) {
      List<Measurement> modeMeasurements = selectMode(measurements, mode);
      System.out.printf(
          Locale.ROOT,
          "%-26s elapsed=%8.2fms bytes=%12s requests=%8d iops=%8d indices=%4d parts=%4d "
              + "comparisons=%6d%n",
          mode.label,
          medianElapsedMs(modeMeasurements),
          formatBytes(medianLong(modeMeasurements, Value.BYTES)),
          medianLong(modeMeasurements, Value.REQUESTS),
          medianLong(modeMeasurements, Value.IOPS),
          medianLong(modeMeasurements, Value.INDICES),
          medianLong(modeMeasurements, Value.PARTS),
          medianLong(modeMeasurements, Value.COMPARISONS));
    }

    double noIndexReduction =
        byteReduction(
            selectMode(measurements, Mode.FULL_NO_INDEX),
            selectMode(measurements, Mode.SLICE_NO_INDEX));
    double indexReduction =
        byteReduction(
            selectMode(measurements, Mode.FULL_WITH_INDEX),
            selectMode(measurements, Mode.SLICE_WITH_INDEX));
    System.out.printf(
        Locale.ROOT, "%nPlanned row reduction:     %.2f%%%n", (1 - pruningRatio) * 100);
    System.out.printf(
        Locale.ROOT,
        "Byte reduction, index off: %.2f%%%s%n",
        noIndexReduction,
        noIndexReduction >= 80.0 ? "  PASS (>= 80%)" : "  BELOW TARGET (< 80%)");
    System.out.printf(Locale.ROOT, "Byte reduction, index on:  %.2f%%%n", indexReduction);
    System.out.println("CSV: " + csvPath.toAbsolutePath());
  }

  private static List<Measurement> selectMode(List<Measurement> measurements, Mode mode) {
    List<Measurement> selected = new ArrayList<>();
    for (Measurement measurement : measurements) {
      if (measurement.mode == mode) {
        selected.add(measurement);
      }
    }
    return selected;
  }

  private static double byteReduction(List<Measurement> baseline, List<Measurement> candidate) {
    long baselineBytes = medianLong(baseline, Value.BYTES);
    long candidateBytes = medianLong(candidate, Value.BYTES);
    return baselineBytes == 0 ? 0 : (1.0 - (double) candidateBytes / baselineBytes) * 100.0;
  }

  private static double medianElapsedMs(List<Measurement> measurements) {
    List<Long> values = new ArrayList<>(measurements.size());
    for (Measurement measurement : measurements) {
      values.add(measurement.elapsedNs);
    }
    return median(values) / 1_000_000.0;
  }

  private static long medianLong(List<Measurement> measurements, Value value) {
    List<Long> values = new ArrayList<>(measurements.size());
    for (Measurement measurement : measurements) {
      values.add(value.get(measurement));
    }
    return median(values);
  }

  private static long median(List<Long> values) {
    if (values.isEmpty()) {
      throw new IllegalArgumentException("Cannot calculate median of an empty list");
    }
    values.sort(Comparator.naturalOrder());
    int midpoint = values.size() / 2;
    if (values.size() % 2 == 1) {
      return values.get(midpoint);
    }
    return values.get(midpoint - 1) + (values.get(midpoint) - values.get(midpoint - 1)) / 2;
  }

  private static String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }
    if (bytes < 1024L * 1024) {
      return String.format(Locale.ROOT, "%.1f KiB", bytes / 1024.0);
    }
    if (bytes < 1024L * 1024 * 1024) {
      return String.format(Locale.ROOT, "%.1f MiB", bytes / (1024.0 * 1024));
    }
    return String.format(Locale.ROOT, "%.2f GiB", bytes / (1024.0 * 1024 * 1024));
  }

  private enum Mode {
    FULL_NO_INDEX("full / scalar=false", false, false),
    SLICE_NO_INDEX("slice / scalar=false", true, false),
    FULL_WITH_INDEX("full / scalar=true", false, true),
    SLICE_WITH_INDEX("slice / scalar=true", true, true);

    private final String label;
    private final boolean useSlice;
    private final boolean useScalarIndex;

    Mode(String label, boolean useSlice, boolean useScalarIndex) {
      this.label = label;
      this.useSlice = useSlice;
      this.useScalarIndex = useScalarIndex;
    }
  }

  private enum Value {
    BYTES {
      @Override
      long get(Measurement measurement) {
        return measurement.stats.getBytesRead();
      }
    },
    REQUESTS {
      @Override
      long get(Measurement measurement) {
        return measurement.stats.getRequests();
      }
    },
    IOPS {
      @Override
      long get(Measurement measurement) {
        return measurement.stats.getIops();
      }
    },
    INDICES {
      @Override
      long get(Measurement measurement) {
        return measurement.stats.getIndicesLoaded();
      }
    },
    PARTS {
      @Override
      long get(Measurement measurement) {
        return measurement.stats.getPartsLoaded();
      }
    },
    COMPARISONS {
      @Override
      long get(Measurement measurement) {
        return measurement.stats.getIndexComparisons();
      }
    };

    abstract long get(Measurement measurement);
  }

  private static final class Measurement {
    private final Mode mode;
    private final int iteration;
    private final long rowsRead;
    private final double pruningRatio;
    private final long elapsedNs;
    private final ScanStats stats;

    private Measurement(
        Mode mode,
        int iteration,
        long rowsRead,
        double pruningRatio,
        long elapsedNs,
        ScanStats stats) {
      this.mode = mode;
      this.iteration = iteration;
      this.rowsRead = rowsRead;
      this.pruningRatio = pruningRatio;
      this.elapsedNs = elapsedNs;
      this.stats = stats;
    }

    private String toConsoleLine() {
      return String.format(
          Locale.ROOT,
          "iter=%d %-26s rows=%,d elapsed=%8.2fms bytes=%s requests=%d iops=%d "
              + "indices=%d parts=%d comparisons=%d",
          iteration,
          mode.label,
          rowsRead,
          elapsedNs / 1_000_000.0,
          formatBytes(stats.getBytesRead()),
          stats.getRequests(),
          stats.getIops(),
          stats.getIndicesLoaded(),
          stats.getPartsLoaded(),
          stats.getIndexComparisons());
    }

    private static String csvHeader() {
      return "iteration,mode,use_slice,use_scalar_index,rows_read,planned_row_ratio,elapsed_ns,"
          + "bytes_read,requests,iops,indices_loaded,parts_loaded,index_comparisons";
    }

    private String toCsv() {
      return String.format(
          Locale.ROOT,
          "%d,%s,%s,%s,%d,%.8f,%d,%d,%d,%d,%d,%d,%d",
          iteration,
          mode.name(),
          mode.useSlice,
          mode.useScalarIndex,
          rowsRead,
          pruningRatio,
          elapsedNs,
          stats.getBytesRead(),
          stats.getRequests(),
          stats.getIops(),
          stats.getIndicesLoaded(),
          stats.getPartsLoaded(),
          stats.getIndexComparisons());
    }
  }

  private static final class Config {
    private String dataDir = "benchmark/data/fragment-slice";
    private String resultsDir = "benchmark/results";
    private long rows = 4_000_000;
    private int payloadBytes = 256;
    private long rowsPerZone = 8_192;
    private int warmups = 1;
    private int iterations = 5;
    private boolean regenerate;

    private static Config parse(String[] args) {
      Config config = new Config();
      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "--data-dir":
            config.dataDir = requireValue(args, ++i, "--data-dir");
            break;
          case "--results-dir":
            config.resultsDir = requireValue(args, ++i, "--results-dir");
            break;
          case "--rows":
            config.rows = Long.parseLong(requireValue(args, ++i, "--rows"));
            break;
          case "--payload-bytes":
            config.payloadBytes = Integer.parseInt(requireValue(args, ++i, "--payload-bytes"));
            break;
          case "--rows-per-zone":
            config.rowsPerZone = Long.parseLong(requireValue(args, ++i, "--rows-per-zone"));
            break;
          case "--warmups":
            config.warmups = Integer.parseInt(requireValue(args, ++i, "--warmups"));
            break;
          case "--iterations":
            config.iterations = Integer.parseInt(requireValue(args, ++i, "--iterations"));
            break;
          case "--regenerate":
            config.regenerate = true;
            break;
          case "--help":
            printUsage();
            System.exit(0);
            break;
          default:
            throw new IllegalArgumentException("Unknown argument: " + args[i]);
        }
      }
      config.validate();
      return config;
    }

    private void validate() {
      if (rows <= 0 || rows > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("--rows must be between 1 and " + Integer.MAX_VALUE);
      }
      if (payloadBytes <= 0) {
        throw new IllegalArgumentException("--payload-bytes must be positive");
      }
      if (rowsPerZone <= 0 || rowsPerZone > rows) {
        throw new IllegalArgumentException(
            "--rows-per-zone must be positive and no greater than rows");
      }
      if (warmups < 0) {
        throw new IllegalArgumentException("--warmups must be non-negative");
      }
      if (iterations <= 0) {
        throw new IllegalArgumentException("--iterations must be positive");
      }
    }

    private static String requireValue(String[] args, int index, String option) {
      if (index >= args.length) {
        throw new IllegalArgumentException("Missing value for " + option);
      }
      return args[index];
    }
  }

  private static void printUsage() {
    System.out.println(
        "Usage: FragmentSliceBenchmark"
            + " [--data-dir <path>]"
            + " [--results-dir <path>]"
            + " [--rows 4000000]"
            + " [--payload-bytes 256]"
            + " [--rows-per-zone 8192]"
            + " [--warmups 1]"
            + " [--iterations 5]"
            + " [--regenerate]");
  }
}
