#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare exact take, approximate Spark sampling, and driver-only native sample.

Run with spark-submit and a matching Lance Spark bundle. Output is JSON Lines;
timings include planning and consumption, with no claim about cold storage caches.
"""

import argparse
import json
import time

from pyspark.sql import SparkSession


def consume_partition(rows):
    # Crossing the RDD boundary materializes all projected columns, unlike count().
    yield sum(1 for _ in rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path")
    parser.add_argument("--version", type=int, required=True)
    parser.add_argument("--size", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--columns", nargs="+", default=["id"])
    parser.add_argument("--batch-sizes", type=int, nargs="+", default=[256, 1024, 4096])
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.version <= 0 or args.size <= 0 or args.repeats <= 0 or min(args.batch_sizes) <= 0:
        parser.error("version, size, repeats, and batch sizes must be positive")

    spark = SparkSession.builder.appName("lance-exact-sampling-benchmark").getOrCreate()
    native = None
    try:
        jvm = spark._jvm
        properties = jvm.java.util.HashMap()
        properties.put("version", str(args.version))
        configured = dict(spark.sparkContext.getConf().getAll())
        catalog = "lance" if "spark.sql.catalog.lance" in configured else "lance_default"
        prefix = f"spark.sql.catalog.{catalog}.storage."
        for key, value in spark.sparkContext.getConf().getAll():
            if key.startswith(prefix):
                properties.put(key[len(prefix):], value)
        options = getattr(jvm.org.lance.spark.LanceSparkReadOptions, "from")(
            properties, args.path
        ).toBuilder().catalogName(catalog).build()
        native = jvm.org.lance.spark.utils.Utils.openDatasetBuilder(options).build()
        population = native.countRows()
        if args.size > population or args.size > 1000000:
            parser.error("size must not exceed the population or the 1,000,000 planning cap")
        columns = jvm.java.util.ArrayList()
        for column in args.columns:
            columns.add(column)

        def read():
            return spark.read.format("lance").option("version", str(args.version))

        modes = [("exact", batch) for batch in args.batch_sizes] + [
            ("spark_fraction", None), ("native_driver", None)
        ]
        for iteration in range(args.repeats):
            # Rotate modes so one reader is not always favored by earlier readers' caches.
            shift = iteration % len(modes)
            for mode, batch in modes[shift:] + modes[:shift]:
                start = time.perf_counter()
                if mode == "native_driver":
                    reader = native.sample(args.size, columns)
                    count = 0
                    try:
                        while reader.loadNextBatch():
                            count += reader.getVectorSchemaRoot().getRowCount()
                    finally:
                        reader.close()
                else:
                    source = read()
                    if mode == "exact":
                        source = source.option("sample_size", str(args.size)) \
                            .option("sample_seed", str(args.seed)) \
                            .option("sample_batch_size", str(batch))
                    frame = source.load(args.path).select(*args.columns)
                    if mode == "spark_fraction":
                        frame = frame.sample(False, args.size / population, args.seed)
                    count = int(frame.rdd.mapPartitions(consume_partition).sum())
                print(json.dumps({
                    "mode": mode, "iteration": iteration, "version": args.version,
                    "population": population, "requested_rows": args.size,
                    "returned_rows": count, "columns": args.columns,
                    "batch_size": batch, "seconds": time.perf_counter() - start,
                    "seed": args.seed if mode != "native_driver" else None,
                }), flush=True)
    finally:
        if native is not None:
            native.close()
        spark.stop()


if __name__ == "__main__":
    main()
