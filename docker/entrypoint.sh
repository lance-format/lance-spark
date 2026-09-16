#!/bin/bash

# Standalone daemons are for notebooks. Tests use SparkSession local[2].
if [[ "${SPARK_STANDALONE:-1}" != "0" ]]; then
  start-master.sh -p 7077
  start-worker.sh spark://spark-lance:7077
  start-history-server.sh
  start-thriftserver.sh --driver-java-options "-Dderby.system.home=/tmp/derby"
fi

# Entrypoint, for example notebook, pyspark or spark-sql
if [[ $# -gt 0 ]] ; then
    eval "$1"
fi
