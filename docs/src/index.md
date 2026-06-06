# Spark Lance Connector

## Introduction

The Apache Spark Connector for Lance allows Apache Spark to efficiently read datasets stored in Lance format.

By using the Apache Spark Connector for Lance, you can leverage Apache Spark's powerful data processing, SQL querying,
and machine learning training capabilities on the AI data lake powered by Lance.

## Features

The connector is built using the Spark DatasourceV2 (DSv2) API.
Please check [this presentation](https://www.slideshare.net/databricks/apache-spark-data-source-v2-with-wenchen-fan-and-gengliang-wang)
to learn more about DSv2 features.
Specifically, you can use the Apache Spark Connector for Lance to:

* **Read & Write Lance Datasets**: Seamlessly read and write datasets stored in the Lance format using Spark.
* **Distributed, Parallel Scans**: Leverage Spark's distributed computing capabilities to perform parallel scans on Lance datasets.
* **Column and Filter Pushdown**: Optimize query performance by pushing down column selections and filters to the data source.
* **SQL Search Table Functions**: Run [vector](operations/dql/vector-search.md), [full-text](operations/dql/search.md), and [hybrid](operations/dql/hybrid-search.md) search through Lance namespace execution.

## Quick Start

The project contains a docker image in the `docker` folder you can build and run a simple example notebook.
To do so, clone the repo and run:

```shell
make docker-build
make docker-up
```

And then open the notebook at `http://localhost:8888`.
