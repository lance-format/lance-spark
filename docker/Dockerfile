# syntax=docker/dockerfile:1
FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      openjdk-17-jdk \
      build-essential \
      software-properties-common \
      ssh \
      python3 \
      python3-dev \
      python3-venv \
      python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create virtual environment and install Python dependencies
COPY requirements.txt .
RUN python3 -m venv /opt/venv && \
    . /opt/venv/bin/activate && \
    pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

ENV PATH="/opt/venv/bin:$PATH"

# Build arguments
ARG SPARK_DOWNLOAD_VERSION=4.0.0
ARG SPARK_MAJOR_VERSION=4.0
ARG SCALA_VERSION=2.13
ARG SPARK_SCALA_SUFFIX=

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

WORKDIR ${SPARK_HOME}

ENV SPARK_VERSION=${SPARK_DOWNLOAD_VERSION}
ENV SPARK_MAJOR_VERSION=${SPARK_MAJOR_VERSION}
ENV SCALA_VERSION=${SCALA_VERSION}
ENV LANCE_SPARK_VERSION=0.1.3-beta.8
ENV LANCE_NS_VERSION=0.4.5

# Download spark
RUN mkdir -p ${SPARK_HOME} \
 && curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3${SPARK_SCALA_SUFFIX}.tgz -o spark.tgz \
 && tar xvzf spark.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark.tgz

# Download lance-spark bundle JAR from Maven
RUN curl -L https://repo1.maven.org/maven2/com/lancedb/lance-spark-bundle-3.5_2.12/${LANCE_SPARK_VERSION}/lance-spark-bundle-3.5_2.12-${LANCE_SPARK_VERSION}.jar \
 -o /opt/spark/jars/lance-spark-bundle_2.12-${LANCE_SPARK_VERSION}.jar

# Download additional Lance namespace implementation jars from Maven
RUN curl -L https://repo1.maven.org/maven2/com/lancedb/lance-namespace-glue/${LANCE_NS_VERSION}/lance-namespace-glue-${LANCE_NS_VERSION}.jar \
 -o /opt/spark/jars/lance-namespace-glue-${LANCE_NS_VERSION}.jar

# For local testing, uncomment the lines below and comment out the Maven downloads above:
#COPY lance-spark-bundle-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${LANCE_SPARK_VERSION}.jar /opt/spark/jars/
#COPY lance-namespace-glue-${LANCE_NS_VERSION}.jar /opt/spark/jars/

# Download OpenDAL native libraries for Linux architectures
ENV OPENDAL_VERSION=0.48.0
RUN mkdir -p /tmp/opendal && cd /tmp/opendal \
 && curl -L https://repo1.maven.org/maven2/org/apache/opendal/opendal/${OPENDAL_VERSION}/opendal-${OPENDAL_VERSION}-linux-x86_64.jar -o opendal-linux-x86_64.jar \
 && curl -L https://repo1.maven.org/maven2/org/apache/opendal/opendal/${OPENDAL_VERSION}/opendal-${OPENDAL_VERSION}-linux-aarch_64.jar -o opendal-linux-aarch_64.jar \
 && mv *.jar /opt/spark/jars/ \
 && cd / && rm -rf /tmp/opendal

# Download AWS SDK Bundle v2
ENV AWS_SDK_VERSION=2.20.43
RUN curl -L https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar \
 -o /opt/spark/jars/aws-sdk-bundle-${AWS_SDK_VERSION}.jar

# Install AWS CLI (optional, for S3 support)
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
 && unzip awscliv2.zip \
 && sudo ./aws/install \
 && rm awscliv2.zip \
 && rm -rf aws/

# Create directories for Lance, notebooks, and Spark
RUN mkdir -p /home/lance/warehouse /home/lance/notebooks /home/lance/spark-events /home/lance/data

# Copy notebooks if available
COPY notebooks/ /home/lance/notebooks/

# Copy tests
COPY tests/ /home/lance/tests/

# Add a notebook command
RUN echo '#! /bin/sh' >> /bin/notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-lab' >> /bin/notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/lance/notebooks --ip='*' --ServerApp.token='' --ServerApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook \
 && echo "pyspark" >> /bin/notebook \
 && chmod u+x /bin/notebook

# Add a pyspark-notebook command (alias for notebook command for backwards-compatibility)
RUN echo '#! /bin/sh' >> /bin/pyspark-notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-lab' >> /bin/pyspark-notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/lance/notebooks --ip='*' --ServerApp.token='' --ServerApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/pyspark-notebook \
 && echo "pyspark" >> /bin/pyspark-notebook \
 && chmod u+x /bin/pyspark-notebook

COPY spark-defaults.conf /opt/spark/conf
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
CMD ["notebook"]
