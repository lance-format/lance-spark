SPARK_DOWNLOAD_VERSION_3.4 := 3.4.4
SPARK_DOWNLOAD_VERSION_3.5 := 3.5.8
SPARK_DOWNLOAD_VERSION_4.0 := 4.0.2
SPARK_DOWNLOAD_VERSION_4.1 := 4.1.1
SPARK_DOWNLOAD_VERSION_4.2 := 4.2.0

# apache/spark has no 3.x Scala 2.13 tag. 3.4 images are Java 11.
SPARK_IMAGE_3.4_2.12 := apache/spark:3.4.4-scala2.12-java11-python3-ubuntu
SPARK_IMAGE_3.5_2.12 := apache/spark:3.5.8-scala2.12-java17-python3-ubuntu
SPARK_IMAGE_4.0_2.13 := apache/spark:4.0.2-scala2.13-java17-python3-ubuntu
SPARK_IMAGE_4.1_2.13 := apache/spark:4.1.1-scala2.13-java17-python3-ubuntu
SPARK_IMAGE_4.2_2.13 := apache/spark:4.2.0-scala2.13-java17-python3-ubuntu
